package xamqp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func waitForCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForNativeRecoveryExhaustion waits for amqp091-go to definitively give up
// native recovery. A fixed delay cannot prove this phase transition: retry jitter
// and slow CI hosts can make the configured attempts take longer than expected.
func waitForNativeRecoveryExhaustion(t *testing.T, stateChanges <-chan *amqp.StateChanged, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case state, ok := <-stateChanges:
			if !ok {
				t.Fatal("connection state listener closed before reporting native recovery exhaustion")
			}
			if state.To != amqp.StateClosed {
				continue
			}
			if state.Err == nil {
				t.Fatal("connection closed gracefully instead of exhausting native recovery")
			}
			return
		case <-timer.C:
			t.Fatal("timed out waiting for native recovery to reach terminal StateClosed")
		}
	}
}

func currentConnection(conn *Conn) *amqp.Connection {
	var current *amqp.Connection
	_ = conn.connManager.WithConnection(func(c *amqp.Connection) error {
		current = c
		return nil
	})
	return current
}

func currentManagedChannel(ch *Channel) *amqp.Channel {
	var current *amqp.Channel
	ch.WithChannelForNewGeneration(func(c *amqp.Channel, _ uint) { current = c })
	return current
}

func publishDirectConfirmed(t *testing.T, ch *Channel, queue, body string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		confirmation, err := ch.PublishWithDeferredConfirmWithContextSafe(
			ctx, "", queue, false, false, amqp.Publishing{Body: []byte(body), DeliveryMode: Persistent})
		cancel()
		if err != nil || confirmation == nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		acked, err := confirmation.WaitContext(ctx)
		cancel()
		if err == nil && acked {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("direct channel did not publish a confirmed message after recovery: %v", lastErr)
}

func awaitDeliveryBody(t *testing.T, deliveries <-chan amqp.Delivery, want string, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				t.Fatal("delivery channel closed instead of surviving recovery")
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("ack %q failed: %v", delivery.Body, err)
			}
			if string(delivery.Body) == want {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for delivery %q", want)
		}
	}
}

func TestNativeRecoveryReusesConnAndDirectChannel(t *testing.T) {
	url := requireExternalBroker(t)
	queue := fmt.Sprintf("xamqp_recovery_native_%d", time.Now().UnixNano())
	cleanupTestQueue(t, url, queue)

	conn, err := NewConn(url, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer conn.Close()
	direct, err := NewChannel(conn, WithChannelOptionsConfirm, WithChannelOptionsQOSPrefetch(10))
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer direct.Close()
	if _, err := direct.QueueDeclareSafe(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclareSafe failed: %v", err)
	}
	deliveries, err := direct.ConsumeSafe(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ConsumeSafe failed: %v", err)
	}

	oldConn := currentConnection(conn)
	oldChannel := currentManagedChannel(direct)
	connStates := make(chan *amqp.StateChanged, 8)
	oldConn.NotifyStateChange(connStates)
	channelStates := make(chan *amqp.StateChanged, 8)
	oldChannel.NotifyStateChange(channelStates)

	publishDirectConfirmed(t, direct, queue, "before-native-recovery")
	awaitDeliveryBody(t, deliveries, "before-native-recovery", 5*time.Second)

	killed := newManagementAPI(t, url).killConnections(t, "")
	if killed == 0 {
		t.Fatal("management API did not close the test connection")
	}
	waitForLifecycleTransition := func(ch <-chan *amqp.StateChanged, want amqp.LifeCycleState, label string) {
		t.Helper()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case state := <-ch:
				if state.To == want {
					return
				}
			case <-deadline:
				t.Fatalf("%s did not reach state %s", label, want)
			}
		}
	}
	waitForLifecycleTransition(connStates, amqp.StateReconnecting, "connection")
	waitForLifecycleTransition(connStates, amqp.StateOpen, "connection")
	waitForLifecycleTransition(channelStates, amqp.StateReconnecting, "channel")
	waitForLifecycleTransition(channelStates, amqp.StateOpen, "channel")

	if got := currentConnection(conn); got != oldConn {
		t.Fatal("native recovery replaced the connection pointer instead of recovering it in place")
	}
	if got := currentManagedChannel(direct); got != oldChannel {
		t.Fatal("native recovery replaced the direct channel pointer instead of recovering it in place")
	}
	if conn.IsClosed() {
		t.Fatal("Conn still reports closed after native recovery reached StateOpen")
	}

	publishDirectConfirmed(t, direct, queue, "after-native-recovery")
	awaitDeliveryBody(t, deliveries, "after-native-recovery", 10*time.Second)
}

func TestFullReconnectRestoresConnChannelPublisherAndConsumer(t *testing.T) {
	url := requireExternalBroker(t)
	container := os.Getenv(rabbitMQContainerEnv)
	if container == "" {
		t.Skipf("set %s to run full recovery matrix", rabbitMQContainerEnv)
	}
	queue := fmt.Sprintf("xamqp_recovery_full_%d", time.Now().UnixNano())
	cleanupTestQueue(t, url, queue)

	conn, err := NewConn(url, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer conn.Close()
	received := make(chan string, 32)
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsQueueDurable)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()
	if err := consumer.Run(func(d Delivery) Action { received <- string(d.Body); return Ack }); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}
	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	direct, err := NewChannel(conn, WithChannelOptionsConfirm)
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer direct.Close()

	confirmEvents := make(chan Confirmation, 16)
	returnEvents := make(chan Return, 4)
	publisher.NotifyPublish(func(c Confirmation) { confirmEvents <- c })
	publisher.NotifyReturn(func(r Return) { returnEvents <- r })
	if !publishAndAwait(t, publisher, queue, "before-full-recovery", received, 10*time.Second) {
		t.Fatal("baseline Publisher/Consumer round-trip failed")
	}
	select {
	case <-confirmEvents:
	case <-time.After(5 * time.Second):
		t.Fatal("baseline publish confirmation callback did not fire")
	}
	publishDirectConfirmed(t, direct, queue, "before-full-direct")
	select {
	case body := <-received:
		if body != "before-full-direct" {
			t.Fatalf("unexpected baseline direct body %q", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("baseline direct Channel message not consumed")
	}

	oldConn := currentConnection(conn)
	oldPublisherChannel := func() *amqp.Channel {
		var ch *amqp.Channel
		publisher.chanManager.WithChannelForNewGeneration(func(current *amqp.Channel, _ uint) { ch = current })
		return ch
	}()
	oldConsumerChannel := func() *amqp.Channel {
		var ch *amqp.Channel
		consumer.chanManager.WithChannelForNewGeneration(func(current *amqp.Channel, _ uint) { ch = current })
		return ch
	}()
	oldDirectChannel := currentManagedChannel(direct)
	connectionStates := make(chan *amqp.StateChanged, 8)
	oldConn.NotifyStateChange(connectionStates)
	connectionGeneration := conn.connManager.ReconnectionCount()
	publisherGeneration := publisher.chanManager.ReconnectionCount()
	consumerGeneration := consumer.chanManager.ReconnectionCount()
	directGeneration := direct.ReconnectionCount()

	t.Cleanup(func() { _ = exec.Command("docker", "start", container).Run() })
	if out, err := exec.Command("docker", "stop", "--time", "2", container).CombinedOutput(); err != nil {
		t.Fatalf("stop RabbitMQ failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	waitForCondition(t, 10*time.Second, "Conn to observe broker outage", conn.IsClosed)
	waitForNativeRecoveryExhaustion(t, connectionStates, 15*time.Second)
	if out, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
		t.Fatalf("start RabbitMQ failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	waitForCondition(t, 30*time.Second, "RabbitMQ readiness", func() bool {
		return exec.Command("docker", "exec", container, "rabbitmq-diagnostics", "-q", "ping").Run() == nil
	})
	waitForCondition(t, 60*time.Second, "connection manager full reconnect", func() bool {
		return conn.connManager.ReconnectionCount() > connectionGeneration && !conn.IsClosed()
	})
	waitForCondition(t, 60*time.Second, "all channel managers to rebuild", func() bool {
		return publisher.chanManager.ReconnectionCount() > publisherGeneration &&
			consumer.chanManager.ReconnectionCount() > consumerGeneration &&
			direct.ReconnectionCount() > directGeneration
	})

	if currentConnection(conn) == oldConn {
		t.Fatal("full reconnect did not replace the dead AMQP connection")
	}
	var newPublisherChannel, newConsumerChannel *amqp.Channel
	publisher.chanManager.WithChannelForNewGeneration(func(ch *amqp.Channel, _ uint) { newPublisherChannel = ch })
	consumer.chanManager.WithChannelForNewGeneration(func(ch *amqp.Channel, _ uint) { newConsumerChannel = ch })
	if newPublisherChannel == oldPublisherChannel || newConsumerChannel == oldConsumerChannel ||
		currentManagedChannel(direct) == oldDirectChannel {
		t.Fatal("one or more channel managers retained a dead channel after full reconnect")
	}

	// Channel rebuild dispatch and Publisher/Consumer setup happen asynchronously after
	// their generation counters advance. Poll public behavior using the same objects.
	if !publishAndAwait(t, publisher, queue, "after-full-recovery", received, 60*time.Second) {
		t.Fatal("same Publisher/Consumer objects did not recover after full reconnect")
	}
	select {
	case confirmation := <-confirmEvents:
		if !confirmation.Ack {
			t.Fatal("post-reconnect publisher confirmation was a nack")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("NotifyPublish callback was not restored after full reconnect")
	}
	missingRoute := queue + "_missing"
	if err := publisher.Publish([]byte("return-after-reconnect"), []string{missingRoute}, WithPublishOptionsMandatory); err != nil {
		t.Fatalf("mandatory publish after reconnect failed: %v", err)
	}
	select {
	case returned := <-returnEvents:
		if returned.RoutingKey != missingRoute || string(returned.Body) != "return-after-reconnect" {
			t.Fatalf("unexpected post-reconnect return: %+v", returned)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("NotifyReturn callback was not restored after full reconnect")
	}
	publishDirectConfirmed(t, direct, queue, "after-full-direct")
	select {
	case body := <-received:
		if body != "after-full-direct" {
			t.Fatalf("unexpected post-reconnect direct body %q", body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("same direct Channel did not recover after full reconnect")
	}
}
