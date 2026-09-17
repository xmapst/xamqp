package xamqp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const extremeIntegrationTestsEnv = "XAMQP_EXTREME_TESTS"

func requireExtremeBroker(t *testing.T) (string, string) {
	t.Helper()
	if strings.ToUpper(os.Getenv(extremeIntegrationTestsEnv)) != "TRUE" {
		t.Skipf("set %s=TRUE to run destructive recovery stress tests", extremeIntegrationTestsEnv)
	}
	url := requireExternalBroker(t)
	container := os.Getenv(rabbitMQContainerEnv)
	if container == "" {
		t.Skipf("set %s to run destructive recovery stress tests", rabbitMQContainerEnv)
	}
	return url, container
}

func startRabbitMQContainer(t *testing.T, container string) {
	t.Helper()
	if out, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
		t.Fatalf("start RabbitMQ failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	waitForCondition(t, 30*time.Second, "RabbitMQ container readiness", func() bool {
		return exec.Command("docker", "exec", container, "rabbitmq-diagnostics", "-q", "ping").Run() == nil
	})
}

func TestExtremeConfirmedLoadSurvivesNativeAndFullReconnect(t *testing.T) {
	url, container := requireExtremeBroker(t)
	queue := fmt.Sprintf("xamqp_extreme_load_%d", time.Now().UnixNano())
	cleanupTestQueue(t, url, queue)

	conn, err := NewConn(url, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer conn.Close()
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
		WithConsumerOptionsConcurrency(16),
		WithConsumerOptionsQOSPrefetch(256),
	)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()

	const total = 10000
	seen := make([]atomic.Int32, total)
	var uniqueConsumed atomic.Int32
	if err := consumer.Run(func(d Delivery) Action {
		id, err := strconv.Atoi(string(d.Body))
		if err != nil || id < 0 || id >= total {
			t.Errorf("invalid message during recovery stress: %q", d.Body)
			return NackDiscard
		}
		if seen[id].Add(1) == 1 {
			uniqueConsumed.Add(1)
		}
		return Ack
	}); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}

	const publisherCount = 4
	publishers := make([]*Publisher, 0, publisherCount)
	for range publisherCount {
		publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		publishers = append(publishers, publisher)
		defer publisher.Close()
	}

	deadline := time.Now().Add(2 * time.Minute)
	var confirmed atomic.Int32
	var publishErrors atomic.Int32
	errCh := make(chan error, publisherCount)
	var wg sync.WaitGroup
	for worker, publisher := range publishers {
		wg.Go(func() {
			for id := worker; id < total; id += publisherCount {
				for {
					if time.Now().After(deadline) {
						errCh <- fmt.Errorf("publisher %d timed out retrying message %d", worker, id)
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					confirmation, err := publisher.PublishWithDeferredConfirmWithContext(
						ctx, strconv.AppendInt(nil, int64(id), 10), []string{queue})
					cancel()
					if err != nil || len(confirmation) != 1 || confirmation[0] == nil {
						publishErrors.Add(1)
						time.Sleep(50 * time.Millisecond)
						continue
					}
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					acked, waitErr := confirmation[0].WaitContext(ctx)
					cancel()
					if waitErr == nil && acked {
						confirmed.Add(1)
						break
					}
					publishErrors.Add(1)
					time.Sleep(50 * time.Millisecond)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	waitForCondition(t, 20*time.Second, "initial confirmed traffic", func() bool { return confirmed.Load() >= 20 })
	if killed := newManagementAPI(t, url).killConnections(t, ""); killed == 0 {
		t.Fatal("native-recovery disruption did not close any connection")
	}
	waitForCondition(t, 30*time.Second, "confirmed traffic to resume after native recovery",
		func() bool { return confirmed.Load() >= 80 })

	connectionGeneration := conn.connManager.ReconnectionCount()
	t.Cleanup(func() { _ = exec.Command("docker", "start", container).Run() })
	if out, err := exec.Command("docker", "stop", "--time", "2", container).CombinedOutput(); err != nil {
		t.Fatalf("stop RabbitMQ failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	waitForCondition(t, 10*time.Second, "connection to observe full outage", conn.IsClosed)
	if confirmed.Load() == total {
		t.Fatal("all messages finished before the full outage; stress test did not overlap active publishing")
	}
	time.Sleep(6 * time.Second)
	startRabbitMQContainer(t, container)
	waitForCondition(t, 60*time.Second, "fallback connection recovery", func() bool {
		return conn.connManager.ReconnectionCount() > connectionGeneration && !conn.IsClosed()
	})

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Until(deadline)):
		t.Fatal("publish workers did not finish after broker recovery")
	}
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	waitForCondition(t, 60*time.Second, "all confirmed messages to be consumed", func() bool {
		return uniqueConsumed.Load() == total
	})
	if confirmed.Load() != total {
		t.Fatalf("confirmed messages = %d, want %d", confirmed.Load(), total)
	}
	duplicates := 0
	for id := range total {
		count := seen[id].Load()
		if count == 0 {
			t.Errorf("message %d was never consumed", id)
		}
		if count > 1 {
			duplicates += int(count - 1)
		}
	}
	t.Logf("DATA extreme load: confirmed=%d unique-consumed=%d retry-errors=%d duplicate-deliveries=%d",
		confirmed.Load(), uniqueConsumed.Load(), publishErrors.Load(), duplicates)
}

func TestExtremeUnackedDeliveryRedeliveredAfterDisconnect(t *testing.T) {
	url, _ := requireExtremeBroker(t)
	queue := fmt.Sprintf("xamqp_extreme_unacked_%d", time.Now().UnixNano())
	cleanupTestQueue(t, url, queue)

	conn, err := NewConn(url, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer conn.Close()
	stateChanges := make(chan *amqp.StateChanged, 8)
	currentConnection(conn).NotifyStateChange(stateChanges)

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	redelivered := make(chan bool, 1)
	var calls atomic.Int32
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
		WithConsumerOptionsQOSPrefetch(1),
	)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()
	if err := consumer.Run(func(d Delivery) Action {
		if calls.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			return Ack
		}
		redelivered <- d.Redelivered
		return Ack
	}); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}
	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
		context.Background(), []byte("in-flight"), []string{queue}, WithPublishOptionsPersistentDelivery)
	if err != nil || len(confirmations) != 1 || confirmations[0] == nil {
		t.Fatalf("publish before disconnect failed: confirmations=%d err=%v", len(confirmations), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	acked, err := confirmations[0].WaitContext(ctx)
	cancel()
	if err != nil || !acked {
		t.Fatalf("broker did not confirm persistent message before disconnect: ack=%v err=%v", acked, err)
	}
	select {
	case <-firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not receive the initial delivery")
	}

	if killed := newManagementAPI(t, url).killConnections(t, ""); killed == 0 {
		t.Fatal("management API did not close the connection")
	}
	waitForCondition(t, 10*time.Second, "connection StateReconnecting", func() bool {
		select {
		case state := <-stateChanges:
			return state.To == amqp.StateReconnecting
		default:
			return false
		}
	})
	close(releaseFirst)

	select {
	case wasRedelivered := <-redelivered:
		if !wasRedelivered {
			t.Fatal("unacked delivery returned after disconnect without Redelivered=true")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("unacked in-flight message was not redelivered after automatic recovery")
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want exactly 2", calls.Load())
	}
}
