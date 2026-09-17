package xamqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newRawTestChannel(t *testing.T, url string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("raw AMQP connection failed: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("raw AMQP channel failed: %v", err)
	}
	t.Cleanup(func() {
		_ = ch.Close()
		_ = conn.Close()
	})
	return conn, ch
}

func waitRawGet(t *testing.T, ch *amqp.Channel, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		delivery, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("basic.get %s failed: %v", queue, err)
		}
		if ok {
			return delivery
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for a message in %s", queue)
	return amqp.Delivery{}
}

func assertQueueEmpty(t *testing.T, ch *amqp.Channel, queue string) {
	t.Helper()
	if delivery, ok, err := ch.Get(queue, true); err != nil {
		t.Fatalf("basic.get %s failed: %v", queue, err)
	} else if ok {
		t.Fatalf("queue %s unexpectedly contained %q", queue, delivery.Body)
	}
}

func cleanupTopology(t *testing.T, ch *amqp.Channel, queues, exchanges []string) {
	t.Helper()
	t.Cleanup(func() {
		for _, queue := range queues {
			_, _ = ch.QueueDelete(queue, false, false, false)
		}
		for _, exchange := range exchanges {
			_ = ch.ExchangeDelete(exchange, false, false)
		}
	})
}

func TestDeclaratorAndDirectChannelCompleteTopology(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_topology_%d", time.Now().UnixNano())
	source := prefix + "_source"
	destination := prefix + "_destination"
	q1 := prefix + "_q1"
	q2 := prefix + "_q2"

	conn := waitForHealthyAmqp(t, url)
	defer conn.Close()
	_, admin := newRawTestChannel(t, url)
	cleanupTopology(t, admin, []string{q1, q2}, []string{destination, source})

	declarator, err := NewDeclarator(conn, WithDeclareOptionsConfirm)
	if err != nil {
		t.Fatalf("NewDeclarator failed: %v", err)
	}
	defer declarator.Close()

	for _, exchange := range []ExchangeOptions{
		{Name: source, Kind: amqp.ExchangeDirect, Durable: true, Declare: true},
		{Name: destination, Kind: amqp.ExchangeFanout, Durable: true, Declare: true},
	} {
		if err := declarator.Exchange(exchange); err != nil {
			t.Fatalf("declare exchange %s failed: %v", exchange.Name, err)
		}
		passive := exchange
		passive.Passive = true
		if err := declarator.Exchange(passive); err != nil {
			t.Fatalf("passive exchange check %s failed: %v", exchange.Name, err)
		}
	}
	for _, queue := range []string{q1, q2} {
		options := QueueOptions{Name: queue, Durable: true, Declare: true, Args: amqp.Table{}}
		if err := declarator.Queue(options); err != nil {
			t.Fatalf("declare queue %s failed: %v", queue, err)
		}
		options.Passive = true
		if err := declarator.Queue(options); err != nil {
			t.Fatalf("passive queue check %s failed: %v", queue, err)
		}
	}

	directBindings := []Binding{
		{Source: source, Destination: q1, RoutingKey: "direct", BindingOptions: BindingOptions{Declare: true}},
		{Source: source, Destination: q2, RoutingKey: "direct", BindingOptions: BindingOptions{Declare: true}},
	}
	if err := declarator.BindQueues(directBindings); err != nil {
		t.Fatalf("BindQueues failed: %v", err)
	}

	ch, err := NewChannel(conn, WithChannelOptionsQOSPrefetch(5), WithChannelOptionsQOSGlobal, WithChannelOptionsConfirm)
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer ch.Close()

	confirmation, err := ch.PublishWithDeferredConfirmWithContextSafe(
		context.Background(), source, "direct", true, false, amqp.Publishing{Body: []byte("direct")})
	if err != nil {
		t.Fatalf("direct publish failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	acked, err := confirmation.WaitContext(ctx)
	cancel()
	if err != nil || !acked {
		t.Fatalf("direct publish confirmation = (%v, %v)", acked, err)
	}
	if got := string(waitRawGet(t, admin, q1, 5*time.Second).Body); got != "direct" {
		t.Fatalf("q1 body = %q", got)
	}
	if got := string(waitRawGet(t, admin, q2, 5*time.Second).Body); got != "direct" {
		t.Fatalf("q2 body = %q", got)
	}
	confirmations := ch.NotifyPublishSafe(make(chan amqp.Confirmation, 1))
	if err := ch.PublishSafe(source, "direct", false, false, amqp.Publishing{Body: []byte("async-confirm")}); err != nil {
		t.Fatalf("direct PublishSafe failed: %v", err)
	}
	select {
	case confirmation := <-confirmations:
		if !confirmation.Ack {
			t.Fatal("direct channel publish was negatively acknowledged")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct channel NotifyPublishSafe did not receive confirmation")
	}
	_ = waitRawGet(t, admin, q1, 5*time.Second)
	_ = waitRawGet(t, admin, q2, 5*time.Second)

	if err := declarator.UnbindQueue(directBindings[0]); err != nil {
		t.Fatalf("UnbindQueue failed: %v", err)
	}
	if err := declarator.UnbindQueues(directBindings[1:]); err != nil {
		t.Fatalf("UnbindQueues failed: %v", err)
	}
	returns := ch.NotifyReturnSafe(make(chan amqp.Return, 1))
	if err := ch.PublishSafe(source, "direct", true, false, amqp.Publishing{Body: []byte("unrouted")}); err != nil {
		t.Fatalf("mandatory direct-channel publish failed: %v", err)
	}
	select {
	case ret := <-returns:
		if ret.ReplyCode != amqp.NoRoute || string(ret.Body) != "unrouted" {
			t.Fatalf("unexpected returned message: %+v", ret)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mandatory unrouted direct-channel publish was not returned")
	}

	e2e := Binding{Source: source, Destination: destination, RoutingKey: "e2e"}
	queueAtDestination := Binding{Source: destination, Destination: q1, RoutingKey: "ignored"}
	if err := declarator.BindQueues([]Binding{queueAtDestination}); err != nil {
		t.Fatalf("bind destination queue failed: %v", err)
	}
	if err := declarator.BindExchanges([]Binding{e2e}); err != nil {
		t.Fatalf("BindExchanges failed: %v", err)
	}
	if err := ch.PublishWithContextSafe(context.Background(), source, "e2e", false, false,
		amqp.Publishing{Body: []byte("exchange-to-exchange")}); err != nil {
		t.Fatalf("exchange-to-exchange publish failed: %v", err)
	}
	if got := string(waitRawGet(t, admin, q1, 5*time.Second).Body); got != "exchange-to-exchange" {
		t.Fatalf("exchange-to-exchange body = %q", got)
	}
	if err := declarator.UnbindExchange(e2e); err != nil {
		t.Fatalf("UnbindExchange failed: %v", err)
	}
	if err := declarator.BindExchanges([]Binding{e2e}); err != nil {
		t.Fatalf("second BindExchanges failed: %v", err)
	}
	if err := declarator.UnbindExchanges([]Binding{e2e}); err != nil {
		t.Fatalf("UnbindExchanges failed: %v", err)
	}
}

func TestPublisherMessagePropertiesMandatoryAndMultipleRoutes(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_publish_%d", time.Now().UnixNano())
	q1, q2 := prefix+"_q1", prefix+"_q2"

	conn := waitForHealthyAmqp(t, url, WithConnectionOptionsConfig(Config{Heartbeat: 5 * time.Second}))
	defer conn.Close()
	if conn.IsClosed() {
		t.Fatal("new connection reports closed")
	}
	_, admin := newRawTestChannel(t, url)
	cleanupTopology(t, admin, []string{q1, q2}, nil)
	for _, queue := range []string{q1, q2} {
		if _, err := admin.QueueDeclare(queue, true, false, false, false,
			amqp.Table{"x-max-priority": int32(10)}); err != nil {
			t.Fatalf("declare %s failed: %v", queue, err)
		}
	}

	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	returned := make(chan Return, 1)
	publisher.NotifyReturn(func(ret Return) { returned <- ret })

	stamp := time.Unix(1_700_000_000, 0).UTC()
	body := []byte(`{"event":"created"}`)
	if err := publisher.Publish(body, []string{q1, q2},
		WithPublishOptionsContentType("application/json"),
		WithPublishOptionsPersistentDelivery,
		WithPublishOptionsExpiration("10000"),
		WithPublishOptionsHeaders(amqp.Table{"trace": "trace-1"}),
		WithPublishOptionsContentEncoding("utf-8"),
		WithPublishOptionsPriority(7),
		WithPublishOptionsCorrelationID("corr-1"),
		WithPublishOptionsReplyTo("reply.queue"),
		WithPublishOptionsMessageID("msg-1"),
		WithPublishOptionsTimestamp(stamp),
		WithPublishOptionsType("order.created"),
		WithPublishOptionsUserID("guest"),
		WithPublishOptionsAppID("xamqp-test"),
	); err != nil {
		t.Fatalf("multi-route publish failed: %v", err)
	}

	for _, queue := range []string{q1, q2} {
		d := waitRawGet(t, admin, queue, 5*time.Second)
		if string(d.Body) != string(body) || d.ContentType != "application/json" ||
			d.DeliveryMode != Persistent || d.Expiration != "10000" || d.Headers["trace"] != "trace-1" ||
			d.ContentEncoding != "utf-8" || d.Priority != 7 || d.CorrelationId != "corr-1" ||
			d.ReplyTo != "reply.queue" || d.MessageId != "msg-1" || !d.Timestamp.Equal(stamp) ||
			d.Type != "order.created" || d.UserId != "guest" || d.AppId != "xamqp-test" {
			t.Fatalf("message properties from %s do not match: %+v", queue, d)
		}
	}

	missing := prefix + "_missing"
	if err := publisher.Publish([]byte("return-me"), []string{missing}, WithPublishOptionsMandatory); err != nil {
		t.Fatalf("mandatory publish failed synchronously: %v", err)
	}
	select {
	case ret := <-returned:
		if ret.ReplyCode != amqp.NoRoute || ret.RoutingKey != missing || string(ret.Body) != "return-me" {
			t.Fatalf("unexpected publisher return: %+v", ret)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publisher NotifyReturn did not receive mandatory unrouted message")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := publisher.PublishWithContext(canceled, []byte("x"), []string{q1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish error = %v, want context.Canceled", err)
	}
}

func TestConsumerRoutingAndAcknowledgementModes(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_consume_%d", time.Now().UnixNano())

	t.Run("topic binding", func(t *testing.T) {
		queue, exchange := prefix+"_topic_q", prefix+"_topic_ex"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, []string{exchange})

		received := make(chan Delivery, 1)
		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsQueueDurable,
			WithConsumerOptionsExchangeName(exchange),
			WithConsumerOptionsExchangeKind(amqp.ExchangeTopic),
			WithConsumerOptionsExchangeDurable,
			WithConsumerOptionsExchangeDeclare,
			WithConsumerOptionsRoutingKey("orders.*"),
			WithConsumerOptionsConsumerName(prefix+"_worker"),
			WithConsumerOptionsQOSGlobal,
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(d Delivery) Action { received <- d; return Ack }); err != nil {
			t.Fatalf("consumer Run failed: %v", err)
		}

		publisher, err := NewPublisher(conn,
			WithPublisherOptionsExchangeName(exchange),
			WithPublisherOptionsExchangeKind(amqp.ExchangeTopic),
			WithPublisherOptionsExchangeDurable,
			WithPublisherOptionsExchangeDeclare,
		)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("topic"), []string{"orders.created"},
			WithPublishOptionsExchange(exchange)); err != nil {
			t.Fatalf("topic publish failed: %v", err)
		}
		select {
		case d := <-received:
			if string(d.Body) != "topic" {
				t.Fatalf("topic body = %q", d.Body)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("topic-routed message not consumed")
		}
	})

	t.Run("nack requeue then ack on quorum queue", func(t *testing.T) {
		queue := prefix + "_requeue"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)

		var calls atomic.Int32
		redelivered := make(chan bool, 1)
		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsQueueDurable,
			WithConsumerOptionsQueueQuorum,
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(d Delivery) Action {
			if calls.Add(1) == 1 {
				return NackRequeue
			}
			redelivered <- d.Redelivered
			return Ack
		}); err != nil {
			t.Fatalf("consumer Run failed: %v", err)
		}
		publisher, err := NewPublisher(conn)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("retry"), []string{queue}); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		select {
		case wasRedelivered := <-redelivered:
			if !wasRedelivered || calls.Load() != 2 {
				t.Fatalf("requeue calls=%d redelivered=%v", calls.Load(), wasRedelivered)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("nacked message was not redelivered")
		}
	})

	t.Run("manual ack", func(t *testing.T) {
		queue := prefix + "_manual"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)

		done := make(chan error, 1)
		consumer, err := NewConsumer(conn, queue, WithConsumerOptionsQueueDurable)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(d Delivery) Action {
			done <- d.Ack(false)
			return Manual
		}); err != nil {
			t.Fatalf("consumer Run failed: %v", err)
		}
		publisher, err := NewPublisher(conn)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("manual"), []string{queue}); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("manual Ack failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("manual-ack handler not called")
		}
	})
}

func TestConcurrentPublishAndConsumeLoad(t *testing.T) {
	url := requireExternalBroker(t)
	queue := fmt.Sprintf("xamqp_api_load_%d", time.Now().UnixNano())
	conn := waitForHealthyAmqp(t, url)
	defer conn.Close()
	_, admin := newRawTestChannel(t, url)
	cleanupTopology(t, admin, []string{queue}, nil)

	const total = 1000
	seen := make([]atomic.Int32, total)
	var consumed atomic.Int32
	done := make(chan struct{})
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
		WithConsumerOptionsConcurrency(16),
		WithConsumerOptionsQOSPrefetch(128),
	)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()
	if err := consumer.Run(func(d Delivery) Action {
		var id int
		if _, err := fmt.Sscanf(string(d.Body), "%d", &id); err != nil || id < 0 || id >= total {
			t.Errorf("invalid load-test message %q: %v", d.Body, err)
			return NackDiscard
		}
		seen[id].Add(1)
		if consumed.Add(1) == total {
			close(done)
		}
		return Ack
	}); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}

	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	var wg sync.WaitGroup
	errCh := make(chan error, total)
	for worker := range 10 {
		wg.Go(func() {
			for id := worker; id < total; id += 10 {
				if err := publisher.Publish(fmt.Appendf(nil, "%d", id), []string{queue}); err != nil {
					errCh <- fmt.Errorf("publish %d: %w", id, err)
				}
			}
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("consumed %d/%d messages", consumed.Load(), total)
	}
	for id := range total {
		if count := seen[id].Load(); count != 1 {
			t.Errorf("message %d consumed %d times", id, count)
		}
	}
}

func TestStartupFailuresAndCloseTimeouts(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_errors_%d", time.Now().UnixNano())

	t.Run("passive resources missing", func(t *testing.T) {
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		consumer, err := NewConsumer(conn, prefix+"_missing_queue",
			WithConsumerOptionsQueuePassive,
			WithConsumerOptionsQueueDurable)
		if err != nil {
			t.Fatalf("NewConsumer failed before passive declaration: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(Delivery) Action { return Ack }); err == nil {
			t.Fatal("passive declaration of a missing queue unexpectedly succeeded")
		}

		if _, err := NewPublisher(conn,
			WithPublisherOptionsExchangeName(prefix+"_missing_exchange"),
			WithPublisherOptionsExchangeKind(amqp.ExchangeDirect),
			WithPublisherOptionsExchangeDurable,
			WithPublisherOptionsExchangeDeclare,
			WithPublisherOptionsExchangePassive,
		); err == nil {
			t.Fatal("passive declaration of a missing exchange unexpectedly succeeded")
		}
	})

	for _, force := range []bool{false, true} {
		name := "context timeout"
		if force {
			name = "forced shutdown"
		}
		t.Run(name, func(t *testing.T) {
			queue := fmt.Sprintf("%s_close_%v", prefix, force)
			conn := waitForHealthyAmqp(t, url)
			defer conn.Close()
			_, admin := newRawTestChannel(t, url)
			cleanupTopology(t, admin, []string{queue}, nil)
			options := []func(*ConsumerOptions){WithConsumerOptionsQueueDurable}
			if force {
				options = append(options, WithConsumerOptionsForceShutdown)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			consumer, err := NewConsumer(conn, queue, options...)
			if err != nil {
				t.Fatalf("NewConsumer failed: %v", err)
			}
			if err := consumer.Run(func(Delivery) Action {
				close(entered)
				<-release
				return Ack
			}); err != nil {
				t.Fatalf("consumer Run failed: %v", err)
			}
			publisher, err := NewPublisher(conn)
			if err != nil {
				t.Fatalf("NewPublisher failed: %v", err)
			}
			defer publisher.Close()
			if err := publisher.Publish([]byte("block"), []string{queue}); err != nil {
				t.Fatalf("publish failed: %v", err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not start")
			}

			started := time.Now()
			if force {
				consumer.Close()
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				consumer.CloseWithContext(ctx)
				cancel()
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("close took %s while handler was blocked", elapsed)
			}
			close(release)
		})
	}
}

func TestQueueTTLStreamAndAutoAck(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_queueopts_%d", time.Now().UnixNano())

	t.Run("message TTL", func(t *testing.T) {
		queue := prefix + "_ttl"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)

		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsQueueDurable,
			WithConsumerOptionsQueueMessageExpiration(200*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		if err := consumer.Run(func(Delivery) Action { return Ack }); err != nil {
			t.Fatalf("consumer Run failed: %v", err)
		}
		consumer.Close()

		publisher, err := NewPublisher(conn)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("expire"), []string{queue}); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		time.Sleep(700 * time.Millisecond)
		assertQueueEmpty(t, admin, queue)
	})

	t.Run("stream offset", func(t *testing.T) {
		queue := prefix + "_stream"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)

		received := make(chan Delivery, 1)
		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsQueueDurable,
			WithConsumerOptionsQueueArgs(amqp.Table{"x-queue-type": "stream"}),
			WithConsumerOptionsConsumerName(prefix+"_stream_consumer"),
			WithConsumerStreamOffset("first"),
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(d Delivery) Action { received <- d; return Ack }); err != nil {
			t.Fatalf("stream consumer Run failed: %v", err)
		}
		publisher, err := NewPublisher(conn)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("stream"), []string{queue}, WithPublishOptionsPersistentDelivery); err != nil {
			t.Fatalf("stream publish failed: %v", err)
		}
		select {
		case d := <-received:
			if string(d.Body) != "stream" {
				t.Fatalf("stream body = %q", d.Body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream message not consumed")
		}
	})

	t.Run("auto ack", func(t *testing.T) {
		queue := prefix + "_autoack"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)

		var calls atomic.Int32
		done := make(chan struct{}, 1)
		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsQueueDurable,
			WithConsumerOptionsConsumerAutoAck(true),
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}
		defer consumer.Close()
		if err := consumer.Run(func(Delivery) Action {
			calls.Add(1)
			done <- struct{}{}
			return NackRequeue // AutoAck 模式应忽略 handler 返回的确认动作。
		}); err != nil {
			t.Fatalf("consumer Run failed: %v", err)
		}
		publisher, err := NewPublisher(conn)
		if err != nil {
			t.Fatalf("NewPublisher failed: %v", err)
		}
		defer publisher.Close()
		if err := publisher.Publish([]byte("auto"), []string{queue}); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("auto-ack message not consumed")
		}
		time.Sleep(300 * time.Millisecond)
		if calls.Load() != 1 {
			t.Fatalf("auto-ack message handled %d times, want 1", calls.Load())
		}
	})
}

func TestConsumerQueueLifecycleModes(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_lifecycle_options_%d", time.Now().UnixNano())

	t.Run("no-declare and passive existing queue", func(t *testing.T) {
		queue := prefix + "_existing"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)
		if _, err := admin.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("predeclare queue failed: %v", err)
		}

		for _, mode := range []struct {
			name string
			opts []func(*ConsumerOptions)
		}{
			{name: "no-declare", opts: []func(*ConsumerOptions){WithConsumerOptionsQueueNoDeclare}},
			{name: "passive", opts: []func(*ConsumerOptions){WithConsumerOptionsQueueDurable, WithConsumerOptionsQueuePassive}},
		} {
			t.Run(mode.name, func(t *testing.T) {
				received := make(chan struct{}, 1)
				consumer, err := NewConsumer(conn, queue, mode.opts...)
				if err != nil {
					t.Fatalf("NewConsumer failed: %v", err)
				}
				if err := consumer.Run(func(Delivery) Action { received <- struct{}{}; return Ack }); err != nil {
					consumer.Close()
					t.Fatalf("consumer Run failed: %v", err)
				}
				publisher, err := NewPublisher(conn)
				if err != nil {
					consumer.Close()
					t.Fatalf("NewPublisher failed: %v", err)
				}
				if err := publisher.Publish([]byte(mode.name), []string{queue}); err != nil {
					publisher.Close()
					consumer.Close()
					t.Fatalf("publish failed: %v", err)
				}
				select {
				case <-received:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s consumer did not receive message", mode.name)
				}
				publisher.Close()
				consumer.Close()
			})
		}
	})

	t.Run("exclusive consumer", func(t *testing.T) {
		queue := prefix + "_exclusive"
		conn := waitForHealthyAmqp(t, url)
		defer conn.Close()
		_, admin := newRawTestChannel(t, url)
		cleanupTopology(t, admin, []string{queue}, nil)
		if _, err := admin.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("predeclare queue failed: %v", err)
		}
		newExclusive := func() *Consumer {
			consumer, err := NewConsumer(conn, queue,
				WithConsumerOptionsQueueNoDeclare,
				WithConsumerOptionsConsumerExclusive)
			if err != nil {
				t.Fatalf("NewConsumer failed: %v", err)
			}
			return consumer
		}
		first := newExclusive()
		defer first.Close()
		if err := first.Run(func(Delivery) Action { return Ack }); err != nil {
			t.Fatalf("first exclusive consumer failed: %v", err)
		}
		second := newExclusive()
		defer second.Close()
		if err := second.Run(func(Delivery) Action { return Ack }); err == nil {
			t.Fatal("second exclusive consumer unexpectedly succeeded")
		}
	})

	for _, mode := range []struct {
		name string
		opts []func(*ConsumerOptions)
	}{
		{name: "auto-delete", opts: []func(*ConsumerOptions){WithConsumerOptionsQueueDurable, WithConsumerOptionsQueueAutoDelete}},
		{name: "expires", opts: []func(*ConsumerOptions){WithConsumerOptionsQueueExpires(500 * time.Millisecond)}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			queue := prefix + "_" + mode.name
			conn := waitForHealthyAmqp(t, url)
			defer conn.Close()
			cleanupTestQueue(t, url, queue)
			consumer, err := NewConsumer(conn, queue, mode.opts...)
			if err != nil {
				t.Fatalf("NewConsumer failed: %v", err)
			}
			if err := consumer.Run(func(Delivery) Action { return Ack }); err != nil {
				t.Fatalf("consumer Run failed: %v", err)
			}
			consumer.Close()

			mgmt := newManagementAPI(t, url)
			deadline := time.Now().Add(8 * time.Second)
			for {
				code, _ := mgmt.do(t, "GET", "/api/queues/%2F/"+queue)
				if code == 404 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s queue still exists after consumer close", mode.name)
				}
				time.Sleep(100 * time.Millisecond)
			}
		})
	}
}

func TestMultipleExchangeBindingsAndAlternateExchange(t *testing.T) {
	url := requireExternalBroker(t)
	prefix := fmt.Sprintf("xamqp_api_exchanges_%d", time.Now().UnixNano())
	queue := prefix + "_multi_q"
	headersExchange := prefix + "_headers"
	topicExchange := prefix + "_topic"
	alternateQueue := prefix + "_alternate_q"
	alternateExchange := prefix + "_alternate"
	primaryExchange := prefix + "_primary"

	conn := waitForHealthyAmqp(t, url)
	defer conn.Close()
	_, admin := newRawTestChannel(t, url)
	cleanupTopology(t, admin, []string{queue, alternateQueue},
		[]string{primaryExchange, alternateExchange, topicExchange, headersExchange})

	received := make(chan string, 4)
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
		WithConsumerOptionsExchangeName(headersExchange),
		WithConsumerOptionsExchangeKind(amqp.ExchangeHeaders),
		WithConsumerOptionsExchangeDurable,
		WithConsumerOptionsExchangeDeclare,
		WithConsumerOptionsBinding(Binding{BindingOptions: BindingOptions{
			Declare: true,
			Args:    amqp.Table{"x-match": "all", "format": "pdf"},
		}}),
		WithConsumerOptionsExchangeOptions(ExchangeOptions{
			Name: headersExchange + "_skipped", Kind: amqp.ExchangeDirect, Declare: false,
		}),
		WithConsumerOptionsExchangeOptions(ExchangeOptions{
			Name: topicExchange, Kind: amqp.ExchangeTopic, Durable: true, Declare: true,
			Bindings: []Binding{{RoutingKey: "audit.#", BindingOptions: BindingOptions{Declare: true}}},
		}),
	)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()
	if err := consumer.Run(func(d Delivery) Action { received <- string(d.Body); return Ack }); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}
	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	if err := publisher.Publish([]byte("headers"), []string{"ignored"},
		WithPublishOptionsExchange(headersExchange),
		WithPublishOptionsHeaders(amqp.Table{"format": "pdf"})); err != nil {
		t.Fatalf("headers publish failed: %v", err)
	}
	if err := publisher.Publish([]byte("topic"), []string{"audit.eu.created"},
		WithPublishOptionsExchange(topicExchange)); err != nil {
		t.Fatalf("second exchange publish failed: %v", err)
	}
	want := map[string]bool{"headers": false, "topic": false}
	deadline := time.After(5 * time.Second)
	for remaining := 2; remaining > 0; {
		select {
		case body := <-received:
			if _, ok := want[body]; !ok || want[body] {
				t.Fatalf("unexpected or duplicate routed body %q", body)
			}
			want[body] = true
			remaining--
		case <-deadline:
			t.Fatalf("did not receive all multi-exchange messages: %#v", want)
		}
	}

	declarator, err := NewDeclarator(conn)
	if err != nil {
		t.Fatalf("NewDeclarator failed: %v", err)
	}
	defer declarator.Close()
	if err := declarator.Exchange(ExchangeOptions{Name: alternateExchange, Kind: amqp.ExchangeFanout, Durable: true, Declare: true}); err != nil {
		t.Fatalf("declare alternate exchange failed: %v", err)
	}
	if err := declarator.Queue(QueueOptions{Name: alternateQueue, Durable: true, Declare: true, Args: amqp.Table{}}); err != nil {
		t.Fatalf("declare alternate queue failed: %v", err)
	}
	if err := declarator.BindQueues([]Binding{{Source: alternateExchange, Destination: alternateQueue}}); err != nil {
		t.Fatalf("bind alternate queue failed: %v", err)
	}
	alternatePublisher, err := NewPublisher(conn,
		WithPublisherOptionsExchangeName(primaryExchange),
		WithPublisherOptionsExchangeKind(amqp.ExchangeDirect),
		WithPublisherOptionsExchangeDurable,
		WithPublisherOptionsExchangeDeclare,
		WithPublisherOptionsExchangeArgs(amqp.Table{"alternate-exchange": alternateExchange}),
	)
	if err != nil {
		t.Fatalf("publisher with alternate exchange failed: %v", err)
	}
	defer alternatePublisher.Close()
	if err := alternatePublisher.Publish([]byte("alternate"), []string{"no-primary-binding"},
		WithPublishOptionsExchange(primaryExchange)); err != nil {
		t.Fatalf("alternate-exchange publish failed: %v", err)
	}
	if got := string(waitRawGet(t, admin, alternateQueue, 5*time.Second).Body); got != "alternate" {
		t.Fatalf("alternate exchange body = %q", got)
	}
}

func TestImmediateFlagClosesChannelAndPublisherRecovers(t *testing.T) {
	url := requireExternalBroker(t)
	queue := fmt.Sprintf("xamqp_api_immediate_%d", time.Now().UnixNano())
	conn := waitForHealthyAmqp(t, url, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	defer conn.Close()
	_, admin := newRawTestChannel(t, url)
	cleanupTopology(t, admin, []string{queue}, nil)
	if _, err := admin.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue failed: %v", err)
	}

	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer publisher.Close()
	stateChanges := make(chan *amqp.StateChanged, 8)
	publisher.chanManager.WithChannelForNewGeneration(func(ch *amqp.Channel, _ uint) {
		ch.NotifyStateChange(stateChanges)
	})

	// RabbitMQ 不实现 immediate；客户端会把标志发给 Broker，Broker 随后以
	// NOT_IMPLEMENTED 关闭通道。这里验证该选项确实透传，且关闭后能恢复。
	_ = publisher.Publish([]byte("immediate"), []string{queue}, WithPublishOptionsImmediate)
	recovering := false
	recovered := false
	deadline := time.Now().Add(10 * time.Second)
	for !recovered && time.Now().Before(deadline) {
		select {
		case state := <-stateChanges:
			if state.To == amqp.StateReconnecting {
				recovering = true
			}
			if recovering && state.To == amqp.StateOpen {
				recovered = true
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !recovering || !recovered {
		t.Fatalf("immediate recovery transitions incomplete: reconnecting=%v open=%v", recovering, recovered)
	}

	var lastErr error
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
			context.Background(), []byte("after-immediate"), []string{queue})
		if err != nil || len(confirmations) != 1 || confirmations[0] == nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		acked, waitErr := confirmations[0].WaitContext(ctx)
		cancel()
		if waitErr == nil && acked {
			if got := string(waitRawGet(t, admin, queue, 5*time.Second).Body); got != "after-immediate" {
				t.Fatalf("post-recovery body = %q", got)
			}
			return
		}
		lastErr = waitErr
	}
	t.Fatalf("publisher did not recover after immediate rejection: %v", lastErr)
}
