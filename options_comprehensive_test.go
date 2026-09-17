package xamqp

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAllOptionFunctions(t *testing.T) {
	t.Run("connection", func(t *testing.T) {
		opts := getDefaultConnectionOptions()
		cfg := Config{Heartbeat: 17 * time.Second, FrameSize: 64 * 1024}
		WithConnectionOptionsReconnectInterval(250 * time.Millisecond)(&opts)
		WithConnectionOptionsConfig(cfg)(&opts)
		if opts.ReconnectInterval != 250*time.Millisecond || !reflect.DeepEqual(opts.Config, cfg) {
			t.Fatalf("connection options not applied: %+v", opts)
		}
	})

	t.Run("channel", func(t *testing.T) {
		opts := getDefaultChannelOptions()
		WithChannelOptionsQOSPrefetch(37)(&opts)
		WithChannelOptionsQOSGlobal(&opts)
		WithChannelOptionsConfirm(&opts)
		if opts.QOSPrefetch != 37 || !opts.QOSGlobal || !opts.ConfirmMode {
			t.Fatalf("channel options not applied: %+v", opts)
		}
	})

	t.Run("declarator", func(t *testing.T) {
		opts := getDefaultDeclareOptions()
		WithDeclareOptionsConfirm(&opts)
		if !opts.ConfirmMode {
			t.Fatal("declare confirm option not applied")
		}
	})

	t.Run("publisher", func(t *testing.T) {
		opts := getDefaultPublisherOptions()
		WithPublisherOptionsExchangeName("events")(&opts)
		WithPublisherOptionsExchangeKind(amqp.ExchangeTopic)(&opts)
		WithPublisherOptionsExchangeDurable(&opts)
		WithPublisherOptionsExchangeAutoDelete(&opts)
		WithPublisherOptionsExchangeInternal(&opts)
		WithPublisherOptionsExchangeNoWait(&opts)
		WithPublisherOptionsExchangeDeclare(&opts)
		WithPublisherOptionsExchangePassive(&opts)
		WithPublisherOptionsExchangeArgs(amqp.Table{"alternate-exchange": "fallback"})(&opts)
		WithPublisherOptionsConfirm(&opts)

		ex := opts.ExchangeOptions
		if ex.Name != "events" || ex.Kind != amqp.ExchangeTopic || !ex.Durable ||
			!ex.AutoDelete || !ex.Internal || !ex.NoWait || !ex.Declare || !ex.Passive ||
			ex.Args["alternate-exchange"] != "fallback" || !opts.ConfirmMode {
			t.Fatalf("publisher options not applied: %+v", opts)
		}
	})

	t.Run("consumer", func(t *testing.T) {
		opts := getDefaultConsumerOptions("queue")
		WithConsumerOptionsQueueDurable(&opts)
		WithConsumerOptionsQueueAutoDelete(&opts)
		WithConsumerOptionsQueueExpires(2 * time.Second)(&opts)
		WithConsumerOptionsQueueExclusive(&opts)
		WithConsumerOptionsQueueNoWait(&opts)
		WithConsumerOptionsQueuePassive(&opts)
		WithConsumerOptionsQueueNoDeclare(&opts)
		WithConsumerOptionsQueueArgs(amqp.Table{"x-max-length": int32(8)})(&opts)
		WithConsumerOptionsExchangeName("events")(&opts)
		WithConsumerOptionsExchangeKind(amqp.ExchangeHeaders)(&opts)
		WithConsumerOptionsExchangeDurable(&opts)
		WithConsumerOptionsExchangeAutoDelete(&opts)
		WithConsumerOptionsExchangeInternal(&opts)
		WithConsumerOptionsExchangeNoWait(&opts)
		WithConsumerOptionsExchangeDeclare(&opts)
		WithConsumerOptionsExchangePassive(&opts)
		WithConsumerOptionsExchangeArgs(amqp.Table{"alternate-exchange": "fallback"})(&opts)
		WithConsumerOptionsRoutingKey("orders.*")(&opts)
		WithConsumerOptionsBinding(Binding{RoutingKey: "custom", BindingOptions: BindingOptions{Declare: false}})(&opts)
		WithConsumerOptionsExchangeOptions(ExchangeOptions{Name: "second"})(&opts)
		WithConsumerOptionsConcurrency(7)(&opts)
		WithConsumerOptionsConsumerName("worker-1")(&opts)
		WithConsumerOptionsConsumerAutoAck(true)(&opts)
		WithConsumerOptionsConsumerExclusive(&opts)
		WithConsumerOptionsConsumerNoWait(&opts)
		WithConsumerOptionsQOSPrefetch(23)(&opts)
		WithConsumerOptionsQOSGlobal(&opts)
		WithConsumerOptionsForceShutdown(&opts)
		WithConsumerOptionsQueueQuorum(&opts)
		WithConsumerOptionsQueueMessageExpiration(3 * time.Second)(&opts)
		WithConsumerStreamOffset("first")(&opts)

		q := opts.QueueOptions
		if !q.Durable || !q.AutoDelete || !q.Exclusive || !q.NoWait || !q.Passive || q.Declare {
			t.Fatalf("queue options not applied: %+v", q)
		}
		if q.Args["x-expires"] != int64(2000) || q.Args["x-message-ttl"] != int64(3000) ||
			q.Args["x-max-length"] != int32(8) || q.Args["x-queue-type"] != "quorum" {
			t.Fatalf("queue args not applied/merged: %#v", q.Args)
		}
		if len(opts.ExchangeOptions) != 2 {
			t.Fatalf("exchange option count = %d, want 2", len(opts.ExchangeOptions))
		}
		ex := opts.ExchangeOptions[0]
		if ex.Name != "events" || ex.Kind != amqp.ExchangeHeaders || !ex.Durable ||
			!ex.AutoDelete || !ex.Internal || !ex.NoWait || !ex.Declare || !ex.Passive ||
			ex.Args["alternate-exchange"] != "fallback" || len(ex.Bindings) != 2 ||
			!ex.Bindings[0].Declare || ex.Bindings[1].Declare {
			t.Fatalf("consumer exchange options not applied: %+v", ex)
		}
		if opts.Concurrency != 7 || opts.QOSPrefetch != 23 || !opts.QOSGlobal || opts.CloseGracefully ||
			opts.RabbitConsumerOptions.Name != "worker-1" || !opts.RabbitConsumerOptions.AutoAck ||
			!opts.RabbitConsumerOptions.Exclusive || !opts.RabbitConsumerOptions.NoWait ||
			opts.RabbitConsumerOptions.Args["x-stream-offset"] != "first" {
			t.Fatalf("consumer runtime options not applied: %+v", opts)
		}
	})

	t.Run("publish", func(t *testing.T) {
		stamp := time.Unix(1_700_000_000, 0).UTC()
		opts := PublishOptions{}
		setters := allPublishOptionSetters(stamp)
		for _, set := range setters {
			set(&opts)
		}
		if opts.Exchange != "events" || !opts.Mandatory || !opts.Immediate ||
			opts.ContentType != "application/json" || opts.DeliveryMode != Persistent ||
			opts.Expiration != "1234" || opts.Headers["trace"] != "abc" ||
			opts.ContentEncoding != "gzip" || opts.Priority != 9 || opts.CorrelationID != "corr" ||
			opts.ReplyTo != "reply" || opts.MessageID != "message" || !opts.Timestamp.Equal(stamp) ||
			opts.Type != "event" || opts.UserID != "guest" || opts.AppID != "tester" {
			t.Fatalf("publish options not applied: %+v", opts)
		}
	})
}

func allPublishOptionSetters(stamp time.Time) []func(*PublishOptions) {
	return []func(*PublishOptions){
		WithPublishOptionsExchange("events"),
		WithPublishOptionsMandatory,
		WithPublishOptionsImmediate,
		WithPublishOptionsContentType("application/json"),
		WithPublishOptionsPersistentDelivery,
		WithPublishOptionsExpiration("1234"),
		WithPublishOptionsHeaders(amqp.Table{"trace": "abc"}),
		WithPublishOptionsContentEncoding("gzip"),
		WithPublishOptionsPriority(9),
		WithPublishOptionsCorrelationID("corr"),
		WithPublishOptionsReplyTo("reply"),
		WithPublishOptionsMessageID("message"),
		WithPublishOptionsTimestamp(stamp),
		WithPublishOptionsType("event"),
		WithPublishOptionsUserID("guest"),
		WithPublishOptionsAppID("tester"),
	}
}

func TestConstructorsRejectMissingConnectionManager(t *testing.T) {
	conn := &Conn{}
	if _, err := NewChannel(conn); err == nil {
		t.Fatal("NewChannel accepted a Conn without a manager")
	}
	if _, err := NewDeclarator(conn); err == nil {
		t.Fatal("NewDeclarator accepted a Conn without a manager")
	}
	if _, err := NewConsumer(conn, "queue"); err == nil {
		t.Fatal("NewConsumer accepted a Conn without a manager")
	}
	if _, err := NewPublisher(conn); err == nil {
		t.Fatal("NewPublisher accepted a Conn without a manager")
	}
}

func TestPublisherRejectsFlowAndConnectionBlocking(t *testing.T) {
	p := &Publisher{
		disablePublishDueToFlowMu:    new(sync.RWMutex),
		disablePublishDueToBlockedMu: new(sync.RWMutex),
	}
	p.disablePublishDueToFlow = true
	if err := p.PublishWithContext(context.Background(), nil, nil); !errors.Is(err, ErrPublishFlowPaused) {
		t.Fatalf("PublishWithContext flow error = %v", err)
	}
	if _, err := p.PublishWithDeferredConfirmWithContext(context.Background(), nil, nil); !errors.Is(err, ErrPublishFlowPaused) {
		t.Fatalf("deferred publish flow error = %v", err)
	}

	p.disablePublishDueToFlow = false
	p.disablePublishDueToBlocked = true
	if err := p.PublishWithContext(context.Background(), nil, nil); !errors.Is(err, ErrPublishBlocked) {
		t.Fatalf("PublishWithContext blocking error = %v", err)
	}
	if _, err := p.PublishWithDeferredConfirmWithContext(context.Background(), nil, nil); !errors.Is(err, ErrPublishBlocked) {
		t.Fatalf("deferred publish blocking error = %v", err)
	}
}

func TestPublisherReadsLatestBlockedState(t *testing.T) {
	p := &Publisher{disablePublishDueToBlockedMu: new(sync.RWMutex)}
	blocking := make(chan amqp.Blocking)
	done := make(chan struct{})
	go func() {
		p.readBlockedNotifications(blocking)
		close(done)
	}()

	assertState := func(want bool) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			p.disablePublishDueToBlockedMu.RLock()
			got := p.disablePublishDueToBlocked
			p.disablePublishDueToBlockedMu.RUnlock()
			if got == want {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("blocked state did not become %v", want)
	}

	blocking <- amqp.Blocking{Active: true, Reason: "alarm"}
	assertState(true)
	blocking <- amqp.Blocking{Active: false}
	assertState(false)
	close(blocking)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked notification reader did not exit")
	}
}

func TestOptionFunctionsInitializeNilArgumentMaps(t *testing.T) {
	consumer := getDefaultConsumerOptions("queue")
	consumer.QueueOptions.Args = nil
	WithConsumerOptionsQueueArgs(amqp.Table{"a": int32(1)})(&consumer)
	if consumer.QueueOptions.Args["a"] != int32(1) {
		t.Fatalf("queue args were not initialized: %#v", consumer.QueueOptions.Args)
	}
	consumer.ExchangeOptions = []ExchangeOptions{{Args: nil}}
	WithConsumerOptionsExchangeArgs(amqp.Table{"b": int32(2)})(&consumer)
	if consumer.ExchangeOptions[0].Args["b"] != int32(2) {
		t.Fatalf("exchange args were not initialized: %#v", consumer.ExchangeOptions[0].Args)
	}

	publisher := getDefaultPublisherOptions()
	publisher.ExchangeOptions.Args = nil
	WithPublisherOptionsExchangeArgs(amqp.Table{"c": int32(3)})(&publisher)
	if publisher.ExchangeOptions.Args["c"] != int32(3) {
		t.Fatalf("publisher exchange args were not initialized: %#v", publisher.ExchangeOptions.Args)
	}
}

func TestWaitForHandlerCompletionContextVariants(t *testing.T) {
	consumer := &Consumer{handlerMu: new(sync.RWMutex)}
	if err := consumer.waitForHandlerCompletion(nil); err != nil {
		t.Fatalf("nil context should behave like Background: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.waitForHandlerCompletion(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context error = %v", err)
	}
}

type recordingAcknowledger struct {
	acked        int
	nacked       int
	rejected     int
	lastMultiple bool
	lastRequeue  bool
}

func (a *recordingAcknowledger) Ack(_ uint64, multiple bool) error {
	a.acked++
	a.lastMultiple = multiple
	return nil
}

func (a *recordingAcknowledger) Nack(_ uint64, multiple, requeue bool) error {
	a.nacked++
	a.lastMultiple = multiple
	a.lastRequeue = requeue
	return nil
}

func (a *recordingAcknowledger) Reject(_ uint64, requeue bool) error {
	a.rejected++
	a.lastRequeue = requeue
	return nil
}

func TestConsumerHandlerActions(t *testing.T) {
	tests := []struct {
		name    string
		autoAck bool
		handler Handler
		acked   int
		nacked  int
		requeue bool
	}{
		{name: "ack", handler: func(Delivery) Action { return Ack }, acked: 1},
		{name: "discard", handler: func(Delivery) Action { return NackDiscard }, nacked: 1},
		{name: "requeue", handler: func(Delivery) Action { return NackRequeue }, nacked: 1, requeue: true},
		{name: "manual", handler: func(Delivery) Action { return Manual }},
		{name: "auto ack", autoAck: true, handler: func(Delivery) Action { return NackRequeue }},
		{name: "panic discards", handler: func(Delivery) Action { panic("boom") }, nacked: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := &recordingAcknowledger{}
			consumer := &Consumer{}
			consumer.handlerWrapper(ConsumerOptions{RabbitConsumerOptions: RabbitConsumerOptions{AutoAck: tt.autoAck}}, tt.handler,
				amqp.Delivery{Acknowledger: ack, DeliveryTag: 1})
			if ack.acked != tt.acked || ack.nacked != tt.nacked || ack.lastRequeue != tt.requeue || ack.rejected != 0 {
				t.Fatalf("acknowledgement result = %+v", ack)
			}
		})
	}
}
