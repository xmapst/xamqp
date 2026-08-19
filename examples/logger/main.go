// Command logger demonstrates how to plug a custom logging backend into xamqp.
//
// xamqp.ILogger is a small 4-method interface (Errorf/Warnf/Infof/Debugf).
// Unlike upstream go-rabbitmq's Logger interface, it does NOT have a Fatalf
// method — xamqp never calls os.Exit/panic on your behalf, so your logger
// implementation doesn't need to provide one either.
//
// Every component that talks to the broker (Conn, Publisher, Consumer,
// Channel, Declarator) accepts its own logger via a WithXxxOptionsLogger
// option, so you can inject a different logger per component or reuse the
// same instance everywhere, as this example does.
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp"
)

// prefixLogger is a minimal custom logger backend that implements
// xamqp.ILogger by wrapping the standard library "log" package and adding a
// level + component prefix to every line. A real application would typically
// forward these calls into its own structured logger (zap, slog, zerolog...)
// instead.
type prefixLogger struct {
	prefix string
}

// Errorf implements xamqp.ILogger.
func (l *prefixLogger) Errorf(format string, args ...any) {
	log.Printf("["+l.prefix+"] ERROR: "+format, args...)
}

// Warnf implements xamqp.ILogger.
func (l *prefixLogger) Warnf(format string, args ...any) {
	log.Printf("["+l.prefix+"] WARN: "+format, args...)
}

// Infof implements xamqp.ILogger.
func (l *prefixLogger) Infof(format string, args ...any) {
	log.Printf("["+l.prefix+"] INFO: "+format, args...)
}

// Debugf implements xamqp.ILogger.
func (l *prefixLogger) Debugf(format string, args ...any) {
	log.Printf("["+l.prefix+"] DEBUG: "+format, args...)
}

func main() {
	myLogger := &prefixLogger{prefix: "myapp"}

	// Injection point #1: the connection. This logger receives connection
	// level events such as dial attempts, reconnects and shutdown.
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@localhost",
		xamqp.WithConnectionOptionsLogger(myLogger),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			myLogger.Errorf("close connection: %v", err)
		}
	}()

	// Injection point #2: the publisher. Components created from the same
	// Conn can each be given their own logger; here we simply reuse myLogger.
	publisher, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsLogger(myLogger),
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	// Returned (unroutable) messages are reported through NotifyReturn; route
	// that straight through our custom logger too.
	publisher.NotifyReturn(func(r xamqp.Return) {
		myLogger.Warnf("message returned from server: %s", string(r.Body))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := publisher.PublishWithContext(
		ctx,
		[]byte("hello, world"),
		[]string{"my_routing_key"},
		xamqp.WithPublishOptionsContentType("application/json"),
		xamqp.WithPublishOptionsPersistentDelivery,
		xamqp.WithPublishOptionsExchange("events"),
	); err != nil {
		myLogger.Errorf("publish failed: %v", err)
	}

	// Injection point #3: the consumer. Same idea — pass the logger through
	// WithConsumerOptionsLogger.
	consumer, err := xamqp.NewConsumer(
		conn,
		"my_queue",
		xamqp.WithConsumerOptionsLogger(myLogger),
		xamqp.WithConsumerOptionsExchangeName("events"),
		xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithConsumerOptionsExchangeDurable,
		xamqp.WithConsumerOptionsExchangeDeclare,
		xamqp.WithConsumerOptionsRoutingKey("my_routing_key"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Consumer.Run() only blocks until the consumer has *started*; it returns
	// as soon as the first startup succeeds and lets reconnect/recovery run in
	// a background goroutine. It does NOT block for the lifetime of the
	// consumer, so main() must keep itself alive some other way if it wants
	// to keep receiving messages (signal.Notify, a channel, sync.WaitGroup,
	// ...). Here we just sleep briefly so the message published above has a
	// chance to be delivered, logged through myLogger, and acked.
	if err := consumer.Run(func(d xamqp.Delivery) xamqp.Action {
		myLogger.Infof("received message: %s", string(d.Body))
		return xamqp.Ack
	}); err != nil {
		log.Fatal(err)
	}

	myLogger.Infof("consumer started; Run() already returned, sleeping briefly to receive the demo message")
	time.Sleep(2 * time.Second)
}
