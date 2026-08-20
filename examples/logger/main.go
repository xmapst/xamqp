// Command logger demonstrates xamqp's logging behavior.
//
// xamqp has no custom logging interface: it logs directly through the
// standard library's log/slog package (slog.Info/Warn/Error/Debug), writing
// to slog.Default(). To customize the output - format, destination, minimum
// level, extra attributes - configure the process-wide default logger with
// slog.SetDefault before creating any xamqp Conn/Publisher/Consumer; xamqp
// picks it up automatically, with no per-component injection needed.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp"
)

func main() {
	// Replace the process-wide default slog logger. Here: JSON output to
	// stdout, debug level enabled, with a fixed "component" attribute so log
	// lines from xamqp can be distinguished from the rest of the app.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})).With(slog.String("component", "myapp")))

	conn, err := xamqp.NewConn("amqp://guest:guest@localhost")
	if err != nil {
		slog.Error("connect failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("close connection", slog.Any("error", err))
		}
	}()

	publisher, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		slog.Error("create publisher failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer publisher.Close()

	// Returned (unroutable) messages are reported through NotifyReturn.
	publisher.NotifyReturn(func(r xamqp.Return) {
		slog.Warn("message returned from server", slog.String("body", string(r.Body)))
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
		slog.Error("publish failed", slog.Any("error", err))
	}

	consumer, err := xamqp.NewConsumer(
		conn,
		"my_queue",
		xamqp.WithConsumerOptionsExchangeName("events"),
		xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithConsumerOptionsExchangeDurable,
		xamqp.WithConsumerOptionsExchangeDeclare,
		xamqp.WithConsumerOptionsRoutingKey("my_routing_key"),
	)
	if err != nil {
		slog.Error("create consumer failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer consumer.Close()

	// Consumer.Run() only blocks until the consumer has *started*; it returns
	// as soon as the first startup succeeds and lets reconnect/recovery run in
	// a background goroutine. It does NOT block for the lifetime of the
	// consumer, so main() must keep itself alive some other way if it wants
	// to keep receiving messages (signal.Notify, a channel, sync.WaitGroup,
	// ...). Here we just sleep briefly so the message published above has a
	// chance to be delivered, logged, and acked.
	if err := consumer.Run(func(d xamqp.Delivery) xamqp.Action {
		slog.Info("received message", slog.String("body", string(d.Body)))
		return xamqp.Ack
	}); err != nil {
		slog.Error("consumer.Run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("consumer started; Run() already returned, sleeping briefly to receive the demo message")
	time.Sleep(2 * time.Second)
}
