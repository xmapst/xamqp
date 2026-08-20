// Command publisher_confirm demonstrates xamqp's two publisher-confirm styles
// against a running RabbitMQ broker.
//
// IMPORTANT and different from the upstream wagslane/go-rabbitmq library:
// putting a Publisher into confirm mode (WithPublisherOptionsConfirm) does NOT
// make Publish/PublishWithContext block until the broker acks the message.
// Those calls stay fire-and-forget even in confirm mode; they only return once
// the message has been written to the socket. There are exactly two ways to
// actually observe a confirmation:
//
//  1. Asynchronous: register a NotifyPublish callback. It fires later, once
//     per published message, each invocation on its own goroutine (bounded
//     concurrency) -- so DeliveryTag order across messages is NOT guaranteed.
//  2. Synchronous: call PublishWithDeferredConfirmWithContext instead of
//     Publish/PublishWithContext. It returns a PublisherConfirmation
//     ([]*amqp.DeferredConfirmation) that you can Wait()/WaitContext() on
//     right where you are, to block for that specific message's ack.
//
// This example shows both. Run a local broker first, e.g.:
//
//	docker run -it --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp"
)

const (
	exchangeName = "publisher_confirm_example"
	routingKey   = "example.confirm"
)

func main() {
	// 1. Connect. xamqp logs reconnect/flow-control activity via the
	// standard library's log/slog package (slog.Default() by default).
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@localhost:5672/",
	)
	if err != nil {
		slog.Error("failed to connect to RabbitMQ", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("error closing connection", slog.Any("error", err))
		}
	}()

	// 2. Create a Publisher with WithPublisherOptionsConfirm. This puts the
	// underlying AMQP channel into RabbitMQ's confirm mode, so the broker
	// will ack/nack every message published on it -- but see the package
	// comment above: the ack/nack itself only ever reaches you via
	// NotifyPublish or via PublishWithDeferredConfirmWithContext's return
	// value, never as a side effect of a plain Publish call blocking.
	publisher, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsExchangeName(exchangeName),
		xamqp.WithPublisherOptionsExchangeKind(amqp.ExchangeDirect),
		xamqp.WithPublisherOptionsExchangeDeclare,
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		slog.Error("failed to create publisher", slog.Any("error", err))
		os.Exit(1)
	}
	defer publisher.Close()

	// Optional: know when a mandatory message couldn't be routed to any queue.
	publisher.NotifyReturn(func(r xamqp.Return) {
		slog.Warn("undeliverable message",
			slog.String("exchange", r.Exchange), slog.String("routing_key", r.RoutingKey), slog.String("body", string(r.Body)))
	})

	// -------------------------------------------------------------------
	// Style 1 (asynchronous): NotifyPublish.
	//
	// This handler receives every publish confirmation for this Publisher's
	// channel, but each call runs concurrently on its own goroutine (bounded
	// by an internal semaphore), so don't assume calls arrive in DeliveryTag
	// order or one-at-a-time. It is the only way to learn the outcome of a
	// plain Publish/PublishWithContext call.
	// -------------------------------------------------------------------
	publisher.NotifyPublish(func(p xamqp.Confirmation) {
		slog.Info("async confirm",
			slog.Bool("ack", p.Ack), slog.Uint64("delivery_tag", p.DeliveryTag), slog.Int("reconnects", p.ReconnectionCount))
	})

	slog.Info("style 1: fire-and-forget PublishWithContext, confirms arrive later via NotifyPublish")
	for i := range 5 {
		body := fmt.Appendf(nil, "async message #%d", i)

		// PublishWithContext returns as soon as the message is written to the
		// socket. It does NOT wait for the broker's ack, even though the
		// channel is in confirm mode. The corresponding Ack/Nack will show up
		// later in the NotifyPublish handler registered above.
		err := publisher.PublishWithContext(
			context.Background(),
			body,
			[]string{routingKey},
			xamqp.WithPublishOptionsExchange(exchangeName),
			xamqp.WithPublishOptionsContentType("text/plain"),
			xamqp.WithPublishOptionsPersistentDelivery,
		)
		if err != nil {
			// ErrPublishFlowPaused / ErrPublishBlocked are retryable sentinel
			// errors: the broker (or the TCP stack) is asking the publisher to
			// pause, not rejecting the message outright.
			switch {
			case errors.Is(err, xamqp.ErrPublishFlowPaused):
				slog.Warn("publish paused by server flow control, retry later", slog.Int("index", i), slog.Any("error", err))
			case errors.Is(err, xamqp.ErrPublishBlocked):
				slog.Warn("publish blocked by TCP-level block, retry later", slog.Int("index", i), slog.Any("error", err))
			default:
				slog.Warn("publish failed", slog.Int("index", i), slog.Any("error", err))
			}
			continue
		}
		slog.Info("sent async message (call returned immediately; no ack yet)", slog.Int("index", i))
	}

	// This program is about to move on to style 2 and then exit, so give the
	// NotifyPublish goroutine a moment to receive the acks for the five
	// messages above. A long-running service normally would NOT need this --
	// the callback keeps firing for as long as the Publisher is open.
	time.Sleep(500 * time.Millisecond)

	// -------------------------------------------------------------------
	// Style 2 (synchronous): PublishWithDeferredConfirmWithContext.
	//
	// Still fire-and-forget over the wire, but it hands back a
	// PublisherConfirmation ([]*amqp.DeferredConfirmation, one entry per
	// routing key) that the caller can explicitly wait on right here, instead
	// of correlating the result in an async callback.
	// -------------------------------------------------------------------
	slog.Info("style 2: PublishWithDeferredConfirmWithContext, wait for the ack explicitly")

	confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
		context.Background(),
		[]byte("synchronously confirmed message"),
		[]string{routingKey},
		xamqp.WithPublishOptionsExchange(exchangeName),
		xamqp.WithPublishOptionsContentType("text/plain"),
		xamqp.WithPublishOptionsPersistentDelivery,
	)
	if err != nil {
		switch {
		case errors.Is(err, xamqp.ErrPublishFlowPaused):
			slog.Error("publish paused by server flow control", slog.Any("error", err))
			os.Exit(1)
		case errors.Is(err, xamqp.ErrPublishBlocked):
			slog.Error("publish blocked by TCP-level block", slog.Any("error", err))
			os.Exit(1)
		default:
			slog.Error("deferred-confirm publish failed", slog.Any("error", err))
			os.Exit(1)
		}
	}

	for i, confirmation := range confirmations {
		if confirmation == nil {
			// Only happens if the publisher somehow isn't in confirm mode.
			slog.Info("no confirmation object (publisher not in confirm mode)", slog.Int("index", i))
			continue
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ok, err := confirmation.WaitContext(waitCtx)
		cancel()
		if err != nil {
			slog.Warn("error waiting for confirmation", slog.Int("index", i), slog.Any("error", err))
			continue
		}
		if ok {
			slog.Info("broker ACKed the message", slog.Int("index", i))
		} else {
			slog.Warn("broker NACKed the message", slog.Int("index", i))
		}
	}

	slog.Info("done")
}
