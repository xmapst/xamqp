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
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp"
)

const (
	exchangeName = "publisher_confirm_example"
	routingKey   = "example.confirm"
)

func main() {
	// 1. Connect. WithConnectionOptionsLogging turns on xamqp's default
	// stdout/stderr logger so reconnect/flow-control activity is visible.
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@localhost:5672/",
		xamqp.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("error closing connection: %v", err)
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
		xamqp.WithPublisherOptionsLogging,
		xamqp.WithPublisherOptionsExchangeName(exchangeName),
		xamqp.WithPublisherOptionsExchangeKind(amqp.ExchangeDirect),
		xamqp.WithPublisherOptionsExchangeDeclare,
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		log.Fatalf("failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Optional: know when a mandatory message couldn't be routed to any queue.
	publisher.NotifyReturn(func(r xamqp.Return) {
		log.Printf("[return] undeliverable message: exchange=%s routing_key=%s body=%s",
			r.Exchange, r.RoutingKey, string(r.Body))
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
		if p.Ack {
			log.Printf("[async confirm] ACK  delivery_tag=%d reconnects=%d", p.DeliveryTag, p.ReconnectionCount)
		} else {
			log.Printf("[async confirm] NACK delivery_tag=%d reconnects=%d", p.DeliveryTag, p.ReconnectionCount)
		}
	})

	log.Println("=== style 1: fire-and-forget PublishWithContext, confirms arrive later via NotifyPublish ===")
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
				log.Printf("publish #%d paused by server flow control, retry later: %v", i, err)
			case errors.Is(err, xamqp.ErrPublishBlocked):
				log.Printf("publish #%d blocked by TCP-level block, retry later: %v", i, err)
			default:
				log.Printf("publish #%d failed: %v", i, err)
			}
			continue
		}
		log.Printf("sent async message #%d (call returned immediately; no ack yet)", i)
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
	log.Println("=== style 2: PublishWithDeferredConfirmWithContext, wait for the ack explicitly ===")

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
			log.Fatalf("publish paused by server flow control: %v", err)
		case errors.Is(err, xamqp.ErrPublishBlocked):
			log.Fatalf("publish blocked by TCP-level block: %v", err)
		default:
			log.Fatalf("deferred-confirm publish failed: %v", err)
		}
	}

	for i, confirmation := range confirmations {
		if confirmation == nil {
			// Only happens if the publisher somehow isn't in confirm mode.
			log.Printf("routing key #%d: no confirmation object (publisher not in confirm mode)", i)
			continue
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ok, err := confirmation.WaitContext(waitCtx)
		cancel()
		if err != nil {
			log.Printf("routing key #%d: error waiting for confirmation: %v", i, err)
			continue
		}
		if ok {
			log.Printf("routing key #%d: broker ACKed the message", i)
		} else {
			log.Printf("routing key #%d: broker NACKed the message", i)
		}
	}

	log.Println("done")
}
