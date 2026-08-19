// Command publisher demonstrates a minimal single-message publish with xamqp:
// connect, declare/bind an exchange via the publisher options, publish one
// message, and correctly react to xamqp's publish-side sentinel errors.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/xmapst/xamqp"
)

func main() {
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@localhost:5672/",
		xamqp.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	publisher, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsLogging,
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind("topic"),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		log.Fatalf("failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// NotifyReturn fires for messages the broker could not route (relevant
	// here because we publish with the mandatory flag below).
	publisher.NotifyReturn(func(r xamqp.Return) {
		log.Printf("message returned by broker: reply_code=%d reply_text=%s body=%s", r.ReplyCode, r.ReplyText, string(r.Body))
	})

	body := fmt.Sprintf(`{"event":"user.signup","email":"user@example.com","ts":%d}`, time.Now().Unix())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = publisher.PublishWithContext(
		ctx,
		[]byte(body),
		[]string{"user.signup"},
		xamqp.WithPublishOptionsContentType("application/json"),
		xamqp.WithPublishOptionsMandatory,
		xamqp.WithPublishOptionsPersistentDelivery,
		xamqp.WithPublishOptionsExchange("events"),
	)
	if err != nil {
		switch {
		case errors.Is(err, xamqp.ErrPublishBlocked):
			// The connection is TCP-blocked by the broker (e.g. resource
			// alarm such as low disk/memory). Retrying immediately will
			// just fail again - back off and retry, or surface the
			// condition to an operator.
			log.Fatalf("publish blocked by broker (TCP block), back off and retry later: %v", err)
		case errors.Is(err, xamqp.ErrPublishFlowPaused):
			// The broker asked us to pause publishing (flow control) due
			// to high load. This is transient - wait and retry.
			log.Fatalf("publish paused by broker flow control, retry later: %v", err)
		default:
			log.Fatalf("publish failed: %v", err)
		}
	}

	log.Println("message published successfully")
}
