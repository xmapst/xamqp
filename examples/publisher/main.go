// Command publisher demonstrates a minimal single-message publish with xamqp:
// connect, declare/bind an exchange via the publisher options, publish one
// message, and correctly react to xamqp's publish-side sentinel errors.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/xmapst/xamqp"
)

func main() {
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@localhost:5672/",
	)
	if err != nil {
		slog.Error("failed to connect", slog.Any("error", err))
		os.Exit(1)
	}
	defer conn.Close()

	publisher, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind("topic"),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		slog.Error("failed to create publisher", slog.Any("error", err))
		os.Exit(1)
	}
	defer publisher.Close()

	// NotifyReturn fires for messages the broker could not route (relevant
	// here because we publish with the mandatory flag below).
	publisher.NotifyReturn(func(r xamqp.Return) {
		slog.Warn("message returned by broker",
			slog.Any("reply_code", r.ReplyCode), slog.String("reply_text", r.ReplyText), slog.String("body", string(r.Body)))
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
			slog.Error("publish blocked by broker (TCP block), back off and retry later", slog.Any("error", err))
			os.Exit(1)
		case errors.Is(err, xamqp.ErrPublishFlowPaused):
			// The broker asked us to pause publishing (flow control) due
			// to high load. This is transient - wait and retry.
			slog.Error("publish paused by broker flow control, retry later", slog.Any("error", err))
			os.Exit(1)
		default:
			slog.Error("publish failed", slog.Any("error", err))
			os.Exit(1)
		}
	}

	slog.Info("message published successfully")
}
