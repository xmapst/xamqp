// Command multipublisher demonstrates running two independent publishers on
// a single shared connection, each on its own ticker-driven publish loop,
// waiting on publisher-confirms with a per-publish timeout, and shutting
// down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		slog.Error("failed to create publisher", slog.Any("error", err))
		os.Exit(1)
	}
	defer publisher.Close()

	publisher.NotifyReturn(func(r xamqp.Return) {
		slog.Warn("message returned by broker",
			slog.String("publisher", "publisher"), slog.Any("reply_code", r.ReplyCode),
			slog.String("reply_text", r.ReplyText), slog.String("body", string(r.Body)))
	})

	publisher2, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind("topic"),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		slog.Error("failed to create publisher2", slog.Any("error", err))
		os.Exit(1)
	}
	defer publisher2.Close()

	publisher2.NotifyReturn(func(r xamqp.Return) {
		slog.Warn("message returned by broker",
			slog.String("publisher", "publisher2"), slog.Any("reply_code", r.ReplyCode),
			slog.String("reply_text", r.ReplyText), slog.String("body", string(r.Body)))
	})

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go runPublishLoop(&wg, stop, publisher, "my_routing_key", "hello, world")

	wg.Add(1)
	go runPublishLoop(&wg, stop, publisher2, "my_routing_key_2", "hello, world 2")

	slog.Info("awaiting signal")
	sig := <-sigs
	slog.Info("received signal, shutting down", slog.Any("signal", sig))

	close(stop)
	wg.Wait()

	slog.Info("stopping publishers")
}

// runPublishLoop publishes a message on the given routing key every couple
// of seconds until stop is closed, waiting for the broker's confirm on each
// publish with a short per-publish timeout.
func runPublishLoop(wg *sync.WaitGroup, stop <-chan struct{}, publisher *xamqp.Publisher, routingKey, body string) {
	defer wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			publishOnce(publisher, routingKey, body)
		case <-stop:
			return
		}
	}
}

func publishOnce(publisher *xamqp.Publisher, routingKey, body string) {
	msg := fmt.Sprintf("%s @ %d", body, time.Now().Unix())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
		ctx,
		[]byte(msg),
		[]string{routingKey},
		xamqp.WithPublishOptionsContentType("application/json"),
		xamqp.WithPublishOptionsMandatory,
		xamqp.WithPublishOptionsPersistentDelivery,
		xamqp.WithPublishOptionsExchange("events"),
	)
	if err != nil {
		switch {
		case errors.Is(err, xamqp.ErrPublishBlocked):
			// The connection is TCP-blocked by the broker (e.g. resource
			// alarm such as low disk/memory). This is transient - back off
			// and retry on the next tick rather than treating it as fatal.
			slog.Warn("publish blocked by broker (TCP block)", slog.String("routing_key", routingKey), slog.Any("error", err))
		case errors.Is(err, xamqp.ErrPublishFlowPaused):
			// The broker asked us to pause publishing (flow control) due to
			// high load. Also transient - retry on the next tick.
			slog.Warn("publish paused by broker flow control", slog.String("routing_key", routingKey), slog.Any("error", err))
		default:
			slog.Warn("publish failed", slog.String("routing_key", routingKey), slog.Any("error", err))
		}
		return
	}

	// One routing key was published, so we expect exactly one (possibly
	// nil, if the publisher isn't in confirm mode) deferred confirmation.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()

	for _, confirmation := range confirmations {
		if confirmation == nil {
			continue
		}
		ack, waitErr := confirmation.WaitContext(waitCtx)
		if waitErr != nil {
			slog.Warn("timed out waiting for confirm", slog.String("routing_key", routingKey), slog.Any("error", waitErr))
			continue
		}
		if !ack {
			slog.Warn("broker nacked message", slog.String("routing_key", routingKey), slog.String("body", msg))
			continue
		}
		slog.Info("message confirmed", slog.String("routing_key", routingKey), slog.String("body", msg))
	}
}
