// Command multipublisher demonstrates running two independent publishers on
// a single shared connection, each on its own ticker-driven publish loop,
// waiting on publisher-confirms with a per-publish timeout, and shutting
// down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		log.Fatalf("failed to create publisher: %v", err)
	}
	defer publisher.Close()

	publisher.NotifyReturn(func(r xamqp.Return) {
		log.Printf("[publisher] message returned by broker: reply_code=%d reply_text=%s body=%s", r.ReplyCode, r.ReplyText, string(r.Body))
	})

	publisher2, err := xamqp.NewPublisher(
		conn,
		xamqp.WithPublisherOptionsLogging,
		xamqp.WithPublisherOptionsExchangeName("events"),
		xamqp.WithPublisherOptionsExchangeKind("topic"),
		xamqp.WithPublisherOptionsExchangeDurable,
		xamqp.WithPublisherOptionsExchangeDeclare,
		xamqp.WithPublisherOptionsConfirm,
	)
	if err != nil {
		log.Fatalf("failed to create publisher2: %v", err)
	}
	defer publisher2.Close()

	publisher2.NotifyReturn(func(r xamqp.Return) {
		log.Printf("[publisher2] message returned by broker: reply_code=%d reply_text=%s body=%s", r.ReplyCode, r.ReplyText, string(r.Body))
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

	log.Println("awaiting signal")
	sig := <-sigs
	log.Printf("received signal %v, shutting down", sig)

	close(stop)
	wg.Wait()

	log.Println("stopping publishers")
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
			log.Printf("[%s] publish blocked by broker (TCP block): %v", routingKey, err)
		case errors.Is(err, xamqp.ErrPublishFlowPaused):
			// The broker asked us to pause publishing (flow control) due to
			// high load. Also transient - retry on the next tick.
			log.Printf("[%s] publish paused by broker flow control: %v", routingKey, err)
		default:
			log.Printf("[%s] publish failed: %v", routingKey, err)
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
			log.Printf("[%s] timed out waiting for confirm: %v", routingKey, waitErr)
			continue
		}
		if !ack {
			log.Printf("[%s] broker nacked message: %s", routingKey, msg)
			continue
		}
		log.Printf("[%s] message confirmed: %s", routingKey, msg)
	}
}
