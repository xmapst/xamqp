package main

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"
)

// This example starts two independent consumers on a single shared
// connection: one for "orders.created" and one for "orders.cancelled".
// Both are bound to the same topic exchange with different routing keys.
func main() {
	conn, err := xamqp.NewConn(
		"amqp://guest:guest@127.0.0.1:5672/",
		xamqp.WithConnectionOptionsReconnectInterval(3*time.Second),
	)
	if err != nil {
		slog.Error("connect failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer conn.Close()

	createdConsumer, err := xamqp.NewConsumer(
		conn,
		"orders.created",
		xamqp.WithConsumerOptionsConsumerName("orders-created-consumer"),
		xamqp.WithConsumerOptionsQueueDurable,
		xamqp.WithConsumerOptionsExchangeName("orders.exchange"),
		xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithConsumerOptionsExchangeDurable,
		xamqp.WithConsumerOptionsExchangeDeclare,
		xamqp.WithConsumerOptionsRoutingKey("orders.created"),
		xamqp.WithConsumerOptionsConcurrency(2),
		xamqp.WithConsumerOptionsQOSPrefetch(10),
	)
	if err != nil {
		slog.Error("create orders.created consumer failed", slog.Any("error", err))
		os.Exit(1)
	}

	cancelledConsumer, err := xamqp.NewConsumer(
		conn,
		"orders.cancelled",
		xamqp.WithConsumerOptionsConsumerName("orders-cancelled-consumer"),
		xamqp.WithConsumerOptionsQueueDurable,
		xamqp.WithConsumerOptionsExchangeName("orders.exchange"),
		xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeTopic),
		xamqp.WithConsumerOptionsExchangeDurable,
		xamqp.WithConsumerOptionsExchangeDeclare,
		xamqp.WithConsumerOptionsRoutingKey("orders.cancelled"),
		xamqp.WithConsumerOptionsConcurrency(2),
		xamqp.WithConsumerOptionsQOSPrefetch(10),
	)
	if err != nil {
		slog.Error("create orders.cancelled consumer failed", slog.Any("error", err))
		os.Exit(1)
	}

	// Run does NOT block: each call returns as soon as its consumer has
	// started successfully. Automatic reconnect/recovery after that point
	// happens in a background goroutine per consumer, so the caller must
	// keep the process alive on its own (here, by waiting for a shutdown
	// signal below).
	err = createdConsumer.Run(func(d xamqp.Delivery) xamqp.Action {
		slog.Info("received", slog.String("queue", "orders.created"), slog.String("body", string(d.Body)))
		return xamqp.Ack
	})
	if err != nil {
		slog.Error("orders.created consumer.Run failed", slog.Any("error", err))
		os.Exit(1)
	}

	err = cancelledConsumer.Run(func(d xamqp.Delivery) xamqp.Action {
		slog.Info("received", slog.String("queue", "orders.cancelled"), slog.String("body", string(d.Body)))
		return xamqp.Ack
	})
	if err != nil {
		slog.Error("orders.cancelled consumer.Run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("both consumers started, waiting for messages...")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	slog.Info("received signal, shutting down", slog.Any("signal", sig))

	// Close both consumers concurrently, then wait for both to finish
	// before closing the shared connection.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		createdConsumer.Close()
	}()
	go func() {
		defer wg.Done()
		cancelledConsumer.Close()
	}()
	wg.Wait()

	if err := conn.Close(); err != nil {
		slog.Error("error closing connection", slog.Any("error", err))
	}
}
