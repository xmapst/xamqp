package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp"
)

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

	consumer, err := xamqp.NewConsumer(
		conn,
		"orders.created",
		xamqp.WithConsumerOptionsQueueDurable,
		xamqp.WithConsumerOptionsExchangeName("orders.exchange"),
		xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeDirect),
		xamqp.WithConsumerOptionsExchangeDurable,
		xamqp.WithConsumerOptionsExchangeDeclare,
		xamqp.WithConsumerOptionsRoutingKey("orders.created"),
		xamqp.WithConsumerOptionsConcurrency(4),
		xamqp.WithConsumerOptionsQOSPrefetch(20),
	)
	if err != nil {
		slog.Error("create consumer failed", slog.Any("error", err))
		os.Exit(1)
	}

	// Run does NOT block: it returns as soon as the consumer has started
	// successfully. Automatic reconnect/recovery after that point happens
	// in a background goroutine, so the caller must keep the process alive
	// on its own (here, by waiting for a shutdown signal below).
	err = consumer.Run(func(d xamqp.Delivery) xamqp.Action {
		slog.Info("received", slog.String("body", string(d.Body)))
		return xamqp.Ack
	})
	if err != nil {
		slog.Error("consumer.Run failed", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("consumer started, waiting for messages...")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	slog.Info("received signal, shutting down", slog.Any("signal", sig))

	consumer.Close()
	if err := conn.Close(); err != nil {
		slog.Error("error closing connection", slog.Any("error", err))
	}
}
