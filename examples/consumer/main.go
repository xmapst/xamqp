package main

import (
	"log"
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
		xamqp.WithConnectionOptionsLogging,
		xamqp.WithConnectionOptionsReconnectInterval(3*time.Second),
	)
	if err != nil {
		log.Fatal(err)
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
		xamqp.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Run does NOT block: it returns as soon as the consumer has started
	// successfully. Automatic reconnect/recovery after that point happens
	// in a background goroutine, so the caller must keep the process alive
	// on its own (here, by waiting for a shutdown signal below).
	err = consumer.Run(func(d xamqp.Delivery) xamqp.Action {
		log.Printf("received: %s", string(d.Body))
		return xamqp.Ack
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("consumer started, waiting for messages...")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Printf("received signal %v, shutting down", sig)

	consumer.Close()
	if err := conn.Close(); err != nil {
		log.Printf("error closing connection: %v", err)
	}
}
