package xamqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/manager/channel"
)

// Action 消息处理完成后的确认动作枚举。
type Action int

// Handler 消息处理函数类型，处理完成后返回确认动作。
type Handler func(d Delivery) (action Action)

const (
	// Ack 确认消息已成功处理，从队列中删除。
	Ack Action = iota
	// NackDiscard 拒绝消息并丢弃（或路由到死信队列），不重新入队。
	// 适合处理格式错误、无法重试的消息（毒丸消息）。
	NackDiscard
	// NackRequeue 拒绝消息并重新入队，由其他消费者重试处理。
	// 注意：若处理失败是系统性问题，会导致消息无限循环，应谨慎使用。
	NackRequeue
	// Manual 手动确认模式，由业务代码调用 msg.Ack() 自行管理确认时机。
	// 适合需要精确控制确认时机的场景（如异步处理完成后再确认）。
	Manual
)

// Consumer 消息消费者，连接到指定队列并消费消息。
//
// 内置自动重连机制：当连接断开时，自动尝试重建消费者，保证消息不丢失。
// handlerMu 用于等待当前正在处理的消息完成（优雅关闭），
// 防止关闭时丢失正在处理中的消息。
type Consumer struct {
	chanManager                *channel.Manager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}
	options                    ConsumerOptions
	handlerMu                  *sync.RWMutex // 读写锁用于等待处理中的消息完成

	isClosedMu *sync.RWMutex
	isClosed   bool
}

// Delivery 封装 amqp.Delivery，代表从队列收到的一条待处理消息。
type Delivery struct {
	amqp.Delivery
}

// NewConsumer 创建消费者并连接到指定队列，立即开始接收消息。
//
// 消费者实例不可复用，关闭后应创建新实例。
// 自动声明队列/交换机/绑定关系（通过 ConsumerOptions 配置），
// 无需调用方手动管理 AMQP 资源。
func NewConsumer(
	conn *Conn,
	queue string,
	optionFuncs ...func(*ConsumerOptions),
) (*Consumer, error) {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(&options)
	}

	if conn.connManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	consumer := &Consumer{
		options:    options,
		handlerMu:  &sync.RWMutex{},
		isClosedMu: &sync.RWMutex{},
		isClosed:   false,
	}
	var err error
	consumer.chanManager, err = channel.New(
		conn.connManager,
		options.Logger,
		conn.connManager.ReconnectInterval,
	)
	if err != nil {
		return nil, err
	}
	consumer.reconnectErrCh, consumer.closeConnectionToManagerCh = consumer.chanManager.NotifyReconnect()

	return consumer, nil
}

// Run 启动消息消费循环，并注册自动重连处理器。
//
// 通过 handlerMu 的读写锁实现优雅关闭：
//   - 消息处理期间持有读锁，允许多个 goroutine 并发处理
//   - CloseWithContext 等待读锁释放（获取写锁），确保所有消息处理完成后才关闭
//
// 重连策略：连接断开后每 3 秒重试一次，直到成功或消费者被关闭。
func (consumer *Consumer) Run(handler Handler) error {
	handlerWrapper := func(d Delivery) (action Action) {
		// TryRLock 失败表示正在关闭，拒绝处理新消息（重新入队）
		if !consumer.handlerMu.TryRLock() {
			return NackRequeue
		}
		defer consumer.handlerMu.RUnlock()
		return handler(d)
	}
	err := consumer.startConsumer(handlerWrapper)
	if err != nil {
		return err
	}

	go func() {
		for range consumer.reconnectErrCh {
			err = consumer.startConsumer(handlerWrapper)
			if err != nil {
				consumer.options.Logger.Warnf("error restarting consumer goroutines: %v", err)
			}
		}
	}()

	return nil
}

// startConsumer 初始化消费者，声明 AMQP 资源并启动消费 goroutine。
//
// 若消费者已关闭则立即返回错误。
// 若启动失败（如网络问题），等待 3 秒后重试，直到成功或消费者关闭。
func (consumer *Consumer) startConsumer(handlerWrapper Handler) error {
	for {
		if consumer.getIsClosed() {
			return fmt.Errorf("consumer closed")
		}
		if err := consumer.startGoroutines(
			handlerWrapper,
			consumer.options,
		); err != nil {
			consumer.options.Logger.Warnf("queue %s consumer restarting", consumer.options.QueueOptions.Name)
			consumer.options.Logger.Warnf("error restarting consumer goroutines after cancel or close: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		consumer.options.Logger.Infof("successful consumer recovery")
		return nil
	}
}

// Close 关闭消费者，默认等待当前处理中的消息完成后再关闭。
//
// 若需要设置等待超时，使用 CloseWithContext 方法。
// 仅调用一次，重复调用行为未定义。
func (consumer *Consumer) Close() {
	consumer.CloseWithContext(context.Background())
}

// cleanupResources 执行实际的关闭操作：标记关闭状态、关闭 AMQP 通道、通知连接管理器。
func (consumer *Consumer) cleanupResources() {
	consumer.isClosedMu.Lock()
	defer consumer.isClosedMu.Unlock()
	consumer.isClosed = true
	// 关闭 AMQP 通道通知 RabbitMQ 服务器停止投递消息
	err := consumer.chanManager.Close()
	if err != nil {
		consumer.options.Logger.Warnf("error while closing the channel: %v", err)
	}

	consumer.options.Logger.Infof("closing consumer...")
	go func() {
		consumer.closeConnectionToManagerCh <- struct{}{}
	}()
}

// CloseWithContext 关闭消费者，使用 context 控制等待处理中消息完成的超时时间。
//
// 若 WithConsumerOptionsForceShutdown 选项开启（CloseGracefully=false），
// 则不等待，立即关闭。
func (consumer *Consumer) CloseWithContext(ctx context.Context) {
	if consumer.options.CloseGracefully {
		consumer.options.Logger.Infof("waiting for handler to finish...")
		err := consumer.waitForHandlerCompletion(ctx)
		if err != nil {
			consumer.options.Logger.Warnf("error while waiting for handler to finish: %v", err)
		}
	}

	consumer.cleanupResources()
}

// startGoroutines 声明 AMQP 资源并为每个并发度启动一个消费 goroutine。
//
// 执行流程：
//  1. 设置 QoS（预取数量，控制未确认消息数量上限）
//  2. 声明交换机
//  3. 声明队列
//  4. 声明绑定关系
//  5. 开始消费，启动 Concurrency 个 goroutine 并发处理消息
func (consumer *Consumer) startGoroutines(
	handler Handler,
	options ConsumerOptions,
) error {
	consumer.isClosedMu.Lock()
	defer consumer.isClosedMu.Unlock()

	err := consumer.chanManager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return fmt.Errorf("declare qos failed: %w", err)
	}

	for _, exchangeOption := range options.ExchangeOptions {
		err = declareExchange(consumer.chanManager, exchangeOption)
		if err != nil {
			return fmt.Errorf("declare exchange failed: %w", err)
		}
	}
	err = declareQueue(consumer.chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareBindings(consumer.chanManager, options)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}

	msgs, err := consumer.chanManager.ConsumeSafe(
		options.QueueOptions.Name,
		options.RabbitConsumerOptions.Name,
		options.RabbitConsumerOptions.AutoAck,
		options.RabbitConsumerOptions.Exclusive,
		false, // no-local 标志 RabbitMQ 不支持，固定为 false
		options.RabbitConsumerOptions.NoWait,
		options.RabbitConsumerOptions.Args,
	)
	if err != nil {
		return err
	}

	for i := 0; i < options.Concurrency; i++ {
		go consumer.handlerGoroutine(msgs, options, handler)
	}
	consumer.options.Logger.Infof("processing messages on %v goroutines", options.Concurrency)
	return nil
}

// IsClosed 返回消费者是否已关闭。
func (consumer *Consumer) IsClosed() bool {
	return consumer.getIsClosed()
}

func (consumer *Consumer) getIsClosed() bool {
	consumer.isClosedMu.RLock()
	defer consumer.isClosedMu.RUnlock()
	return consumer.isClosed
}

// handlerGoroutine 消费者消息处理 goroutine，从消息通道读取并调用处理函数。
//
// 当消费者关闭时，msgs 通道被关闭，for range 循环退出。
func (consumer *Consumer) handlerGoroutine(msgs <-chan amqp.Delivery, consumeOptions ConsumerOptions, handler Handler) {
	defer consumer.options.Logger.Infof("rabbit consumer goroutine closed")

	for msg := range msgs {
		if consumer.getIsClosed() {
			break
		}

		consumer.handlerWrapper(consumeOptions, handler, msg)
	}
}

// handlerWrapper 执行消息处理并根据返回的 Action 进行相应的确认/拒绝操作。
//
// 通过 recover 捕获处理函数中的 panic，对非 AutoAck 的消息执行 Nack（丢弃），
// 防止毒丸消息导致消费者反复崩溃重启（拒绝不重入队）。
func (consumer *Consumer) handlerWrapper(consumeOptions ConsumerOptions, handler Handler, msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			consumer.options.Logger.Errorf("panic in consumer handler: %v", r)
			if !consumeOptions.RabbitConsumerOptions.AutoAck {
				_ = msg.Nack(false, false) // 丢弃而非重入队，防止毒丸消息反复触发 panic
			}
		}
	}()

	if consumeOptions.RabbitConsumerOptions.AutoAck {
		handler(Delivery{msg}) // AutoAck 模式下 RabbitMQ 自动确认，无需手动操作
		return
	}

	switch handler(Delivery{msg}) {
	case Ack:
		err := msg.Ack(false)
		if err != nil {
			consumer.options.Logger.Errorf("can't ack message: %v", err)
		}
	case NackDiscard:
		err := msg.Nack(false, false) // requeue=false：丢弃到死信队列
		if err != nil {
			consumer.options.Logger.Errorf("can't nack message: %v", err)
		}
	case NackRequeue:
		err := msg.Nack(false, true) // requeue=true：重新入队等待重试
		if err != nil {
			consumer.options.Logger.Errorf("can't nack message: %v", err)
		}
	default:
		// Manual 模式或其他情况：由业务代码自行调用 msg.Ack()/Nack()
	}
}

// waitForHandlerCompletion 等待所有正在处理中的消息处理完成。
//
// 通过尝试获取 handlerMu 的写锁来实现等待：
//   - 所有消息处理 goroutine 持有读锁期间，写锁无法获取
//   - 当所有读锁释放后，写锁获取成功，表示消息全部处理完毕
//
// ctx 超时时，此函数返回 ctx.Err()，上层决定是否强制关闭。
func (consumer *Consumer) waitForHandlerCompletion(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	} else if ctx.Err() != nil {
		return ctx.Err()
	}
	c := make(chan struct{})
	go func() {
		consumer.handlerMu.Lock()
		defer consumer.handlerMu.Unlock()
		close(c)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c:
		return nil
	}
}
