package channel

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// safe_wraps.go 提供 amqp.Channel 所有关键操作的线程安全封装。
//
// 所有 Safe 后缀方法在执行前持有 channelMu 读锁，
// 保证在重连期间（写锁）不会与通道替换操作并发执行，
// 防止操作到已关闭或正在关闭的旧通道。

// ConsumeSafe 线程安全版本的 amqp.Channel.Consume，开始消费指定队列。
func (m *Manager) ConsumeSafe(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.Consume(
		queue,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
}

// QueueDeclarePassiveSafe 线程安全版本的被动队列声明（仅检查不创建）。
func (m *Manager) QueueDeclarePassiveSafe(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.QueueDeclarePassive(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// QueueDeclareSafe 线程安全版本的队列声明（不存在则创建）。
func (m *Manager) QueueDeclareSafe(
	name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table,
) (amqp.Queue, error) {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// ExchangeDeclarePassiveSafe 线程安全版本的被动交换机声明（仅检查不创建）。
func (m *Manager) ExchangeDeclarePassiveSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.ExchangeDeclarePassive(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// ExchangeDeclareSafe 线程安全版本的交换机声明（不存在则创建）。
func (m *Manager) ExchangeDeclareSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// ExchangeBindSafe 线程安全版本的交换机到交换机绑定（E2E Binding）。
func (m *Manager) ExchangeBindSafe(
	name string, key string, exchange string, noWait bool, args amqp.Table,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.ExchangeBind(
		name,
		key,
		exchange,
		noWait,
		args,
	)
}

// QueueBindSafe 线程安全版本的队列到交换机绑定。
func (m *Manager) QueueBindSafe(
	name string, key string, exchange string, noWait bool, args amqp.Table,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.QueueBind(
		name,
		key,
		exchange,
		noWait,
		args,
	)
}

// QosSafe 线程安全版本的 QoS 设置，控制未确认消息的预取数量。
func (m *Manager) QosSafe(
	prefetchCount int, prefetchSize int, global bool,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.Qos(
		prefetchCount,
		prefetchSize,
		global,
	)
}

// PublishSafe 线程安全的消息发布（使用 background context）。
func (m *Manager) PublishSafe(
	exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) error {
	return m.PublishWithContextSafe(
		context.Background(),
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
}

// PublishWithContextSafe 线程安全的带上下文消息发布，支持取消和超时控制。
//
// 发布后等待 DeferredConfirmation 完成（若通道处于 confirm 模式），
// 确保消息确实被 RabbitMQ 服务器接收。若确认失败则返回错误，
// 调用方可据此决定是否重试。
func (m *Manager) PublishWithContextSafe(
	ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) error {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()
	confirm, err := m.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
	if err != nil {
		return err
	}
	if confirm != nil {
		// 等待服务器确认消息已接收（仅在 confirm 模式下 confirm 非 nil）
		var ok bool
		ok, err = confirm.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("message publishing not confirmed")
		}
	}
	return nil
}

// PublishWithDeferredConfirmWithContextSafe 线程安全的延迟确认消息发布，
// 返回可稍后等待确认的 DeferredConfirmation 对象。
func (m *Manager) PublishWithDeferredConfirmWithContextSafe(
	ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
}

// NotifyReturnSafe 线程安全地注册 basic.return 消息回调通道。
func (m *Manager) NotifyReturnSafe(
	c chan amqp.Return,
) chan amqp.Return {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.NotifyReturn(
		c,
	)
}

// ConfirmSafe 线程安全地将通道置于发布确认模式。
//
// 使用写锁（非读锁），因为此操作修改通道状态，不应与其他操作并发。
func (m *Manager) ConfirmSafe(
	noWait bool,
) error {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	return m.channel.Confirm(
		noWait,
	)
}

// NotifyPublishSafe 线程安全地注册发布确认事件通道。
func (m *Manager) NotifyPublishSafe(
	confirm chan amqp.Confirmation,
) chan amqp.Confirmation {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.NotifyPublish(
		confirm,
	)
}

// NotifyFlowSafe 线程安全地注册流控事件通道，接收服务器的 Flow 控制信号。
func (m *Manager) NotifyFlowSafe(
	c chan bool,
) chan bool {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.NotifyFlow(
		c,
	)
}
