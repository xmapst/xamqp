package channel

import (
	"context"

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
// 始终是 fire-and-forget：即使通道处于 confirm 模式，也不在这里等待
// DeferredConfirmation——确认结果只通过 NotifyPublish 注册的回调异步交付。
// 若在这里顺带等待确认，confirm 模式一开，Publish/PublishWithContext 就会
// 从"发出去就返回"退化成每条消息都同步等 broker ack 的往返调用，
// 且期间还攥着 channelMu 读锁，会拖慢并发重连。需要等待某条消息确认时，
// 应改用 PublishWithDeferredConfirmWithContextSafe。
//
// 使用 lockChannelRead 而非裸 RLock：重连期间写锁被占用时，
// 调用方传入的 ctx 超时/取消能够中断等待，而不是被无条件阻塞。
func (m *Manager) PublishWithContextSafe(
	ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) error {
	if err := m.lockChannelRead(ctx); err != nil {
		return err
	}
	defer m.channelMu.RUnlock()

	return m.channel.PublishWithContext(
		ctx,
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
}

// PublishWithDeferredConfirmWithContextSafe 线程安全的延迟确认消息发布，
// 返回可稍后等待确认的 DeferredConfirmation 对象。
//
// 使用 lockChannelRead 而非裸 RLock，理由同 PublishWithContextSafe。
func (m *Manager) PublishWithDeferredConfirmWithContextSafe(
	ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	if err := m.lockChannelRead(ctx); err != nil {
		return nil, err
	}
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
// 幂等：已经处于 confirm 模式时直接返回 nil，不重复调用底层 Confirm。
// confirmMode 状态同时被 reconnect() 用来判断是否需要在新通道上重新启用。
func (m *Manager) ConfirmSafe(
	noWait bool,
) error {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	if m.confirmMode {
		return nil
	}
	if err := m.channel.Confirm(noWait); err != nil {
		return err
	}
	m.confirmMode = true
	return nil
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

// WithChannelForNewGeneration 在同一个读锁临界区内原子地完成三件事：
// 读取当前通道的"代号"（重连计数）、把代号交给调用方判断是否需要为这一代
// 注册监听、以及（需要时）在当前通道上完成注册。
//
// 存在的理由：调用方（Publisher）需要保证"每一代通道最多注册一个监听者"。
// 如果分成"先取代号 → 解锁 → 再注册"两步，中间可能插入一次完整的重连，
// 注册就会落到新通道上却记着旧代号，随后按新代号判断的调用方会再注册一次，
// 同一个通道上出现两个监听者——而 amqp091-go 的 NotifyReturn/NotifyPublish
// 允许多个监听者并会把每个事件广播给全部监听者、不做去重，结果就是每个
// 事件被处理两次。读锁会挡住 reconnect() 的写锁，因此在 register 回调执行
// 期间通道不可能被替换，代号与实际注册到的通道严格对应。
//
// register 为 nil（调用方判断这一代已经有监听者）时不做任何注册。
// 注意：register 在持有 channelMu 读锁时被调用，实现里不得再去获取写锁
// （例如不要调用 ConfirmSafe），否则会自锁。
func (m *Manager) WithChannelForNewGeneration(
	register func(ch *amqp.Channel, generation uint),
) {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	register(m.channel, m.ReconnectionCount())
}

// NotifyFlowSafe 线程安全地注册流控事件通道，接收服务器的 Flow 控制信号。
func (m *Manager) NotifyFlowSafe(
	c chan bool,
) chan bool {
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()

	return m.channel.NotifyFlow(c)
}
