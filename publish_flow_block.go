package xamqp

import amqp "github.com/rabbitmq/amqp091-go"

// startNotifyFlowHandler 监听 RabbitMQ 服务器的 Flow 控制信号，实现自动流量控制。
//
// 当服务器资源紧张（如内存告警）时，会发送 active=true 的 Flow 信号，
// Publisher 收到后暂停发布（disablePublishDueToFlow=true）；
// 资源恢复后发送 active=false 的信号，Publisher 恢复发布。
//
// 使用 disablePublishDueToFlowMu 写锁保护标志位修改，
// 允许发布操作以读锁并发检查，但状态更新时独占锁。
func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool))
	publisher.disablePublishDueToFlowMu.Lock()
	publisher.disablePublishDueToFlow = false // 初始化为允许发布
	publisher.disablePublishDueToFlowMu.Unlock()

	for ok := range notifyFlowChan {
		publisher.disablePublishDueToFlowMu.Lock()
		if ok {
			// ok=true 表示服务器请求暂停发布（Flow 激活）
			publisher.options.Logger.Warnf("pausing publishing due to flow request from server")
			publisher.disablePublishDueToFlow = true
		} else {
			// ok=false 表示服务器允许恢复发布（Flow 解除）
			publisher.disablePublishDueToFlow = false
			publisher.options.Logger.Warnf("resuming publishing due to flow request from server")
		}
		publisher.disablePublishDueToFlowMu.Unlock()
	}
}

// startNotifyBlockedHandler 注册并启动 TCP 级别连接阻塞信号的监听，实现背压控制。
//
// TCP 阻塞信号（Blocking）由 RabbitMQ 在连接层面发送，
// 比 Flow 信号更底层，通常在服务器资源极度紧张时触发。
//
// 注意：此方法只应在 Publisher 构造时调用一次，而不是像 Flow 监听那样
// 在每次 channel 级别重连后重新调用——阻塞信号是连接级别的，与 channel
// 的生命周期无关，重复调用只会不断在连接管理器的广播列表中堆积新的
// 接收 channel 和常驻 goroutine（旧的从不会被移除），造成泄漏。
// 注册（registerBlockedReceiver）是同步完成的，保证本方法返回时
// publisher.blocking 已经就绪，调用方即使立刻 Close() 也不会与异步
// 注册产生竞争而漏掉清理。
func (publisher *Publisher) startNotifyBlockedHandler() {
	blocking := publisher.registerBlockedReceiver()
	go publisher.readBlockedNotifications(blocking)
}

// registerBlockedReceiver 同步地向连接管理器注册阻塞通知接收 channel。
//
// blocking.Active=true：TCP 写缓冲区满，暂停所有发布操作；
// blocking.Active=false：TCP 缓冲区恢复，继续发布。
//
// 通过 NotifyBlockedSafe 将接收通道注册到连接管理器的广播列表，
// 使多个 Publisher 共用同一连接时都能收到阻塞通知。
func (publisher *Publisher) registerBlockedReceiver() chan amqp.Blocking {
	// 容量为 1：配合 connection.Manager 的"非阻塞发送、满了就丢旧值换新值"
	// 广播策略，保证本 Publisher 最终总能读到最新的阻塞状态。
	blocking := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking, 1))
	publisher.disablePublishDueToBlockedMu.Lock()
	publisher.blocking = blocking // 保存引用，用于 Close() 时从广播列表移除
	publisher.disablePublishDueToBlocked = false
	publisher.disablePublishDueToBlockedMu.Unlock()
	return blocking
}

// readBlockedNotifications 持续读取阻塞通知并更新发布暂停状态，直到 Close() 关闭该 channel。
func (publisher *Publisher) readBlockedNotifications(blocking chan amqp.Blocking) {
	for b := range blocking {
		publisher.disablePublishDueToBlockedMu.Lock()
		if b.Active {
			publisher.options.Logger.Warnf("pausing publishing due to TCP blocking from server")
			publisher.disablePublishDueToBlocked = true
		} else {
			publisher.disablePublishDueToBlocked = false
			publisher.options.Logger.Warnf("resuming publishing due to TCP blocking from server")
		}
		publisher.disablePublishDueToBlockedMu.Unlock()
	}
}
