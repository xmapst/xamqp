package connection

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// NotifyBlockedSafe 线程安全地注册 TCP 阻塞通知接收者。
//
// 单一接收者广播设计：AMQP 连接的 NotifyBlocked 只能注册一次，
// 多次注册会导致信号只被最后一个接收者收到。
// 此方法将调用者注册到 publisherNotifyBlockingReceivers 列表中，
// 由 readUniversalBlockReceiver 统一广播信号，
// 解决多个 Publisher 共用同一连接时的信号分发问题。
func (m *Manager) NotifyBlockedSafe(
	receiver chan amqp.Blocking,
) chan amqp.Blocking {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	// 将接收者加入广播列表
	m.publisherNotifyBlockingReceiversMu.Lock()
	m.publisherNotifyBlockingReceivers = append(m.publisherNotifyBlockingReceivers, receiver)
	m.publisherNotifyBlockingReceiversMu.Unlock()

	// 仅在第一个 Publisher 注册时，将通用通道注册给底层连接
	if !m.universalNotifyBlockingReceiverUsed {
		m.connection.NotifyBlocked(
			m.universalNotifyBlockingReceiver,
		)
		m.universalNotifyBlockingReceiverUsed = true
	}

	return receiver
}

// readUniversalBlockReceiver 持续读取 universalNotifyBlockingReceiver 并广播给所有 Publisher。
//
// 作为后台 goroutine 运行，将底层连接的 TCP 阻塞信号（Blocking）
// 广播给所有已注册的 Publisher，使每个 Publisher 都能感知到阻塞状态并暂停发布。
// 使用读锁遍历接收者列表，允许并发查询接收者数量。
func (m *Manager) readUniversalBlockReceiver() {
	for b := range m.universalNotifyBlockingReceiver {
		m.publisherNotifyBlockingReceiversMu.RLock()
		for _, br := range m.publisherNotifyBlockingReceivers {
			br <- b // 将阻塞信号转发给每个 Publisher
		}
		m.publisherNotifyBlockingReceiversMu.RUnlock()
	}
}

// RemovePublisherBlockingReceiver 从广播列表中移除指定接收者，并关闭其通道。
//
// 在 Publisher.Close() 时调用，清理不再需要的阻塞通知接收者，
// 防止已关闭的 Publisher 的通道在广播时阻塞（向已无人读取的通道发送会阻塞 for range 循环）。
// 关闭通道通知接收者的 range 循环退出，防止 goroutine 泄漏。
func (m *Manager) RemovePublisherBlockingReceiver(receiver chan amqp.Blocking) {
	m.publisherNotifyBlockingReceiversMu.Lock()
	for i, br := range m.publisherNotifyBlockingReceivers {
		if br == receiver {
			// 原地删除：将后续元素前移，保持切片顺序
			m.publisherNotifyBlockingReceivers = append(m.publisherNotifyBlockingReceivers[:i], m.publisherNotifyBlockingReceivers[i+1:]...)
		}
	}
	m.publisherNotifyBlockingReceiversMu.Unlock()
	close(receiver) // 关闭通道，通知接收方 goroutine 退出
}
