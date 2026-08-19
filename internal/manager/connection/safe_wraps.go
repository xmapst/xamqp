package connection

import (
	"slices"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// blockingReceiver 包装一个阻塞通知接收 channel，附带用于保证
// “广播发送”与“移除并关闭”互斥的私有锁。
//
// 必须以指针形式保存在 publisherNotifyBlockingReceivers 切片、
// readUniversalBlockReceiver 的克隆快照中，不能按值拷贝：mu 和 removed
// 需要在这几处共享同一份数据，否则 RemovePublisherBlockingReceiver 对
// removed 的写入不会被 readUniversalBlockReceiver 已经取走的快照看到，
// 就起不到互斥作用（这与 internal/dispatcher 的 dispatchSubscriber
// 必须用指针是同一个原因）。
type blockingReceiver struct {
	ch      chan amqp.Blocking // 实际的阻塞通知接收 channel，由某个 Publisher 创建并持有
	mu      *sync.Mutex        // 保护 removed 及 ch 关闭操作的私有锁
	removed bool               // 是否已被 RemovePublisherBlockingReceiver 标记移除，true 时广播跳过发送
}

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
	m.publisherNotifyBlockingReceivers = append(m.publisherNotifyBlockingReceivers, &blockingReceiver{
		ch: receiver,
		mu: &sync.Mutex{},
	})
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

// readUniversalBlockReceiver 持续读取 receiver（即调用方传入的当前
// universalNotifyBlockingReceiver）并广播给所有 Publisher。
//
// 作为后台 goroutine 运行，将底层连接的 TCP 阻塞信号（Blocking）
// 广播给所有已注册的 Publisher，使每个 Publisher 都能感知到阻塞状态并暂停发布。
// 以参数而非直接读取 m.universalNotifyBlockingReceiver 字段的方式接收
// 目标 channel：字段本身会在每次连接重连时被重新赋值（见 connection.go
// reconnect()），若在此处直接读字段，一旦背靠背发生两次重连，本协程
// 尚未被调度到第一行时字段就可能已指向更新的 channel，读到错误的
// （非本协程应该消费的）channel。调用方在持锁、字段仍是最新值的那一刻
// 把 channel 作为参数传入，可以完全避免这个无锁读取。
//
// 先在读锁保护下克隆接收者列表（拷贝的是 *blockingReceiver 指针，
// 与仍留在列表中的、以及 RemovePublisherBlockingReceiver 拿到的是
// 同一份数据）再释放锁，发送时不再持有 publisherNotifyBlockingReceiversMu：
// 避免某个 Publisher 的接收协程尚未就绪或处理缓慢时，阻塞发送会一并
// 卡住 NotifyBlockedSafe/RemovePublisherBlockingReceiver（它们需要写锁）。
// 发送前后持有该接收者私有的 mu 并检查 removed 标志，
// 与 RemovePublisherBlockingReceiver 的关闭操作互斥，
// 避免向已关闭的 channel 发送导致 panic。
//
// 发送本身是非阻塞的"保留最新值"语义（要求 br.ch 是容量为 1 的 buffered
// channel，由 publish_flow_block.go 创建）：能直接放入就放入；
// 放不进去说明接收者还没消费上一个值，先丢弃那个旧值再放入最新值。
// 这样广播协程永远不会被某个迟迟不消费的接收者卡住，同时保证接收者
// 最终读到的一定是最新状态，不会像"超时即丢弃"那样永久错过一次
// 状态翻转（例如错过一次"已解除阻塞"的通知，导致该 Publisher
// 误以为自己一直处于阻塞状态）。
func (m *Manager) readUniversalBlockReceiver(receiver chan amqp.Blocking) {
	for b := range receiver {
		m.publisherNotifyBlockingReceiversMu.RLock()
		receivers := slices.Clone(m.publisherNotifyBlockingReceivers)
		m.publisherNotifyBlockingReceiversMu.RUnlock()

		for _, br := range receivers {
			br.mu.Lock()
			if br.removed {
				br.mu.Unlock()
				continue
			}
			sendLatestNonBlocking(br.ch, b)
			br.mu.Unlock()
		}
	}
}

// sendLatestNonBlocking 尝试非阻塞地把 b 放入 ch；若 ch（容量为 1）已满，
// 先丢弃其中排队的旧值，再放入最新值，使接收者最终总能读到最新状态。
func sendLatestNonBlocking(ch chan amqp.Blocking, b amqp.Blocking) {
	select {
	case ch <- b:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- b:
	default:
		// 极少数情况下会与另一个并发的读取者错开导致依旧放不进去，
		// 直接放弃这一次投递：下一次广播会带着更新的状态重试。
	}
}

// RemovePublisherBlockingReceiver 从广播列表中移除指定接收者，并关闭其通道。
//
// 在 Publisher.Close() 时调用，清理不再需要的阻塞通知接收者，
// 防止已关闭的 Publisher 的通道在广播时阻塞。
// 关闭通道通知接收者的 range 循环退出，防止 goroutine 泄漏。
//
// 通过重建切片而非原地 append 删除，避免在同一底层数组上
// 边遍历边修改导致漏检或读到过期数据（Go 中原地删除同时 range 是已知陷阱）。
// 找到目标后在其私有锁保护下标记 removed 并关闭 channel，
// 与 readUniversalBlockReceiver 的发送互斥，避免向已关闭 channel 发送导致 panic。
func (m *Manager) RemovePublisherBlockingReceiver(receiver chan amqp.Blocking) {
	m.publisherNotifyBlockingReceiversMu.Lock()
	var target *blockingReceiver
	remaining := make([]*blockingReceiver, 0, len(m.publisherNotifyBlockingReceivers))
	for _, br := range m.publisherNotifyBlockingReceivers {
		if br.ch == receiver {
			target = br
			continue
		}
		remaining = append(remaining, br)
	}
	m.publisherNotifyBlockingReceivers = remaining
	m.publisherNotifyBlockingReceiversMu.Unlock()

	if target == nil {
		// 理论上不应发生（每个 receiver 只会注册一次，且只会被移除一次），
		// 兜底直接关闭，通知接收方 goroutine 退出。
		close(receiver)
		return
	}

	target.mu.Lock()
	target.removed = true
	close(receiver) // 关闭通道，通知接收方 goroutine 退出
	target.mu.Unlock()
}
