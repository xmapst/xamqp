package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/backoff"
	"github.com/xmapst/xamqp/internal/dispatcher"
	"github.com/xmapst/xamqp/internal/logger"
	"github.com/xmapst/xamqp/internal/manager/connection"
)

// Manager 管理单个 AMQP 通道的生命周期，实现通道断线自动重建。
//
// AMQP 通道（Channel）是在连接之上的逻辑多路复用单元，
// 每个 Publisher/Consumer 持有独立的通道，通道异常不影响其他通道。
//
// 并发安全：
//   - channelMu RWMutex 保护 channel/closed/confirmMode 字段的并发访问
//   - 读操作（Consume/Publish/Declare 等）持有读锁，允许并发
//   - 写操作（reconnect/Close）持有写锁，独占访问
//   - reconnectionCountMu 单独保护重连计数，避免与 channelMu 产生锁竞争
type Manager struct {
	logger              logger.ILogger         // 日志实现
	channel             *amqp.Channel          // 底层 AMQP 通道，受 channelMu 保护
	connManager         *connection.Manager    // 所属的连接管理器，重连时用于借用连接开新通道
	channelMu           *sync.RWMutex          // 读写锁：允许多读单写
	closed              bool                   // 是否已调用 Close，受 channelMu 保护，防止与在途重连竞争
	closeSignal         chan struct{}          // Close() 时关闭，用于立刻打断正在等待退避的 reconnectLoop
	confirmMode         bool                   // 是否已进入 confirm 模式，受 channelMu 保护；重连时据此在新通道上重新启用
	reconnectInterval   time.Duration          // 首次重连等待时间（指数退避的基础值）
	reconnectionCount   uint                   // 累计重连次数，用于确认消息的 ID 计算
	reconnectionCountMu *sync.Mutex            // 单独保护重连计数，避免与 channelMu 产生锁竞争
	dispatcher          *dispatcher.Dispatcher // 重连事件广播器
}

// errClosed 表示通道管理器已被 Close，重连循环应立即停止，不再创建新通道。
var errClosed = errors.New("channel manager is closed")

// New 创建通道管理器，初始化时立即建立 AMQP 通道并启动异常监听。
func New(connManager *connection.Manager, log logger.ILogger, reconnectInterval time.Duration) (*Manager, error) {
	chanManager := &Manager{
		logger:              log,
		connManager:         connManager,
		channelMu:           &sync.RWMutex{},
		closeSignal:         make(chan struct{}),
		reconnectInterval:   reconnectInterval,
		reconnectionCount:   0,
		reconnectionCountMu: &sync.Mutex{},
		dispatcher:          dispatcher.New(),
	}

	ch, err := chanManager.getNewChannel()
	if err != nil {
		return nil, err
	}

	chanManager.channel = ch
	go chanManager.startNotifyCancelOrClosed() // 异步监听通道关闭/取消事件
	return chanManager, nil
}

// getNewChannel 从连接管理器借用连接并开启新通道。
func (m *Manager) getNewChannel() (*amqp.Channel, error) {
	var ch *amqp.Channel
	err := m.connManager.WithConnection(func(conn *amqp.Connection) error {
		var err error
		ch, err = conn.Channel()
		return err
	})
	if err != nil {
		return nil, err
	}

	return ch, nil
}

// startNotifyCancelOrClosed 监听通道的关闭（NotifyClose）和取消（NotifyCancel）事件。
//
// 通道关闭原因：
//   - 服务器主动关闭（如队列被删除、Exchange 不存在）→ NotifyClose 收到非 nil 错误
//   - 客户端正常关闭（调用 Close()）→ NotifyClose 收到 nil
//   - 消费者被取消（队列删除导致）→ NotifyCancel 收到队列名字符串
//
// 异常关闭时自动触发指数退避重连，重连成功后广播事件通知订阅者（Publisher/Consumer）重新初始化。
func (m *Manager) startNotifyCancelOrClosed() {
	notifyCloseChan := m.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := m.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-notifyCloseChan:
		if err != nil {
			m.logger.Errorf("attempting to reconnect to amqp server after close with error: %v", err)
			m.reconnectLoop()
			m.logger.Warnf("successfully reconnected to amqp server")
			_ = m.dispatcher.Dispatch(err)
		}

		if err == nil {
			m.logger.Infof("amqp channel closed gracefully")
		}
	case err := <-notifyCancelChan:
		m.logger.Errorf("attempting to reconnect to amqp server after cancel with error: %s", err)
		m.reconnectLoop()
		m.logger.Warnf("successfully reconnected to amqp server after cancel")
		if _err := m.dispatcher.Dispatch(errors.New(err)); _err != nil {
			m.logger.Warnf("channel dispatch err: %v", err)
		}
	}
}

// ReconnectionCount 获取累计重连次数，用于 Publisher 中唯一标识发布确认。
func (m *Manager) ReconnectionCount() uint {
	m.reconnectionCountMu.Lock()
	defer m.reconnectionCountMu.Unlock()

	return m.reconnectionCount
}

func (m *Manager) incrementReconnectionCount() {
	m.reconnectionCountMu.Lock()
	defer m.reconnectionCountMu.Unlock()

	m.reconnectionCount++
}

// reconnectLoop 持续尝试重建通道，使用带抖动的指数退避，直到成功或管理器被关闭。
//
// 等待退避时间时 select 着 closeSignal：Close() 会立刻打断等待并退出，
// 不必等满一整个退避周期才发现管理器已经关闭。
func (m *Manager) reconnectLoop() {
	attempt := 0

	for {
		delay := backoff.Delay(m.reconnectInterval, attempt)
		m.logger.Infof("waiting %s to attempt to reconnect to amqp server", delay)
		select {
		case <-m.closeSignal:
			m.logger.Infof("channel manager closed, stopping reconnect loop")
			return
		case <-time.After(delay):
		}

		err := m.reconnect()
		if err != nil {
			if errors.Is(err, errClosed) {
				m.logger.Infof("channel manager closed, stopping reconnect loop")
				return
			}
			m.logger.Errorf("error reconnecting to amqp server: %v", err)
			attempt++
		} else {
			m.incrementReconnectionCount()
			go m.startNotifyCancelOrClosed() // 重连成功后继续监听新通道的异常
			return
		}
	}
}

// reconnect 建立新通道后，在写锁保护下做指针替换；开通道 RPC 本身在锁外完成，
// 避免长时间持锁做网络 I/O。
//
// 若此时管理器已被 Close，则丢弃刚建立的新通道并返回 errClosed，
// 防止 Close 与重连竞争产生一个再也没有人引用、也没有人能关闭的孤立通道。
//
// 例外：若此前已通过 ConfirmSafe 进入过 confirm 模式，会在安装新通道的
// 同一把写锁临界区内于新通道上重新启用 confirm 模式（newChannel.Confirm），
// 而不是先解锁再做——这是有意的取舍：只有这样才能保证"新通道对外可见"
// 和"confirm 模式已重新启用"这两件事原子发生，不会出现"重连已完成、
// 但还没来得及重新进入 confirm 模式"的窗口（该窗口期间发布的消息会拿到
// nil 的 DeferredConfirmation，静默地失去确认保证）。代价是 Confirm 这次
// AMQP RPC 本身没有超时/ctx（amqp091-go 未提供），最坏情况下会在写锁内
// 阻塞到连接心跳超时为止；期间所有走 lockChannelRead 的 Publish 系列调用
// 能通过 ctx 提前退出，但其余仍用裸 RLock 的 *Safe 方法（Consume/Declare/
// Qos/Notify* 等）会跟着无条件卡住。只在开启了确认模式的 Publisher 触发
// 重连时才会命中，且有连接心跳兜底，不会永久卡死。
func (m *Manager) reconnect() error {
	newChannel, err := m.getNewChannel()
	if err != nil {
		return err
	}

	m.channelMu.Lock()
	if m.closed {
		m.channelMu.Unlock()
		if cerr := newChannel.Close(); cerr != nil {
			m.logger.Warnf("error closing redundant channel after manager was closed: %v", cerr)
		}
		return errClosed
	}

	if m.confirmMode {
		if cerr := newChannel.Confirm(false); cerr != nil {
			m.channelMu.Unlock()
			if cerr2 := newChannel.Close(); cerr2 != nil {
				m.logger.Warnf("error closing channel after failed confirm re-arm: %v", cerr2)
			}
			return fmt.Errorf("failed to re-arm confirm mode after reconnect: %w", cerr)
		}
	}

	oldChannel := m.channel
	m.channel = newChannel
	m.channelMu.Unlock()

	// 先建立新通道，再关闭旧通道，防止期间有操作无通道可用
	if oldChannel != nil {
		if err = oldChannel.Close(); err != nil {
			m.logger.Warnf("error closing channel while reconnecting: %v", err)
		}
	}
	return nil
}

// Close 安全关闭 AMQP 通道，释放服务器端资源。
//
// 幂等：重复调用直接返回 nil。同时标记 closed 并关闭 closeSignal，
// 使任何在途的 reconnect() 在完成开通道 RPC 后能感知到关闭并放弃安装新通道，
// 也使正在等待退避的 reconnectLoop 立刻退出。
func (m *Manager) Close() error {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true
	close(m.closeSignal)

	m.logger.Infof("closing channel manager...")
	err := m.channel.Close()
	if err != nil {
		m.logger.Errorf("close err: %v", err)
		return err
	}

	return nil
}

// lockChannelRead 在 ctx 未取消的前提下获取通道读锁。
//
// 先尝试非阻塞的 TryRLock；若暂时拿不到（例如正在重连、写锁被占用），
// 转为按 1 毫秒间隔轮询 TryRLock，同时 select 着 ctx.Done()。
// 这样调用方传入的超时/取消能够中断等待，而不是像裸 RLock() 那样
// 无条件阻塞到写锁释放为止——否则一次正在进行的重连会让所有携带
// 较短超时的 Publish 调用统统卡穿其 ctx 的deadline。
func (m *Manager) lockChannelRead(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.channelMu.TryRLock() {
		return nil
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if m.channelMu.TryRLock() {
				return nil
			}
		}
	}
}

// NotifyReconnect 订阅通道重连成功事件，返回事件通道和关闭信号通道。
func (m *Manager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return m.dispatcher.AddSubscriber()
}
