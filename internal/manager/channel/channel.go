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

	// notifyConnReconnect 订阅底层连接的重连成功事件，用于让 reconnectLoop
	// 在连接刚恢复时跳过剩余的退避等待、立即重试，而不是坐等自己独立的
	// 退避计时器。New() 中默认指向 connManager.NotifyReconnect；单独抽成
	// 字段是为了在没有真实 connManager 的单元测试里可以替换为假实现，
	// 不必依赖需要真实拨号才能构造出来的 connection.Manager。
	notifyConnReconnect func() (<-chan error, chan<- struct{})
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
	chanManager.notifyConnReconnect = connManager.NotifyReconnect

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

// startNotifyCancelOrClosed 并行监听通道的取消（NotifyCancel）和生命周期
// 变化（NotifyStateChange）事件。
//
// basic.cancel（如队列被删除）不受 amqp091-go 原生恢复处理——它只在
// 连接/通道层面重连时生效，对一个仍然存活的通道上单纯的消费者取消不起
// 作用，所以 NotifyCancel 分支始终由 xamqp 自己处理：一旦收到取消，
// 立即触发自己的 reconnectLoop 重建整个通道（重新声明拓扑、重新订阅）。
//
// 通道对象本身的存亡则交给 NotifyStateChange：原生恢复期间 *amqp.Channel
// 原地复用（同一指针），中间态（StateReconnecting/StateOpen）不需要
// xamqp 做任何事；只有终态 StateClosed 且 Err != nil（原生恢复重试耗尽、
// 遇到不可恢复错误，或者这次连接根本没有有效的 Recovery 配置）才说明
// 这个通道对象已经报废，此时才兜底走 xamqp 自己的 reconnect() 全新通道
// 流程。StateClosed 且 Err == nil 对应正常关闭，仅记录日志。
func (m *Manager) startNotifyCancelOrClosed() {
	notifyCancelChan := m.channel.NotifyCancel(make(chan string, 1))
	stateCh := make(chan *amqp.StateChanged, 1)
	m.channel.NotifyStateChange(stateCh)
	m.handleCancelOrStateChange(notifyCancelChan, stateCh)
}

// handleCancelOrStateChange 是 startNotifyCancelOrClosed 的事件循环部分，
// 独立拆出便于在没有真实 broker 的环境下用手动驱动的 channel 单元测试其分支逻辑。
//
// NotifyCancel 与 NotifyStateChange 并行监听：两者中任意一个先触发需要
// 处理的事件就 return（外层 reconnectLoop 成功后会重新调用 startNotifyCancelOrClosed
// 监听新通道），未触发的那个分支若因通道对象被销毁而被关闭，读到零值后
// 置为 nil 防止忙轮询，让 select 只在有意义的分支上继续等待。
func (m *Manager) handleCancelOrStateChange(notifyCancelChan <-chan string, stateCh <-chan *amqp.StateChanged) {
	for {
		select {
		case tag, ok := <-notifyCancelChan:
			if !ok {
				notifyCancelChan = nil
				continue
			}
			m.logger.Errorf("attempting to reconnect to amqp server after cancel with error: %s", tag)
			m.reconnectLoop()
			m.logger.Warnf("successfully reconnected to amqp server after cancel")
			if err := m.dispatcher.Dispatch(errors.New(tag)); err != nil {
				m.logger.Warnf("channel dispatch err: %v", err)
			}
			return
		case sc, ok := <-stateCh:
			if !ok {
				return
			}
			switch sc.To {
			case amqp.StateReconnecting:
				m.logger.Warnf("amqp channel lost, native recovery in progress")
			case amqp.StateOpen:
				m.logger.Infof("amqp channel recovered by native recovery")
			case amqp.StateClosed:
				if sc.Err != nil {
					m.logger.Errorf("native recovery exhausted, falling back to full channel rebuild: %v", sc.Err)
					m.reconnectLoop()
					m.logger.Warnf("successfully reconnected to amqp server")
					_ = m.dispatcher.Dispatch(sc.Err)
				} else {
					m.logger.Infof("amqp channel closed gracefully")
				}
				return
			default:
			}
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
//
// 同时 select 着 notifyConnReconnect()（默认即 connManager.NotifyReconnect，
// 见 Manager 结构体字段注释）：本方法开新通道依赖的是 connManager 当前
// 持有的连接，若那个连接自己也在做（原生恢复放弃后的）兜底重连，两边
// 的退避计时器互相独立、互不知情——曾实测到通道这一层
// 白白多等了 20+ 秒才重试，而彼时底层连接其实早就恢复了。收到连接重连
// 事件后立刻跳过剩余的等待、马上重试一次，能把这段浪费的时间压缩到
// 几乎为零；若这次重试因为其他原因失败，照常回落到正常的退避节奏，
// 不重置 attempt 计数，避免连接反复抖动时形成打不停的重试。
//
// 局限：NotifyReconnect 的事件通道是无缓冲的，Dispatch 只在本 goroutine
// 正停在 select 里等待时才能送达；若那一刻本方法恰好在执行 reconnect()
// 本身（网络 I/O 中），Dispatch 最多阻塞 5 秒后放弃投递，这次事件就错过
// 了——退化为原来"各自独立退避"的行为，不会更差，只是没能加速。
func (m *Manager) reconnectLoop() {
	attempt := 0

	connReconnected, unsubscribeConnReconnect := m.notifyConnReconnect()
	defer func() { unsubscribeConnReconnect <- struct{}{} }()

	for {
		delay := backoff.Delay(m.reconnectInterval, attempt)
		m.logger.Infof("waiting %s to attempt to reconnect to amqp server", delay)
		select {
		case <-m.closeSignal:
			m.logger.Infof("channel manager closed, stopping reconnect loop")
			return
		case <-connReconnected:
			m.logger.Infof("underlying connection recovered, retrying immediately instead of waiting out the backoff")
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
