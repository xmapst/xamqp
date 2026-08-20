package connection

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/backoff"
	"github.com/xmapst/xamqp/internal/dispatcher"
)

// Manager 管理 RabbitMQ 连接的生命周期，实现连接断线自动重建。
//
// 并发安全设计：
//   - connectionMu RWMutex：保护 connection/closed 字段，允许多个通道并发读取同一连接
//   - reconnectionCountMu Mutex：单独保护重连计数，与 connectionMu 解耦避免锁竞争
//   - publisherNotifyBlockingReceiversMu：保护阻塞接收者列表的并发修改
//
// 阻塞信号广播机制：
//   - universalNotifyBlockingReceiver 唯一接收底层连接的阻塞信号
//   - readUniversalBlockReceiver 将信号广播给所有已注册的 Publisher
//   - 避免多个 Publisher 各自注册 NotifyBlocked 导致的信号丢失问题
//   - 连接重连后会在新连接上重新注册，避免通知永久失效
//
// 重连策略（amqp091-go >= v1.13.0 原生恢复 + xamqp 自己的兜底重连两层）：
//   - New() 会为每个连接自动填充 amqp.Config.Recovery（除非调用方已经通过
//     WithConnectionOptionsConfig 显式设置），默认启用底层库的原生自动恢复：
//     TCP 重连、exchange/queue/binding 拓扑、消费者订阅全部原地在同一个
//     *amqp.Connection 指针上透明重建，已注册的 NotifyBlocked/NotifyStateChange
//     等监听者随之保留，不丢失、不需要重新注册。
//   - startNotifyClose 统一通过 NotifyStateChange 监听：中间态
//     （StateReconnecting/StateOpen）只记录日志；只有终态 StateClosed 且
//     Err != nil（原生恢复重试耗尽或遇到不可恢复错误，比如认证失败，
//     或者干脆没有配置有效的 Recovery）才说明这个连接对象已经报废——
//     amqp091-go 自身此后不会再做任何事（closeResources 会把
//     noNotify 置为 true、清空 channels/allocator，watchConnection 的
//     监听协程也随着 NotifyClose 通道被关闭而退出），此时才触发 xamqp
//     自己的 dial() 全新连接兜底流程，保证连接会一直重试下去，不会像
//     只靠原生恢复那样在重试耗尽后永久停摆。StateClosed 且 Err == nil
//     对应用户主动 Close()，仅记录日志。
type Manager struct {
	url                 string           // 连接地址，创建后不再变更
	connection          *amqp.Connection // AMQP 连接，受 connectionMu 保护
	amqpConfig          amqp.Config      // 建立连接时使用的协商参数，创建后不再变更
	connectionMu        *sync.RWMutex    // 读写锁：保护 connection/closed 字段
	closed              bool             // 是否已调用 Close，受 connectionMu 保护，防止与在途重连竞争
	closeSignal         chan struct{}    // Close() 时关闭，用于立刻打断正在等待退避的 reconnectLoop
	ReconnectInterval   time.Duration    // 首次重连等待时间（指数退避的基础值），同时也是原生恢复重试间隔的默认来源
	reconnectionCount   uint             // 累计重连次数
	reconnectionCountMu *sync.Mutex      // 单独保护重连计数，避免与 connectionMu 产生锁竞争

	dispatcher *dispatcher.Dispatcher // 重连事件广播器

	// 单一接收者模式：只注册一个 NotifyBlocked 通道，避免多次注册
	universalNotifyBlockingReceiver     chan amqp.Blocking  // 唯一接收底层连接 TCP 阻塞信号的通道，重连后会被替换为新的
	universalNotifyBlockingReceiverUsed bool                // 是否已有 Publisher 注册过阻塞通知，决定重连后是否需要重新注册
	publisherNotifyBlockingReceiversMu  *sync.RWMutex       // 保护阻塞接收者列表的并发修改
	publisherNotifyBlockingReceivers    []*blockingReceiver // 所有 Publisher 的阻塞通知通道（每个附带互斥锁，定义见 safe_wraps.go）
}

// errClosed 表示连接管理器已被 Close，重连循环应立即停止，不再创建新连接。
var errClosed = errors.New("connection manager is closed")

// defaultRecovery 构造 xamqp 默认使用的原生恢复配置。
//
// 只设置 ReconnectionConfig：RetryInterval 复用 xamqp 自己已有的
// ReconnectInterval 配置项，让它同时控制原生恢复的重试节奏和（原生恢复
// 放弃后）xamqp 自己兜底 reconnectLoop 的退避基础值，用户只需要调一个
// 参数。MaxRetryCount 用 amqp091-go 自己的默认值（5）：原生恢复只会反复
// 重试同一个 URL，重试次数不宜太大，否则会拖慢 xamqp 多节点故障转移的
// 响应速度；ConnectionRecovery/TopologyRecovery/TopologyRecoveryMode
// 留空，amqp091-go 在 DialConfig 内部会自动补上对应的默认实现
// （DefaultConnectionRecovery/DefaultTopologyRecovery/全量拓扑恢复）。
func defaultRecovery(reconnectInterval time.Duration) *amqp.Recovery {
	return &amqp.Recovery{
		ReconnectionConfig: &amqp.ReconnectionConfig{
			MaxRetryCount: amqp.DefaultMaxRetryCount,
			RetryInterval: reconnectInterval,
		},
	}
}

// dial 连接到 amqp 服务器。
//
// 日志中的 URL 通过 maskPassword 脱敏，防止密码出现在日志文件中。
// 不做多节点轮询：生产环境的 RabbitMQ 集群通常在前面有负载均衡（HAProxy、
// LVS、云厂商 LB 等），对外只暴露一个入口地址，节点故障转移由负载均衡层
// 负责，xamqp 自己不需要、也不应该重新实现一遍地址选择策略。
func (m *Manager) dial() (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(m.url, m.amqpConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to amqp server %s: %w", m.maskPassword(m.url), err)
	}
	return conn, nil
}

// New 创建连接管理器，建立初始连接并启动连接异常监听和阻塞信号读取。
func New(url string, conf amqp.Config, reconnectInterval time.Duration) (*Manager, error) {
	if conf.Recovery == nil {
		conf.Recovery = defaultRecovery(reconnectInterval)
	}

	connManager := Manager{
		url:                                url,
		amqpConfig:                         conf,
		connectionMu:                       &sync.RWMutex{},
		closeSignal:                        make(chan struct{}),
		ReconnectInterval:                  reconnectInterval,
		reconnectionCount:                  0,
		reconnectionCountMu:                &sync.Mutex{},
		dispatcher:                         dispatcher.New(),
		universalNotifyBlockingReceiver:    make(chan amqp.Blocking),
		publisherNotifyBlockingReceiversMu: &sync.RWMutex{},
	}
	conn, err := connManager.dial()
	if err != nil {
		return nil, err
	}
	connManager.connection = conn
	go connManager.startNotifyClose() // 监听连接关闭事件
	// 注意：readUniversalBlockReceiver 不在这里启动。它只有在有 Publisher 通过
	// NotifyBlockedSafe 真正注册阻塞通知时才会被启动（见 safe_wraps.go），此时
	// universalNotifyBlockingReceiver 才会被注册进 m.connection.NotifyBlocked，
	// 该 channel 才会在连接关闭时被 amqp091-go 关闭，range 循环才有退出的机会。
	// 若在此处无条件启动，只使用 NewChannel/NewConsumer、从未创建过 Publisher
	// 的 Conn 上，这个 channel 永远不会被注册也就永远不会被关闭，
	// readUniversalBlockReceiver 的 `for b := range receiver` 会永久阻塞，
	// 泄漏一个 goroutine 直到进程退出，即使调用了 Conn.Close() 也不例外。
	return &connManager, nil
}

// Close 安全关闭 AMQP 连接，通知 RabbitMQ 服务器正常断开。
//
// 幂等：重复调用直接返回 nil。同时标记 closed 并关闭 closeSignal，
// 使任何在途的 reconnect() 在完成拨号后能感知到关闭并放弃安装新连接，
// 也使正在等待退避的 reconnectLoop 立刻退出，而不必等满一整个退避周期，
// 避免 Close 与重连竞争导致产生一个再也无法关闭的孤立连接。
func (m *Manager) Close() error {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true
	close(m.closeSignal)

	slog.Info("closing connection manager...")
	return m.connection.Close()
}

// NotifyReconnect 订阅连接重建成功事件。
func (m *Manager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return m.dispatcher.AddSubscriber()
}

// WithConnection 在读锁保护下安全地将当前连接借给 fn 使用。
//
// 相比"借出/归还"两个方法配对调用的模式，
// 此处将持锁范围收敛在一次函数调用内，调用方不可能忘记释放锁，
// 也不会在 fn 提前返回或 panic 时永久占用读锁导致 reconnect()/Close() 死锁。
func (m *Manager) WithConnection(fn func(*amqp.Connection) error) error {
	m.connectionMu.RLock()
	defer m.connectionMu.RUnlock()

	return fn(m.connection)
}

// startNotifyClose 通过 NotifyStateChange 监听连接的生命周期变化，
// 原生恢复放弃时触发 xamqp 自己的兜底重连流程。
//
// amqp091-go 的原生恢复会原地复用同一个 *amqp.Connection：TCP 重连、
// exchange/queue/binding 拓扑、消费者订阅全部由库自身透明重建，期间该
// 连接对象上已注册的 NotifyBlocked 等监听者无需重新注册（resetState 不会
// 清空这些监听者列表），所以中间态（StateReconnecting/StateOpen）xamqp
// 不需要做任何事——下游 Publisher/Consumer 看到的始终是同一个连接对象，
// 不存在需要"重新初始化"的东西。
//
// 只有终态 StateClosed 且 Err != nil（原生恢复重试耗尽、遇到不可恢复
// 错误，或者这次连接根本没有有效的 Recovery 配置）才说明这个连接对象
// 已经报废，此时才兜底走 xamqp 自己的 dial() 全新连接流程。
// 优雅关闭（用户调用 Close()）对应 StateClosed 且 Err == nil，仅记录日志。
func (m *Manager) startNotifyClose() {
	stateCh := make(chan *amqp.StateChanged, 1)
	m.connection.NotifyStateChange(stateCh)
	m.handleStateChanges(stateCh)
}

// handleStateChanges 是 startNotifyClose 的事件循环部分，独立拆出便于
// 在没有真实 broker 的环境下用手动驱动的 channel 单元测试其分支逻辑。
func (m *Manager) handleStateChanges(stateCh <-chan *amqp.StateChanged) {
	for sc := range stateCh {
		switch sc.To {
		case amqp.StateReconnecting:
			slog.Warn("amqp connection lost, native recovery in progress")
		case amqp.StateOpen:
			slog.Info("amqp connection recovered by native recovery")
		case amqp.StateClosed:
			if sc.Err != nil {
				slog.Error("native recovery exhausted, falling back to full reconnect", slog.Any("error", sc.Err))
				m.reconnectLoop()
				slog.Warn("successfully reconnected to amqp server")
				_ = m.dispatcher.Dispatch(sc.Err)
			} else {
				slog.Info("amqp connection closed gracefully")
			}
			return
		default:
		}
	}
}

// ReconnectionCount 获取累计重连次数。
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

// reconnectLoop 持续重连直到成功或管理器被关闭，使用带抖动的指数退避。
//
// 等待退避时间时 select 着 closeSignal：Close() 会立刻打断等待并退出，
// 不必等满一整个退避周期才发现管理器已经关闭。
func (m *Manager) reconnectLoop() {
	attempt := 0

	for {
		delay := backoff.Delay(m.ReconnectInterval, attempt)
		slog.Info("waiting to attempt to reconnect to amqp server", slog.Duration("delay", delay))
		select {
		case <-m.closeSignal:
			slog.Info("connection manager closed, stopping reconnect loop")
			return
		case <-time.After(delay):
		}

		err := m.reconnect()
		if err != nil {
			if errors.Is(err, errClosed) {
				slog.Info("connection manager closed, stopping reconnect loop")
				return
			}
			slog.Error("error reconnecting to amqp server", slog.Any("error", err))
			attempt++
		} else {
			m.incrementReconnectionCount()
			go m.startNotifyClose() // 重连成功后继续监听新连接
			return
		}
	}
}

// reconnect 建立新连接后，仅在写锁保护下做指针替换，避免长时间持锁做网络 I/O。
//
// 拨号（网络 I/O，耗时较长）在锁外完成，
// 只有安装新连接、关闭旧连接前的状态检查才需要写锁；
// 若此时管理器已被 Close，则丢弃刚建立的新连接并返回 errClosed，
// 防止 Close 与重连竞争产生一个再也没有人引用、也没有人能关闭的孤立连接。
func (m *Manager) reconnect() error {
	conn, err := m.dial()
	if err != nil {
		return err
	}

	m.connectionMu.Lock()
	if m.closed {
		m.connectionMu.Unlock()
		if cerr := conn.Close(); cerr != nil {
			slog.Warn("error closing redundant connection after manager was closed", slog.Any("error", cerr))
		}
		return errClosed
	}

	oldConnection := m.connection
	m.connection = conn

	// 旧连接关闭时，amqp091-go 会关闭所有通过 NotifyBlocked 注册在其上的通道，
	// 包括 universalNotifyBlockingReceiver，导致 readUniversalBlockReceiver 退出。
	// 若此前已有 Publisher 注册过阻塞通知，需要在新连接上重新注册并重启广播协程，
	// 否则 TCP 阻塞背压通知会在重连后永久失效。
	if m.universalNotifyBlockingReceiverUsed {
		newReceiver := make(chan amqp.Blocking)
		m.universalNotifyBlockingReceiver = newReceiver
		m.connection.NotifyBlocked(newReceiver)
		go m.readUniversalBlockReceiver(newReceiver)
	}
	m.connectionMu.Unlock()

	// 先建立并安装新连接，再关闭旧连接，保证不出现无连接可用的空窗期。
	if err = oldConnection.Close(); err != nil {
		slog.Warn("error closing connection while reconnecting", slog.Any("error", err))
	}
	return nil
}

// IsClosed 检查连接是否已关闭。
func (m *Manager) IsClosed() bool {
	m.connectionMu.RLock()
	defer m.connectionMu.RUnlock()

	return m.closed || m.connection.IsClosed()
}

// maskPassword 将 AMQP URL 中的密码替换为 "***"，用于安全日志输出。
//
// 解析失败时返回固定占位串而不是原始输入：原始字符串可能就是一个
// 带密码、只是格式有误的连接串，直接透传会把密码原样打进日志。
func (m *Manager) maskPassword(s string) string {
	parsedUrl, err := url.Parse(s)
	if err != nil {
		return "<unparseable url redacted>"
	}
	return parsedUrl.Redacted() // 标准库 Redacted() 将密码替换为 "xxxxx"
}
