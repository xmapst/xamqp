package connection

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/dispatcher"
	"github.com/xmapst/xamqp/internal/logger"
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
type Manager struct {
	logger              logger.ILogger   // 日志实现
	resolver            IResolver        // 连接地址解析器
	connection          *amqp.Connection // AMQP 连接，受 connectionMu 保护
	amqpConfig          amqp.Config      // 建立连接时使用的协商参数，创建后不再变更
	connectionMu        *sync.RWMutex    // 读写锁：保护 connection/closed 字段
	closed              bool             // 是否已调用 Close，受 connectionMu 保护，防止与在途重连竞争
	ReconnectInterval   time.Duration    // 首次重连等待时间（指数退避的基础值）
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

// IResolver 连接地址解析器接口，支持自定义节点选择策略。
type IResolver interface {
	Resolve() ([]string, error)
}

// dial 按顺序尝试连接所有地址，返回第一个成功的连接。
//
// 多地址支持集群高可用：若首选节点不可用，自动尝试备用节点。
// 日志中的 URL 通过 maskPassword 脱敏，防止密码出现在日志文件中。
func (m *Manager) dial() (*amqp.Connection, error) {
	urls, err := m.resolver.Resolve()
	if err != nil {
		return nil, fmt.Errorf("error resolving amqp server urls: %w", err)
	}

	var errs []error
	for _, _url := range urls {
		conn, err := amqp.DialConfig(_url, m.amqpConfig)
		if err == nil {
			return conn, err
		}
		m.logger.Warnf("failed to connect to amqp server %s: %v", m.maskPassword(_url), err)
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...) // 汇总所有节点的连接错误
}

// New 创建连接管理器，建立初始连接并启动连接异常监听和阻塞信号读取。
func New(resolver IResolver, conf amqp.Config, log logger.ILogger, reconnectInterval time.Duration) (*Manager, error) {
	connManager := Manager{
		logger:                             log,
		resolver:                           resolver,
		amqpConfig:                         conf,
		connectionMu:                       &sync.RWMutex{},
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
	go connManager.startNotifyClose()                                                      // 监听连接关闭事件
	go connManager.readUniversalBlockReceiver(connManager.universalNotifyBlockingReceiver) // 广播 TCP 阻塞信号给所有 Publisher
	return &connManager, nil
}

// Close 安全关闭 AMQP 连接，通知 RabbitMQ 服务器正常断开。
//
// 幂等：重复调用直接返回 nil。同时标记 closed，
// 使任何在途的 reconnect() 在完成拨号后能感知到关闭并放弃安装新连接，
// 避免 Close 与重连竞争导致产生一个再也无法关闭的孤立连接。
func (m *Manager) Close() error {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	m.logger.Infof("closing connection manager...")
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

// startNotifyClose 监听连接关闭事件，异常关闭时触发重连流程。
//
// 正常关闭（Close() 调用）收到 nil 错误，仅记录日志。
// 异常关闭（网络断开、服务器重启等）收到非 nil 错误，触发指数退避重连。
func (m *Manager) startNotifyClose() {
	notifyCloseChan := m.connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-notifyCloseChan
	if err != nil {
		m.logger.Errorf("attempting to reconnect to amqp server after connection close with error: %v", err)
		m.reconnectLoop()
		m.logger.Warnf("successfully reconnected to amqp server")
		_ = m.dispatcher.Dispatch(err)
	}
	if err == nil {
		m.logger.Infof("amqp connection closed gracefully")
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

// reconnectLoop 持续重连直到成功或管理器被关闭，使用指数退避（上限 60 秒）。
func (m *Manager) reconnectLoop() {
	backoff := m.ReconnectInterval
	const maxBackoff = time.Second * 60

	for {
		m.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", backoff)
		time.Sleep(backoff)
		err := m.reconnect()
		if err != nil {
			if errors.Is(err, errClosed) {
				m.logger.Infof("connection manager closed, stopping reconnect loop")
				return
			}
			m.logger.Errorf("error reconnecting to amqp server: %v", err)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			m.incrementReconnectionCount()
			go m.startNotifyClose() // 重连成功后继续监听新连接
			return
		}
	}
}

// reconnect 建立新连接后，仅在写锁保护下做指针替换，避免长时间持锁做网络 I/O。
//
// 拨号（可能重试多个集群节点，耗时较长）在锁外完成，
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
			m.logger.Warnf("error closing redundant connection after manager was closed: %v", cerr)
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
		m.logger.Warnf("error closing connection while reconnecting: %v", err)
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
func (m *Manager) maskPassword(s string) string {
	parsedUrl, err := url.Parse(s)
	if err != nil {
		return s
	}
	return parsedUrl.Redacted() // 标准库 Redacted() 将密码替换为 "xxxxx"
}
