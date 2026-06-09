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
//   - connectionMu RWMutex：保护 connection 字段，允许多个通道并发读取同一连接
//   - reconnectionCountMu Mutex：单独保护重连计数，与 connectionMu 解耦避免锁竞争
//   - publisherNotifyBlockingReceiversMu：保护阻塞接收者列表的并发修改
//
// 阻塞信号广播机制：
//   - universalNotifyBlockingReceiver 唯一接收底层连接的阻塞信号
//   - readUniversalBlockReceiver 将信号广播给所有已注册的 Publisher
//   - 避免多个 Publisher 各自注册 NotifyBlocked 导致的信号丢失问题
type Manager struct {
	logger              logger.ILogger
	resolver            IResolver
	connection          *amqp.Connection // AMQP 连接，受 connectionMu 保护
	amqpConfig          amqp.Config
	connectionMu        *sync.RWMutex
	ReconnectInterval   time.Duration // 首次重连等待时间（指数退避的基础值）
	reconnectionCount   uint
	reconnectionCountMu *sync.Mutex

	dispatcher *dispatcher.Dispatcher // 重连事件广播器

	// 单一接收者模式：只注册一个 NotifyBlocked 通道，避免多次注册
	universalNotifyBlockingReceiver     chan amqp.Blocking
	universalNotifyBlockingReceiverUsed bool
	publisherNotifyBlockingReceiversMu  *sync.RWMutex
	publisherNotifyBlockingReceivers    []chan amqp.Blocking // 所有 Publisher 的阻塞通知通道
}

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
	go connManager.startNotifyClose()           // 监听连接关闭事件
	go connManager.readUniversalBlockReceiver() // 广播 TCP 阻塞信号给所有 Publisher
	return &connManager, nil
}

// Close 安全关闭 AMQP 连接，通知 RabbitMQ 服务器正常断开。
func (m *Manager) Close() error {
	m.logger.Infof("closing connection manager...")
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	err := m.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect 订阅连接重建成功事件。
func (m *Manager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return m.dispatcher.AddSubscriber()
}

// CheckoutConnection 借出连接用于创建通道，需配对调用 CheckinConnection。
//
// 通过持有读锁防止借用期间连接被替换（重连时持写锁），
// 保证在同一连接上创建的通道不会指向已关闭的连接。
func (m *Manager) CheckoutConnection() *amqp.Connection {
	m.connectionMu.RLock()
	return m.connection
}

// CheckinConnection 归还连接，释放 CheckoutConnection 持有的读锁。
func (m *Manager) CheckinConnection() {
	m.connectionMu.RUnlock()
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

// reconnectLoop 持续重连直到成功，使用指数退避（上限 60 秒）。
func (m *Manager) reconnectLoop() {
	backoff := m.ReconnectInterval
	const maxBackoff = time.Second * 60

	for {
		m.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", backoff)
		time.Sleep(backoff)
		err := m.reconnect()
		if err != nil {
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

// reconnect 在写锁保护下安全替换底层 AMQP 连接。
//
// 先建立新连接再关闭旧连接，保证不出现无连接可用的空窗期。
func (m *Manager) reconnect() error {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	conn, err := m.dial()
	if err != nil {
		return err
	}

	// 先建立新连接，再关闭旧连接
	if err = m.connection.Close(); err != nil {
		m.logger.Warnf("error closing connection while reconnecting: %v", err)
	}

	m.connection = conn
	return nil
}

// IsClosed 检查连接是否已关闭。
func (m *Manager) IsClosed() bool {
	m.connectionMu.Lock()
	defer m.connectionMu.Unlock()

	return m.connection.IsClosed()
}

// maskPassword 将 AMQP URL 中的密码替换为 "***"，用于安全日志输出。
func (m *Manager) maskPassword(s string) string {
	parsedUrl, err := url.Parse(s)
	if err != nil {
		return s
	}
	return parsedUrl.Redacted() // 标准库 Redacted() 将密码替换为 "xxxxx"
}
