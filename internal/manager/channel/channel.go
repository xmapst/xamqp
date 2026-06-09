package channel

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

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
//   - channelMu RWMutex 保护 channel 字段的并发访问
//   - 读操作（Consume/Publish/Declare 等）持有读锁，允许并发
//   - 写操作（reconnect/Close）持有写锁，独占访问
//   - reconnectionCountMu 单独保护重连计数，避免与 channelMu 产生锁竞争
type Manager struct {
	logger              logger.ILogger
	channel             *amqp.Channel // 底层 AMQP 通道，受 channelMu 保护
	connManager         *connection.Manager
	channelMu           *sync.RWMutex // 读写锁：允许多读单写
	reconnectInterval   time.Duration
	reconnectionCount   uint // 累计重连次数，用于确认消息的 ID 计算
	reconnectionCountMu *sync.Mutex
	dispatcher          *dispatcher.Dispatcher // 重连事件广播器
}

// New 创建通道管理器，初始化时立即建立 AMQP 通道并启动异常监听。
func New(connManager *connection.Manager, log logger.ILogger, reconnectInterval time.Duration) (*Manager, error) {
	chanManager := &Manager{
		logger:              log,
		connManager:         connManager,
		channelMu:           &sync.RWMutex{},
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
//
// 使用 Checkout/Checkin 模式访问连接，通过引用计数防止连接在使用中被关闭。
func (m *Manager) getNewChannel() (*amqp.Channel, error) {
	conn := m.connManager.CheckoutConnection()
	defer m.connManager.CheckinConnection()

	ch, err := conn.Channel()
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

// reconnectLoop 持续尝试重建通道，使用指数退避策略（最大等待 60 秒）。
//
// 指数退避的意义：避免在服务器故障期间因频繁重试加剧服务器负载，
// 同时通过设置上限（60s）保证最终能在合理时间内重连。
func (m *Manager) reconnectLoop() {
	backoff := m.reconnectInterval
	const maxBackoff = time.Second * 60

	for {
		m.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", backoff)
		time.Sleep(backoff)
		err := m.reconnect()
		if err != nil {
			m.logger.Errorf("error reconnecting to amqp server: %v", err)
			backoff *= 2 // 指数退避：每次失败后等待时间翻倍
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			m.incrementReconnectionCount()
			go m.startNotifyCancelOrClosed() // 重连成功后继续监听新通道的异常
			return
		}
	}
}

// reconnect 在写锁保护下安全替换底层 AMQP 通道。
//
// 先获取新通道，再关闭旧通道，保证不会出现无通道可用的空窗期。
func (m *Manager) reconnect() error {
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	newChannel, err := m.getNewChannel()
	if err != nil {
		return err
	}

	// 先建立新通道，再关闭旧通道，防止期间有操作无通道可用
	if m.channel != nil {
		if err = m.channel.Close(); err != nil {
			m.logger.Warnf("error closing channel while reconnecting: %v", err)
		}
	}

	m.channel = newChannel
	return nil
}

// Close 安全关闭 AMQP 通道，释放服务器端资源。
func (m *Manager) Close() error {
	m.logger.Infof("closing channel manager...")
	m.channelMu.Lock()
	defer m.channelMu.Unlock()

	err := m.channel.Close()
	if err != nil {
		m.logger.Errorf("close err: %v", err)
		return err
	}

	return nil
}

// NotifyReconnect 订阅通道重连成功事件，返回事件通道和关闭信号通道。
func (m *Manager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return m.dispatcher.AddSubscriber()
}
