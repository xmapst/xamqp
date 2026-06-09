package xamqp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/manager/channel"
	"github.com/xmapst/xamqp/internal/manager/connection"
)

// 消息持久化模式常量，与 amqp.Transient/Persistent 保持一致。
//
// Transient（非持久化）：吞吐量更高，但 Broker 重启后消息丢失。
// Persistent（持久化）：消息存储到磁盘，Broker 重启后可恢复，但性能略低。
// 注意：消息持久化与队列持久化相互独立，非持久化消息即使在持久化队列中也不会恢复。
const (
	Transient  = amqp.Transient
	Persistent = amqp.Persistent
)

// Return 捕获因 mandatory 或 immediate 标志导致无法路由到队列的消息信息。
//
// 当消息设置了 mandatory=true 但没有匹配的队列时，
// RabbitMQ 通过 basic.return 将消息返回给发布者，通过 NotifyReturn 处理。
type Return struct {
	amqp.Return
}

// Confirmation 消息发布确认通知，包含投递标签和重连计数。
//
// 在 ConfirmMode 下，每条消息发布后会收到一个 Confirmation，
// ReconnectionCount 用于处理重连后投递标签（DeliveryTag）从 0 重置的情况，
// 使用 ReconnectionCount + DeliveryTag 组合可唯一标识每条消息。
type Confirmation struct {
	amqp.Confirmation
	ReconnectionCount int
}

// Publisher 消息发布者，支持多路由键发布和连接断线重连。
//
// 线程安全设计：
//   - disablePublishDueToFlow/Blocked 通过 RWMutex 保护，读多写少场景性能高
//   - notifyReturnHandler/notifyPublishHandler 通过 handlerMu 保护注册时的并发安全
//
// 流控机制：当 RabbitMQ 服务器资源紧张时会发送 Flow/Block 信号，
// Publisher 收到信号后自动暂停发布，防止继续向过载的 Broker 发送消息。
type Publisher struct {
	chanManager                *channel.Manager
	connManager                *connection.Manager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	disablePublishDueToFlow   bool          // 流控禁止发布标志
	disablePublishDueToFlowMu *sync.RWMutex // 保护流控标志的读写锁

	disablePublishDueToBlocked   bool          // TCP 阻塞禁止发布标志
	disablePublishDueToBlockedMu *sync.RWMutex // 保护 TCP 阻塞标志的读写锁

	handlerMu            *sync.Mutex
	notifyReturnHandler  func(r Return)
	notifyPublishHandler func(p Confirmation)

	options PublisherOptions

	blockings chan amqp.Blocking // 接收 TCP 阻塞事件的通道
}

// PublisherConfirmation 多路由键发布时每条消息的延迟确认集合。
type PublisherConfirmation []*amqp.DeferredConfirmation

// NewPublisher 创建消息发布者，自动处理连接重建和流控信号。
//
// ConfirmMode 开启时，每条发布的消息都会收到 RabbitMQ 的确认或否认，
// 通过 NotifyPublish 注册处理函数接收确认事件，保证消息不丢失。
// 若不需要确认，使用默认模式可获得更高的吞吐量。
func NewPublisher(conn *Conn, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	options := new(getDefaultPublisherOptions())
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}
	publisher := &Publisher{
		connManager:                  conn.connManager,
		disablePublishDueToFlow:      false,
		disablePublishDueToFlowMu:    &sync.RWMutex{},
		disablePublishDueToBlocked:   false,
		disablePublishDueToBlockedMu: &sync.RWMutex{},
		handlerMu:                    &sync.Mutex{},
		notifyReturnHandler:          nil,
		notifyPublishHandler:         nil,
		options:                      *options,
	}
	var err error
	publisher.chanManager, err = channel.New(conn.connManager, options.Logger, conn.connManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	publisher.reconnectErrCh, publisher.closeConnectionToManagerCh = publisher.chanManager.NotifyReconnect()

	err = publisher.startup()
	if err != nil {
		return nil, err
	}

	// ConfirmMode：注册空白处理器使通道进入 confirm 模式，实际处理由业务代码注册
	if options.ConfirmMode {
		publisher.NotifyPublish(func(_ Confirmation) {
			// 空处理器：仅用于开启 confirm 模式，不处理具体确认事件
		})
	}

	// 后台 goroutine：监听重连事件，重连后重新初始化流控监听
	go func() {
		for err = range publisher.reconnectErrCh {
			publisher.options.Logger.Infof("successful publisher recovery from: %v", err)
			err = publisher.startup()
			if err != nil {
				publisher.options.Logger.Errorf("error on startup for publisher after cancel or close: %v", err)
				publisher.options.Logger.Errorf("publisher closing, unable to recover")
				return
			}
			publisher.startReturnHandler()
			publisher.startPublishHandler()
		}
	}()

	return publisher, nil
}

// startup 初始化发布者：声明交换机并启动流控监听器。
func (publisher *Publisher) startup() error {
	err := declareExchange(publisher.chanManager, publisher.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	go publisher.startNotifyFlowHandler()    // 监听服务器 Flow 控制信号
	go publisher.startNotifyBlockedHandler() // 监听 TCP 阻塞信号
	return nil
}

// Publish 向指定路由键发布消息（使用 context.Background()）。
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	return publisher.PublishWithContext(context.Background(), data, routingKeys, optionFuncs...)
}

// PublishWithContext 向指定路由键集合发布消息，支持上下文取消和流控检查。
//
// 流控检查：发布前先检查流控和 TCP 阻塞状态，
// 若 Broker 发出了暂停信号，立即返回错误而非阻塞等待，
// 调用方可自行实现重试逻辑（如带退避的重试）。
func (publisher *Publisher) PublishWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMu.RLock()
	defer publisher.disablePublishDueToFlowMu.RUnlock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	defer publisher.disablePublishDueToBlockedMu.RUnlock()
	if publisher.disablePublishDueToBlocked {
		return fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient // 默认非持久化，获取更高吞吐量
	}

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = options.Headers
		message.Expiration = options.Expiration
		message.ContentEncoding = options.ContentEncoding
		message.Priority = options.Priority
		message.CorrelationId = options.CorrelationID
		message.ReplyTo = options.ReplyTo
		message.MessageId = options.MessageID
		message.Timestamp = options.Timestamp
		message.Type = options.Type
		message.UserId = options.UserID
		message.AppId = options.AppID

		err := publisher.chanManager.PublishWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishWithDeferredConfirmWithContext 在确认模式下发布消息，返回可等待的确认对象。
//
// 返回的 PublisherConfirmation 切片中每个元素对应一个路由键的发布确认，
// 调用方可通过 DeferredConfirmation.Wait() 等待 RabbitMQ 确认消息已持久化。
// 若 Publisher 未处于确认模式，返回的确认对象始终为 nil。
func (publisher *Publisher) PublishWithDeferredConfirmWithContext(
	ctx context.Context,
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) (PublisherConfirmation, error) {
	publisher.disablePublishDueToFlowMu.RLock()
	defer publisher.disablePublishDueToFlowMu.RUnlock()
	if publisher.disablePublishDueToFlow {
		return nil, fmt.Errorf("publishing blocked due to high flow on the server")
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	defer publisher.disablePublishDueToBlockedMu.RUnlock()
	if publisher.disablePublishDueToBlocked {
		return nil, fmt.Errorf("publishing blocked due to TCP block on the server")
	}

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	var deferredConfirmations []*amqp.DeferredConfirmation

	for _, routingKey := range routingKeys {
		message := amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = options.Headers
		message.Expiration = options.Expiration
		message.ContentEncoding = options.ContentEncoding
		message.Priority = options.Priority
		message.CorrelationId = options.CorrelationID
		message.ReplyTo = options.ReplyTo
		message.MessageId = options.MessageID
		message.Timestamp = options.Timestamp
		message.Type = options.Type
		message.UserId = options.UserID
		message.AppId = options.AppID

		conf, err := publisher.chanManager.PublishWithDeferredConfirmWithContextSafe(
			ctx,
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return nil, err
		}
		deferredConfirmations = append(deferredConfirmations, conf)
	}
	return deferredConfirmations, nil
}

// Close 关闭发布者，释放 AMQP 通道和连接管理器订阅。
//
// 关闭后此 Publisher 实例不可复用，仅调用一次。
func (publisher *Publisher) Close() {
	err := publisher.chanManager.Close()
	if err != nil {
		publisher.options.Logger.Warnf("error while closing the channel: %v", err)
	}
	publisher.options.Logger.Infof("closing publisher...")
	publisher.connManager.RemovePublisherBlockingReceiver(publisher.blockings)
	go func() {
		publisher.closeConnectionToManagerCh <- struct{}{}
	}()
}

// NotifyReturn 注册 basic.return 消息的处理函数。
//
// 当使用 mandatory=true 发布的消息无法路由到任何队列时，
// RabbitMQ 会通过 basic.return 方法将消息退回。
// 注意：这些通知在整个连接级别共享，多个 Publisher 共用同一连接时需注意。
func (publisher *Publisher) NotifyReturn(handler func(r Return)) {
	publisher.handlerMu.Lock()
	start := publisher.notifyReturnHandler == nil
	publisher.notifyReturnHandler = handler
	publisher.handlerMu.Unlock()

	if start {
		publisher.startReturnHandler()
	}
}

// NotifyPublish 注册发布确认事件的处理函数，开启确认模式。
//
// 首次调用此方法会自动将 AMQP 通道置于确认模式（Confirm Mode），
// 此后每条发布的消息都会收到 ack 或 nack 确认。
// 注意：确认通知在整个连接级别共享，多个 Publisher 共用同一连接时需注意。
func (publisher *Publisher) NotifyPublish(handler func(p Confirmation)) {
	publisher.handlerMu.Lock()
	shouldStart := publisher.notifyPublishHandler == nil
	publisher.notifyPublishHandler = handler
	publisher.handlerMu.Unlock()

	if shouldStart {
		publisher.startPublishHandler()
	}
}

// startReturnHandler 启动 basic.return 事件监听 goroutine。
func (publisher *Publisher) startReturnHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyReturnHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()

	go func() {
		returns := publisher.chanManager.NotifyReturnSafe(make(chan amqp.Return, 1))
		for ret := range returns {
			go publisher.notifyReturnHandler(Return{ret})
		}
	}()
}

// startPublishHandler 开启确认模式并启动确认事件处理 goroutine。
//
// 使用信号量（sem）限制并发处理的确认事件数量，防止 goroutine 爆炸。
// 确认处理函数在独立 goroutine 中执行，通过 recover 保护防止 panic 影响整体。
func (publisher *Publisher) startPublishHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyPublishHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()
	_ = publisher.chanManager.ConfirmSafe(false)

	go func() {
		// 信号量控制并发数，防止大量确认事件导致 goroutine 数量无限增长
		const maxConcurrentHandlers = 100
		sem := make(chan struct{}, maxConcurrentHandlers)

		confirmationCh := publisher.chanManager.NotifyPublishSafe(make(chan amqp.Confirmation, 100))
		for conf := range confirmationCh {
			sem <- struct{}{}
			go func(c amqp.Confirmation) {
				defer func() { <-sem }()
				defer func() {
					if r := recover(); r != nil {
						publisher.options.Logger.Errorf("panic in publish handler: %v", r)
					}
				}()
				publisher.notifyPublishHandler(Confirmation{
					Confirmation:      c,
					ReconnectionCount: int(publisher.chanManager.ReconnectionCount()),
				})
			}(conf)
		}
	}()
}
