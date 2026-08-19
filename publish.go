package xamqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

// ErrPublishFlowPaused 表示因服务器请求的流控（Flow）而暂停发布。
//
// 属于可重试错误：调用方可用 errors.Is(err, ErrPublishFlowPaused) 判断，
// 并在稍后重试而不是当作永久性失败处理。
var ErrPublishFlowPaused = errors.New("publishing blocked due to high flow on the server")

// ErrPublishBlocked 表示因 TCP 级别的连接阻塞而暂停发布。
//
// 属于可重试错误：调用方可用 errors.Is(err, ErrPublishBlocked) 判断，
// 并在稍后重试而不是当作永久性失败处理。
var ErrPublishBlocked = errors.New("publishing blocked due to TCP block on the server")

// Return 捕获因 mandatory 或 immediate 标志导致无法路由到队列的消息信息。
//
// 当消息设置了 mandatory=true 但没有匹配的队列时，
// RabbitMQ 通过 basic.return 将消息返回给发布者，通过 NotifyReturn 处理。
type Return struct {
	amqp.Return // 内嵌原生 basic.return 消息内容
}

// Confirmation 消息发布确认通知，包含投递标签和重连计数。
//
// 在 ConfirmMode 下，每条消息发布后会收到一个 Confirmation，
// ReconnectionCount 用于处理重连后投递标签（DeliveryTag）从 0 重置的情况，
// 使用 ReconnectionCount + DeliveryTag 组合可唯一标识每条消息。
type Confirmation struct {
	amqp.Confirmation     // 内嵌原生确认内容（DeliveryTag、Ack 等）
	ReconnectionCount int // 收到此确认时通道的累计重连次数，用于与 DeliveryTag 组合唯一标识消息
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
	chanManager                *channel.Manager    // 底层通道管理器，负责实际的通道建立/重连
	connManager                *connection.Manager // 底层连接管理器，用于注册/移除 TCP 阻塞通知
	reconnectErrCh             <-chan error        // 订阅自 chanManager 的重连事件通道，触发重新初始化
	closeConnectionToManagerCh chan<- struct{}     // 取消订阅信号：Close() 时写入，通知 chanManager 停止推送重连事件
	closeOnce                  sync.Once           // 保证 Close() 幂等，避免重复调用时重复清理或泄漏 goroutine

	isClosedMu *sync.RWMutex // 保护 isClosed 的并发读写
	isClosed   bool          // 是否已关闭，关闭后重连恢复协程停止重试

	disablePublishDueToFlow   bool          // 流控禁止发布标志
	disablePublishDueToFlowMu *sync.RWMutex // 保护流控标志的读写锁

	disablePublishDueToBlocked   bool          // TCP 阻塞禁止发布标志
	disablePublishDueToBlockedMu *sync.RWMutex // 保护 TCP 阻塞标志的读写锁

	handlerMu            *sync.Mutex          // 保护 notifyReturnHandler/notifyPublishHandler 注册时的并发安全
	notifyReturnHandler  func(r Return)       // NotifyReturn 注册的 basic.return 处理函数，nil 表示未注册
	notifyPublishHandler func(p Confirmation) // NotifyPublish 注册的发布确认处理函数，nil 表示未注册（未开启 confirm 模式）

	options PublisherOptions // 创建时传入的发布者选项快照

	blocking chan amqp.Blocking // 接收 TCP 阻塞事件的通道，仅在 Close() 时用于从广播列表移除
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
		isClosedMu:                   &sync.RWMutex{},
		isClosed:                     false,
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
		// 用 publisher.Close() 而非只关 chanManager：此时 dispatcher 订阅
		// （NotifyReconnect 返回的 closeConnectionToManagerCh）已经建立，
		// 只关 chanManager 会让 dispatcher 里那个等待取消信号的清理协程
		// 永久泄漏。publisher.blocking 此时还是 nil，Close() 会正确跳过
		// RemovePublisherBlockingReceiver，不会误操作。
		publisher.Close()
		return nil, err
	}

	// 阻塞通知是连接级别的，只需注册一次；同步完成，确保此处返回时
	// publisher.blocking 已就绪，即使调用方随后立即 Close() 也不会漏清理。
	publisher.startNotifyBlockedHandler()

	// ConfirmMode：先同步确认通道成功进入 confirm 模式再返回，
	// 避免调用方拿到一个自称开启了 ConfirmMode、实际并未真正确认消息的 Publisher。
	// ConfirmSafe 是幂等的，随后 NotifyPublish 内部再次调用不会重复生效。
	if options.ConfirmMode {
		if err = publisher.chanManager.ConfirmSafe(false); err != nil {
			// 同上，用 publisher.Close() 完整清理：此时 startNotifyBlockedHandler
			// 已经同步完成注册，publisher.blocking 非 nil，只关 chanManager
			// 会漏掉 RemovePublisherBlockingReceiver 和 dispatcher 取消订阅，
			// 泄漏 readBlockedNotifications 和 dispatcher 清理协程。
			publisher.Close()
			return nil, fmt.Errorf("could not put channel in confirm mode: %w", err)
		}
		publisher.NotifyPublish(func(_ Confirmation) {
			// 空处理器：仅用于开启 confirm 模式，不处理具体确认事件
		})
	}

	// 后台 goroutine：监听重连事件，重连后重新初始化流控监听。
	// 与 Consumer.startConsumerWithRetry 一致，startup 失败时以退避重试直到成功或
	// Publisher 被关闭，而不是放弃恢复——否则一次瞬时错误就会让 Publisher
	// 永久停止刷新流控状态和确认回调，且不给调用方任何提示。
	go func() {
		for reconnectErr := range publisher.reconnectErrCh {
			publisher.options.Logger.Infof("successful publisher recovery from: %v", reconnectErr)
			for {
				if publisher.getIsClosed() {
					return
				}
				if err := publisher.startup(); err != nil {
					publisher.options.Logger.Errorf("error on startup for publisher after cancel or close: %v", err)
					time.Sleep(3 * time.Second)
					continue
				}
				break
			}
			publisher.startReturnHandler()
			publisher.startPublishHandler()
		}
	}()

	return publisher, nil
}

// startup 初始化发布者：声明交换机并启动流控监听器。
//
// 注意：不在此处启动阻塞通知监听（startNotifyBlockedHandler）——
// 阻塞信号是连接级别的，与本方法会在每次 channel 重连后重复调用不同，
// 阻塞监听只应在构造时启动一次，详见 NewPublisher 和 publish_flow_block.go。
func (publisher *Publisher) startup() error {
	err := declareExchange(publisher.chanManager, publisher.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	go publisher.startNotifyFlowHandler() // 监听服务器 Flow 控制信号
	return nil
}

// getIsClosed 返回 Publisher 是否已关闭。
func (publisher *Publisher) getIsClosed() bool {
	publisher.isClosedMu.RLock()
	defer publisher.isClosedMu.RUnlock()
	return publisher.isClosed
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
	// 只在读锁保护下拷贝标志位后立即释放锁，不跨越后面的网络发布和
	// confirm 等待——否则 Flow/Blocked 的 handler 更新状态时需要的写锁
	// 会因为 Go RWMutex 的写者优先语义，被所有并发 Publish 调用连带卡住，
	// 直到最慢的一次发布等到 broker 确认为止。
	publisher.disablePublishDueToFlowMu.RLock()
	blockedByFlow := publisher.disablePublishDueToFlow
	publisher.disablePublishDueToFlowMu.RUnlock()
	if blockedByFlow {
		return ErrPublishFlowPaused
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	blockedByTCP := publisher.disablePublishDueToBlocked
	publisher.disablePublishDueToBlockedMu.RUnlock()
	if blockedByTCP {
		return ErrPublishBlocked
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
	// 同 PublishWithContext：拷贝标志位后立即释放锁，不跨越后面的网络发布。
	publisher.disablePublishDueToFlowMu.RLock()
	blockedByFlow := publisher.disablePublishDueToFlow
	publisher.disablePublishDueToFlowMu.RUnlock()
	if blockedByFlow {
		return nil, ErrPublishFlowPaused
	}

	publisher.disablePublishDueToBlockedMu.RLock()
	blockedByTCP := publisher.disablePublishDueToBlocked
	publisher.disablePublishDueToBlockedMu.RUnlock()
	if blockedByTCP {
		return nil, ErrPublishBlocked
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
// 关闭后此 Publisher 实例不可复用。幂等：重复调用是安全的，
// 后续调用直接返回，不会重复关闭 channel 或重复发送取消订阅信号。
func (publisher *Publisher) Close() {
	publisher.closeOnce.Do(func() {
		publisher.isClosedMu.Lock()
		publisher.isClosed = true
		publisher.isClosedMu.Unlock()

		err := publisher.chanManager.Close()
		if err != nil {
			publisher.options.Logger.Warnf("error while closing the channel: %v", err)
		}
		publisher.options.Logger.Infof("closing publisher...")
		publisher.disablePublishDueToBlockedMu.RLock()
		blocking := publisher.blocking
		publisher.disablePublishDueToBlockedMu.RUnlock()
		if blocking != nil {
			publisher.connManager.RemovePublisherBlockingReceiver(blocking)
		}
		go func() {
			publisher.closeConnectionToManagerCh <- struct{}{}
		}()
	})
}

// NotifyReturn 注册 basic.return 消息的处理函数。
//
// 当使用 mandatory=true 发布的消息无法路由到任何队列时，
// RabbitMQ 会通过 basic.return 方法将消息退回。
// 注意：该通知只绑定在此 Publisher 自身独占的 AMQP 通道上，
// 与其他 Publisher（即使共用同一连接）互不影响；仅当多个 goroutine
// 并发调用同一个 Publisher 实例的 NotifyReturn 时才需要注意覆盖问题。
//
// handler 为每个事件单独起一个 goroutine 并发调用（见 startReturnHandler），
// 不保证到达顺序、可能并发执行；这是有意的取舍：amqp091-go 用同一个协程
// 读取整条底层 TCP 连接上所有通道的帧，若在那个读取循环里同步调用业务
// handler，一个耗时较久或卡住的 handler 会连带阻塞共用同一条连接的所有
// 其他 Publisher/Consumer。并发派发能让事件很快从内部缓冲区被取走，
// 把这个风险降到最低。
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
// 注意：该通知同样只绑定在此 Publisher 自身独占的 AMQP 通道上，
// 不与其他 Publisher 共享（即使它们共用同一连接）。
//
// handler 同样为每个确认事件并发调用（受信号量限制并发数，见
// startPublishHandler），语义和取舍同 NotifyReturn。
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
//
// 为每个事件单独起一个 goroutine 并发调用 handler：读取事件的这个循环
// 本身只负责尽快把事件从 channel 里取走再转手扔给新协程处理，不会被
// 慢 handler 拖住——这个循环的读取速度直接决定 amqp091-go 共享的连接
// 读取协程会不会被写阻塞（详见 NotifyReturn 文档），所以这里不能改成
// 同步调用。代价是不保证多个 basic.return 之间的处理顺序。
func (publisher *Publisher) startReturnHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyReturnHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()

	go func() {
		returns := publisher.chanManager.NotifyReturnSafe(make(chan amqp.Return, 100))
		for ret := range returns {
			go publisher.invokeReturnHandler(Return{ret})
		}
	}()
}

// invokeReturnHandler 执行 basic.return 处理函数，通过 recover 保护防止 panic 影响整体。
func (publisher *Publisher) invokeReturnHandler(r Return) {
	defer func() {
		if rec := recover(); rec != nil {
			publisher.options.Logger.Errorf("panic in return handler: %v", rec)
		}
	}()
	publisher.notifyReturnHandler(r)
}

// startPublishHandler 开启确认模式并启动确认事件处理 goroutine。
//
// 用信号量把并发处理的确认事件数量限制在 maxConcurrentHandlers，
// 避免海量确认事件瞬间涌入时无限制地起协程；理由同 startReturnHandler，
// 读取循环本身仍然只负责尽快转手，不同步调用 handler。注意信号量本身
// 意味着一旦 maxConcurrentHandlers 个 handler 同时卡住，第 101 个事件
// 的转手会被 `sem <- struct{}{}` 卡住——这是有意选择的、有界的背压，
// 而不是完全消除慢 handler 的影响；handler 仍然应当尽量不阻塞。
func (publisher *Publisher) startPublishHandler() {
	publisher.handlerMu.Lock()
	if publisher.notifyPublishHandler == nil {
		publisher.handlerMu.Unlock()
		return
	}
	publisher.handlerMu.Unlock()

	if err := publisher.chanManager.ConfirmSafe(false); err != nil {
		publisher.options.Logger.Errorf("could not put channel in confirm mode: %v", err)
		return
	}

	go func() {
		const maxConcurrentHandlers = 100
		sem := make(chan struct{}, maxConcurrentHandlers)

		confirmationCh := publisher.chanManager.NotifyPublishSafe(make(chan amqp.Confirmation, 100))
		for conf := range confirmationCh {
			sem <- struct{}{}
			go func(c amqp.Confirmation) {
				defer func() { <-sem }()
				publisher.invokePublishHandler(c)
			}(conf)
		}
	}()
}

// invokePublishHandler 执行发布确认处理函数，通过 recover 保护防止 panic 影响整体。
func (publisher *Publisher) invokePublishHandler(c amqp.Confirmation) {
	defer func() {
		if r := recover(); r != nil {
			publisher.options.Logger.Errorf("panic in publish handler: %v", r)
		}
	}()
	publisher.notifyPublishHandler(Confirmation{
		Confirmation:      c,
		ReconnectionCount: int(publisher.chanManager.ReconnectionCount()),
	})
}
