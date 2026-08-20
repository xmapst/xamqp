package xamqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
//   - notifyReturnHandler/notifyPublishHandler 通过 handlerMu 保护注册和读取的并发安全
//     （invokeReturnHandler/invokePublishHandler 读取时也要加锁，不能只在注册时加锁）
//   - returnHandlerGeneration/publishHandlerGeneration 同样受 handlerMu 保护，
//     用于给 start*Handler 去重：NotifyReturn/NotifyPublish 与重连成功后的
//     自动重启都会调用 start*Handler，没有这层去重会在同一通道上重复注册
//     监听协程，导致每个事件被投递两次
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

	handlerMu                *sync.Mutex          // 保护 notifyReturnHandler/notifyPublishHandler 及下面两个"代"号字段的并发安全
	notifyReturnHandler      func(r Return)       // NotifyReturn 注册的 basic.return 处理函数，nil 表示未注册
	notifyPublishHandler     func(p Confirmation) // NotifyPublish 注册的发布确认处理函数，nil 表示未注册（未开启 confirm 模式）
	returnHandlerGeneration  int                  // 已成功为其启动过监听协程的通道"代"号（即当时的 chanManager.ReconnectionCount()），-1 表示尚未启动过
	publishHandlerGeneration int                  // 同上，用于确认事件监听协程

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
		returnHandlerGeneration:      -1,
		publishHandlerGeneration:     -1,
		options:                      *options,
	}
	var err error
	publisher.chanManager, err = channel.New(conn.connManager, conn.connManager.ReconnectInterval)
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
			slog.Info("successful publisher recovery", slog.Any("error", reconnectErr))
			for {
				if publisher.getIsClosed() {
					return
				}
				if err := publisher.startup(); err != nil {
					slog.Error("error on startup for publisher after cancel or close", slog.Any("error", err))
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
			slog.Warn("error while closing the channel", slog.Any("error", err))
		}
		slog.Info("closing publisher...")
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
	publisher.notifyReturnHandler = handler
	publisher.handlerMu.Unlock()

	// 无条件调用：是否真的需要启动一个新的监听协程，完全交给
	// startReturnHandler 内部基于"代"号的原子判断，而不是在这里自己算一次
	// "要不要启动"再重复判断一次——两次独立判断之间没有互斥，正是曾经
	// 导致同一通道被重复注册两个监听协程、每个 basic.return 被投递两次的
	// 根源（该竞态与重连后自动重启监听协程的后台协程之间产生）。
	publisher.startReturnHandler()
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
	publisher.notifyPublishHandler = handler
	publisher.handlerMu.Unlock()

	// 无条件调用，理由同 NotifyReturn：去重判断完全交给 startPublishHandler
	// 内部基于"代"号的原子判断。
	publisher.startPublishHandler()
}

// startReturnHandler 启动 basic.return 事件监听 goroutine。
//
// 为每个事件单独起一个 goroutine 并发调用 handler：读取事件的这个循环
// 本身只负责尽快把事件从 channel 里取走再转手扔给新协程处理，不会被
// 慢 handler 拖住——这个循环的读取速度直接决定 amqp091-go 共享的连接
// 读取协程会不会被写阻塞（详见 NotifyReturn 文档），所以这里不能改成
// 同步调用。代价是不保证多个 basic.return 之间的处理顺序。
//
// 去重：本方法可能被两条独立路径并发调用——用户直接调用 NotifyReturn，
// 以及重连成功后 NewPublisher 里的后台协程自动重新调用一遍。两者之间
// 没有互斥，若各自都认为"当前还没有监听协程、我来启动"，会各自在同一个
// 通道上调用一次 NotifyReturnSafe，注册两个独立的监听者——amqp091-go
// 的 NotifyReturn 支持多个并发监听者、且会把每个事件广播给所有监听者，
// 不会去重，于是每个 basic.return 会被投递并处理两次。
//
// 用 returnHandlerGeneration（当时的 chanManager.ReconnectionCount()）
// 而不是简单的布尔标志来去重：布尔标志无法区分"这一代通道已经启动过
// 监听协程，不用再启动"和"通道已经变成下一代了，需要为新通道重新启动"，
// 用重连计数作为代号可以天然地把这两种情况分开，且不依赖旧监听协程
// 何时真正退出这种时序细节。
//
// 关键：判断代号和真正注册监听必须在同一个 channelMu 读锁临界区内原子完成
// （WithChannelForNewGeneration），不能"先取代号、解锁、再由新协程去注册"——
// 后者中间可以插入一次完整的重连，注册会落到新通道上却记着旧代号，随后
// 按新代号判断的调用方会再注册一次，同一个通道上就有了两个监听者，事件
// 被处理两次。外层的 handlerMu 负责把并发的多个调用方串行化，内层的
// channelMu 读锁负责挡住重连，两者合起来才能真正保证"每一代通道最多一个
// 监听协程"。
func (publisher *Publisher) startReturnHandler() {
	publisher.handlerMu.Lock()
	defer publisher.handlerMu.Unlock()

	if publisher.notifyReturnHandler == nil {
		return
	}

	var returns chan amqp.Return
	publisher.chanManager.WithChannelForNewGeneration(func(ch *amqp.Channel, generation uint) {
		if publisher.returnHandlerGeneration == int(generation) {
			return // 这一代通道已经有监听协程了，不重复注册
		}
		publisher.returnHandlerGeneration = int(generation)
		returns = ch.NotifyReturn(make(chan amqp.Return, 100))
	})
	if returns == nil {
		return
	}

	go func() {
		for ret := range returns {
			go publisher.invokeReturnHandler(Return{ret})
		}
	}()
}

// invokeReturnHandler 执行 basic.return 处理函数，通过 recover 保护防止 panic 影响整体。
//
// 在 handlerMu 保护下把 handler 函数值拷贝出来再解锁调用：notifyReturnHandler
// 只在 NotifyReturn 里加锁写入，若在这里不加锁直接读字段，与那次写入之间
// 就是未同步的并发读写（数据竞争，go test -race 会报出来），即使实践中
// 一次函数指针赋值很少真的产生撕裂读，也不应该依赖这种运气。不在持锁期间
// 调用 handler 本身，避免慢 handler 连带卡住并发的 NotifyReturn 调用。
func (publisher *Publisher) invokeReturnHandler(r Return) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("panic in return handler", slog.Any("panic", rec))
		}
	}()
	publisher.handlerMu.Lock()
	handler := publisher.notifyReturnHandler
	publisher.handlerMu.Unlock()
	if handler != nil {
		handler(r)
	}
}

// startPublishHandler 开启确认模式并启动确认事件处理 goroutine。
//
// 用信号量把并发处理的确认事件数量限制在 maxConcurrentHandlers，
// 避免海量确认事件瞬间涌入时无限制地起协程；理由同 startReturnHandler，
// 读取循环本身仍然只负责尽快转手，不同步调用 handler。注意信号量本身
// 意味着一旦 maxConcurrentHandlers 个 handler 同时卡住，第 101 个事件
// 的转手会被 `sem <- struct{}{}` 卡住——这是有意选择的、有界的背压，
// 而不是完全消除慢 handler 的影响；handler 仍然应当尽量不阻塞。
//
// 去重：与 startReturnHandler 同样的问题和同样的修复——本方法可能被
// NotifyPublish 和重连成功后的自动重启并发调用，用 publishHandlerGeneration
// 配合 WithChannelForNewGeneration 的原子"判断代号 + 注册"，保证每一代通道
// 最多只成功启动一次监听协程。
//
// ConfirmSafe 刻意放在 handlerMu 之外、且在原子注册之前：它取的是 channelMu
// 写锁，绝不能在 WithChannelForNewGeneration 的读锁回调里调用（RWMutex 会
// 自锁）。相应地，代号是在 ConfirmSafe 成功之后才被标记消耗的，所以这一代
// 的 ConfirmSafe 失败不会把该代号"用掉"，下一次调用仍然会重试——比之前
// 先标记再 ConfirmSafe 的写法更不容易静默丢失确认能力。
func (publisher *Publisher) startPublishHandler() {
	publisher.handlerMu.Lock()
	registered := publisher.notifyPublishHandler != nil
	publisher.handlerMu.Unlock()
	if !registered {
		return
	}

	if err := publisher.chanManager.ConfirmSafe(false); err != nil {
		slog.Error("could not put channel in confirm mode", slog.Any("error", err))
		return
	}

	publisher.handlerMu.Lock()
	defer publisher.handlerMu.Unlock()

	// 上面短暂放开过 handlerMu，期间 handler 有可能被并发改动，重新确认一次。
	if publisher.notifyPublishHandler == nil {
		return
	}

	var confirmationCh chan amqp.Confirmation
	publisher.chanManager.WithChannelForNewGeneration(func(ch *amqp.Channel, generation uint) {
		if publisher.publishHandlerGeneration == int(generation) {
			return // 这一代通道已经有监听协程了，不重复注册
		}
		publisher.publishHandlerGeneration = int(generation)
		confirmationCh = ch.NotifyPublish(make(chan amqp.Confirmation, 100))
	})
	if confirmationCh == nil {
		return
	}

	go func() {
		const maxConcurrentHandlers = 100
		sem := make(chan struct{}, maxConcurrentHandlers)

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
//
// 加锁读取 notifyPublishHandler 的理由同 invokeReturnHandler。
func (publisher *Publisher) invokePublishHandler(c amqp.Confirmation) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in publish handler", slog.Any("panic", r))
		}
	}()
	publisher.handlerMu.Lock()
	handler := publisher.notifyPublishHandler
	publisher.handlerMu.Unlock()
	if handler == nil {
		return
	}
	handler(Confirmation{
		Confirmation:      c,
		ReconnectionCount: int(publisher.chanManager.ReconnectionCount()),
	})
}
