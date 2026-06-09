package xamqp

import (
	"maps"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// getDefaultConsumerOptions 返回消费者选项的默认值。
func getDefaultConsumerOptions(queueName string) ConsumerOptions {
	return ConsumerOptions{
		RabbitConsumerOptions: RabbitConsumerOptions{
			Name:      "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
			Args:      make(amqp.Table),
		},
		QueueOptions: QueueOptions{
			Name:       queueName,
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Passive:    false,
			Args:       make(amqp.Table),
			Declare:    true,
		},
		ExchangeOptions: []ExchangeOptions{},
		Concurrency:     1,
		CloseGracefully: true,
		Logger:          stdDebugLogger{},
		QOSPrefetch:     10,
		QOSGlobal:       false,
	}
}

// getDefaultExchangeOptions 返回交换机选项的默认值。
func getDefaultExchangeOptions() ExchangeOptions {
	return ExchangeOptions{
		Name:       "",
		Kind:       amqp.ExchangeDirect,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Passive:    false,
		Args:       make(amqp.Table),
		Declare:    false,
		Bindings:   []Binding{},
	}
}

// getDefaultBindingOptions 返回绑定选项的默认值。
func getDefaultBindingOptions() BindingOptions {
	return BindingOptions{
		NoWait:  false,
		Args:    make(amqp.Table),
		Declare: true,
	}
}

// ConsumerOptions 消费者配置选项，描述消费者的创建参数。
//
// 如果 QueueOptions.Declare=true，将在启动时声明队列；
// 如果 ExchangeOptions 不为空，将声明对应的交换机；
// 如果存在 Bindings，队列将被绑定到这些交换机上。
type ConsumerOptions struct {
	RabbitConsumerOptions RabbitConsumerOptions // RabbitMQ 原生消费者参数
	QueueOptions          QueueOptions          // 队列声明参数
	CloseGracefully       bool                  // 关闭时是否等待当前消息处理完成
	ExchangeOptions       []ExchangeOptions     // 关联的交换机声明参数列表
	Concurrency           int                   // 并发消费 goroutine 数量
	Logger                ILogger               // 自定义日志实现
	QOSPrefetch           int                   // QoS 预取数量
	QOSGlobal             bool                  // QoS 是否全局生效
}

// RabbitConsumerOptions RabbitMQ 服务端消费者参数，直接传递给 AMQP 协议。
type RabbitConsumerOptions struct {
	Name      string     // 消费者标识名，空字符串时服务器自动生成唯一名
	AutoAck   bool       // 自动确认：收到消息后自动 Ack，不等待处理完成
	Exclusive bool       // 独占消费：确保此消费者是队列的唯一消费者
	NoWait    bool       // 不等待服务器确认
	NoLocal   bool       // RabbitMQ 不支持此标志，固定为 false
	Args      amqp.Table // 额外参数
}

// QueueOptions 队列配置选项。
//
// Passive=true 时假定队列已存在，若不存在则 RabbitMQ 返回异常；
// Passive=false 时若队列不存在则自动创建。
type QueueOptions struct {
	Name       string     // 队列名称
	Durable    bool       // 持久化：RabbitMQ 重启后队列依然存在
	AutoDelete bool       // 自动删除：最后一个消费者断开后自动删除队列
	Exclusive  bool       // 独占队列：只有创建它的连接可使用，连接关闭时自动删除
	NoWait     bool       // 不等待服务器确认
	Passive    bool       // 被动模式：若不存在则报错而非创建
	Args       amqp.Table // 额外参数（如 x-message-ttl、x-dead-letter-exchange 等）
	Declare    bool       // 是否在启动时声明
}

// Binding 描述队列到交换机的路由键绑定关系。
type Binding struct {
	Source      string // 源交换机名称
	Destination string // 目标队列名称
	RoutingKey  string // 路由键
	BindingOptions
}

// BindingOptions 绑定配置选项。
type BindingOptions struct {
	NoWait  bool       // 不等待服务器确认
	Args    amqp.Table // 额外参数
	Declare bool       // 是否在启动时声明此绑定
}

// WithConsumerOptionsQueueDurable 设置队列为持久化队列（RabbitMQ 重启后保留）。
func WithConsumerOptionsQueueDurable(options *ConsumerOptions) {
	options.QueueOptions.Durable = true
}

// WithConsumerOptionsQueueAutoDelete 设置队列为自动删除队列（最后一个消费者断开后删除）。
func WithConsumerOptionsQueueAutoDelete(options *ConsumerOptions) {
	options.QueueOptions.AutoDelete = true
}

// WithConsumerOptionsQueueExpires 设置队列的存活时间（TTL）。
//
// 队列在指定时间内无消费者连接时自动删除。
// 过期队列必须是持久化队列，否则 RabbitMQ 会拒绝声明。
func WithConsumerOptionsQueueExpires(expire time.Duration) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		if options.QueueOptions.Args == nil {
			options.QueueOptions.Args = make(amqp.Table)
		}
		options.QueueOptions.Durable = true                            // 过期队列必须持久化
		options.QueueOptions.Args["x-expires"] = expire.Milliseconds() // 单位：毫秒
	}
}

// WithConsumerOptionsQueueExclusive 设置队列为独占队列（仅当前连接可使用）。
func WithConsumerOptionsQueueExclusive(options *ConsumerOptions) {
	options.QueueOptions.Exclusive = true
}

// WithConsumerOptionsQueueNoWait 设置队列声明操作不等待服务器确认。
func WithConsumerOptionsQueueNoWait(options *ConsumerOptions) {
	options.QueueOptions.NoWait = true
}

// WithConsumerOptionsQueuePassive 设置队列为被动模式（仅检查存在性，不创建）。
func WithConsumerOptionsQueuePassive(options *ConsumerOptions) {
	options.QueueOptions.Passive = true
}

// WithConsumerOptionsQueueNoDeclare 禁止在启动时声明队列（连接到已存在的队列）。
func WithConsumerOptionsQueueNoDeclare(options *ConsumerOptions) {
	options.QueueOptions.Declare = false
}

// WithConsumerOptionsQueueArgs 追加队列声明的额外参数。
func WithConsumerOptionsQueueArgs(args amqp.Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		if options.QueueOptions.Args == nil {
			options.QueueOptions.Args = make(amqp.Table)
		}
		maps.Copy(options.QueueOptions.Args, args)
	}
}

// ensureExchangeOptions 确保 ExchangeOptions 列表至少有一个元素（懒初始化）。
func ensureExchangeOptions(options *ConsumerOptions) {
	if len(options.ExchangeOptions) == 0 {
		options.ExchangeOptions = append(options.ExchangeOptions, getDefaultExchangeOptions())
	}
}

// WithConsumerOptionsExchangeName 设置默认交换机名称。
func WithConsumerOptionsExchangeName(name string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Name = name
	}
}

// WithConsumerOptionsExchangeKind 设置默认交换机类型（direct/topic/fanout/headers）。
func WithConsumerOptionsExchangeKind(kind string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Kind = kind
	}
}

// WithConsumerOptionsExchangeDurable 设置默认交换机为持久化。
func WithConsumerOptionsExchangeDurable(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Durable = true
}

// WithConsumerOptionsExchangeAutoDelete 设置默认交换机为自动删除。
func WithConsumerOptionsExchangeAutoDelete(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].AutoDelete = true
}

// WithConsumerOptionsExchangeInternal 设置默认交换机为内部交换机。
func WithConsumerOptionsExchangeInternal(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Internal = true
}

// WithConsumerOptionsExchangeNoWait 设置默认交换机声明不等待服务器确认。
func WithConsumerOptionsExchangeNoWait(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].NoWait = true
}

// WithConsumerOptionsExchangeDeclare 设置在启动时声明默认交换机。
func WithConsumerOptionsExchangeDeclare(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Declare = true
}

// WithConsumerOptionsExchangePassive 设置默认交换机为被动模式（仅检查存在性）。
func WithConsumerOptionsExchangePassive(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Passive = true
}

// WithConsumerOptionsExchangeArgs 追加默认交换机的额外参数。
func WithConsumerOptionsExchangeArgs(args amqp.Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		if options.ExchangeOptions[0].Args == nil {
			options.ExchangeOptions[0].Args = make(amqp.Table)
		}
		maps.Copy(options.ExchangeOptions[0].Args, args)
	}
}

// WithConsumerOptionsRoutingKey 使用默认绑定选项将队列绑定到指定路由键。
func WithConsumerOptionsRoutingKey(routingKey string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, Binding{
			RoutingKey:     routingKey,
			BindingOptions: getDefaultBindingOptions(),
		})
	}
}

// WithConsumerOptionsBinding 使用自定义绑定选项添加队列绑定。
//
// 比 WithConsumerOptionsRoutingKey 更灵活，允许自定义 NoWait、Args、Declare 等选项。
func WithConsumerOptionsBinding(binding Binding) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, binding)
	}
}

// WithConsumerOptionsExchangeOptions 添加额外的交换机配置，支持同一消费者监听多个交换机。
func WithConsumerOptionsExchangeOptions(exchangeOptions ExchangeOptions) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.ExchangeOptions = append(options.ExchangeOptions, exchangeOptions)
	}
}

// WithConsumerOptionsConcurrency 设置并发消费 goroutine 数量。
//
// 更多的并发 goroutine 可提升吞吐量，但会增加消息乱序风险。
func WithConsumerOptionsConcurrency(concurrency int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Concurrency = concurrency
	}
}

// WithConsumerOptionsConsumerName 设置服务端消费者标识名。
//
// 未设置时 RabbitMQ 自动生成随机名称。自定义名称便于在管理控制台中识别消费者。
func WithConsumerOptionsConsumerName(consumerName string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.RabbitConsumerOptions.Name = consumerName
	}
}

// WithConsumerOptionsConsumerAutoAck 设置消息自动确认模式。
//
// 自动确认时消息一经投递即视为已消费，无需调用 Ack()，
// 但若处理失败无法重试，适合幂等或可丢弃的消息场景。
func WithConsumerOptionsConsumerAutoAck(autoAck bool) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.RabbitConsumerOptions.AutoAck = autoAck
	}
}

// WithConsumerOptionsConsumerExclusive 设置消费者为独占模式。
//
// 独占模式确保此消费者是队列的唯一消费者，
// 其他消费者尝试连接同一队列时会收到错误。
func WithConsumerOptionsConsumerExclusive(options *ConsumerOptions) {
	options.RabbitConsumerOptions.Exclusive = true
}

// WithConsumerOptionsConsumerNoWait 设置消费者订阅不等待服务器确认。
//
// 不等待时立即开始接收消息，若服务器无法满足请求则通道关闭并返回错误。
func WithConsumerOptionsConsumerNoWait(options *ConsumerOptions) {
	options.RabbitConsumerOptions.NoWait = true
}

// WithConsumerOptionsLogging 使用默认日志（输出到标准输出）。
func WithConsumerOptionsLogging(options *ConsumerOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithConsumerOptionsLogger 设置自定义日志实现。
func WithConsumerOptionsLogger(log ILogger) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Logger = log
	}
}

// WithConsumerOptionsQOSPrefetch 设置 QoS 预取数量。
//
// 控制服务器预先发送给消费者的最大未确认消息数，
// 不影响处理并发数（消息仍按序处理），但影响吞吐量。
func WithConsumerOptionsQOSPrefetch(prefetchCount int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithConsumerOptionsQOSGlobal 将 QoS 设置为连接级别全局生效。
func WithConsumerOptionsQOSGlobal(options *ConsumerOptions) {
	options.QOSGlobal = true
}

// WithConsumerOptionsForceShutdown 设置消费者关闭时不等待当前消息处理完成。
//
// 默认（CloseGracefully=true）会等待所有正在处理中的消息完成后再关闭，
// 此选项将其设为 false，适合需要快速关闭的场景。
func WithConsumerOptionsForceShutdown(options *ConsumerOptions) {
	options.CloseGracefully = false
}

// WithConsumerOptionsQueueQuorum 设置队列为仲裁队列（Quorum Queue）。
//
// 仲裁队列在集群多个节点之间复制消息，提供更高的可靠性和数据安全性，
// 相比经典镜像队列有更好的性能和一致性保证。
func WithConsumerOptionsQueueQuorum(options *ConsumerOptions) {
	if options.QueueOptions.Args == nil {
		options.QueueOptions.Args = make(amqp.Table)
	}

	options.QueueOptions.Args["x-queue-type"] = "quorum"
}

// WithConsumerOptionsQueueMessageExpiration 设置队列级别的消息 TTL。
//
// 消息在队列中等待超过指定时间后自动丢弃（或路由到死信队列）。
// 与单条消息的 Expiration 不同，此配置对队列内所有消息生效。
// 参考：https://www.rabbitmq.com/docs/ttl#per-queue-message-ttl
func WithConsumerOptionsQueueMessageExpiration(ttl time.Duration) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		if options.QueueOptions.Args == nil {
			options.QueueOptions.Args = make(amqp.Table)
		}
		options.QueueOptions.Args["x-message-ttl"] = ttl.Milliseconds()
	}
}

// WithConsumerStreamOffset 设置 Stream 队列的消费偏移量。
//
// 用于 RabbitMQ Streams 功能，允许消费者从指定位置开始消费历史消息。
func WithConsumerStreamOffset(offset any) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.RabbitConsumerOptions.Args["x-stream-offset"] = offset
	}
}
