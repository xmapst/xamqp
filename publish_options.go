package xamqp

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishOptions 单次消息发布的配置选项，对应 AMQP Publishing 结构体的各字段。
type PublishOptions struct {
	Exchange string // 目标交换机名称

	// Mandatory=true 时若消息无法路由到任何队列，服务器通过 basic.return 返回给发布者
	Mandatory bool
	// Immediate=true 时若消费者不可立即接收消息，服务器通过 basic.return 返回给发布者
	// 注意：RabbitMQ 3.x 已废弃 Immediate 标志
	Immediate bool

	ContentType     string     // MIME 内容类型，如 "application/json"
	DeliveryMode    uint8      // 持久化模式：Transient(1) 或 Persistent(2)
	Expiration      string     // 消息 TTL（毫秒字符串），超时后丢弃或投递到死信队列
	ContentEncoding string     // MIME 内容编码，如 "utf-8"、"gzip"
	Priority        uint8      // 消息优先级（0-9），需队列启用优先级支持
	CorrelationID   string     // 关联标识符，通常用于 RPC 模式匹配请求与响应
	ReplyTo         string     // 回复目标队列名，用于 RPC 模式指定响应队列
	MessageID       string     // 消息唯一标识符，由发布者生成
	Timestamp       time.Time  // 消息创建时间戳
	Type            string     // 消息类型名称，由应用层定义语义
	UserID          string     // 发布者用户 ID（如 "guest"），服务器可验证）
	AppID           string     // 发布应用 ID，用于标识消息来源
	Headers         amqp.Table // 应用层扩展头信息，headers 交换机会检查此字段进行路由
}

// WithPublishOptionsExchange 设置发布目标的交换机名称。
func WithPublishOptionsExchange(exchange string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Exchange = exchange
	}
}

// WithPublishOptionsMandatory 设置 mandatory 标志，无路由队列时通过 returns 通道返回消息。
func WithPublishOptionsMandatory(options *PublishOptions) {
	options.Mandatory = true
}

// WithPublishOptionsImmediate 设置 immediate 标志，无可用消费者时通过 returns 通道返回消息。
func WithPublishOptionsImmediate(options *PublishOptions) {
	options.Immediate = true
}

// WithPublishOptionsContentType 设置消息的 MIME 内容类型，如 "application/json"。
func WithPublishOptionsContentType(contentType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentType = contentType
	}
}

// WithPublishOptionsPersistentDelivery 设置消息为持久化投递模式。
//
// 持久化消息会写入磁盘，Broker 重启后可恢复。
// 非持久化消息（默认）存储在内存中，性能更好但重启后丢失。
// 注意：消息持久化需配合队列持久化使用才能真正保证不丢失。
func WithPublishOptionsPersistentDelivery(options *PublishOptions) {
	options.DeliveryMode = Persistent
}

// WithPublishOptionsExpiration 设置消息过期时间（毫秒字符串）。
//
// 消息在队列中等待超过指定时间后自动丢弃（或路由到死信队列）。
// 参考：https://www.rabbitmq.com/ttl.html#per-message-ttl-in-publishers
func WithPublishOptionsExpiration(expiration string) func(options *PublishOptions) {
	return func(options *PublishOptions) {
		options.Expiration = expiration
	}
}

// WithPublishOptionsHeaders 设置消息头信息，headers 交换机基于此进行路由。
func WithPublishOptionsHeaders(headers amqp.Table) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Headers = headers
	}
}

// WithPublishOptionsContentEncoding 设置内容编码，如 "utf-8"、"gzip"。
func WithPublishOptionsContentEncoding(contentEncoding string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentEncoding = contentEncoding
	}
}

// WithPublishOptionsPriority 设置消息优先级（0-9）。
//
// 高优先级消息在队列中排在低优先级消息前面，需队列声明时启用 x-max-priority 参数。
func WithPublishOptionsPriority(priority uint8) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Priority = priority
	}
}

// WithPublishOptionsCorrelationID 设置关联标识符，用于 RPC 模式匹配请求与响应。
func WithPublishOptionsCorrelationID(correlationID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.CorrelationID = correlationID
	}
}

// WithPublishOptionsReplyTo 设置响应队列名，用于 RPC 模式指定回复地址。
func WithPublishOptionsReplyTo(replyTo string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ReplyTo = replyTo
	}
}

// WithPublishOptionsMessageID 设置消息唯一标识符。
func WithPublishOptionsMessageID(messageID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.MessageID = messageID
	}
}

// WithPublishOptionsTimestamp 设置消息的创建时间戳。
func WithPublishOptionsTimestamp(timestamp time.Time) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Timestamp = timestamp
	}
}

// WithPublishOptionsType 设置消息类型名称（应用层语义，由业务代码定义）。
func WithPublishOptionsType(messageType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Type = messageType
	}
}

// WithPublishOptionsUserID 设置发布者用户 ID（RabbitMQ 可配置为验证此字段）。
func WithPublishOptionsUserID(userID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.UserID = userID
	}
}

// WithPublishOptionsAppID 设置发布应用的标识符，用于追踪消息来源。
func WithPublishOptionsAppID(appID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.AppID = appID
	}
}
