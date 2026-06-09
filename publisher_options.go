package xamqp

import (
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherOptions 发布者配置选项，描述 Publisher 的创建参数。
type PublisherOptions struct {
	ExchangeOptions ExchangeOptions // 关联的交换机声明参数
	Logger          ILogger         // 自定义日志实现
	ConfirmMode     bool            // 是否启用发布确认模式
}

// getDefaultPublisherOptions 返回发布者选项的默认值。
func getDefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ExchangeOptions: ExchangeOptions{
			Name:       "",
			Kind:       amqp.ExchangeDirect,
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Passive:    false,
			Args:       make(amqp.Table),
			Declare:    false,
		},
		Logger:      stdDebugLogger{},
		ConfirmMode: false,
	}
}

// WithPublisherOptionsLogging 使用默认日志（输出到标准输出）。
func WithPublisherOptionsLogging(options *PublisherOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithPublisherOptionsLogger 设置自定义日志实现。
func WithPublisherOptionsLogger(log ILogger) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Logger = log
	}
}

// WithPublisherOptionsExchangeName 设置发布目标的交换机名称。
func WithPublisherOptionsExchangeName(name string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Name = name
	}
}

// WithPublisherOptionsExchangeKind 设置交换机类型（direct/topic/fanout/headers）。
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable 设置交换机为持久化。
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	options.ExchangeOptions.Durable = true
}

// WithPublisherOptionsExchangeAutoDelete 设置交换机为自动删除。
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	options.ExchangeOptions.AutoDelete = true
}

// WithPublisherOptionsExchangeInternal 设置交换机为内部交换机（不接受外部发布）。
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	options.ExchangeOptions.Internal = true
}

// WithPublisherOptionsExchangeNoWait 设置交换机声明不等待服务器确认。
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	options.ExchangeOptions.NoWait = true
}

// WithPublisherOptionsExchangeDeclare 设置在启动时声明交换机（不存在时自动创建）。
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	options.ExchangeOptions.Declare = true
}

// WithPublisherOptionsExchangePassive 设置交换机为被动模式（仅检查存在性，不创建）。
func WithPublisherOptionsExchangePassive(options *PublisherOptions) {
	options.ExchangeOptions.Passive = true
}

// WithPublisherOptionsExchangeArgs 追加交换机的额外参数。
func WithPublisherOptionsExchangeArgs(args amqp.Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		if options.ExchangeOptions.Args == nil {
			options.ExchangeOptions.Args = make(amqp.Table)
		}
		maps.Copy(options.ExchangeOptions.Args, args)
	}
}

// WithPublisherOptionsConfirm 开启发布确认模式。
//
// 开启后每条发布的消息都需要 RabbitMQ 服务器确认，
// 可保证消息成功持久化，适合对消息可靠性要求高的场景。
func WithPublisherOptionsConfirm(options *PublisherOptions) {
	options.ConfirmMode = true
}
