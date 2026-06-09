package xamqp

// ChannelOptions 通道配置选项，描述 Channel 的创建参数。
type ChannelOptions struct {
	Logger      ILogger // 自定义日志实现
	QOSPrefetch int     // 消息预取数量
	QOSGlobal   bool    // 是否对连接内所有通道全局生效
	ConfirmMode bool    // 是否启用发布确认模式
}

// getDefaultChannelOptions 返回通道选项的默认值。
func getDefaultChannelOptions() ChannelOptions {
	return ChannelOptions{
		Logger:      stdDebugLogger{},
		QOSPrefetch: 10,
		QOSGlobal:   false,
	}
}

// WithChannelOptionsQOSPrefetch 设置 QoS 预取数量。
//
// 预取数量决定服务器预先发送给消费者的最大未确认消息数，
// 值越大吞吐量越高，但单个消费者崩溃时丢失的未确认消息越多。
func WithChannelOptionsQOSPrefetch(prefetchCount int) func(*ChannelOptions) {
	return func(options *ChannelOptions) {
		options.QOSPrefetch = prefetchCount
	}
}

// WithChannelOptionsQOSGlobal 将 QoS 设置为连接级别全局生效。
//
// 全局 QoS 影响同一连接上所有现有和未来通道，
// 非全局模式（默认）仅影响当前通道。
func WithChannelOptionsQOSGlobal(options *ChannelOptions) {
	options.QOSGlobal = true
}

// WithChannelOptionsLogging 使用默认日志（输出到标准输出）。
func WithChannelOptionsLogging(options *ChannelOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithChannelOptionsLogger 设置自定义日志实现。
func WithChannelOptionsLogger(log ILogger) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.Logger = log
	}
}

// WithChannelOptionsConfirm 开启发布确认模式（Confirm Mode）。
//
// 开启后，每条发布的消息都需要 RabbitMQ 服务器确认，
// 可保证消息不丢失，但会降低发布吞吐量。
func WithChannelOptionsConfirm(options *ChannelOptions) {
	options.ConfirmMode = true
}
