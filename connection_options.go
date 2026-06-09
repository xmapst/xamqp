package xamqp

import (
	"time"
)

// ConnectionOptions 连接配置选项，描述 RabbitMQ 连接的创建参数。
type ConnectionOptions struct {
	ReconnectInterval time.Duration // 断线重连的初始等待时间（指数退避的基础值）
	Logger            ILogger       // 自定义日志实现
	Config            Config        // AMQP 连接协商参数
}

// getDefaultConnectionOptions 返回连接选项的默认值。
func getDefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		ReconnectInterval: time.Second * 3,
		Logger:            stdDebugLogger{},
		Config:            Config{},
	}
}

// WithConnectionOptionsReconnectInterval 设置断线重连的等待间隔。
//
// 该值作为指数退避的初始值，每次重连失败后等待时间翻倍，最大 60 秒。
func WithConnectionOptionsReconnectInterval(interval time.Duration) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.ReconnectInterval = interval
	}
}

// WithConnectionOptionsLogging 使用默认日志（输出到标准输出）。
func WithConnectionOptionsLogging(options *ConnectionOptions) {
	options.Logger = stdDebugLogger{}
}

// WithConnectionOptionsLogger 设置自定义日志实现。
func WithConnectionOptionsLogger(log ILogger) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Logger = log
	}
}

// WithConnectionOptionsConfig 设置 AMQP 连接协商参数。
//
// 可配置帧大小（FrameSize）、心跳间隔（Heartbeat）等底层连接参数。
func WithConnectionOptionsConfig(cfg Config) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Config = cfg
	}
}
