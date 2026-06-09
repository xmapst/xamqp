package logger

// ILogger 定义 xamqp 内部组件的日志接口。
//
// 作为内部接口与外部日志实现解耦，
// 可通过 WithPublisherOptionsLogger() 或 WithConsumerOptionsLogger() 注入自定义实现。
// 默认使用 slog 标准库实现，生产环境建议注入与业务系统统一的日志实例。
type ILogger interface {
	Errorf(format string, args ...any)
	Warnf(format string, args ...any)
	Infof(format string, args ...any)
	Debugf(format string, args ...any)
}
