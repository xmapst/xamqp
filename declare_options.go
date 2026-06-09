package xamqp

// DeclareOptions 资源声明器的配置选项。
type DeclareOptions struct {
	Logger      ILogger // 自定义日志实现
	ConfirmMode bool    // 是否启用发布确认模式（预留，目前声明器不使用）
}

// getDefaultDeclareOptions 返回声明器选项的默认值。
func getDefaultDeclareOptions() DeclareOptions {
	return DeclareOptions{
		Logger:      stdDebugLogger{},
		ConfirmMode: false,
	}
}

// WithDeclareOptionsLogging 使用默认日志（输出到标准输出）。
func WithDeclareOptionsLogging(options *DeclareOptions) {
	options.Logger = &stdDebugLogger{}
}

// WithDeclareOptionsLogger 设置自定义日志实现。
func WithDeclareOptionsLogger(log ILogger) func(options *DeclareOptions) {
	return func(options *DeclareOptions) {
		options.Logger = log
	}
}

// WithDeclareOptionsConfirm 开启发布确认模式。
func WithDeclareOptionsConfirm(options *DeclareOptions) {
	options.ConfirmMode = true
}
