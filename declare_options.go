package xamqp

// DeclareOptions 资源声明器的配置选项。
type DeclareOptions struct {
	ConfirmMode bool // 是否启用发布确认模式（预留，目前声明器不使用）
}

// getDefaultDeclareOptions 返回声明器选项的默认值。
func getDefaultDeclareOptions() DeclareOptions {
	return DeclareOptions{
		ConfirmMode: false,
	}
}

// WithDeclareOptionsConfirm 开启发布确认模式。
func WithDeclareOptionsConfirm(options *DeclareOptions) {
	options.ConfirmMode = true
}
