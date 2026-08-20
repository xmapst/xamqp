package xamqp

import (
	"errors"
	"fmt"

	"github.com/xmapst/xamqp/internal/manager/channel"
)

// Channel 对外暴露底层通道管理器的所有操作，可直接使用 AMQP 原语。
//
// 相比 Publisher/Consumer 的高层封装，Channel 提供更底层的控制能力，
// 适合需要直接操作 AMQP 通道的高级场景（如自定义消费模式、特殊声明逻辑等）。
type Channel struct {
	*channel.Manager // 内嵌通道管理器，直接暴露其所有 Safe 方法供调用方使用
}

// NewChannel 创建并初始化一个 AMQP 通道实例。
//
// 创建时立即应用 QoS 配置，若开启 ChannelOptions.ConfirmMode 则同时
// 进入发布确认模式；任一步失败都会关闭通道并返回错误，
// 防止通道在非预期的 QoS/确认状态下被使用。
func NewChannel(conn *Conn, optionFuncs ...func(*ChannelOptions)) (*Channel, error) {
	options := new(getDefaultChannelOptions())
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}
	chanManager, err := channel.New(conn.connManager, conn.connManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	err = chanManager.QosSafe(options.QOSPrefetch, 0, options.QOSGlobal)
	if err != nil {
		_ = chanManager.Close()
		return nil, fmt.Errorf("declare qos failed: %w", err)
	}
	if options.ConfirmMode {
		if err = chanManager.ConfirmSafe(false); err != nil {
			_ = chanManager.Close()
			return nil, fmt.Errorf("declare confirm mode failed: %w", err)
		}
	}
	return &Channel{chanManager}, nil
}
