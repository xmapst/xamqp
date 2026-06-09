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
	*channel.Manager
}

// NewChannel 创建并初始化一个 AMQP 通道实例。
//
// 创建时立即应用 QoS 配置，若 QoS 设置失败则关闭通道并返回错误，
// 防止通道在非预期的 QoS 状态下被使用。
func NewChannel(conn *Conn, optionFuncs ...func(*ChannelOptions)) (*Channel, error) {
	options := new(getDefaultChannelOptions())
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}
	chanManager, err := channel.New(conn.connManager, options.Logger, conn.connManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	err = chanManager.QosSafe(options.QOSPrefetch, 0, options.QOSGlobal)
	if err != nil {
		_ = chanManager.Close()
		return nil, fmt.Errorf("declare qos failed: %w", err)
	}
	return &Channel{chanManager}, nil
}
