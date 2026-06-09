package xamqp

import (
	"math/rand/v2"
	"slices"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/manager/connection"
)

// Conn 管理与 RabbitMQ 集群的连接，可在 Publisher 和 Consumer 之间共享。
//
// 封装了底层连接管理器，自动处理断线重连，
// 调用方无需感知连接中断，只需在使用 Publisher/Consumer 时处理具体操作的错误。
type Conn struct {
	connManager                *connection.Manager
	reconnectErrCh             <-chan error
	closeConnectionToManagerCh chan<- struct{}

	options ConnectionOptions
}

// Config 封装 amqp.Config，用于连接建立时协商连接参数。
//
// 协商参数包括帧大小、心跳间隔、通道数量上限等，
// 通过 DialConfig 或 Open 方法传递给 RabbitMQ 服务器。
type Config amqp.Config

// IResolver 连接地址解析器接口，允许自定义连接目标的选择策略。
type IResolver = connection.IResolver

// StaticResolver 静态地址列表解析器，支持固定地址集合和随机打乱顺序。
//
// shuffle=true 时每次解析随机打乱地址顺序，
// 实现简单的客户端负载均衡（每次连接随机选择不同节点）。
type StaticResolver struct {
	urls    []string
	shuffle bool
}

// Resolve 返回可用的连接地址列表，shuffle=true 时随机打乱顺序。
func (r *StaticResolver) Resolve() ([]string, error) {
	var urls = slices.Clone(r.urls) // 拷贝避免修改原切片
	if r.shuffle {
		rand.Shuffle(len(urls), func(i, j int) {
			urls[i], urls[j] = urls[j], urls[i]
		})
	}
	return urls, nil
}

// NewStaticResolver 创建静态地址解析器。
func NewStaticResolver(urls []string, shuffle bool) *StaticResolver {
	return &StaticResolver{urls: urls, shuffle: shuffle}
}

// NewConn 使用单个 URL 创建 RabbitMQ 连接管理器。
func NewConn(url string, opts ...func(*ConnectionOptions)) (*Conn, error) {
	return NewClusterConn(NewStaticResolver([]string{url}, false), opts...)
}

// NewClusterConn 使用自定义地址解析器创建 RabbitMQ 集群连接管理器。
//
// 支持连接到 RabbitMQ 集群的多个节点，通过 IResolver 实现节点选择策略（轮询、随机等）。
// 后台 goroutine 监听重连事件并记录日志，调用方无感知自动恢复。
func NewClusterConn(resolver IResolver, opts ...func(*ConnectionOptions)) (*Conn, error) {
	options := new(getDefaultConnectionOptions())
	for _, optFn := range opts {
		optFn(options)
	}

	conn := &Conn{
		options: *options,
	}
	var err error
	conn.connManager, err = connection.New(resolver, amqp.Config(options.Config), options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	conn.reconnectErrCh, conn.closeConnectionToManagerCh = conn.connManager.NotifyReconnect()
	go conn.handleRestarts() // 后台监听重连事件，记录恢复日志
	return conn, nil
}

// handleRestarts 监听连接重建事件并记录日志。
func (conn *Conn) handleRestarts() {
	for err := range conn.reconnectErrCh {
		conn.options.Logger.Infof("successful connection recovery from: %v", err)
	}
}

// Close 关闭连接，释放所有资源。
//
// 关闭前应先关闭所有基于此连接创建的 Consumer 和 Publisher，
// 否则可能导致这些组件的操作返回意外错误。
// 关闭后此 Conn 实例不可复用。
func (conn *Conn) Close() error {
	conn.closeConnectionToManagerCh <- struct{}{}
	return conn.connManager.Close()
}

// IsClosed 返回连接是否已关闭。
func (conn *Conn) IsClosed() bool {
	return conn.connManager.IsClosed()
}
