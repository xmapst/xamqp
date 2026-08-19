package xamqp

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/manager/connection"
)

// Conn 管理与 RabbitMQ 集群的连接，可在 Publisher 和 Consumer 之间共享。
//
// 封装了底层连接管理器，自动处理断线重连，
// 调用方无需感知连接中断，只需在使用 Publisher/Consumer 时处理具体操作的错误。
type Conn struct {
	connManager                *connection.Manager // 底层连接管理器，负责实际的连接建立/重连
	reconnectErrCh             <-chan error        // 订阅自 connManager 的重连事件通道，仅用于记录恢复日志
	closeConnectionToManagerCh chan<- struct{}     // 取消订阅信号：Close() 时写入，通知 connManager 停止推送重连事件
	closeOnce                  sync.Once           // 保证 Close() 幂等，避免重复调用时永久阻塞

	options ConnectionOptions // 创建时传入的连接选项快照
}

// Config 封装 amqp.Config，用于连接建立时协商连接参数。
//
// 协商参数包括帧大小、心跳间隔、通道数量上限等，
// 通过 DialConfig 或 Open 方法传递给 RabbitMQ 服务器。
type Config amqp.Config

// NewConn 使用给定地址创建 RabbitMQ 连接管理器。
//
// 只接受单个地址：生产环境的 RabbitMQ 集群通常在前面有负载均衡（HAProxy、
// LVS、云厂商 LB 等），对外只暴露一个入口地址，节点故障转移由负载均衡层
// 负责，无需 xamqp 自己实现多节点选择/轮询。
// 后台 goroutine 监听重连事件并记录日志，调用方无感知自动恢复。
func NewConn(url string, opts ...func(*ConnectionOptions)) (*Conn, error) {
	options := new(getDefaultConnectionOptions())
	for _, optFn := range opts {
		optFn(options)
	}

	conn := &Conn{
		options: *options,
	}
	var err error
	conn.connManager, err = connection.New(url, amqp.Config(options.Config), options.Logger, options.ReconnectInterval)
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
//
// 幂等：重复调用是安全的，第二次及以后的调用直接返回 nil。
// 若不做此保护，向 closeConnectionToManagerCh 的同步发送在第二次调用时
// 将没有接收者（dispatcher 的清理协程只接收一次），调用方会永久阻塞。
func (conn *Conn) Close() error {
	var err error
	conn.closeOnce.Do(func() {
		conn.closeConnectionToManagerCh <- struct{}{}
		err = conn.connManager.Close()
	})
	return err
}

// IsClosed 返回连接是否已关闭。
func (conn *Conn) IsClosed() bool {
	return conn.connManager.IsClosed()
}
