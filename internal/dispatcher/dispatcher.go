package dispatcher

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Dispatcher 事件广播器，实现一对多的异步通知机制。
//
// 用于将 RabbitMQ 连接/通道的重连事件广播给所有订阅者（Publisher/Consumer），
// 每个订阅者独立接收相同的事件，互不干扰。
//
// 并发安全：subscribersMu 保护订阅者 map 的并发读写，
// subscriberIDCounter 使用原子操作生成唯一 ID，避免加锁。
type Dispatcher struct {
	subscribers         map[int]dispatchSubscriber
	subscribersMu       *sync.Mutex
	subscriberIDCounter atomic.Int64
}

// dispatchSubscriber 单个订阅者的通道对，实现发布和取消订阅。
type dispatchSubscriber struct {
	notifyCancelOrCloseChan chan error      // 事件通知通道，接收重连错误
	closeCh                 <-chan struct{} // 关闭信号通道，订阅者关闭时写入
}

// New 创建事件广播器实例。
func New() *Dispatcher {
	return &Dispatcher{
		subscribers:   make(map[int]dispatchSubscriber),
		subscribersMu: &sync.Mutex{},
	}
}

// Dispatch 向所有当前订阅者广播事件（错误信息）。
//
// 使用 5 秒超时防止单个订阅者接收缓慢阻塞整个广播：
// 若某个订阅者 5 秒内未读取事件，则跳过该订阅者并记录警告。
// 广播是同步串行的（依次发送给每个订阅者），适合低频事件（如重连）场景。
func (d *Dispatcher) Dispatch(err error) error {
	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()
	for _, subscriber := range d.subscribers {
		select {
		case <-time.After(time.Second * 5):
			slog.Warn("Unexpected rabbitmq error: timeout in dispatch")
		case subscriber.notifyCancelOrCloseChan <- err:
		}
	}
	return nil
}

// AddSubscriber 添加新订阅者，返回事件接收通道和关闭信号通道。
//
// 返回值：
//   - <-chan error：接收重连事件的只读通道（通道关闭表示订阅已结束）
//   - chan<- struct{}：关闭信号发送通道（向此通道写入即可取消订阅）
//
// 取消订阅机制：后台 goroutine 监听 closeCh，
// 收到信号后关闭事件通道并从 subscribers map 中删除，
// 防止已停止的组件在后续 Dispatch 时阻塞广播流程。
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}) {
	id := int(d.subscriberIDCounter.Add(1))

	closeCh := make(chan struct{})
	notifyCancelOrCloseChan := make(chan error)

	d.subscribersMu.Lock()
	d.subscribers[id] = dispatchSubscriber{
		notifyCancelOrCloseChan: notifyCancelOrCloseChan,
		closeCh:                 closeCh,
	}
	d.subscribersMu.Unlock()

	// 后台 goroutine：监听 closeCh，自动清理已关闭的订阅者
	go func(id int) {
		<-closeCh
		d.subscribersMu.Lock()
		defer d.subscribersMu.Unlock()
		sub, ok := d.subscribers[id]
		if !ok {
			return
		}
		close(sub.notifyCancelOrCloseChan) // 关闭通道通知 range 循环结束
		delete(d.subscribers, id)
	}(id)
	return notifyCancelOrCloseChan, closeCh
}
