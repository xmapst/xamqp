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
	subscribers         map[int]*dispatchSubscriber // 当前所有订阅者，键为订阅 ID
	subscribersMu       *sync.Mutex                 // 保护 subscribers map 的并发读写
	subscriberIDCounter atomic.Int64                // 订阅 ID 生成器，原子自增避免加锁
}

// dispatchSubscriber 单个订阅者的通道对，实现发布和取消订阅。
//
// 必须以指针形式保存/传递（map 中存 *dispatchSubscriber，Dispatch 的
// 快照切片也存指针），而不能按值拷贝：mu 和 removed 需要在“克隆快照”
// “写入 map”“清理协程持有的引用”这几处共享同一份数据，若按值拷贝，
// 清理协程对 removed 的写入只会作用于它自己那份副本，Dispatch 读到的
// 快照副本永远看不到这次更新，起不到互斥作用。
//
// mu 保护 removed 与 notifyCancelOrCloseChan 的关闭：发送（Dispatch）
// 和关闭（取消订阅的清理协程）必须互斥，否则可能出现向已关闭 channel
// 发送导致 panic。使用订阅者私有的锁而非 Dispatcher 全局锁，
// 这样广播耗时（最长 5 秒超时）只会阻塞这一个订阅者自身的取消订阅，
// 不会连带阻塞其他订阅者的注册/清理。
type dispatchSubscriber struct {
	notifyCancelOrCloseChan chan error      // 事件通知通道，接收重连错误
	closeCh                 <-chan struct{} // 关闭信号通道，订阅者关闭时写入
	mu                      *sync.Mutex     // 保护 removed 及 notifyCancelOrCloseChan 关闭操作的私有锁
	removed                 bool            // 是否已被清理协程标记移除，true 时 Dispatch 跳过发送
}

// New 创建事件广播器实例。
func New() *Dispatcher {
	return &Dispatcher{
		subscribers:   make(map[int]*dispatchSubscriber),
		subscribersMu: &sync.Mutex{},
	}
}

// Dispatch 向所有当前订阅者广播事件（错误信息）。
//
// 使用 5 秒超时防止单个订阅者接收缓慢阻塞整个广播：
// 若某个订阅者 5 秒内未读取事件，则跳过该订阅者并记录警告。
// 广播是串行的（依次发送给每个订阅者），适合低频事件（如重连）场景。
//
// 订阅者列表在持有 subscribersMu 期间被克隆后立即释放该全局锁，
// 实际发送在全局锁之外完成，避免单个订阅者的发送耗时（最长 5 秒超时）
// 阻塞 AddSubscriber 或其他订阅者的取消订阅。发送前后持有该订阅者
// 私有的 mu 并检查 removed 标志，与清理协程的关闭操作互斥，
// 避免向已关闭的 notifyCancelOrCloseChan 发送而 panic。
func (d *Dispatcher) Dispatch(err error) error {
	d.subscribersMu.Lock()
	subscribers := make([]*dispatchSubscriber, 0, len(d.subscribers))
	for _, subscriber := range d.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	d.subscribersMu.Unlock()

	for _, subscriber := range subscribers {
		subscriber.mu.Lock()
		if subscriber.removed {
			subscriber.mu.Unlock()
			continue
		}
		select {
		case <-time.After(time.Second * 5):
			slog.Warn("Unexpected rabbitmq error: timeout in dispatch")
		case subscriber.notifyCancelOrCloseChan <- err:
		}
		subscriber.mu.Unlock()
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
// 收到信号后在该订阅者的私有锁保护下标记 removed 并关闭事件通道，
// 再从 subscribers map 中删除，防止已停止的组件在后续 Dispatch 时
// 阻塞广播流程，也防止与并发的 Dispatch 发送竞争导致 panic。
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}) {
	id := int(d.subscriberIDCounter.Add(1))

	closeCh := make(chan struct{})
	notifyCancelOrCloseChan := make(chan error)

	sub := &dispatchSubscriber{
		notifyCancelOrCloseChan: notifyCancelOrCloseChan,
		closeCh:                 closeCh,
		mu:                      &sync.Mutex{},
	}

	d.subscribersMu.Lock()
	d.subscribers[id] = sub
	d.subscribersMu.Unlock()

	// 后台 goroutine：监听 closeCh，自动清理已关闭的订阅者
	go func(id int) {
		<-closeCh
		d.subscribersMu.Lock()
		_, ok := d.subscribers[id]
		if ok {
			delete(d.subscribers, id)
		}
		d.subscribersMu.Unlock()
		if !ok {
			return
		}

		sub.mu.Lock()
		sub.removed = true
		close(sub.notifyCancelOrCloseChan) // 关闭通道通知 range 循环结束
		sub.mu.Unlock()
	}(id)
	return notifyCancelOrCloseChan, closeCh
}
