package connection

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newTestManager 构造一个不依赖真实 AMQP 连接的最小 Manager，
// 专用于测试 publisherNotifyBlockingReceivers 相关逻辑。
//
// universalNotifyBlockingReceiverUsed 预先置为 true，跳过
// NotifyBlockedSafe 中"首次注册需要调用 m.connection.NotifyBlocked"
// 的分支，从而不需要一个真正拨号成功的 m.connection。
func newTestManager() *Manager {
	return &Manager{
		connectionMu:                        &sync.RWMutex{},
		publisherNotifyBlockingReceiversMu:  &sync.RWMutex{},
		universalNotifyBlockingReceiverUsed: true,
	}
}

// TestNotifyBlockedSafe_NotBlockedByStuckBroadcast 验证当广播协程
// readUniversalBlockReceiver 卡在向某个迟迟不读取的接收者（stuck）发送时，
// 不会连带阻塞对另一个无关接收者（other）的注册/移除。
//
// 注意：移除 stuck 自身不在此列——它和广播共用同一把私有锁，
// 被卡住最长 5 秒是设计上刻意接受的代价（只影响这一个接收者自己的
// 取消订阅，不影响其他任何人），不是本测试要验证的东西。
//
// 修复前：readUniversalBlockReceiver 在持有
// publisherNotifyBlockingReceiversMu 读锁的情况下，对列表里的每一个
// 接收者依次做无超时的阻塞发送（既不释放锁，也没有 select+timeout）。
// 只要排在前面的某个接收者不读取，这个读锁就会被永久占用，
// 导致任何需要写锁的调用（NotifyBlockedSafe、
// RemovePublisherBlockingReceiver）永久阻塞——不管目标是不是 stuck
// 本身。也就是说任何 Publisher 的 Close() 都可能因为另一个完全无关的
// Publisher 卡住而永久挂起。
//
// 修复后：读锁只在克隆接收者列表期间短暂持有，随后释放；
// 对 stuck 的发送尝试（含最长 5 秒超时）在锁外、且只持有 stuck 自己的
// 私有锁进行，不影响 other 的注册/移除。
func TestNotifyBlockedSafe_NotBlockedByStuckBroadcast(t *testing.T) {
	m := newTestManager()

	// stuck 故意不被任何人读取，模拟一个迟迟不消费阻塞通知的 Publisher。
	_ = m.NotifyBlockedSafe(make(chan amqp.Blocking))
	// other 是另一个正常的、与 stuck 无关的 Publisher。
	other := m.NotifyBlockedSafe(make(chan amqp.Blocking))

	universal := make(chan amqp.Blocking)
	go m.readUniversalBlockReceiver(universal)

	go func() {
		// 广播一次阻塞事件；readUniversalBlockReceiver 会先卡在 stuck 上。
		universal <- amqp.Blocking{Active: true}
	}()

	// 给广播协程一点时间先进入对 stuck 的发送尝试。
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	var registerElapsed, removeElapsed time.Duration
	go func() {
		start := time.Now()
		_ = m.NotifyBlockedSafe(make(chan amqp.Blocking)) // 注册一个全新的、无关的 Publisher
		registerElapsed = time.Since(start)

		start = time.Now()
		m.RemovePublisherBlockingReceiver(other) // 移除另一个无关的 Publisher
		removeElapsed = time.Since(start)
		close(done)
	}()

	select {
	case <-done:
		t.Logf("NotifyBlockedSafe took %s, RemovePublisherBlockingReceiver(other) took %s while broadcast was stuck on a different receiver",
			registerElapsed, removeElapsed)
		if registerElapsed > 2*time.Second || removeElapsed > 2*time.Second {
			t.Fatalf("registration/removal of an unrelated receiver was blocked (register=%s, remove=%s) "+
				"by a broadcast stuck on a different receiver", registerElapsed, removeElapsed)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("NotifyBlockedSafe/RemovePublisherBlockingReceiver(other) did not return within 8s — the collection " +
			"lock is likely being held across the broadcast's blocking send instead of being released after cloning " +
			"the receiver list")
	}
}

// TestRemovePublisherBlockingReceiver_DuplicateRegistrationDoesNotPanic 用同一个
// channel 值注册两次后只移除一次，确认不会 panic。
//
// 这是一个不依赖任何并发时序、完全确定性的复现：修复前
// RemovePublisherBlockingReceiver 用 append(slice[:i], slice[i+1:]...)
// 原地删除元素的同时还在 range 这个切片。当同一个 channel 值出现两次时，
// 第一次命中在索引 i 做原地删除，把后续元素向左搬移一位，但 range
// 内部记录的长度不变，循环继续读到的是搬移后仍残留在末尾的旧值副本——
// 这个副本恰好等于 receiver，于是发生第二次"命中"，此时切片已经缩短
// 了一位，再次执行 append(slice[:i2], slice[i2+1:]...) 时
// slice[i2+1:] 的起始下标已经越界，触发
// "slice bounds out of range" panic。
//
// 修复后改为重建一个新切片再整体替换，不存在这个问题。
func TestRemovePublisherBlockingReceiver_DuplicateRegistrationDoesNotPanic(t *testing.T) {
	m := newTestManager()

	ch := make(chan amqp.Blocking)
	m.NotifyBlockedSafe(ch)
	m.NotifyBlockedSafe(ch) // 故意用同一个 channel 值重复注册

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RemovePublisherBlockingReceiver panicked on duplicate registration: %v", r)
		}
	}()
	m.RemovePublisherBlockingReceiver(ch)
}
