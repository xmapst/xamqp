package xamqp

// 本文件是针对真实 RabbitMQ broker 的 goroutine 泄漏 / 生命周期实测。
//
// 与 integration_test.go 不同，它不启动 Docker，而是直接连接一个已经跑起来的
// broker，由环境变量 XAMQP_TEST_AMQP_URL 指定，例如：
//
//	XAMQP_TEST_AMQP_URL=amqp://<user>:<pass>@<host>:5672/ go test -run TestLeak -v .
//
// 未设置该变量时全部跳过，不影响常规 go test ./...。
//
// 这些用例的重点是"拿数据说话"：每个用例都统计 goroutine 数量的净增长，
// 而不是只断言功能正确——协程泄漏不会让功能出错，只会让进程慢慢变胖。
//
// 注意：RabbitMQ 4.x 起 transient non-exclusive 队列已废弃，所有队列必须声明为
// durable，否则 broker 会以 541 INTERNAL_ERROR 拒绝。

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const externalAmqpURLEnv = "XAMQP_TEST_AMQP_URL"

// requireExternalBroker 返回外部 broker 地址，未配置时跳过用例。
func requireExternalBroker(t *testing.T) string {
	t.Helper()
	url := os.Getenv(externalAmqpURLEnv)
	if url == "" {
		t.Skipf("set %s to run leak/lifecycle tests against a real broker", externalAmqpURLEnv)
	}
	return url
}

// quietLogger 丢弃全部日志，避免实测输出被重连日志淹没。
type quietLogger struct{}

func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Warnf(string, ...any)  {}
func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Debugf(string, ...any) {}

// settledGoroutines 等待 goroutine 数量稳定后返回，避免把"还没来得及退出"
// 误判成"泄漏"。连续 stableFor 个采样点都不再下降就认为已经稳定。
func settledGoroutines(d time.Duration) int {
	const stableFor = 6
	deadline := time.Now().Add(d)
	best := runtime.NumGoroutine()
	stable := 0
	for time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n < best {
			best = n
			stable = 0
			continue
		}
		stable++
		if stable >= stableFor {
			break
		}
	}
	return best
}

// TestLeakConsumerOnlyConnLifecycle 验证只用 Consumer（从不创建 Publisher）的
// Conn 在 Close 之后不残留 goroutine。
//
// 对应缺陷：connection.New() 曾无条件启动 readUniversalBlockReceiver，而它
// 监听的 channel 只有 Publisher 调用 NotifyBlockedSafe 时才会被注册进
// amqp091-go、进而才可能被关闭。没有 Publisher 时该 channel 永不关闭，
// 那个 goroutine 就永久卡在 range 上——每建一个 Conn 泄漏一个，Close 也救不回来。
func TestLeakConsumerOnlyConnLifecycle(t *testing.T) {
	url := requireExternalBroker(t)

	const cycles = 8
	queue := fmt.Sprintf("xamqp_leak_consumeronly_%d", time.Now().UnixNano())

	// 预热一轮：首次连接会初始化若干一次性的运行时/库内部 goroutine，
	// 不预热会把这些一次性开销算进"泄漏"。
	warm, err := NewConn(url, WithConnectionOptionsLogger(quietLogger{}))
	if err != nil {
		t.Fatalf("warmup connect failed: %v", err)
	}
	wc, err := NewConsumer(warm, queue, WithConsumerOptionsLogger(quietLogger{}), WithConsumerOptionsQueueDurable)
	if err != nil {
		t.Fatalf("warmup consumer failed: %v", err)
	}
	wc.Close()
	_ = warm.Close()

	before := settledGoroutines(5 * time.Second)

	for i := range cycles {
		conn, err := NewConn(url, WithConnectionOptionsLogger(quietLogger{}))
		if err != nil {
			t.Fatalf("cycle %d: connect failed: %v", i, err)
		}
		consumer, err := NewConsumer(conn, queue,
			WithConsumerOptionsLogger(quietLogger{}),
			WithConsumerOptionsQueueDurable,
		)
		if err != nil {
			t.Fatalf("cycle %d: consumer failed: %v", i, err)
		}
		if err := consumer.Run(func(Delivery) Action { return Ack }); err != nil {
			t.Fatalf("cycle %d: run failed: %v", i, err)
		}
		consumer.Close()
		if err := conn.Close(); err != nil {
			t.Fatalf("cycle %d: close failed: %v", i, err)
		}
	}

	after := settledGoroutines(10 * time.Second)
	leaked := after - before
	t.Logf("DATA consumer-only Conn lifecycle: cycles=%d goroutines before=%d after=%d leaked=%d (%.2f per cycle)",
		cycles, before, after, leaked, float64(leaked)/float64(cycles))

	// 每个循环泄漏 1 个即为缺陷复现；留一点余量给运行时抖动。
	if leaked >= cycles {
		t.Fatalf("leaked %d goroutines over %d Conn open/close cycles (~1 per Conn): "+
			"a goroutine started per connection is never terminated by Close()", leaked, cycles)
	}
}

// TestLeakBasicCancelChannelRebuild 验证反复触发 basic.cancel（删除队列）导致的
// 通道重建不会累积 goroutine。
//
// 对应缺陷：handleCancelOrStateChange 的 NotifyCancel 分支 return 后不再消费
// stateCh，而旧通道随后还会推送 StateClosing/StateClosed 两个事件；amqp091-go
// 的状态投递是无保护的阻塞发送、stateCh 只有 1 个缓冲位，第二个事件会让库内部
// 的投递协程永久卡死（且它后续的 close(ch) 也永远执行不到）——每次 basic.cancel
// 泄漏一个协程。生产中队列被删、镜像队列主从切换都会触发。
func TestLeakBasicCancelChannelRebuild(t *testing.T) {
	url := requireExternalBroker(t)

	const cancels = 6
	queue := fmt.Sprintf("xamqp_leak_cancel_%d", time.Now().UnixNano())

	conn, err := NewConn(url,
		WithConnectionOptionsLogger(quietLogger{}),
		WithConnectionOptionsReconnectInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(quietLogger{}),
		WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Run(func(Delivery) Action { return Ack }); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	time.Sleep(1500 * time.Millisecond) // 让首轮消费者完全就绪

	before := settledGoroutines(5 * time.Second)

	// 用一条独立的原生连接删除队列，触发 broker 向消费者发送 basic.cancel。
	admin, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("admin connect failed: %v", err)
	}
	defer admin.Close()

	for i := range cancels {
		ach, err := admin.Channel()
		if err != nil {
			t.Fatalf("cancel %d: admin channel failed: %v", i, err)
		}
		if _, err := ach.QueueDelete(queue, false, false, false); err != nil {
			t.Logf("cancel %d: queue delete returned %v (continuing)", i, err)
		}
		_ = ach.Close()
		// 等待 xamqp 收到 cancel、重建通道并重新声明队列。
		time.Sleep(2 * time.Second)
	}

	after := settledGoroutines(10 * time.Second)
	leaked := after - before
	t.Logf("DATA basic.cancel channel rebuild: cancels=%d goroutines before=%d after=%d leaked=%d (%.2f per cancel)",
		cancels, before, after, leaked, float64(leaked)/float64(cancels))

	// 判据是"是否随重建次数线性增长"，而不是"是否为 0"：稳态下会有 2 个
	// 常驻协程（实测把 cancels 从 6 提到 15，泄漏数稳定在 2 不变，属于固定
	// 开销）。缺陷存在时是每次 cancel 泄漏约 1 个（实测 6 次 → 8 个），
	// 所以用 cancels/2 作为阈值既能稳定通过，又能抓住线性增长的回归。
	if leaked > cancels/2 {
		t.Fatalf("leaked %d goroutines over %d basic.cancel rebuilds (%.2f per cancel, expected ~0): "+
			"the abandoned NotifyStateChange channel parks amqp091-go's delivery goroutine forever",
			leaked, cancels, float64(leaked)/float64(cancels))
	}
}

// TestLeakConnCloseBeforeChildren 验证"先关 Conn，再关 Consumer"这种顺序
// （README 建议反过来，但没有强制，且顶层 defer conn.Close() 很容易写成这样）
// 不会留下一个永远重试的通道重建循环。
//
// 对应缺陷：channel.Manager.reconnectLoop 把"连接管理器已永久关闭"当成瞬时
// 错误——它比较的是 channel 包自己的 errClosed 哨兵，而底层返回的是
// amqp.ErrClosed，对不上——于是每 48 秒重试一次直到进程结束，连带它对连接
// dispatcher 的订阅和订阅对应的清理协程一起回收不掉。
func TestLeakConnCloseBeforeChildren(t *testing.T) {
	url := requireExternalBroker(t)

	queue := fmt.Sprintf("xamqp_leak_closeorder_%d", time.Now().UnixNano())

	before := settledGoroutines(5 * time.Second)

	conn, err := NewConn(url,
		WithConnectionOptionsLogger(quietLogger{}),
		WithConnectionOptionsReconnectInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(quietLogger{}),
		WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}
	if err := consumer.Run(func(Delivery) Action { return Ack }); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	time.Sleep(time.Second)

	// 故意反着来：先关连接，再关消费者。
	if err := conn.Close(); err != nil {
		t.Fatalf("conn close failed: %v", err)
	}
	consumer.Close()

	after := settledGoroutines(15 * time.Second)
	leaked := after - before
	t.Logf("DATA conn-closed-before-children: goroutines before=%d after=%d leaked=%d", before, after, leaked)

	if leaked > 3 {
		t.Fatalf("leaked %d goroutines after closing Conn before its Consumer: "+
			"the channel rebuild loop likely keeps retrying against a permanently closed connection manager", leaked)
	}
}

// TestGracefulCloseWaitsForInFlightHandler 验证优雅关闭确实"等所有处理完成"：
// CloseWithContext 返回之后，不允许还有 handler 在执行，也不允许有新的 handler
// 被启动。
//
// 对应缺陷：CloseWithContext 曾先等待再设置 isClosed，而等待用的写锁在
// waitForHandlerCompletion 内部拿到后立刻就被 defer 释放了。那个空档里
// isClosed 仍是 false，handlerGoroutine 完全可以取走一条已预取的消息并启动新的
// handler，于是 Close() 在用户 handler 仍在执行时就返回了。
func TestGracefulCloseWaitsForInFlightHandler(t *testing.T) {
	url := requireExternalBroker(t)

	queue := fmt.Sprintf("xamqp_graceful_%d", time.Now().UnixNano())

	conn, err := NewConn(url, WithConnectionOptionsLogger(quietLogger{}))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(quietLogger{}),
		WithConsumerOptionsQueueDurable,
		WithConsumerOptionsQOSPrefetch(50), // 预取多条，制造"关闭时本地还有存货"
		WithConsumerOptionsConcurrency(16), // 多个 handlerGoroutine 同时抢 msgs，
		// 放大"屏障释放到通道真正关闭"之间那个极窄窗口被调度到的概率
	)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}

	var (
		running    atomic.Int32
		started    atomic.Int32
		closedAtNs atomic.Int64 // CloseWithContext 返回的时刻，0 表示尚未返回
		afterClose atomic.Int32
	)

	// 必须先 Run 再发布：队列是在 Run() -> startGoroutines 里才声明的，
	// NewConsumer 本身并不声明。早于 Run 发布的消息会因为默认 exchange
	// 找不到目标队列而被直接丢弃（第一版测试就栽在这里，表现为"一条消息
	// 都没收到"，而不是任何真实缺陷）。
	err = consumer.Run(func(Delivery) Action {
		startedAt := time.Now().UnixNano()
		started.Add(1)
		running.Add(1)
		defer running.Add(-1)
		// 用时间戳而不是布尔标志判定"是否在 Close 返回之后才开始"：
		// 布尔标志要在 CloseWithContext 返回之后才被置上，中间存在窗口，
		// 恰恰会漏掉最该被抓住的那次启动。
		if c := closedAtNs.Load(); c != 0 && startedAt > c {
			afterClose.Add(1)
		}
		time.Sleep(4 * time.Second) // 模拟耗时业务处理：必须显著长于下面的观测点，
		// 否则"优雅等待"会在采样之前就结束，采到的 IsClosed() 是等待结束后的值，
		// 两个版本都为 true，测不出任何差异（这正是上一版失败的原因）
		return Ack
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // 等 basic.consume 真正就绪

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(quietLogger{}))
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	const messages = 60
	for i := range messages {
		if err := publisher.Publish(fmt.Appendf(nil, "msg-%d", i), []string{queue}); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	// 等到确实有 handler 正在跑，再多给一点时间让预取缓冲填满，
	// 这样 Close 时本地一定还压着若干条未处理的消息。
	deadline := time.Now().Add(15 * time.Second)
	for running.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no message was ever delivered to the handler")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	// 关键观测点：在"优雅等待"仍在进行的中途去看 IsClosed()。
	//
	// 直接去抢那个竞态窗口是不现实的——从屏障释放到 chanManager.Close()
	// 生效只有微秒级，实测 16 路并发也打不中。但缺陷的本质是**顺序**问题，
	// 而顺序本身是可以稳定观测的：
	//   修复前：先等待、后置 isClosed，所以整个等待期间 IsClosed() 都是 false，
	//           handlerGoroutine 的 getIsClosed 检查形同虚设，屏障一放开就会
	//           继续取预取的消息；
	//   修复后：先置 isClosed 再等待，等待期间 IsClosed() 立刻为 true，
	//           handlerGoroutine 会主动停止取新消息，"等待读锁全部释放"
	//           才真正等价于"所有消息处理完毕"。
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		consumer.CloseWithContext(context.Background())
	}()

	time.Sleep(1500 * time.Millisecond) // handler 还要跑 4s，此刻等待必然仍在进行中
	closedDuringWait := consumer.IsClosed()

	select {
	case <-closeDone:
	case <-time.After(30 * time.Second):
		t.Fatal("CloseWithContext did not return within 30s")
	}
	stillRunning := running.Load()
	closedAtNs.Store(time.Now().UnixNano())

	time.Sleep(3 * time.Second) // 若关闭后还有 handler 被启动，这段时间足够暴露

	t.Logf("DATA graceful close: handlers started=%d closed-flag-visible-during-wait=%v "+
		"still-running-at-close-return=%d started-after-close=%d",
		started.Load(), closedDuringWait, stillRunning, afterClose.Load())

	if !closedDuringWait {
		t.Fatal("consumer still reported IsClosed()==false while CloseWithContext was already waiting for " +
			"in-flight handlers: the close flag is set only AFTER the drain barrier, so handlerGoroutine's " +
			"getIsClosed() check cannot stop it from picking up more prefetched deliveries once the barrier releases")
	}
	if stillRunning != 0 {
		t.Fatalf("CloseWithContext returned while %d handler(s) were still executing; "+
			"the documented graceful-shutdown guarantee is not honored", stillRunning)
	}
	if afterClose.Load() != 0 {
		t.Fatalf("%d handler(s) started AFTER CloseWithContext returned; "+
			"the consumer kept pulling prefetched deliveries past the shutdown barrier", afterClose.Load())
	}
}

// TestPublishConfirmationsNotDuplicatedAcrossReconnect 验证发布确认不会被重复
// 投递——即使中途发生通道重连、且用户在重连前后都注册过 handler。
//
// 对应缺陷：start*Handler 的"代号"去重曾不是原子的（先在 handlerMu 下取代号、
// 解锁、再由新协程去 channelMu 下注册）。中间插入一次重连时，注册会落到新通道
// 上却记着旧代号，随后重连 watcher 按新代号再注册一个，同一通道上出现两个
// 监听者。amqp091-go 会把每个确认广播给所有监听者且不去重，于是每条确认被
// 处理两次——Confirmation 携带的 ReconnectionCount 正是用来做消息去重的，
// 确认本身重复会直接破坏基于它的投递统计。
func TestPublishConfirmationsNotDuplicatedAcrossReconnect(t *testing.T) {
	url := requireExternalBroker(t)

	queue := fmt.Sprintf("xamqp_confirm_dup_%d", time.Now().UnixNano())

	conn, err := NewConn(url,
		WithConnectionOptionsLogger(quietLogger{}),
		WithConnectionOptionsReconnectInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsLogger(quietLogger{}),
		WithPublisherOptionsConfirm,
	)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	var mu sync.Mutex
	seen := make(map[string]int)
	record := func(c Confirmation) {
		mu.Lock()
		seen[fmt.Sprintf("%d-%d", c.ReconnectionCount, c.DeliveryTag)]++
		mu.Unlock()
	}
	publisher.NotifyPublish(record)

	// 制造竞态：反复强制通道重连，同时在重连窗口里高频调用 NotifyPublish。
	// 缺陷需要"取代号"和"真正注册"之间恰好插入一次重连才会触发，所以必须
	// 主动把这个窗口撑开并反复尝试，否则单纯串行发布是碰不到的（第一版
	// 测试就因为没有这一步，在有缺陷的代码上同样通过，属于假阴性）。
	//
	// 向一个不存在的 exchange 发布会让 broker 以 NOT_FOUND 关闭通道，
	// 从而触发 xamqp 自己的通道重建。
	const rounds = 12
	for r := range rounds {
		_ = publisher.Publish([]byte("boom"), []string{"unused"},
			WithPublishOptionsExchange(fmt.Sprintf("xamqp-missing-exchange-%d-%d", time.Now().UnixNano(), r)))

		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 25 {
					publisher.NotifyPublish(record)
					time.Sleep(time.Millisecond)
				}
			})
		}
		wg.Wait()
		time.Sleep(400 * time.Millisecond) // 让重连稳定下来
	}

	// 竞态窗口过去之后再发布：此时若通道上挂了两个监听者，每条确认都会
	// 被投递两次。
	const messages = 40
	for i := range messages {
		if err := publisher.Publish(fmt.Appendf(nil, "m-%d", i), []string{queue}); err != nil {
			t.Logf("publish %d failed (channel may still be recovering): %v", i, err)
			continue
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(4 * time.Second) // 等确认全部到达

	mu.Lock()
	defer mu.Unlock()
	dupes := 0
	total := 0
	maxSeen := 0
	for _, n := range seen {
		total += n
		if n > 1 {
			dupes++
		}
		if n > maxSeen {
			maxSeen = n
		}
	}
	t.Logf("DATA publish confirmations: distinct=%d totalDeliveries=%d duplicated=%d maxPerTag=%d",
		len(seen), total, dupes, maxSeen)

	if len(seen) == 0 {
		t.Fatal("no confirmations were observed at all; the test did not exercise the intended path")
	}
	if dupes != 0 {
		t.Fatalf("%d confirmation(s) were delivered more than once (total deliveries %d for %d distinct tags, max %d per tag): "+
			"duplicate listeners are registered on the same channel", dupes, total, len(seen), maxSeen)
	}
}
