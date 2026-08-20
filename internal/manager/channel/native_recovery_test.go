package channel

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/dispatcher"
)

// spyLogger 是一个 slog.Handler，记录所有 Info 级别日志的 Message。
// reconnectLoop 的每次迭代都会无条件先记一条 "waiting to attempt to
// reconnect" 日志，早于它内部 select 着 closeSignal 的等待逻辑，所以这条
// 日志本身就是"reconnectLoop 确实被进入过"的可靠信号，不需要真的借用一个
// 连接开新通道就能验证 handleCancelOrStateChange 的分支是否正确。
//
// 通过 slog.SetDefault 安装为进程默认 logger 来捕获生产代码里
// slog.Info/Warn/Error 包级调用输出的日志。
type spyLogger struct {
	mu    sync.Mutex
	infos []string
}

func (l *spyLogger) Enabled(context.Context, slog.Level) bool { return true }

func (l *spyLogger) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelInfo {
		l.mu.Lock()
		l.infos = append(l.infos, r.Message)
		l.mu.Unlock()
	}
	return nil
}

func (l *spyLogger) WithAttrs(_ []slog.Attr) slog.Handler { return l }
func (l *spyLogger) WithGroup(_ string) slog.Handler      { return l }

func (l *spyLogger) hasInfoContaining(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.infos {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// installSpyLogger 把 spy 设为进程默认 slog logger，并注册测试结束时的
// 还原，避免污染同一进程内其他测试。
func installSpyLogger(t *testing.T, spy *spyLogger) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(spy))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// stubNotifyReconnect 是 Manager.notifyConnReconnect 的假实现，供测试直接
// 驱动 reconnectLoop 而不需要一个真实的、只能通过实际拨号才能构造出来的
// connection.Manager。返回的 closeCh 带 1 个缓冲，让 reconnectLoop 退出时
// 的取消订阅发送（`unsubscribeConnReconnect <- struct{}{}`）不必等一个
// 接收方就能立即完成。
func stubNotifyReconnect() (<-chan error, chan<- struct{}) {
	return make(chan error), make(chan struct{}, 1)
}

// TestHandleCancelOrStateChange_TransientAndGracefulNeverFallback 验证中间态
// （StateReconnecting/StateOpen）和优雅关闭（StateClosed 且 Err == nil）都
// 不会触发 xamqp 自己的兜底 reconnectLoop。
//
// notifyCancelChan 传 nil：该 select 分支永远不会就绪，逻辑完全由
// stateCh 驱动。同步调用（不开 goroutine）：若逻辑有误、错误地进入了
// reconnectLoop，会在 getNewChannel() 里对 nil 的 connManager 调用方法
// 直接 panic——但这个 panic 发生在测试自身的 goroutine 里，会被 go test
// 正常捕获并报告为失败，不会拖垮整个测试进程。
func TestHandleCancelOrStateChange_TransientAndGracefulNeverFallback(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	m := &Manager{
		dispatcher: dispatcher.New(),
	}

	stateCh := make(chan *amqp.StateChanged, 3)
	stateCh <- &amqp.StateChanged{To: amqp.StateReconnecting}
	stateCh <- &amqp.StateChanged{To: amqp.StateOpen}
	stateCh <- &amqp.StateChanged{To: amqp.StateClosed, Err: nil}

	m.handleCancelOrStateChange(nil, stateCh)

	if spy.hasInfoContaining("waiting") {
		t.Fatal("transient states and a graceful close must never enter reconnectLoop")
	}
}

// TestHandleCancelOrStateChange_ExhaustedRecoveryEntersReconnectLoop 验证
// 终态 StateClosed 且 Err != nil（原生恢复重试耗尽/遇到不可恢复错误）确实
// 会触发 xamqp 自己的兜底 reconnectLoop。
//
// closeSignal 提前关闭，同时把 reconnectInterval 设得足够大：
// reconnectLoop 第一次迭代里 select 着 {closeSignal, time.After(delay)}
// 时，若 delay 为 0（零值 reconnectInterval 经 backoff.Delay 就是 0），
// 一个刚创建、到期时间为 0 的 timer 完全可能在 select 求值的那一刻已经
// 触发，与"提前关闭"的 closeSignal 构成真正的两个都就绪的竞争，select
// 会在两者间随机选择——曾直接导致本测试间歇性走到需要借用连接的
// getNewChannel() 并 panic。delay 设得远大于测试同步执行的耗时后，
// time.After(delay) 在 select 求值时必然还没触发，closeSignal 是唯一
// 就绪的分支，才能保证测试完全确定、无需 sleep。
func TestHandleCancelOrStateChange_ExhaustedRecoveryEntersReconnectLoop(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	closeSignal := make(chan struct{})
	close(closeSignal)

	m := &Manager{
		closeSignal:         closeSignal,
		reconnectInterval:   time.Hour,
		dispatcher:          dispatcher.New(),
		notifyConnReconnect: stubNotifyReconnect,
	}

	stateCh := make(chan *amqp.StateChanged, 1)
	stateCh <- &amqp.StateChanged{To: amqp.StateClosed, Err: errors.New("recovery exhausted")}

	m.handleCancelOrStateChange(nil, stateCh)

	if !spy.hasInfoContaining("waiting") {
		t.Fatal("expected exhausted native recovery (StateClosed with a non-nil Err) to enter reconnectLoop, but it did not")
	}
}

// TestHandleCancelOrStateChange_CancelEntersReconnectLoop 验证 basic.cancel
// （NotifyCancel）独立于原生恢复触发 xamqp 自己的兜底 reconnectLoop——
// 一个仍然存活的通道被服务器取消消费者时，不会被原生恢复处理，必须由
// xamqp 自己重建整个通道。同样把 reconnectInterval 设得足够大，理由同上。
func TestHandleCancelOrStateChange_CancelEntersReconnectLoop(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	closeSignal := make(chan struct{})
	close(closeSignal)

	m := &Manager{
		closeSignal:         closeSignal,
		reconnectInterval:   time.Hour,
		dispatcher:          dispatcher.New(),
		notifyConnReconnect: stubNotifyReconnect,
	}

	notifyCancelChan := make(chan string, 1)
	notifyCancelChan <- "some-queue"

	m.handleCancelOrStateChange(notifyCancelChan, nil)

	if !spy.hasInfoContaining("waiting") {
		t.Fatal("expected a basic.cancel notification to enter reconnectLoop, but it did not")
	}
}

// TestHandleCancelOrStateChange_CancelBranchKeepsDrainingStateChannel 是针对
// 一个真实 goroutine 泄漏的回归测试。
//
// 背景：NotifyCancel 分支处理完就 return，之后再也不读 stateCh；但此时旧通道
// 接下来一定还会推送 StateClosing、StateClosed 两个事件（reconnectLoop 里会
// 关闭旧通道）。amqp091-go 投递状态变化用的是无保护的阻塞发送
// （lifecycle.go 的 `ch <- sc`），而 stateCh 只有 1 个缓冲位：第一个事件填满
// 缓冲，第二个事件就会让库内部那个投递协程永久卡死，且它后续的
// close(ch)/removeListener 也永远执行不到——每次 basic.cancel 触发的通道
// 重建都泄漏一个协程。
//
// 修复方式是在离开该分支前起一个排空协程。本测试模拟库的投递行为：
// handleCancelOrStateChange 返回后连续塞入两个事件，第二次发送若在超时内
// 无法完成，就说明没人在排空，泄漏依然存在。
func TestHandleCancelOrStateChange_CancelBranchKeepsDrainingStateChannel(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	closeSignal := make(chan struct{})
	close(closeSignal)

	m := &Manager{
		closeSignal:         closeSignal,
		reconnectInterval:   time.Hour,
		dispatcher:          dispatcher.New(),
		notifyConnReconnect: stubNotifyReconnect,
	}

	notifyCancelChan := make(chan string, 1)
	notifyCancelChan <- "some-queue"
	stateCh := make(chan *amqp.StateChanged, 1) // 与生产代码一致：缓冲为 1

	m.handleCancelOrStateChange(notifyCancelChan, stateCh)

	// 模拟旧通道关闭时库推送的两个事件。
	send := func(sc *amqp.StateChanged) bool {
		select {
		case stateCh <- sc:
			return true
		case <-time.After(3 * time.Second):
			return false
		}
	}

	if !send(&amqp.StateChanged{To: amqp.StateClosing}) {
		t.Fatal("first state change send blocked; the 1-slot buffer should have accepted it")
	}
	if !send(&amqp.StateChanged{To: amqp.StateClosed}) {
		t.Fatal("second state change send blocked forever: nothing is draining the abandoned stateCh, " +
			"so amqp091-go's delivery goroutine would be parked permanently (one leaked goroutine per basic.cancel)")
	}

	// 库在投递完终态后会关闭通道；排空协程应当据此退出。
	close(stateCh)
}

// TestDrainStateChanges_ExitsWhenChannelClosed 验证排空协程在通道关闭后确实
// 会退出（而不是自己变成另一个泄漏源），并且 nil 通道不会让它永久阻塞。
func TestDrainStateChanges_ExitsWhenChannelClosed(t *testing.T) {
	stateCh := make(chan *amqp.StateChanged, 1)
	done := make(chan struct{})
	go func() {
		(&Manager{}).drainStateChanges(stateCh)
		close(done)
	}()

	stateCh <- &amqp.StateChanged{To: amqp.StateClosing}
	stateCh <- &amqp.StateChanged{To: amqp.StateClosed}
	close(stateCh)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drainStateChanges did not exit after the channel was closed")
	}

	nilDone := make(chan struct{})
	go func() {
		(&Manager{}).drainStateChanges(nil)
		close(nilDone)
	}()
	select {
	case <-nilDone:
	case <-time.After(3 * time.Second):
		t.Fatal("drainStateChanges blocked forever on a nil channel")
	}
}

// TestHandleCancelOrStateChange_StateChannelClosedWithoutTerminalEvent 验证
// stateCh 被关闭、但从未真正送达过任何终态事件时（例如通道对象在中途被
// 直接销毁），函数会通过 `case sc, ok := <-stateCh: if !ok { return }`
// 干净返回，而不是死循环或误入 reconnectLoop。notifyCancelChan 传 nil
// 使该分支永不就绪，保证只测这一条路径。
func TestHandleCancelOrStateChange_StateChannelClosedWithoutTerminalEvent(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	m := &Manager{
		dispatcher: dispatcher.New(),
	}

	stateCh := make(chan *amqp.StateChanged)
	close(stateCh)

	m.handleCancelOrStateChange(nil, stateCh)

	if spy.hasInfoContaining("waiting") {
		t.Fatal("a state channel closed without a terminal event must not enter reconnectLoop")
	}
}

// TestHandleCancelOrStateChange_CancelChannelClosedFallsThroughToState 验证
// notifyCancelChan 被关闭、但从未真正送达过取消通知时，`if !ok { notifyCancelChan
// = nil; continue }` 分支会把它禁用而不是误当成一次真实取消去触发
// reconnectLoop，逻辑继续交给 stateCh 驱动（这里用优雅关闭让函数正常返回）。
func TestHandleCancelOrStateChange_CancelChannelClosedFallsThroughToState(t *testing.T) {
	spy := &spyLogger{}
	installSpyLogger(t, spy)
	m := &Manager{
		dispatcher: dispatcher.New(),
	}

	notifyCancelChan := make(chan string)
	close(notifyCancelChan)

	stateCh := make(chan *amqp.StateChanged, 1)
	stateCh <- &amqp.StateChanged{To: amqp.StateClosed, Err: nil}

	m.handleCancelOrStateChange(notifyCancelChan, stateCh)

	if spy.hasInfoContaining("waiting") {
		t.Fatal("a closed cancel channel (never actually canceled) must not enter reconnectLoop")
	}
}
