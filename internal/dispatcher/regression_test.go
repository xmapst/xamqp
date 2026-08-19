package dispatcher

import (
	"errors"
	"testing"
	"time"
)

// TestAddSubscriber_NotBlockedByStuckDispatch 验证 Dispatch 在广播给一个
// 迟迟不读取事件的订阅者时，不会连带阻塞 AddSubscriber。
//
// 修复前：Dispatch 用 defer 持有 subscribersMu 直到整个广播循环结束，
// 循环内对每个订阅者的发送最长等待 5 秒（select + time.After）。
// 一旦有订阅者不读取事件，Dispatch 会在持锁状态下卡住最长 5 秒，
// 期间任何需要 subscribersMu 的调用（包括 AddSubscriber）都会被卡住。
//
// 修复后：Dispatch 只在克隆订阅者快照期间持有 subscribersMu，随后立即
// 释放，实际发送（含最长 5 秒的超时等待）在锁外完成，AddSubscriber
// 不再受影响，应当在毫秒级返回。
func TestAddSubscriber_NotBlockedByStuckDispatch(t *testing.T) {
	d := New()

	// sub1 故意不读取 notifyCancelOrCloseChan，模拟“订阅者迟迟不消费事件”。
	_, _ = d.AddSubscriber()

	dispatchDone := make(chan struct{})
	go func() {
		_ = d.Dispatch(errors.New("boom"))
		close(dispatchDone)
	}()

	// 给 Dispatch 一点时间先拿到快照、进入对 sub1 的发送尝试。
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	_, _ = d.AddSubscriber()
	elapsed := time.Since(start)

	t.Logf("AddSubscriber returned after %s while Dispatch was stuck on an undrained subscriber", elapsed)
	if elapsed > 2*time.Second {
		t.Fatalf("AddSubscriber blocked for %s (>2s) — Dispatch is holding subscribersMu across its blocking send, "+
			"instead of releasing it before attempting delivery", elapsed)
	}

	select {
	case <-dispatchDone:
	case <-time.After(6 * time.Second):
		t.Fatal("Dispatch itself never returned within 6s")
	}
}
