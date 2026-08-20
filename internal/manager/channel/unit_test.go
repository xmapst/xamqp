package channel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// newTestManager 构造一个不依赖真实 AMQP 通道的最小 Manager，
// 只用于测试完全不触碰 m.channel 字段的逻辑（锁获取、confirmMode 状态）。
func newTestManager() *Manager {
	return &Manager{
		channelMu: &sync.RWMutex{},
	}
}

// TestLockChannelReadHonorsContext 验证当写锁被长时间占用时，
// lockChannelRead 会在 ctx 超时/取消时及时返回，而不是像裸 RLock() 那样
// 无条件阻塞到写锁释放为止。
func TestLockChannelReadHonorsContext(t *testing.T) {
	m := newTestManager()

	m.channelMu.Lock() // 模拟一次正在进行、迟迟不释放的重连
	defer m.channelMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.lockChannelRead(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected lockChannelRead to return the context error, got nil (it must have acquired the lock, which should be impossible here)")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("lockChannelRead took %s to honor a 50ms context timeout", elapsed)
	}
}

// TestLockChannelReadRejectsAlreadyCanceledContext 验证已经取消的 ctx
// 会被立即拒绝，不会先去尝试拿锁。
func TestLockChannelReadRejectsAlreadyCanceledContext(t *testing.T) {
	m := newTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := m.lockChannelRead(ctx); err == nil {
		t.Fatal("expected lockChannelRead to reject an already-canceled context")
	}
}

// TestLockChannelReadSucceedsWhenLockIsFree 验证正常情况下（没有并发写锁）
// 能够正常拿到锁，不会被 ctx 逻辑误伤。
func TestLockChannelReadSucceedsWhenLockIsFree(t *testing.T) {
	m := newTestManager()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := m.lockChannelRead(ctx); err != nil {
		t.Fatalf("lockChannelRead failed with no contention: %v", err)
	}
	m.channelMu.RUnlock()
}

// TestConfirmSafeIdempotent 验证 ConfirmSafe 在已经处于 confirm 模式时
// 直接返回 nil，不会再去调用底层 channel（否则在这个不带真实 AMQP
// 通道的测试 Manager 上会因为 m.channel 为 nil 而 panic —— 这本身
// 就是幂等短路生效与否的一个直接信号）。
func TestConfirmSafeIdempotent(t *testing.T) {
	m := newTestManager()
	m.confirmMode = true // 模拟已经成功进入过 confirm 模式

	if err := m.ConfirmSafe(false); err != nil {
		t.Fatalf("ConfirmSafe returned an error when already in confirm mode: %v", err)
	}
}
