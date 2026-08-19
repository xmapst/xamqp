package connection

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xmapst/xamqp/internal/dispatcher"
)

// spyLogger 记录 Infof 调用内容。reconnectLoop 的每次迭代都会无条件先记一条
// "waiting ... to attempt to reconnect" 日志，早于它内部 select 着 closeSignal
// 的等待逻辑，所以这条日志本身就是"reconnectLoop 确实被进入过"的可靠信号，
// 不需要真的建立/拨号一个连接就能验证 handleStateChanges 的分支是否正确。
type spyLogger struct {
	mu    sync.Mutex
	infos []string
}

func (l *spyLogger) Errorf(string, ...any) {}
func (l *spyLogger) Warnf(string, ...any)  {}
func (l *spyLogger) Infof(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, format)
}
func (l *spyLogger) Debugf(string, ...any) {}

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

// TestHandleStateChanges_TransientAndGracefulNeverFallback 验证中间态
// （StateReconnecting/StateOpen）和优雅关闭（StateClosed 且 Err == nil）
// 都不会触发 xamqp 自己的兜底 reconnectLoop——原生恢复期间连接对象原地
// 复用，这些状态下没有任何需要 xamqp 插手的事情。
//
// 同步调用（不开 goroutine）：若逻辑有误、错误地进入了 reconnectLoop，
// 会在 dial() 里用空字符串 URL 调用 amqp.DialConfig，返回一个普通的
// 拨号错误（不会 panic），reconnectLoop 会记录错误后重试；预先关闭的
// closeSignal 保证即便走到这一步也会在下一次退避等待时立刻退出，
// 不会真的死循环卡住测试。真正的断言仍然是下面的
// hasInfoContaining("waiting") 检查。
func TestHandleStateChanges_TransientAndGracefulNeverFallback(t *testing.T) {
	spy := &spyLogger{}
	closeSignal := make(chan struct{})
	close(closeSignal)

	m := &Manager{
		logger:      spy,
		closeSignal: closeSignal,
		dispatcher:  dispatcher.New(),
	}

	stateCh := make(chan *amqp.StateChanged, 3)
	stateCh <- &amqp.StateChanged{From: amqp.StateOpen, To: amqp.StateReconnecting}
	stateCh <- &amqp.StateChanged{From: amqp.StateReconnecting, To: amqp.StateOpen}
	stateCh <- &amqp.StateChanged{From: amqp.StateOpen, To: amqp.StateClosed, Err: nil}

	m.handleStateChanges(stateCh) // 处理完第 3 个事件（优雅关闭）后应自行返回

	if spy.hasInfoContaining("waiting") {
		t.Fatal("transient states and a graceful close must never enter reconnectLoop")
	}
}

// TestHandleStateChanges_ExhaustedRecoveryEntersReconnectLoop 验证终态
// StateClosed 且 Err != nil（原生恢复重试耗尽/遇到不可恢复错误）确实会
// 触发 xamqp 自己的兜底 reconnectLoop。
//
// closeSignal 提前关闭，同时把 ReconnectInterval 设得足够大：
// reconnectLoop 第一次迭代里 select 着 {closeSignal, time.After(delay)}
// 时，若 delay 为 0（零值 ReconnectInterval 经 backoff.Delay 就是 0），
// 一个刚创建、到期时间为 0 的 timer 完全可能在 select 求值的那一刻已经
// 触发，与"提前关闭"的 closeSignal 构成真正的两个都就绪的竞争，
// select 会在两者间随机选择——曾直接导致本测试间歇性走到需要网络连接
// 的 dial() 并 panic。delay 设得远大于测试同步执行的耗时后，
// time.After(delay) 在 select 求值时必然还没触发，closeSignal 是唯一
// 就绪的分支，才能保证测试完全确定、无需 sleep。
func TestHandleStateChanges_ExhaustedRecoveryEntersReconnectLoop(t *testing.T) {
	spy := &spyLogger{}
	closeSignal := make(chan struct{})
	close(closeSignal)

	m := &Manager{
		logger:            spy,
		closeSignal:       closeSignal,
		ReconnectInterval: time.Hour,
		dispatcher:        dispatcher.New(),
	}

	stateCh := make(chan *amqp.StateChanged, 1)
	stateCh <- &amqp.StateChanged{To: amqp.StateClosed, Err: errors.New("recovery exhausted")}

	m.handleStateChanges(stateCh)

	if !spy.hasInfoContaining("waiting") {
		t.Fatal("expected exhausted native recovery (StateClosed with a non-nil Err) to enter reconnectLoop, but it did not")
	}
}

// TestNew_DefaultsRecoveryWhenUnset 验证 New() 在调用方没有显式设置
// amqp.Config.Recovery 时，会自动填充一份带有效 MaxRetryCount 的默认值，
// 而不是让调用方安静地跑在"没有原生恢复"的状态下。
func TestDefaultRecovery_HasEffectiveReconnectionConfig(t *testing.T) {
	rec := defaultRecovery(0)
	if rec == nil || rec.ReconnectionConfig == nil {
		t.Fatal("defaultRecovery must return a non-nil Recovery with a non-nil ReconnectionConfig")
	}
	if rec.ReconnectionConfig.MaxRetryCount <= 0 {
		t.Fatalf("defaultRecovery must set a positive MaxRetryCount (amqp091-go treats <= 0 as recovery disabled), got %d",
			rec.ReconnectionConfig.MaxRetryCount)
	}
}
