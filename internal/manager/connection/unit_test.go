package connection

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMaskPassword(t *testing.T) {
	m := newTestManager()

	t.Run("valid URL redacts the password", func(t *testing.T) {
		got := m.maskPassword("amqp://user:secret@localhost:5672/")
		if got == "amqp://user:secret@localhost:5672/" {
			t.Fatalf("maskPassword did not redact the password: %s", got)
		}
		if want := "secret"; contains(got, want) {
			t.Fatalf("maskPassword leaked the password into the output: %s", got)
		}
	})

	t.Run("unparseable URL returns a safe placeholder, not the raw input", func(t *testing.T) {
		// 一个带控制字符的畸形 URL：net/url.Parse 会报错。
		malformed := "amqp://user:hunter2@\x7f/"
		got := m.maskPassword(malformed)
		if got == malformed {
			t.Fatalf("maskPassword returned the raw (credential-bearing) input on parse failure: %s", got)
		}
		if contains(got, "hunter2") {
			t.Fatalf("maskPassword leaked the password on parse failure: %s", got)
		}
	})
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestSendLatestNonBlockingKeepsLatest 验证非阻塞广播在接收者不消费时，
// 保留的是最新一次的值，而不是最早一次的值或直接丢弃。
func TestSendLatestNonBlockingKeepsLatest(t *testing.T) {
	ch := make(chan amqp.Blocking, 1)

	sendLatestNonBlocking(ch, amqp.Blocking{Active: true, Reason: "first"})
	sendLatestNonBlocking(ch, amqp.Blocking{Active: false, Reason: "second"})

	select {
	case got := <-ch:
		if got.Reason != "second" || got.Active {
			t.Fatalf("expected the latest value {Active:false, Reason:second}, got %+v", got)
		}
	default:
		t.Fatal("expected a buffered value to be available, channel was empty")
	}

	// channel 应该正好被清空，没有残留第三个值。
	select {
	case v := <-ch:
		t.Fatalf("expected channel to be drained, but got an extra value: %+v", v)
	default:
	}
}

// TestReadUniversalBlockReceiver_SlowSubscriberSeesLatestState 端到端验证：
// 一个从不主动消费的订阅者，在广播了两次状态变化之后，最终读到的是最新状态，
// 而不是错过第二次变化、永远停留在第一次的状态上。
func TestReadUniversalBlockReceiver_SlowSubscriberSeesLatestState(t *testing.T) {
	m := newTestManager()

	receiver := m.NotifyBlockedSafe(make(chan amqp.Blocking, 1))

	universal := make(chan amqp.Blocking)
	go m.readUniversalBlockReceiver(universal)

	// 连续广播两次状态变化，中途不读取 receiver。
	//
	// 注意：universal 上的第二次 send 返回，只保证广播协程已经"收到"
	// 第二个事件（unbuffered channel 的握手语义），并不保证它已经跑完
	// sendLatestNonBlocking 把值写进 receiver——那是另一个 goroutine
	// 里独立发生的后续步骤。所以下面用带超时的轮询代替一次性阻塞读，
	// 避免把"两个独立 goroutine 之间没有 happens-before 保证"误判成
	// 生产代码的 bug。
	universal <- amqp.Blocking{Active: true, Reason: "blocked"}
	universal <- amqp.Blocking{Active: false, Reason: "unblocked"}

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case got := <-receiver:
			if !got.Active {
				return // 观察到了最新状态，通过
			}
			// 读到的是还没被覆盖掉的第一次状态，继续轮询等待覆盖发生。
		case <-time.After(time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting to observe the latest (unblocked) state; " +
				"the slow subscriber may have permanently missed the state transition")
		}
	}
}
