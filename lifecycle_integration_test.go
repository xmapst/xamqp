package xamqp

// 本文件针对真实 broker 验证"生命周期"本身没有被破坏：连接断开、通道报错、
// 队列被删之后，Publisher/Consumer 是否还能自动恢复并继续正常收发。
//
// 与 leak_integration_test.go 一样由 XAMQP_TEST_AMQP_URL 门控。
//
// 这些用例刻意只断言"对外可观测的行为"（消息还能不能收发、确认还有没有），
// 不断言任何内部计数器——启用 amqp091-go 原生恢复之后，通道级故障可能
// 完全由底层库原地修复，xamqp 自己的 ReconnectionCount 不会变化。
// 用内部计数器判断"是否恢复"会得到假阴性（老的
// TestPublisherConfirmModeSurvivesReconnect 就是这么失效的）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitMQContainerEnv = "XAMQP_TEST_RABBITMQ_CONTAINER"

// managementAPI 从 AMQP 连接串推导出管理接口地址与凭据。
type managementAPI struct {
	base string
	user string
	pass string
}

func newManagementAPI(t *testing.T, amqpURL string) *managementAPI {
	t.Helper()
	u, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatalf("cannot parse amqp url: %v", err)
	}
	pass, _ := u.User.Password()
	host := u.Hostname()
	return &managementAPI{
		base: fmt.Sprintf("http://%s:15672", host),
		user: u.User.Username(),
		pass: pass,
	}
}

func (m *managementAPI) do(t *testing.T, method, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, m.base+path, nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.SetBasicAuth(m.user, m.pass)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Skipf("management API unreachable at %s (%v); skipping connection-drop test", m.base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

// killConnections 通过管理接口强制关闭所有名字里带 nameHint 的连接，
// 模拟真实的网络中断 / broker 侧踢连接。返回被关掉的连接数。
func (m *managementAPI) killConnections(t *testing.T, nameHint string) int {
	t.Helper()

	// RabbitMQ 管理插件的统计是异步汇集的，连接建立之后要过一小会儿才会
	// 出现在 /api/connections 里。不等待就直接查会拿到空列表，把"还没登记"
	// 误当成"没有连接可断"，用例就变成了假阴性（第一版正是这样静默跳过的）。
	var conns []struct {
		Name string `json:"name"`
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body := m.do(t, http.MethodGet, "/api/connections")
		if code != http.StatusOK {
			t.Skipf("cannot list connections (HTTP %d); skipping connection-drop test", code)
		}
		conns = conns[:0]
		if err := json.Unmarshal(body, &conns); err != nil {
			t.Fatalf("cannot decode connections: %v", err)
		}
		if len(conns) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	killed := 0
	for _, c := range conns {
		if nameHint != "" && !strings.Contains(c.Name, nameHint) {
			continue
		}
		code, _ := m.do(t, http.MethodDelete, "/api/connections/"+url.PathEscape(c.Name))
		if code == http.StatusNoContent || code == http.StatusOK {
			killed++
		}
	}
	return killed
}

// TestLifecycleChannelErrorRecovery 验证通道级错误（向不存在的 exchange 发布，
// broker 以 NOT_FOUND 关闭通道）之后，Publisher 依然可用，且确认模式仍然生效。
//
// 这正是老用例 TestPublisherConfirmModeSurvivesReconnect 想验证的东西，
// 但那个用例是靠"ReconnectionCount 是否变化"判断恢复的；启用原生恢复后
// 通道由底层库原地修复、该计数不再变化，于是它超时失败——失败的是判据，
// 不是功能。这里改成直接验证功能本身。
func TestLifecycleChannelErrorRecovery(t *testing.T) {
	silenceLogging(t)
	amqpURL := requireExternalBroker(t)

	queue := fmt.Sprintf("xamqp_lifecycle_chanerr_%d", time.Now().UnixNano())

	conn, err := NewConn(amqpURL,
		WithConnectionOptionsReconnectInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsConfirm,
	)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	// 先确认正常路径可用。
	if _, err := publisher.PublishWithDeferredConfirmWithContext(
		context.Background(), []byte("before"), []string{queue}); err != nil {
		t.Fatalf("baseline publish failed: %v", err)
	}

	// 制造通道级错误：向一个不存在的 exchange 发布。
	_ = publisher.Publish([]byte("boom"), []string{"unused"},
		WithPublishOptionsExchange(fmt.Sprintf("xamqp-missing-%d", time.Now().UnixNano())))

	// 恢复后必须仍然：能发布成功、拿到非 nil 的 DeferredConfirmation、并被 ack。
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().After(deadline) == false {
		confs, err := publisher.PublishWithDeferredConfirmWithContext(
			context.Background(), []byte("after"), []string{queue})
		if err != nil {
			lastErr = err
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if len(confs) != 1 || confs[0] == nil {
			lastErr = fmt.Errorf("confirm mode was lost after recovery (got %d confirmations, first nil=%v)",
				len(confs), len(confs) > 0 && confs[0] == nil)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		acked, werr := confs[0].WaitContext(ctx)
		cancel()
		if werr != nil {
			lastErr = fmt.Errorf("waiting for confirmation failed: %w", werr)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if !acked {
			// 恢复窗口内被 nack 是正常的（通道刚被 broker 关掉，这条消息
			// 根本没落地），继续重试而不是直接判定失败——只有"始终恢复不了"
			// 才是缺陷。
			lastErr = fmt.Errorf("message was nacked (channel likely still recovering)")
			time.Sleep(300 * time.Millisecond)
			continue
		}
		t.Logf("DATA channel-error recovery: publish+confirm works again after a NOT_FOUND channel error")
		return
	}
	t.Fatalf("publisher never recovered from a channel-level error within 30s: %v", lastErr)
}

// TestLifecycleConnectionDropRecovery 验证真实连接中断（通过管理接口强制踢掉
// 连接，等价于网络断开）之后，Consumer 能自动恢复并继续收到新消息，
// Publisher 也能继续发布。
//
// 这是最能代表"生命周期是否被破坏"的用例：它同时覆盖连接层恢复、通道层
// 重建、消费者重新订阅、以及重连之后拓扑是否还在。
func TestLifecycleConnectionDropRecovery(t *testing.T) {
	silenceLogging(t)
	amqpURL := requireExternalBroker(t)
	mgmt := newManagementAPI(t, amqpURL)

	queue := fmt.Sprintf("xamqp_lifecycle_drop_%d", time.Now().UnixNano())
	cleanupTestQueue(t, amqpURL, queue)

	conn, err := NewConn(amqpURL,
		WithConnectionOptionsReconnectInterval(300*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	received := make(chan string, 256)
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Run(func(d Delivery) Action {
		select {
		case received <- string(d.Body):
		default:
		}
		return Ack
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	// 断连之前先跑通一次，确认基线正常。
	if !publishAndAwait(t, publisher, queue, "pre-drop", received, 20*time.Second) {
		t.Fatal("baseline round-trip failed before any disruption")
	}
	t.Log("DATA connection-drop: baseline round-trip OK")

	killed := mgmt.killConnections(t, "")
	t.Logf("DATA connection-drop: forcibly closed %d broker connection(s)", killed)
	if killed == 0 {
		t.Skip("no connections were closed; cannot exercise the drop path")
	}

	// 恢复之后必须还能完整跑通一次收发。
	if !publishAndAwait(t, publisher, queue, "post-drop", received, 60*time.Second) {
		t.Fatal("consumer/publisher did NOT recover after the connection was forcibly dropped: " +
			"the reconnect lifecycle is broken")
	}
	t.Log("DATA connection-drop: round-trip OK again after forced disconnect (lifecycle recovered)")

	// 再断一次，确认恢复能力是可重复的，而不是只灵一次。
	killed = mgmt.killConnections(t, "")
	t.Logf("DATA connection-drop: forcibly closed %d broker connection(s) (second round)", killed)
	if !publishAndAwait(t, publisher, queue, "post-drop-2", received, 60*time.Second) {
		t.Fatal("consumer/publisher recovered once but NOT a second time: recovery is not repeatable")
	}
	t.Log("DATA connection-drop: round-trip OK after a second forced disconnect")
}

// TestLifecycleQueueDeletedRecovery 验证队列被删除（触发 basic.cancel）之后，
// Consumer 会重建通道、重新声明队列并重新订阅，继续收到新消息。
//
// 这条路径正是本次加了排空协程的那个分支，必须确认修复没有把恢复本身弄坏。
func TestLifecycleQueueDeletedRecovery(t *testing.T) {
	silenceLogging(t)
	amqpURL := requireExternalBroker(t)

	queue := fmt.Sprintf("xamqp_lifecycle_qdel_%d", time.Now().UnixNano())
	cleanupTestQueue(t, amqpURL, queue)

	conn, err := NewConn(amqpURL,
		WithConnectionOptionsReconnectInterval(300*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	received := make(chan string, 256)
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Run(func(d Delivery) Action {
		select {
		case received <- string(d.Body):
		default:
		}
		return Ack
	}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	if !publishAndAwait(t, publisher, queue, "pre-delete", received, 20*time.Second) {
		t.Fatal("baseline round-trip failed before deleting the queue")
	}
	t.Log("DATA queue-deleted: baseline round-trip OK")

	// 删除队列，触发 broker 向消费者下发 basic.cancel。
	mgmt := newManagementAPI(t, amqpURL)
	code, body := mgmt.do(t, http.MethodDelete, "/api/queues/%2F/"+url.PathEscape(queue))
	t.Logf("DATA queue-deleted: delete queue via management API -> HTTP %d", code)
	if code != http.StatusNoContent {
		t.Fatalf("delete queue via management API returned HTTP %d, want %d; body=%s",
			code, http.StatusNoContent, strings.TrimSpace(string(body)))
	}

	// 消费者应重建通道、重新声明队列并重新订阅。
	if !publishAndAwait(t, publisher, queue, "post-delete", received, 60*time.Second) {
		t.Fatal("consumer did NOT resubscribe after its queue was deleted: " +
			"the basic.cancel recovery path is broken")
	}
	t.Log("DATA queue-deleted: round-trip OK again after queue deletion (consumer resubscribed)")
}

// TestLifecycleRecoveryAfterNativeRecoveryExhaustion 验证 amqp091-go 的原生
// 恢复重试全部耗尽后，xamqp 能切换到自己的全连接重建，并带动 Consumer 和
// Publisher 的通道恢复。该路径需要停止/启动测试 RabbitMQ 容器，因此除
// XAMQP_TEST_AMQP_URL 外还需显式设置 XAMQP_TEST_RABBITMQ_CONTAINER。
func TestLifecycleRecoveryAfterNativeRecoveryExhaustion(t *testing.T) {
	silenceLogging(t)
	amqpURL := requireExternalBroker(t)
	container := os.Getenv(rabbitMQContainerEnv)
	if container == "" {
		t.Skipf("set %s to run broker restart recovery test", rabbitMQContainerEnv)
	}

	queue := fmt.Sprintf("xamqp_lifecycle_exhausted_%d", time.Now().UnixNano())
	cleanupTestQueue(t, amqpURL, queue)
	conn, err := NewConn(amqpURL, WithConnectionOptionsReconnectInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	received := make(chan string, 256)
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsQueueDurable)
	if err != nil {
		t.Fatalf("consumer failed: %v", err)
	}
	defer consumer.Close()
	if err := consumer.Run(func(d Delivery) Action {
		received <- string(d.Body)
		return Ack
	}); err != nil {
		t.Fatalf("consumer Run failed: %v", err)
	}
	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	if !publishAndAwait(t, publisher, queue, "before-full-reconnect", received, 10*time.Second) {
		t.Fatal("baseline round-trip failed")
	}
	initialGeneration := conn.connManager.ReconnectionCount()
	connectionStates := make(chan *amqp.StateChanged, 8)
	currentConnection(conn).NotifyStateChange(connectionStates)

	// 无论测试在哪一步失败，都尽最大努力把用户提供的 RabbitMQ 容器恢复运行。
	t.Cleanup(func() { _ = exec.Command("docker", "start", container).Run() })
	if out, err := exec.Command("docker", "stop", "--time", "2", container).CombinedOutput(); err != nil {
		t.Fatalf("stop RabbitMQ container failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	closedDeadline := time.Now().Add(10 * time.Second)
	for !conn.IsClosed() && time.Now().Before(closedDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !conn.IsClosed() {
		t.Fatal("connection never observed the stopped broker")
	}

	// 等到原生恢复明确进入带错误的终态，再启动 broker；这样后续
	// 恢复必然来自 xamqp 的全连接重建，不依赖重试抖动和机器速度。
	waitForNativeRecoveryExhaustion(t, connectionStates, 15*time.Second)
	if out, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
		t.Fatalf("start RabbitMQ container failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	readyDeadline := time.Now().Add(30 * time.Second)
	for {
		if err := exec.Command("docker", "exec", container, "rabbitmq-diagnostics", "-q", "ping").Run(); err == nil {
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatal("RabbitMQ container did not become ready after restart")
		}
		time.Sleep(500 * time.Millisecond)
	}

	reconnectDeadline := time.Now().Add(60 * time.Second)
	for conn.connManager.ReconnectionCount() <= initialGeneration && time.Now().Before(reconnectDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := conn.connManager.ReconnectionCount(); got <= initialGeneration {
		t.Fatalf("full reconnect did not occur: generation before=%d after=%d", initialGeneration, got)
	}
	if !publishAndAwait(t, publisher, queue, "after-full-reconnect", received, 60*time.Second) {
		t.Fatal("consumer/publisher did not recover after native recovery exhaustion and full reconnect")
	}
	t.Logf("DATA full reconnect: connection generation %d -> %d; round-trip recovered",
		initialGeneration, conn.connManager.ReconnectionCount())
}

// TestPublisherHonorsBrokerBlockedAlarm 通过降低 RabbitMQ 内存高水位制造真实
// connection.blocked / connection.unblocked 通知，验证 Publisher 会暂停并在告警
// 解除后恢复发布。测试结束会把当前测试容器的高水位恢复为其原配置值 0.6。
func TestPublisherHonorsBrokerBlockedAlarm(t *testing.T) {
	silenceLogging(t)
	amqpURL := requireExternalBroker(t)
	container := os.Getenv(rabbitMQContainerEnv)
	if container == "" {
		t.Skipf("set %s to run broker resource-alarm test", rabbitMQContainerEnv)
	}

	conn, err := NewConn(amqpURL)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()
	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("publisher failed: %v", err)
	}
	defer publisher.Close()

	restore := func() error {
		return exec.Command("docker", "exec", container, "rabbitmqctl",
			"set_vm_memory_high_watermark", "0.6").Run()
	}
	t.Cleanup(func() {
		if err := restore(); err != nil {
			t.Logf("failed to restore RabbitMQ memory watermark: %v", err)
		}
	})
	if out, err := exec.Command("docker", "exec", container, "rabbitmqctl",
		"set_vm_memory_high_watermark", "0.000001").CombinedOutput(); err != nil {
		t.Fatalf("set RabbitMQ memory alarm failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	blockedDeadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err = publisher.PublishWithContext(ctx, []byte("blocked-probe"), []string{"xamqp_blocked_probe"})
		cancel()
		if errors.Is(err, ErrPublishBlocked) {
			break
		}
		if time.Now().After(blockedDeadline) {
			t.Fatalf("Publisher never observed RabbitMQ connection.blocked; last publish error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := restore(); err != nil {
		t.Fatalf("clear RabbitMQ memory alarm failed: %v", err)
	}
	unblockedDeadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = publisher.PublishWithContext(ctx, []byte("unblocked-probe"), []string{"xamqp_blocked_probe"})
		cancel()
		if err == nil {
			break
		}
		if !errors.Is(err, ErrPublishBlocked) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("publish after clearing alarm failed: %v", err)
		}
		if time.Now().After(unblockedDeadline) {
			t.Fatalf("Publisher remained blocked after RabbitMQ alarm cleared: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("DATA broker blocking: publisher paused on memory alarm and resumed after alarm cleared")
}

// publishAndAwait 反复发布带标记的消息，直到消费者收到同一标记或超时。
//
// 之所以要循环重发：恢复期间发布本身可能失败，或消息落在了尚未重建好的
// 队列上而被丢弃，单发一条无法区分"没恢复"和"恰好丢了这一条"。
func publishAndAwait(t *testing.T, p *Publisher, queue, marker string, received <-chan string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := p.Publish([]byte(marker), []string{queue}); err != nil {
			t.Logf("  publish(%s) failed, will retry: %v", marker, err)
		}
		select {
		case body := <-received:
			if body == marker {
				return true
			}
		case <-ticker.C:
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}
