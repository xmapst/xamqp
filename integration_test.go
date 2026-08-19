package xamqp

// 本文件是针对真实 RabbitMQ broker 的集成测试，默认跳过。
// 设置环境变量 ENABLE_DOCKER_INTEGRATION_TESTS=TRUE 后，
// go test -v ./... 才会真正启动一个 Docker 容器并执行这些用例。
//
// 注意：这些用例是对照 go-rabbitmq（github.com/wagslane/go-rabbitmq）
// 的 integration_test.go 编写的，但按 xamqp 实际的 API 改写——两者的
// 选项函数名、Publisher/Consumer 行为细节并不完全一致，不能直接照搬。
// 这份文件在编写时所在的开发环境没有可用的 Docker/RabbitMQ，
// 没有条件真正跑一遍验证；如果发现编译或运行问题，请对照
// internal/manager/connection、internal/manager/channel、publish.go、
// consume.go 的实际实现修正。

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const enableDockerIntegrationTestsFlag = `ENABLE_DOCKER_INTEGRATION_TESTS`

// testLogger 是一个最小化的 ILogger 实现，把日志转发给 t.Logf。
type testLogger struct {
	logf func(format string, args ...any)
}

func (l testLogger) Errorf(format string, args ...any) { l.logf("ERROR: "+format, args...) }
func (l testLogger) Warnf(format string, args ...any)  { l.logf("WARN: "+format, args...) }
func (l testLogger) Infof(format string, args ...any)  { l.logf("INFO: "+format, args...) }
func (l testLogger) Debugf(format string, args ...any) { l.logf("DEBUG: "+format, args...) }

// prepareDockerTest 检查是否启用了集成测试；启用时用 docker 启动一个
// 一次性的 RabbitMQ 容器，返回连接串，并注册测试结束时的清理。
func prepareDockerTest(t *testing.T) (connStr string) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach",
		"--publish=5672:5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
	if err != nil {
		t.Fatalf("error launching rabbitmq in docker: %v (output: %s)", err, string(out))
	}
	containerID := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		t.Logf("shutting down container %q", containerID)
		if err := exec.Command("docker", "rm", "--force", containerID).Run(); err != nil {
			t.Logf("failed to stop container: %v", err)
		}
	})
	return "amqp://guest:guest@localhost:5672/"
}

// waitForHealthyAmqp 轮询直到能够成功连接并发出一次 ping 发布，
// 避免容器刚起来、AMQP 端口尚未就绪时测试立刻失败。
func waitForHealthyAmqp(t *testing.T, connStr string, optionFuncs ...func(*ConnectionOptions)) *Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	quietLogger := testLogger{logf: func(string, ...any) {}}

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for a healthy amqp connection: %v", lastErr)
			return nil
		case <-ticker.C:
			opts := append([]func(*ConnectionOptions){WithConnectionOptionsLogger(quietLogger)}, optionFuncs...)
			conn, err := NewConn(connStr, opts...)
			if err != nil {
				lastErr = err
				t.Logf("connection attempt failed, retrying: %v", err)
				continue
			}

			pingErr := func() error {
				pub, err := NewPublisher(conn, WithPublisherOptionsLogger(quietLogger))
				if err != nil {
					return fmt.Errorf("failed to set up ping publisher: %w", err)
				}
				defer pub.Close()
				return pub.PublishWithContext(ctx, []byte("ping"), []string{"__xamqp_ping__"})
			}()
			if pingErr != nil {
				lastErr = pingErr
				t.Logf("ping publish failed, retrying: %v", pingErr)
				_ = conn.Close()
				continue
			}
			return conn
		}
	}
}

// TestSimplePubSub 验证最基本的发布/消费闭环：声明队列、发布一条消息、
// 消费者能收到并 Ack。
func TestSimplePubSub(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	const queueName = "xamqp_integration_simple_pubsub"

	consumer, err := NewConsumer(conn, queueName,
		WithConsumerOptionsLogger(testLogger{logf: t.Logf}),
		WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		t.Fatalf("error creating consumer: %v", err)
	}
	defer consumer.CloseWithContext(context.Background())

	consumed := make(chan Delivery, 1)
	err = consumer.Run(func(d Delivery) Action {
		select {
		case consumed <- d:
		default:
		}
		return Ack
	})
	if err != nil {
		t.Fatalf("consumer.Run failed: %v", err)
	}

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(testLogger{logf: t.Logf}))
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// 消费者的 goroutine 是异步启动的，第一次发布时可能还没就绪，
	// 所以循环重试发布直到看到消息被消费。
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the published message to round-trip")
		case <-ticker.C:
			if err := publisher.Publish([]byte("hello xamqp"), []string{queueName}); err != nil {
				t.Logf("publish attempt failed, retrying: %v", err)
			}
		case d := <-consumed:
			if string(d.Body) != "hello xamqp" {
				t.Fatalf("unexpected message body: %q", string(d.Body))
			}
			return
		}
	}
}

// TestCloseIsIdempotent 验证 Conn/Consumer/Publisher 的 Close 系列方法
// 重复调用是安全的（不会 panic、不会死锁）。
func TestCloseIsIdempotent(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)

	consumer, err := NewConsumer(conn, "xamqp_integration_close_idempotent")
	if err != nil {
		t.Fatalf("error creating consumer: %v", err)
	}
	consumer.Close()
	consumer.Close()
	consumer.CloseWithContext(context.Background())

	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}
	publisher.Close()
	publisher.Close()

	if err := conn.Close(); err != nil {
		t.Fatalf("error closing connection: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second connection close returned an error: %v", err)
	}
}

// TestPublisherConfirmModeSurvivesReconnect 验证 confirm 模式在通道
// 因错误重连后依然生效：重连完成后再发布的消息应当仍然能拿到非 nil 的
// DeferredConfirmation，而不是静默丢失确认能力。
//
// 通过反复向一个不存在的 exchange 发布触发通道被 broker 关闭、进而重连，
// 用 chanManager.ReconnectionCount() 的变化判断重连是否真的发生过。
func TestPublisherConfirmModeSurvivesReconnect(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr, WithConnectionOptionsReconnectInterval(50*time.Millisecond))
	defer conn.Close()

	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}
	defer publisher.Close()

	reconnectionCount := publisher.chanManager.ReconnectionCount()

	// 向一个不存在的 exchange 发布会导致 broker 以 NOT_FOUND 关闭通道，
	// 触发 channel.Manager 的自动重连。
	_ = publisher.Publish([]byte("trigger reconnect"), []string{"unused"},
		WithPublishOptionsExchange(fmt.Sprintf("xamqp-integration-missing-exchange-%d", time.Now().UnixNano())))

	deadline := time.Now().Add(10 * time.Second)
	for publisher.chanManager.ReconnectionCount() == reconnectionCount {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the channel to reconnect")
		}
		time.Sleep(50 * time.Millisecond)
	}

	confirmations, err := publisher.PublishWithDeferredConfirmWithContext(
		context.Background(), []byte("confirmed after reconnect"), []string{"unused"},
	)
	if err != nil {
		t.Fatalf("publish after reconnect failed: %v", err)
	}
	if len(confirmations) != 1 || confirmations[0] == nil {
		t.Fatal("expected a non-nil DeferredConfirmation after reconnect; confirm mode was not restored on the new channel")
	}
	if _, err := confirmations[0].WaitContext(context.Background()); err != nil {
		t.Fatalf("error waiting for confirmation after reconnect: %v", err)
	}
}

// TestPublisherConfirmationsAllArriveExactlyOnce 验证开启确认模式后，
// NotifyPublish 对每一条发布的消息都恰好收到一次确认，覆盖全部
// DeliveryTag，且并发派发不会丢事件或重复派发。
//
// 注意：NotifyPublish 的 handler 是并发调用的（详见 publish.go 的文档），
// 不保证到达顺序，所以这里只校验"集合完整、无重复"，不校验顺序。
func TestPublisherConfirmationsAllArriveExactlyOnce(t *testing.T) {
	const messageCount = 50

	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	publisher, err := NewPublisher(conn, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}
	defer publisher.Close()

	var mu sync.Mutex
	seen := make(map[uint64]int)
	publisher.NotifyPublish(func(c Confirmation) {
		mu.Lock()
		seen[c.DeliveryTag]++
		mu.Unlock()
	})

	for i := range messageCount {
		if err := publisher.Publish([]byte("concurrent"), []string{"xamqp_integration_unrouted"}); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		count := len(seen)
		mu.Unlock()
		if count >= messageCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for confirmations: got %d of %d distinct delivery tags", count, messageCount)
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != messageCount {
		t.Fatalf("expected exactly %d distinct delivery tags, got %d: %v", messageCount, len(seen), seen)
	}
	for tag, count := range seen {
		if count != 1 {
			t.Fatalf("delivery tag %d was confirmed %d times, want exactly 1", tag, count)
		}
	}
}
