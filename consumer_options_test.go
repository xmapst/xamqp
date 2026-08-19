package xamqp

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestWithConsumerOptionsQueueArgsMerges 验证多次调用 WithConsumerOptionsQueueArgs
// 是合并（保留先前已设置的键），而不是整体替换掉 Args map。
func TestWithConsumerOptionsQueueArgsMerges(t *testing.T) {
	options := getDefaultConsumerOptions("q")

	WithConsumerOptionsQueueArgs(amqp.Table{"x-message-ttl": int32(1000)})(&options)
	WithConsumerOptionsQueueArgs(amqp.Table{"x-max-length": int32(100)})(&options)

	if got, ok := options.QueueOptions.Args["x-message-ttl"]; !ok || got != int32(1000) {
		t.Errorf("x-message-ttl missing or overwritten after second call: %v (present=%v)", got, ok)
	}
	if got, ok := options.QueueOptions.Args["x-max-length"]; !ok || got != int32(100) {
		t.Errorf("x-max-length missing after second call: %v (present=%v)", got, ok)
	}
}

// TestWithConsumerOptionsQueueQuorumDoesNotClobberOtherArgs 验证
// WithConsumerOptionsQueueQuorum 只新增 x-queue-type，不会清掉此前
// 已经通过 WithConsumerOptionsQueueArgs 设置的其他参数。
func TestWithConsumerOptionsQueueQuorumDoesNotClobberOtherArgs(t *testing.T) {
	options := getDefaultConsumerOptions("q")

	WithConsumerOptionsQueueArgs(amqp.Table{"x-message-ttl": int32(1000)})(&options)
	WithConsumerOptionsQueueQuorum(&options)

	if got := options.QueueOptions.Args["x-queue-type"]; got != "quorum" {
		t.Errorf("x-queue-type = %v, want \"quorum\"", got)
	}
	if got, ok := options.QueueOptions.Args["x-message-ttl"]; !ok || got != int32(1000) {
		t.Errorf("WithConsumerOptionsQueueQuorum clobbered x-message-ttl: %v (present=%v)", got, ok)
	}
}

// TestWithConsumerOptionsExchangeArgsMerges 验证交换机参数同样是合并语义。
func TestWithConsumerOptionsExchangeArgsMerges(t *testing.T) {
	options := getDefaultConsumerOptions("q")

	WithConsumerOptionsExchangeArgs(amqp.Table{"alternate-exchange": "ax"})(&options)
	WithConsumerOptionsExchangeArgs(amqp.Table{"x-custom": "v"})(&options)

	if len(options.ExchangeOptions) != 1 {
		t.Fatalf("expected exactly one ExchangeOptions entry, got %d", len(options.ExchangeOptions))
	}
	args := options.ExchangeOptions[0].Args
	if args["alternate-exchange"] != "ax" || args["x-custom"] != "v" {
		t.Errorf("exchange args not merged correctly: %#v", args)
	}
}

// TestWithConsumerOptionsQueueMessageExpirationPreservesExistingArgs 验证
// 设置队列级 TTL 不会清空此前设置的其他 Args。
func TestWithConsumerOptionsQueueMessageExpirationPreservesExistingArgs(t *testing.T) {
	options := getDefaultConsumerOptions("q")

	WithConsumerOptionsQueueArgs(amqp.Table{"x-max-length": int32(50)})(&options)
	WithConsumerOptionsQueueMessageExpiration(1000)(&options)

	if _, ok := options.QueueOptions.Args["x-max-length"]; !ok {
		t.Error("WithConsumerOptionsQueueMessageExpiration clobbered a pre-existing arg")
	}
	if _, ok := options.QueueOptions.Args["x-message-ttl"]; !ok {
		t.Error("x-message-ttl was not set")
	}
}
