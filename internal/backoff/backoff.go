package backoff

import (
	"math/rand/v2"
	"time"
)

// maxAttempt 指数增长的封顶档位：2^4 = 16 倍 base，与重连成功后重新计数配合，
// 保证退避时间不会无限增长下去。
const maxAttempt = 4

// Delay 计算第 attempt 次重试前应等待的时长。
//
// 以 base 为基础按 2^attempt 指数增长，最高封顶 16 倍 base，
// 再减去至多 25% 的随机抖动。抖动的意义：避免同一 Broker 下的大量客户端
// 在网络恢复或 Broker 重启后，因为退避时间完全一致而同一时刻集中重连，
// 加剧 Broker 的瞬时压力（惊群效应）。attempt 从 0 开始计数。
func Delay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	capped := min(attempt, maxAttempt)
	delay := base << capped

	jitter := time.Duration(rand.Int64N(int64(delay)/4 + 1))
	return delay - jitter
}
