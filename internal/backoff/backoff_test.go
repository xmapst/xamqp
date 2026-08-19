package backoff

import (
	"testing"
	"time"
)

func TestDelayGrowsAndCaps(t *testing.T) {
	base := 100 * time.Millisecond

	// attempt 0：应接近 base（允许最多 25% 抖动）。
	d0 := Delay(base, 0)
	if d0 <= 0 || d0 > base {
		t.Fatalf("Delay(base, 0) = %s, want in (0, %s]", d0, base)
	}

	// 随着 attempt 增大，理论上限（未减抖动前）应按 2 的幂增长，直到 16x 封顶。
	for attempt, wantMax := range map[int]time.Duration{
		1:  2 * base,
		2:  4 * base,
		3:  8 * base,
		4:  16 * base,
		10: 16 * base, // 超过封顶档位后不再继续增长
	} {
		d := Delay(base, attempt)
		if d <= 0 || d > wantMax {
			t.Errorf("Delay(base, %d) = %s, want in (0, %s]", attempt, d, wantMax)
		}
	}
}

func TestDelayJitterVaries(t *testing.T) {
	base := 1 * time.Second
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[Delay(base, 3)] = true
	}
	// 抖动是随机的，50 次采样里几乎不可能全部相同；
	// 这里只做一个弱校验，避免样本量小导致偶发失败的同时仍能捕获"忘记加抖动"的回归。
	if len(seen) < 2 {
		t.Errorf("Delay appears deterministic across %d samples: %v", 50, seen)
	}
}

func TestDelayZeroBase(t *testing.T) {
	if d := Delay(0, 0); d != 0 {
		t.Errorf("Delay(0, 0) = %s, want 0", d)
	}
}
