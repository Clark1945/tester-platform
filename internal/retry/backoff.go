// Package retry 提供退避（backoff）計算與可取消的等待。
// 階段 3 會在這裡加入錯誤分類。
package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

// Backoff 是指數退避的參數：第 n 次等待約為 Base * 2^(n-1)，上限 Max。
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Delay 回傳第 attempt 次（從 1 起算）失敗後要等多久。
// 結果落在 [d/2, d]（equal jitter）：保留一半的固定等待避免 hot loop，
// 另一半隨機化，避免多個 client 在同一瞬間一起重試（thundering herd）。
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.ceiling(attempt)
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + rand.N(d-half+1)
}

// ceiling 是不含 jitter 的等待上限。
func (b Backoff) ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= b.Max || d <= 0 { // d <= 0：連乘溢位
			return b.Max
		}
	}
	return min(d, b.Max)
}

// Sleep 等待 d，或在 ctx 取消時提前返回 ctx 的錯誤。
// 用 time.Sleep 的話，服務在關閉時會被卡到睡醒為止。
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
