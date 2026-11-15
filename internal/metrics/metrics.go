// Package metrics 提供行程內的計數器與 /metrics 輸出。
//
// 計數器存在記憶體，行程重啟就歸零。所以它們適合「這個行程啟動以來發生了什麼」，
// 不適合當成跨重啟的帳本——那要以資料庫的實際筆數為準（見 cmd/verify）。
package metrics

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// APICounters 是 api 服務的計數器。欄位是 atomic，可被多個 goroutine 同時累加。
// 結構內含 atomic 值，只能以指標傳遞，不可複製。
type APICounters struct {
	Accepted      atomic.Int64 // broker 已確認、回 202 的筆數
	Rejected4xx   atomic.Int64 // 因 payload / device 問題被拒絕的筆數
	PublishFailed atomic.Int64 // 未獲 broker 確認、回 503 的筆數
}

// Snapshot 回傳目前的數值。
func (c *APICounters) Snapshot() map[string]any {
	return map[string]any{
		"accepted":       c.Accepted.Load(),
		"rejected_4xx":   c.Rejected4xx.Load(),
		"publish_failed": c.PublishFailed.Load(),
	}
}

// ReceiverCounters 是 receiver 服務的計數器。
//
// 每筆收到的訊息最後一定落在 Written / Duplicate / DLQ / Requeued 其中之一：
//
//	Received = Written + Duplicate + DLQ + Requeued + 處理中
type ReceiverCounters struct {
	Received  atomic.Int64 // 從 queue 收到的訊息數（含重新投遞）
	Written   atomic.Int64 // 新寫入資料庫
	Duplicate atomic.Int64 // 資料庫已有（重送）
	Retried   atomic.Int64 // 退避重試的次數
	DLQ       atomic.Int64 // 進了 dead letter queue
	Requeued  atomic.Int64 // 還給 queue（服務關閉中，或 DLQ 發不出去）

	lastWrittenAt atomic.Int64 // UnixNano；0 代表尚未寫入過
}

// MarkWritten 記錄一次成功寫入。
func (c *ReceiverCounters) MarkWritten(now time.Time) {
	c.Written.Add(1)
	c.lastWrittenAt.Store(now.UnixNano())
}

// LastWrittenAt 回傳最後一次成功寫入的時間；從未寫入時回傳零值。
func (c *ReceiverCounters) LastWrittenAt() time.Time {
	ns := c.lastWrittenAt.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

// Settled 是已有明確結局的訊息數，watchdog 用它判斷 consumer 是否還有進展。
func (c *ReceiverCounters) Settled() int64 {
	return c.Written.Load() + c.Duplicate.Load() + c.DLQ.Load()
}

// Snapshot 回傳目前的數值。
func (c *ReceiverCounters) Snapshot() map[string]any {
	var last any // 從未寫入時輸出 null
	if t := c.LastWrittenAt(); !t.IsZero() {
		last = t.Format(time.RFC3339Nano)
	}
	return map[string]any{
		"received":        c.Received.Load(),
		"written":         c.Written.Load(),
		"duplicate":       c.Duplicate.Load(),
		"retried":         c.Retried.Load(),
		"dlq":             c.DLQ.Load(),
		"requeued":        c.Requeued.Load(),
		"last_written_at": last,
	}
}

// Handler 以 JSON 輸出計數器。
func Handler(snapshot func() map[string]any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot()) // 寫入失敗代表 client 斷線，無從補救
	})
}
