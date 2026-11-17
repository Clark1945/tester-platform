// Package notify 發送告警到 webhook，並對同類告警節流。
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	sendTimeout = 3 * time.Second
	// 告警送失敗後，隔多久允許下一則同類告警再試一次
	retryAfter = 5 * time.Second
)

// Alert 是一則告警。Kind 是節流的單位：同一種 Kind 在間隔內只會送出一次。
type Alert struct {
	Kind    string
	Message string
	Fields  map[string]any
}

// payload 是送給 webhook 的 JSON。
type payload struct {
	Service    string         `json:"service"`
	Kind       string         `json:"kind"`
	Message    string         `json:"message"`
	Time       time.Time      `json:"time"`
	Suppressed int            `json:"suppressed_since_last"` // 上次送出後，被節流掉幾則同類告警
	Fields     map[string]any `json:"fields,omitempty"`
}

// Notifier 可被多個 goroutine 同時呼叫。
type Notifier struct {
	url         string
	service     string
	minInterval time.Duration
	client      *http.Client
	log         *slog.Logger
	now         func() time.Time // 可替換的時鐘，測試節流時不用真的等一分鐘

	mu         sync.Mutex
	lastSent   map[string]time.Time
	suppressed map[string]int
}

// New 建立 notifier。url 為空時告警只寫 log。
func New(url, service string, minInterval time.Duration, log *slog.Logger) *Notifier {
	return &Notifier{
		url:         url,
		service:     service,
		minInterval: minInterval,
		client:      &http.Client{Timeout: sendTimeout},
		log:         log,
		now:         time.Now,
		lastSent:    map[string]time.Time{},
		suppressed:  map[string]int{},
	}
}

// Notify 送出告警；同類告警在 minInterval 內只送一次，其餘只累計次數。
//
// 不回傳錯誤：告警失敗不該影響訊息處理的結果，所以在這裡記錄後就結束。
// 節流的理由：資料庫掛掉時可能一秒內有上千筆訊息失敗，逐筆通知只會讓值班的人
// 把告警靜音——那比沒有告警更糟。
func (n *Notifier) Notify(ctx context.Context, a Alert) {
	suppressed, ok := n.admit(a.Kind)
	if !ok {
		return
	}

	p := payload{
		Service:    n.service,
		Kind:       a.Kind,
		Message:    a.Message,
		Time:       n.now().UTC(),
		Suppressed: suppressed,
		Fields:     a.Fields,
	}
	n.log.Warn("alert", "kind", a.Kind, "message", a.Message, "suppressed_since_last", suppressed, "fields", a.Fields)

	if n.url == "" {
		return
	}
	if err := n.send(ctx, p); err != nil {
		n.log.Error("webhook delivery failed", "kind", a.Kind, "error", err)
		n.sendFailed(a.Kind, suppressed)
	}
}

// sendFailed 在告警送不出去時，把節流的狀態調回來。
//
// admit 在送出之前就開始計算節流間隔。如果送失敗卻什麼都不做，這則告警等於沒發出，
// 而接下來一整個間隔內的同類告警又都被節流掉——webhook 一次短暫的失敗，會換來一整分鐘的靜默。
//
// 做法是把下次可以送的時間提前到 retryAfter 之後，並把這一則（連同它帶著的累計數）算回
// 「尚未通知」的數量。不是立刻重試：webhook 真的掛掉時，每一則告警都去等一次逾時，
// 只會拖慢訊息處理。
func (n *Notifier) sendFailed(kind string, carried int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.suppressed[kind] += carried + 1
	if n.minInterval > retryAfter {
		n.lastSent[kind] = n.now().Add(retryAfter - n.minInterval)
	}
}

// admit 決定這則告警要不要送。放行時一併回傳並清空被節流的累計數。
// 只有這一小段需要鎖；HTTP 請求在鎖外進行，不會讓其他 goroutine 排隊等網路。
func (n *Notifier) admit(kind string) (suppressed int, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := n.now()
	if last, sent := n.lastSent[kind]; sent && now.Sub(last) < n.minInterval {
		n.suppressed[kind]++
		return 0, false
	}

	suppressed = n.suppressed[kind]
	n.suppressed[kind] = 0
	n.lastSent[kind] = now
	return suppressed, true
}

func (n *Notifier) send(ctx context.Context, p payload) error {
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode alert: %w", err)
	}

	// 用不受關閉訊號影響的 context：服務關閉途中產生的告警也要送得出去
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer resp.Body.Close()
	// 把 body 讀完，連線才能被 keep-alive 重複使用
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}
