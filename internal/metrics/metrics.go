// Package metrics 提供行程內的計數器，並以兩種格式輸出：
//
//   - /metrics       Prometheus 格式，供 Prometheus 抓取、Grafana 繪圖
//   - /metrics.json  JSON，供 verify / chaos 對帳時讀取
//
// 兩者讀的是同一組數值，不會有兩份帳。
//
// 計數器存在記憶體，行程重啟就歸零。所以它們適合「這個行程啟動以來發生了什麼」，
// 不適合當成跨重啟的帳本——那要以資料庫的實際筆數為準（見 cmd/verify）。
// Prometheus 的 rate() 會自動處理歸零，所以畫成「每秒幾筆」的趨勢圖不受重啟影響。
package metrics

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// APIOutcome 是一個 POST /v1/readings 請求的結局。
type APIOutcome string

const (
	APIAccepted      APIOutcome = "accepted"       // broker 已確認，回 202
	APIRejected      APIOutcome = "rejected"       // payload / device 有問題，回 4xx
	APIPublishFailed APIOutcome = "publish_failed" // 未獲 broker 確認，回 503
)

// APICounters 是 api 服務的指標。欄位是 atomic，可被多個 goroutine 同時累加。
// 結構內含 atomic 值，只能以指標傳遞，不可複製。
//
// 零值可用（測試方便），但只有 NewAPICounters 建立的才會記錄延遲分布並能輸出 Prometheus 格式。
type APICounters struct {
	Accepted      atomic.Int64
	Rejected4xx   atomic.Int64
	PublishFailed atomic.Int64

	requestSeconds *prometheus.HistogramVec
	registry       *prometheus.Registry
}

// NewAPICounters 建立 api 的指標並註冊到自己的 registry。
//
// 用獨立的 registry 而不是 client_golang 的全域預設 registry：
// 不依賴全域狀態，測試之間也不會互相干擾。
func NewAPICounters() *APICounters {
	c := &APICounters{
		requestSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tester_api_request_duration_seconds",
			Help:    "Time to handle POST /v1/readings, including the wait for the publisher confirm.",
			Buckets: prometheus.DefBuckets, // 5ms … 10s
		}, []string{"outcome"}),
		registry: newRegistry(),
	}

	// 計數器以 CounterFunc 包裝既有的 atomic 值：Prometheus 抓取時才讀，
	// 和 /metrics.json、watchdog 讀到的是同一個數字。
	const name, help = "tester_api_readings_total", "Readings handled by the API, by outcome."
	c.registry.MustRegister(
		counterFunc(name, help, string(APIAccepted), &c.Accepted),
		counterFunc(name, help, string(APIRejected), &c.Rejected4xx),
		counterFunc(name, help, string(APIPublishFailed), &c.PublishFailed),
		c.requestSeconds,
	)
	return c
}

// Observe 記錄一個請求的結局與處理時間。
func (c *APICounters) Observe(outcome APIOutcome, elapsed time.Duration) {
	switch outcome {
	case APIAccepted:
		c.Accepted.Add(1)
	case APIRejected:
		c.Rejected4xx.Add(1)
	case APIPublishFailed:
		c.PublishFailed.Add(1)
	}
	if c.requestSeconds != nil {
		c.requestSeconds.WithLabelValues(string(outcome)).Observe(elapsed.Seconds())
	}
}

// Snapshot 回傳目前的數值（JSON 輸出用）。
func (c *APICounters) Snapshot() map[string]any {
	return map[string]any{
		"accepted":       c.Accepted.Load(),
		"rejected_4xx":   c.Rejected4xx.Load(),
		"publish_failed": c.PublishFailed.Load(),
	}
}

// PrometheusHandler 以 Prometheus 格式輸出指標。
func (c *APICounters) PrometheusHandler() http.Handler {
	return prometheusHandler(c.registry)
}

// ReceiverOutcome 是一筆訊息的結局。
type ReceiverOutcome string

const (
	ReceiverWritten   ReceiverOutcome = "written"   // 新寫入資料庫
	ReceiverDuplicate ReceiverOutcome = "duplicate" // 資料庫已有（重送）
	ReceiverDLQ       ReceiverOutcome = "dlq"       // 進了 dead letter queue
	ReceiverRequeued  ReceiverOutcome = "requeued"  // 還給 queue（服務關閉中，或 DLQ 發不出去）
)

// ReceiverCounters 是 receiver 服務的指標。
//
// 每筆收到的訊息最後一定落在 Written / Duplicate / DLQ / Requeued 其中之一：
//
//	Received = Written + Duplicate + DLQ + Requeued + 處理中
type ReceiverCounters struct {
	Received  atomic.Int64 // 從 queue 收到的訊息數（含重新投遞）
	Written   atomic.Int64
	Duplicate atomic.Int64
	Retried   atomic.Int64 // 退避重試的次數
	DLQ       atomic.Int64
	Requeued  atomic.Int64

	lastWrittenAt atomic.Int64 // UnixNano；0 代表尚未寫入過

	processSeconds *prometheus.HistogramVec
	registry       *prometheus.Registry
}

// NewReceiverCounters 建立 receiver 的指標並註冊到自己的 registry。
func NewReceiverCounters() *ReceiverCounters {
	c := &ReceiverCounters{
		processSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tester_receiver_process_duration_seconds",
			Help: "Time from receiving a message to settling it, including retries and backoff.",
			// 含重試的處理時間可能長達數十秒，所以上限比預設的 10 秒高
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"outcome"}),
		registry: newRegistry(),
	}

	const name, help = "tester_receiver_messages_total", "Messages settled by the receiver, by outcome."
	c.registry.MustRegister(
		counterFunc(name, help, string(ReceiverWritten), &c.Written),
		counterFunc(name, help, string(ReceiverDuplicate), &c.Duplicate),
		counterFunc(name, help, string(ReceiverDLQ), &c.DLQ),
		counterFunc(name, help, string(ReceiverRequeued), &c.Requeued),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "tester_receiver_messages_received_total",
			Help: "Messages delivered to the receiver, including redeliveries.",
		}, func() float64 { return float64(c.Received.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "tester_receiver_retries_total",
			Help: "Write attempts that failed and were retried after a backoff.",
		}, func() float64 { return float64(c.Retried.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "tester_receiver_last_written_timestamp_seconds",
			Help: "Unix time of the last successful database write; 0 if nothing has been written yet.",
		}, func() float64 { return float64(c.lastWrittenAt.Load()) / 1e9 }),
		c.processSeconds,
	)
	return c
}

// Settle 記錄一筆訊息的結局，以及從收到到處置完成花了多久。
func (c *ReceiverCounters) Settle(outcome ReceiverOutcome, elapsed time.Duration) {
	switch outcome {
	case ReceiverWritten:
		c.MarkWritten(time.Now())
	case ReceiverDuplicate:
		c.Duplicate.Add(1)
	case ReceiverDLQ:
		c.DLQ.Add(1)
	case ReceiverRequeued:
		c.Requeued.Add(1)
	}
	if c.processSeconds != nil {
		c.processSeconds.WithLabelValues(string(outcome)).Observe(elapsed.Seconds())
	}
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

// Snapshot 回傳目前的數值（JSON 輸出用）。
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

// PrometheusHandler 以 Prometheus 格式輸出指標。
func (c *ReceiverCounters) PrometheusHandler() http.Handler {
	return prometheusHandler(c.registry)
}

// Handler 以 JSON 輸出計數器。
func Handler(snapshot func() map[string]any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot()) // 寫入失敗代表 client 斷線，無從補救
	})
}

// newRegistry 建立 registry，並附上 Go 執行環境與行程的標準指標
// （goroutine 數、GC、記憶體、檔案描述符），用來觀察有沒有洩漏。
func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

func counterFunc(name, help, outcome string, value *atomic.Int64) prometheus.CounterFunc {
	return prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name:        name,
		Help:        help,
		ConstLabels: prometheus.Labels{"outcome": outcome},
	}, func() float64 { return float64(value.Load()) })
}

func prometheusHandler(reg *prometheus.Registry) http.Handler {
	if reg == nil {
		// 零值的計數器沒有 registry（只會出現在測試）
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
