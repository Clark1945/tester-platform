// Package simulator 模擬一群 device：每台各自一個 goroutine，依固定間隔送資料，
// 失敗時以相同的 uni_id / seq / record_time 重試。結束時回報每台送到第幾號，
// 供 verify 對帳。
package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"tester-receiver/internal/model"
	"tester-receiver/internal/retry"
)

// Config 是模擬的參數。
type Config struct {
	URL            string        // API 的 base URL，例如 http://localhost:8090
	Devices        int           // device 數量；device-001 … device-N
	Duration       time.Duration // 產生新資料的時間長度
	Interval       time.Duration // 每台 device 兩筆資料的間隔
	RequestTimeout time.Duration // 單次 HTTP 請求的上限
	DrainTimeout   time.Duration // Duration 結束後，最多再花多久把「送到一半」的那一筆重試到成功
	Backoff        retry.Backoff
	Log            *slog.Logger
}

// DeviceReport 是單台 device 的結果。
type DeviceReport struct {
	// LastSeq 是最後一個「已被 API 接受（202）」的序號。
	// device 依序送出、前一筆確認後才送下一筆，所以 1..LastSeq 全部都已被接受。
	LastSeq  int64 `json:"last_seq"`
	Accepted int64 `json:"accepted"`
	Retries  int64 `json:"retries"`  // 因 5xx / 逾時 / 連線失敗而重送的次數
	Rejected int64 `json:"rejected"` // 收到 4xx、不重試的筆數（正常情況應為 0）
	// Unconfirmed：模擬結束時仍未獲確認就放棄的筆數（0 或 1）。
	// 這一筆可能其實已經進了系統（回應遺失），所以 verify 對這台 device 會容許多出一筆。
	Unconfirmed int64 `json:"unconfirmed"`
}

// Report 是整次模擬的結果。
type Report struct {
	StartedAt  time.Time               `json:"started_at"`
	FinishedAt time.Time               `json:"finished_at"`
	Devices    map[string]DeviceReport `json:"devices"`

	TotalAccepted    int64 `json:"total_accepted"`
	TotalRetries     int64 `json:"total_retries"`
	TotalRejected    int64 `json:"total_rejected"`
	TotalUnconfirmed int64 `json:"total_unconfirmed"`
}

// DeviceID 回傳第 n 台（從 1 起算）device 的 ID。
func DeviceID(n int) string {
	return fmt.Sprintf("device-%03d", n)
}

// Run 執行模擬直到 Duration 結束或 ctx 取消（例如 Ctrl+C），然後回傳報告。
// 不論哪種方式結束都會回傳完整的報告。
func Run(ctx context.Context, cfg Config) Report {
	client := &http.Client{Timeout: cfg.RequestTimeout}

	// genCtx：控制「還要不要產生新資料」
	genCtx, stopGenerating := context.WithTimeout(ctx, cfg.Duration)
	defer stopGenerating()

	// sendCtx：控制「還要不要繼續重試手上這一筆」。比 genCtx 晚 DrainTimeout 才取消，
	// 讓停止當下送到一半的那筆有機會重試到成功，報告的 LastSeq 才準確。
	sendCtx, stopSending := context.WithCancel(context.Background())
	defer stopSending()
	go func() {
		<-genCtx.Done()
		if retry.Sleep(sendCtx, cfg.DrainTimeout) == nil {
			stopSending()
		}
	}()

	report := Report{StartedAt: time.Now().UTC(), Devices: make(map[string]DeviceReport, cfg.Devices)}

	var (
		wg sync.WaitGroup
		mu sync.Mutex // 保護 report.Devices；每台 device 結束時才寫一次
	)
	for n := 1; n <= cfg.Devices; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := &device{id: DeviceID(n), cfg: cfg, client: client}
			d.run(genCtx, sendCtx)

			mu.Lock()
			report.Devices[d.id] = d.report
			mu.Unlock()
		}()
	}
	wg.Wait()

	report.FinishedAt = time.Now().UTC()
	for _, d := range report.Devices {
		report.TotalAccepted += d.Accepted
		report.TotalRetries += d.Retries
		report.TotalRejected += d.Rejected
		report.TotalUnconfirmed += d.Unconfirmed
	}
	return report
}

type device struct {
	id     string
	cfg    Config
	client *http.Client
	report DeviceReport
}

type sendResult int

const (
	accepted sendResult = iota
	rejected
	unconfirmed
)

func (d *device) run(genCtx, sendCtx context.Context) {
	// 各台 device 錯開起跑時間，避免 100 台在同一毫秒一起送
	if retry.Sleep(genCtx, rand.N(d.cfg.Interval)) != nil {
		return
	}

	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()

	var seq int64
	for {
		seq++
		// record_time 取到微秒：PostgreSQL 的 timestamptz 精度是微秒
		reading := model.Reading{
			UniID:      uuid.New(),
			DeviceID:   d.id,
			Seq:        seq,
			RecordTime: time.Now().UTC().Truncate(time.Microsecond),
			Freq:       1000 + rand.Int32N(100),
		}

		switch d.send(sendCtx, reading) {
		case accepted:
			d.report.Accepted++
			d.report.LastSeq = seq
		case rejected:
			d.report.Rejected++
		case unconfirmed:
			d.report.Unconfirmed++
			return
		}

		select {
		case <-genCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

// send 送出一筆資料，重試到有明確結果為止。
// 重試時 body 完全相同（相同的 uni_id / seq / record_time）——這是冪等的前提。
func (d *device) send(ctx context.Context, reading model.Reading) sendResult {
	body, err := json.Marshal(reading)
	if err != nil {
		d.cfg.Log.Error("encode reading failed", "device_id", d.id, "seq", reading.Seq, "error", err)
		return rejected
	}

	for attempt := 1; ; attempt++ {
		status, err := d.post(ctx, body)
		switch {
		case err == nil && status == http.StatusAccepted:
			return accepted
		case err == nil && status >= 400 && status < 500:
			// 4xx：資料本身有問題，重送也不會成功
			d.cfg.Log.Error("reading rejected", "device_id", d.id, "seq", reading.Seq, "uni_id", reading.UniID, "status", status)
			return rejected
		}

		// 5xx、逾時、連線失敗：無法分辨伺服器有沒有收到，只能重送
		d.report.Retries++
		delay := d.cfg.Backoff.Delay(attempt)
		d.cfg.Log.Debug("send failed; retrying", "device_id", d.id, "seq", reading.Seq,
			"attempt", attempt, "status", status, "error", err, "delay", delay.String())

		if retry.Sleep(ctx, delay) != nil {
			d.cfg.Log.Warn("gave up on unconfirmed reading", "device_id", d.id, "seq", reading.Seq, "uni_id", reading.UniID)
			return unconfirmed
		}
	}
}

func (d *device) post(ctx context.Context, body []byte) (status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.URL+"/v1/readings", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post reading: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10)) // 讀完 body 連線才能重複使用
	return resp.StatusCode, nil
}

// WriteFile 把報告寫成 JSON 檔。
func (r Report) WriteFile(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write report %s: %w", path, err)
	}
	return nil
}

// ReadReport 讀回 WriteFile 寫出的報告。
func ReadReport(path string) (Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, fmt.Errorf("read report %s: %w", path, err)
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return Report{}, fmt.Errorf("decode report %s: %w", path, err)
	}
	return r, nil
}
