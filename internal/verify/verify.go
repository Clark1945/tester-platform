// Package verify 對帳：比對 simulator 的報告、資料庫、queue 與計數器，
// 回答「這次執行有沒有掉資料、有沒有重複」。
package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"tester-receiver/internal/mq"
	"tester-receiver/internal/retry"
	"tester-receiver/internal/simulator"
	"tester-receiver/internal/store"
)

// DB 是對帳需要的查詢（由 *store.Store 滿足）。
type DB interface {
	MissingSeqs(ctx context.Context, limit int) (int64, []store.MissingSeq, error)
	MaxSeqByDevice(ctx context.Context) (map[string]int64, error)
	Stats(ctx context.Context) (store.ReadingStats, error)
}

// Queues 讀取 queue 深度（由 *mq.Management 滿足）。
type Queues interface {
	QueueDepth(ctx context.Context, queue string) (mq.QueueDepth, error)
}

// Options 是對帳的輸入。
type Options struct {
	Report simulator.Report
	DB     DB
	Queues Queues
	Queue  string
	DLQ    string

	// 計數器恆等式。兩個 URL 都給了才檢查。
	APIMetricsURL      string
	ReceiverMetricsURL string
	// StrictCounters：恆等式不成立就算失敗。只適用於「api 與 receiver 啟動後沒有重啟、
	// 也沒有重送」的穩態執行。chaos test 會重啟服務（計數器歸零）並造成重送，
	// 此時恆等式本來就不會成立，只列出數字供參考。
	StrictCounters bool

	DrainTimeout time.Duration // 等 queue 排空的上限
	PollInterval time.Duration // 多久看一次 queue
	StablePolls  int           // 連續幾次為 0 才算排空（管理 API 的數字有延遲）

	Out io.Writer
}

const maxSamples = 10

type runner struct {
	opts   Options
	failed bool
}

func (r *runner) pass(name, detail string) { fmt.Fprintf(r.opts.Out, "[PASS] %-22s %s\n", name, detail) }
func (r *runner) info(name, detail string) { fmt.Fprintf(r.opts.Out, "[INFO] %-22s %s\n", name, detail) }
func (r *runner) fail(name, detail string) {
	r.failed = true
	fmt.Fprintf(r.opts.Out, "[FAIL] %-22s %s\n", name, detail)
}

// Run 執行所有檢查並把結果印到 opts.Out。全部通過才回傳 true。
// 一項失敗不會中斷後面的檢查：一次看到全部的差異比較好除錯。
func Run(ctx context.Context, opts Options) bool {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.StablePolls <= 0 {
		opts.StablePolls = 4
	}
	r := &runner{opts: opts}

	r.checkDrained(ctx)
	r.checkGaps(ctx)
	r.checkLastSeqs(ctx)
	r.checkCountAndDuplicates(ctx)
	r.checkCounters(ctx)

	if r.failed {
		fmt.Fprintln(opts.Out, "\nRESULT: FAILED")
	} else {
		fmt.Fprintln(opts.Out, "\nRESULT: PASSED")
	}
	return !r.failed
}

// checkDrained 等主 queue 排空，並確認 DLQ 是空的。
// 還有訊息在路上時就去數資料庫，一定會得到「少資料」的假警報。
func (r *runner) checkDrained(ctx context.Context) {
	const name = "queue drained"
	deadline := time.Now().Add(r.opts.DrainTimeout)

	var (
		depth  mq.QueueDepth
		err    error
		stable int
	)
	for {
		depth, err = r.opts.Queues.QueueDepth(ctx, r.opts.Queue)
		if err == nil && depth.Total() == 0 {
			stable++
		} else {
			stable = 0
		}
		if stable >= r.opts.StablePolls {
			break
		}
		if time.Now().After(deadline) || retry.Sleep(ctx, r.opts.PollInterval) != nil {
			if err != nil {
				r.fail(name, fmt.Sprintf("cannot read queue %s: %v", r.opts.Queue, err))
			} else {
				r.fail(name, fmt.Sprintf("%s still has ready=%d unacked=%d after %s",
					r.opts.Queue, depth.Ready, depth.Unacked, r.opts.DrainTimeout))
			}
			return
		}
	}

	dead, err := r.opts.Queues.QueueDepth(ctx, r.opts.DLQ)
	switch {
	case err != nil:
		r.fail(name, fmt.Sprintf("cannot read queue %s: %v", r.opts.DLQ, err))
	case dead.Total() > 0:
		r.fail(name, fmt.Sprintf("%s holds %d messages (data that never reached the database)", r.opts.DLQ, dead.Total()))
	default:
		r.pass(name, fmt.Sprintf("%s ready=0 unacked=0, %s=0", r.opts.Queue, r.opts.DLQ))
	}
}

// checkGaps：每台 device 的序號 1..max(seq) 不可有缺號。
func (r *runner) checkGaps(ctx context.Context) {
	const name = "sequence gaps"
	total, sample, err := r.opts.DB.MissingSeqs(ctx, maxSamples)
	if err != nil {
		r.fail(name, err.Error())
		return
	}
	if total == 0 {
		r.pass(name, "0 missing")
		return
	}

	parts := make([]string, 0, len(sample))
	for _, m := range sample {
		parts = append(parts, fmt.Sprintf("%s#%d", m.DeviceID, m.Seq))
	}
	r.fail(name, fmt.Sprintf("%d missing, e.g. %s", total, strings.Join(parts, ", ")))
}

// checkLastSeqs：資料庫的 max(seq) 必須等於 simulator 回報的最後序號。
// 這是缺口查詢的補強——尾端遺失不會產生缺口，只有這項抓得到。
func (r *runner) checkLastSeqs(ctx context.Context) {
	const name = "last seq per device"
	got, err := r.opts.DB.MaxSeqByDevice(ctx)
	if err != nil {
		r.fail(name, err.Error())
		return
	}

	var diffs []string
	for id, d := range r.opts.Report.Devices {
		// 放棄未確認的那一筆可能其實已經寫入，所以容許多出 Unconfirmed 筆
		if have := got[id]; have < d.LastSeq || have > d.LastSeq+d.Unconfirmed {
			diffs = append(diffs, fmt.Sprintf("%s: db max=%d, simulator last=%d (lost %d)", id, have, d.LastSeq, d.LastSeq-have))
		}
	}
	for id, have := range got {
		if _, known := r.opts.Report.Devices[id]; !known {
			diffs = append(diffs, fmt.Sprintf("%s: db max=%d but the device is not in the simulator report", id, have))
		}
	}

	if len(diffs) == 0 {
		r.pass(name, fmt.Sprintf("%d devices match", len(r.opts.Report.Devices)))
		return
	}
	sort.Strings(diffs)
	shown := diffs[:min(len(diffs), maxSamples)]
	r.fail(name, fmt.Sprintf("%d devices differ:\n         %s", len(diffs), strings.Join(shown, "\n         ")))
}

// checkCountAndDuplicates：總筆數等於 simulator 被接受的筆數，且沒有重複。
func (r *runner) checkCountAndDuplicates(ctx context.Context) {
	stats, err := r.opts.DB.Stats(ctx)
	if err != nil {
		r.fail("row count", err.Error())
		return
	}

	expected, slack := r.opts.Report.TotalAccepted, r.opts.Report.TotalUnconfirmed
	switch {
	case stats.Count >= expected && stats.Count <= expected+slack:
		r.pass("row count", fmt.Sprintf("%d rows = %d accepted by the API", stats.Count, expected))
	default:
		r.fail("row count", fmt.Sprintf("%d rows, expected %d (difference %+d)", stats.Count, expected, stats.Count-expected))
	}

	if stats.DuplicateKeys == 0 && stats.DuplicateDeviceSeqs == 0 {
		r.pass("duplicates", "0 by (uni_id, record_time), 0 by (device_id, seq)")
	} else {
		r.fail("duplicates", fmt.Sprintf("%d by (uni_id, record_time), %d by (device_id, seq)",
			stats.DuplicateKeys, stats.DuplicateDeviceSeqs))
	}
}

// checkCounters 檢查恆等式：
//
//	published(confirmed) = written + duplicate + dlq + queue(ready + unacked)
func (r *runner) checkCounters(ctx context.Context) {
	const name = "counter identity"
	if r.opts.APIMetricsURL == "" || r.opts.ReceiverMetricsURL == "" {
		r.info(name, "skipped (metrics URLs not provided)")
		return
	}

	report := func(detail string) {
		if r.opts.StrictCounters {
			r.fail(name, detail)
		} else {
			r.info(name, detail)
		}
	}

	api, err := fetchMetrics(ctx, r.opts.APIMetricsURL)
	if err != nil {
		report(err.Error())
		return
	}
	recv, err := fetchMetrics(ctx, r.opts.ReceiverMetricsURL)
	if err != nil {
		report(err.Error())
		return
	}
	depth, err := r.opts.Queues.QueueDepth(ctx, r.opts.Queue)
	if err != nil {
		report(err.Error())
		return
	}

	left := api["accepted"]
	right := recv["written"] + recv["duplicate"] + recv["dlq"] + depth.Total()
	detail := fmt.Sprintf("accepted=%d ; written=%d + duplicate=%d + dlq=%d + queue=%d = %d  (retried=%d requeued=%d)",
		left, recv["written"], recv["duplicate"], recv["dlq"], depth.Total(), right, recv["retried"], recv["requeued"])

	switch {
	case left == right:
		r.pass(name, detail)
	case r.opts.StrictCounters:
		r.fail(name, detail+fmt.Sprintf("  → off by %+d", right-left))
	default:
		r.info(name, detail+"  → not expected to balance: counters are in-memory and reset when a service restarts")
	}
}

// fetchMetrics 讀取 /metrics.json，只保留數值欄位。
func fetchMetrics(ctx context.Context, url string) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: %s", url, resp.Status)
	}

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode %s: %w", url, err)
	}
	out := make(map[string]int64, len(raw))
	for k, v := range raw {
		if n, ok := v.(float64); ok { // encoding/json 把所有數字解成 float64
			out[k] = int64(n)
		}
	}
	return out, nil
}
