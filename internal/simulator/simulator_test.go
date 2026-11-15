package simulator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tester-receiver/internal/model"
	"tester-receiver/internal/retry"
)

// fakeAPI 記下每個請求的 body，並可設定前幾次要回什麼狀態碼。
type fakeAPI struct {
	mu       sync.Mutex
	requests map[string][]string // device_id → 依序收到的 body
	failures map[string]int      // device_id → 還要回幾次 503
	status   int                 // 非 0 時一律回這個狀態碼
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{requests: map[string][]string{}, failures: map[string]int{}}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	reading, err := model.ParseReading(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests[reading.DeviceID] = append(f.requests[reading.DeviceID], string(body))

	switch {
	case f.status != 0:
		w.WriteHeader(f.status)
	case f.failures[reading.DeviceID] > 0:
		f.failures[reading.DeviceID]--
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

func testConfig(url string) Config {
	return Config{
		URL:            url,
		Devices:        3,
		Duration:       300 * time.Millisecond,
		Interval:       50 * time.Millisecond,
		RequestTimeout: time.Second,
		DrainTimeout:   2 * time.Second,
		Backoff:        retry.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestRun_SequencesAreContiguousAndReported(t *testing.T) {
	api := newFakeAPI()
	srv := httptest.NewServer(api)
	defer srv.Close()

	report := Run(context.Background(), testConfig(srv.URL))

	if len(report.Devices) != 3 {
		t.Fatalf("report has %d devices, want 3", len(report.Devices))
	}
	for id, d := range report.Devices {
		if d.LastSeq < 2 {
			t.Errorf("%s sent only %d readings in 300ms at a 50ms interval", id, d.LastSeq)
		}
		if d.Accepted != d.LastSeq || d.Retries != 0 || d.Rejected != 0 || d.Unconfirmed != 0 {
			t.Errorf("%s: %+v", id, d)
		}

		// 伺服器收到的序號必須是 1, 2, 3, … 不跳號
		bodies := api.requests[id]
		if int64(len(bodies)) != d.LastSeq {
			t.Errorf("%s: server saw %d requests, report says last_seq %d", id, len(bodies), d.LastSeq)
		}
		for i, body := range bodies {
			r, _ := model.ParseReading([]byte(body))
			if r.Seq != int64(i+1) {
				t.Errorf("%s: request %d has seq %d", id, i, r.Seq)
			}
		}
	}
	if report.TotalAccepted == 0 || report.FinishedAt.Before(report.StartedAt) {
		t.Errorf("bad totals: %+v", report)
	}
}

func TestRun_RetriesResendTheIdenticalBody(t *testing.T) {
	api := newFakeAPI()
	api.failures["device-001"] = 3 // 第一筆要送 4 次才成功
	srv := httptest.NewServer(api)
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Devices = 1
	report := Run(context.Background(), cfg)

	d := report.Devices["device-001"]
	if d.Retries != 3 {
		t.Errorf("retries = %d, want 3", d.Retries)
	}

	bodies := api.requests["device-001"]
	if len(bodies) < 4 {
		t.Fatalf("server saw %d requests, want at least 4", len(bodies))
	}
	// 冪等的前提：重送的內容（uni_id、seq、record_time）必須一字不差
	for i := 1; i < 4; i++ {
		if bodies[i] != bodies[0] {
			t.Errorf("retry %d differs from the original:\n  %s\n  %s", i, bodies[0], bodies[i])
		}
	}
	if int64(len(bodies))-3 != d.LastSeq {
		t.Errorf("last_seq = %d, want %d distinct readings", d.LastSeq, len(bodies)-3)
	}
}

func TestRun_ClientErrorsAreNotRetried(t *testing.T) {
	api := newFakeAPI()
	api.status = http.StatusNotFound
	srv := httptest.NewServer(api)
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Devices = 1
	report := Run(context.Background(), cfg)

	d := report.Devices["device-001"]
	if d.Retries != 0 || d.Accepted != 0 || d.LastSeq != 0 {
		t.Errorf("4xx must not be retried or counted as accepted: %+v", d)
	}
	if d.Rejected == 0 || int64(len(api.requests["device-001"])) != d.Rejected {
		t.Errorf("rejected = %d, server saw %d requests", d.Rejected, len(api.requests["device-001"]))
	}
}

func TestRun_GivesUpAfterDrainTimeout(t *testing.T) {
	api := newFakeAPI()
	api.status = http.StatusServiceUnavailable // 伺服器一直不可用
	srv := httptest.NewServer(api)
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Devices = 2
	cfg.Duration = 50 * time.Millisecond
	cfg.DrainTimeout = 100 * time.Millisecond

	start := time.Now()
	report := Run(context.Background(), cfg)

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Run took %v; it should stop after duration + drain timeout", elapsed)
	}
	for id, d := range report.Devices {
		if d.Unconfirmed != 1 || d.LastSeq != 0 || d.Retries == 0 {
			t.Errorf("%s: %+v, want exactly one unconfirmed reading", id, d)
		}
	}
	if report.TotalUnconfirmed != 2 {
		t.Errorf("total_unconfirmed = %d, want 2", report.TotalUnconfirmed)
	}
}

func TestReport_FileRoundTrip(t *testing.T) {
	in := Report{
		StartedAt: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC),
		Devices:   map[string]DeviceReport{"device-001": {LastSeq: 60, Accepted: 60, Retries: 2}},
	}
	path := filepath.Join(t.TempDir(), "report.json")

	if err := in.WriteFile(path); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := ReadReport(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.Devices["device-001"] != in.Devices["device-001"] || !out.StartedAt.Equal(in.StartedAt) {
		t.Errorf("round trip mismatch: %+v", out)
	}
}
