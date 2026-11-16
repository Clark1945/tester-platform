package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReceiverCounters_ConcurrentUpdates(t *testing.T) {
	var c ReceiverCounters

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				c.Received.Add(1)
				c.MarkWritten(time.Now())
			}
		}()
	}
	wg.Wait()

	if got := c.Written.Load(); got != 5000 {
		t.Errorf("written = %d, want 5000 (lost updates)", got)
	}
	if c.Settled() != 5000 {
		t.Errorf("settled = %d, want 5000", c.Settled())
	}
}

func TestReceiverCounters_LastWrittenAt(t *testing.T) {
	var c ReceiverCounters
	if !c.LastWrittenAt().IsZero() {
		t.Error("LastWrittenAt should be zero before any write")
	}
	if c.Snapshot()["last_written_at"] != nil {
		t.Error("last_written_at should be null before any write")
	}

	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	c.MarkWritten(at)
	if !c.LastWrittenAt().Equal(at) {
		t.Errorf("LastWrittenAt = %v, want %v", c.LastWrittenAt(), at)
	}
}

func TestReceiverCounters_PrometheusOutputMatchesCounters(t *testing.T) {
	c := NewReceiverCounters()
	c.Received.Add(5)
	c.Retried.Add(3)
	c.Settle(ReceiverWritten, 20*time.Millisecond)
	c.Settle(ReceiverWritten, 30*time.Millisecond)
	c.Settle(ReceiverDuplicate, time.Millisecond)
	c.Settle(ReceiverDLQ, 2*time.Second)
	c.Settle(ReceiverRequeued, time.Second)

	// Settle 同時更新了 atomic 計數器（watchdog 與 /metrics.json 讀的就是它們）
	if c.Written.Load() != 2 || c.Duplicate.Load() != 1 || c.DLQ.Load() != 1 || c.Requeued.Load() != 1 {
		t.Fatalf("counters = %v", c.Snapshot())
	}

	rec := httptest.NewRecorder()
	c.PrometheusHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		`tester_receiver_messages_total{outcome="written"} 2`,
		`tester_receiver_messages_total{outcome="duplicate"} 1`,
		`tester_receiver_messages_total{outcome="dlq"} 1`,
		`tester_receiver_messages_total{outcome="requeued"} 1`,
		`tester_receiver_messages_received_total 5`,
		`tester_receiver_retries_total 3`,
		`tester_receiver_process_duration_seconds_count{outcome="written"} 2`,
		// 20ms 與 30ms 都落在 ≤ 50ms 的 bucket
		`tester_receiver_process_duration_seconds_bucket{outcome="written",le="0.05"} 2`,
		`tester_receiver_last_written_timestamp_seconds`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("prometheus output lacks %q", want)
		}
	}
}

func TestZeroValueCountersStillWork(t *testing.T) {
	// 零值沒有 histogram 與 registry，不能 panic
	var c ReceiverCounters
	c.Settle(ReceiverWritten, time.Millisecond)
	if c.Written.Load() != 1 {
		t.Errorf("written = %d, want 1", c.Written.Load())
	}

	rec := httptest.NewRecorder()
	c.PrometheusHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 404 {
		t.Errorf("status = %d, want 404 for counters without a registry", rec.Code)
	}
}

func TestHandler_OutputsJSON(t *testing.T) {
	var c APICounters
	c.Accepted.Add(3)
	c.PublishFailed.Add(1)

	rec := httptest.NewRecorder()
	Handler(c.Snapshot).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	var got map[string]int64
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid json: %v; body = %s", err, rec.Body)
	}
	if got["accepted"] != 3 || got["publish_failed"] != 1 || got["rejected_4xx"] != 0 {
		t.Errorf("unexpected metrics: %v", got)
	}
}
