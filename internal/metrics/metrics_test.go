package metrics

import (
	"encoding/json"
	"net/http/httptest"
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
