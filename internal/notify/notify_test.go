package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// sink 是假的 webhook 接收端，記下收到的每則告警。
type sink struct {
	mu       sync.Mutex
	received []payload
	status   int
}

func (s *sink) handler(w http.ResponseWriter, r *http.Request) {
	var p payload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.received = append(s.received, p)
	status := s.status
	s.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)
	}
}

func (s *sink) all() []payload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]payload(nil), s.received...)
}

// fakeClock 讓測試自己控制時間
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func setup(t *testing.T) (*Notifier, *sink, *fakeClock) {
	t.Helper()
	s := &sink{}
	srv := httptest.NewServer(http.HandlerFunc(s.handler))
	t.Cleanup(srv.Close)

	clock := &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	n := New(srv.URL, "receiver", time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n.now = clock.now
	return n, s, clock
}

func TestNotify_SendsPayload(t *testing.T) {
	n, s, _ := setup(t)

	n.Notify(context.Background(), Alert{
		Kind:    "dlq",
		Message: "message dead-lettered",
		Fields:  map[string]any{"reason": "invalid_payload"},
	})

	got := s.all()
	if len(got) != 1 {
		t.Fatalf("received %d alerts, want 1", len(got))
	}
	if got[0].Kind != "dlq" || got[0].Service != "receiver" || got[0].Fields["reason"] != "invalid_payload" {
		t.Errorf("unexpected payload: %+v", got[0])
	}
}

func TestNotify_ThrottlesPerKind(t *testing.T) {
	n, s, clock := setup(t)
	ctx := context.Background()

	// 同一分鐘內 50 則同類告警：只送第一則
	for range 50 {
		n.Notify(ctx, Alert{Kind: "dlq", Message: "m"})
		clock.advance(time.Second)
	}
	if got := len(s.all()); got != 1 {
		t.Fatalf("received %d alerts within the interval, want 1", got)
	}

	// 不同種類的告警不受影響
	n.Notify(ctx, Alert{Kind: "consumer_stuck", Message: "m"})
	if got := len(s.all()); got != 2 {
		t.Fatalf("a different kind was throttled: received %d, want 2", got)
	}

	// 過了間隔：再送一則，並回報中間被節流掉的數量
	clock.advance(time.Minute)
	n.Notify(ctx, Alert{Kind: "dlq", Message: "m"})

	got := s.all()
	if len(got) != 3 {
		t.Fatalf("received %d alerts after the interval, want 3", len(got))
	}
	if last := got[2]; last.Kind != "dlq" || last.Suppressed != 49 {
		t.Errorf("kind = %s suppressed = %d, want dlq with 49 suppressed", last.Kind, last.Suppressed)
	}
}

func TestNotify_ConcurrentCallsSendOnce(t *testing.T) {
	n, s, _ := setup(t)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Notify(context.Background(), Alert{Kind: "dlq", Message: "m"})
		}()
	}
	wg.Wait()

	if got := len(s.all()); got != 1 {
		t.Errorf("received %d alerts from concurrent callers, want 1", got)
	}
}

func TestNotify_WebhookFailureDoesNotPanicOrBlock(t *testing.T) {
	n, s, _ := setup(t)
	s.status = http.StatusInternalServerError

	n.Notify(context.Background(), Alert{Kind: "dlq", Message: "m"}) // 只會記錄錯誤

	if got := len(s.all()); got != 1 {
		t.Errorf("received %d requests, want 1", got)
	}
}

func TestNotify_EmptyURLOnlyLogs(t *testing.T) {
	n := New("", "receiver", time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n.Notify(context.Background(), Alert{Kind: "dlq", Message: "m"}) // 不應該嘗試連線或 panic
}

func TestNotify_SendsEvenWhenContextCancelled(t *testing.T) {
	// 服務關閉途中（ctx 已取消）產生的告警仍要送出
	n, s, _ := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n.Notify(ctx, Alert{Kind: "dlq", Message: "m"})

	if got := len(s.all()); got != 1 {
		t.Errorf("received %d alerts, want 1", got)
	}
}
