package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tester-receiver/internal/metrics"
	"tester-receiver/internal/model"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/store"
)

// 以下 fake 讓 handler 測試不需要資料庫與 broker。

type fakeStore struct {
	failWith  error
	lastQuery store.ReadingQuery
}

func (f *fakeStore) Ping(context.Context) error { return f.failWith }

func (f *fakeStore) QueryReadings(_ context.Context, q store.ReadingQuery) ([]model.Reading, error) {
	f.lastQuery = q
	return []model.Reading{}, f.failWith
}

type fakeDevices map[string]bool

func (f fakeDevices) Exists(id string) bool { return f[id] }

type fakePublisher struct {
	failWith     error
	disconnected bool
	published    []string // 每筆訊息的 body
	ids          []string
}

func (f *fakePublisher) Publish(_ context.Context, m mq.Message) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.ids = append(f.ids, m.ID)
	f.published = append(f.published, string(m.Body))
	return nil
}

func (f *fakePublisher) Connected() bool { return !f.disconnected }

type fixture struct {
	store   *fakeStore
	pub     *fakePublisher
	metrics *metrics.APICounters
	srv     http.Handler
}

func newFixture() *fixture {
	f := &fixture{store: &fakeStore{}, pub: &fakePublisher{}, metrics: metrics.NewAPICounters()}
	h := &handler{
		store:     f.store,
		devices:   fakeDevices{"device-001": true},
		publisher: f.pub,
		metrics:   f.metrics,
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		timeout:   time.Second,
	}
	f.srv = h.routes()
	return f
}

func (f *fixture) do(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

const (
	validID   = "3f1c2a52-6f0e-4b7e-9d59-0a4a6c1f2b10"
	validBody = `{"uni_id":"` + validID + `","device_id":"device-001","seq":1,"record_time":"2026-11-15T12:00:00Z","freq":1000}`
)

func TestPostReading_PublishesCanonicalMessage(t *testing.T) {
	f := newFixture()

	// 欄位順序與空白都和標準格式不同，另外多帶一個未知欄位
	body := `{ "freq":1000, "extra":"x", "seq":1, "record_time":"2026-11-15T12:00:00+00:00",
	           "device_id":"device-001", "uni_id":"` + strings.ToUpper(validID) + `" }`

	rec := f.do(t, http.MethodPost, "/v1/readings", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", rec.Code, rec.Body)
	}
	if len(f.pub.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(f.pub.published))
	}
	if f.pub.published[0] != validBody {
		t.Errorf("message = %s\nwant      %s", f.pub.published[0], validBody)
	}
	if f.pub.ids[0] != validID {
		t.Errorf("message id = %s, want %s", f.pub.ids[0], validID)
	}

	// receiver 會用同一個解析函式讀訊息，必須解析得回來
	if _, err := model.ParseReading([]byte(f.pub.published[0])); err != nil {
		t.Errorf("published message does not parse: %v", err)
	}
}

func TestPostReading_DuplicateIsPublishedAgain(t *testing.T) {
	// API 層不去重：重送照常發佈，由資料庫的 unique constraint 負責
	f := newFixture()
	for range 2 {
		if rec := f.do(t, http.MethodPost, "/v1/readings", validBody); rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", rec.Code)
		}
	}
	if len(f.pub.published) != 2 {
		t.Errorf("published %d messages, want 2", len(f.pub.published))
	}
}

func TestPostReading_Rejections(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"malformed json", `{`, http.StatusBadRequest},
		{"missing field", `{"uni_id":"` + validID + `"}`, http.StatusBadRequest},
		{"non-UTC record_time", strings.Replace(validBody, "12:00:00Z", "12:00:00+08:00", 1), http.StatusBadRequest},
		{"unknown device", strings.Replace(validBody, "device-001", "device-999", 1), http.StatusNotFound},
		{"body too large", `{"pad":"` + strings.Repeat("x", maxBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			rec := f.do(t, http.MethodPost, "/v1/readings", tc.body)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body = %s", rec.Code, tc.want, rec.Body)
			}
			if len(f.pub.published) != 0 {
				t.Errorf("rejected request was published")
			}
		})
	}
}

func TestPostReading_UnconfirmedPublishReturns503(t *testing.T) {
	f := newFixture()
	f.pub.failWith = errors.New("broker unavailable: waiting for confirm: context deadline exceeded")

	rec := f.do(t, http.MethodPost, "/v1/readings", validBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "confirm") {
		t.Errorf("internal error leaked to client: %s", rec.Body)
	}
}

func TestPostReading_DoesNotNeedDatabase(t *testing.T) {
	// 資料庫故障時 api 仍要能收資料
	f := newFixture()
	f.store.failWith = errors.New("db down")

	if rec := f.do(t, http.MethodPost, "/v1/readings", validBody); rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202 even when the database is down", rec.Code)
	}
}

func TestGetReadings_QueryParsing(t *testing.T) {
	cases := []struct {
		name      string
		target    string
		wantCode  int
		wantLimit int
	}{
		{"defaults", "/v1/readings", http.StatusOK, 100},
		{"explicit limit", "/v1/readings?device_id=device-001&limit=5", http.StatusOK, 5},
		{"limit capped", "/v1/readings?limit=99999", http.StatusOK, 1000},
		{"limit zero", "/v1/readings?limit=0", http.StatusBadRequest, 0},
		{"limit not a number", "/v1/readings?limit=abc", http.StatusBadRequest, 0},
		{"bad from", "/v1/readings?from=yesterday", http.StatusBadRequest, 0},
		{"valid range", "/v1/readings?from=2026-11-15T00:00:00Z&to=2026-11-16T00:00:00Z", http.StatusOK, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			rec := f.do(t, http.MethodGet, tc.target, "")
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tc.wantCode, rec.Body)
			}
			if tc.wantCode == http.StatusOK && f.store.lastQuery.Limit != tc.wantLimit {
				t.Errorf("limit = %d, want %d", f.store.lastQuery.Limit, tc.wantLimit)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	f := newFixture()
	if rec := f.do(t, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("healthy: status = %d, want 200", rec.Code)
	}

	// 資料庫掛掉：仍然健康，但回報 database down
	f.store.failWith = errors.New("db down")
	rec := f.do(t, http.MethodGet, "/healthz", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"database":"down"`) {
		t.Errorf("db down: status = %d body = %s, want 200 with database down", rec.Code, rec.Body)
	}

	// broker 斷線：不健康
	f.pub.disconnected = true
	if rec := f.do(t, http.MethodGet, "/healthz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("broker down: status = %d, want 503", rec.Code)
	}
}

func TestMetrics_CountEachOutcome(t *testing.T) {
	f := newFixture()

	f.do(t, http.MethodPost, "/v1/readings", validBody)                                               // accepted
	f.do(t, http.MethodPost, "/v1/readings", validBody)                                               // accepted（重送也算）
	f.do(t, http.MethodPost, "/v1/readings", `{`)                                                     // 400
	f.do(t, http.MethodPost, "/v1/readings", strings.Replace(validBody, "device-001", "device-999", 1)) // 404
	f.pub.failWith = errors.New("no confirm")
	f.do(t, http.MethodPost, "/v1/readings", validBody) // 503

	if a, r, p := f.metrics.Accepted.Load(), f.metrics.Rejected4xx.Load(), f.metrics.PublishFailed.Load(); a != 2 || r != 2 || p != 1 {
		t.Errorf("accepted=%d rejected_4xx=%d publish_failed=%d, want 2 / 2 / 1", a, r, p)
	}

	rec := f.do(t, http.MethodGet, "/metrics.json", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"accepted":2`) {
		t.Errorf("GET /metrics.json: status = %d body = %s", rec.Code, rec.Body)
	}

	// Prometheus 格式讀的是同一組數值，另外多了延遲分布
	prom := f.do(t, http.MethodGet, "/metrics", "").Body.String()
	for _, want := range []string{
		`tester_api_readings_total{outcome="accepted"} 2`,
		`tester_api_readings_total{outcome="rejected"} 2`,
		`tester_api_readings_total{outcome="publish_failed"} 1`,
		`tester_api_request_duration_seconds_count{outcome="accepted"} 2`,
		`go_goroutines`,
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("GET /metrics lacks %q", want)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := newFixture().do(t, http.MethodDelete, "/v1/readings", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}
