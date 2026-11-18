package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"tester-receiver/internal/model"
)

func loadRequest(c *loadController, method, path, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	c.register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func awaitLoad(t *testing.T, c *loadController) loadResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r := c.snapshot()
		if r.State == "completed" || r.State == "stopped" {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("load test did not finish")
	return loadResult{}
}

func TestLoadLifecycleAndPayload(t *testing.T) {
	var received atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/readings" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var reading model.Reading
		if err := json.NewDecoder(r.Body).Decode(&reading); err != nil {
			t.Error(err)
		}
		if reading.DeviceID != "device-001" && reading.DeviceID != "device-002" {
			t.Errorf("invalid device: %q", reading.DeviceID)
		}
		if reading.Seq < 1 || reading.UniID.String() == "00000000-0000-0000-0000-000000000000" {
			t.Error("missing identity")
		}
		switch received.Add(1) % 3 {
		case 0:
			w.WriteHeader(503)
		case 1:
			w.WriteHeader(202)
		case 2:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newLoadController(api.URL)
	defer c.stop()
	if w := loadRequest(c, "POST", "/api/loadtest", `{"rps":12,"devices":2,"duration_seconds":1}`); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := loadRequest(c, "POST", "/api/loadtest", `{"rps":1,"devices":1,"duration_seconds":1}`); w.Code != 409 {
		t.Fatalf("overlap: %d", w.Code)
	}
	r := awaitLoad(t, c)
	if r.Sent+r.Skipped != 12 || r.Completed != r.Sent || received.Load() != r.Sent {
		t.Fatalf("counts: %+v, received=%d", r, received.Load())
	}
	if r.Accepted == 0 || r.ClientErrors == 0 || r.ServerErrors == 0 || r.Accepted+r.ClientErrors+r.ServerErrors != r.Completed {
		t.Fatalf("outcomes: %+v", r)
	}
	if r.Elapsed < 1 || r.AverageMS <= 0 || r.MaxMS < r.AverageMS {
		t.Fatalf("timing: %+v", r)
	}
	// A new run replaces counters and receives a distinct ID.
	if w := loadRequest(c, "POST", "/api/loadtest", `{"rps":1,"devices":1,"duration_seconds":1}`); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if c.snapshot().ID == r.ID {
		t.Fatal("run ID reused")
	}
	c.stop()
	awaitLoad(t, c)
}

func TestLoadStopSlowRequestsAndSkippedSlots(t *testing.T) {
	entered := make(chan struct{}, 100)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
		w.WriteHeader(202)
	}))
	defer api.Close()
	c := newLoadController(api.URL)
	defer c.stop()
	loadRequest(c, "POST", "/api/loadtest", `{"rps":1000,"devices":1,"duration_seconds":10}`)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no request")
	}
	time.Sleep(50 * time.Millisecond)
	if w := loadRequest(c, "POST", "/api/loadtest/stop", `{}`); w.Code != 200 {
		t.Fatal(w.Code)
	}
	r := awaitLoad(t, c)
	if r.State != "stopped" || r.Skipped == 0 || r.Completed != r.Sent || r.Canceled == 0 {
		t.Fatalf("stop/backpressure: %+v", r)
	}
}

func TestLoadValidationAndOrigin(t *testing.T) {
	c := newLoadController("http://127.0.0.1:1")
	for _, body := range []string{`{}`, `{"rps":0,"devices":1,"duration_seconds":1}`, `{"rps":10001,"devices":1,"duration_seconds":1}`, `{"rps":1,"devices":101,"duration_seconds":1}`, `{"rps":1,"devices":1,"duration_seconds":3601}`, `{"rps":1.5}`, `{} {}`, `{"url":"http://external"}`} {
		if w := loadRequest(c, "POST", "/api/loadtest", body); w.Code != 400 {
			t.Errorf("%s: %d", body, w.Code)
		}
	}
	mux := http.NewServeMux()
	c.register(mux)
	for _, path := range []string{"/api/loadtest", "/api/loadtest/stop"} {
		r := httptest.NewRequest("POST", path, strings.NewReader(`{"rps":1,"devices":1,"duration_seconds":1}`))
		r.Header.Set("Origin", "https://evil.example")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Errorf("cross-origin %s: %d", path, w.Code)
		}
	}
}

func TestLoadNetworkFailure(t *testing.T) {
	c := newLoadController("http://127.0.0.1:1")
	defer c.stop()
	loadRequest(c, "POST", "/api/loadtest", `{"rps":3,"devices":2,"duration_seconds":1}`)
	r := awaitLoad(t, c)
	if r.NetworkErrors == 0 || r.NetworkErrors != r.Sent || r.Completed != r.Sent {
		t.Fatalf("network outcomes: %+v", r)
	}
}
