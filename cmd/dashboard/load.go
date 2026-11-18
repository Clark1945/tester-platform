package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"tester-receiver/internal/model"
	"tester-receiver/internal/simulator"
)

// Load testing intentionally does not retry: RPS counts HTTP attempts, not accepted readings.
// The target is configured by the operator, never supplied by a browser request.
type loadConfig struct {
	RPS      int `json:"rps"`
	Devices  int `json:"devices"`
	Duration int `json:"duration_seconds"`
}

type loadResult struct {
	ID            string     `json:"id"`
	Config        loadConfig `json:"config"`
	State         string     `json:"state"`
	Started       time.Time  `json:"started_at"`
	Finished      *time.Time `json:"finished_at,omitempty"`
	Elapsed       float64    `json:"elapsed_seconds"`
	Sent          int64      `json:"sent"`
	Completed     int64      `json:"completed"`
	Accepted      int64      `json:"accepted"`
	ClientErrors  int64      `json:"client_errors"`
	ServerErrors  int64      `json:"server_errors"`
	Other         int64      `json:"other_responses"`
	NetworkErrors int64      `json:"network_errors"`
	Canceled      int64      `json:"canceled"`
	Skipped       int64      `json:"skipped"`
	ActualRPS     float64    `json:"actual_rps"`
	AverageMS     float64    `json:"average_latency_ms"`
	MaxMS         float64    `json:"max_latency_ms"`
}

type loadController struct {
	mu      sync.Mutex
	result  loadResult
	latency time.Duration
	cancel  context.CancelFunc
	target  string
}

func newLoadController(target string) *loadController {
	return &loadController{target: target, result: loadResult{State: "idle"}}
}

func (c *loadController) snapshot() loadResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.result
	if !r.Started.IsZero() {
		end := time.Now()
		if r.Finished != nil {
			end = *r.Finished
		}
		r.Elapsed = end.Sub(r.Started).Seconds()
		if r.Elapsed > 0 {
			r.ActualRPS = float64(r.Sent) / r.Elapsed
		}
		if r.Completed > 0 {
			r.AverageMS = float64(c.latency) / float64(time.Millisecond) / float64(r.Completed)
		}
	}
	return r
}

func (c *loadController) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.result.State = "stopping"
		c.cancel()
	}
}

func loadJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func loadError(w http.ResponseWriter, status int, message string) {
	loadJSON(w, status, map[string]string{"error": message})
}

func (c *loadController) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/loadtest", func(w http.ResponseWriter, r *http.Request) {
		loadJSON(w, http.StatusOK, c.snapshot())
	})
	mux.HandleFunc("POST /api/loadtest", c.start)
	mux.HandleFunc("POST /api/loadtest/stop", func(w http.ResponseWriter, r *http.Request) {
		if !loadSameOrigin(w, r) {
			return
		}
		c.stop()
		loadJSON(w, http.StatusOK, c.snapshot())
	})
}

// Reject browser requests originating from another site, including form submissions.
func loadSameOrigin(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		loadError(w, http.StatusForbidden, "cross-site request rejected")
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if err != nil || u.Host != r.Host || u.Scheme != scheme {
			loadError(w, http.StatusForbidden, "origin mismatch")
			return false
		}
	}
	return true
}

func (c *loadController) start(w http.ResponseWriter, r *http.Request) {
	if !loadSameOrigin(w, r) {
		return
	}
	var cfg loadConfig
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		loadError(w, http.StatusBadRequest, "invalid JSON configuration")
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		loadError(w, http.StatusBadRequest, "expected one JSON object")
		return
	}
	if cfg.RPS < 1 || cfg.RPS > 10000 || cfg.Devices < 1 || cfg.Devices > 100 || cfg.Duration < 1 || cfg.Duration > 3600 {
		loadError(w, http.StatusBadRequest, "RPS: 1–10000; devices: 1–100; duration_seconds: 1–3600")
		return
	}
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		loadError(w, http.StatusConflict, "a load test is already running")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.latency = 0
	c.result = loadResult{ID: uuid.NewString(), Config: cfg, State: "running", Started: time.Now().UTC()}
	c.mu.Unlock()
	// Snapshot before starting avoids returning a different run when a short run finishes.
	result := c.snapshot()
	go c.runLoad(ctx, cfg)
	loadJSON(w, http.StatusAccepted, result)
}

func (c *loadController) runLoad(ctx context.Context, cfg loadConfig) {
	transport := &http.Transport{MaxIdleConns: cfg.Devices, MaxIdleConnsPerHost: cfg.Devices, IdleConnTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	jobs := make(chan model.Reading)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Devices; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for reading := range jobs {
				c.sendLoad(ctx, client, reading)
			}
		}()
	}
	start := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	seq := make([]int64, cfg.Devices)
	// Absolute deadlines prevent latency from accumulating into scheduling drift.
	// A busy generator skips slots; it never queues a burst to catch up later.
	total := int64(cfg.RPS) * int64(cfg.Duration)
	for slot := int64(0); slot < total; slot++ {
		deadline := start.Add(time.Duration(slot) * time.Second / time.Duration(cfg.RPS))
		timer.Reset(time.Until(deadline))
		select {
		case <-ctx.Done():
			goto finished
		case <-timer.C:
		}
		lateSlots := int64(time.Since(deadline)) * int64(cfg.RPS) / int64(time.Second)
		if lateSlots > 0 {
			if lateSlots > total-slot {
				lateSlots = total - slot
			}
			c.mu.Lock()
			c.result.Skipped += lateSlots
			c.mu.Unlock()
			slot += lateSlots
			if slot >= total {
				break
			}
		}
		device := int(slot % int64(cfg.Devices))
		reading := model.Reading{UniID: uuid.New(), DeviceID: simulator.DeviceID(device + 1),
			Seq: seq[device] + 1, RecordTime: time.Now().UTC().Truncate(time.Microsecond), Freq: 1000}
		// Update Sent before handing off, so live completed never exceeds sent.
		c.mu.Lock()
		select {
		case jobs <- reading:
			seq[device]++
			c.result.Sent++
		default:
			c.result.Skipped++
		}
		c.mu.Unlock()
	}
	// Keep the configured generation window even when the last slot was sent early.
	timer.Reset(time.Until(start.Add(time.Duration(cfg.Duration) * time.Second)))
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
finished:
	close(jobs)
	c.mu.Lock()
	if ctx.Err() == nil {
		c.result.State = "draining"
	}
	c.mu.Unlock()
	wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.result.Finished = &now
	c.result.State = "completed"
	if ctx.Err() != nil {
		c.result.State = "stopped"
	}
	c.cancel()
	c.cancel = nil
}

func (c *loadController) sendLoad(ctx context.Context, client *http.Client, reading model.Reading) {
	body, _ := json.Marshal(reading)
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.target+"/v1/readings", bytes.NewReader(body))
	var status int
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		var resp *http.Response
		resp, err = client.Do(req)
		if resp != nil {
			status = resp.StatusCode
			_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			if err == nil {
				err = readErr
			}
		}
	}
	elapsed := time.Since(start)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.result.Completed++
	c.latency += elapsed
	ms := float64(elapsed) / float64(time.Millisecond)
	if ms > c.result.MaxMS {
		c.result.MaxMS = ms
	}
	switch {
	case err != nil && ctx.Err() != nil:
		c.result.Canceled++
	case err != nil:
		c.result.NetworkErrors++
	case status == http.StatusAccepted:
		c.result.Accepted++
	case status >= 400 && status < 500:
		c.result.ClientErrors++
	case status >= 500:
		c.result.ServerErrors++
	default:
		c.result.Other++
	}
}
