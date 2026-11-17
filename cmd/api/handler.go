package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"tester-receiver/internal/devices"
	"tester-receiver/internal/metrics"
	"tester-receiver/internal/model"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/store"
)

const (
	maxBodyBytes = 64 << 10 // 64 KiB，一筆 reading 遠小於此
	defaultLimit = 100
	maxLimit     = 1000
)

// 以下三個介面是 handler 需要的能力。
// 介面定義在「使用端」而不是實作端：*store.Store、*devices.Cache、*mq.Publisher
// 不需要宣告就自動滿足它們，測試時可以換成假的實作。

type queryStore interface {
	Ping(ctx context.Context) error
	QueryReadings(ctx context.Context, q store.ReadingQuery) ([]model.Reading, error)
}

type deviceChecker interface {
	Exists(deviceID string) bool
}

type publisher interface {
	Publish(ctx context.Context, m mq.Message) error
	Connected() bool
}

type handler struct {
	store     queryStore
	devices   deviceChecker
	publisher publisher
	metrics   *metrics.APICounters
	activity  *devices.Activity // 記錄哪些 device 最近有送資料；nil 時不記錄
	log       *slog.Logger
	timeout   time.Duration
}

func (h *handler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/readings", h.postReading)
	mux.HandleFunc("GET /v1/readings", h.getReadings)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.Handle("GET /metrics", h.metrics.PrometheusHandler())           // 給 Prometheus 抓取
	mux.Handle("GET /metrics.json", metrics.Handler(h.metrics.Snapshot)) // 給 verify / chaos 對帳
	return mux
}

func (h *handler) postReading(w http.ResponseWriter, r *http.Request) {
	start := time.Now() // 用來量測整個請求的處理時間（含等待 confirm）
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		h.metrics.Observe(metrics.APIRejected, time.Since(start))
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, "cannot read request body")
		return
	}

	reading, err := model.ParseReading(body)
	if err != nil {
		// ParseReading 的錯誤都是 ErrInvalid，訊息可以安全地回給 client
		h.metrics.Observe(metrics.APIRejected, time.Since(start))
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	log := h.log.With("device_id", reading.DeviceID, "uni_id", reading.UniID, "seq", reading.Seq)

	// 只查記憶體快取，不碰資料庫：資料庫故障時 api 仍能收資料
	if !h.devices.Exists(reading.DeviceID) {
		h.metrics.Observe(metrics.APIRejected, time.Since(start))
		h.writeError(w, http.StatusNotFound, "unknown device_id")
		return
	}

	// 發佈的是驗證後重新序列化的內容，而不是原始 body：receiver 收到的格式一定是標準的
	msg, err := json.Marshal(reading)
	if err != nil {
		log.Error("encode message failed", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Publish 等到 broker 確認訊息落地才返回。在那之前不能回 202：
	// 否則 device 以為送達而不再重試，broker 若在此時掛掉，這筆資料就永久遺失。
	if err := h.publisher.Publish(ctx, mq.Message{ID: reading.UniID.String(), Body: msg}); err != nil {
		// 回 503 讓 device 以相同的 uni_id / seq / record_time 重試。
		// 重複的 uni_id 不在這裡判斷，由資料庫的 unique constraint 去重。
		h.metrics.Observe(metrics.APIPublishFailed, time.Since(start))
		log.Warn("publish not confirmed", "error", err)
		h.writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	// 只有 broker 確認後才計入 accepted：這個數字就是恆等式左邊的 published(confirmed)
	h.metrics.Observe(metrics.APIAccepted, time.Since(start))
	h.activity.Seen(reading.DeviceID, time.Now())
	h.writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (h *handler) getReadings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	q, err := parseReadingQuery(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	readings, err := h.store.QueryReadings(ctx, q)
	if err != nil {
		h.log.Error("query failed", "error", err, "device_id", q.DeviceID)
		h.writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"count": len(readings), "readings": readings})
}

// healthz：api 的核心職責是「收資料並送進 broker」，所以健康與否只看 broker 連線。
// 資料庫狀態僅供參考——資料庫掛掉時 api 仍應被視為健康並繼續收資料。
func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status := map[string]string{"status": "ok", "broker": "ok", "database": "ok"}
	code := http.StatusOK

	if err := h.store.Ping(ctx); err != nil {
		status["database"] = "down"
	}
	if !h.publisher.Connected() {
		status["status"], status["broker"] = "unavailable", "down"
		code = http.StatusServiceUnavailable
	}
	h.writeJSON(w, code, status)
}

func parseReadingQuery(r *http.Request) (store.ReadingQuery, error) {
	params := r.URL.Query()
	q := store.ReadingQuery{DeviceID: params.Get("device_id"), Limit: defaultLimit}

	var err error
	if q.From, err = parseTimeParam(params.Get("from"), "from"); err != nil {
		return store.ReadingQuery{}, err
	}
	if q.To, err = parseTimeParam(params.Get("to"), "to"); err != nil {
		return store.ReadingQuery{}, err
	}

	if v := params.Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit < 1 {
			return store.ReadingQuery{}, errors.New("limit must be a positive integer")
		}
		q.Limit = min(limit, maxLimit)
	}
	return q, nil
}

func parseTimeParam(v, name string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339", name)
	}
	return &t, nil
}

func (h *handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// header 已送出，無法再改狀態碼；通常是 client 斷線，只能記錄
		h.log.Warn("write response failed", "error", err)
	}
}

func (h *handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}
