package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"

	"tester-receiver/internal/metrics"
	"tester-receiver/internal/model"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/notify"
	"tester-receiver/internal/retry"
)

// fakeStore 可以排定「接下來幾次寫入要回什麼錯誤」，用來模擬各種故障。
type fakeStore struct {
	devices    map[string]bool
	rows       map[string]bool // key: uni_id
	insertErrs []error         // 依序消耗；用完之後寫入成功
	pingErr    error
	inserts    int // InsertReading 被呼叫的次數
	panicWith  any
}

func newFakeStore() *fakeStore {
	return &fakeStore{devices: map[string]bool{"device-001": true}, rows: map[string]bool{}}
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) DeviceExists(_ context.Context, id string) (bool, error) {
	return f.devices[id], nil
}

func (f *fakeStore) InsertReading(_ context.Context, r model.Reading) (bool, error) {
	f.inserts++
	if f.panicWith != nil {
		panic(f.panicWith)
	}
	if len(f.insertErrs) > 0 {
		err := f.insertErrs[0]
		f.insertErrs = f.insertErrs[1:]
		return false, err
	}
	key := r.UniID.String()
	if f.rows[key] {
		return false, nil
	}
	f.rows[key] = true
	return true, nil
}

// fakeAcker 實作 amqp.Acknowledger，記錄訊息被怎麼處置。
type fakeAcker struct{ calls []string }

func (a *fakeAcker) Ack(uint64, bool) error { a.calls = append(a.calls, "ack"); return nil }

func (a *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	if requeue {
		a.calls = append(a.calls, "requeue")
	} else {
		a.calls = append(a.calls, "drop")
	}
	return nil
}

func (a *fakeAcker) Reject(uint64, bool) error { a.calls = append(a.calls, "reject"); return nil }

type fakeDLQ struct {
	failWith error
	messages []mq.Message
}

func (f *fakeDLQ) Publish(_ context.Context, m mq.Message) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.messages = append(f.messages, m)
	return nil
}

type fakeAlerter struct{ alerts []notify.Alert }

func (f *fakeAlerter) Notify(_ context.Context, a notify.Alert) { f.alerts = append(f.alerts, a) }

type harness struct {
	store  *fakeStore
	dlq    *fakeDLQ
	alerts *fakeAlerter
	acker  *fakeAcker
	proc   *processor
}

func newHarness() *harness {
	h := &harness{store: newFakeStore(), dlq: &fakeDLQ{}, alerts: &fakeAlerter{}, acker: &fakeAcker{}}
	h.proc = &processor{
		store:       h.store,
		dlq:         h.dlq,
		alerts:      h.alerts,
		metrics:     &metrics.ReceiverCounters{},
		log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		queue:       "readings",
		timeout:     time.Second,
		backoff:     retry.Backoff{Base: time.Millisecond, Max: 4 * time.Millisecond}, // 測試用極短的退避
		maxAttempts: 5,
	}
	return h
}

func (h *harness) handle(ctx context.Context, body string) {
	h.proc.handle(ctx, amqp.Delivery{Acknowledger: h.acker, DeliveryTag: 1, MessageId: validID, Body: []byte(body)})
}

// assertSettled 驗證訊息「恰好被處置一次」且方式正確
func (h *harness) assertSettled(t *testing.T, want string) {
	t.Helper()
	if len(h.acker.calls) != 1 || h.acker.calls[0] != want {
		t.Errorf("settlement = %v, want exactly [%s]", h.acker.calls, want)
	}
}

func (h *harness) assertDeadLettered(t *testing.T, reason, body string) {
	t.Helper()
	if len(h.dlq.messages) != 1 {
		t.Fatalf("dead-lettered %d messages, want 1", len(h.dlq.messages))
	}
	m := h.dlq.messages[0]
	if got := m.Headers["x-failure-reason"]; got != reason {
		t.Errorf("x-failure-reason = %v, want %s", got, reason)
	}
	if string(m.Body) != body {
		t.Errorf("DLQ body = %s, want the original payload %s", m.Body, body)
	}
	if m.Headers["x-original-queue"] != "readings" || m.Headers["x-failure-detail"] == "" {
		t.Errorf("missing headers: %v", m.Headers)
	}
	if len(h.alerts.alerts) != 1 || h.alerts.alerts[0].Kind != alertKindDLQ {
		t.Errorf("alerts = %+v, want one %q alert", h.alerts.alerts, alertKindDLQ)
	}
}

const (
	validID  = "3f1c2a52-6f0e-4b7e-9d59-0a4a6c1f2b10"
	validMsg = `{"uni_id":"` + validID + `","device_id":"device-001","seq":1,"record_time":"2026-11-15T12:00:00Z","freq":1000}`
)

var (
	errDeadlock   = &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	errFKViolated = &pgconn.PgError{Code: "23503", Message: "foreign key violation"}
	errDBShutdown = &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}
)

func TestHandle_WrittenAndDuplicateAreAcked(t *testing.T) {
	h := newHarness()
	h.handle(context.Background(), validMsg)
	h.assertSettled(t, "ack")
	if len(h.store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(h.store.rows))
	}

	// 同一筆再來一次（重送）
	h.acker.calls = nil
	h.handle(context.Background(), validMsg)
	h.assertSettled(t, "ack")
	if len(h.store.rows) != 1 || len(h.dlq.messages) != 0 {
		t.Errorf("rows = %d dlq = %d, want 1 and 0", len(h.store.rows), len(h.dlq.messages))
	}
}

func TestHandle_PermanentErrorsGoToDLQWithoutRetry(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		setup   func(*fakeStore)
		reason  string
		inserts int
	}{
		{"unparseable payload", `not json`, nil, reasonInvalidPayload, 0},
		{"missing field", `{"device_id":"device-001"}`, nil, reasonInvalidPayload, 0},
		{"unknown device", validMsg, func(s *fakeStore) { delete(s.devices, "device-001") }, reasonUnknownDevice, 0},
		{"constraint violation", validMsg, func(s *fakeStore) { s.insertErrs = []error{errFKViolated} }, reasonPermanentError, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness()
			if tc.setup != nil {
				tc.setup(h.store)
			}
			h.handle(context.Background(), tc.body)

			h.assertSettled(t, "ack") // 進了 DLQ 之後 ack 原訊息
			h.assertDeadLettered(t, tc.reason, tc.body)
			if h.store.inserts != tc.inserts {
				t.Errorf("insert attempts = %d, want %d (permanent errors must not be retried)", h.store.inserts, tc.inserts)
			}
		})
	}
}

func TestHandle_TransientErrorIsRetriedUntilSuccess(t *testing.T) {
	h := newHarness()
	h.store.insertErrs = []error{errDeadlock, errDeadlock, errDeadlock}

	h.handle(context.Background(), validMsg)

	h.assertSettled(t, "ack")
	if h.store.inserts != 4 || len(h.store.rows) != 1 || len(h.dlq.messages) != 0 {
		t.Errorf("inserts = %d rows = %d dlq = %d, want 4 / 1 / 0", h.store.inserts, len(h.store.rows), len(h.dlq.messages))
	}
}

func TestHandle_TransientErrorExhaustsRetriesThenDLQ(t *testing.T) {
	h := newHarness()
	for range 20 {
		h.store.insertErrs = append(h.store.insertErrs, errDeadlock)
	}

	h.handle(context.Background(), validMsg)

	h.assertSettled(t, "ack")
	h.assertDeadLettered(t, reasonRetriesExhausted, validMsg)
	if h.store.inserts != 5 {
		t.Errorf("insert attempts = %d, want exactly maxAttempts (5)", h.store.inserts)
	}
	if got := h.dlq.messages[0].Headers["x-attempts"]; got != int32(5) {
		t.Errorf("x-attempts = %v, want 5", got)
	}
}

func TestHandle_DatabaseOutageIsRetriedBeyondMaxAttempts(t *testing.T) {
	// 資料庫不可用 30 次（遠超過 maxAttempts）後恢復：訊息不該進 DLQ
	h := newHarness()
	for range 30 {
		h.store.insertErrs = append(h.store.insertErrs, errDBShutdown)
	}

	h.handle(context.Background(), validMsg)

	h.assertSettled(t, "ack")
	if h.store.inserts != 31 || len(h.store.rows) != 1 || len(h.dlq.messages) != 0 {
		t.Errorf("inserts = %d rows = %d dlq = %d, want 31 / 1 / 0", h.store.inserts, len(h.store.rows), len(h.dlq.messages))
	}
}

func TestHandle_TimeoutsDuringOutageDoNotDeadLetter(t *testing.T) {
	// 資料庫沒回應時，錯誤看起來只是 timeout（暫時性）。
	// 重試用完時 Ping 也失敗 → 判定為資料庫不可用，繼續重試而不是進 DLQ。
	h := newHarness()
	for range 12 {
		h.store.insertErrs = append(h.store.insertErrs, context.DeadlineExceeded)
	}
	h.store.pingErr = errors.New("no response")

	h.handle(context.Background(), validMsg)

	h.assertSettled(t, "ack")
	if len(h.dlq.messages) != 0 || len(h.store.rows) != 1 {
		t.Errorf("dlq = %d rows = %d, want 0 and 1", len(h.dlq.messages), len(h.store.rows))
	}
}

func TestHandle_CountersAccountForEveryMessage(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	m := h.proc.metrics

	h.handle(ctx, validMsg)   // written
	h.handle(ctx, validMsg)   // duplicate
	h.handle(ctx, `not json`) // dlq
	h.store.insertErrs = []error{errDeadlock, errDeadlock}
	h.handle(ctx, strings.Replace(validMsg, validID, "11111111-2222-4333-8444-555555555555", 1)) // 重試 2 次後 written

	if m.Received.Load() != 4 || m.Written.Load() != 2 || m.Duplicate.Load() != 1 || m.DLQ.Load() != 1 || m.Retried.Load() != 2 {
		t.Errorf("counters = %v", m.Snapshot())
	}
	// 每筆收到的訊息都有明確的結局
	if got := m.Written.Load() + m.Duplicate.Load() + m.DLQ.Load() + m.Requeued.Load(); got != m.Received.Load() {
		t.Errorf("written+duplicate+dlq+requeued = %d, received = %d", got, m.Received.Load())
	}
	if m.LastWrittenAt().IsZero() {
		t.Error("last_written_at was not updated")
	}
}

func TestHandle_PanicIsRecoveredAndDeadLettered(t *testing.T) {
	h := newHarness()
	h.store.panicWith = "boom"

	h.handle(context.Background(), validMsg) // 不能讓 panic 傳出去

	h.assertSettled(t, "ack")
	h.assertDeadLettered(t, reasonPanic, validMsg)
}

func TestHandle_DLQUnavailableReturnsMessageToQueue(t *testing.T) {
	// DLQ 發不出去時不能 ack（否則訊息兩邊都不在），要放回 queue
	h := newHarness()
	h.dlq.failWith = mq.ErrUnavailable

	h.handle(context.Background(), `not json`)

	h.assertSettled(t, "requeue")
	if len(h.alerts.alerts) != 0 {
		t.Errorf("alert sent although nothing was dead-lettered")
	}
}

func TestHandle_FinishesWriteEvenAfterShutdownSignal(t *testing.T) {
	// 關閉訊號已到（ctx 已取消），但已經領到的訊息仍要寫完並 ack
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := newHarness()
	h.handle(ctx, validMsg)

	h.assertSettled(t, "ack")
	if len(h.store.rows) != 1 {
		t.Errorf("rows = %d, want 1", len(h.store.rows))
	}
}

func TestHandle_ShutdownDuringRetryReturnsMessageToQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := newHarness()
	h.store.insertErrs = []error{errDBShutdown}
	h.proc.backoff = retry.Backoff{Base: time.Minute, Max: time.Minute} // 若沒有提前返回，測試會卡一分鐘

	start := time.Now()
	h.handle(ctx, validMsg)

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("handle blocked for %v during shutdown", elapsed)
	}
	h.assertSettled(t, "requeue")
	if len(h.dlq.messages) != 0 {
		t.Errorf("message was dead-lettered during shutdown")
	}
}
