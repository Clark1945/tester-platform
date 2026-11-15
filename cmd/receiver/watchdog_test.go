package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"tester-receiver/internal/metrics"
	"tester-receiver/internal/mq"
)

type fakeDepths struct {
	depths map[string]mq.QueueDepth
	err    error
}

func (f *fakeDepths) QueueDepth(_ context.Context, queue string) (mq.QueueDepth, error) {
	return f.depths[queue], f.err
}

type watchHarness struct {
	w      *watchdog
	depths *fakeDepths
	alerts *fakeAlerter
	clock  time.Time
}

func newWatchHarness() *watchHarness {
	h := &watchHarness{
		depths: &fakeDepths{depths: map[string]mq.QueueDepth{}},
		alerts: &fakeAlerter{},
		clock:  time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC),
	}
	h.w = &watchdog{
		queues:     h.depths,
		counters:   &metrics.ReceiverCounters{},
		alerts:     h.alerts,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		queue:      "readings",
		dlq:        "readings.dlq",
		interval:   time.Minute,
		staleAfter: 2 * time.Minute,
		now:        func() time.Time { return h.clock },
	}
	h.w.startedAt = h.clock
	return h
}

func (h *watchHarness) setQueue(ready, unacked int64) {
	h.depths.depths["readings"] = mq.QueueDepth{Ready: ready, Unacked: unacked}
}

// tick 前進一分鐘並執行一輪檢查，回傳這一輪新產生的告警種類
func (h *watchHarness) tick() []string {
	h.clock = h.clock.Add(time.Minute)
	before := len(h.alerts.alerts)
	h.w.check(context.Background())

	var kinds []string
	for _, a := range h.alerts.alerts[before:] {
		kinds = append(kinds, a.Kind)
	}
	return kinds
}

func contains(kinds []string, want string) bool {
	for _, k := range kinds {
		if k == want {
			return true
		}
	}
	return false
}

func TestWatchdog_HealthyConsumerRaisesNothing(t *testing.T) {
	h := newWatchHarness()

	// queue 有積壓而且在變長，但 receiver 持續有在寫入 → 只是忙，不是卡住
	for i := range 5 {
		h.setQueue(int64(100*(i+1)), 50)
		h.w.counters.MarkWritten(h.clock)
		if kinds := h.tick(); len(kinds) != 0 {
			t.Fatalf("tick %d raised %v, want none", i, kinds)
		}
	}
}

func TestWatchdog_EmptyIdleQueueRaisesNothing(t *testing.T) {
	// 沒有流量：很久沒寫入，但 queue 是空的 → 正常
	h := newWatchHarness()
	for i := range 10 {
		if kinds := h.tick(); len(kinds) != 0 {
			t.Fatalf("tick %d raised %v, want none", i, kinds)
		}
	}
}

func TestWatchdog_DLQNotEmpty(t *testing.T) {
	h := newWatchHarness()
	h.depths.depths["readings.dlq"] = mq.QueueDepth{Ready: 3}

	if kinds := h.tick(); !contains(kinds, alertKindDLQNotEmpty) {
		t.Errorf("alerts = %v, want %s", kinds, alertKindDLQNotEmpty)
	}
}

func TestWatchdog_ConsumerStuck(t *testing.T) {
	h := newWatchHarness()
	h.w.counters.MarkWritten(h.clock)

	h.setQueue(100, 50)
	if kinds := h.tick(); contains(kinds, alertKindConsumerStuck) {
		t.Fatalf("first check has no baseline and must not alert, got %v", kinds)
	}

	// queue 變長，期間沒有任何訊息被處理完
	h.setQueue(160, 50)
	if kinds := h.tick(); !contains(kinds, alertKindConsumerStuck) {
		t.Errorf("alerts = %v, want %s", kinds, alertKindConsumerStuck)
	}

	// 恢復處理後不再告警，即使 queue 還在變長
	h.w.counters.MarkWritten(h.clock)
	h.setQueue(200, 50)
	if kinds := h.tick(); contains(kinds, alertKindConsumerStuck) {
		t.Errorf("alerted although messages were settled: %v", kinds)
	}
}

func TestWatchdog_NoWritesWhileQueueNotEmpty(t *testing.T) {
	h := newWatchHarness()
	h.setQueue(0, 50) // ready 沒有成長（不會觸發 consumer_stuck），但 50 筆卡在 unacked

	// 啟動後從未寫入：前兩分鐘內不算異常
	if kinds := h.tick(); contains(kinds, alertKindNoWrites) {
		t.Fatalf("alerted after 1 minute, threshold is 2 minutes: %v", kinds)
	}
	if kinds := h.tick(); contains(kinds, alertKindNoWrites) {
		t.Fatalf("alerted at exactly the threshold: %v", kinds)
	}
	if kinds := h.tick(); !contains(kinds, alertKindNoWrites) {
		t.Errorf("alerts = %v, want %s after 3 idle minutes", kinds, alertKindNoWrites)
	}

	// 有寫入之後恢復正常
	h.w.counters.MarkWritten(h.clock)
	if kinds := h.tick(); contains(kinds, alertKindNoWrites) {
		t.Errorf("alerted right after a successful write: %v", kinds)
	}
}

func TestWatchdog_DepthUnavailableDoesNotAlertOrPanic(t *testing.T) {
	h := newWatchHarness()
	h.depths.err = errors.New("management api down")

	if kinds := h.tick(); len(kinds) != 0 {
		t.Errorf("alerts = %v, want none when depth is unknown", kinds)
	}
}
