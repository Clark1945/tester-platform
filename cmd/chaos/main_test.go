package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"tester-receiver/internal/mq"
)

// scriptedDepths 依序回傳事先排好的 queue 深度；用完之後重複最後一個
type scriptedDepths struct {
	depths []mq.QueueDepth
	err    error
	calls  int
}

func (s *scriptedDepths) QueueDepth(context.Context, string) (mq.QueueDepth, error) {
	if s.err != nil {
		return mq.QueueDepth{}, s.err
	}
	i := min(s.calls, len(s.depths)-1)
	s.calls++
	return s.depths[i], nil
}

func TestWaitQuiet_WaitsForInFlightMessages(t *testing.T) {
	// purge 之後 ready 是 0，但 receiver 手上還有 50 筆（unacked）。
	// 修正前：這時就清資料表，那 50 筆稍後才寫入，對帳會多出不屬於這次執行的資料。
	q := &scriptedDepths{depths: []mq.QueueDepth{
		{Ready: 0, Unacked: 50}, {Ready: 0, Unacked: 12}, {Ready: 0, Unacked: 0},
	}}
	if err := waitQuietEvery(context.Background(), q, "readings", time.Second, time.Millisecond); err != nil {
		t.Fatalf("waitQuiet: %v", err)
	}
	// 兩次還在忙 + 連續 quietPolls 次安靜
	if want := 2 + quietPolls; q.calls != want {
		t.Errorf("polled %d times, want %d (must not return on the first zero reading)", q.calls, want)
	}
}

func TestWaitQuiet_ASingleZeroReadingIsNotEnough(t *testing.T) {
	// 管理 API 的數字有延遲：看到一次 0 之後又冒出訊息，要重新計數
	q := &scriptedDepths{depths: []mq.QueueDepth{
		{}, {}, {Ready: 30}, {}, {}, {}, {},
	}}
	if err := waitQuietEvery(context.Background(), q, "readings", time.Second, time.Millisecond); err != nil {
		t.Fatalf("waitQuiet: %v", err)
	}
	if q.calls != 3+quietPolls {
		t.Errorf("polled %d times, want %d: the quiet streak must restart after a non-zero reading", q.calls, 3+quietPolls)
	}
}

func TestWaitQuiet_FailsWhenTrafficNeverStops(t *testing.T) {
	q := &scriptedDepths{depths: []mq.QueueDepth{{Ready: 400, Unacked: 50}}}
	err := waitQuietEvery(context.Background(), q, "readings", 30*time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("expected an error when the queue never drains")
	}
	if !strings.Contains(err.Error(), "ready=400") || !strings.Contains(err.Error(), "unacked=50") {
		t.Errorf("error should say what is still in the queue: %v", err)
	}
}

func TestWaitQuiet_PropagatesLookupErrorsAndCancellation(t *testing.T) {
	down := &scriptedDepths{err: errors.New("management api down")}
	if err := waitQuietEvery(context.Background(), down, "readings", time.Second, time.Millisecond); err == nil {
		t.Error("expected the lookup error to be returned")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	busy := &scriptedDepths{depths: []mq.QueueDepth{{Ready: 1}}}
	if err := waitQuietEvery(ctx, busy, "readings", time.Minute, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
