package main

import (
	"context"
	"log/slog"
	"time"

	"tester-receiver/internal/metrics"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/notify"
)

const (
	alertKindDLQNotEmpty   = "dlq_not_empty"
	alertKindConsumerStuck = "consumer_stuck"
	alertKindNoWrites      = "no_writes"
)

type depthReader interface {
	QueueDepth(ctx context.Context, queue string) (mq.QueueDepth, error)
}

// watchdog 定期檢查「receiver 活著但沒在做事」的狀況。
//
// /healthz 只能回答「行程還在、連線還在」。consumer 卡住時（例如所有 worker 都在等一個
// 永遠不回應的呼叫）行程和連線都正常，healthz 是綠的，但資料不再進資料庫。
// 這種故障只能從「結果」來發現：queue 在長、卻沒有訊息被處理完。
type watchdog struct {
	queues     depthReader
	counters   *metrics.ReceiverCounters
	alerts     alerter
	log        *slog.Logger
	queue      string
	dlq        string
	interval   time.Duration
	staleAfter time.Duration // 多久沒有寫入算異常
	now        func() time.Time

	startedAt time.Time
	prev      *watchSample // 上一次檢查的數值；第一次檢查時為 nil
}

type watchSample struct {
	ready   int64
	settled int64
}

func (w *watchdog) run(ctx context.Context) {
	w.startedAt = w.now()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.check(ctx)
		}
	}
}

// check 執行一輪檢查。三個條件各自獨立，各有自己的告警種類（各自節流）。
func (w *watchdog) check(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	main, err := w.queues.QueueDepth(ctx, w.queue)
	if err != nil {
		// 查不到深度時無法判斷，也不能假裝一切正常：留下警告，下一輪再試
		w.log.Warn("watchdog cannot read queue depth", "queue", w.queue, "error", err)
		return
	}
	dead, err := w.queues.QueueDepth(ctx, w.dlq)
	if err != nil {
		w.log.Warn("watchdog cannot read queue depth", "queue", w.dlq, "error", err)
		return
	}

	// 1. DLQ 有東西：有資料沒進資料庫，需要人來看
	if dead.Total() > 0 {
		w.alerts.Notify(ctx, notify.Alert{
			Kind:    alertKindDLQNotEmpty,
			Message: "dead letter queue is not empty",
			Fields:  map[string]any{"queue": w.dlq, "messages": dead.Total()},
		})
	}

	// 2. consumer 卡住：等待中的訊息比上一輪多，但這段期間一筆都沒處理完
	settled := w.counters.Settled()
	if w.prev != nil && main.Ready > w.prev.ready && settled == w.prev.settled {
		w.alerts.Notify(ctx, notify.Alert{
			Kind:    alertKindConsumerStuck,
			Message: "queue is growing but no message has been settled since the last check",
			Fields: map[string]any{
				"queue":          w.queue,
				"ready_previous": w.prev.ready,
				"ready_now":      main.Ready,
				"unacked":        main.Unacked,
				"interval":       w.interval.String(),
			},
		})
	}
	w.prev = &watchSample{ready: main.Ready, settled: settled}

	// 3. 太久沒有寫入，而 queue 裡明明有東西。
	//    從未寫入過時，以啟動時間為基準（否則剛啟動就會誤報）。
	last := w.counters.LastWrittenAt()
	if last.IsZero() {
		last = w.startedAt
	}
	if idle := w.now().Sub(last); idle > w.staleAfter && main.Total() > 0 {
		w.alerts.Notify(ctx, notify.Alert{
			Kind:    alertKindNoWrites,
			Message: "no successful write for too long while the queue is not empty",
			Fields: map[string]any{
				"queue":     w.queue,
				"idle":      idle.Round(time.Second).String(),
				"threshold": w.staleAfter.String(),
				"ready":     main.Ready,
				"unacked":   main.Unacked,
			},
		})
	}
}
