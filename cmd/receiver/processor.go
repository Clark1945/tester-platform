package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"tester-receiver/internal/metrics"
	"tester-receiver/internal/model"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/notify"
	"tester-receiver/internal/retry"
	"tester-receiver/internal/store"
)

// 進 DLQ 的原因，寫在訊息的 x-failure-reason header。
const (
	reasonInvalidPayload   = "invalid_payload"
	reasonUnknownDevice    = "unknown_device"
	reasonKeyConflict      = "key_conflict" // 同一個 uni_id + record_time 出現了內容不同的資料
	reasonPermanentError   = "permanent_error"
	reasonRetriesExhausted = "retries_exhausted"
	reasonPanic            = "panic"
)

const alertKindDLQ = "dlq"

type readingStore interface {
	Ping(ctx context.Context) error
	InsertReading(ctx context.Context, r model.Reading) (bool, error)
}

type deadLetterPublisher interface {
	Publish(ctx context.Context, m mq.Message) error
}

type alerter interface {
	Notify(ctx context.Context, a notify.Alert)
}

type processor struct {
	store       readingStore
	dlq         deadLetterPublisher
	alerts      alerter
	metrics     *metrics.ReceiverCounters
	log         *slog.Logger
	queue       string        // 來源 queue 名稱，記在 DLQ 訊息的 header
	timeout     time.Duration // 單次資料庫操作 / DLQ 發佈的上限
	backoff     retry.Backoff
	maxAttempts int // 單筆暫時錯誤最多嘗試幾次（含第一次）
}

// job 是一筆處理中的訊息。settled 記錄它是否已被 ack / nack，
// 讓 panic 的補救流程知道還需不需要處置，避免對同一筆訊息 ack 兩次。
type job struct {
	d       amqp.Delivery
	log     *slog.Logger
	start   time.Time // 收到訊息的時間，用來量測處理時間（含重試與退避）
	settled bool
}

func (j *job) ack() {
	j.settle("ack", j.d.Ack(false))
}

func (j *job) requeue() {
	j.settle("nack-requeue", j.d.Nack(false, true))
}

// settle 檢查 ack / nack 本身是否成功。
// 失敗通常是連線已斷：broker 會把未 ack 的訊息重新投遞，冪等寫入會吸收重複。
func (j *job) settle(action string, err error) {
	j.settled = true
	if err != nil {
		j.log.Warn("acknowledgement failed; broker will redeliver", "action", action, "error", err)
	}
}

// handle 是 mq.HandlerFunc：處理一筆訊息，結局只有三種——
//  1. 寫入成功 → ack
//  2. 重複 → ack
//  3. 永久錯誤 / 重試耗盡 / panic → 發到 DLQ 後 ack，並發告警
//
// 另外只有在「服務正在關閉」或「DLQ 發不出去」時會把訊息放回 queue。
// 沒有任何一條路徑是只寫 log 就丟掉訊息的。
func (p *processor) handle(ctx context.Context, d amqp.Delivery) {
	j := &job{d: d, log: p.log.With("redelivered", d.Redelivered), start: time.Now()}
	p.metrics.Received.Add(1)

	// 一筆訊息 panic 不能讓整個 receiver 掛掉（那會讓 prefetch 的 50 筆全部重送，
	// 而且毒訊息重送後會再 panic，形成 crash loop）。攔下來當成失敗，走 DLQ。
	defer func() {
		if r := recover(); r != nil {
			j.log.Error("panic while processing message", "panic", r, "stack", string(debug.Stack()))
			if !j.settled {
				p.deadLetter(ctx, j, reasonPanic, fmt.Errorf("panic: %v", r), 1)
			}
		}
	}()

	reading, err := model.ParseReading(d.Body)
	if err != nil {
		p.deadLetter(ctx, j, reasonInvalidPayload, err, 1)
		return
	}
	j.log = j.log.With("device_id", reading.DeviceID, "uni_id", reading.UniID, "seq", reading.Seq)

	var (
		attempt int // 這筆訊息的嘗試次數（Unavailable 不計入）
		outage  int // 連續遇到「資料庫不可用」的次數，只用來算退避時間
	)
	for {
		inserted, err := p.write(ctx, reading)
		if err == nil {
			if inserted {
				p.metrics.Settle(metrics.ReceiverWritten, time.Since(j.start))
			} else {
				p.metrics.Settle(metrics.ReceiverDuplicate, time.Since(j.start))
				j.log.Info("duplicate reading ignored")
			}
			j.ack()
			return
		}

		class := classify(err)
		if class == retry.Transient {
			attempt++
			if attempt >= p.maxAttempts {
				// 重試用完了。進 DLQ 之前先確認資料庫是否還活著：
				// 資料庫沒回應時所有操作都會逾時，看起來就像「這筆訊息一直失敗」，
				// 但問題其實不在訊息，不該把它丟進 DLQ。
				if p.databaseReachable(ctx) {
					p.deadLetter(ctx, j, reasonRetriesExhausted, err, attempt)
					return
				}
				class = retry.Unavailable
			}
		}

		var delay time.Duration
		switch class {
		case retry.Permanent:
			reason := reasonPermanentError
			switch {
			case errors.Is(err, store.ErrUnknownDevice):
				reason = reasonUnknownDevice
			case errors.Is(err, store.ErrKeyConflict):
				reason = reasonKeyConflict
			}
			p.deadLetter(ctx, j, reason, err, attempt+1)
			return
		case retry.Unavailable:
			// 資料庫不可用期間的失敗不是這筆訊息的問題，所以把它的重試額度還給它。
			// 不歸零的話，停機期間累積的逾時會把額度用完；資料庫一恢復，
			// 只要再失敗一次（例如恢復前就發出、正好逾時的那次寫入），Ping 已經成功，
			// 訊息就會被誤判為「資料庫活著但寫不進去」而進 DLQ。
			attempt = 0
			outage++
			delay = p.backoff.Delay(outage)
		default: // Transient
			delay = p.backoff.Delay(attempt)
		}

		p.metrics.Retried.Add(1)
		j.log.Warn("write failed; retrying after backoff",
			"error", err, "class", class.String(), "attempt", attempt, "delay", delay.String())

		// 退避等待可被關閉訊號中斷：關閉時把訊息還給 queue，下次啟動再處理
		if retry.Sleep(ctx, delay) != nil {
			j.log.Info("shutting down during retry; message returned to queue")
			p.metrics.Settle(metrics.ReceiverRequeued, time.Since(j.start))
			j.requeue()
			return
		}
	}
}

// write 嘗試寫入一次。
//
// device 是否存在由資料庫的 foreign key 把關（store 會回報 ErrUnknownDevice），這裡不另外查詢。
// api 的快取只是前置過濾；這個 foreign key 才是權威，而且檢查與寫入是同一個動作。
func (p *processor) write(ctx context.Context, reading model.Reading) (inserted bool, err error) {
	// 用不受關閉訊號影響的 context：服務關閉時，已開始的寫入要做完
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.timeout)
	defer cancel()

	inserted, err = p.store.InsertReading(ctx, reading)
	if err != nil {
		return false, fmt.Errorf("insert: %w", err)
	}
	return inserted, nil
}

// classify 在資料庫錯誤分類之上，加上這兩種「資料本身有問題」的永久錯誤。
func classify(err error) retry.Class {
	if errors.Is(err, store.ErrUnknownDevice) || errors.Is(err, store.ErrKeyConflict) {
		return retry.Permanent
	}
	return retry.Classify(err)
}

func (p *processor) databaseReachable(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	return p.store.Ping(ctx) == nil
}

// deadLetter 把訊息發到 DLQ，**確認進了 DLQ 之後**才 ack 原訊息，最後發告警。
//
// 順序不能反：先 ack 再發 DLQ，中間掛掉訊息就兩邊都不在了。
// DLQ 發不出去（broker 出問題）時不 ack，稍等後把原訊息放回 queue，之後會再走一次這個流程。
func (p *processor) deadLetter(ctx context.Context, j *job, reason string, cause error, attempts int) {
	log := j.log.With("reason", reason, "attempts", attempts)

	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.timeout)
	defer cancel()

	err := p.dlq.Publish(pubCtx, mq.Message{
		ID:   j.d.MessageId,
		Body: j.d.Body, // 保留原始 payload，修好問題後可以原封不動重放
		Headers: map[string]any{
			"x-failure-reason": reason,
			"x-failure-detail": truncate(cause.Error(), 512),
			"x-attempts":       int32(attempts),
			"x-original-queue": p.queue,
			"x-failed-at":      time.Now().UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		log.Error("dead-letter publish failed; returning message to queue", "error", err, "cause", cause)
		_ = retry.Sleep(ctx, p.backoff.Max) // 避免 hot loop；關閉時會提前返回
		p.metrics.Settle(metrics.ReceiverRequeued, time.Since(j.start))
		j.requeue()
		return
	}

	log.Error("message dead-lettered", "error", cause, "body", truncate(string(j.d.Body), 256))
	p.metrics.Settle(metrics.ReceiverDLQ, time.Since(j.start))
	j.ack()

	p.alerts.Notify(ctx, notify.Alert{
		Kind:    alertKindDLQ,
		Message: "message moved to dead letter queue",
		Fields: map[string]any{
			"reason":     reason,
			"detail":     truncate(cause.Error(), 256),
			"message_id": j.d.MessageId,
			"queue":      p.queue + ".dlq",
		},
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
