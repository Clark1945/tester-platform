package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrUnavailable 代表訊息「未被確認」進入 broker：未連線、發佈失敗、confirm 逾時或被 nack。
// 呼叫端應回 503 讓 device 重試。注意逾時的情況下訊息仍可能已進入 queue，
// 重試會造成重複，由下游的冪等寫入吸收。
var ErrUnavailable = errors.New("broker unavailable")

// Message 是要發佈的一筆訊息。
type Message struct {
	ID      string         // 對應 AMQP 的 message-id，方便在管理介面追查
	Body    []byte
	Headers map[string]any // 選填；DLQ 用來記錄失敗原因。值必須是 AMQP 支援的型別（字串、整數、時間…）
}

type publishRequest struct {
	ctx   context.Context
	msg   amqp.Publishing
	reply chan publishReply // buffer 1：publisher goroutine 回覆時不會被卡住
}

type publishReply struct {
	confirm *amqp.DeferredConfirmation
	err     error
}

// Publisher 以「單一 goroutine 持有 amqp channel」的方式發佈訊息。
//
// amqp channel 不是 goroutine-safe，所以 HTTP handler 不直接碰它，
// 而是把請求丟進 Go channel（reqs），由 Run 的 goroutine 依序發佈。
// 等待 confirm 則在各自的 handler goroutine 進行，不會擋住後面的發佈。
type Publisher struct {
	url            string
	queue          string
	confirmTimeout time.Duration
	log            *slog.Logger

	reqs      chan publishRequest
	connected atomic.Bool
}

// NewPublisher 建立 publisher；需另外以 goroutine 執行 Run 才會開始連線。
func NewPublisher(url, queue string, confirmTimeout time.Duration, log *slog.Logger) *Publisher {
	return &Publisher{
		url:            url,
		queue:          queue,
		confirmTimeout: confirmTimeout,
		log:            log,
		reqs:           make(chan publishRequest), // 無 buffer：沒人接手就代表 publisher 忙碌或斷線
	}
}

// Connected 回報目前是否有可用的 broker 連線（供 /healthz 使用）。
func (p *Publisher) Connected() bool {
	return p.connected.Load()
}

// Run 維持與 broker 的連線並處理發佈請求，直到 ctx 取消才返回。
func (p *Publisher) Run(ctx context.Context) {
	runWithReconnect(ctx, p.log, "publisher", p.session)
	p.log.Info("publisher stopped")
}

// session 是一次連線的生命週期：連線、開啟 confirm 模式、處理請求直到出錯。
func (p *Publisher) session(ctx context.Context, onConnected func()) error {
	conn, err := dial(p.url)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // 關閉失敗沒有可補救的動作

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	// confirm 模式：broker 會對每筆訊息回 ack（已落地）或 nack
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	if err := declareQueue(ch, p.queue); err != nil {
		return err
	}

	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	p.connected.Store(true)
	defer p.connected.Store(false)
	onConnected()
	p.log.Info("publisher connected", "queue", p.queue)

	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr := <-connClosed:
			return closeError("connection", amqpErr)
		case amqpErr := <-chClosed:
			return closeError("channel", amqpErr)
		case req := <-p.reqs:
			// 這個請求在排隊時就已經逾時或被取消（例如 client 斷線）：直接回覆，不碰 amqp channel。
			// 這是「這一個請求」的問題，不是連線的問題。
			if err := req.ctx.Err(); err != nil {
				req.reply <- publishReply{err: err}
				continue
			}
			confirm, err := ch.PublishWithDeferredConfirmWithContext(
				req.ctx,
				"",      // default exchange：routing key 就是 queue 名稱
				p.queue, // routing key
				false,   // mandatory
				false,   // immediate
				req.msg,
			)
			req.reply <- publishReply{confirm: confirm, err: err}
			if err != nil && !requestScoped(err) {
				// 連線或 channel 層級的失敗：這條 channel 的狀態不可信，重建連線。
				// 只因為單一請求的 context 到期就拆掉連線的話，其他正在等 confirm 的請求
				// 會全部跟著失敗，一個慢請求被放大成一波 503。
				return fmt.Errorf("publish: %w", err)
			}
		}
	}
}

// requestScoped 回報這個錯誤是否只屬於單一請求（它自己的 context 到期或被取消），
// 與連線是否健康無關。
func requestScoped(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Publish 發佈一筆 persistent 訊息，並等到 broker 確認後才返回 nil。
// 任何未確認的情況都回傳包裝 ErrUnavailable 的錯誤。可被多個 goroutine 同時呼叫。
func (p *Publisher) Publish(ctx context.Context, m Message) error {
	if !p.connected.Load() {
		return fmt.Errorf("%w: not connected", ErrUnavailable) // 斷線時立刻失敗，不讓請求堆積
	}

	ctx, cancel := context.WithTimeout(ctx, p.confirmTimeout)
	defer cancel()

	req := publishRequest{
		ctx: ctx,
		msg: amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // 寫入磁碟，broker 重啟後訊息還在
			MessageId:    m.ID,
			Timestamp:    time.Now().UTC(),
			Headers:      amqp.Table(m.Headers),
			Body:         m.Body,
		},
		reply: make(chan publishReply, 1),
	}

	select {
	case p.reqs <- req:
	case <-ctx.Done():
		return fmt.Errorf("%w: waiting for publisher: %v", ErrUnavailable, ctx.Err())
	}

	// publisher goroutine 一旦收下請求就必定回覆，所以這裡不需要 select
	reply := <-req.reply
	if reply.err != nil {
		return fmt.Errorf("%w: publish: %v", ErrUnavailable, reply.err)
	}

	acked, err := reply.confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: waiting for confirm: %v", ErrUnavailable, err)
	}
	if !acked {
		return fmt.Errorf("%w: broker nacked the message", ErrUnavailable)
	}
	return nil
}
