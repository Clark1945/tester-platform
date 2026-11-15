package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HandlerFunc 處理一筆訊息，並且**必須**自行 ack / nack / reject。
// ctx 在服務關閉時會被取消，handler 可藉此中斷等待；但已開始的寫入應該做完。
type HandlerFunc func(ctx context.Context, d amqp.Delivery)

// Consumer 以手動 ack 消費 queue，斷線時自動重連。
type Consumer struct {
	url      string
	queue    string
	prefetch int
	workers  int
	log      *slog.Logger

	connected atomic.Bool
}

// NewConsumer 建立 consumer；需呼叫 Run 才會開始消費。
func NewConsumer(url, queue string, prefetch, workers int, log *slog.Logger) *Consumer {
	return &Consumer{url: url, queue: queue, prefetch: prefetch, workers: workers, log: log}
}

// Connected 回報目前是否正在消費（供 /healthz 使用）。
func (c *Consumer) Connected() bool {
	return c.connected.Load()
}

// Run 消費訊息直到 ctx 取消。返回時所有處理中的訊息都已完成並 ack/nack。
func (c *Consumer) Run(ctx context.Context, handle HandlerFunc) {
	runWithReconnect(ctx, c.log, "consumer", func(ctx context.Context, onConnected func()) error {
		return c.session(ctx, onConnected, handle)
	})
	c.log.Info("consumer stopped")
}

func (c *Consumer) session(ctx context.Context, onConnected func(), handle HandlerFunc) error {
	conn, err := dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // 關閉失敗沒有可補救的動作

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	// prefetch：broker 最多同時給這個 consumer N 筆未 ack 的訊息。
	// 沒設的話 broker 會把整個 queue 倒過來，receiver 掛掉時全部要重送。
	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}
	if err := declareQueue(ch, c.queue); err != nil {
		return err
	}

	const consumerTag = "receiver"
	deliveries, err := ch.Consume(
		c.queue,
		consumerTag,
		false, // autoAck 關閉：處理成功後才手動 ack
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming: %w", err)
	}

	c.connected.Store(true)
	defer c.connected.Store(false)
	onConnected()
	c.log.Info("consumer connected", "queue", c.queue, "prefetch", c.prefetch, "workers", c.workers)

	// 收到關閉訊號時取消訂閱：broker 不再送新訊息，library 把已送達的交完後關閉 deliveries。
	// 連線本身保持開啟，處理中的訊息才 ack 得回去。
	sessionDone := make(chan struct{})
	defer close(sessionDone)
	go func() {
		select {
		case <-ctx.Done():
			if err := ch.Cancel(consumerTag, false); err != nil {
				c.log.Warn("cancel consumer failed", "error", err)
			}
		case <-sessionDone:
		}
	}()

	var wg sync.WaitGroup
	for range c.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// deliveries 被關閉（取消訂閱或斷線）時 range 結束
			for d := range deliveries {
				// 關閉訊號已到：已預取（prefetch）但還沒開始處理的訊息直接還給 queue。
				// 優雅關閉要等的是「處理中」的訊息，不是把 buffer 裡的 50 筆都做完——
				// 資料庫故障時那會拖過容器的停止寬限而被 SIGKILL。
				if ctx.Err() != nil {
					if err := d.Nack(false, true); err != nil {
						c.log.Warn("requeue on shutdown failed; broker will redeliver", "error", err)
					}
					continue
				}
				handle(ctx, d)
			}
		}()
	}
	wg.Wait()

	if ctx.Err() != nil {
		return nil // 正常關閉
	}
	return errors.New("delivery channel closed")
}
