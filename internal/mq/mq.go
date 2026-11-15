// Package mq 封裝 RabbitMQ：publisher（confirm）、consumer（手動 ack）與斷線重連。
package mq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"tester-receiver/internal/retry"
)

const (
	dialTimeout = 5 * time.Second
	heartbeat   = 10 * time.Second
)

// 重連的退避：0.5s 起跳、上限 10s。
var reconnectBackoff = retry.Backoff{Base: 500 * time.Millisecond, Max: 10 * time.Second}

func dial(url string) (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: heartbeat,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(dialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("dial broker: %w", err)
	}
	return conn, nil
}

// declareQueue 宣告 durable queue。宣告是冪等的，api 與 receiver 誰先啟動都可以。
func declareQueue(ch *amqp.Channel, name string) error {
	_, err := ch.QueueDeclare(
		name,
		true,  // durable：broker 重啟後 queue 還在
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("declare queue %q: %w", name, err)
	}
	return nil
}

// runWithReconnect 反覆執行 session，直到 ctx 取消。
// session 回傳代表連線結束；回傳前若曾成功連上（connected() 為 true 過）就重置退避。
func runWithReconnect(ctx context.Context, log *slog.Logger, name string, session func(ctx context.Context, onConnected func()) error) {
	attempt := 0
	for {
		err := session(ctx, func() { attempt = 0 })
		if ctx.Err() != nil {
			return
		}

		attempt++
		delay := reconnectBackoff.Delay(attempt)
		log.Warn("broker session ended; reconnecting",
			"component", name, "error", err, "attempt", attempt, "delay", delay.String())

		if retry.Sleep(ctx, delay) != nil {
			return // ctx 取消
		}
	}
}

// closeError 把 NotifyClose 收到的值轉成 error。
// 連線被正常關閉時，library 只會關閉 channel 而不送值，此時 amqpErr 為 nil。
func closeError(what string, amqpErr *amqp.Error) error {
	if amqpErr == nil {
		return fmt.Errorf("%s closed", what)
	}
	return fmt.Errorf("%s closed: %w", what, amqpErr)
}
