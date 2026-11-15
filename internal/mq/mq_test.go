package mq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPublish_FailsFastWhenNotConnected(t *testing.T) {
	// 沒有呼叫 Run，所以永遠是未連線狀態
	p := NewPublisher("amqp://unused", "q", 5*time.Second, quietLog())

	start := time.Now()
	err := p.Publish(context.Background(), Message{ID: "id", Body: []byte("{}")})

	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Publish took %v; it should fail immediately when disconnected", elapsed)
	}
}

// 以下為整合測試：需要 docker compose 的 RabbitMQ。沒有設定 AMQP_URL 時跳過。
func testURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL not set; skipping integration test")
	}
	return url
}

// tempQueue 回傳本次測試專用的 queue 名稱，測試結束時刪除。
func tempQueue(t *testing.T, url string) string {
	t.Helper()
	name := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		conn, err := amqp.Dial(url)
		if err != nil {
			t.Errorf("cleanup dial: %v", err)
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Errorf("cleanup channel: %v", err)
			return
		}
		if _, err := ch.QueueDelete(name, false, false, false); err != nil {
			t.Errorf("cleanup delete queue: %v", err)
		}
	})
	return name
}

func waitConnected(t *testing.T, connected func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !connected() {
		if time.Now().After(deadline) {
			t.Fatal("not connected to broker within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPublishAndConsume_RoundTrip(t *testing.T) {
	url := testURL(t)
	queue := tempQueue(t, url)

	ctx, cancel := context.WithCancel(context.Background())
	var running sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		running.Wait() // 確認 Run 在 ctx 取消後真的會返回
	})

	pub := NewPublisher(url, queue, 5*time.Second, quietLog())
	running.Add(1)
	go func() { defer running.Done(); pub.Run(ctx) }()
	waitConnected(t, pub.Connected)

	// 多個 goroutine 同時發佈：驗證「單一 publisher goroutine」的設計在並行下正確
	const total = 200
	var publishers sync.WaitGroup
	errs := make(chan error, total)
	for i := range total {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			id := fmt.Sprintf("msg-%d", i)
			msg := Message{ID: id, Body: []byte(id), Headers: map[string]any{"x-test": "yes", "x-attempts": int32(3)}}
			if err := pub.Publish(ctx, msg); err != nil {
				errs <- err
			}
		}()
	}
	publishers.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("publish: %v", err)
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		done = make(chan struct{})
	)
	cons := NewConsumer(url, queue, 20, 4, quietLog())
	running.Add(1)
	go func() {
		defer running.Done()
		cons.Run(ctx, func(_ context.Context, d amqp.Delivery) {
			if d.DeliveryMode != amqp.Persistent {
				t.Errorf("message %s is not persistent", d.MessageId)
			}
			if d.Headers["x-test"] != "yes" || d.Headers["x-attempts"] != int32(3) {
				t.Errorf("message %s headers = %v", d.MessageId, d.Headers)
			}
			mu.Lock()
			seen[string(d.Body)]++
			complete := len(seen) == total
			mu.Unlock()

			if err := d.Ack(false); err != nil {
				t.Errorf("ack: %v", err)
			}
			if complete {
				select {
				case <-done:
				default:
					close(done)
				}
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		t.Fatalf("received %d/%d messages within 15s", got, total)
	}

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %s delivered %d times, want 1", id, n)
		}
	}
}
