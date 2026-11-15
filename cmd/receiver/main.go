// receiver 服務：消費 RabbitMQ 的訊息並寫入資料庫，寫入成功後才 ack。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"tester-receiver/internal/config"
	"tester-receiver/internal/metrics"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/notify"
	"tester-receiver/internal/retry"
	"tester-receiver/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "receiver")

	if err := run(logger); err != nil {
		logger.Error("receiver exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConnect()
	st, err := store.New(connectCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer st.Close()

	// DLQ 的 publisher 用自己的連線，並且要活得比 consumer 久：
	// 關閉途中仍在處理的訊息可能還需要進 DLQ。所以不直接用會被 signal 取消的 ctx。
	dlqCtx, stopDLQ := context.WithCancel(context.Background())
	defer stopDLQ()
	dlq := mq.NewPublisher(cfg.AMQPURL, cfg.DLQName(), cfg.PublishConfirmTimeout, logger)
	dlqDone := make(chan struct{})
	go func() {
		defer close(dlqDone)
		dlq.Run(dlqCtx)
	}()

	counters := &metrics.ReceiverCounters{}
	alerts := notify.New(cfg.WebhookURL, "receiver", cfg.WebhookMinInterval, logger)

	proc := &processor{
		store:       st,
		dlq:         dlq,
		alerts:      alerts,
		metrics:     counters,
		log:         logger,
		queue:       cfg.QueueName,
		timeout:     cfg.RequestTimeout,
		backoff:     retry.Backoff{Base: cfg.RetryBase, Max: cfg.RetryMax},
		maxAttempts: cfg.RetryMaxAttempts,
	}
	consumer := mq.NewConsumer(cfg.AMQPURL, cfg.QueueName, cfg.Prefetch, cfg.Workers, logger)

	// consumer.Run 會阻塞到 ctx 取消且處理中的訊息都完成；用 channel 得知它何時結束
	handle := proc.handle
	if cfg.FaultStall {
		logger.Warn("FAULT_STALL is enabled: workers will hold messages without processing them")
		handle = stall
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Run(ctx, handle)
	}()

	// watchdog 隨關閉訊號一起停；它只讀數值，不需要等它收尾
	if cfg.RabbitMQMgmtURL == "" {
		logger.Warn("RABBITMQ_MGMT_URL is not set; watchdog disabled")
	} else {
		mgmt, err := mq.NewManagement(cfg.RabbitMQMgmtURL)
		if err != nil {
			return err
		}
		wd := &watchdog{
			queues:     mgmt,
			counters:   counters,
			alerts:     alerts,
			log:        logger,
			queue:      cfg.QueueName,
			dlq:        cfg.DLQName(),
			interval:   cfg.WatchdogInterval,
			staleAfter: cfg.WatchdogStaleAfter,
			now:        time.Now,
		}
		go wd.run(ctx)
		logger.Info("watchdog started", "interval", cfg.WatchdogInterval.String(), "stale_after", cfg.WatchdogStaleAfter.String())
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           routes(st, consumer, counters),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("receiver health endpoint listening", "addr", cfg.HTTPAddr)
		serverErr <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-serverErr:
		runErr = fmt.Errorf("http server: %w", err)
		stop() // 讓 consumer 也收到關閉訊號
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	// 優雅關閉：先等 consumer 把處理中的訊息做完並 ack，再關 HTTP 與資料庫連線池
	select {
	case <-consumerDone:
	case <-time.After(shutdownTimeout):
		// 逾時就放棄等待：未 ack 的訊息會在連線關閉後由 broker 重新投遞，不會遺失
		logger.Warn("consumer did not stop in time; unacked messages will be redelivered")
	}

	// consumer 停了之後才停 DLQ publisher
	stopDLQ()
	<-dlqDone

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown http server: %w", err)
	}
	if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) && runErr == nil {
		runErr = fmt.Errorf("http server: %w", err)
	}

	if runErr == nil {
		logger.Info("receiver stopped cleanly")
	}
	return runErr
}

type pinger interface {
	Ping(ctx context.Context) error
}

type connectionReporter interface {
	Connected() bool
}

// stall 是 FAULT_STALL 的 handler：拿著訊息不處理，直到服務關閉才還給 queue。
func stall(ctx context.Context, d amqp.Delivery) {
	<-ctx.Done()
	_ = d.Nack(false, true) // 失敗代表連線已斷，broker 會自行重新投遞
}

// routes：/healthz 要求資料庫與 broker 都可用才算健康；/metrics 輸出計數器。
func routes(db pinger, broker connectionReporter, counters *metrics.ReceiverCounters) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler(counters.Snapshot))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status := map[string]string{"status": "ok", "broker": "ok", "database": "ok"}
		code := http.StatusOK
		if err := db.Ping(ctx); err != nil {
			status["status"], status["database"] = "unavailable", "down"
			code = http.StatusServiceUnavailable
		}
		if !broker.Connected() {
			status["status"], status["broker"] = "unavailable", "down"
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(status) // header 已送出，寫入失敗（client 斷線）無從補救
	})
	return mux
}
