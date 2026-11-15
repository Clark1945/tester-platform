// api 服務：接收 device 的 HTTP 請求，驗證後發佈到 RabbitMQ，
// 等 broker 確認（publisher confirm）才回 202。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"tester-receiver/internal/config"
	"tester-receiver/internal/devices"
	"tester-receiver/internal/metrics"
	"tester-receiver/internal/mq"
	"tester-receiver/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "api")

	// main 只負責把 run 的錯誤轉成 exit code；
	// 這樣 run 裡的 defer（關連線池等）在結束前一定會執行，os.Exit 則不會跑 defer。
	if err := run(logger); err != nil {
		logger.Error("api exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// 收到 SIGINT / SIGTERM 時 ctx 會被取消，作為整個服務的關閉訊號
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConnect()
	st, err := store.New(connectCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer st.Close()

	// 啟動時必須成功載入一次 device 名單，否則所有請求都會被當成未知 device
	cache := devices.NewCache(st, logger)
	if err := cache.Refresh(connectCtx); err != nil {
		return err
	}
	logger.Info("device cache loaded", "devices", cache.Len())

	// 背景 goroutine 用獨立的 context：關閉時要等 HTTP 的 in-flight 請求做完，
	// 才能停 publisher（那些請求還在等 confirm）。所以不能直接用會被 signal 取消的 ctx。
	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	var background sync.WaitGroup

	pub := mq.NewPublisher(cfg.AMQPURL, cfg.QueueName, cfg.PublishConfirmTimeout, logger)
	background.Add(2)
	go func() { defer background.Done(); pub.Run(bgCtx) }()
	go func() { defer background.Done(); cache.Run(bgCtx, cfg.DeviceRefreshInterval) }()

	h := &handler{
		store:     st,
		devices:   cache,
		publisher: pub,
		metrics:   &metrics.APICounters{},
		log:       logger,
		timeout:   cfg.RequestTimeout,
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// ListenAndServe 會阻塞，所以放到 goroutine；錯誤透過 channel 傳回來
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		serverErr <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-serverErr:
		// 還沒收到關閉訊號就結束，代表啟動失敗（例如 port 被佔用）
		runErr = fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		logger.Info("shutdown signal received")

		// 優雅關閉第一步：停止接受新連線，等 in-flight 請求完成（最多 shutdownTimeout）
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			runErr = fmt.Errorf("graceful shutdown: %w", err)
		} else if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("http server: %w", err)
		}
	}

	// 第二步：HTTP 已停，再停 publisher 與快取刷新，並等它們結束
	stopBackground()
	background.Wait()

	if runErr == nil {
		logger.Info("api stopped cleanly")
	}
	return runErr
}
