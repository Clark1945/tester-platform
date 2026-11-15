// Package config 從環境變數載入設定。
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 是服務的執行設定。api 與 receiver 共用同一份，各取所需。
type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	RequestTimeout time.Duration

	AMQPURL               string
	QueueName             string
	PublishConfirmTimeout time.Duration // api：等 publisher confirm 的上限
	Prefetch              int           // receiver：未 ack 訊息的上限
	Workers               int           // receiver：處理訊息的 goroutine 數

	DeviceRefreshInterval time.Duration // api：device 快取多久從 DB 刷新一次

	// receiver：失敗處理
	RetryBase          time.Duration // 第一次重試前的等待
	RetryMax           time.Duration // 單次等待的上限
	RetryMaxAttempts   int           // 單筆暫時錯誤最多嘗試幾次（含第一次）
	WebhookURL         string        // 空值則告警只寫 log
	WebhookMinInterval time.Duration // 同類告警的最小間隔（節流）

	// receiver：watchdog
	RabbitMQMgmtURL    string        // 空值則停用 watchdog
	WatchdogInterval   time.Duration // 多久檢查一次
	WatchdogStaleAfter time.Duration // 多久沒有寫入（且 queue 非空）算異常

	// FaultStall 是測試用的故障注入：worker 收到訊息後不處理也不 ack，
	// 模擬「consumer 活著但卡住」，用來驗證 watchdog。正式環境不可開啟。
	FaultStall bool
}

// DLQName 是 dead letter queue 的名稱。
func (c Config) DLQName() string {
	return c.QueueName + ".dlq"
}

// Load 讀取環境變數；必要欄位缺漏或格式錯誤時回傳錯誤，由 main 決定如何結束。
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		AMQPURL:     os.Getenv("AMQP_URL"),
		QueueName:   getenv("QUEUE_NAME", "readings"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.AMQPURL == "" {
		return Config{}, errors.New("AMQP_URL is required")
	}

	var err error
	if cfg.RequestTimeout, err = getDuration("REQUEST_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.PublishConfirmTimeout, err = getDuration("PUBLISH_CONFIRM_TIMEOUT", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.DeviceRefreshInterval, err = getDuration("DEVICE_REFRESH_INTERVAL", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Prefetch, err = getPositiveInt("PREFETCH", 50); err != nil {
		return Config{}, err
	}
	if cfg.Workers, err = getPositiveInt("WORKERS", 4); err != nil {
		return Config{}, err
	}
	if cfg.RetryBase, err = getDuration("RETRY_BASE", 100*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.RetryMax, err = getDuration("RETRY_MAX", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxAttempts, err = getPositiveInt("RETRY_MAX_ATTEMPTS", 5); err != nil {
		return Config{}, err
	}
	if cfg.WebhookMinInterval, err = getDuration("WEBHOOK_MIN_INTERVAL", time.Minute); err != nil {
		return Config{}, err
	}
	cfg.WebhookURL = os.Getenv("WEBHOOK_URL")

	if cfg.WatchdogInterval, err = getDuration("WATCHDOG_INTERVAL", time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.WatchdogStaleAfter, err = getDuration("WATCHDOG_STALE_AFTER", 2*time.Minute); err != nil {
		return Config{}, err
	}
	cfg.RabbitMQMgmtURL = os.Getenv("RABBITMQ_MGMT_URL")

	if v := os.Getenv("FAULT_STALL"); v != "" {
		if cfg.FaultStall, err = strconv.ParseBool(v); err != nil {
			return Config{}, fmt.Errorf("parse FAULT_STALL: %w", err)
		}
	}

	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", key, v)
	}
	return d, nil
}

func getPositiveInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s must be >= 1, got %d", key, n)
	}
	return n, nil
}
