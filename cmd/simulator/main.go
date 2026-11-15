// simulator 模擬一群 device 送資料，結束時輸出每台 device 的最後序號（JSON）。
//
//	simulator -url http://localhost:8090 -devices 100 -duration 60s -out report.json
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tester-receiver/internal/retry"
	"tester-receiver/internal/simulator"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "simulator")
	if err := run(logger); err != nil {
		logger.Error("simulator failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var (
		url      = flag.String("url", envOr("API_URL", "http://localhost:8090"), "API base URL")
		devices  = flag.Int("devices", 100, "number of devices")
		duration = flag.Duration("duration", 60*time.Second, "how long to generate readings")
		interval = flag.Duration("interval", time.Second, "interval between readings per device")
		drain    = flag.Duration("drain-timeout", 60*time.Second, "how long to keep retrying in-flight readings after the duration ends")
		out      = flag.String("out", "", "write the report to this file (default: stdout)")
	)
	flag.Parse()

	if *devices < 1 || *duration <= 0 || *interval <= 0 {
		return errors.New("devices, duration and interval must be positive")
	}

	// Ctrl+C 提前結束時仍會輸出報告
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("simulation started", "url", *url, "devices", *devices, "duration", duration.String(), "interval", interval.String())
	report := simulator.Run(ctx, simulator.Config{
		URL:            *url,
		Devices:        *devices,
		Duration:       *duration,
		Interval:       *interval,
		RequestTimeout: 5 * time.Second,
		DrainTimeout:   *drain,
		Backoff:        retry.Backoff{Base: 200 * time.Millisecond, Max: 5 * time.Second},
		Log:            logger,
	})
	logger.Info("simulation finished",
		"accepted", report.TotalAccepted, "retries", report.TotalRetries,
		"rejected", report.TotalRejected, "unconfirmed", report.TotalUnconfirmed)

	if *out == "" {
		*out = "/dev/stdout"
	}
	if err := report.WriteFile(*out); err != nil {
		return err
	}

	// 有被拒絕的資料代表 simulator 或系統設定有問題，以 exit code 反映
	if report.TotalRejected > 0 {
		return fmt.Errorf("%d readings were rejected with 4xx", report.TotalRejected)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
