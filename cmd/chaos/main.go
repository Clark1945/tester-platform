// chaos 一鍵執行故障注入測試（SPEC §9.3）：
//
//  1. 清空資料，啟動 simulator
//  2. 流量進行中依序：停 receiver → 等 → 啟動；停 postgres → 等 → 啟動；（選配）重啟 rabbitmq
//  3. simulator 結束後等 queue 排空
//  4. 對帳；任一項失敗 exit code 為 1
//
// 以容器執行（需要掛載 Docker socket 才能停啟其他容器）：
//
//	docker compose --profile tools run --rm chaos
//	docker compose --profile tools run --rm chaos -duration 90s -rabbitmq
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tester-receiver/internal/mq"
	"tester-receiver/internal/retry"
	"tester-receiver/internal/simulator"
	"tester-receiver/internal/store"
	"tester-receiver/internal/verify"
)

func main() {
	ok, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(2) // 2：測試本身無法執行；1：對帳沒通過
	}
	if !ok {
		os.Exit(1)
	}
}

type options struct {
	apiURL, receiverURL  string
	databaseURL, mgmtURL string
	queue                string
	socket, project      string
	devices              int
	duration, outage     time.Duration
	drainTimeout         time.Duration
	restartRabbitMQ      bool
	reportPath           string
}

func parseFlags() options {
	var o options
	flag.StringVar(&o.apiURL, "api-url", envOr("API_URL", "http://api:8080"), "api base URL")
	flag.StringVar(&o.receiverURL, "receiver-url", envOr("RECEIVER_URL", "http://receiver:8080"), "receiver base URL")
	flag.StringVar(&o.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL")
	flag.StringVar(&o.mgmtURL, "mgmt-url", os.Getenv("RABBITMQ_MGMT_URL"), "RabbitMQ management URL")
	flag.StringVar(&o.queue, "queue", "readings", "queue name")
	flag.StringVar(&o.socket, "docker-socket", "/var/run/docker.sock", "Docker Engine socket")
	flag.StringVar(&o.project, "project", envOr("COMPOSE_PROJECT", "tester-receiver"), "docker compose project name")
	flag.IntVar(&o.devices, "devices", 100, "number of simulated devices")
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "how long the simulator generates traffic")
	flag.DurationVar(&o.outage, "outage", 10*time.Second, "how long each service stays down")
	flag.DurationVar(&o.drainTimeout, "drain-timeout", 3*time.Minute, "how long to wait for the queue to drain")
	flag.BoolVar(&o.restartRabbitMQ, "rabbitmq", false, "also restart RabbitMQ during the run")
	flag.StringVar(&o.reportPath, "report", "", "also write the simulator report to this file")
	flag.Parse()
	return o
}

func run() (bool, error) {
	o := parseFlags()
	if o.databaseURL == "" || o.mgmtURL == "" {
		return false, errors.New("DATABASE_URL and RABBITMQ_MGMT_URL are required")
	}
	// 三次故障要排得進流量期間內，否則會變成「流量結束後才停機」，測不到東西
	if minimum := 4*o.outage + 20*time.Second; o.duration < minimum {
		return false, fmt.Errorf("-duration %s is too short for -outage %s; need at least %s", o.duration, o.outage, minimum)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	docker := newDockerClient(o.socket, o.project)
	mgmt, err := mq.NewManagement(o.mgmtURL)
	if err != nil {
		return false, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := store.New(connectCtx, o.databaseURL)
	if err != nil {
		return false, err
	}
	defer st.Close()

	// ---- 1. 前置檢查與清場 ----
	step("preflight: waiting for api and receiver to be healthy")
	for _, u := range []string{o.apiURL, o.receiverURL} {
		if err := waitHealthy(ctx, u+"/healthz", time.Minute); err != nil {
			return false, err
		}
	}
	// 先確認 Docker socket 可用，免得跑到一半才發現停不了容器
	for _, service := range []string{"receiver", "postgres", "rabbitmq"} {
		if _, err := docker.containerID(ctx, service); err != nil {
			return false, fmt.Errorf("docker access check: %w", err)
		}
	}
	// 清場的順序：先清 queue，等 receiver 把手上的訊息處理完，最後才清資料表。
	// purge 只清得掉「等待中」的訊息；receiver 已經取走（unacked）的那些不受影響，
	// 如果先清資料表，它們會在清完之後才寫入，對帳時就多出不屬於這次執行的資料。
	step("preflight: purging queues and waiting for in-flight messages to settle")
	for _, q := range []string{o.queue, o.queue + ".dlq"} {
		if err := mgmt.PurgeQueue(ctx, q); err != nil {
			return false, err
		}
	}
	if err := waitQuiet(ctx, mgmt, o.queue, time.Minute); err != nil {
		return false, err
	}
	step("preflight: truncating readings")
	if err := st.TruncateReadings(ctx); err != nil {
		return false, err
	}

	// ---- 2. 流量 + 故障注入 ----
	step("starting simulator: %d devices for %s", o.devices, o.duration)
	reportCh := make(chan simulator.Report, 1)
	go func() {
		reportCh <- simulator.Run(ctx, simulator.Config{
			URL:            o.apiURL,
			Devices:        o.devices,
			Duration:       o.duration,
			Interval:       time.Second,
			RequestTimeout: 5 * time.Second,
			DrainTimeout:   90 * time.Second,
			Backoff:        retry.Backoff{Base: 200 * time.Millisecond, Max: 5 * time.Second},
			Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
	}()

	// 故障時間點平均分布在流量期間內。每次故障結束後留一段恢復時間再進行下一個。
	gap := (o.duration - 3*o.outage) / 4
	faults := []struct {
		name string
		run  func() error
	}{
		{"receiver down", func() error {
			return outage(ctx, o.outage, "receiver",
				func() error { return docker.stop(ctx, "receiver", 20) },
				func() error { return docker.start(ctx, "receiver") })
		}},
		{"postgres down", func() error {
			return outage(ctx, o.outage, "postgres",
				func() error { return docker.stop(ctx, "postgres", 10) },
				func() error { return docker.start(ctx, "postgres") })
		}},
	}
	if o.restartRabbitMQ {
		faults = append(faults, struct {
			name string
			run  func() error
		}{"rabbitmq restart", func() error {
			step("fault: restarting rabbitmq")
			return docker.restart(ctx, "rabbitmq", 10)
		}})
	}

	for _, f := range faults {
		if err := retry.Sleep(ctx, gap); err != nil {
			return false, err
		}
		if err := f.run(); err != nil {
			// 故障注入失敗時務必嘗試把服務都啟動回來，不要留下停著的容器
			restoreAll(docker)
			return false, fmt.Errorf("%s: %w", f.name, err)
		}
	}

	step("faults done; waiting for the simulator to finish")
	report := <-reportCh
	step("simulator finished: accepted=%d retries=%d rejected=%d unconfirmed=%d",
		report.TotalAccepted, report.TotalRetries, report.TotalRejected, report.TotalUnconfirmed)
	if o.reportPath != "" {
		if err := report.WriteFile(o.reportPath); err != nil {
			return false, err
		}
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// ---- 3、4. 等排空並對帳 ----
	// postgres 剛重啟過，等服務都恢復健康再對帳
	for _, u := range []string{o.apiURL, o.receiverURL} {
		if err := waitHealthy(ctx, u+"/healthz", 2*time.Minute); err != nil {
			return false, err
		}
	}
	step("verifying (waiting up to %s for the queue to drain)", o.drainTimeout)
	fmt.Println()
	ok := verify.Run(ctx, verify.Options{
		Report:             report,
		DB:                 st,
		Queues:             mgmt,
		Queue:              o.queue,
		DLQ:                o.queue + ".dlq",
		APIMetricsURL:      o.apiURL + "/metrics.json",
		ReceiverMetricsURL: o.receiverURL + "/metrics.json",
		StrictCounters:     false, // receiver 被重啟過，計數器已歸零，恆等式僅供參考
		DrainTimeout:       o.drainTimeout,
		Out:                os.Stdout,
	})
	return ok, nil
}

// outage 停掉一個服務、等一段時間、再啟動。
func outage(ctx context.Context, d time.Duration, service string, stop, start func() error) error {
	step("fault: stopping %s for %s", service, d)
	if err := stop(); err != nil {
		return err
	}
	sleepErr := retry.Sleep(ctx, d)

	// 即使被 Ctrl+C 中斷也要把服務啟動回來
	step("fault: starting %s", service)
	if err := start(); err != nil {
		return err
	}
	return sleepErr
}

type depthReader interface {
	QueueDepth(ctx context.Context, queue string) (mq.QueueDepth, error)
}

const (
	quietPolls        = 4 // 管理 API 的數字約 5 秒更新一次，所以要連續觀察幾次
	quietPollInterval = 2 * time.Second
)

// waitQuiet 等到 queue 完全沒有訊息（等待中與處理中都是 0），並且連續 quietPolls 次都是如此。
// 還有別的來源在送資料（例如另一個 simulator 沒關）時它不會安靜下來，此時回報錯誤而不是硬跑。
func waitQuiet(ctx context.Context, queues depthReader, queue string, timeout time.Duration) error {
	return waitQuietEvery(ctx, queues, queue, timeout, quietPollInterval)
}

func waitQuietEvery(ctx context.Context, queues depthReader, queue string, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	var (
		quiet int
		last  mq.QueueDepth
	)
	for {
		depth, err := queues.QueueDepth(ctx, queue)
		if err != nil {
			return fmt.Errorf("preflight: %w", err)
		}
		last = depth
		if depth.Total() == 0 {
			quiet++
		} else {
			quiet = 0
		}
		if quiet >= quietPolls {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("preflight: queue %s is still busy after %s (ready=%d unacked=%d); is something else sending traffic?",
				queue, timeout, last.Ready, last.Unacked)
		}
		if err := retry.Sleep(ctx, interval); err != nil {
			return err
		}
	}
}

func restoreAll(docker *dockerClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, service := range []string{"postgres", "rabbitmq", "receiver"} {
		if err := docker.start(ctx, service); err != nil {
			fmt.Fprintf(os.Stderr, "chaos: could not restart %s: %v\n", service, err)
		}
	}
}

func waitHealthy(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}

	var last string
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = resp.Status
		} else {
			last = err.Error()
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy after %s (last: %s)", url, timeout, last)
		}
		if err := retry.Sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

var started = time.Now()

// step 印出帶有相對時間的進度
func step(format string, args ...any) {
	fmt.Printf("[t+%3ds] %s\n", int(time.Since(started).Seconds()), fmt.Sprintf(format, args...))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
