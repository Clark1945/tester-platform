// verify 對帳工具：讀 simulator 的報告，比對資料庫、queue 與計數器。
// 任一項檢查失敗時 exit code 為 1，並印出差異。
//
//	verify -report report.json            # chaos 模式：計數器恆等式僅供參考
//	verify -report report.json -strict    # 穩態模式：恆等式不成立也算失敗
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tester-receiver/internal/mq"
	"tester-receiver/internal/simulator"
	"tester-receiver/internal/store"
	"tester-receiver/internal/verify"
)

func main() {
	ok, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(2) // 2：工具本身無法執行；1：檢查沒通過
	}
	if !ok {
		os.Exit(1)
	}
}

func run() (bool, error) {
	var (
		reportPath  = flag.String("report", "", "simulator report (JSON)")
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL")
		mgmtURL     = flag.String("mgmt-url", os.Getenv("RABBITMQ_MGMT_URL"), "RabbitMQ management URL")
		queue       = flag.String("queue", "readings", "queue name")
		apiURL      = flag.String("api-metrics", os.Getenv("API_METRICS_URL"), "api /metrics.json URL (optional)")
		receiverURL = flag.String("receiver-metrics", os.Getenv("RECEIVER_METRICS_URL"), "receiver /metrics.json URL (optional)")
		strict      = flag.Bool("strict", false, "fail when the counter identity does not hold")
		drain       = flag.Duration("drain-timeout", 3*time.Minute, "how long to wait for the queue to drain")
	)
	flag.Parse()

	if *reportPath == "" || *databaseURL == "" || *mgmtURL == "" {
		return false, fmt.Errorf("-report, -database-url (or DATABASE_URL) and -mgmt-url (or RABBITMQ_MGMT_URL) are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	report, err := simulator.ReadReport(*reportPath)
	if err != nil {
		return false, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := store.New(connectCtx, *databaseURL)
	if err != nil {
		return false, err
	}
	defer st.Close()

	mgmt, err := mq.NewManagement(*mgmtURL)
	if err != nil {
		return false, err
	}

	return verify.Run(ctx, verify.Options{
		Report:             report,
		DB:                 st,
		Queues:             mgmt,
		Queue:              *queue,
		DLQ:                *queue + ".dlq",
		APIMetricsURL:      *apiURL,
		ReceiverMetricsURL: *receiverURL,
		StrictCounters:     *strict,
		DrainTimeout:       *drain,
		Out:                os.Stdout,
	}), nil
}
