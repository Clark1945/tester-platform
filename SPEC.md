# Tester Receiver System — 開發規格（給 Claude Code）

> 使用方式：把本檔放在專案根目錄（可改名為 `CLAUDE.md` 或 `SPEC.md`）。請**依「階段」順序開發，一次只做一個階段**，每階段完成後確認驗收條件通過再進入下一階段。不要實作「非目標」章節列出的項目。

## 1. 專案目標

用 Go 實作一個接收 tester（測試機台）資料的後端服務。核心主題是**可靠性**：資料不丟、重送不重複，且「是否有掉資料」可以被驗證。

這是求職用的練習專案（Go 後端），重點是：
- 展示 at-least-once delivery + 冪等寫入
- 展示失敗處理（退避、DLQ、告警）
- 展示可驗證性（序號缺口查詢、chaos test 對帳）
- 實際練到 Go 慣用法（goroutine、channel、context、graceful shutdown、error handling）

## 2. 規模假設

- 100 台 device，每台每秒 1 筆，約 100 msg/s。
- 單機 docker compose 即可。吞吐量不是目標；使用 MQ 的理由是：DB 故障時資料不丟、突發流量緩衝、收資料與寫入可獨立擴展（代價是複雜度）。

## 3. 技術選型

| 項目 | 選擇 | 備註 |
|---|---|---|
| 語言 | Go 1.22+ | 標準庫 `net/http` 的 ServeMux 即可，不強制框架 |
| DB | PostgreSQL + TimescaleDB | 若 Timescale 不用，改一般 table，見 §5 |
| MQ | RabbitMQ | 使用 `github.com/rabbitmq/amqp091-go` |
| DB driver | `pgx/v5`（pgxpool） | |
| Log | 標準庫 `log/slog`（JSON） | |
| 容器 | Docker / docker-compose | |
| 壓測 | k6（純壓力）+ Go 寫的 simulator（重試/序號/對帳） | |

## 4. 架構

```
Device Simulator ──HTTP──> API Service ──publish(confirm)──> RabbitMQ ──consume(manual ack)──> Receiver ──> PostgreSQL/Timescale
                              │                                  │                                  │
                         (device 檢查)                      readings.dlq  <───── 永久錯誤/重試耗盡 ──┘──> Webhook 通知
```

- `cmd/api`：收 HTTP、驗證、檢查 device 是否存在、發佈到 RabbitMQ，**等 publisher confirm 後才回應**。
- `cmd/receiver`：消費 queue、再檢查 device、寫 DB、依結果 ack / 重試 / 進 DLQ。
- `cmd/simulator`：模擬 N 台 device，含重試與序號。
- `cmd/verify`：對帳工具（見 §9），失敗時 exit code 非 0。

## 5. 資料模型

```sql
CREATE TABLE devices (
  device_id   text PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE readings (
  uni_id       uuid        NOT NULL,   -- 即 request_id，重送時必須相同
  device_id    text        NOT NULL REFERENCES devices(device_id),
  seq          bigint      NOT NULL,   -- per-device 遞增序號，從 1 開始，重送沿用
  record_time  timestamptz NOT NULL,   -- device 產生，UTC，重送時不變
  freq         integer     NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (uni_id, record_time)         -- hypertable 的 unique 必須包含分區欄位
);

-- 使用 TimescaleDB 時：
SELECT create_hypertable('readings', 'record_time');
CREATE INDEX ON readings (device_id, seq);
```

注意：
- TimescaleDB hypertable 的 unique constraint **必須包含分區欄位**（`record_time`），所以是 `UNIQUE(uni_id, record_time)`。因為 `record_time` 由 device 產生、重送時不變，冪等仍成立。
- 若不用 Timescale，改為一般 table 並使用 `UNIQUE(uni_id)`。
- 寫入一律 `INSERT ... ON CONFLICT DO NOTHING`，用 `RowsAffected()` 區分「新寫入」與「重複」。
- 啟動時 seed 100 筆 devices（`device-001` … `device-100`）。

## 6. API 規格

### `POST /v1/readings`

Request body：
```json
{
  "uni_id": "uuid",
  "device_id": "device-001",
  "seq": 42,
  "record_time": "2026-11-15T12:00:00Z",
  "freq": 1000
}
```

| 狀況 | HTTP | Device 是否重試 |
|---|---|---|
| 已確認進入 RabbitMQ（publisher confirm 成功） | 202 | 否 |
| payload 格式錯誤 / 欄位缺漏 / record_time 非 UTC 可解析 | 400 | 否 |
| device_id 不存在 | 404（或 422） | 否 |
| Broker 不可用、confirm 逾時或失敗 | 503 | **是**（沿用相同 uni_id / seq / record_time） |

重複的 `uni_id` 在 API 層不特別判斷，一律照常發佈；去重由 DB 的 unique constraint 負責（冪等）。

### `GET /v1/readings?device_id=&from=&to=&limit=`
查詢資料（`limit` 預設 100、上限 1000，依 `record_time` 排序）。

### `GET /healthz`
存活檢查（API 另外檢查 broker 連線、receiver 檢查 DB 連線）。

### `GET /metrics`
以 `expvar` 或純文字輸出計數器（見 §8）。

## 7. 可靠性規則（必須遵守）

### API → RabbitMQ
- Queue `readings`：durable；訊息 `DeliveryMode = Persistent`。
- 啟用 **publisher confirms**；回 202 之前必須收到 ack。confirm 失敗或逾時（例如 2s）→ 回 503。
- amqp channel **不是 goroutine-safe**：使用單一 publisher goroutine（以 channel 接收發佈請求）或 channel pool，不可多個 goroutine 共用同一個 channel 發佈。
- Broker 連線斷線需自動重連（含退避）。

### RabbitMQ → Receiver
- **關閉 auto-ack**，使用手動 ack。
- 設定 `Qos(prefetch=N)`（預設 50，可由環境變數設定）。
- 處理順序：解析 → 檢查 device 存在 → 寫 DB → **成功後才 ack**。
- 結局只能是下列三種之一，且每一種都要累加計數器：
  1. 寫入成功 → ack（`written`）
  2. 重複（`ON CONFLICT` 未新增）→ ack（`duplicate`）
  3. 永久錯誤或重試耗盡 → 發到 `readings.dlq` 後 ack 原訊息，並觸發 webhook（`dlq`）
- 禁止「只 log 不處理」的路徑。Worker goroutine 內必須 `recover()` panic，並視為失敗進入 DLQ 流程。

### 錯誤分類與重試
- **暫時性錯誤**（DB 連線失敗、timeout、deadlock 等）：指數退避 + jitter 重試。預設 base 100ms、倍數 2、上限 10s、最多 5 次，皆可設定；退避等待必須可被 `context` 取消。
- **永久性錯誤**（payload 無法解析、device 不存在、違反其他 constraint）：不重試，直接進 DLQ。
- 重試耗盡 → 進 DLQ 並發 webhook。
- Webhook 需做簡單節流（例如同類告警每分鐘最多一次），避免告警風暴。
- DLQ：`readings.dlq`，durable；訊息保留原 payload 並在 header 帶失敗原因。

### 優雅關閉
- 兩個服務都用 `signal.NotifyContext` 處理 SIGINT / SIGTERM。
- API：停止接受新請求，等 in-flight 完成（`http.Server.Shutdown`）。
- Receiver：停止取新訊息，等處理中的訊息完成並 ack/nack，再關閉連線。

## 8. 可觀測性（最小版）

計數器（`/metrics`）：
- API：`accepted`、`rejected_4xx`、`publish_failed`
- Receiver：`received`、`written`、`duplicate`、`retried`、`dlq`、`last_written_at`

恆等式（驗證時使用）：

```
published(confirmed) = written + duplicate + dlq + queue(ready + unacked)
```

Receiver 內建一個 watchdog goroutine（每分鐘）：以下任一成立就發 webhook —
- `readings.dlq` 深度 > 0
- `messages_ready` 持續上升且 ack rate 為 0（consumer 卡住）
- `last_written_at` 超過閾值（例如 2 分鐘）仍未更新，且 queue 非空

Queue 深度透過 RabbitMQ Management API（`messages_ready`、`messages_unacknowledged`）取得。

## 9. 驗證（如何證明沒有掉資料）

### 9.1 序號缺口查詢
```sql
SELECT d.device_id, s AS missing_seq
FROM (SELECT device_id, max(seq) AS m FROM readings GROUP BY device_id) d,
     generate_series(1, d.m) s
WHERE NOT EXISTS (
  SELECT 1 FROM readings r WHERE r.device_id = d.device_id AND r.seq = s);
```
已知盲點：尾端遺失不會產生缺口。補法：simulator 結束時回報每台 device 最後送出的 seq，`verify` 與 `max(seq)` 比對。

### 9.2 Simulator 行為
- N 台 device（預設 100），每台每秒 1 筆，各自 goroutine。
- 每筆產生 `uni_id`（uuid）、遞增 `seq`、`record_time`（UTC）。
- 收到 5xx / timeout：以指數退避 + jitter 重試，**沿用相同 uni_id / seq / record_time**；收到 4xx：不重試，記錄為錯誤。
- 結束時輸出每台 device 的最後 seq（JSON）。

### 9.3 Chaos test 流程（`make chaos`）
1. 啟動所有服務與 simulator（持續 60–120 秒）。
2. 流量進行中依序：`docker stop` receiver → 等 10s → `start`；`stop` postgres → 等 10s → `start`；（選配）`restart` rabbitmq。
3. simulator 結束後等待 queue 排空：`messages_ready = 0`、`messages_unacknowledged = 0`、DLQ = 0。
4. 執行 `cmd/verify`，必須全部通過：
   - 缺口查詢結果為 0 筆
   - 每台 device 的 `max(seq)` = simulator 回報的最後 seq
   - `count(*)` = 預期總筆數
   - 重複筆數（`count(*) - count(DISTINCT (uni_id, record_time))`）= 0
   - 計數器恆等式成立
5. 任一項失敗 → exit code 非 0 並印出詳細差異。

## 10. 目錄結構（建議）

```
.
├── cmd/
│   ├── api/main.go
│   ├── receiver/main.go
│   ├── simulator/main.go
│   └── verify/main.go
├── internal/
│   ├── config/        # 環境變數載入
│   ├── model/         # Reading 等結構與驗證
│   ├── mq/            # publisher（confirm）、consumer、重連
│   ├── store/         # pgx：insert、query、device 查詢
│   ├── retry/         # backoff + jitter、錯誤分類
│   ├── notify/        # webhook + 節流
│   └── metrics/       # 計數器
├── migrations/        # SQL
├── loadtest/k6.js
├── docker-compose.yml
├── Makefile
├── README.md          # 設計說明與取捨（面試用）
└── SPEC.md            # 本檔
```

## 11. 設定（環境變數）

| 變數 | 預設 | 說明 |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API 監聽位址 |
| `DATABASE_URL` | — | Postgres 連線字串 |
| `AMQP_URL` | — | RabbitMQ 連線字串 |
| `QUEUE_NAME` | `readings` | |
| `PREFETCH` | `50` | consumer prefetch |
| `WORKERS` | `4` | receiver worker 數 |
| `PUBLISH_CONFIRM_TIMEOUT` | `2s` | |
| `RETRY_BASE` / `RETRY_MAX` / `RETRY_MAX_ATTEMPTS` | `100ms` / `10s` / `5` | |
| `WEBHOOK_URL` | — | 空值則只寫 log |
| `RABBITMQ_MGMT_URL` | — | 供 watchdog / verify 查 queue 深度 |

## 12. 開發階段與驗收條件

### 階段 1：直接寫 DB 的 API
- docker-compose 起 Postgres（Timescale）；migration、seed devices。
- API 驗證 payload、檢查 device、直接 `INSERT ... ON CONFLICT DO NOTHING`；實作查詢 API、`/healthz`。
- 驗收：重複送同一筆 `uni_id` 只會有一筆資料；k6 100 VU 跑 1 分鐘無錯誤；有基本單元測試（model 驗證、store）。

### 階段 2：加入 RabbitMQ
- API 改為發佈到 MQ（publisher confirm、單一 publisher goroutine、persistent、durable queue）。
- 新增 receiver：手動 ack、prefetch、寫 DB；Docker compose 加入 RabbitMQ 與 receiver。
- 驗收：正常流量下 DB 筆數 = 送出筆數；停掉 receiver 再啟動，積壓訊息被消化且無遺失、無重複。

### 階段 3：失敗處理與優雅關閉
- 錯誤分類、退避 + jitter、DLQ、webhook（含節流）、`recover()`。
- `signal.NotifyContext` 優雅關閉（API 與 receiver）。
- 驗收：停掉 Postgres 時 receiver 退避重試，不會 hot loop；恢復後資料完整；注入永久錯誤的訊息會進 DLQ 並觸發 webhook；SIGTERM 不會遺失處理中的訊息。

### 階段 4：驗證工具與 chaos test
- 加入 `seq`（若前面階段未含）、`cmd/simulator`、`cmd/verify`、計數器 `/metrics`、watchdog。
- `make chaos` 一鍵跑完 §9.3 流程。
- 驗收：chaos test 全部通過；故意製造 consumer 卡住時 watchdog 會告警。

### 階段 5（選配）
- Device 快取（記憶體 map + 定期從 DB 刷新）；或以 bloom filter 作為前置過濾（注意 false positive，receiver 端的 DB 檢查才是權威）。
- README 補充設計取捨。

## 13. 非目標（請勿實作）

- RabbitMQ 高可用 / 叢集 / quorum queue、Prometheus exporter 與完整監控堆疊
- Device 端的 `unsend_log` 落地檔與午夜整檔上傳 NAS/FTP 的批次匯入路徑（僅在 README 以文字描述設計）
- React dashboard
- 認證授權、多租戶、K8s / Helm 部署

## 14. 程式風格要求

- 錯誤一律回傳並處理，不可吞掉；用 `fmt.Errorf("...: %w", err)` 包裝；用 `errors.Is/As` 做分類。
- 所有 I/O 都要傳 `context.Context`，並有 timeout。
- 不使用全域可變狀態；以結構體 + 建構函式注入依賴。
- 關鍵邏輯（retry、錯誤分類、冪等寫入、驗證）要有單元測試；整合測試可使用 docker compose 或 testcontainers。
- Log 使用 `slog` 的結構化欄位（`device_id`、`uni_id`、`seq`、`attempt`）。
- 每個階段完成時更新 README 的「進度」與「設計取捨」。

## 15. 面試用的設計取捨（寫進 README）

- 為什麼 100 msg/s 還用 MQ？（DB 故障緩衝、突發流量、解耦；代價是複雜度）
- 為什麼是 at-least-once + 冪等，而不是追求 exactly-once？
- 為什麼 202 要等 publisher confirm？ack 的時機為何是寫 DB 之後？
- 為什麼要區分暫時性與永久性錯誤？jitter 為什麼重要？
- 如何證明沒有掉資料？尾端遺失如何處理？
- Consumer 卡住（活著但不消化）如何被發現？
- Hypertable 為什麼 unique constraint 必須含 `record_time`？
- 若規模變成 100 倍，下一步會改什麼？（多 receiver、quorum queue、批次寫入 COPY、分區策略）
