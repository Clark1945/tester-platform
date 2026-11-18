# 網頁壓力測試

啟動或更新 Dashboard：

```powershell
docker compose up -d --build dashboard
```

先確認 API、RabbitMQ 與資料庫已啟動。開啟 http://localhost:8095/loadtest.html，或從流程監控頁面右上角進入。

- 總 RPS：1–10,000，所有設備合計的目標 HTTP 請求速率。
- 設備數：1–100，依序使用 `device-001` 至 `device-N`，需要事先存在於資料庫（預設 seed 為 100 台）。
- 持續時間：1–3,600 秒。停止產生新請求後，最多再等待單次請求逾時 5 秒；可按停止取消處理中的請求。
- 按開始、觀察結果、完成後下載 JSON 報告。關閉瀏覽器不影響測試，重新開啟會取得當前測試。重啟 Dashboard 會清除記憶體中的狀態與報告。

## 流量語意

這是 API 壓測，與既有的可靠性 simulator 分開。每個排程輪流分配設備，每筆使用新的 UUID，每台設備在單場測試中產生遞增 seq。跨測試 seq 重新起算；本工具的結果不能交給 simulator 的 verify 做序號對帳。

每次請求只送一次，不對 503 或網路錯誤重試，因此設定的 RPS 不會被重試放大。最大併發數等於設備數。若 HTTP 回應太慢、沒有閒置 worker，或排程落後，會增加 `skipped`，不補送逾期排程；因此目標 RPS 並非保證實際達到的流量。

`actual_rps = sent / elapsed_seconds`，包含最後等待回應的時間。平均與最大延遲包含所有已完成嘗試（含錯誤與取消）。`completed` 的結果分類總和應等於 accepted + client_errors + server_errors + other_responses + network_errors + canceled；完成後 `sent = completed`。一般自然完成時 `sent + skipped = rps × duration_seconds`。

202 表示 API 已取得 broker confirm，不代表資料庫已寫入。搭配流程監控頁或 RabbitMQ 管理介面觀察積壓與排空。停止時被取消的請求仍可能已進入系統。

## 控制介面

目標固定由 Dashboard 的 `API_URL` 決定，預設 `http://api:8080`；瀏覽器不能指定任意目標。Docker Compose 已設定此變數。從主機直接執行 Dashboard 時可設為 `http://localhost:8090`。

| Method | 路徑 | 行為 |
|---|---|---|
| GET | `/api/loadtest` | 200：最近一場測試狀態；尚未執行為 `idle` |
| POST | `/api/loadtest` | JSON：`{"rps":100,"devices":100,"duration_seconds":30}`；202 開始、400 參數錯誤、409 已有執行中的測試 |
| POST | `/api/loadtest/stop` | 200：取消目前測試；沒有測試時不做事 |

狀態：`idle` → `running` → `draining` → `completed`；停止則 `stopping` → `stopped`。新測試會覆蓋前一場結果，請先下載需要保留的報告。

控制端點拒絕跨站瀏覽器請求（403）。目前沒有使用者登入或授權，任何能連到 Dashboard 的人都能操作，僅適用可信任的測試網路；對外部署需在反向代理加入存取控制。每場測試會新增真實資料，並且產生真實後端負載。

## 驗證

```powershell
.\scripts\go.ps1 test -race ./cmd/dashboard
.\scripts\go.ps1 vet ./...
node --check cmd/dashboard/web/loadtest.js
```

Dashboard 測試透過本機 HTTP 測試伺服器驗證實際請求的 payload、202/404/503 分類、不重試、排程上限、慢回應下略過、取消、網路錯誤、參數範圍、重複開始及跨站拒絕，不代表正式環境吞吐量。
