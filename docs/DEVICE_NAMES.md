# Receiver 設備查詢與名稱快照

API 的 device 快取仍做第一層過濾。Receiver 每次嘗試寫入時，`store.InsertReading` 在同一個 transaction 執行：

1. `SELECT COALESCE(NULLIF(name, ''), device_id) FROM devices WHERE device_id = $1 FOR SHARE`。
2. 查無設備回報 `ErrUnknownDevice`，依現有流程進 DLQ；查詢故障依原有錯誤分類重試。
3. 將取得的名稱連同量測欄位寫入 `readings.device_name`。foreign key 保留。
4. commit 成功後，Receiver 才 ACK。

`FOR SHARE` 在 transaction 結束前阻止其他 transaction 修改或刪除該設備列，同時允許其他 Receiver 對相同設備進行查詢。資料庫錯誤、commit 錯誤仍沿用現有重試與冪等處理。

名稱是伺服器端取得的快照，不信任 JSON 中的 `device_name`。名稱不參與冪等內容比對：設備改名後，同一筆 reading 重送仍視為重複，保留原先的名稱；新的 reading 使用新名稱。名稱空字串時以 device ID 代替。

## 既有資料庫升級

新建的 Docker volume 會自動執行 `003_device_names.sql`。既有 volume 需要先執行：

```powershell
Get-Content migrations/003_device_names.sql -Raw | docker compose exec -T postgres psql -U tester -d tester -v ON_ERROR_STOP=1
```

再更新 API、Receiver 與 Dashboard：

```powershell
docker compose up -d --build api receiver dashboard
```

若使用自訂 Compose port override，兩個指令都需帶上同樣的 `-f` 選項。不要刪除 volume。

Migration 新增 `devices.name`，既有設備以 device ID 作為初始名稱；新增 nullable 的 `readings.device_name`。既有 readings 不補寫現在名稱，因為無法得知當時名稱，查詢回應省略此欄位，表格顯示「—」。Migration 可重複執行。

目前沒有設備名稱編輯介面；名稱由管理者更新 `devices.name`。查詢 API 的 `device_name` 為新增選填輸出欄位，設備上傳的必要欄位不變。

每筆資料多了設備查詢與顯式 transaction 的成本，README 歷史壓測數字不能代表這個版本的吞吐量。
