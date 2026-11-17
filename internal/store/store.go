// Package store 封裝 PostgreSQL 存取（pgxpool）。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"tester-receiver/internal/model"
)

// Store 持有連線池。pgxpool.Pool 本身是 goroutine-safe，可被多個 handler 共用。
type Store struct {
	pool *pgxpool.Pool
}

// New 建立連線池並確認資料庫可連線。
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close 釋放連線池。
func (s *Store) Close() {
	s.pool.Close()
}

// Ping 供 /healthz 使用。
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	return nil
}

// DeviceExists 回報 device 是否已註冊。
func (s *Store) DeviceExists(ctx context.Context, deviceID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM devices WHERE device_id = $1)`, deviceID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check device %q: %w", deviceID, err)
	}
	return exists, nil
}

// ListDeviceIDs 回傳所有已註冊的 device，供 api 的記憶體快取使用。
func (s *Store) ListDeviceIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT device_id FROM devices`)
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate devices: %w", err)
	}
	return ids, nil
}

// ErrUnknownDevice：寫入時資料庫以 foreign key 拒絕，代表 device 不在 devices 表中。
var ErrUnknownDevice = errors.New("unknown device")

// ErrKeyConflict：(uni_id, record_time) 已存在，但內容和這次要寫的不一樣。
// 這不是重送（重送的內容會完全相同），而是兩筆不同的資料用了同一個鍵——
// 通常是 device 端產生 uni_id 的方式有問題。不能當成重複默默略過，否則第二筆就無聲無息地消失了。
var ErrKeyConflict = errors.New("idempotency key reused with different content")

const sqlStateForeignKeyViolation = "23503"

// InsertReading 冪等寫入一筆資料。
//
//   - inserted = true：新寫入。
//   - inserted = false, err = nil：已存在且內容相同，是重送。
//   - ErrKeyConflict：已存在但內容不同。
//   - ErrUnknownDevice：device 不存在。device 的檢查由 foreign key 完成，不另外 SELECT——
//     檢查與寫入是同一個動作，沒有「查的時候還在、寫的時候被刪了」的空窗，也少一次資料庫往返。
func (s *Store) InsertReading(ctx context.Context, r model.Reading) (inserted bool, err error) {
	// 一個敘述完成「寫入」與「若已存在，比對內容」。
	// 第二個子查詢看到的是敘述開始時的資料：新寫入時它查不到這一筆（回傳 NULL），
	// 只有在鍵已存在時才會回傳比對結果。
	var same *bool
	err = s.pool.QueryRow(ctx,
		`WITH ins AS (
		   INSERT INTO readings (uni_id, device_id, seq, record_time, freq)
		   VALUES ($1::uuid, $2::text, $3::bigint, $4::timestamptz, $5::integer)
		   ON CONFLICT (uni_id, record_time) DO NOTHING
		   RETURNING 1
		 )
		 SELECT EXISTS (SELECT 1 FROM ins),
		        (SELECT device_id = $2::text AND seq = $3::bigint AND freq = $5::integer
		           FROM readings
		          WHERE uni_id = $1::uuid AND record_time = $4::timestamptz)`,
		r.UniID.String(), r.DeviceID, r.Seq, r.RecordTime, r.Freq,
	).Scan(&inserted, &same)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlStateForeignKeyViolation {
			// readings 只有 device_id 這一個 foreign key
			return false, fmt.Errorf("insert reading %s: %w: %s", r.UniID, ErrUnknownDevice, r.DeviceID)
		}
		return false, fmt.Errorf("insert reading %s: %w", r.UniID, err)
	}
	if !inserted && same != nil && !*same {
		return false, fmt.Errorf("insert reading %s: %w", r.UniID, ErrKeyConflict)
	}
	return inserted, nil
}

// ReadingQuery 是查詢條件；零值欄位代表不過濾。
type ReadingQuery struct {
	DeviceID string
	From     *time.Time // 含
	To       *time.Time // 不含
	Limit    int
}

// QueryReadings 依 record_time 排序回傳資料。
func (s *Store) QueryReadings(ctx context.Context, q ReadingQuery) ([]model.Reading, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT uni_id::text, device_id, seq, record_time, freq
		 FROM readings
		 WHERE ($1 = '' OR device_id = $1)
		   AND ($2::timestamptz IS NULL OR record_time >= $2)
		   AND ($3::timestamptz IS NULL OR record_time <  $3)
		 ORDER BY record_time, device_id, seq
		 LIMIT $4`,
		q.DeviceID, q.From, q.To, q.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query readings: %w", err)
	}
	defer rows.Close()

	readings := make([]model.Reading, 0, q.Limit)
	for rows.Next() {
		var (
			r  model.Reading
			id string
		)
		if err := rows.Scan(&id, &r.DeviceID, &r.Seq, &r.RecordTime, &r.Freq); err != nil {
			return nil, fmt.Errorf("scan reading: %w", err)
		}
		if r.UniID, err = uuid.Parse(id); err != nil {
			return nil, fmt.Errorf("parse uni_id %q: %w", id, err)
		}
		r.RecordTime = r.RecordTime.UTC()
		readings = append(readings, r)
	}
	// rows.Next() 回傳 false 可能是讀完，也可能是中途出錯，必須再檢查 rows.Err()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate readings: %w", err)
	}
	return readings, nil
}
