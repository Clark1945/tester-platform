// Package store 封裝 PostgreSQL 存取（pgxpool）。
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// InsertReading 冪等寫入一筆資料。
// inserted 為 true 代表新寫入；false 代表 (uni_id, record_time) 已存在（重送）。
func (s *Store) InsertReading(ctx context.Context, r model.Reading) (inserted bool, err error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO readings (uni_id, device_id, seq, record_time, freq)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (uni_id, record_time) DO NOTHING`,
		r.UniID.String(), r.DeviceID, r.Seq, r.RecordTime, r.Freq,
	)
	if err != nil {
		return false, fmt.Errorf("insert reading %s: %w", r.UniID, err)
	}
	return tag.RowsAffected() == 1, nil
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
