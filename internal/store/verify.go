package store

import (
	"context"
	"fmt"
)

// 本檔是對帳（cmd/verify、cmd/chaos）專用的查詢，服務本身不會用到。

// MissingSeq 是一個缺口：某台 device 的序號 1..max(seq) 之間沒出現的號碼。
type MissingSeq struct {
	DeviceID string
	Seq      int64
}

// MissingSeqs 找出序號缺口。回傳缺口總數，以及最多 limit 筆樣本。
//
// 已知盲點：尾端遺失（最後幾筆全掉）不會產生缺口，因為 max(seq) 也跟著變小。
// 那要靠 MaxSeqByDevice 與 simulator 回報的最後序號比對才抓得到。
func (s *Store) MissingSeqs(ctx context.Context, limit int) (total int64, sample []MissingSeq, err error) {
	rows, err := s.pool.Query(ctx,
		`SELECT d.device_id, s AS missing_seq
		 FROM (SELECT device_id, max(seq) AS m FROM readings GROUP BY device_id) d,
		      generate_series(1, d.m) s
		 WHERE NOT EXISTS (
		   SELECT 1 FROM readings r WHERE r.device_id = d.device_id AND r.seq = s)
		 ORDER BY d.device_id, s`)
	if err != nil {
		return 0, nil, fmt.Errorf("query missing seqs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var m MissingSeq
		if err := rows.Scan(&m.DeviceID, &m.Seq); err != nil {
			return 0, nil, fmt.Errorf("scan missing seq: %w", err)
		}
		total++
		if len(sample) < limit {
			sample = append(sample, m)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("iterate missing seqs: %w", err)
	}
	return total, sample, nil
}

// MaxSeqByDevice 回傳每台 device 在資料庫中的最大序號（沒有資料的 device 不會出現）。
func (s *Store) MaxSeqByDevice(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT device_id, max(seq) FROM readings GROUP BY device_id`)
	if err != nil {
		return nil, fmt.Errorf("query max seq: %w", err)
	}
	defer rows.Close()

	result := map[string]int64{}
	for rows.Next() {
		var (
			id  string
			max int64
		)
		if err := rows.Scan(&id, &max); err != nil {
			return nil, fmt.Errorf("scan max seq: %w", err)
		}
		result[id] = max
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate max seq: %w", err)
	}
	return result, nil
}

// ReadingStats 是對帳用的彙總數字。
type ReadingStats struct {
	Count int64
	// DuplicateKeys：(uni_id, record_time) 重複的筆數。unique constraint 保證它為 0，
	// 檢查它等於確認 constraint 還在。
	DuplicateKeys int64
	// DuplicateDeviceSeqs：(device_id, seq) 重複的筆數。沒有 constraint 保護——
	// 若 device 重送時誤換了 uni_id，冪等失效，這裡才抓得到。
	DuplicateDeviceSeqs int64
}

// Stats 回傳 readings 的總筆數與重複筆數。
func (s *Store) Stats(ctx context.Context) (ReadingStats, error) {
	var st ReadingStats
	err := s.pool.QueryRow(ctx,
		`SELECT count(*),
		        count(*) - count(DISTINCT (uni_id, record_time)),
		        count(*) - count(DISTINCT (device_id, seq))
		 FROM readings`,
	).Scan(&st.Count, &st.DuplicateKeys, &st.DuplicateDeviceSeqs)
	if err != nil {
		return ReadingStats{}, fmt.Errorf("query reading stats: %w", err)
	}
	return st, nil
}

// TruncateReadings 清空 readings。只給測試工具在開跑前使用。
func (s *Store) TruncateReadings(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `TRUNCATE readings`); err != nil {
		return fmt.Errorf("truncate readings: %w", err)
	}
	return nil
}
