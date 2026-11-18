package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"tester-receiver/internal/model"
)

// 整合測試：需要 docker compose 的 Postgres。沒有設定 DATABASE_URL 時跳過。
func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	s, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	return s, ctx
}

// 測試資料放在遠古的時間區間，不會和壓測或 simulator 的資料混在一起。
func testReading(seq int64) model.Reading {
	return model.Reading{
		UniID:      uuid.New(),
		DeviceID:   "device-001",
		Seq:        seq,
		RecordTime: time.Date(2001, 1, 1, 0, 0, int(seq), 0, time.UTC),
		Freq:       1000,
	}
}

func cleanup(t *testing.T, s *Store, ids ...uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range ids {
			if _, err := s.pool.Exec(ctx, `DELETE FROM readings WHERE uni_id = $1`, id.String()); err != nil {
				t.Errorf("cleanup %s: %v", id, err)
			}
		}
	})
}

func TestLatestReadings(t *testing.T) {
	s, ctx := newTestStore(t)
	stamp := time.Date(2005, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		r := testReading(int64(100 + i))
		r.RecordTime = stamp
		if i == 3 {
			r.RecordTime = stamp.Add(time.Second)
		}
		cleanup(t, s, r.UniID)
		if _, err := s.InsertReading(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	to := stamp.Add(2 * time.Second)
	rows, err := s.QueryReadings(ctx, ReadingQuery{From: &stamp, To: &to, Limit: 4, NewestFirst: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows=%d", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		prev, cur := rows[i-1], rows[i]
		if prev.RecordTime.Before(cur.RecordTime) || (prev.RecordTime.Equal(cur.RecordTime) && prev.UniID.String() < cur.UniID.String()) {
			t.Fatalf("not newest first: %+v", rows)
		}
	}
	limited, err := s.QueryReadings(ctx, ReadingQuery{From: &stamp, To: &to, Limit: 2, NewestFirst: true})
	if err != nil || len(limited) != 2 || limited[0].UniID != rows[0].UniID || limited[1].UniID != rows[1].UniID {
		t.Fatalf("latest limit: %+v %v", limited, err)
	}
}

func TestDeviceExists(t *testing.T) {
	s, ctx := newTestStore(t)

	cases := []struct {
		deviceID string
		want     bool
	}{
		{"device-001", true},
		{"device-100", true},
		{"device-101", false},
		{"", false},
	}
	for _, tc := range cases {
		got, err := s.DeviceExists(ctx, tc.deviceID)
		if err != nil {
			t.Fatalf("DeviceExists(%q): %v", tc.deviceID, err)
		}
		if got != tc.want {
			t.Errorf("DeviceExists(%q) = %v, want %v", tc.deviceID, got, tc.want)
		}
	}
}

func TestInsertReading_Idempotent(t *testing.T) {
	s, ctx := newTestStore(t)
	r := testReading(1)
	cleanup(t, s, r.UniID)

	inserted, err := s.InsertReading(ctx, r)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !inserted {
		t.Fatal("first insert: inserted = false, want true")
	}

	// 重送：相同 uni_id 與 record_time
	inserted, err = s.InsertReading(ctx, r)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if inserted {
		t.Fatal("second insert: inserted = true, want false (duplicate)")
	}

	var count int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM readings WHERE uni_id = $1`, r.UniID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1", count)
	}
}

func TestInsertReading_UnknownDeviceIsReportedAsSuch(t *testing.T) {
	s, ctx := newTestStore(t)
	r := testReading(1)
	r.DeviceID = "no-such-device"

	_, err := s.InsertReading(ctx, r)
	if err == nil {
		cleanup(t, s, r.UniID)
		t.Fatal("expected an error, got nil")
	}
	// receiver 靠這個錯誤判斷「device 不存在」，所以它必須認得出來
	if !errors.Is(err, ErrUnknownDevice) {
		t.Errorf("err = %v, want ErrUnknownDevice", err)
	}
}

func TestInsertReading_SameKeyDifferentContentIsAConflict(t *testing.T) {
	s, ctx := newTestStore(t)
	first := testReading(20)
	cleanup(t, s, first.UniID)
	if _, err := s.InsertReading(ctx, first); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// 每個案例：鍵（uni_id, record_time）相同，但有一個欄位不同
	cases := map[string]func(*model.Reading){
		"different seq":    func(r *model.Reading) { r.Seq = 21 },
		"different freq":   func(r *model.Reading) { r.Freq = 999 },
		"different device": func(r *model.Reading) { r.DeviceID = "device-002" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			other := first
			change(&other)
			inserted, err := s.InsertReading(ctx, other)
			if !errors.Is(err, ErrKeyConflict) {
				t.Fatalf("err = %v, want ErrKeyConflict (inserted = %v)", err, inserted)
			}
		})
	}

	// 內容完全相同才是重送：不報錯，也不新增
	inserted, err := s.InsertReading(ctx, first)
	if err != nil || inserted {
		t.Errorf("true resend: inserted = %v err = %v, want false and nil", inserted, err)
	}

	// 原本那一筆沒有被改動
	var seq int64
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT max(seq), count(*) FROM readings WHERE uni_id = $1`, first.UniID.String()).Scan(&seq, &count); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if seq != 20 || count != 1 {
		t.Errorf("stored seq = %d count = %d, want 20 and 1", seq, count)
	}
}

func TestInsertReading_ConcurrentResendsInsertOnce(t *testing.T) {
	// 同一筆同時送 20 次：只能有一次是新寫入，其餘都是重複，而且不能有錯誤
	s, ctx := newTestStore(t)
	r := testReading(30)
	cleanup(t, s, r.UniID)

	var wg sync.WaitGroup
	var insertedCount, errCount atomic.Int32
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inserted, err := s.InsertReading(ctx, r)
			if err != nil {
				errCount.Add(1)
				t.Errorf("concurrent insert: %v", err)
			} else if inserted {
				insertedCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if insertedCount.Load() != 1 || errCount.Load() != 0 {
		t.Errorf("inserted %d times with %d errors, want exactly 1 and 0", insertedCount.Load(), errCount.Load())
	}
}

func TestQueryReadings(t *testing.T) {
	s, ctx := newTestStore(t)

	r1, r2, r3 := testReading(10), testReading(11), testReading(12)
	cleanup(t, s, r1.UniID, r2.UniID, r3.UniID)
	// 故意不照時間順序寫入，確認結果有排序
	for _, r := range []model.Reading{r3, r1, r2} {
		if _, err := s.InsertReading(ctx, r); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	from, to := r1.RecordTime, r3.RecordTime // to 不含，所以只會拿到 r1、r2
	got, err := s.QueryReadings(ctx, ReadingQuery{DeviceID: "device-001", From: &from, To: &to, Limit: 100})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d readings, want 2: %+v", len(got), got)
	}
	if got[0].UniID != r1.UniID || got[1].UniID != r2.UniID {
		t.Errorf("unexpected order: %s, %s", got[0].UniID, got[1].UniID)
	}
	if got[0].Seq != 10 || got[0].Freq != 1000 || !got[0].RecordTime.Equal(r1.RecordTime) {
		t.Errorf("unexpected reading: %+v", got[0])
	}

	limited, err := s.QueryReadings(ctx, ReadingQuery{DeviceID: "device-001", From: &from, Limit: 1})
	if err != nil {
		t.Fatalf("query with limit: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit 1 returned %d readings", len(limited))
	}
}
