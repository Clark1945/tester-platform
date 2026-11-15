package store

import (
	"context"
	"os"
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

func TestInsertReading_UnknownDeviceFails(t *testing.T) {
	s, ctx := newTestStore(t)
	r := testReading(1)
	r.DeviceID = "no-such-device"

	if _, err := s.InsertReading(ctx, r); err == nil {
		cleanup(t, s, r.UniID)
		t.Fatal("expected foreign key violation, got nil")
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
