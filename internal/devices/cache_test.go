package devices

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeSource 是假的資料庫：可設定名單、可設定故障，並記錄被查了幾次
type fakeSource struct {
	mu      sync.Mutex
	ids     []string
	err     error
	lookups int // DeviceExists 被呼叫的次數
}

func (f *fakeSource) set(ids []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids, f.err = ids, err
}

func (f *fakeSource) lookupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookups
}

func (f *fakeSource) ListDeviceIDs(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ids, f.err
}

func (f *fakeSource) DeviceExists(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if f.err != nil {
		return false, f.err
	}
	for _, known := range f.ids {
		if known == id {
			return true, nil
		}
	}
	return false, nil
}

func newCache(src Source) *Cache {
	return NewCache(src, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func loaded(t *testing.T, src *fakeSource) *Cache {
	t.Helper()
	c := newCache(src)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return c
}

func TestCache_EmptyBeforeRefresh(t *testing.T) {
	c := newCache(&fakeSource{ids: []string{"device-001"}})
	if c.Exists("device-001") {
		t.Error("cache should be empty before the first Refresh")
	}
}

func TestCache_RefreshReplacesEntries(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001", "device-002"}}
	c := loaded(t, src)
	if !c.Exists("device-001") || !c.Exists("device-002") || c.Exists("device-003") {
		t.Errorf("unexpected cache content after first refresh (len=%d)", c.Len())
	}

	// device-001 被移除、device-003 新增
	src.set([]string{"device-002", "device-003"}, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if c.Exists("device-001") || !c.Exists("device-003") || c.Len() != 2 {
		t.Errorf("cache was not replaced (len=%d)", c.Len())
	}
}

func TestCache_KeepsStaleEntriesWhenRefreshFails(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)

	src.set(nil, errors.New("db down"))
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("expected refresh error")
	}
	if !c.Exists("device-001") {
		t.Error("stale entry was dropped after a failed refresh")
	}
}

func TestCheck_KnownDeviceNeverTouchesTheDatabase(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)
	src.set([]string{"device-001"}, errors.New("db down")) // 資料庫掛了

	for range 100 {
		ok, err := c.Check(context.Background(), "device-001")
		if err != nil || !ok {
			t.Fatalf("Check = %v, %v; a cached device must be accepted even when the database is down", ok, err)
		}
	}
	if n := src.lookupCount(); n != 0 {
		t.Errorf("database was queried %d times for a cached device", n)
	}
}

func TestCheck_NewlyRegisteredDeviceIsAcceptedBeforeTheNextRefresh(t *testing.T) {
	// 修正前：快取沒命中就回「不存在」→ api 回 404 → device 不重試 → 資料永久遺失
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)

	src.set([]string{"device-001", "device-101"}, nil) // 剛註冊，快取還沒刷新
	ok, err := c.Check(context.Background(), "device-101")
	if err != nil || !ok {
		t.Fatalf("Check = %v, %v; want the device to be confirmed against the database", ok, err)
	}

	// 之後就在快取裡了，不必再查
	before := src.lookupCount()
	for range 10 {
		if ok, _ := c.Check(context.Background(), "device-101"); !ok {
			t.Fatal("learned device was forgotten")
		}
	}
	if src.lookupCount() != before || !c.Exists("device-101") || c.Len() != 2 {
		t.Errorf("learned device is not cached: lookups %d -> %d, len = %d", before, src.lookupCount(), c.Len())
	}
}

func TestCheck_UnknownDeviceIsRememberedBriefly(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)
	clock := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return clock }

	// 有人拿不存在的 ID 連續打 1000 次：只查資料庫一次
	for range 1000 {
		ok, err := c.Check(context.Background(), "device-999")
		if err != nil || ok {
			t.Fatalf("Check = %v, %v; want false, nil", ok, err)
		}
	}
	if n := src.lookupCount(); n != 1 {
		t.Errorf("database was queried %d times, want 1 (cache penetration)", n)
	}

	// 過了記憶的期限會再確認一次——所以之後註冊的 device 最慢幾秒內就會被認得
	clock = clock.Add(missTTL + time.Second)
	src.set([]string{"device-001", "device-999"}, nil)
	if ok, err := c.Check(context.Background(), "device-999"); err != nil || !ok {
		t.Errorf("Check after the miss expired = %v, %v; want true", ok, err)
	}
}

func TestCheck_DatabaseDownIsAnErrorNotANo(t *testing.T) {
	// 確認不了的時候不能回「不存在」：那會讓 api 回 404，device 就不重試了
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)
	src.set(nil, errors.New("db down"))

	ok, err := c.Check(context.Background(), "device-101")
	if err == nil {
		t.Fatalf("Check = %v, nil; want an error when the device cannot be confirmed", ok)
	}

	// 而且不能把這次失敗記成「不存在」
	src.set([]string{"device-101"}, nil)
	if ok, err := c.Check(context.Background(), "device-101"); err != nil || !ok {
		t.Errorf("Check after recovery = %v, %v; a failed lookup must not be remembered as a miss", ok, err)
	}
}

func TestCheck_RefreshForgetsMisses(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001"}}
	c := loaded(t, src)
	if ok, _ := c.Check(context.Background(), "device-050"); ok {
		t.Fatal("device-050 should not exist yet")
	}

	src.set([]string{"device-001", "device-050"}, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if ok, _ := c.Check(context.Background(), "device-050"); !ok {
		t.Error("a remembered miss survived a refresh that contains the device")
	}
}

func TestCheck_MissMemoryIsBounded(t *testing.T) {
	c := loaded(t, &fakeSource{ids: []string{"device-001"}})
	for i := range maxMisses + 500 {
		if _, err := c.Check(context.Background(), fmt.Sprintf("junk-%d", i)); err != nil {
			t.Fatalf("check: %v", err)
		}
	}
	c.mu.Lock()
	n := len(c.misses)
	c.mu.Unlock()
	if n > maxMisses {
		t.Errorf("remembered %d misses, want at most %d", n, maxMisses)
	}
}

// 以 go test -race 執行時可驗證讀、確認、刷新並行沒有 data race
func TestCache_ConcurrentUse(t *testing.T) {
	src := &fakeSource{ids: []string{"device-001", "device-002"}}
	c := loaded(t, src)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				c.Exists("device-001")
				if _, err := c.Check(context.Background(), fmt.Sprintf("device-%03d", (g*i)%5)); err != nil {
					t.Errorf("check: %v", err)
				}
			}
		}()
	}
	for range 100 {
		if err := c.Refresh(context.Background()); err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	wg.Wait()
	if !c.Exists("device-001") || !c.Exists("device-002") {
		t.Error("known devices were lost during concurrent use")
	}
}
