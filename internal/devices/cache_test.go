package devices

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
)

type fakeLister struct {
	mu  sync.Mutex
	ids []string
	err error
}

func (f *fakeLister) set(ids []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids, f.err = ids, err
}

func (f *fakeLister) ListDeviceIDs(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ids, f.err
}

func newCache(src Lister) *Cache {
	return NewCache(src, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestCache_EmptyBeforeRefresh(t *testing.T) {
	c := newCache(&fakeLister{ids: []string{"device-001"}})
	if c.Exists("device-001") {
		t.Error("cache should be empty before the first Refresh")
	}
}

func TestCache_RefreshReplacesEntries(t *testing.T) {
	src := &fakeLister{ids: []string{"device-001", "device-002"}}
	c := newCache(src)

	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
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
	src := &fakeLister{ids: []string{"device-001"}}
	c := newCache(src)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	src.set(nil, errors.New("db down"))
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("expected refresh error")
	}
	if !c.Exists("device-001") {
		t.Error("stale entry was dropped after a failed refresh")
	}
}

// 以 go test -race 執行時可驗證讀寫並行沒有 data race
func TestCache_ConcurrentReadAndRefresh(t *testing.T) {
	c := newCache(&fakeLister{ids: []string{"device-001"}})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				c.Exists("device-001")
			}
		}()
	}
	for range 100 {
		if err := c.Refresh(context.Background()); err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	wg.Wait()
}
