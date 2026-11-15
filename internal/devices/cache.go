// Package devices 提供 device 名單的記憶體快取。
//
// api 用它做前置過濾，這樣資料庫故障時 api 仍能收資料（MQ 存在的主要理由）。
// 快取可能過期：剛註冊的 device 在下次刷新前會被 api 拒絕（404），
// 剛刪除的 device 會被放行。所以它只是「過濾」，receiver 寫入前的資料庫檢查才是權威。
package devices

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

const refreshTimeout = 5 * time.Second

// Lister 是快取的資料來源（由 *store.Store 滿足）。
type Lister interface {
	ListDeviceIDs(ctx context.Context) ([]string, error)
}

// Cache 可被多個 goroutine 同時讀取。
//
// 用 atomic.Pointer 整份替換，而不是 RWMutex + 就地修改：
// 讀取端（每個 HTTP 請求）完全不用鎖；刷新時建好新的 map 再一次換上去。
type Cache struct {
	src Lister
	log *slog.Logger
	set atomic.Pointer[map[string]struct{}]
}

// NewCache 建立空的快取；使用前應先呼叫 Refresh。
func NewCache(src Lister, log *slog.Logger) *Cache {
	c := &Cache{src: src, log: log}
	empty := map[string]struct{}{}
	c.set.Store(&empty)
	return c
}

// Exists 回報 device 是否在快取中。
func (c *Cache) Exists(deviceID string) bool {
	_, ok := (*c.set.Load())[deviceID]
	return ok
}

// Len 回傳快取中的 device 數。
func (c *Cache) Len() int {
	return len(*c.set.Load())
}

// Refresh 從資料來源重新載入名單。失敗時保留舊的名單。
func (c *Cache) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	ids, err := c.src.ListDeviceIDs(ctx)
	if err != nil {
		return fmt.Errorf("refresh device cache: %w", err)
	}

	next := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		next[id] = struct{}{}
	}
	c.set.Store(&next)
	return nil
}

// Run 定期刷新，直到 ctx 取消。刷新失敗只記錄警告並沿用舊名單：
// 資料庫故障時 api 應該繼續用最後一份已知的名單收資料。
func (c *Cache) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.Warn("device cache refresh failed; keeping stale entries",
					"error", err, "cached", c.Len())
			}
		}
	}
}
