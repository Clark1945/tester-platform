// Package devices 提供 device 名單的記憶體快取。
//
// api 用它做前置過濾，這樣資料庫故障時 api 仍能收資料（MQ 存在的主要理由）。
// 快取可能過期，所以它只是「過濾」；寫入時資料庫的 foreign key 才是權威。
package devices

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	refreshTimeout = 5 * time.Second
	confirmTimeout = 2 * time.Second

	// 確認過「不存在」的 device 記住多久。期間內同一個 ID 不再查資料庫，
	// 否則有人拿不存在的 ID 狂打，每一筆都會穿過快取打到資料庫（快取穿透）。
	missTTL = 5 * time.Second
	// 最多記住幾個不存在的 ID，避免被大量亂數 ID 撐爆記憶體
	maxMisses = 10000
)

// Source 是快取的資料來源（由 *store.Store 滿足）。
type Source interface {
	ListDeviceIDs(ctx context.Context) ([]string, error)
	DeviceExists(ctx context.Context, deviceID string) (bool, error)
}

// Cache 可被多個 goroutine 同時使用。
//
// 已知的 device 放在 atomic.Pointer 指向的 map：讀取端（每個 HTTP 請求）完全不用鎖，
// 更新時建好新的 map 再整份換上去。mu 只用來讓「寫入端」彼此排隊，以及保護 misses。
type Cache struct {
	src Source
	log *slog.Logger
	now func() time.Time
	set atomic.Pointer[map[string]struct{}]

	mu     sync.Mutex
	misses map[string]time.Time // device ID → 這筆「不存在」的紀錄何時失效
}

// NewCache 建立空的快取；使用前應先呼叫 Refresh。
func NewCache(src Source, log *slog.Logger) *Cache {
	c := &Cache{src: src, log: log, now: time.Now, misses: map[string]time.Time{}}
	empty := map[string]struct{}{}
	c.set.Store(&empty)
	return c
}

// Exists 回報 device 是否在快取中。只查記憶體，不碰資料庫。
func (c *Cache) Exists(deviceID string) bool {
	_, ok := (*c.set.Load())[deviceID]
	return ok
}

// Len 回傳快取中的 device 數。
func (c *Cache) Len() int {
	return len(*c.set.Load())
}

// Check 回報 device 是否存在，必要時向資料庫確認。
//
//   - 快取裡有：直接回 true，不碰資料庫。資料庫故障時，已知的 device 完全不受影響。
//   - 快取裡沒有：向資料庫確認一次。存在就加進快取；不存在就記住一小段時間。
//   - 確認不了（資料庫故障）：回傳錯誤。呼叫端應該回 503 讓 device 重試，而不是 404。
//
// 為什麼不能在快取沒命中時直接回「不存在」：對 device 而言 404 的意思是「不要重試」。
// 剛註冊的 device 在快取刷新前送來的資料會被永久丟棄，而且沒有任何告警。
func (c *Cache) Check(ctx context.Context, deviceID string) (bool, error) {
	if c.Exists(deviceID) {
		return true, nil
	}

	now := c.now()
	c.mu.Lock()
	until, known := c.misses[deviceID]
	c.mu.Unlock()
	if known && now.Before(until) {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(ctx, confirmTimeout)
	defer cancel()
	exists, err := c.src.DeviceExists(ctx, deviceID)
	if err != nil {
		return false, fmt.Errorf("confirm device %q: %w", deviceID, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !exists {
		if len(c.misses) >= maxMisses {
			clear(c.misses)
		}
		c.misses[deviceID] = now.Add(missTTL)
		return false, nil
	}

	// 新認得的 device：複製一份名單加上它再換上去（這種事很少發生，複製的成本可以忽略）
	current := *c.set.Load()
	next := make(map[string]struct{}, len(current)+1)
	for id := range current {
		next[id] = struct{}{}
	}
	next[deviceID] = struct{}{}
	c.set.Store(&next)
	delete(c.misses, deviceID)
	c.log.Info("device learned on cache miss", "device_id", deviceID)
	return true, nil
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

	c.mu.Lock()
	defer c.mu.Unlock()
	c.set.Store(&next)
	clear(c.misses) // 名單是新的，先前「不存在」的結論不再可信
	return nil
}

// Run 定期刷新，直到 ctx 取消。刷新失敗只記錄警告並沿用舊名單：
// 資料庫故障時 api 應該繼續用最後一份已知的名單收資料。
//
// 定期刷新仍然需要：Check 只會「加入」新的 device，被刪除的 device 要靠刷新才會從快取消失。
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
