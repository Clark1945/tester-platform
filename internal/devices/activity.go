package devices

import (
	"sync"
	"time"
)

// Activity 記錄每台 device 最後一次成功送資料的時間，用來回答「現在有幾台在線」。
//
// 「已註冊」（Cache）與「在線」是兩件事：已註冊 100 台不代表 100 台都在送資料。
// 零值可用；nil 也可用（方法會直接返回），測試不必特別建立。
type Activity struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
}

// Seen 記錄某台 device 剛送了一筆被接受的資料。
func (a *Activity) Seen(deviceID string, now time.Time) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastSeen == nil {
		a.lastSeen = map[string]time.Time{}
	}
	a.lastSeen[deviceID] = now
}

// ActiveWithin 回傳在 window 時間內送過資料的 device 數，並順手清掉過期的紀錄，
// 讓 map 的大小不會超過「曾經在線的 device 數」。
func (a *Activity) ActiveWithin(window time.Duration, now time.Time) int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	active := 0
	for id, seen := range a.lastSeen {
		if now.Sub(seen) <= window {
			active++
		} else {
			delete(a.lastSeen, id)
		}
	}
	return active
}
