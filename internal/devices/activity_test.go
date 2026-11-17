package devices

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestActivity_CountsDevicesSeenWithinWindow(t *testing.T) {
	var a Activity
	t0 := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)

	a.Seen("device-001", t0)
	a.Seen("device-002", t0.Add(20*time.Second))
	a.Seen("device-001", t0.Add(25*time.Second)) // 同一台再送一次，不會被算兩次

	if got := a.ActiveWithin(30*time.Second, t0.Add(30*time.Second)); got != 2 {
		t.Errorf("active = %d, want 2", got)
	}
	// device-002 最後出現在 t0+20s，到 t0+60s 已超過 30 秒
	if got := a.ActiveWithin(30*time.Second, t0.Add(60*time.Second)); got != 0 {
		t.Errorf("active = %d, want 0 after both devices went quiet", got)
	}
	if len(a.lastSeen) != 0 {
		t.Errorf("stale entries were not pruned: %d left", len(a.lastSeen))
	}
}

func TestActivity_NilAndConcurrentUse(t *testing.T) {
	var none *Activity
	none.Seen("device-001", time.Now()) // 不能 panic
	if none.ActiveWithin(time.Minute, time.Now()) != 0 {
		t.Error("nil Activity should report 0")
	}

	var a Activity
	now := time.Now()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				a.Seen(fmt.Sprintf("device-%03d", i), now)
				a.ActiveWithin(time.Minute, now)
			}
		}()
	}
	wg.Wait()
	if got := a.ActiveWithin(time.Minute, now); got != 50 {
		t.Errorf("active = %d, want 50", got)
	}
}
