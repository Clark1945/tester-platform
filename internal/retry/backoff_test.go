package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_Ceiling(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond}, // 小於 1 視為 1
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{7, 6400 * time.Millisecond},
		{8, 10 * time.Second}, // 12.8s 被上限截斷
		{500, 10 * time.Second},
	}
	for _, tc := range cases {
		if got := b.ceiling(tc.attempt); got != tc.want {
			t.Errorf("ceiling(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestBackoff_DelayWithinJitterRange(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second}

	for attempt := 1; attempt <= 12; attempt++ {
		ceiling := b.ceiling(attempt)
		sawDifferent := false
		first := b.Delay(attempt)

		for range 200 {
			d := b.Delay(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("Delay(%d) = %v, want within [%v, %v]", attempt, d, ceiling/2, ceiling)
			}
			if d != first {
				sawDifferent = true
			}
		}
		if !sawDifferent {
			t.Errorf("Delay(%d) returned the same value 200 times; jitter is missing", attempt)
		}
	}
}

func TestSleep_ReturnsEarlyOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := Sleep(ctx, time.Minute)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Sleep blocked for %v after cancel", elapsed)
	}
}

func TestSleep_WaitsFullDuration(t *testing.T) {
	start := time.Now()
	if err := Sleep(context.Background(), 30*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("Sleep returned after %v, want >= 30ms", elapsed)
	}
}
