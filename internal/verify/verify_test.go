package verify

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tester-receiver/internal/mq"
	"tester-receiver/internal/simulator"
	"tester-receiver/internal/store"
)

type fakeDB struct {
	missing []store.MissingSeq
	maxSeq  map[string]int64
	stats   store.ReadingStats
}

func (f *fakeDB) MissingSeqs(_ context.Context, limit int) (int64, []store.MissingSeq, error) {
	return int64(len(f.missing)), f.missing[:min(limit, len(f.missing))], nil
}
func (f *fakeDB) MaxSeqByDevice(context.Context) (map[string]int64, error) { return f.maxSeq, nil }
func (f *fakeDB) Stats(context.Context) (store.ReadingStats, error)        { return f.stats, nil }

type fakeQueues map[string]mq.QueueDepth

func (f fakeQueues) QueueDepth(_ context.Context, queue string) (mq.QueueDepth, error) {
	return f[queue], nil
}

// scenario 是一次「完全正確」的執行；各測試再弄壞其中一處
type scenario struct {
	opts Options
	db   *fakeDB
	q    fakeQueues
	out  *bytes.Buffer
}

func healthy() *scenario {
	s := &scenario{
		db: &fakeDB{
			maxSeq: map[string]int64{"device-001": 60, "device-002": 58},
			stats:  store.ReadingStats{Count: 118},
		},
		q:   fakeQueues{},
		out: &bytes.Buffer{},
	}
	s.opts = Options{
		Report: simulator.Report{
			Devices: map[string]simulator.DeviceReport{
				"device-001": {LastSeq: 60, Accepted: 60},
				"device-002": {LastSeq: 58, Accepted: 58},
			},
			TotalAccepted: 118,
		},
		DB:           s.db,
		Queues:       s.q,
		Queue:        "readings",
		DLQ:          "readings.dlq",
		DrainTimeout: 200 * time.Millisecond,
		PollInterval: time.Millisecond,
		StablePolls:  2,
		Out:          s.out,
	}
	return s
}

func (s *scenario) run(t *testing.T) bool {
	t.Helper()
	ok := Run(context.Background(), s.opts)
	t.Log("\n" + s.out.String())
	return ok
}

func (s *scenario) assertFails(t *testing.T, check string) {
	t.Helper()
	if s.run(t) {
		t.Fatal("verification passed, want failure")
	}
	for _, line := range strings.Split(s.out.String(), "\n") {
		if strings.HasPrefix(line, "[FAIL]") && strings.Contains(line, check) {
			return
		}
	}
	t.Errorf("no [FAIL] line for %q", check)
}

func TestRun_HealthyRunPasses(t *testing.T) {
	s := healthy()
	if !s.run(t) {
		t.Error("verification failed, want pass")
	}
	if strings.Contains(s.out.String(), "[FAIL]") || !strings.Contains(s.out.String(), "RESULT: PASSED") {
		t.Errorf("unexpected output:\n%s", s.out)
	}
}

func TestRun_DetectsEachKindOfProblem(t *testing.T) {
	cases := []struct {
		name  string
		check string
		break_ func(*scenario)
	}{
		{"gap in the middle", "sequence gaps", func(s *scenario) {
			s.db.missing = []store.MissingSeq{{DeviceID: "device-001", Seq: 17}}
		}},
		{"tail loss (no gap)", "last seq per device", func(s *scenario) {
			s.db.maxSeq["device-002"] = 55 // 最後 3 筆掉了，缺口查詢看不出來
		}},
		{"device missing entirely", "last seq per device", func(s *scenario) {
			delete(s.db.maxSeq, "device-002")
		}},
		{"unexpected device in db", "last seq per device", func(s *scenario) {
			s.db.maxSeq["device-099"] = 5
		}},
		{"fewer rows than accepted", "row count", func(s *scenario) { s.db.stats.Count = 117 }},
		{"more rows than accepted", "row count", func(s *scenario) { s.db.stats.Count = 119 }},
		{"duplicate (device_id, seq)", "duplicates", func(s *scenario) { s.db.stats.DuplicateDeviceSeqs = 1 }},
		{"messages left in queue", "queue drained", func(s *scenario) {
			s.q["readings"] = mq.QueueDepth{Unacked: 3}
		}},
		{"dead letters", "queue drained", func(s *scenario) {
			s.q["readings.dlq"] = mq.QueueDepth{Ready: 1}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := healthy()
			tc.break_(s)
			s.assertFails(t, tc.check)
		})
	}
}

func TestRun_UnconfirmedReadingMayOrMayNotExist(t *testing.T) {
	// device-002 放棄了第 59 筆（未獲確認）。它可能其實已經寫入，兩種情況都算正確。
	for _, landed := range []bool{false, true} {
		s := healthy()
		d := s.opts.Report.Devices["device-002"]
		d.Unconfirmed = 1
		s.opts.Report.Devices["device-002"] = d
		s.opts.Report.TotalUnconfirmed = 1
		if landed {
			s.db.maxSeq["device-002"] = 59
			s.db.stats.Count = 119
		}
		if !s.run(t) {
			t.Errorf("landed=%v: verification failed, want pass", landed)
		}
	}
}

func metricsServer(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestRun_CounterIdentity(t *testing.T) {
	balanced := `{"written":116,"duplicate":1,"dlq":1,"retried":4,"requeued":0,"last_written_at":"2026-11-15T12:00:00Z"}`
	afterRestart := `{"written":40,"duplicate":0,"dlq":0,"retried":0,"requeued":0,"last_written_at":null}`

	cases := []struct {
		name     string
		receiver string
		strict   bool
		wantOK   bool
		wantLine string
	}{
		{"balanced, strict", balanced, true, true, "[PASS] counter identity"},
		{"unbalanced, strict", afterRestart, true, false, "[FAIL] counter identity"},
		{"unbalanced, chaos mode", afterRestart, false, true, "[INFO] counter identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := healthy()
			s.opts.APIMetricsURL = metricsServer(t, `{"accepted":118,"rejected_4xx":0,"publish_failed":0}`)
			s.opts.ReceiverMetricsURL = metricsServer(t, tc.receiver)
			s.opts.StrictCounters = tc.strict

			if ok := s.run(t); ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !strings.Contains(s.out.String(), tc.wantLine) {
				t.Errorf("output lacks %q", tc.wantLine)
			}
		})
	}
}
