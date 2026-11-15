package model

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const validID = "3f1c2a52-6f0e-4b7e-9d59-0a4a6c1f2b10"

func TestParseReading_Valid(t *testing.T) {
	body := `{"uni_id":"` + validID + `","device_id":"device-001","seq":42,"record_time":"2026-11-15T12:00:00Z","freq":1000}`

	got, err := ParseReading([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.UniID.String() != validID {
		t.Errorf("UniID = %s, want %s", got.UniID, validID)
	}
	if got.DeviceID != "device-001" || got.Seq != 42 || got.Freq != 1000 {
		t.Errorf("unexpected reading: %+v", got)
	}
	want := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	if !got.RecordTime.Equal(want) || got.RecordTime.Location() != time.UTC {
		t.Errorf("RecordTime = %v, want %v in UTC", got.RecordTime, want)
	}
}

func TestParseReading_AcceptsZeroFreqAndExplicitUTCOffset(t *testing.T) {
	body := `{"uni_id":"` + validID + `","device_id":"d","seq":1,"record_time":"2026-11-15T12:00:00.123+00:00","freq":0}`

	got, err := ParseReading([]byte(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Freq != 0 {
		t.Errorf("Freq = %d, want 0", got.Freq)
	}
}

func TestParseReading_Invalid(t *testing.T) {
	// 以合法 payload 為基底，每個案例只弄壞一個地方
	fields := func(override map[string]string) string {
		f := map[string]string{
			"uni_id":      `"` + validID + `"`,
			"device_id":   `"device-001"`,
			"seq":         `1`,
			"record_time": `"2026-11-15T12:00:00Z"`,
			"freq":        `1000`,
		}
		for k, v := range override {
			if v == "" {
				delete(f, k)
			} else {
				f[k] = v
			}
		}
		parts := make([]string, 0, len(f))
		for k, v := range f {
			parts = append(parts, `"`+k+`":`+v)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}

	cases := []struct {
		name string
		body string
	}{
		{"malformed json", `{"uni_id":`},
		{"not an object", `[]`},
		{"missing uni_id", fields(map[string]string{"uni_id": ""})},
		{"uni_id not uuid", fields(map[string]string{"uni_id": `"abc"`})},
		{"missing device_id", fields(map[string]string{"device_id": ""})},
		{"empty device_id", fields(map[string]string{"device_id": `""`})},
		{"device_id too long", fields(map[string]string{"device_id": `"` + strings.Repeat("x", 65) + `"`})},
		{"missing seq", fields(map[string]string{"seq": ""})},
		{"seq zero", fields(map[string]string{"seq": `0`})},
		{"seq negative", fields(map[string]string{"seq": `-5`})},
		{"seq wrong type", fields(map[string]string{"seq": `"1"`})},
		{"missing record_time", fields(map[string]string{"record_time": ""})},
		{"record_time not RFC3339", fields(map[string]string{"record_time": `"2026-11-15 12:00:00"`})},
		{"record_time not UTC", fields(map[string]string{"record_time": `"2026-11-15T20:00:00+08:00"`})},
		{"missing freq", fields(map[string]string{"freq": ""})},
		{"freq overflows int32", fields(map[string]string{"freq": `3000000000`})},
		{"freq null", fields(map[string]string{"freq": `null`})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseReading([]byte(tc.body))
			if err == nil {
				t.Fatalf("expected error for body %s", tc.body)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("error %v does not wrap ErrInvalid", err)
			}
		})
	}
}
