// Package model 定義 Reading 與其解析、驗證規則。
// API（HTTP body）與 receiver（MQ 訊息）共用同一份解析邏輯。
package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInvalid 代表 payload 不合法（永久性錯誤，重試也不會成功）。
// 呼叫端用 errors.Is(err, model.ErrInvalid) 判斷。
var ErrInvalid = errors.New("invalid reading")

const maxDeviceIDLen = 64

// Reading 是一筆 device 上傳的量測資料。
type Reading struct {
	UniID      uuid.UUID `json:"uni_id"`
	DeviceID   string    `json:"device_id"`
	Seq        int64     `json:"seq"`
	RecordTime time.Time `json:"record_time"`
	Freq       int32     `json:"freq"`
}

// rawReading 用指標欄位區分「欄位沒給」與「給了零值」（例如 freq = 0 是合法值）。
type rawReading struct {
	UniID      *string `json:"uni_id"`
	DeviceID   *string `json:"device_id"`
	Seq        *int64  `json:"seq"`
	RecordTime *string `json:"record_time"`
	Freq       *int32  `json:"freq"`
}

// ParseReading 解析並驗證 JSON。任何失敗都會包裝 ErrInvalid。
func ParseReading(data []byte) (Reading, error) {
	var raw rawReading
	if err := json.Unmarshal(data, &raw); err != nil {
		return Reading{}, invalidf("malformed json: %v", err)
	}

	if raw.UniID == nil {
		return Reading{}, invalidf("uni_id is required")
	}
	id, err := uuid.Parse(*raw.UniID)
	if err != nil {
		return Reading{}, invalidf("uni_id is not a uuid")
	}

	if raw.DeviceID == nil || *raw.DeviceID == "" {
		return Reading{}, invalidf("device_id is required")
	}
	if len(*raw.DeviceID) > maxDeviceIDLen {
		return Reading{}, invalidf("device_id is longer than %d bytes", maxDeviceIDLen)
	}

	if raw.Seq == nil {
		return Reading{}, invalidf("seq is required")
	}
	if *raw.Seq < 1 {
		return Reading{}, invalidf("seq must be >= 1")
	}

	if raw.RecordTime == nil {
		return Reading{}, invalidf("record_time is required")
	}
	recordTime, err := parseUTC(*raw.RecordTime)
	if err != nil {
		return Reading{}, err
	}

	if raw.Freq == nil {
		return Reading{}, invalidf("freq is required")
	}

	return Reading{
		UniID:      id,
		DeviceID:   *raw.DeviceID,
		Seq:        *raw.Seq,
		RecordTime: recordTime,
		Freq:       *raw.Freq,
	}, nil
}

// parseUTC 只接受 UTC 的 RFC3339（"Z" 或 "+00:00"）。
// record_time 是冪等鍵的一部分，強制 UTC 可避免同一時刻因時區表示法不同而混淆。
func parseUTC(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, invalidf("record_time is not RFC3339")
	}
	if _, offset := t.Zone(); offset != 0 {
		return time.Time{}, invalidf("record_time must be UTC")
	}
	return t.UTC(), nil
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}
