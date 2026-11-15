package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassify(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code, Message: "test"} }

	cases := []struct {
		name string
		err  error
		want Class
	}{
		// 永久性：資料本身的問題
		{"foreign key violation", pg("23503"), Permanent},
		{"not null violation", pg("23502"), Permanent},
		{"check violation", pg("23514"), Permanent},
		{"numeric out of range", pg("22003"), Permanent},
		{"undefined table", pg("42P01"), Permanent},

		// 暫時性：單筆操作的問題
		{"deadlock", pg("40P01"), Transient},
		{"serialization failure", pg("40001"), Transient},
		{"statement timeout", pg("57014"), Transient},
		{"lock not available", pg("55P03"), Transient},
		{"unknown sqlstate", pg("XX000"), Transient},
		{"context deadline", context.DeadlineExceeded, Transient},
		{"unrecognised error", errors.New("something odd"), Transient},

		// 資料庫不可用
		{"admin shutdown", pg("57P01"), Unavailable},
		{"cannot connect now", pg("57P03"), Unavailable},
		{"connection failure", pg("08006"), Unavailable},
		{"too many connections", pg("53300"), Unavailable},
		{"connect error", &pgconn.ConnectError{}, Unavailable},
		{"dial refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, Unavailable},
		{"connection dropped", io.ErrUnexpectedEOF, Unavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify = %s, want %s", got, tc.want)
			}

			// 實際的錯誤都被 fmt.Errorf("...: %w") 包過好幾層，分類必須不受影響
			wrapped := fmt.Errorf("insert reading abc: %w", fmt.Errorf("exec: %w", tc.err))
			if got := Classify(wrapped); got != tc.want {
				t.Errorf("Classify(wrapped) = %s, want %s", got, tc.want)
			}
		})
	}
}
