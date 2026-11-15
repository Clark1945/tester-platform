package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Class 是錯誤的分類，決定失敗後該怎麼辦。
type Class int

const (
	// Permanent：資料本身有問題，重試一百次結果也一樣（例如違反 constraint）。
	// 不重試，直接進 DLQ。
	Permanent Class = iota

	// Transient：這一次沒成功，但同樣的操作稍後可能成功（deadlock、逾時）。
	// 有次數上限地重試；用完就進 DLQ，避免單筆訊息永遠卡住 worker。
	Transient

	// Unavailable：資料庫整個連不上。問題不在訊息，換哪一筆來都會失敗。
	// 不計入單筆的重試次數，持續退避重試直到恢復——
	// 否則一次十秒的資料庫重啟就會把所有處理中的訊息都送進 DLQ。
	Unavailable
)

func (c Class) String() string {
	switch c {
	case Permanent:
		return "permanent"
	case Transient:
		return "transient"
	case Unavailable:
		return "unavailable"
	}
	return "unknown"
}

// Classify 判斷資料庫操作的錯誤屬於哪一類。
//
// 用 errors.As / errors.Is 沿著 %w 包裝鏈往下找，所以不管錯誤被包了幾層都認得出來。
// 判斷順序有意義：連線錯誤裡面常包著 timeout，要先認出連線錯誤。
func Classify(err error) Class {
	// 伺服器有回應，並回報了 SQLSTATE
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return classifySQLState(pgErr.Code)
	}

	// 連線建立失敗（拒絕連線、DNS 失敗、認證階段斷線…）
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return Unavailable
	}

	// 操作逾時。無法單從錯誤分辨是「這筆查詢太慢」還是「資料庫沒回應」，
	// 先當成單筆的暫時錯誤；呼叫端在重試耗盡時會再用 Ping 確認資料庫是否還活著。
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return Transient
	}

	// 既有連線在使用途中斷掉
	var netErr net.Error
	if errors.As(err, &netErr) {
		return Unavailable
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable
	}

	// 不認得的錯誤：保守地當成暫時性——有上限地重試，仍失敗就進 DLQ 讓人來看
	return Transient
}

// classifySQLState 依 PostgreSQL 錯誤碼分類。前兩碼是錯誤類別（class）。
// https://www.postgresql.org/docs/current/errcodes-appendix.html
func classifySQLState(code string) Class {
	switch code {
	case "57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03": // cannot_connect_now（啟動中 / 復原中）
		return Unavailable
	case "40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"57014": // query_canceled（statement_timeout）
		return Transient
	}

	switch {
	case strings.HasPrefix(code, "08"): // connection exception
		return Unavailable
	case strings.HasPrefix(code, "53"): // insufficient resources（連線數滿、磁碟滿、記憶體不足）
		return Unavailable
	case strings.HasPrefix(code, "22"), // data exception（數值超出範圍、格式錯誤）
		strings.HasPrefix(code, "23"), // integrity constraint violation（FK、NOT NULL、CHECK）
		strings.HasPrefix(code, "42"): // syntax error or access rule violation
		return Permanent
	}
	return Transient
}
