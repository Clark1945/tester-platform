package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRoutes(t *testing.T) {
	// 假的 Prometheus：記下收到的路徑
	var gotPath, gotQuery string
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	defer prom.Close()

	promURL, _ := url.Parse(prom.URL)
	h, err := routes(promURL)
	if err != nil {
		t.Fatalf("routes: %v", err)
	}

	get := func(method, target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
		return rec
	}

	// 頁面本身
	if rec := get("GET", "/"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("GET /: status = %d, want the embedded page", rec.Code)
	}

	// 頁面引用的判讀邏輯也要一併嵌入並提供
	if rec := get("GET", "/logic.js"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "function assess") {
		t.Errorf("GET /logic.js: status = %d, want the embedded script", rec.Code)
	}

	// 查詢會被轉發，且 /prom 前綴被拿掉
	if rec := get("GET", "/prom/api/v1/query?query=up"); rec.Code != 200 {
		t.Errorf("query: status = %d", rec.Code)
	}
	if gotPath != "/api/v1/query" || gotQuery != "up" {
		t.Errorf("proxied path = %q query = %q", gotPath, gotQuery)
	}

	// 查詢以外的端點不開放
	for _, target := range []string{"/prom/api/v1/admin/tsdb/delete_series", "/prom/api/v1/targets", "/prom/-/quit"} {
		gotPath = ""
		rec := get("GET", target)
		if gotPath != "" {
			t.Errorf("%s was forwarded to prometheus", target)
		}
		if rec.Code == 200 && strings.Contains(rec.Body.String(), "success") {
			t.Errorf("%s returned a prometheus response", target)
		}
	}
	if rec := get("POST", "/prom/api/v1/query?query=up"); rec.Code == 200 {
		t.Errorf("POST query should not be allowed, got %d", rec.Code)
	}
}

func TestRoutes_PrometheusDown(t *testing.T) {
	promURL, _ := url.Parse("http://127.0.0.1:1") // 沒有服務在聽
	h, err := routes(promURL)
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/prom/api/v1/query?query=up", nil))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 when prometheus is unreachable", rec.Code)
	}
}
