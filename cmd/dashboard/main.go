// dashboard 提供即時監控頁面：靜態檔案 + 轉發到 Prometheus 的查詢代理。
//
// 頁面的資料全部來自 Prometheus。瀏覽器不直接連 Prometheus，而是經由本服務的 /prom/ 轉發：
// 頁面與資料同一個來源（不必處理 CORS），而且只開放「查詢」兩個端點，
// 不會把 Prometheus 的管理 API 一併暴露出去。
package main

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// 頁面檔案在編譯時嵌入執行檔，映像檔裡不需要另外放檔案
//
//go:embed web
var webFiles embed.FS

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dashboard")
	if err := run(logger); err != nil {
		logger.Error("dashboard exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := envOr("HTTP_ADDR", ":8080")
	promURL, err := url.Parse(envOr("PROMETHEUS_URL", "http://prometheus:9090"))
	if err != nil {
		return err
	}

	load := newLoadController(envOr("API_URL", "http://api:8080"))
	defer load.stop()
	handler, err := routesWithLoad(promURL, load)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("dashboard listening", "addr", addr, "prometheus", promURL.String())
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func routes(promURL *url.URL) (http.Handler, error) {
	return routesWithLoad(promURL, newLoadController(envOr("API_URL", "http://api:8080")))
}

func routesWithLoad(promURL *url.URL, load *loadController) (http.Handler, error) {
	static, err := fs.Sub(webFiles, "web")
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(promURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		// Prometheus 連不上時回 502，頁面會顯示「無法取得資料」而不是停在舊數字
		http.Error(w, `{"status":"error","error":"prometheus unreachable"}`, http.StatusBadGateway)
	}

	mux := http.NewServeMux()
	load.register(mux)
	apiURL, err := url.Parse(load.target)
	if err != nil {
		return nil, err
	}
	readingsProxy := httputil.NewSingleHostReverseProxy(apiURL)
	director := readingsProxy.Director
	readingsProxy.Director = func(r *http.Request) {
		director(r)
		r.URL.Path = "/v1/readings"
		r.URL.RawPath = ""
		r.Header.Del("Cookie")
		r.Header.Del("Authorization")
	}
	readingsProxy.Transport = &http.Transport{ResponseHeaderTimeout: 6 * time.Second}
	readingsProxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		loadError(w, http.StatusBadGateway, "readings API unreachable")
	}
	mux.Handle("GET /api/readings", readingsProxy)
	// 只轉發唯讀的查詢端點
	mux.Handle("GET /prom/api/v1/query", http.StripPrefix("/prom", proxy))
	mux.Handle("GET /prom/api/v1/query_range", http.StripPrefix("/prom", proxy))
	mux.Handle("GET /", http.FileServerFS(static))
	return mux, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
