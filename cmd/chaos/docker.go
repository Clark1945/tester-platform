package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// dockerClient 透過 unix socket 直接呼叫 Docker Engine API，只實作停、啟、重啟。
//
// 不用 docker CLI：chaos 工具本身跑在容器裡，映像檔沒有 CLI；
// 不用官方 SDK：只需要三個端點，沒必要拉進一大包相依套件。
type dockerClient struct {
	http    *http.Client
	project string // docker compose 的專案名稱，用來找出對應的容器
}

func newDockerClient(socketPath, project string) *dockerClient {
	transport := &http.Transport{
		// 不管 URL 寫什麼 host，一律連到 unix socket
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		},
	}
	return &dockerClient{
		http:    &http.Client{Transport: transport, Timeout: 90 * time.Second},
		project: project,
	}
}

// containerID 以 compose 的 label 找出某個服務的容器（包含已停止的）。
func (d *dockerClient) containerID(ctx context.Context, service string) (string, error) {
	filters, err := json.Marshal(map[string][]string{
		"label": {
			"com.docker.compose.project=" + d.project,
			"com.docker.compose.service=" + service,
		},
	})
	if err != nil {
		return "", fmt.Errorf("encode filters: %w", err)
	}

	query := url.Values{"all": {"true"}, "filters": {string(filters)}}
	var containers []struct {
		ID string `json:"Id"`
	}
	if err := d.call(ctx, http.MethodGet, "/containers/json?"+query.Encode(), &containers); err != nil {
		return "", fmt.Errorf("find container of service %q: %w", service, err)
	}
	if len(containers) != 1 {
		return "", fmt.Errorf("found %d containers for service %q in project %q, want 1", len(containers), service, d.project)
	}
	return containers[0].ID, nil
}

// stop 送 SIGTERM，graceSeconds 後仍未結束才 SIGKILL（等同 docker stop -t）。
func (d *dockerClient) stop(ctx context.Context, service string, graceSeconds int) error {
	return d.action(ctx, service, fmt.Sprintf("stop?t=%d", graceSeconds))
}

func (d *dockerClient) start(ctx context.Context, service string) error {
	return d.action(ctx, service, "start")
}

func (d *dockerClient) restart(ctx context.Context, service string, graceSeconds int) error {
	return d.action(ctx, service, fmt.Sprintf("restart?t=%d", graceSeconds))
}

func (d *dockerClient) action(ctx context.Context, service, action string) error {
	id, err := d.containerID(ctx, service)
	if err != nil {
		return err
	}
	if err := d.call(ctx, http.MethodPost, "/containers/"+id+"/"+action, nil); err != nil {
		return fmt.Errorf("%s %s: %w", action, service, err)
	}
	return nil
}

func (d *dockerClient) call(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return fmt.Errorf("call docker api: %w", err)
	}
	defer resp.Body.Close()

	// 304：容器已經是目標狀態（例如對已停止的容器再 stop），視為成功
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return fmt.Errorf("docker api returned %s: %s", resp.Status, body)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode docker api response: %w", err)
	}
	return nil
}
