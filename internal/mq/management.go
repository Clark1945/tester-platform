package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Management 是 RabbitMQ Management HTTP API 的 client（只用到 queue 深度與清空）。
//
// 注意：管理 API 的統計數字約每 5 秒更新一次，不是即時值。
// 要判斷「queue 已排空」必須連續觀察幾次，不能只看一次。
type Management struct {
	base     string // 不含帳密與結尾斜線，例如 http://rabbitmq:15672
	username string
	password string
	client   *http.Client
}

// NewManagement 解析形如 http://user:pass@host:15672 的 URL。
func NewManagement(rawURL string) (*Management, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse management url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("management url %q must include scheme and host", rawURL)
	}

	m := &Management{client: &http.Client{Timeout: 5 * time.Second}}
	if u.User != nil {
		m.username = u.User.Username()
		m.password, _ = u.User.Password()
		u.User = nil // 帳密改用 Basic Auth header，不留在 URL 裡（避免出現在 log）
	}
	m.base = strings.TrimRight(u.String(), "/")
	return m, nil
}

// QueueDepth 是 queue 中的訊息數。
type QueueDepth struct {
	Ready   int64 // 等待被消費
	Unacked int64 // 已送給 consumer 但尚未 ack
}

// Total 是 queue 內尚未處理完的訊息總數。
func (d QueueDepth) Total() int64 {
	return d.Ready + d.Unacked
}

// QueueDepth 查詢 default vhost（"/"）下某個 queue 的深度。
func (m *Management) QueueDepth(ctx context.Context, queue string) (QueueDepth, error) {
	var body struct {
		Ready   int64 `json:"messages_ready"`
		Unacked int64 `json:"messages_unacknowledged"`
	}
	if err := m.do(ctx, http.MethodGet, m.queueURL(queue), &body); err != nil {
		return QueueDepth{}, fmt.Errorf("queue depth of %q: %w", queue, err)
	}
	return QueueDepth{Ready: body.Ready, Unacked: body.Unacked}, nil
}

// PurgeQueue 清空 queue 內所有 ready 的訊息（測試工具在開跑前使用）。
func (m *Management) PurgeQueue(ctx context.Context, queue string) error {
	if err := m.do(ctx, http.MethodDelete, m.queueURL(queue)+"/contents", nil); err != nil {
		return fmt.Errorf("purge queue %q: %w", queue, err)
	}
	return nil
}

// vhost "/" 在路徑中必須寫成 %2F
func (m *Management) queueURL(queue string) string {
	return m.base + "/api/queues/%2F/" + url.PathEscape(queue)
}

func (m *Management) do(ctx context.Context, method, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if m.username != "" {
		req.SetBasicAuth(m.username, m.password)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("call management api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("management api returned %s", resp.Status)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
