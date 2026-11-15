package mq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManagement_QueueDepth(t *testing.T) {
	var gotPath, gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotUser, gotPass, _ = r.BasicAuth()
		_, _ = w.Write([]byte(`{"messages_ready":12,"messages_unacknowledged":50,"messages":62}`))
	}))
	defer srv.Close()

	// 把帳密塞進 URL，模擬 RABBITMQ_MGMT_URL 的格式
	m, err := NewManagement(strings.Replace(srv.URL, "http://", "http://tester:secret@", 1) + "/")
	if err != nil {
		t.Fatalf("NewManagement: %v", err)
	}

	depth, err := m.QueueDepth(context.Background(), "readings.dlq")
	if err != nil {
		t.Fatalf("QueueDepth: %v", err)
	}
	if depth.Ready != 12 || depth.Unacked != 50 || depth.Total() != 62 {
		t.Errorf("depth = %+v", depth)
	}
	if gotPath != "/api/queues/%2F/readings.dlq" {
		t.Errorf("path = %s, want the vhost encoded as %%2F", gotPath)
	}
	if gotUser != "tester" || gotPass != "secret" {
		t.Errorf("basic auth = %s:%s", gotUser, gotPass)
	}
	if strings.Contains(m.base, "secret") {
		t.Errorf("credentials leaked into base url: %s", m.base)
	}
}

func TestManagement_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	m, err := NewManagement(srv.URL)
	if err != nil {
		t.Fatalf("NewManagement: %v", err)
	}
	if _, err := m.QueueDepth(context.Background(), "missing"); err == nil {
		t.Error("expected an error for a 404 response")
	}
}

func TestNewManagement_RejectsBadURL(t *testing.T) {
	for _, raw := range []string{"", "rabbitmq:15672", "://x"} {
		if _, err := NewManagement(raw); err == nil {
			t.Errorf("NewManagement(%q) should fail", raw)
		}
	}
}
