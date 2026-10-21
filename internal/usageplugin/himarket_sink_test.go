package usageplugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestHiMarketSinkBuildAndDeliver(t *testing.T) {
	var received []HiMarketUsageEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-HiMarket-Internal-Secret") != "hm-secret-123" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var ev HiMarketUsageEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received = append(received, ev)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	ctx := internallogging.WithClientRequestMetadata(context.Background(), internallogging.ClientRequestMetadata{
		SessionID:        "sess_voice_99",
		TenantID:         "dev_tenant_01",
		BusinessAPIKeyID: "ak_live_777",
	})

	rec := usage.Record{
		RequestID:     "req-uuid-001",
		Model:         "deepseek-v4.1-flash:1017-cloud",
		RequestedAt:   time.Now(),
		TTFT:          120 * time.Millisecond,
		Latency:       450 * time.Millisecond,
		Generate:      usage.GenerateFlag(true),
		Detail: usage.Detail{
			InputTokens:     128,
			OutputTokens:    64,
			ReasoningTokens: 32,
			CachedTokens:    48,
		},
	}

	ev, ok := BuildHiMarketEvent(ctx, rec)
	if !ok {
		t.Fatal("expected BuildHiMarketEvent to succeed")
	}
	if ev.TenantID != "dev_tenant_01" || ev.APIKeyID != "ak_live_777" || ev.SessionID != "sess_voice_99" {
		t.Fatalf("unexpected identity fields: %+v", ev)
	}
	if *ev.Metrics.PromptTokens != 128 || *ev.Metrics.CompletionTokens != 96 || *ev.Metrics.CachedTokens != 48 {
		t.Fatalf("unexpected token metrics: %+v", ev.Metrics)
	}

	sink := NewHiMarketSink(srv.URL, "hm-secret-123")
	if err := sink.DeliverOne(ctx, ev); err != nil {
		t.Fatalf("DeliverOne failed: %v", err)
	}
	if len(received) != 1 || received[0].IdempotencyKey != "cli_proxy:req-uuid-001" {
		t.Fatalf("unexpected received events: %+v", received)
	}
}
