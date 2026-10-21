package usageplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

type HiMarketUsageEvent struct {
	EventID        string               `json:"event_id"`
	IdempotencyKey string               `json:"idempotency_key"`
	SessionID      string               `json:"session_id"`
	RoomName       string               `json:"room_name,omitempty"`
	TenantID       string               `json:"tenant_id"`
	APIKeyID       string               `json:"api_key_id"`
	Source         string               `json:"source"`
	Category       string               `json:"category"`
	Model          string               `json:"model"`
	OccurredAtMs   int64                `json:"occurred_at_ms"`
	Metrics        HiMarketUsageMetrics `json:"metrics"`
}

type HiMarketUsageMetrics struct {
	PromptTokens     *int64 `json:"prompt_tokens,omitempty"`
	CompletionTokens *int64 `json:"completion_tokens,omitempty"`
	CachedTokens     *int64 `json:"cached_tokens,omitempty"`
	TtftMs           *int64 `json:"ttft_ms,omitempty"`
	LatencyMs        *int64 `json:"latency_ms,omitempty"`
	CostUsdMicros    int64  `json:"cost_usd_micros"`
}

type HiMarketSink struct {
	endpointURL  string
	sharedSecret string
	client       *http.Client
	queue        chan HiMarketUsageEvent
}

func NewHiMarketSink(endpointURL, sharedSecret string) *HiMarketSink {
	s := &HiMarketSink{
		endpointURL:  strings.TrimSpace(endpointURL),
		sharedSecret: strings.TrimSpace(sharedSecret),
		client:       &http.Client{Timeout: 5 * time.Second},
		queue:        make(chan HiMarketUsageEvent, 4096),
	}
	go s.worker()
	return s
}

func RegisterFromEnv() *HiMarketSink {
	url := strings.TrimSpace(os.Getenv("HIMARKET_METERING_URL"))
	if url == "" {
		return nil
	}
	secret := strings.TrimSpace(os.Getenv("HIMARKET_INTERNAL_SECRET"))
	sink := NewHiMarketSink(url, secret)
	usage.RegisterNamedPlugin("himarket-metering", sink)
	log.WithField("endpoint", url).Info("HiMarket usage metering sink registered")
	return sink
}

func BuildHiMarketEvent(ctx context.Context, record usage.Record) (HiMarketUsageEvent, bool) {
	if record.Failed || !usage.GenerateEnabled(record.Generate) {
		return HiMarketUsageEvent{}, false
	}

	clientMeta := internallogging.GetClientRequestMetadata(ctx)
	tenantID := record.TenantID
	if tenantID == "" {
		tenantID = clientMeta.TenantID
	}
	apiKeyID := record.BusinessAPIKeyID
	if apiKeyID == "" {
		apiKeyID = clientMeta.BusinessAPIKeyID
	}
	sessionID := record.SessionID
	if sessionID == "" {
		sessionID = clientMeta.SessionID
	}

	modelName := strings.TrimSpace(record.ResponseModel)
	if modelName == "" {
		modelName = strings.TrimSpace(record.Model)
	}

	promptTokens := record.Detail.InputTokens
	completionTokens := record.Detail.OutputTokens + record.Detail.ReasoningTokens
	cachedTokens := record.Detail.CachedTokens
	ttftMs := record.TTFT.Milliseconds()
	latencyMs := record.Latency.Milliseconds()

	occurredAt := record.RequestedAt
	if occurredAt.IsZero() {
		occurredAt = time.Now()
	}

	ev := HiMarketUsageEvent{
		EventID:        "evt_cpa_" + record.RequestID,
		IdempotencyKey: fmt.Sprintf("cli_proxy:%s", record.RequestID),
		SessionID:      sessionID,
		TenantID:       tenantID,
		APIKeyID:       apiKeyID,
		Source:         "cli_proxy_api",
		Category:       "text_llm",
		Model:          modelName,
		OccurredAtMs:   occurredAt.UnixMilli(),
		Metrics: HiMarketUsageMetrics{
			PromptTokens:     &promptTokens,
			CompletionTokens: &completionTokens,
			CachedTokens:     &cachedTokens,
			TtftMs:           &ttftMs,
			LatencyMs:        &latencyMs,
			CostUsdMicros:    0,
		},
	}
	return ev, true
}

func (s *HiMarketSink) HandleUsage(ctx context.Context, record usage.Record) {
	if s == nil || s.endpointURL == "" {
		return
	}
	ev, ok := BuildHiMarketEvent(ctx, record)
	if !ok {
		return
	}
	select {
	case s.queue <- ev:
	default:
		log.Warn("HiMarket usage sink queue full, dropping usage event")
	}
}

func (s *HiMarketSink) DeliverOne(ctx context.Context, ev HiMarketUsageEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpointURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.sharedSecret != "" {
		req.Header.Set("X-HiMarket-Internal-Secret", s.sharedSecret)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}

func (s *HiMarketSink) worker() {
	for ev := range s.queue {
		if err := s.DeliverOne(context.Background(), ev); err != nil {
			log.WithError(err).WithField("request_id", ev.IdempotencyKey).Warn("failed to deliver usage event to HiMarket")
		}
	}
}
