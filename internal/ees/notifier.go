// Package ees - UPF Event Exposure Service (EES)
// notifier.go: deliver EES Notify payloads to subscriber endpoints.
//
// Behavior (MVP):
// - Sends USER_DATA_USAGE_MEASURES notifications to sub.NotifURI via HTTP POST.
// - Payload contains subscription ID, event ID, granularity, and a list of items,
//   each item includes LocalSEID, RemoteSEID, UL/DL bytes/packets, StartTime/EndTime,
//   and optional derived throughputs.
// - Uses a per-request timeout; no retry (keep it simple for MVP).
// - Logs both success and failure with descriptive fields.

package ees

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Notifier is responsible for delivering EES notifications to subscribers.
type Notifier struct {
	httpClient         *http.Client
	logger             *zap.Logger
	defaultUserAgent   string
	requestTimeout     time.Duration
	maxResponseBodyLen int64
}

// NewNotifier creates a notifier with sane defaults.
// - request timeout: 5s
// - connect + TLS handshake timeouts are governed by the http.Transport below.
func NewNotifier(logger *zap.Logger) *Notifier {
	transport := &http.Transport{
		// Reasonable defaults for a control-plane style HTTP call.
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &Notifier{
		httpClient: &http.Client{
			Transport: transport,
		},
		logger:             logger,
		defaultUserAgent:   "go-upf-ees/notify",
		requestTimeout:     5 * time.Second,
		maxResponseBodyLen: 4 << 10, // 4 KiB cap when logging response bodies
	}
}

// notifyPayload is the JSON structure sent to subscriber endpoints.
// Keep field names explicit for readability.
type notifyPayload struct {
	SubscriptionID      string       `json:"subscriptionId"`
	NotifyCorrelationID string       `json:"notifyCorrelationId"`
	EventID             string       `json:"eventId"`
	Granularity         string       `json:"granularity"`
	Timestamp           time.Time    `json:"timestamp"`
	Items               []notifyItem `json:"items"`
}

// notifyItem represents a single per-session measurement record.
type notifyItem struct {
	LocalSEID       uint64    `json:"localSeid"`
	RemoteSEID      uint64    `json:"remoteSeid"`
	ULBytes         uint64    `json:"ulBytes"`
	DLBytes         uint64    `json:"dlBytes"`
	ULPackets       uint64    `json:"ulPackets"`
	DLPackets       uint64    `json:"dlPackets"`
	StartTime       time.Time `json:"startTime"`
	EndTime         time.Time `json:"endTime"`
	ULThroughputBps float64   `json:"ulThroughputBps,omitempty"`
	DLThroughputBps float64   `json:"dlThroughputBps,omitempty"`
}

// Notify converts a set of UsageMeasures into a single POST toward sub.NotifURI.
// The function assumes sub.Event == USER_DATA_USAGE_MEASURES and sub.Granularity == perPduSession in MVP.
func (notifier *Notifier) Notify(subscription *Subscription, measures []UsageMeasures) error {
	if subscription == nil {
		return fmt.Errorf("notify: subscription is nil")
	}
	if subscription.NotifURI == "" {
		return fmt.Errorf("notify: empty NotifURI for subscriptionId=%s", subscription.ID)
	}

	items := make([]notifyItem, 0, len(measures))
	for _, m := range measures {
		items = append(items, notifyItem{
			LocalSEID:       m.Key.LocalSEID,
			RemoteSEID:      m.Key.RemoteSEID,
			ULBytes:         m.ULBytesDelta,
			DLBytes:         m.DLBytesDelta,
			ULPackets:       m.ULPacketsDelta,
			DLPackets:       m.DLPacketsDelta,
			StartTime:       m.StartTime,
			EndTime:         m.EndTime,
			ULThroughputBps: m.ULThroughputBps,
			DLThroughputBps: m.DLThroughputBps,
		})
	}

	payload := notifyPayload{
		SubscriptionID:      subscription.ID,
		NotifyCorrelationID: subscription.NotifyCorrelationID,
		EventID:             string(subscription.Event),
		Granularity:         string(subscription.Granularity),
		Timestamp:           time.Now(),
		Items:               items,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("notify: marshal payload failed: %w", err)
	}

	// Context with timeout for the request lifecycle.
	ctx, cancel := context.WithTimeout(context.Background(), notifier.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, subscription.NotifURI, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("notify: new request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", notifier.defaultUserAgent)
	req.Header.Set("X-EES-EventId", string(subscription.Event))
	req.Header.Set("X-EES-SubscriptionId", subscription.ID)

	resp, err := notifier.httpClient.Do(req)
	if err != nil {
		notifier.logger.Warn("ees notify failed: http request error",
			zap.String("subscriptionId", subscription.ID),
			zap.String("notifUri", subscription.NotifURI),
			zap.Error(err),
			zap.Int("items", len(items)),
		)
		return fmt.Errorf("notify: http request failed: %w", err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			notifier.logger.Debug("ees notify: close response body failed",
				zap.String("subscriptionId", subscription.ID),
				zap.Error(closeErr),
			)
		}
	}()

	// Accept any 2xx as success.
	if resp.StatusCode/100 != 2 {
		// Best-effort small response read for logging; avoid large buffers.
		var snippet string
		limit := notifier.maxResponseBodyLen
		if limit <= 0 {
			limit = 4096
		}
		// Regardless of the value of ContentLength, always use limited read and check for read errors.
		bodyLimited := io.LimitReader(resp.Body, limit)
		bodyBytes, readErr := io.ReadAll(bodyLimited)
		if readErr != nil {
			snippet = fmt.Sprintf("status=%s body=(read error: %v)", resp.Status, readErr)
		} else if resp.ContentLength != -1 && resp.ContentLength > limit {
			snippet = fmt.Sprintf("status=%s body=%q (truncated)", resp.Status, string(bodyBytes))
		} else {
			snippet = fmt.Sprintf("status=%s body=%q", resp.Status, string(bodyBytes))
		}

		notifier.logger.Warn("ees notify failed: non-2xx response",
			zap.String("subscriptionId", subscription.ID),
			zap.String("notifUri", subscription.NotifURI),
			zap.Int("statusCode", resp.StatusCode),
			zap.Int("items", len(items)),
			zap.String("response", snippet),
		)
		return fmt.Errorf("notify: non-2xx response: %s", resp.Status)
	}

	// Success log (compact, with essential identifiers).
	notifier.logger.Debug("ees notify success",
		zap.String("subscriptionId", subscription.ID),
		zap.String("notifUri", subscription.NotifURI),
		zap.Int("items", len(items)),
	)

	return nil
}
