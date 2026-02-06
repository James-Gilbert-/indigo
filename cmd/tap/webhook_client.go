package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type WebhookClient struct {
	logger        *slog.Logger
	webhookURL    string
	adminPassword string
	httpClient    *http.Client
}

func (w *WebhookClient) Send(ctx context.Context, evt *OutboxEvt, ackFn func(uint)) {
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for retries := 0; ctx.Err() == nil; {
		if err := w.Post(ctx, evt); err != nil {
			w.logger.Warn("webhook failed, retrying", "error", err, "id", evt.ID, "retries", retries)
			timer.Reset(backoff(retries, 10))
			select {
			case <-ctx.Done():
			case <-timer.C:
				retries++
			}
			continue
		}
		ackFn(evt.ID)
		return
	}
}

func (w *WebhookClient) Post(ctx context.Context, evt *OutboxEvt) error {
	req, err := http.NewRequestWithContext(ctx, "POST", w.webhookURL, bytes.NewReader(evt.Event))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent())
	if w.adminPassword != "" {
		req.SetBasicAuth("admin", w.adminPassword)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		webhookRequests.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		webhookRequests.WithLabelValues("non_2xx").Inc()
		return fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
	}

	webhookRequests.WithLabelValues("success").Inc()
	return nil
}
