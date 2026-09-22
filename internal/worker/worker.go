// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker // import "miniflux.app/v2/internal/worker"

import (
	"log/slog"
	"sync"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/metric"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/reader/fetcher"
	feedHandler "miniflux.app/v2/internal/reader/handler"
	"miniflux.app/v2/internal/storage"
	"miniflux.app/v2/internal/urllib"
)

type worker struct {
	id    int
	store *storage.Storage
}

// Run processes feed refresh jobs from the channel until it is closed.
func (w *worker) Run(c <-chan model.Job, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Debug("Worker started",
		slog.Int("worker_id", w.id),
	)

	for job := range c {
		slog.Debug("Job received by worker",
			slog.Int("worker_id", w.id),
			slog.Int64("user_id", job.UserID),
			slog.Int64("feed_id", job.FeedID),
			slog.String("feed_url", job.FeedURL),
		)

		startTime := time.Now()
		result := feedHandler.RefreshFeed(w.store, job.UserID, job.FeedID, false)

		if result.LocalizedError != nil {
			slog.Warn("Unable to refresh feed",
				slog.Int("worker_id", w.id),
				slog.Int64("user_id", job.UserID),
				slog.Int64("feed_id", job.FeedID),
				slog.Any("error", result.LocalizedError.Error()),
			)

			// Delay subsequent requests to the same host when the failure is
			// server-side (429 or 5xx) to avoid hammering an overloaded server.
			if cooldown := result.HostCooldown(); cooldown > 0 {
				feedHostname := urllib.Domain(job.FeedURL)
				slog.Info("Throttling host after server-side error",
					slog.Int("worker_id", w.id),
					slog.String("feed_hostname", feedHostname),
					slog.String("cooldown", cooldown.String()),
				)
				fetcher.DefaultHostLimiter.Penalize(feedHostname, cooldown)
			}
		}

		if config.Opts.HasMetricsCollector() {
			status := metric.StatusSuccess
			if result.LocalizedError != nil {
				status = metric.StatusError
			}
			metric.BackgroundFeedRefreshDuration.WithLabelValues(status).Observe(time.Since(startTime).Seconds())
		}
	}
}
