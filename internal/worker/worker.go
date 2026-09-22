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
	feedHandler "miniflux.app/v2/internal/reader/handler"
	"miniflux.app/v2/internal/reader/ratelimit"
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
		feedHostname := urllib.Domain(job.FeedURL)
		if backoffRemaining := ratelimit.Shared().BackoffRemaining(feedHostname); backoffRemaining > 0 {
			slog.Info("Skipping feed refresh, host is in backoff",
				slog.Int("worker_id", w.id),
				slog.Int64("user_id", job.UserID),
				slog.Int64("feed_id", job.FeedID),
				slog.String("feed_url", job.FeedURL),
				slog.String("feed_hostname", feedHostname),
				slog.Int("backoff_remaining_in_seconds", int(backoffRemaining.Seconds())),
			)
			continue
		}

		slog.Debug("Job received by worker",
			slog.Int("worker_id", w.id),
			slog.Int64("user_id", job.UserID),
			slog.Int64("feed_id", job.FeedID),
			slog.String("feed_url", job.FeedURL),
		)

		startTime := time.Now()
		localizedError := feedHandler.RefreshFeed(w.store, job.UserID, job.FeedID, false)
		if localizedError != nil {
			slog.Warn("Unable to refresh feed",
				slog.Int("worker_id", w.id),
				slog.Int64("user_id", job.UserID),
				slog.Int64("feed_id", job.FeedID),
				slog.String("feed_url", job.FeedURL),
				slog.String("feed_hostname", feedHostname),
				slog.Int("consecutive_failures", ratelimit.Shared().FailureCount(feedHostname)),
				slog.Any("error", localizedError.Error()),
			)
		}

		if config.Opts.HasMetricsCollector() {
			status := metric.StatusSuccess
			if localizedError != nil {
				status = metric.StatusError
			}
			metric.BackgroundFeedRefreshDuration.WithLabelValues(status).Observe(time.Since(startTime).Seconds())
		}
	}
}
