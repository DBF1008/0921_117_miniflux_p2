// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cli // import "miniflux.app/v2/internal/cli"

import (
	"log/slog"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/reader/ratelimit"
	"miniflux.app/v2/internal/storage"
	"miniflux.app/v2/internal/worker"
	"miniflux.app/v2/internal/urllib"
)

func runScheduler(store *storage.Storage, pool *worker.Pool) {
	slog.Debug(`Starting background scheduler...`)

	go feedScheduler(
		store,
		pool,
		config.Opts.PollingFrequency(),
		config.Opts.BatchSize(),
		config.Opts.PollingParsingErrorLimit(),
		config.Opts.PollingLimitPerHost(),
	)

	go cleanupScheduler(
		store,
		config.Opts.CleanupFrequency(),
	)
}

func feedScheduler(store *storage.Storage, pool *worker.Pool, frequency time.Duration, batchSize, errorLimit, limitPerHost int) {
	for range time.Tick(frequency) {
		// Generate a batch of feeds for any user that has feeds to refresh.
		jobs, err := store.NewBatchBuilder().
			WithBatchSize(batchSize).
			WithErrorLimit(errorLimit).
			WithoutDisabledFeeds().
			WithNextCheckExpired().
			WithLimitPerHost(limitPerHost).
			FetchJobs()

		if err != nil {
			slog.Error("Unable to fetch jobs from database", slog.Any("error", err))
		} else if len(jobs) > 0 {
			jobs = filterJobsInBackoff(jobs)
		}

		if err == nil && len(jobs) > 0 {
			slog.Debug("Feed URLs in this batch", slog.Any("feed_urls", jobs.FeedURLs()))
			pool.Push(jobs)
		}
	}
}

// filterJobsInBackoff removes jobs targeting a host that is currently
// backing off after repeated fetch failures, so that throttled origin
// servers are not polled again until their backoff delay has elapsed.
func filterJobsInBackoff(jobs model.JobList) model.JobList {
	hostLimiter := ratelimit.Shared()
	filtered := make(model.JobList, 0, len(jobs))
	for _, job := range jobs {
		feedHostname := urllib.Domain(job.FeedURL)
		if backoffRemaining := hostLimiter.BackoffRemaining(feedHostname); backoffRemaining > 0 {
			slog.Debug("Job deferred, host is in backoff",
				slog.String("feed_url", job.FeedURL),
				slog.String("feed_hostname", feedHostname),
				slog.Int("backoff_remaining_in_seconds", int(backoffRemaining.Seconds())),
			)
			continue
		}
		filtered = append(filtered, job)
	}
	return filtered
}

func cleanupScheduler(store *storage.Storage, frequency time.Duration) {
	for range time.Tick(frequency) {
		runCleanupTasks(store)
	}
}
