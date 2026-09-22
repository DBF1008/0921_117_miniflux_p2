// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package fetcher // import "miniflux.app/v2/internal/reader/fetcher"

import (
	"strings"
	"sync"
	"time"

	"miniflux.app/v2/internal/config"
)

const defaultMaxConcurrentRequestsPerHost = 1

// HostConcurrencyLimiter limits the number of simultaneous HTTP requests sent
// to a single host and supports temporary cooldown periods during which new
// requests to a throttled host are delayed.
type HostConcurrencyLimiter struct {
	mutex     sync.Mutex
	limitFunc func() int
	slots     map[string]chan struct{}
	cooldowns map[string]time.Time
}

// NewHostConcurrencyLimiter creates a limiter that allows at most limit
// concurrent requests per host.
func NewHostConcurrencyLimiter(limit int) *HostConcurrencyLimiter {
	return newHostConcurrencyLimiter(func() int { return limit })
}

func newHostConcurrencyLimiter(limitFunc func() int) *HostConcurrencyLimiter {
	return &HostConcurrencyLimiter{
		limitFunc: limitFunc,
		slots:     make(map[string]chan struct{}),
		cooldowns: make(map[string]time.Time),
	}
}

// DefaultHostLimiter is the limiter used by default for all outgoing
// requests. The per-host concurrency limit is derived from the
// POLLING_LIMIT_PER_HOST configuration option when set, so that the scheduler
// batch limit and the live request concurrency stay consistent. Otherwise it
// falls back to a single concurrent request per host.
var DefaultHostLimiter = newHostConcurrencyLimiter(func() int {
	if config.Opts != nil && config.Opts.PollingLimitPerHost() > 0 {
		return config.Opts.PollingLimitPerHost()
	}
	return defaultMaxConcurrentRequestsPerHost
})

// Acquire blocks until any cooldown period for the host has elapsed and a
// concurrency slot is available. The returned release function must be called
// once the request is complete.
func (l *HostConcurrencyLimiter) Acquire(host string) (release func()) {
	host = strings.ToLower(host)

	// Wait for any active cooldown period to expire.
	for {
		l.mutex.Lock()
		until, onCooldown := l.cooldowns[host]
		if !onCooldown || !time.Now().Before(until) {
			delete(l.cooldowns, host)
			l.mutex.Unlock()
			break
		}
		l.mutex.Unlock()
		time.Sleep(time.Until(until))
	}

	l.mutex.Lock()
	slot, found := l.slots[host]
	if !found {
		slot = make(chan struct{}, max(l.limitFunc(), 1))
		l.slots[host] = slot
	}
	l.mutex.Unlock()

	slot <- struct{}{}
	return func() { <-slot }
}

// Penalize delays new requests to the given host for the provided duration.
// If the host is already cooling down for a longer period, the existing
// cooldown is kept.
func (l *HostConcurrencyLimiter) Penalize(host string, duration time.Duration) {
	if duration <= 0 {
		return
	}

	host = strings.ToLower(host)
	until := time.Now().Add(duration)

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if existing, found := l.cooldowns[host]; found && existing.After(until) {
		return
	}
	l.cooldowns[host] = until
}
