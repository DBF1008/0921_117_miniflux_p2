// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ratelimit // import "miniflux.app/v2/internal/reader/ratelimit"

import (
	"sync"
	"time"

	"miniflux.app/v2/internal/config"
)

const (
	// DefaultMaxConcurrentPerHost is the fallback number of simultaneous
	// in-flight HTTP requests allowed for a single host when
	// POLLING_LIMIT_PER_HOST is not configured.
	DefaultMaxConcurrentPerHost = 4

	// DefaultBaseBackoff is the delay applied after the first consecutive
	// failure of a host. Each subsequent failure doubles it.
	DefaultBaseBackoff = time.Minute

	// DefaultMaxBackoff caps the exponential backoff delay.
	DefaultMaxBackoff = 30 * time.Minute
)

// hostState tracks the concurrency semaphore and the backoff state of a
// single host.
type hostState struct {
	semaphore    chan struct{}
	failures     int
	backoffUntil time.Time
}

// HostLimiter coordinates per-host concurrency limits and adaptive
// exponential backoff across the fetcher, handler and scheduler layers.
//
// The fetcher layer uses Acquire/Release to cap the number of simultaneous
// HTTP requests sent to the same host. The handler layer reports the outcome
// of each fetch with ReportSuccess/ReportFailure, which drives an
// exponential backoff per host (honoring server-provided Retry-After
// delays). The scheduler/worker layer uses BackoffRemaining to defer jobs
// targeting a host that is currently backing off.
type HostLimiter struct {
	mu                   sync.Mutex
	hosts                map[string]*hostState
	maxConcurrentPerHost int
	baseBackoff          time.Duration
	maxBackoff           time.Duration
	now                  func() time.Time
}

// NewHostLimiter creates a HostLimiter. Non-positive limits are replaced
// with sane defaults.
func NewHostLimiter(maxConcurrentPerHost int, baseBackoff, maxBackoff time.Duration) *HostLimiter {
	if maxConcurrentPerHost <= 0 {
		maxConcurrentPerHost = DefaultMaxConcurrentPerHost
	}
	if baseBackoff <= 0 {
		baseBackoff = DefaultBaseBackoff
	}
	if maxBackoff < baseBackoff {
		maxBackoff = baseBackoff
	}
	return &HostLimiter{
		hosts:                make(map[string]*hostState),
		maxConcurrentPerHost: maxConcurrentPerHost,
		baseBackoff:          baseBackoff,
		maxBackoff:           maxBackoff,
		now:                  time.Now,
	}
}

func (l *HostLimiter) state(host string) *hostState {
	state, found := l.hosts[host]
	if !found {
		state = &hostState{semaphore: make(chan struct{}, l.maxConcurrentPerHost)}
		l.hosts[host] = state
	}
	return state
}

// Acquire blocks until a request slot is available for the given host.
func (l *HostLimiter) Acquire(host string) {
	l.mu.Lock()
	state := l.state(host)
	l.mu.Unlock()
	state.semaphore <- struct{}{}
}

// Release frees a request slot previously acquired with Acquire.
func (l *HostLimiter) Release(host string) {
	l.mu.Lock()
	state, found := l.hosts[host]
	l.mu.Unlock()
	if found {
		<-state.semaphore
	}
}

// ReportSuccess resets the backoff state of the given host.
func (l *HostLimiter) ReportSuccess(host string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if state, found := l.hosts[host]; found {
		state.failures = 0
		state.backoffUntil = time.Time{}
	}
}

// ReportFailure records a fetch failure for the given host and returns the
// backoff delay that now applies. The delay grows exponentially with the
// number of consecutive failures (capped at maxBackoff), and is raised to
// retryAfter when the server explicitly asked for a longer delay (e.g. via
// the Retry-After header of a 429 response).
func (l *HostLimiter) ReportFailure(host string, retryAfter time.Duration) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.state(host)
	state.failures++

	backoff := l.baseBackoff
	for i := 1; i < state.failures && backoff < l.maxBackoff; i++ {
		backoff *= 2
	}
	backoff = min(backoff, l.maxBackoff)
	backoff = max(backoff, retryAfter)

	state.backoffUntil = l.now().Add(backoff)
	return backoff
}

// BackoffRemaining returns how long the given host must still wait before
// being eligible for new requests. It returns 0 when the host is not in
// backoff.
func (l *HostLimiter) BackoffRemaining(host string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	state, found := l.hosts[host]
	if !found || state.backoffUntil.IsZero() {
		return 0
	}
	return max(state.backoffUntil.Sub(l.now()), 0)
}

// InBackoff reports whether the given host is currently backing off.
func (l *HostLimiter) InBackoff(host string) bool {
	return l.BackoffRemaining(host) > 0
}

// FailureCount returns the number of consecutive failures recorded for the
// given host.
func (l *HostLimiter) FailureCount(host string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if state, found := l.hosts[host]; found {
		return state.failures
	}
	return 0
}

var (
	sharedOnce sync.Once
	shared     *HostLimiter
)

// Shared returns the process-wide HostLimiter used by the fetcher, handler
// and scheduler layers. The per-host concurrency limit defaults to
// POLLING_LIMIT_PER_HOST when configured.
func Shared() *HostLimiter {
	sharedOnce.Do(func() {
		maxConcurrentPerHost := DefaultMaxConcurrentPerHost
		if config.Opts != nil && config.Opts.PollingLimitPerHost() > 0 {
			maxConcurrentPerHost = config.Opts.PollingLimitPerHost()
		}
		shared = NewHostLimiter(maxConcurrentPerHost, DefaultBaseBackoff, DefaultMaxBackoff)
	})
	return shared
}
