// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ratelimit // import "miniflux.app/v2/internal/reader/ratelimit"

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquireReleaseLimitsConcurrency(t *testing.T) {
	limiter := NewHostLimiter(2, time.Minute, 10*time.Minute)

	limiter.Acquire("example.com")
	limiter.Acquire("example.com")

	// The third acquire must block because only 2 slots are available.
	acquired := make(chan struct{})
	go func() {
		limiter.Acquire("example.com")
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("third acquire should block while both slots are taken")
	case <-time.After(50 * time.Millisecond):
	}

	limiter.Release("example.com")

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("third acquire should succeed after a slot is released")
	}
}

func TestAcquireDifferentHostsAreIndependent(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 10*time.Minute)

	limiter.Acquire("a.example.com")

	acquired := make(chan struct{})
	go func() {
		limiter.Acquire("b.example.com")
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("acquiring a different host should not block")
	}
}

func TestConcurrentAcquireNeverExceedsLimit(t *testing.T) {
	limiter := NewHostLimiter(3, time.Minute, 10*time.Minute)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limiter.Acquire("example.com")
			current := inFlight.Add(1)
			for {
				peak := maxInFlight.Load()
				if current <= peak || maxInFlight.CompareAndSwap(peak, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inFlight.Add(-1)
			limiter.Release("example.com")
		}()
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > 3 {
		t.Fatalf("expected at most 3 concurrent requests, got %d", got)
	}
}

func TestReportFailureExponentialBackoff(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 30*time.Minute)

	expected := []time.Duration{
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
		16 * time.Minute,
		30 * time.Minute, // Capped at maxBackoff.
		30 * time.Minute,
	}

	for i, want := range expected {
		if got := limiter.ReportFailure("example.com", 0); got != want {
			t.Fatalf("failure #%d: expected backoff %v, got %v", i+1, want, got)
		}
	}

	if got := limiter.FailureCount("example.com"); got != len(expected) {
		t.Fatalf("expected %d consecutive failures, got %d", len(expected), got)
	}
}

func TestReportFailureHonorsRetryAfter(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 30*time.Minute)

	// A Retry-After delay larger than the computed exponential backoff wins.
	if got := limiter.ReportFailure("example.com", 10*time.Minute); got != 10*time.Minute {
		t.Fatalf("expected backoff of 10m from Retry-After, got %v", got)
	}

	// A Retry-After delay smaller than the computed backoff is ignored.
	if got := limiter.ReportFailure("example.com", time.Second); got != 2*time.Minute {
		t.Fatalf("expected exponential backoff of 2m, got %v", got)
	}
}

func TestReportSuccessResetsBackoff(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 30*time.Minute)

	limiter.ReportFailure("example.com", 0)
	limiter.ReportFailure("example.com", 0)

	if !limiter.InBackoff("example.com") {
		t.Fatal("host should be in backoff after failures")
	}

	limiter.ReportSuccess("example.com")

	if limiter.InBackoff("example.com") {
		t.Fatal("host should not be in backoff after a success")
	}
	if got := limiter.FailureCount("example.com"); got != 0 {
		t.Fatalf("expected failure count to be reset, got %d", got)
	}

	// The backoff restarts from the base delay after a reset.
	if got := limiter.ReportFailure("example.com", 0); got != time.Minute {
		t.Fatalf("expected backoff to restart at 1m, got %v", got)
	}
}

func TestBackoffRemaining(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 30*time.Minute)

	if got := limiter.BackoffRemaining("unknown.example.com"); got != 0 {
		t.Fatalf("expected no backoff for unknown host, got %v", got)
	}

	limiter.ReportFailure("example.com", 0)

	remaining := limiter.BackoffRemaining("example.com")
	if remaining <= 0 || remaining > time.Minute {
		t.Fatalf("expected remaining backoff in (0, 1m], got %v", remaining)
	}
}

func TestBackoffExpires(t *testing.T) {
	limiter := NewHostLimiter(1, 20*time.Millisecond, time.Minute)

	limiter.ReportFailure("example.com", 0)
	if !limiter.InBackoff("example.com") {
		t.Fatal("host should be in backoff right after a failure")
	}

	time.Sleep(30 * time.Millisecond)

	if limiter.InBackoff("example.com") {
		t.Fatal("host should no longer be in backoff after the delay elapsed")
	}
	if got := limiter.BackoffRemaining("example.com"); got != 0 {
		t.Fatalf("expected zero remaining backoff, got %v", got)
	}
}

func TestBackoffStateIsPerHost(t *testing.T) {
	limiter := NewHostLimiter(1, time.Minute, 30*time.Minute)

	limiter.ReportFailure("a.example.com", 0)

	if !limiter.InBackoff("a.example.com") {
		t.Fatal("a.example.com should be in backoff")
	}
	if limiter.InBackoff("b.example.com") {
		t.Fatal("b.example.com should not be affected by another host backoff")
	}
}

func TestNewHostLimiterDefaults(t *testing.T) {
	limiter := NewHostLimiter(0, 0, 0)

	// The default concurrency limit should allow DefaultMaxConcurrentPerHost
	// slots and block the next one.
	for range DefaultMaxConcurrentPerHost {
		limiter.Acquire("example.com")
	}

	acquired := make(chan struct{})
	go func() {
		limiter.Acquire("example.com")
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("acquire should block once the default limit is reached")
	case <-time.After(50 * time.Millisecond):
	}

	if got := limiter.ReportFailure("example.com", 0); got != DefaultBaseBackoff {
		t.Fatalf("expected default base backoff %v, got %v", DefaultBaseBackoff, got)
	}
}

func TestSharedReturnsSingleton(t *testing.T) {
	if Shared() != Shared() {
		t.Fatal("Shared should always return the same instance")
	}
}
