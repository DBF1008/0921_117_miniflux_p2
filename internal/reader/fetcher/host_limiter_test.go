// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package fetcher // import "miniflux.app/v2/internal/reader/fetcher"

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostConcurrencyLimiterLimitsConcurrentRequests(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	var current atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			release := limiter.Acquire("example.org")
			defer release()

			now := current.Add(1)
			for {
				observed := maxObserved.Load()
				if now <= observed || maxObserved.CompareAndSwap(observed, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			current.Add(-1)
		}()
	}
	wg.Wait()

	if maxObserved.Load() != 1 {
		t.Errorf("Expected at most 1 concurrent request, got %d", maxObserved.Load())
	}
}

func TestHostConcurrencyLimiterAllowsUpToLimit(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(2)

	var current atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			release := limiter.Acquire("example.org")
			defer release()

			now := current.Add(1)
			for {
				observed := maxObserved.Load()
				if now <= observed || maxObserved.CompareAndSwap(observed, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			current.Add(-1)
		}()
	}
	wg.Wait()

	if maxObserved.Load() > 2 {
		t.Errorf("Expected at most 2 concurrent requests, got %d", maxObserved.Load())
	}
}

func TestHostConcurrencyLimiterBlocksUntilRelease(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	release := limiter.Acquire("example.org")

	acquired := make(chan struct{})
	go func() {
		secondRelease := limiter.Acquire("example.org")
		secondRelease()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("Second acquire on the same host should block")
	case <-time.After(50 * time.Millisecond):
	}

	release()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("Acquire should succeed once the slot is released")
	}
}

func TestHostConcurrencyLimiterDifferentHostsAreIndependent(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	release := limiter.Acquire("example.org")
	defer release()

	done := make(chan struct{})
	go func() {
		secondRelease := limiter.Acquire("example.com")
		secondRelease()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Acquiring a different host should not block")
	}
}

func TestHostConcurrencyLimiterPenalizeDelaysAcquire(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	limiter.Penalize("example.org", 100*time.Millisecond)

	start := time.Now()
	release := limiter.Acquire("example.org")
	release()

	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("Acquire should wait for the cooldown to expire, waited only %s", elapsed)
	}
}

func TestHostConcurrencyLimiterPenalizeKeepsLongestCooldown(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	limiter.Penalize("EXAMPLE.org", 200*time.Millisecond)
	limiter.Penalize("example.org", 50*time.Millisecond)

	start := time.Now()
	release := limiter.Acquire("Example.ORG")
	release()

	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("Expected the longest cooldown to be kept, waited only %s", elapsed)
	}
}

func TestHostConcurrencyLimiterPenalizeWithZeroDurationIsNoop(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	limiter.Penalize("example.org", 0)

	start := time.Now()
	release := limiter.Acquire("example.org")
	release()

	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("A zero-duration penalty should not delay acquire, waited %s", elapsed)
	}
}

func TestHostConcurrencyLimiterExpiredCooldownIsRemoved(t *testing.T) {
	limiter := NewHostConcurrencyLimiter(1)

	limiter.Penalize("example.org", 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	start := time.Now()
	release := limiter.Acquire("example.org")
	release()

	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("An expired cooldown should not delay acquire, waited %s", elapsed)
	}
}
