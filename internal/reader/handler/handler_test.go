// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package handler // import "miniflux.app/v2/internal/reader/handler"

import (
	"net/http"
	"testing"
	"time"
)

func TestRefreshResultIsRateLimited(t *testing.T) {
	result := &RefreshResult{HTTPStatusCode: http.StatusTooManyRequests}
	if !result.IsRateLimited() {
		t.Error("A 429 status code should be reported as rate limited")
	}

	result = &RefreshResult{HTTPStatusCode: http.StatusOK}
	if result.IsRateLimited() {
		t.Error("A 200 status code should not be reported as rate limited")
	}
}

func TestRefreshResultIsServerError(t *testing.T) {
	testCases := map[int]bool{
		http.StatusOK:                            false,
		http.StatusTooManyRequests:               false,
		http.StatusInternalServerError:           true,
		http.StatusBadGateway:                    true,
		http.StatusServiceUnavailable:            true,
		http.StatusGatewayTimeout:                true,
		http.StatusNetworkAuthenticationRequired: true,
	}

	for statusCode, expected := range testCases {
		result := &RefreshResult{HTTPStatusCode: statusCode}
		if result.IsServerError() != expected {
			t.Errorf("IsServerError for status code %d should be %v", statusCode, expected)
		}
	}
}

func TestRefreshResultShouldThrottleHost(t *testing.T) {
	testCases := map[int]bool{
		http.StatusOK:                  false,
		http.StatusNotFound:            false,
		http.StatusForbidden:           false,
		http.StatusTooManyRequests:     true,
		http.StatusInternalServerError: true,
		http.StatusServiceUnavailable:  true,
	}

	for statusCode, expected := range testCases {
		result := &RefreshResult{HTTPStatusCode: statusCode}
		if result.ShouldThrottleHost() != expected {
			t.Errorf("ShouldThrottleHost for status code %d should be %v", statusCode, expected)
		}
	}
}

func TestRefreshResultHostCooldownWithoutThrottle(t *testing.T) {
	result := &RefreshResult{HTTPStatusCode: http.StatusNotFound, RetryDelay: time.Minute}
	if cooldown := result.HostCooldown(); cooldown != 0 {
		t.Errorf("A client error should not trigger a host cooldown, got %s", cooldown)
	}
}

func TestRefreshResultHostCooldownUsesRetryDelay(t *testing.T) {
	result := &RefreshResult{HTTPStatusCode: http.StatusTooManyRequests, RetryDelay: 2 * time.Minute}
	if cooldown := result.HostCooldown(); cooldown != 2*time.Minute {
		t.Errorf("Expected the server-provided retry delay, got %s", cooldown)
	}
}

func TestRefreshResultHostCooldownCapsRetryDelay(t *testing.T) {
	result := &RefreshResult{HTTPStatusCode: http.StatusServiceUnavailable, RetryDelay: time.Hour}
	if cooldown := result.HostCooldown(); cooldown != maxHostCooldown {
		t.Errorf("Expected the retry delay to be capped at %s, got %s", maxHostCooldown, cooldown)
	}
}

func TestRefreshResultHostCooldownFallsBackToDefault(t *testing.T) {
	result := &RefreshResult{HTTPStatusCode: http.StatusInternalServerError}
	if cooldown := result.HostCooldown(); cooldown != defaultHostCooldown {
		t.Errorf("Expected the default cooldown %s, got %s", defaultHostCooldown, cooldown)
	}
}
