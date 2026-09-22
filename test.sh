#!/bin/sh
# Manual unit-test runner for the feed-fetching rate-limit / adaptive
# backoff changes.
#
# Usage:
#   ./test.sh           # run everything (ratelimit focus + full suite)
#   ./test.sh ratelimit # run only the new ratelimit package tests (verbose)
#   ./test.sh affected  # run tests for all packages touched by this change
#   ./test.sh all       # run the full unit-test suite (same as `make test`)
#
# Note: tests that bind local ports (httptest) or need PostgreSQL require a
# normal, non-sandboxed environment.

set -e

cd "$(dirname "$0")"

run_ratelimit() {
    echo "==> ratelimit: host concurrency + exponential backoff (verbose, -race)"
    go test -race -count=1 -v ./internal/reader/ratelimit/
}

run_affected() {
    echo "==> affected packages: fetcher, handler, worker, cli, scheduler"
    go test -count=1 \
        ./internal/reader/ratelimit/ \
        ./internal/reader/fetcher/ \
        ./internal/reader/handler/ \
        ./internal/worker/ \
        ./internal/cli/
}

run_all() {
    echo "==> full unit-test suite (mirrors \`make test\`)"
    go test -cover -race -count=1 ./...
}

case "${1:-}" in
    ratelimit)
        run_ratelimit
        ;;
    affected)
        run_affected
        ;;
    all)
        run_all
        ;;
    "")
        run_ratelimit
        run_affected
        run_all
        ;;
    *)
        echo "unknown target: $1 (expected: ratelimit | affected | all)" >&2
        exit 1
        ;;
esac

echo "==> done"
