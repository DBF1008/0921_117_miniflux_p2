#!/usr/bin/env bash
# test.sh — manually run the unit test suite.
#
# Usage:
#   ./test.sh                        Run vet + all unit tests (race detector + coverage).
#   ./test.sh ./internal/reader/...  Run vet + tests for specific packages only.
#   ./test.sh -run TestFeedScheduleBackoff ./internal/model/
#                                    Pass extra flags through to `go test`.
#
# Note: database-backed integration tests are not included here;
# use `make integration-test` for those (requires PostgreSQL).

set -euo pipefail

cd "$(dirname "$0")"

PACKAGES=()
EXTRA_ARGS=()
for arg in "$@"; do
    case "$arg" in
        ./*|miniflux.app/*) PACKAGES+=("$arg") ;;
        *) EXTRA_ARGS+=("$arg") ;;
    esac
done
if [ ${#PACKAGES[@]} -eq 0 ]; then
    PACKAGES=("./...")
fi

echo "==> go vet ${PACKAGES[*]}"
go vet "${PACKAGES[@]}"

echo "==> go test -cover -race -count=1 ${EXTRA_ARGS[*]+"${EXTRA_ARGS[@]}"} ${PACKAGES[*]}"
go test -cover -race -count=1 ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} "${PACKAGES[@]}"

echo "==> All unit tests passed"
