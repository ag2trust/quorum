#!/bin/sh
# Run the opt-in, repeated real-process SQLite contention canaries.
#
# The normal test suite keeps one exact smoke race for each path. This lane runs
# the original multi-round and multi-racer depths. Every helper is waited for by
# the test support harness, which kills and reaps it on timeout or unwind.

set -eu

cd "$(dirname "$0")/.."

cargo test -p quorum-core --features test-support --test test_helper_races -- --ignored
cargo test -p quorum-core --features test-support --test assessment_process -- --ignored
cargo test -p quorum-core --features test-support --test decomposition_process -- --ignored
cargo test -p quorum --test provider_retry_race -- --ignored
