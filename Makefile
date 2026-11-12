.PHONY: verify check fmt clippy test test-integration doc

# Full local verification, same sequence as CI. The integration tests need
# Docker (testcontainers + LocalStack); without Docker use `make check` and
# report the blocker, never replace them with mocks.
verify: fmt clippy test test-integration doc

# Same as verify without the Docker-dependent integration tests.
check: fmt clippy test doc

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings

# Unit tests; tests marked #[ignore = "requires Docker"] are skipped.
test:
	cargo test

# Round trip against a real LocalStack (Kinesis) via testcontainers.
test-integration:
	cargo test -- --ignored

doc:
	cargo doc --no-deps
