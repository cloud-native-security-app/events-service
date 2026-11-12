.PHONY: verify fmt clippy test doc

# Full local verification, same sequence as CI.
verify: fmt clippy test doc

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test

doc:
	cargo doc --no-deps
