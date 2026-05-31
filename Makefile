.PHONY: test lint format build

build:
	cargo build --workspace

test: lint
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings

format:
	cargo fmt --all
