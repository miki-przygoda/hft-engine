# rust-hft-software — common tasks.
# Release builds are mandatory (the hot path depends on optimisation).

SHELL  := /bin/bash
PAIR   ?= XBT/USD
DUR    ?= 30
SAMPLE ?= recordings/sample.krkr

.PHONY: help build test bench run synth replay live clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-9s\033[0m %s\n", $$1, $$2}'

build: ## Release build of every binary
	cargo build --release

test: ## Unit tests (SHA-1 / base64 / WebSocket / Kraken parser)
	cargo test

bench: build ## Single- and multi-core SIMD throughput ceilings
	./target/release/bench-one-threaded
	./target/release/bench-multi-threaded

run: build ## Self-contained in-process simulation (synthetic transit)
	./target/release/trading-engine

synth: build ## Fabricate an offline sample capture ($(SAMPLE))
	@mkdir -p recordings
	./target/release/kraken-feed --synth $(SAMPLE)

replay: ## Offline: replay a capture through the engine (no network). FILE=path optional
	@scripts/replay.sh $(if $(FILE),$(FILE),$(SAMPLE))

live: ## Live: stream real Kraken trades for DUR=$(DUR)s (needs stunnel). PAIR=$(PAIR)
	@scripts/live.sh $(DUR) $(PAIR)

clean: ## Remove build artifacts
	cargo clean
