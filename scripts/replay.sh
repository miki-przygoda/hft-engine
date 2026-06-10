#!/usr/bin/env bash
# Offline replay: feed a captured (or synthesized) .krkr file through the engine
# with no network, then print the full-stack latency report. Works anywhere.
set -euo pipefail
cd "$(dirname "$0")/.."

FILE="${1:-recordings/sample.krkr}"

cargo build --release
mkdir -p recordings
if [ ! -f "$FILE" ]; then
  echo "[replay] $FILE not found — synthesizing a sample capture"
  ./target/release/kraken-feed --synth "$FILE"
fi

echo "[replay] starting engine (HFT_EXTERNAL_FEED=1 — internal simulator off)…"
[ -n "${HFT_TARGET_PRICE:-}" ] && echo "[replay] target-price mode: HFT_TARGET_PRICE=$HFT_TARGET_PRICE"
env HFT_EXTERNAL_FEED=1 ${HFT_TARGET_PRICE:+HFT_TARGET_PRICE="$HFT_TARGET_PRICE"} \
  ./target/release/trading-engine &
ENG=$!
trap 'kill "$ENG" 2>/dev/null || true' EXIT
sleep 1.5

echo "[replay] replaying $FILE through the ingestor…"
./target/release/kraken-feed --replay "$FILE"

echo "[replay] feed done — waiting ~10s for the engine's idle-shutdown report…"
wait "$ENG" || true
trap - EXIT
