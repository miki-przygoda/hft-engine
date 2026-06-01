#!/usr/bin/env bash
# Live run: stream real Kraken trades through the engine for DUR seconds, record
# the capture, then print the full-stack latency report. macOS + Linux.
#
# Usage:   scripts/live.sh [DURATION_SECONDS] [PAIR]
# Example: scripts/live.sh 30 XBT/USD
#
# Requires stunnel (TLS terminator):
#   macOS:  brew install stunnel
#   Ubuntu: sudo apt-get install stunnel4
#
# On Linux, prefix the engine with sudo for SCHED_FIFO/affinity (CAP_SYS_NICE):
#   SUDO=sudo scripts/live.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-30}"
PAIR="${2:-XBT/USD}"
SUDO="${SUDO:-}"

STUNNEL="$(command -v stunnel || command -v stunnel4 || true)"
if [ -z "$STUNNEL" ]; then
  echo "stunnel not found. Install it first:"
  echo "  macOS:  brew install stunnel"
  echo "  Ubuntu: sudo apt-get install stunnel4"
  exit 1
fi

cargo build --release
mkdir -p recordings
REC="recordings/live-$(date +%Y%m%d-%H%M%S).krkr"

ST=""; ENG=""; FEED=""
cleanup() { for p in "$FEED" "$ENG" "$ST"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "[live] starting stunnel ($STUNNEL) → ws.kraken.com:443 (127.0.0.1:8443)…"
"$STUNNEL" docs/stunnel.conf & ST=$!
sleep 1

echo "[live] starting engine (HFT_EXTERNAL_FEED=1)…"
HFT_EXTERNAL_FEED=1 $SUDO ./target/release/trading-engine & ENG=$!
sleep 1.5

echo "[live] streaming $PAIR for ${DUR}s — recording → $REC"
./target/release/kraken-feed --live 127.0.0.1:8443 --pair "$PAIR" --record "$REC" & FEED=$!
sleep "$DUR"
kill "$FEED" 2>/dev/null || true
wait "$FEED" 2>/dev/null || true
FEED=""

echo "[live] feed stopped — waiting ~10s for the engine's idle-shutdown report…"
wait "$ENG" 2>/dev/null || true
ENG=""
echo "[live] done. Replay this capture offline anytime with:"
echo "       scripts/replay.sh $REC"
