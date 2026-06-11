# rust-hft-software

[![CI](https://github.com/miki-przygoda/hft-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/miki-przygoda/hft-engine/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Rust 2024](https://img.shields.io/badge/Rust-edition%202024-orange.svg)
![Platforms](https://img.shields.io/badge/platforms-macOS%20arm64%20%7C%20Linux%20x86__64-blue.svg)

A high-frequency trading engine built from scratch in Rust, targeting Apple Silicon (ARM64 / M-series) with an AVX2 path for x86_64 Linux. Zero external dependencies. Every architectural decision is evaluated in nanoseconds.

> **New here?** [`CLAUDE.md`](CLAUDE.md) is the full architecture & design reference — the *why* behind every decision below. Want to contribute? See [`CONTRIBUTING.md`](CONTRIBUTING.md).

**Measured latency — in-process simulation:**

| Platform                      | Min       | p50    | p95    | p99      | p99.9     | Max        |
|-------------------------------|-----------|--------|--------|----------|-----------|------------|
| macOS — M3 Max (signal)       | **41 ns** | 125 ns | 250 ns | 458 ns   | 1,917 ns  | 1,917 ns   |
| macOS — M3 Max (round-trip)   | **41 ns** | 84 ns  | 375 ns | 3,458 ns | 10,001 ns | 105,083 ns |
| Linux — i9-9900K (signal)     | 89 ns     | 118 ns | 143 ns | 150 ns   | 1,317 ns  | 1,317 ns   |
| Linux — i9-9900K (round-trip) | 92 ns     | 108 ns | 140 ns | 199 ns   | 1,263 ns  | 1,263 ns   |

The two platforms make different tradeoffs. The M3 Max achieves a lower floor (41 ns vs 89 ns) — ARM64 NEON and the P-core cluster's memory subsystem. Linux on x86_64 delivers tighter tail discipline — p99 round-trip 199 ns vs 3,458 ns on macOS. The Mac's scheduling spikes are rarer (6,868 stalls/run) but longer when they happen; Linux stalls more frequently (21,741/run) but more uniformly. Neither is "better" — they're different OS scheduling personalities against the same spin-poll workload.

External UDP mode (3-process, kernel boundaries): **43–135 µs** — ~163× higher than in-process. That gap is the architectural thesis.

---

## What it does

The engine ingests market tick data, runs a momentum signal over a sliding 8-price window, submits orders to an in-process exchange, and records per-trade latency at nanosecond resolution. The current simulation is self-contained — one binary spawns the market feed, the exchange, and the strategy thread internally.

Three threads share elevated priority (macOS `QOS_USER_INTERACTIVE` / P-core bias; Linux `sched_setaffinity` equivalent is a planned addition):

- **Ingestor** — binds UDP 34254, spin-polls, writes ticks into a lock-free ring buffer
- **Strategy** — spin-polls the ring buffer, evaluates the momentum signal, commits trades
- **Exchange** — spin-polls the order ring, writes round-trip timestamps back to the trade log

A **watchdog** (default priority) monitors for idle/feed-loss conditions and shuts down after 10 s idle or 30 s without a feed packet.

---

## Architecture

The design eliminates every source of unpredictable latency on the hot path. Each decision below has a measurable consequence.

### In-process exchange over external UDP

The exchange thread shares memory with the strategy thread. The round-trip path — order submission → confirmation write → timestamp read — crosses zero kernel boundaries. The `OrderRing` SPSC buffer connects them via `UnsafeCell<[OrderEntry; 1024]>` and a single `AtomicU64` cursor.

The standalone `fake-exchange` binary exists for external UDP measurement when kernel-path characterisation is needed (43–135 µs — the cost of 4× EL0→EL1 crossings plus 2 process wakeups on loopback).

### Lock-free data structures throughout

| Structure          | Pattern                           | Purpose                                      |
|--------------------|-----------------------------------|----------------------------------------------|
| `RingBuffer`       | SPSC, `AtomicU64` write head      | Ingestor → strategy tick delivery            |
| `TradeLog`         | Single-writer, `AtomicU64` cursor | Strategy commits; exchange reads slot index  |
| `OrderRing`        | SPSC, `AtomicU64` cursor          | Strategy → exchange order submission         |
| `LatencyHistogram` | Per-thread sole-writer buckets    | ns-resolution recording, no sort at shutdown |

No mutex, no condvar, no blocking synchronisation on the hot path.

### Cache-line alignment everywhere

Every struct that crosses thread boundaries is `#[repr(C, align(64))]`. `MarketTick` is exactly 64 bytes — price/volume/sequence/timestamps plus the live-feed fields (bid/ask, mark price, funding rate) and 8 bytes of padding — one tick per cache line, no false sharing, no partial-line loads.

**`start_time` is co-located with `latest_idx` in `RingBuffer`.** The strategy spin-poll loads `latest_idx` on every iteration; `start_time` at +8 bytes sits in the same cache line and is always L1-hot for free. This eliminates the timestamp cold-start penalty without any extra memory traffic.

### Register-resident momentum signal (ARM64 NEON)

The 8-price window lives in two NEON registers (`v28`/`v29`) across loop iterations — no L1 access for window state between ticks:

1. `LD1` loads 16 bytes from the new tick
2. `EXT` slides each register by one f32 lane — O(1) window update, one instruction
3. Two `FADDP` passes + `FMUL` compute the mean
4. `FCMGT` compares current price to `mean × (1 + threshold)` — result is the trigger bit

Total signal computation: ~6 NEON instructions, one tick load, zero window memory accesses between ticks. x86_64 uses an equivalent **register-resident AVX2** path: the 8-price window lives in a single `__m256` (ymm) register, shifted each tick with `vextractf128`/`vpalignr`/`vinsertf128`, reduced with two `vhaddps` passes, and compared branchlessly via `vucomiss` + `seta`.

### Hot-path startup sequence

Before the trading loop begins:

1. **Page pre-touch** — all three shared buffers written with `write_volatile` before any thread spawns. `std::mem::zeroed()` on a fresh allocation does not commit physical pages on macOS (zero-fill-on-demand); without pre-touch, the first real write triggers a page fault (~3–5 µs).
2. **NEON warmup** — 10,000 `fmul v0.4s` + `elapsed_ns()` calls. Warms vector execution units, pulls hot-path instructions into the cache, commits OS pages for the trade log array.
3. **Warmup packets** — the first 10 sequences run the full hot path without committing to the trade log. Trains the branch predictor and warms all hot-path cache lines without polluting latency measurements.

### `#[cold]` on the halt path

The halt check sits at the top of every hot-path iteration. Marking the halt function `#[cold]` biases the branch predictor toward the not-taken direction after warmup — without requiring nightly intrinsics.

### Spin-based watchdog

The watchdog spins rather than sleeping. `thread::sleep(500ms)` surrenders the thread to the scheduler, which macOS can use as an opportunity to reschedule the strategy thread. A spin-based watchdog that amortises its `elapsed_ns` call over 2^24 iterations never generates that scheduling event.

### Core affinity note

`set_thread_affinity_tag(1)` hints the macOS scheduler toward the same P-core cluster — not hard core pinning. Hard pinning was tested and produced worse results: Apple Silicon's scheduler outperforms forced single-core assignment under thermal load. The soft hint is measurably faster in practice.

### `Instant::elapsed()` over `mach_absolute_time()` FFI

`Instant` on Apple Silicon uses the commpage fast path. Direct comparison showed raw `mach_absolute_time()` FFI produced ~42× worse latencies — and it returns 24 MHz ticks requiring `× 125 / 3` conversion, adding fragility with no benefit.

---

## Latency interpretation

The OS stall count (idle-spin gaps > 500 ns, recorded per-run) is the honest proxy for scheduler interference:

| Platform       | Stalls/run | p99 round-trip | Failure mode                    |
|----------------|------------|----------------|---------------------------------|
| macOS M3 Max   | ~6,900     | 3,458 ns       | Rare but long scheduling spikes |
| Linux i9-9900K | ~21,700    | 199 ns         | Frequent but short and uniform  |

The minimum latency floor is bounded by L2 cache miss latency at the 10 ms inter-packet gaps in the simulation. At higher tick rates the cache stays warmer and the floor drops toward the hardware minimum.

---

## Running it

### Requirements

- **macOS Apple Silicon** — full NEON path, `pthread_set_qos_class_self_np` QOS, P-core affinity hint
- **Linux x86_64** — register-resident AVX2 path, `SCHED_FIFO` priority + `sched_setaffinity` core pinning (require `CAP_SYS_NICE`; run with `sudo` to enable, otherwise the engine still runs without elevated scheduling)
- **Windows** — not supported; OS scheduler overhead is incompatible with sub-microsecond targets

### Build

```bash
cargo build --release
```

### Run the simulation

```bash
cargo run --release --bin trading-engine
```

Self-contained — spawns the market feed and in-process exchange internally.

### Expected output

```
[engine] starting — running full simulation in-process
[engine] all systems ready — entering trading loop
Total trades executed: 480

Sequence     Sig Latency (ns)     Round Trip (ns)
───────────────────────────────────────────────
11           125                  84
12           83                   83
13           41                   41
...
───────────────────────────────────────────────
Signal latency — Avg:     146 ns  Min:      41 ns  Max:   1917 ns
                p50:     125 ns  p95:     250 ns  p99:     458 ns  p99.9:   1917 ns
Round trip     — Avg:     417 ns  Min:      41 ns  Max: 105083 ns
                p50:      84 ns  p95:     375 ns  p99:    3458 ns  p99.9:  10001 ns
───────────────────────────────────────────────
OS stalls (>500ns spin gap): 6868  |  Seq gaps: 0  |  Net position: 480  |  Halt: false
───────────────────────────────────────────────
Memory — Total RAM: 65536 MB
  [1] start          Peak RSS:   1 MB
  [2] after ready    Peak RSS:   1 MB
  [3] before log     Peak RSS:   2 MB
[log] saved → logs/v0.1.2/2026-04-06/11-00-50.json
```

Each run writes a JSON log to `logs/v{version}/{date}/{HH-MM-SS}.json` with per-trade data, latency percentiles, stall/gap counts, and RSS memory snapshots.

### External UDP mode (kernel-path measurement)

```bash
./target/release/fake-exchange &
./target/release/trading-engine &
./target/release/market-simulator
```

Round-trip rises to 43–135 µs — the cost of kernel UDP boundaries. Useful for isolating the syscall overhead.

### Benchmarks

```bash
cargo run --release --bin bench-one-threaded    # single-core SIMD ceiling
cargo run --release --bin bench-multi-threaded  # all-core Apple Silicon stress ("The Kraken")
```

---

## Structure

```
src/
├── main.rs              # Thread orchestration, buffer pre-touch, startup
├── engine.rs            # Runtime logic: ingestor, exchange, watchdog, simulator, strategy, logging
├── models.rs            # Data structures: MarketTick, RingBuffer, TradeLog, OrderBook, OrderRing
├── model.rs             # AlphaModel + learned Policy (tiny MLP): signal + execution, shared by live/backtest/train
├── lib.rs               # Shared config constants
├── bin/
│   ├── fake-exchange.rs         # Standalone spin-poll UDP exchange (external round-trip measurement)
│   ├── market-simulator.rs      # Standalone UDP packet sender
│   └── kraken-feed.rs           # Kraken spot + futures feed adapter (hand-rolled WebSocket, RTT, record/replay/synth)
└── testing_scripts/
    ├── one_threaded.rs          # Single-threaded SIMD throughput benchmark
    └── multi_threaded.rs        # All-core Apple Silicon stress test ("The Kraken")
```

---

## Live crypto feed

The `kraken-feed` adapter brings **real Kraken trades** into the engine and measures the full reaction stack — network transit (RTT/2), signal latency, and round-trip confirm — so you can see how the data spends *milliseconds* in flight while the engine reacts in *hundreds of nanoseconds*. It's pure zero-dependency Rust: TLS is terminated by a local `stunnel`, and the adapter speaks the WebSocket protocol (handshake, RFC6455 framing, ping/pong) by hand. It also records and deterministically replays captures for offline runs.

```bash
make replay              # offline: synthesize a capture and replay it through the engine
make live                # live: 30s of real Kraken XBT/USD (needs stunnel — see below)
make live DUR=60 PAIR=ETH/USD
make help                # all targets
```

Under the hood (equivalent to `make live` / `make replay`):

```bash
# Offline (no network): synthesize a capture, then replay it through the engine
cargo build --release
./target/release/kraken-feed --synth recordings/sample.krkr
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --replay recordings/sample.krkr

# Live: needs stunnel terminating TLS to ws.kraken.com (see docs/stunnel.conf)
#   macOS:  brew install stunnel        Ubuntu: sudo apt-get install stunnel4
stunnel docs/stunnel.conf &
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --live 127.0.0.1:8443 --pair XBT/USD --record recordings/live.krkr
```

On Linux, prefix the engine with `sudo` (or `SUDO=sudo make live`) for `SCHED_FIFO` + affinity (`CAP_SYS_NICE`).

**Kraken Futures (perpetuals).** `--futures` streams the public Futures `ticker` feed (`futures.kraken.com`, **no auth** — only private order feeds need signing) for a perp like `PF_XBTUSD`, carrying **real bid/ask spread, mark price, and funding rate**. These are the inputs for honest perpetual cost accounting (see [Realistic perpetual costs](#realistic-perpetual-costs-spread-funding-mark-sizing)). Needs a third stunnel service → `futures.kraken.com:443` (in `docs/stunnel.conf`):

```bash
stunnel docs/stunnel.conf &
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --futures PF_XBTUSD --record recordings/futures.krkr
```

The engine's market block then reports the observed spread (bps) and live funding rate. The adapter packs them into a 49-byte **v4 packet** (the first 33 bytes byte-identical to the spot v3 packet, so older senders stay valid).

**Historical data (longer sessions).** To backtest a real multi-hour "day" without waiting, pull historical trades from Kraken's REST API (needs a second stunnel service → `api.kraken.com:443`, see `docs/stunnel.conf`):

```bash
./target/release/kraken-feed --history --hours 24 --pair XBT/USD --ref-pair ETH/USD --out recordings/day.krkr
./target/release/trading-engine --backtest recordings/day.krkr      # sweep the whole day
```

For offline testing, `HFT_SYNTH_TICKS=100000 kraken-feed --synth recordings/day.krkr` fabricates a long deterministic session. Over a long session the backtest verdict is statistically confident (hundreds of out-of-sample round-trips) rather than small-sample noise.

### Target price & slippage

The engine buys at market on a trigger, then measures how far the **fill drifts from the price you wanted because of the latency gap** — the real cost of being slow. Two ways to trigger:

```bash
# Downtick: buy on ANY price decrease — guaranteed to fire on any feed that moves
# at all. Best for flat/thin markets (e.g. a quiet alt pair).
HFT_DOWNTICK=1 make live PAIR=XBT/USD

# Relative-dip: buy on an N-bps dip below a rolling reference — adapts to any
# price level. Use a small N on quiet markets (the report prints the bps it moved).
HFT_TARGET_DIP_BPS=5 make live PAIR=XBT/USD

# Absolute target: buy when the price dips through a fixed level you set.
# (Set it WITHIN the live market range, or it never triggers — see note below.)
HFT_TARGET_PRICE=60000 make replay
```

Each order's fill is the market price *one transit (RTT/2) later*, so the report gains an execution block — attempts / filled / pending and **slippage in basis points** (e.g. `mean +35 bps`, meaning the fill landed ~0.35% off the price you acted on while the order was in flight). With nothing set it runs the breakout signal and measures slippage vs the entry price. The report and JSON also break latency into **transit** and **end-to-end** stages.

> **Calibrating an absolute target:** it only fires when the price crosses *down through* your level, so set it just below the current market. The report always prints the **observed price range** — if you get 0 attempts, set `HFT_TARGET_PRICE` inside that range, or just use `HFT_TARGET_DIP_BPS` which fires at any level. See [`CLAUDE.md`](CLAUDE.md#execution-model-target-price--slippage).

### Trading model & P&L scorecard

`HFT_TRADE=1` turns the engine into a **long & short mean-reversion** model: it buys small dips / shorts small rips against a rolling reference, sizes up dynamically on bigger dislocations, and exits on **take-profit / stop-loss / opposite signal**. At shutdown it prints a P&L scorecard — round-trips, win rate, net P&L (bps & quote), profit factor, max drawdown, Sharpe, avg hold — **net of fees and scaled by leverage**:

```bash
# ADAPTIVE (recommended): thresholds auto-scale to realized volatility (entry 1σ /
# TP 1.5σ / SL 2.5σ), so it fires in any regime — no guessing bps against a market
# that might only move a fraction of your threshold.
HFT_TRADE=1 HFT_ADAPTIVE=1 HFT_FEE_BPS=2.6 HFT_LEVERAGE=2 make live PAIR=XBT/USD

# Fixed bps (you pick the levels — must be within the market's actual range):
HFT_TRADE=1 HFT_ENTRY_BPS=3 HFT_TP_BPS=10 HFT_SL_BPS=20 make live PAIR=XBT/USD

# Offline, deterministic (mean-reverting random-walk sample):
HFT_TRADE=1 HFT_ADAPTIVE=1 make replay
HFT_TRADE=1 HFT_ADAPTIVE=1 HFT_FEE_BPS=0 make replay   # isolate the gross edge
```

On the realistic (trending) synth, mean-reversion *fights the drift*: gross hit rate ≈38%, and the edge is **negative even before fees** — gross −4.2 bps/trade, of which ~1.5 bps is just crossing the bid/ask spread. A realistic ~5 bps round-trip fee deepens it to −9.4. The scorecard now accounts for the **full perpetual cost stack** — bid/ask spread, slippage, funding, and mark-price liquidation — and breaks out each cost so nothing hides: micro mean-reversion is a negative-edge game once you pay to trade.

```
TRADING SCORECARD  (long&short mean-reversion, ADAPTIVE (entry 1σ/TP 1.5σ/SL 2.5σ), 1x lev, 2.6 bps/side fee)
Observed price range: [54325.97, 60041.45]  (1052.1 bps span)  |  volatility ~2.61 bps/tick
Market data: spread 1.50–1.50 bps  |  funding -0.00000988/hr (relative, latest)
Round-trips: 471  (307 long / 164 short)  |  liquidations 0
Hit rate (signal accuracy, gross): 38.0% (179/471)   |   net-win rate (after fees): 13.6% (64W/407L)
Capital 10000.00 → equity 8480.85   (-15.19% return on capital)
Net P&L: -1519.15 quote   (gross -4.20 bps/trade, net -9.40 bps/trade after fees)
Avg win +1.28 bps  |  avg loss -11.08 bps  |  profit factor 0.02
Max drawdown 1520.00 quote (15.2%)  |  Sharpe(/trade) -1.28  |  fees 829.55  |  spread cost 1.50 bps/trade  |  avg hold 19.6 ms
Funding: +0.0000 quote total  — a per-hour cost, ≈0 at ms holds; scales with hold time
Risk: Sortino -0.79  |  Calmar -1.00  |  time-in-drawdown 100.0%  |  turnover 159.5x capital
By side: LONG 307 net -10.25 bps  11.4% win  -1135.87 quote   |   SHORT 164 net -7.80 bps  17.7% win  -383.28 quote
→ net LOSS after fees over this run.
```

**Order flow & leverage.** `HFT_USE_FLOW=1` only takes entries that order flow confirms (buy dips into net buying, short rips into net selling), using signed trade volume (buy +, sell −) from the Kraken feed. Sizing is **capital-based with real leverage**: you set `HFT_CAPITAL` and `HFT_RISK_FRAC` (margin per trade as a fraction of equity), equity compounds across trades, and a position is **liquidated** if it moves ≥ `1/leverage` against you (isolated-margin: you can't lose more than the posted margin). At high leverage a negative net edge compounds into ruin fast — the scorecard reports return on capital, liquidations, max drawdown %, and a `RUINED` flag.

```bash
# 50x leverage on $10k — watch a sub-bp gross edge get amplified into a blow-up:
HFT_TRADE=1 HFT_ADAPTIVE=1 HFT_USE_FLOW=1 HFT_LEVERAGE=50 \
  HFT_CAPITAL=10000 HFT_RISK_FRAC=0.1 HFT_FEE_BPS=2.6 make replay
```

Knobs (all env-overridable): `HFT_ENTRY_BPS`, `HFT_TP_BPS`, `HFT_SL_BPS`, `HFT_FEE_BPS` (per side), `HFT_LEVERAGE`, `HFT_CAPITAL`, `HFT_RISK_FRAC`, `HFT_MAX_SIZE_MULT`, `HFT_USE_FLOW`, `HFT_ADAPTIVE`, `HFT_NO_SHORT`. The JSON log gains a `trading` scorecard (capital, final equity, return %, liquidations, ruined), an `equity_curve`, and a `round_trip_log`. **Fees + leverage are the killers** — a sub-bp gross edge that survives at 1× compounds into a blow-up at 50×. See [`CLAUDE.md`](CLAUDE.md#trading-model-hft_trade).

### Realistic perpetual costs (spread, funding, mark, sizing)

On the Kraken Futures feed (`--futures`) the model accounts for the **real costs of trading a perp**, not just the exchange fee — and breaks each one out so nothing hides in the net:

- **Spread-crossing fills.** Entries fill at the **ask**, exits at the **bid** (a long buys the offer and sells the bid; a short mirrors it). The half-spread is paid on each leg and lands in realized P&L. `HFT_SLIPPAGE_BPS` adds extra adverse slippage to model walking the book. The scorecard reports `spread cost X bps/trade`.
- **Funding accrual.** A held position accrues funding from the feed's `relative_funding_rate` (per-hour): a positive rate means **longs pay shorts**, charged continuously over the hold. At the model's ~20 ms holds funding is ≈0 (it's a per-*hour* rate) — it only bites on minute-to-hour holds. `HFT_FUNDING_BPS_PER_HR` overrides the rate for offline testing.
- **Mark-price liquidation.** Liquidation triggers on the perp **mark price** vs entry (not the mid) — the genuine perp reference.
- **Vol-target sizing.** `HFT_VOL_TARGET_BPS` sizes so a stop-loss risks ~N bps of equity (size *down* when volatility is high), bounded by margin capacity (`notional ≤ equity·leverage`). `HFT_MAX_EXPOSURE_MULT` hard-caps notional as a multiple of equity. Both default off — conviction sizing is unchanged.
- **Richer risk metrics.** The scorecard adds **Sortino**, **Calmar**, **time-in-drawdown** (timestamp-weighted), **turnover** (notional ÷ capital), and a **long-vs-short split** — all in the JSON too.

Everything defaults to **behavior-preserving on non-futures feeds** (no bid/ask → fill at mid; no funding rate → no funding). The offline synth emits a synthetic spread + funding rate, so `make replay` exercises the whole stack deterministically. New knobs: `HFT_SLIPPAGE_BPS`, `HFT_FUNDING_BPS_PER_HR`, `HFT_VOL_TARGET_BPS`, `HFT_MAX_EXPOSURE_MULT`.

### Trend-following + cross-market signal

`HFT_MOMENTUM=1` trades **with** the market instead of against it, using a continuous buy/sell signal `S` that blends the traded market's trend + order flow with a **reference market** (basket momentum + lead-lag). It rides the trend but times entries on pullbacks, and exits on signal-flip + trailing stop. The adapter streams two pairs (`--pair` + `--ref-pair`), routed to separate ring buffers via the v3 packet.

```bash
# Two correlated synthetic markets (reference leads); A/B the two models:
./target/release/kraken-feed --synth recordings/two.krkr
HFT_TRADE=1 HFT_MOMENTUM=1 make replay FILE=recordings/two.krkr   # with the trend
HFT_TRADE=1 HFT_ADAPTIVE=1 make replay FILE=recordings/two.krkr   # against (mean-reversion)

# Live: traded + reference pair
HFT_TRADE=1 HFT_MOMENTUM=1 make live PAIR=XBT/USD   # set HFT_REF_PAIR=ETH/USD
```

On a trending tape the difference is directional: going *with* the trend earns a thin **positive gross** edge (+0.2 bps/trade, gross profit factor 1.14 — wins out-size losses) where going *against* it is **negative even before fees** (−2.4 bps/trade, gross PF 0.50). Realistic fees sink both — trend-following just starts on the right side of zero. The JSON exposes `latest_signal_bps`, a `signal_series`, and per-trade `signal_at_entry`. New knobs: `HFT_W_TREND/W_FLOW/W_BASKET/W_LEADLAG`, `HFT_SIGNAL_THR_BPS`, `HFT_PULLBACK_BPS`, `HFT_TRAIL_BPS`, `HFT_REF_PAIR`.

### Backtest sweep & cost-aware execution

The model and backtester share one `AlphaModel` (single source of truth), so the sweep measures exactly what runs live. `trading-engine --backtest <capture>` runs the model over a capture **walk-forward** (continuous warm state; round-trips bucketed in-sample / out-of-sample by time) across a parameter grid, ranked by **out-of-sample** return:

```bash
make sweep                         # synth a capture, sweep, print the OOS-ranked table
HFT_FEE_BPS=2.6 ./target/release/trading-engine --backtest recordings/two.krkr
HFT_MAKER_BPS=-1 ./target/release/trading-engine --backtest recordings/two.krkr   # maker rebate
HFT_NORMALIZE=1  ./target/release/trading-engine --backtest recordings/two.krkr   # z-scored signal
```

Cost-aware knobs (default to prior behavior): `HFT_MAKER` + `HFT_MAKER_BPS` (passive maker entry / rebate, taker exit), `HFT_FEE_GATE` + `HFT_MIN_EDGE_BPS` (only trade when the expected move clears the round-trip cost), `HFT_NORMALIZE` (z-score the signal terms so the cross-market weights matter). The honest result on the synth: taker fees lose, a maker rebate helps, and **z-scoring the signal is what flips the best config out-of-sample-positive** — a bigger lever than the fee. Point it at your recorded live captures for the real verdict.

### Learned policy (the model gives the signal, the algo does the work)

Instead of hand-tuning the signal weights, train a **tiny neural net** on historical captures to produce the buy/sell signal — the same gate / pullback / sizing / trailing-exit machinery then executes it. The net is a **6 → 8 → 1 MLP (65 weights, 260 bytes)**: small enough to live in **L1**, branchless inference (~56 MACs + 8 `tanh`). Training is by **cross-entropy method** — a gradient-free evolutionary search, so still **zero dependencies** (no autodiff, no `rand`). It's walk-forward (train on the first 70% by time, report the held-out last 30% with an overfit flag) and deterministic.

```bash
make train                          # synth a capture, train, write models/policy.bin
HFT_TRADE=1 ./target/release/trading-engine --train recordings/two.krkr   # → HFT_MODEL
# then run the engine with the learned policy:
HFT_EXTERNAL_FEED=1 HFT_TRADE=1 HFT_MOMENTUM=1 HFT_MODEL=models/policy.bin \
    ./target/release/trading-engine &
./target/release/kraken-feed --replay recordings/two.krkr
```

To **score a trained policy** over a long capture instantly (offline, no real-time replay), run `--backtest` *with* `HFT_MODEL` set — it plays the learned policy over the whole capture and prints a scorecard (with a first-half/second-half stability split). Train on one capture, evaluate on different (unseen-seed) captures for a true cross-session out-of-sample read:

```bash
HFT_TRADE=1 HFT_MODEL=models/policy.bin ./target/release/trading-engine --backtest recordings/other-day.krkr
```

Knobs (defaults in `config`): `HFT_MODEL` (weights path), `HFT_POP` / `HFT_GEN` (CEM population / generations), `HFT_SEED`. Unset `HFT_MODEL` and the engine uses the hand-weighted signal — the learned path is purely additive. **Honesty:** CEM on a short capture overfits readily; the trade-count penalty and OOS report make that visible. Train on a long real capture and trust the out-of-sample column.

---

## What isn't here

- **Kernel-bypass networking** — the live feed arrives over loopback UDP from the adapter; the next step for the data path is AF_XDP / DPDK and multicast reception (e.g. CME MDP 3.0).
- **Calibrated signal logic** — the breakout signal is structurally sound and demonstrates the latency budget; the threshold is a tunable placeholder. The `--train` CEM path *does* learn a trading signal (a tiny L1-resident MLP) from captures; folding it onto the register-resident SIMD hot path is the next step.
- **Real exchange connectivity** — the `OrderRing` has the right shape for draining to FIX/OUCH/binary protocol over a real NIC from a dedicated submission thread.
- **Generic x86 fallback** — the x86_64 signal path requires AVX2; there is no runtime SSE/scalar fallback for older CPUs yet, and the affinity core map is tuned for the i9-9900K topology.

---

## Skills demonstrated

- ARM64 NEON and x86_64 AVX2 intrinsics and inline assembly (`std::arch::asm!`) for register-resident signal computation
- Cache-line-aware data structure design (`#[repr(C, align(64))]`, deliberate field ordering for L1 co-location)
- Lock-free concurrent data structures — SPSC ring buffers with `AtomicU64` and `Acquire`/`Release` ordering
- Latency measurement methodology — ns-resolution histograms, percentile reporting, OS stall detection, cross-platform comparison
- Cross-platform systems performance — macOS QOS / Apple Silicon and Linux `SCHED_FIFO` + `sched_setaffinity`, page pre-touch, branch predictor hints, spin-loop vs sleep tradeoffs
- Zero-dependency Rust — hand-rolled JSON output, Gregorian calendar calculation, no serde/chrono/rand

---

## License

Released under the [MIT License](LICENSE).