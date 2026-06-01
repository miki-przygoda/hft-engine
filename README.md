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

Every struct that crosses thread boundaries is `#[repr(C, align(64))]`. `MarketTick` is padded to exactly 64 bytes (`_unused: [u8; 36]`) — one tick per cache line, no false sharing, no partial-line loads.

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
├── lib.rs               # Shared config constants
├── bin/
│   ├── fake-exchange.rs         # Standalone spin-poll UDP exchange (external round-trip measurement)
│   ├── market-simulator.rs      # Standalone UDP packet sender
│   └── kraken-feed.rs           # Live Kraken feed adapter (hand-rolled WebSocket, RTT, record/replay)
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

---

## What isn't here

- **Kernel-bypass networking** — the live feed arrives over loopback UDP from the adapter; the next step for the data path is AF_XDP / DPDK and multicast reception (e.g. CME MDP 3.0).
- **Calibrated signal logic** — the breakout signal is structurally sound and demonstrates the latency budget; the threshold is a tunable placeholder, not calibrated alpha.
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