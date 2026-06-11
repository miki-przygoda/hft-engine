# Architecture & Design — `rust-hft-software`

> A zero-dependency, latency-obsessed high-frequency trading engine written from
> scratch in Rust. This document is the design reference for the codebase: it
> explains *what* the engine does, *how* it's put together, and *why* each
> decision was made in terms of nanoseconds. It is written to be read by anyone
> exploring the repository — and it also serves as the working context document
> for AI-assisted development (hence the filename).

If you just want to build and run it, jump to [Quick start](#quick-start). If you
want to understand the engineering, read on from [Design philosophy](#design-philosophy).

---

## Design philosophy

The single guiding constraint is **latency**: every architectural decision is
evaluated in nanoseconds, not developer ergonomics. The engine is built to make
the *hot path* — the journey from a market tick arriving to an order being
acknowledged — as short and as predictable as physically possible on commodity
hardware.

That goal drives three recurring themes you'll see throughout the code:

1. **No surprises on the hot path.** No heap allocation, no locks, no syscalls,
   no blocking. Everything the trading loop touches is pre-allocated and
   pre-warmed before the loop begins.
2. **Mechanical sympathy.** Data structures are laid out to match the CPU —
   cache-line alignment, deliberate field ordering for L1 co-location, and
   register-resident state that never touches memory between ticks.
3. **Honest measurement.** Latency is recorded at nanosecond resolution with a
   no-allocation histogram, reported as percentiles (not just averages), and
   accompanied by an OS-stall counter so scheduler interference is visible
   rather than hidden in the tail.

This is an educational and research project, but the engineering standards are
production-grade. Where a shortcut was taken (the signal logic, the simulated
feed), it is clearly marked as a placeholder with the right *shape* for the real
thing.

---

## Quick start

```bash
# Build (release is mandatory — the hot path depends on optimisation)
cargo build --release

# Run the self-contained simulation: spawns the feed, exchange, and strategy
cargo run --release --bin trading-engine
```

Each run prints a latency report and writes a JSON log to
`logs/v{version}/{date}/{HH-MM-SS}.json`.

Other binaries:

```bash
cargo run --release --bin bench-one-threaded     # single-core SIMD throughput ceiling
cargo run --release --bin bench-multi-threaded   # all-core stress test ("The Kraken")

# External UDP mode (kernel-path round-trip measurement, 3 processes):
./target/release/fake-exchange &
./target/release/trading-engine &
./target/release/market-simulator
```

> **Platform note.** The full performance path targets macOS on Apple Silicon
> (ARM64 NEON, `QOS_CLASS_USER_INTERACTIVE`). x86_64 Linux is fully supported via
> an AVX2 path plus `SCHED_FIFO` / `sched_setaffinity` (thread priority and
> pinning require `CAP_SYS_NICE` — run with `sudo` to enable them; without it the
> engine still runs, just without elevated scheduling). The engine has no
> external dependencies and `edition = "2024"`.

---

## Project layout

### File map

```
src/
├── main.rs                          # Thread orchestration, buffer pre-touch, startup
├── engine.rs                        # Runtime: ingestor, exchange, watchdog, simulator, strategy, logging
├── models.rs                        # Data structures: MarketTick, RingBuffer, TradeLog, LatencyHistogram, OrderBook, OrderRing
├── model.rs                         # AlphaModel + learned Policy (tiny MLP): signal + execution (shared by live, --backtest, --train)
├── lib.rs                           # Shared config constants (rust_hft_software::config)
├── bin/
│   ├── market-simulator.rs          # Standalone UDP packet sender (warmup + real packets)
│   ├── fake-exchange.rs             # Standalone spin-poll UDP exchange, echoes orders as confirms
│   └── kraken-feed.rs               # Live Kraken feed adapter (hand-rolled WebSocket, RTT, record/replay)
└── testing_scripts/
    ├── mod.rs                       # Declares one_threaded and multi_threaded submodules
    ├── one_threaded.rs              # Single-threaded SIMD throughput benchmark (x86 + ARM)
    └── multi_threaded.rs            # Multi-threaded all-core stress benchmark ("The Kraken")

CLAUDE.md                            # This file — architecture & design reference
README.md                            # Public overview, measured latency tables
LICENSE                              # MIT license
CONTRIBUTING.md                      # How to build, test, and contribute
Makefile                             # Common tasks: build/test/run/replay/live/bench
scripts/
├── replay.sh                        # Offline: replay a capture through the engine
└── live.sh                          # Live: stream real Kraken trades (needs stunnel)
docs/
└── stunnel.conf                     # Example TLS terminator config for the live Kraken feed
.github/
├── workflows/ci.yml                 # CI: build (hard gate) + clippy matrix on macOS and Linux
└── ISSUE_TEMPLATE/
    ├── bug_report.md                # Bug report template
    └── feature_request.md           # Feature request template
```

Binaries declared in `Cargo.toml`:

| Binary | Source | Purpose |
|--------|--------|---------|
| `trading-engine` | `src/main.rs` | Self-contained in-process simulation |
| `bench-one-threaded` | `src/testing_scripts/one_threaded.rs` | Single-core SIMD ceiling |
| `bench-multi-threaded` | `src/testing_scripts/multi_threaded.rs` | All-core stress test |

`src/bin/market-simulator.rs` and `src/bin/fake-exchange.rs` are auto-discovered
binaries used for external (kernel-path) UDP testing. The `testing_scripts`
module is gated behind the `testing` Cargo feature when compiled as part of the
library.

---

## How it works (the 10,000-foot view)

The `trading-engine` binary is a complete, self-contained simulation. A single
process spawns five threads that pass data through three lock-free shared
buffers:

```
  ┌────────────┐   UDP    ┌────────────┐  RingBuffer  ┌────────────┐  OrderRing  ┌────────────┐
  │ simulator  │ ───────▶ │  ingestor  │ ───────────▶ │  strategy  │ ──────────▶ │  exchange  │
  │ (burst gen)│  :34254  │ (recv→ring)│  (SPSC)      │ (hot path) │  (SPSC)     │ (confirm)  │
  └────────────┘          └────────────┘              └─────┬──────┘             └─────┬──────┘
                                                            │ TradeLog                 │
                                                            ▼  (single-writer)         │
                                                      ┌──────────────┐                 │
                                                      │ latency log + │◀────────────────┘
                                                      │  histograms   │  round_trip_ns written back
                                                      └──────────────┘
        ┌────────────┐
        │  watchdog  │  spins; detects idle / no-feed; prints stats; writes JSON; exits
        └────────────┘
```

1. The **simulator** waits for the ingestor to bind, then sends warmup packets
   followed by bursts of market ticks over UDP loopback.
2. The **ingestor** spin-polls the socket, copies each tick into the
   `RingBuffer`, stamps it with an ingest timestamp, and publishes it by
   bumping an atomic cursor (`Release`).
3. The **strategy** spin-polls that cursor (`Acquire`), runs a register-resident
   momentum signal, and on a trigger writes a `TradeExecution` into the
   `TradeLog` and an order into the `OrderRing`.
4. The **exchange** spin-polls the `OrderRing`, reads a confirmation timestamp,
   and writes `round_trip_ns` back into the corresponding trade-log slot.
5. The **watchdog** spins at low priority, watching for idle/no-feed conditions;
   when the run ends it prints the latency report, writes the JSON log, and
   exits the process.

The defining choice is the **in-process exchange**: the round trip
(order → confirmation → timestamp) crosses zero kernel boundaries. That is why
in-process round-trip latency (tens of nanoseconds) is ~163× lower than the
external-UDP path (tens of microseconds). The external path exists only so that
the kernel cost can be measured deliberately.

---

## Thread model (`src/main.rs`)

| Thread | Function | Scheduling | Affinity tag |
|--------|----------|------------|--------------|
| Watchdog | `run_watchdog` — idle detection, shutdown, reporting | default | none |
| Exchange | `run_in_process_exchange` — order confirmation | elevated¹ | 3 |
| Ingestor | `run_ingestor` — UDP recv → ring buffer | elevated¹ | 2 |
| Simulator | `run_market_simulator` — internal market feed | default | none |
| Strategy (main) | `trading_strategy` — the hot path | elevated¹ | 1 |

¹ *Elevated* = `QOS_CLASS_USER_INTERACTIVE` (P-core bias) on macOS, or
`SCHED_FIFO` priority 50 on Linux. The watchdog and simulator deliberately stay
at default priority so they cannot preempt the critical threads.

The strategy thread spins on two `AtomicBool` greenlights (`ingestor_ready`,
`exchange_ready`) before entering the trading loop. The main thread joins the
strategy thread; the watchdog calls `std::process::exit` when the run completes.

### Buffer pre-touch (before any thread is spawned)

`std::mem::zeroed()` on a fresh heap allocation does **not** commit physical
pages on macOS (zero-fill-on-demand). Without forcing commitment, the first real
write to any shared buffer would trigger a page fault (~3–5 µs) — right in the
middle of the hot path. So `main()` walks every cache line of all three buffers
with `write_volatile` before spawning threads:

```rust
// RingBuffer ticks — one u64 write per 8-u64 tick
// OrderRing entries — one u64 write per 8-u64 entry
// TradeLog entries  — one u64 write per 6-u64 TradeExecution
```

The step sizes (`8`, `8`, `6`) match the field counts of each struct. This both
commits the OS pages and pulls the lines into cache.

### Thread priority & affinity APIs

- **macOS:** `pthread_set_qos_class_self_np(0x21, 0)` (Apple-private QOS API)
  biases a thread toward the P-cores. `thread_policy_set` with
  `THREAD_AFFINITY_POLICY` provides a *grouping hint* (not a hard pin) — the
  scheduler tries to co-schedule same-tagged threads on one cluster.
- **Linux:** raw `sched_setscheduler` syscall sets `SCHED_FIFO`; raw
  `sched_setaffinity` pins each critical thread to a dedicated core (strategy→2,
  ingestor→3, exchange→4, leaving 0–1 for the OS). Both are issued as inline-asm
  syscalls to avoid a libc dependency and require `CAP_SYS_NICE`.

Hard core *pinning* on Apple Silicon was tested and produced **worse** results —
the scheduler outperforms forced single-core assignment under thermal load — so
the macOS path uses only the soft grouping hint.

---

## Data structures (`src/models.rs`)

All cross-thread structures are `#[repr(C, align(64))]` and built around
single-producer / single-consumer (SPSC) or single-writer lock-free protocols.

### `MarketTick` — exactly one cache line

```rust
#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price:          f32,  // offset  0
    pub(crate) volume:         f32,  // offset  4
    pub(crate) sequence:       u64,  // offset  8
    pub(crate) timestamp:      u64,  // offset 16 — ingest time (ns since engine start)
    pub(crate) origin_ts_ns:   u64,  // offset 24 — exchange trade time (ns since epoch)
    pub(crate) transit_est_ns: u64,  // offset 32 — RTT/2 one-way transit estimate
    pub(crate) bid:            f32,  // offset 40 — best bid (v4 futures feed; 0 otherwise)
    pub(crate) ask:            f32,  // offset 44 — best ask (v4)
    pub(crate) mark_price:     f32,  // offset 48 — perp mark price (v4)
    pub(crate) funding_rate:   f32,  // offset 52 — relative funding rate, per hr (v4)
    _unused: [u8; 8],                // padding to 64 bytes
}
```

64 bytes exactly — one tick per cache line, no false sharing, no partial-line
loads. The ingestor writes `timestamp` before publishing via the ring cursor;
the strategy reads it after the matching acquire load, so visibility is
guaranteed by the acquire/release pair. The first 16 bytes are still
price/volume/sequence, so legacy 16-byte packets still populate the tick; the
live feed adds `origin_ts_ns` and `transit_est_ns` (see *Live data feed* below).

### `RingBuffer` — SPSC tick delivery

```rust
#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks:      [MarketTick; BUFFER_SIZE],  // 65 536 bytes
    pub(crate) latest_idx: AtomicU64,                  // offset 65536 — cache-line boundary
    pub(crate) start_time: Instant,                    // offset 65544 — SAME cache line
}
```

**The `latest_idx` / `start_time` co-location is load-bearing.** The strategy
loads `latest_idx` on every spin iteration; `start_time` sits 8 bytes later in
the *same* cache line and is therefore always L1-hot — every `elapsed_ns()` call
gets its clock anchor for free. Inserting any field between them breaks this.

### `TradeExecution` & `TradeLog` — single-writer latency log

```rust
#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence:       u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns:    u64,
    pub latency_ns:     u64,   // buy_time_ns - ingest_time_ns (signal latency)
    pub order_send_ns:  u64,   // when the order was pushed to the OrderRing
    pub round_trip_ns:  u64,   // confirm_recv_ns - order_send_ns (written by exchange)
    pub transit_est_ns: u64,   // RTT/2 transit estimate, copied from the tick
    pub target_price:   f32,   // intended buy price (config target, or entry price)
    pub fill_price:     f32,   // market price one transit later (0.0 = unfilled)
}
```

64 bytes (7×u64 + 2×f32 = one cache line). **Write protocol:** the strategy fills
all fields, then commits with `fetch_add(1, Release)` on the log's `write_idx`.
**Read protocol:** `load(Acquire)` to get the committed count, then read
`[0..count]`. The exchange thread writes only `round_trip_ns`; the strategy writes
`fill_price` later (when the simulated fill comes due) — distinct fields, distinct
memory locations, so there is never a conflicting write. See *Execution model*.

### `LatencyHistogram` — no-allocation percentile recording

```rust
pub(crate) struct LatencyHistogram {
    pub(crate) buckets:  UnsafeCell<[u64; 10_001]>,  // one bucket per ns, 0–10 000 ns
    pub(crate) overflow: AtomicU64,                  // ≥ 10 001 ns
}
```

Records a value by incrementing a bucket (or the overflow counter) — no sort, no
allocation, in the hot path or at shutdown. Percentiles are a single linear walk
of the bucket array. Single-writer: the strategy owns `sig_hist`, the exchange
owns `rt_hist`; reads happen only at shutdown after all hot-path activity stops.

### `OrderBook` — shared run state

Holds the `trade_log`, both histograms, and the run's bookkeeping atomics:
`stall_count` (idle-spin gaps > 500 ns), `gap_count` (sequence gaps), `dirty`
(set by ingestor on a gap, cleared by the strategy after N clean ticks), `halt`
(permanent risk kill-switch), `net_position`, memory-snapshot fields, the
execution/slippage counters and observed price range, the `TradeCfg`, and the
`RoundTripLog` (completed round-trips for the trading-model scorecard — a
single-writer log of 64-byte `RoundTrip` records, same protocol as `TradeLog`).

### `OrderRing` & `OrderEntry` — SPSC order submission

```rust
#[repr(C, align(64))]
pub(crate) struct OrderEntry {
    pub(crate) sequence:      u64,
    pub(crate) slot:          u64,   // trade-log index for O(1) confirm lookup
    pub(crate) order_send_ns: u64,
    _pad: [u8; 40],                  // padding to 64 bytes
}
```

The strategy is the sole writer, the exchange the sole reader. The `slot` field
lets the exchange index straight into the trade log to write `round_trip_ns` —
no scanning.

### Multi-instrument scaffold

`InstrumentId(u8)` and `InstrumentBuffers { buffers: [RingBuffer; MAX_INSTRUMENTS] }`
are the pre-allocated, flat-array (no `HashMap`, no hot-path allocation) shape
for multi-instrument support. Only slot 0 is wired up today; the rest are
reserved for when a real feed provides instrument IDs.

---

## The hot path (`trading_strategy`)

### Startup sequence

1. **NEON / AVX2 warmup** — 10 000 iterations of a vector multiply plus an
   `elapsed_ns()` call, both passed through `black_box`. Warms the vector
   execution units, pulls hot-path code into the instruction cache, and commits
   the `start_time` cache line.
2. **Warmup packets** — `WARMUP_PACKETS = 10`. The first 10 sequences run the
   full hot path (signal, timing, ring reads) but skip the trade-log commit.
   This trains the branch predictor and warms every hot-path cache line without
   polluting the latency measurements. The value lives in
   `rust_hft_software::config` and is shared by the engine and the standalone
   simulator — they must agree.

### The spin-poll loop

```rust
loop {
    let current_seq = buffer.latest_idx.load(Ordering::Acquire);
    if current_seq > last_processed_seq {
        // hot path: signal → (risk checks) → trade-log write → order-ring push
    } else {
        // idle: stall detection, then YIELD + prefetch next trade-log slot
        std::hint::spin_loop();
    }
}
```

No sleep, no condvar, no mutex — ever. `spin_loop()` emits `YIELD` on ARM64 /
`PAUSE` on x86 so the busy-wait doesn't burn full pipeline resources. In the idle
branch the loop prefetches the next trade-log write slot into L1 in exclusive
(store-ready) state (`PRFM PSTL1KEEP` on ARM64, `_mm_prefetch(_MM_HINT_ET0)` on
x86) so the line is hot when the next tick lands. The tick buffer itself is
deliberately *not* prefetched from the strategy — the ingestor writes to it, and
a load-prefetch from the consumer would cause coherence traffic.

### The signal: a register-resident breakout window

An 8-price sliding window lives entirely in vector registers across loop
iterations — no memory access for window state between ticks. The rule is a
**breakout**: trigger when the new price exceeds the *maximum* of the previous 8
ticks by `SIGNAL_MOMENTUM_BPS` basis points (a configurable threshold in
`config`; default 10 bps).

- **ARM64 (NEON):** the window is a `float32x4_t` pair (`win_lo` / `win_hi`)
  bound via `inout(vreg)`. Each tick: `FMAX` + `FMAXV` reduce the previous 8
  prices to their max, two `EXT`s slide the new price in, and `FCMGT` compares
  the current price to `prev_max × (1 + bps)` — the result bit is the trigger.
- **x86_64 (AVX2):** the window is a single `__m256` (8×f32 = 256 bits).
  `vmaxps` + `vpermilps` reduce the previous 8 to their max; `vextractf128` +
  `vpalignr` + `vinsertf128` shift the new price in; `vucomiss` + `seta` produce
  the branchless 0/1 trigger.

This is a more defensible momentum rule than a bare mean comparison, but still a
demonstration signal — it shows real signal computation fits inside the latency
budget, not a calibrated trading strategy. The threshold is the one tunable knob.

### Risk management

Before committing a trade the strategy checks (all `Relaxed` loads, branching on
registers): a permanent `halt` flag, a `net_position` ceiling (`MAX_POSITION`),
and a cumulative-gap kill switch (`gap_count > MAX_GAP_COUNT`). The
`halt_trading` function is marked `#[cold]` so the branch predictor biases the
hot path toward *not* halting after warmup. When the ingestor flags a sequence
gap (`dirty`), the strategy pauses trading until `CLEAN_SEQ_THRESHOLD`
consecutive clean ticks have arrived.

### Round-trip measurement (in-process)

1. Strategy fills a `TradeExecution` (including `order_send_ns`) and commits with
   `fetch_add(Release)`.
2. Strategy writes an `OrderEntry` (sequence, slot, `order_send_ns`) to the
   `OrderRing` and commits with `fetch_add(Release)`.
3. The exchange thread spin-polls the ring, reads
   `confirm_recv_ns = elapsed_ns(&start_time)`, and writes
   `round_trip_ns = confirm_recv_ns - order_send_ns` directly into the trade-log
   slot via the `slot` index.

No kernel crossings — the entire round trip is userspace shared memory.

---

## Live data feed (`src/bin/kraken-feed.rs`)

The engine can run on real market data via the `kraken-feed` adapter, which
re-emits live Kraken trades as the engine's UDP packets. This lets the engine
measure the **full reaction stack** on real data: how long the data spent in
flight, then how fast the engine reacts.

### Packet format v2 (32 bytes, little-endian)

The first 16 bytes are byte-identical to the legacy packet (so old senders keep
working); the extra 16 carry feed metadata:

```
[ 0.. 4] price f32      [ 4.. 8] volume f32 (signed: buy +, sell −)  [ 8..16] sequence u64
[16..24] origin_ts_ns   (exchange trade time, ns since epoch — informational)
[24..32] transit_est_ns (RTT/2 one-way transit estimate — the distance metric)
[  32  ] instrument u8  (v3 only: 0 = traded, 1 = reference — for cross-market)
```

The ingestor parses the extra fields only when it receives ≥ 32 bytes; 16-byte
packets leave them zero. The **v3 packet (33 bytes)** appends a 1-byte instrument
id so a second market (the cross-market reference) routes to its own ring buffer;
< 33-byte packets are instrument 0. `volume` carries *signed* trade volume (buy +,
sell −) — the order-flow input.

### Zero dependencies, TLS by proxy

Kraken requires `wss://` (TLS). Rather than link a TLS crate (which would break
the zero-dependency invariant), TLS is terminated by a local **stunnel**
instance, and `kraken-feed` speaks plaintext TCP to it while implementing the
WebSocket protocol *by hand*: the HTTP/1.1 `Upgrade` handshake (with hand-rolled
SHA-1 + base64 for `Sec-WebSocket-Accept`), RFC6455 frame parse/build with
client-side masking, and ping/pong. stunnel is an external system tool, **not** a
cargo dependency. See `docs/stunnel.conf`.

### Latency stages reported

The shutdown report (console + JSON) now breaks the stack into four stages:

1. **Transit (RTT/2)** — source → local arrival, estimated from WebSocket
   ping/pong. *Milliseconds*, so it's reported in µs and computed from the trade
   array at shutdown (it's outside the 0–10,000 ns `LatencyHistogram` range).
2. **Signal latency** — ingest → order send (existing `sig_hist`). *Nanoseconds*.
3. **Round trip** — order send → confirm (existing `rt_hist`). *Nanoseconds*.
4. **End-to-end** — the sum, per trade.

The headline contrast: the data spends *milliseconds* reaching us, while the
engine reacts in *hundreds of nanoseconds* — the ns stages are a rounding error
against transit.

### Record / replay / history

`--record FILE` captures the live feed (packets + inter-arrival timing);
`--replay FILE` re-emits it deterministically with no network — the way to run
and verify offline. `--synth FILE` fabricates a capture (`HFT_SYNTH_TICKS` sets
the length — use a large value for a long "day" session). `--history --hours N
--out FILE` pulls **real historical trades** from Kraken's REST `Trades` endpoint
(paginated via the `last` cursor, both `--pair` and `--ref-pair`, interleaved by
time) through a second stunnel service → `api.kraken.com:443`; transit is 0 (no
live RTT). Set `HFT_EXTERNAL_FEED=1` when running `trading-engine` so the internal
simulator is disabled and the external feed drives the ingestor alone.

```bash
# Offline (no network):
./target/release/kraken-feed --synth recordings/sample.krkr
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --replay recordings/sample.krkr

# Live (needs stunnel — see docs/stunnel.conf):
stunnel docs/stunnel.conf &
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --live 127.0.0.1:8443 --pair XBT/USD --record recordings/live.krkr
```

---

## Execution model (target price & slippage)

The point of the live feed is to measure not just *how fast* the engine reacts but
*what that speed is worth* — how far the price moves against you in the latency gap.

- **Downtick mode** (`HFT_DOWNTICK=1`, highest priority): buy on any price
  decrease. Guaranteed to fire on any feed that moves at all — the fallback for
  very flat/thin markets (a quiet pair can move < 1 bps in 30 s, below any dip
  threshold).
- **Relative-dip mode** (`HFT_TARGET_DIP_BPS=<bps>`): buy on a dip of N bps below
  a rolling EMA reference. Adapts to any absolute price level, so it fires on real
  data without knowing the market price up front (a re-arming detector waits for
  recovery to the reference before firing again).
- **Target-price mode** (`HFT_TARGET_PRICE=<price>`): buy each time the price dips
  down through a fixed target (a re-arming downward cross). Only fires when the
  price crosses the level *from above*, so the target must sit just below the
  current market — otherwise 0 attempts (the report prints the observed price range
  to calibrate against). Unset / `0`, and no dip set → breakout mode.
- **Deferred fill:** when an order is sent we don't yet know the fill price — it's
  the market price one transit (RTT/2) later. Each attempt is pushed onto a small
  FIFO of pending fills; a later tick whose timestamp passes the due time
  (`order_send_ns + transit_est_ns`) supplies the `fill_price`. Orders still in
  flight at shutdown are reported as *pending*.
- **Slippage** = `fill_price − target_price`, reported in basis points
  (signed mean, plus `|slip|` p50/p95). Positive = filled above the intended price
  (adverse for a buy). In breakout mode the reference is the entry price, so it
  measures the same thing: how far price drifted during the latency gap.
- The shutdown report adds an **execution block** (target, attempts/filled/pending,
  observed price range, slippage) and the JSON gains `target_price`, `attempts`,
  `filled`, `pending`, `price_range`, `slippage_bps`, and per-trade
  `target_price`/`fill_price`/`slippage_bps`. The observed price range is tracked
  by the ingestor (sole writer) so an empty run still tells you where to set the
  target.

`HFT_EXTERNAL_FEED=1` disables the internal simulator so the live/replay feed
drives the ingestor alone.

---

## Trading model (`HFT_TRADE`)

With `HFT_TRADE=1` the strategy runs a closed-loop **long & short mean-reversion**
book instead of one-sided buy attempts, and scores its P&L.

- **Entry:** long when `price ≤ ref·(1−entry_bps)`, short when `price ≥ ref·(1+entry_bps)`,
  where `ref` is the EMA reference (`α = 1/64`). Position **size scales with the dip
  depth** (`depth/entry_bps`, clamped to `max_size_mult`) — the "dynamic, maximise
  output" lever. `HFT_NO_SHORT` makes it long-only.
- **Adaptive thresholds** (`HFT_ADAPTIVE=1`): instead of fixed bps, entry/TP/SL are
  multiples of a rolling volatility estimate (EMA of |per-tick return|): entry 1σ,
  TP 1.5σ, SL 2.5σ. This auto-calibrates to whatever the market is doing, so it
  fires in flat tapes too — the honest result being that micro-moves rarely beat
  the fee. The scorecard reports the realized σ (bps/tick).
- **Exit:** take-profit (`+tp_bps`), stop-loss (`−sl_bps`), or the opposite signal,
  whichever comes first. Fills at the observed price; latency slippage is reported
  separately (it's ~sub-bp, dwarfed by the fee).
- **Order flow** (`HFT_USE_FLOW=1`): entries must be confirmed by an order-flow EMA
  (signed trade volume — buy +, sell − — carried in the tick's `volume` field). Long
  dips are only taken into net buying, short rips into net selling. The Kraken
  adapter signs volume from the trade `side`; the synth generates flow correlated
  with the reversion so it's a *noisy* leading signal.
- **Leverage & capital:** sizing is capital-based. `margin = risk_frac·equity`,
  `notional = margin·leverage`, and equity **compounds**. A position is **liquidated**
  when it moves ≥ `1/leverage` against you, capped at the posted margin
  (isolated-margin — you can't lose more than that). `HFT_CAPITAL`, `HFT_RISK_FRAC`,
  `HFT_LEVERAGE` are the inputs; the scorecard reports return on capital, max
  drawdown %, liquidations, and a ruin flag.
- **Accounting:** each round-trip records `gross_bps`, `net_bps` (gross − 2·fee),
  `pnl_quote` (notional·move − fees), a liquidation flag, and hold time into a
  single-writer `RoundTripLog` (64-byte `RoundTrip`, like `TradeLog`). Entries and
  exits are also emitted as orders so signal-latency and round-trip stages populate.
- **Scorecard** (console + JSON, from the round-trip array at shutdown): round-trips,
  long/short split, win rate, liquidations, capital → final equity & return %, net
  P&L (bps & quote), avg win/loss, profit factor, max drawdown (quote & %), Sharpe
  (per-trade), avg hold, ruin flag. JSON also gets an `equity_curve` and `round_trip_log`.
- **Config** (env-overridable; defaults in `config`): `HFT_ENTRY_BPS`, `HFT_TP_BPS`,
  `HFT_SL_BPS`, `HFT_FEE_BPS` (per side), `HFT_LEVERAGE`, `HFT_CAPITAL`,
  `HFT_RISK_FRAC`, `HFT_MAX_SIZE_MULT`, `HFT_USE_FLOW`, `HFT_ADAPTIVE`, `HFT_NO_SHORT`.

---

## Trend-following + cross-market signal (`HFT_MOMENTUM`)

With `HFT_TRADE=1 HFT_MOMENTUM=1` the strategy trades **with the trend** using a
continuous buy/sell signal that blends the traded market and one **reference
market** (slot 1), instead of fading dislocations. This exists because the data
said so: on a trending tape, mean-reversion (fading) gets destroyed while
momentum (riding) roughly breaks even.

- **Composite signal** `S` (bps), recomputed every tick and exposed via
  `OrderBook.latest_signal_bits`:
  `S = w_trend·own_trend + w_flow·flow + w_basket·ref_trend + w_leadlag·ref_return`.
  `own_trend`/`ref_trend` = `(fastEMA−slowEMA)/slowEMA` (α 1/16 vs 1/128); `flow`
  is the normalized order-flow EMA; `ref_return` is the reference's recent return
  (it **leads**). The reference is sampled read-only across its own cursor.
- **Entry — trend + pullback:** when `S > +thr` (bullish) buy a short-term dip
  below the fast EMA by `pullback_bps`; when `S < −thr` (bearish) short a rip.
  Size scales with `|S|` conviction. Capital/leverage/liquidation as in HFT_TRADE.
- **Exit — signal-flip + trailing:** exit when `S` flips, or a trailing stop
  retraces `trail_bps` from the best price since entry; TP/SL/liquidation are caps.
- **Two instruments:** the adapter subscribes to `--pair` (id 0) + `--ref-pair`
  (id 1); the ingestor routes by the v3 packet's instrument byte to two ring
  buffers; the strategy spins on slot 0 and read-snapshots slot 1. The
  order-ring/exchange/trade-log stay tied to slot 0.
- **Signal as a first-class output:** the JSON `trading` block adds the weights +
  `latest_signal_bps`, a downsampled `signal_series`, and per-trade
  `signal_at_entry` — so the buy/sell call is inspectable at any point.
- **Config:** `HFT_MOMENTUM`, `HFT_W_TREND/W_FLOW/W_BASKET/W_LEADLAG`,
  `HFT_SIGNAL_THR_BPS`, `HFT_PULLBACK_BPS`, `HFT_TRAIL_BPS`, `HFT_SIGNAL_EXIT_BPS`,
  `HFT_BETA`, and `--ref-pair` / `HFT_REF_PAIR` (adapter).

`kraken-feed --synth` writes **two correlated markets** (a shared trending latent
factor; the reference leads the traded market by a lag) so the basket and lead-lag
terms have genuine predictive value offline. The mean-reversion default (`HFT_TRADE`
without `HFT_MOMENTUM`) still trades a single-market OU walk; flipping `HFT_MOMENTUM`
on the same capture is the honest A/B (momentum ~58% hit / ~break-even vs
mean-reversion ~30% hit / deep loss on a trend).

---

## Backtest harness + cost-aware execution

The trade model lost because fees structurally exceed the edge, and we'd only ever
tested in-sample. Two pieces address that:

### `AlphaModel` — single source of truth (`src/model.rs`)
The HFT_MOMENTUM signal + execution is factored into `AlphaModel`
(`on_reference_tick` / `on_traded_tick` → `Decision`). The live `trading_strategy`
calls it and applies side effects (latency order, round-trip log, `net_position`,
signal logging); the backtester calls the **identical** model. The previous inline
logic moved verbatim — a seeded replay produces the same scorecard before/after.

### Backtest / sweep — `trading-engine --backtest <capture.krkr>`
Offline (no threads/UDP/sleeps): loads a capture, runs `AlphaModel` over it
**continuously** (warm EMAs, no future leakage), and **walk-forward** buckets the
round-trips into in-sample (first 70% by time) and out-of-sample (last 30%). It
sweeps a small grid (maker on/off × signal-threshold × trailing × fee-gate) and
prints the configs **ranked by OOS return** with an overfit flag. `make sweep`.

### Cost-aware knobs (all default to the prior behavior when off)
- `HFT_MAKER` + `HFT_MAKER_BPS`: model a **passive (maker) entry** — you post a
  limit at the dip/pullback and pay the maker fee (often a negative rebate) while
  the exit crosses (taker = `HFT_FEE_BPS`). Round-trip cost = maker + taker.
  *(Models the fee, not queue/fill realism — fill-when-crossed is a follow-up.)*
- `HFT_FEE_GATE` + `HFT_MIN_EDGE_BPS`: only enter when the **expected move
  (~`max(tp, 3σ)`) clears the round-trip cost** + buffer — kills doomed trades.
- `HFT_NORMALIZE`: **z-score** the composite-signal terms (each / its rolling
  |value|, re-expressed in σ units) so `w_flow`/`w_basket`/`w_leadlag` actually
  matter instead of being swamped by raw `trend_bps`.

On the realistic AR(1)-trend synth the sweep is honest: taker fees lose
(best OOS ≈ −0.4%), a maker rebate helps (≈ −0.3%), zero-fee barely breaks even
(weak gross edge) — and **z-scoring the signal is the lever that flips the best
config OOS-positive**, a bigger effect than the fee. The harness's real use is
pointing it at the user's recorded *live* captures.

---

## Learned policy (`HFT_MODEL` / `--train`)

Instead of hand-tuning the composite-signal weights, the signal `S` can be
produced by a **tiny neural net trained offline on historical captures**. The net
gives the buy/sell call; the same gate / pullback-timing / sizing / trailing-exit
machinery (above) does the rest — *"the model gives the signal, the algo does the
work."* It's deliberately small enough to be **L1-resident** so it costs nothing on
the hot path.

### The policy (`src/model.rs::Policy`)
A **6 → 8 → 1 MLP**: 6 standardized features → 8-unit `tanh` hidden layer → 1
linear output (the signal `S`, in bps). That's **65 `f32` weights = 260 bytes** —
it fits in L1, and inference is ~56 multiply-adds + 8 `tanh`, fully branchless. The
six features (each standardized by a rolling `|value|` EMA so the net sees
O(1)-scaled inputs in any price/vol regime) are: own trend, pullback vs the fast
EMA, normalized order flow, reference trend, reference lead-lag return, and
realized vol. When `AlphaModel` holds a `Policy`, `S = policy.forward(features)`
*replaces* the hand-weighted composite; everything downstream is unchanged.

### Training — `trading-engine --train <capture.krkr>`
Training is by **cross-entropy method (CEM)** — a gradient-free evolutionary
search, so there's no autodiff and still **zero dependencies**. Keep a Gaussian
over the 65-weight vector; each generation sample a population, keep the elite
(best-fitness) fraction, refit the Gaussian, repeat. Fitness is a **Sharpe-like**
score with a quadratic penalty below 30 trades (a high Sharpe over a handful of
trades is noise, not edge). Walk-forward: train on the first 70% by time, report
the held-out last 30% (with an overfit flag). The best policy is persisted as raw
little-endian `f32`. Deterministic (seeded) → reproducible runs.

```bash
# Train over a capture, then run the engine with the learned policy:
HFT_TRADE=1 ./target/release/trading-engine --train recordings/two.krkr   # → models/policy.bin
HFT_EXTERNAL_FEED=1 HFT_TRADE=1 HFT_MOMENTUM=1 HFT_MODEL=models/policy.bin \
    ./target/release/trading-engine &
./target/release/kraken-feed --replay recordings/two.krkr
# or: make train         (synth + train)
```

### Evaluating a trained policy — `--backtest` with `HFT_MODEL`
`--replay` re-emits at the recorded inter-arrival cadence, so a multi-day capture
takes *hours* of wall-time. To score a trained policy over a long capture
**instantly and offline**, run `--backtest <capture>` *with* `HFT_MODEL` set: the
engine plays the learned policy over the whole capture at full speed and prints a
scorecard (plus a first-half/second-half split to show the edge is stable, not
front-loaded). Train on one capture, evaluate on *different* (unseen-seed)
captures for a true cross-session out-of-sample read. Without `HFT_MODEL`,
`--backtest` falls back to the hand-weighted config sweep.

**Config** (env-overridable; defaults in `config`): `HFT_MODEL` (weights path —
load at startup, write target for `--train`), `HFT_POP` (population, 256),
`HFT_GEN` (generations, 40), `HFT_SEED` (RNG seed). If `HFT_MODEL` is unset the
engine uses the hand-weighted signal as before — the learned path is purely
additive.

> **Honesty.** CEM on a short capture overfits readily (high in-sample Sharpe over
> a few trades); the trade-count penalty and the out-of-sample report exist to make
> that visible rather than hide it. As with the rest of the project, the value is
> the *measurement* — point `--train` at a long real capture and trust the OOS
> column, not the in-sample one.

---

## Timing primitive

`elapsed_ns(start: &Instant) -> u64` is the single source of time, calling
`start.elapsed().as_nanos() as u64`. On Apple Silicon `Instant::elapsed()` uses
the commpage fast path; a direct `mach_absolute_time()` FFI version was measured
to be ~42× worse *and* returns 24 MHz ticks (requiring a fragile `× 125 / 3`
conversion). Do not replace it.

---

## Run logging (`write_log`)

After each run the engine writes `logs/v{version}/{date}/{HH-MM-SS}.json`:

- Version from `env!("CARGO_PKG_VERSION")` (tracks `Cargo.toml` automatically).
- Date/time from a stdlib-only Gregorian-calendar calculation — **no `chrono`**.
- JSON built by manual string formatting — **no `serde`**.
- Contents: version, timestamp, total trades, net position, halt state, stall &
  gap counts, memory snapshots, per-stage latency percentiles
  (avg/min/max/p50/p95/p99/p99.9) for signal latency, round trip, **transit**, and
  **end-to-end**, the **execution block** (target price, attempts/filled/pending,
  observed price range, **slippage** in bps), and the full per-trade array (each
  trade carries `transit_est_ns`, `end_to_end_ns`, `target_price`, `fill_price`,
  and `slippage_bps`).

---

## Latency methodology & observed numbers

The OS-stall counter (idle-spin gaps > 500 ns) is the honest proxy for scheduler
interference and is reported every run alongside the percentiles. The minimum
latency floor is bounded by L2-miss latency at the inter-burst gaps in the
simulation; at higher tick rates the cache stays warmer and the floor drops.

Measured, in-process simulation (see `README.md` for the full tables):

| Metric | Min | p50 | p99 | Notes |
|--------|-----|-----|-----|-------|
| Signal latency (M3 Max) | 41 ns | 125 ns | 458 ns | NEON + P-core cluster |
| Round trip (M3 Max) | 41 ns | 84 ns | 3 458 ns | rare-but-long macOS scheduling spikes |
| Signal latency (i9-9900K) | 89 ns | 118 ns | 150 ns | AVX2 |
| Round trip (i9-9900K) | 92 ns | 108 ns | 199 ns | tighter tail discipline on Linux |

External UDP mode (3 processes, kernel boundaries): **43–135 µs** — ~163× the
in-process path. That gap is the architectural thesis.

---

## Benchmarking tools (`src/testing_scripts/`)

- **`one_threaded.rs`** — runtime SIMD detection (`is_x86_feature_detected!` for
  AVX-512/AVX2/SSE; `cfg`-gated NEON for ARM64). Runs ~1 billion float
  multiplies and reports Gops/s — the single-core hardware ceiling the trading
  loop cannot exceed.
- **`multi_threaded.rs` ("The Kraken")** — progressive thermal ramp spinning up
  threads over ~10 s, then full parallel NEON execution across all cores.
  Measures aggregate wall-clock throughput and per-op latency.

---

## Platform support

| Platform | Status | Path |
|----------|--------|------|
| macOS, Apple Silicon (ARM64) | Primary target | NEON signal, QOS P-core bias |
| Linux, x86_64 | Supported | AVX2 signal, `SCHED_FIFO` + affinity (`CAP_SYS_NICE`) |
| Windows | Not supported | OS scheduler overhead incompatible with the targets |

The `one_threaded.rs` benchmark is the only file with cross-platform SIMD
dispatch; the engine's NEON and AVX2 paths are selected with
`#[cfg(target_arch = …)]`. macOS-private APIs (`pthread_set_qos_class_self_np`,
`thread_policy_set`) are behind `#[cfg(target_os = "macos")]`; the Linux syscalls
are behind `#[cfg(target_os = "linux")]`.

---

## Roadmap — what isn't here yet

1. **Real market data feed** — *partially delivered:* the `kraken-feed` adapter
   brings live Kraken trades in over UDP (see *Live data feed*). Next: kernel-bypass
   networking (AF_XDP / DPDK) and multicast reception (e.g. CME MDP 3.0), keeping
   the single-writer-per-`latest_idx` invariant. Consider one `RingBuffer` per
   instrument (the scaffold is in place).
2. **Calibrated signal logic** — *partially delivered:* the `--train` CEM path
   learns the trading signal (a tiny L1-resident MLP) from historical captures
   (see *Learned policy*). The NEON/AVX2 *breakout* path is still a placeholder
   threshold. Next: train on long real captures, then fold the learned policy onto
   the register-resident SIMD hot path (the MLP inference is small enough).
3. **Real order submission** — drain the `OrderRing` to FIX / OUCH / an
   exchange-native binary protocol over a real NIC from a dedicated submission
   thread (the ring already has the right shape).
4. **Deeper risk & position management** — richer limits, drawdown kill switches,
   and full sequence-gap recovery.
5. **Multi-instrument** — wire up `InstrumentBuffers` beyond slot 0.

---

## Invariants — do not break these

These are the load-bearing assumptions the lock-free design depends on. Breaking
any of them introduces a data race or a latency regression that won't show up in
a quick test.

1. **`MarketTick` stays 64 bytes.** Adjust `_unused` if fields change. Verify
   with `assert_eq!(size_of::<MarketTick>(), 64)`.
2. **`latest_idx` and `start_time` stay adjacent in `RingBuffer`.** Same 64-byte
   cache line — this keeps `start_time` L1-hot for free.
3. **Only one thread writes `latest_idx`.** The lock-free reader depends on
   single-writer semantics.
4. **Only one thread writes any given `TradeLog` slot.** The strategy is the sole
   writer; the exchange writes only `round_trip_ns`, only after the strategy has
   moved on.
5. **Each `LatencyHistogram` has a single writer.** `sig_hist` ← strategy,
   `rt_hist` ← exchange. Reads only at shutdown.
6. **No mutex, condvar, or blocking sync in the trading loop.**
7. **No heap allocation on the hot path.** No `Box`, `Vec`, or `String` in
   `trading_strategy()` or tick processing.
8. **`Acquire` on read, `Release` on write** for every ring cursor. Relaxed
   ordering races on weakly-ordered ARM.
9. **`WARMUP_PACKETS` is consistent** across `rust_hft_software::config`,
   `engine.rs`, and `market-simulator.rs`. The engine gates trade-log writes on
   `current_seq > WARMUP_PACKETS`; the simulator must send exactly that many
   warmup packets first.
10. **The pre-touch step sizes match the structs** (8 u64s per tick/order-entry,
    **8 u64s per `TradeExecution`** — it is now 64 bytes). Changing a struct's
    size means changing the pre-touch loop. Compile-time `assert!`s in `models.rs`
    pin `MarketTick`, `TradeExecution`, and `RoundTrip` to 64 bytes (the
    round-trip log is pre-touched with the same step-8 loop).
11. **Do not replace `Instant::elapsed()` with `mach_absolute_time()` FFI.**
    Empirically ~42× slower and unit-fragile.
12. **Market-data packets are versioned and back-compatible.** Legacy = 16 bytes,
    v2 = 32 (adds `origin_ts_ns`/`transit_est_ns`), v3 = 33 (adds a 1-byte
    `instrument` id at [32]). The ingestor parses each tier by `amt`; old senders
    stay valid and route to instrument 0.
13. **stunnel terminates TLS externally; `kraken-feed` is plaintext TCP.** The
    adapter speaks WebSocket by hand and never links a TLS library — this is what
    keeps the workspace zero-dependency while consuming a `wss://` feed.

---

## Key Rust features in use

| Feature | Where | Why |
|---------|-------|-----|
| `std::arch::asm!` | engine.rs, testing_scripts | Direct SIMD / syscall control |
| `AtomicU64` + Acquire/Release | models.rs, engine.rs | Lock-free producer/consumer |
| `UnsafeCell<[T; N]>` | models.rs | Interior mutability for lock-free buffers |
| `#[repr(C, align(64))]` | models.rs | Cache-line alignment |
| `std::hint::spin_loop()` | engine.rs | YIELD/PAUSE in busy-wait |
| `std::hint::black_box` | engine.rs | Prevent elimination of warmup work |
| `std::ptr::write_volatile` | main.rs | Force OS page commitment (pre-touch) |
| `#[cold]` | engine.rs | Bias branch predictor away from the halt path |
| `unsafe extern "C"` | engine.rs, fake-exchange.rs | Call OS-private / libc APIs |
| `env!("CARGO_PKG_VERSION")` | engine.rs | Compile-time version for log paths |
| `#[cfg(target_arch / target_os)]` | throughout | Per-platform SIMD & scheduling |

---

## License

MIT — see [`LICENSE`](LICENSE). Contributions welcome; see
[`CONTRIBUTING.md`](CONTRIBUTING.md).
