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
├── lib.rs                           # Shared config constants (rust_hft_software::config)
├── bin/
│   ├── market-simulator.rs          # Standalone UDP packet sender (warmup + real packets)
│   └── fake-exchange.rs             # Standalone spin-poll UDP exchange, echoes orders as confirms
└── testing_scripts/
    ├── mod.rs                       # Declares one_threaded and multi_threaded submodules
    ├── one_threaded.rs              # Single-threaded SIMD throughput benchmark (x86 + ARM)
    └── multi_threaded.rs            # Multi-threaded all-core stress benchmark ("The Kraken")

CLAUDE.md                            # This file — architecture & design reference
README.md                            # Public overview, measured latency tables
LICENSE                              # MIT license
CONTRIBUTING.md                      # How to build, test, and contribute
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
    pub(crate) price:     f32,   // 4 bytes
    pub(crate) volume:    f32,   // 4 bytes
    pub(crate) sequence:  u64,   // 8 bytes
    pub(crate) timestamp: u64,   // 8 bytes — ingest time (ns since engine start)
    _unused: [u8; 36],           // padding to 64 bytes
}
```

64 bytes exactly — one tick per cache line, no false sharing, no partial-line
loads. The ingestor writes `timestamp` before publishing via the ring cursor;
the strategy reads it after the matching acquire load, so visibility is
guaranteed by the acquire/release pair.

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
}
```

48 bytes (6 × u64). **Write protocol:** the strategy fills all fields, then
commits with `fetch_add(1, Release)` on the log's `write_idx`. **Read protocol:**
`load(Acquire)` to get the committed count, then read `[0..count]`. The exchange
thread writes only `round_trip_ns`, and only on a slot the strategy has already
committed and moved past — so there is never a concurrent write to one field.

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
(permanent risk kill-switch), `net_position`, and memory-snapshot fields.

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

### The signal: a register-resident momentum window

An 8-price sliding window lives entirely in vector registers across loop
iterations — no memory access for window state between ticks:

- **ARM64 (NEON):** the window is a `float32x4_t` pair (`win_lo` / `win_hi`)
  bound via `inout(vreg)`. Each tick: `LD1` loads the new price, two `EXT`s
  slide the window by one f32 lane, a `FADDP` tree sums the eight prices, and
  `FCMGT` compares the current price to `mean × 1.001` — the result bit is the
  trigger. ~6 NEON instructions, one tick load, zero window-state memory traffic.
- **x86_64 (AVX2):** the window is a single `__m256` (8×f32 = 256 bits).
  `vextractf128` + `vpalignr` + `vinsertf128` shift it by one lane; two
  `vhaddps` passes plus `vpermilps`/`vaddss` reduce to the scalar sum; `vucomiss`
  + `seta` produce the branchless 0/1 trigger.

This is a structurally correct momentum signal, but the threshold is a
placeholder — it demonstrates that real signal computation fits inside the
latency budget, not a calibrated trading strategy.

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
  gap counts, memory snapshots, signal-latency and round-trip percentiles
  (avg/min/max/p50/p95/p99/p99.9), and the full per-trade array.

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

1. **Real market data feed** — replace the UDP simulator with kernel-bypass
   networking (AF_XDP / DPDK) and multicast reception (e.g. CME MDP 3.0), keeping
   the single-writer-per-`latest_idx` invariant. Consider one `RingBuffer` per
   instrument (the scaffold is in place).
2. **Calibrated signal logic** — the NEON/AVX2 momentum path is structurally
   correct but the threshold is a placeholder. Real signals (momentum, VWAP
   deviation) should stay register-resident and branchless.
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
    6 u64s per `TradeExecution`). Changing a struct's size means changing the
    pre-touch loop.
11. **Do not replace `Instant::elapsed()` with `mach_absolute_time()` FFI.**
    Empirically ~42× slower and unit-fragile.

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
