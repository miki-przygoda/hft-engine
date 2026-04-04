# CLAUDE.md — Context for LLM-assisted development

This document is the single source of truth for any LLM working on this codebase. Read this before touching anything.

---

## Project goal

Build a high-frequency trading (HFT) engine in Rust targeting Apple Silicon (ARM64 / M-series). The guiding constraint is latency: every architectural decision should be evaluated in terms of nanoseconds, not developer ergonomics. This is an educational and research project but the engineering standards are production-grade.

---

## Current codebase state (as of 2026-04-04)

### File map

```
src/
├── main.rs                          # Thread orchestration, buffer pre-touch, startup
├── engine.rs                        # All runtime logic: ingestor, exchange, watchdog, simulator, strategy, logging
├── models.rs                        # Core data structures: MarketTick, RingBuffer, TradeLog, OrderBook, OrderRing
├── lib.rs                           # Shared config constants for standalone binaries
├── bin/
│   ├── market-simulator.rs          # Standalone UDP packet sender (10 warmup + 100 real packets)
│   └── fake-exchange.rs             # Standalone spin-poll UDP exchange, echoes orders as confirms
└── testing_scripts/
    ├── mod.rs                       # Declares one_threaded and multi_threaded submodules
    ├── one_threaded.rs              # Single-threaded SIMD throughput benchmark
    └── multi_threaded.rs            # Multi-threaded all-core stress benchmark (Apple Silicon)
```

Binaries defined in `Cargo.toml`:
- `trading-engine` → `src/main.rs` (self-contained simulation)
- `fake-exchange` → `src/bin/fake-exchange.rs`
- `market-simulator` → `src/bin/market-simulator.rs`
- `bench-one-threaded` → `src/testing_scripts/one_threaded.rs`
- `bench-multi-threaded` → `src/testing_scripts/multi_threaded.rs`

No external dependencies. `Cargo.toml` has an empty `[dependencies]` section and uses `edition = "2024"`.

---

## Data structures (`src/models.rs`)

### `MarketTick`
```rust
#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price: f32,       // 4 bytes
    pub(crate) volume: f32,      // 4 bytes
    pub(crate) sequence: u64,    // 8 bytes
    pub(crate) timestamp: u64,   // 8 bytes — ingest time (ns since engine start)
    _unused: [u8; 36],           // padding to 64 bytes
}
```
- Exactly one CPU cache line (64 bytes). Non-negotiable.
- `timestamp` is written by the ingestor thread before `latest_idx.store(Release)`. The strategy reads it after `latest_idx.load(Acquire)` — the acquire/release pair guarantees visibility.
- `_unused` is intentional padding — do not remove it.

### `RingBuffer`
```rust
pub(crate) const BUFFER_SIZE: usize = 1024;

#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks: [MarketTick; BUFFER_SIZE],  // 65536 bytes
    pub(crate) latest_idx: AtomicU64,              // offset 65536 — cache line boundary
    pub(crate) start_time: Instant,               // offset 65544 — SAME cache line as latest_idx
}
```
- Lock-free single-producer / single-consumer ring buffer.
- **Cache layout invariant:** `latest_idx` lands at offset 65536 (exactly a 64-byte boundary). `start_time` sits 8 bytes later in the same cache line. Every spin-poll `load(Acquire)` on `latest_idx` keeps `start_time` in L1 at zero extra cost. Do not insert any field between `latest_idx` and `start_time`.
- `start_time` must stay in `RingBuffer`. It was deliberately moved here from `OrderBook` for the cache co-location. Do not move it back.

### `TradeExecution`
```rust
#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence: u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns: u64,
    pub latency_ns: u64,        // buy_time_ns - ingest_time_ns
    pub order_send_ns: u64,     // when order was submitted to the exchange ring
    pub round_trip_ns: u64,     // confirm_recv_ns - order_send_ns; written by exchange thread
}
```
- 48 bytes. `Copy + Clone`.
- `round_trip_ns` is written by `run_in_process_exchange` after the strategy has committed the slot and moved on. No concurrent write to the same field.

### `TradeLog`
```rust
pub(crate) const TRADE_LOG_SIZE: usize = 1024;

pub(crate) struct TradeLog {
    pub(crate) entries: UnsafeCell<[TradeExecution; TRADE_LOG_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}
unsafe impl Sync for TradeLog {}
```
- Lock-free single-writer trade log.
- **Write protocol:** fill `entries[write_idx & MASK]` with all fields, then `fetch_add(1, Release)` on `write_idx` to commit.
- **Read protocol:** `load(Acquire)` on `write_idx` to get committed count, then read `entries[0..count]`.
- Initialized via `TradeLog::new()` → `unsafe { std::mem::zeroed() }`. All pages must be committed via `write_volatile` in the pre-touch phase in `main.rs` before threads are spawned.

### `OrderBook`
```rust
#[repr(C, align(64))]
pub(crate) struct OrderBook {
    pub(crate) trade_log: TradeLog,
}
```
- Contains only the `TradeLog`. There is no `buy_count` field — it was removed. Do not add it back unless needed.
- `start_time` is NOT here — it lives in `RingBuffer`. Do not move it back.

### `OrderRing`
```rust
pub(crate) const ORDER_RING_SIZE: usize = 1024;

pub(crate) struct OrderRing {
    pub(crate) entries: UnsafeCell<[OrderEntry; ORDER_RING_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}

#[repr(C, align(64))]
pub(crate) struct OrderEntry {
    pub(crate) sequence:      u64,
    pub(crate) slot:          u64,   // trade log index for O(1) confirm lookup
    pub(crate) order_send_ns: u64,
    _pad: [u8; 40],                  // padding to 64 bytes
}
```
- SPSC ring connecting the strategy thread (writer) to the exchange thread (reader).
- The exchange thread uses `slot` to index directly into the trade log without scanning.

---

## Engine (`src/engine.rs`)

All runtime logic lives here. Key functions:

| Function | Role |
|----------|------|
| `run_ingestor` | Binds UDP 34254, spin-polls, writes ticks into `RingBuffer` |
| `run_in_process_exchange` | Consumes `OrderRing`, writes `round_trip_ns` back to trade log |
| `run_watchdog` | 500ms poll — 10s idle shutdown, 30s no-feed timeout |
| `run_market_simulator` | Internal simulator — waits for ingestor greenlight, sends packets |
| `trading_strategy` | Hot path — NEON warmup, spin-poll loop, signal, trade log write |
| `print_stats` | Print latency report to stdout |
| `write_log` | Persist run results to `logs/v{version}/{date}/{HH-MM-SS}.json` |

### Timing

`elapsed_ns(start: &std::time::Instant) -> u64` is the single timing primitive. It calls `start.elapsed().as_nanos() as u64`. Do NOT replace this with `mach_absolute_time()` FFI — `Instant::elapsed()` uses the commpage fast path and is faster and more accurate than raw `mach_absolute_time()` FFI on Apple Silicon. This was verified empirically: the FFI version produced 42x worse latencies.

---

## Thread model (`src/main.rs`)

| Thread | Function | Priority |
|--------|----------|----------|
| Watchdog | `run_watchdog` — idle detection, shutdown | default |
| Exchange | `run_in_process_exchange` — in-process order confirmation | `QOS_USER_INTERACTIVE` (0x21) |
| Ingestor | `run_ingestor` — UDP recv → ring buffer | `QOS_USER_INTERACTIVE` (0x21) |
| Simulator | `run_market_simulator` — internal market data | default |
| Strategy (main) | `trading_strategy` — hot path | `QOS_USER_INTERACTIVE` (0x21) |

The strategy thread spins on two `Arc<AtomicBool>` greenlights (`ingestor_ready`, `exchange_ready`) before calling `trading_strategy`. The main thread joins the strategy thread. The watchdog calls `std::process::exit` when done.

### Thread priority
```rust
unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}
// called with qos = 0x21 (QOS_CLASS_USER_INTERACTIVE), relpri = 0
```
- macOS-only Apple-private API. Biases the thread toward P-cores.
- Applied to: strategy, ingestor, exchange threads.

### Buffer pre-touch (before spawning any threads)

All three shared buffers are written with `write_volatile` in `main()` before threads are spawned:

```rust
// RingBuffer ticks — 1024 entries × 8 u64 words = 8192 writes
let ticks = buffer.ticks.as_ptr() as *mut u64;
for i in (0..BUFFER_SIZE * 8).step_by(8) {
    std::ptr::write_volatile(ticks.add(i), 0);
}
// OrderRing entries
let ring = (*order_ring.entries.get()).as_ptr() as *mut u64;
for i in (0..ORDER_RING_SIZE * 8).step_by(8) {
    std::ptr::write_volatile(ring.add(i), 0);
}
// TradeLog entries
let log = (*order_book.trade_log.entries.get()).as_ptr() as *mut u64;
for i in (0..TRADE_LOG_SIZE * 6).step_by(6) {
    std::ptr::write_volatile(log.add(i), 0);
}
```

`std::mem::zeroed()` on a fresh heap allocation does not commit physical pages on macOS (zero-fill-on-demand). Without this, the first real write to any of these buffers triggers a page fault (~3–5µs). The step sizes match the struct field counts (8 u64s per tick/order-entry, 6 u64s per trade-execution).

### Port assignments (standalone binaries only)

| Port | Direction | Purpose |
|------|-----------|---------|
| 34254 | simulator → ingestor | market tick packets |
| 34255 | engine → exchange | order packets (24 bytes) |
| 34256 | exchange → engine | order confirmations (24 bytes echoed) |

Ports 34255/34256 are used by the standalone `fake-exchange` binary. The `trading-engine` binary uses the `OrderRing` in-process and does not open order/confirm sockets.

---

## Warmup sequence (inside `trading_strategy`)

1. **NEON warmup loop** — 10,000 iterations of `fmul v0.4s` + `elapsed_ns()` call. Both outputs are passed through `black_box` to prevent dead-code elimination. Warms vector execution units, instruction cache, and the `start_time` cache line.

```rust
for _ in 0..10_000 {
    let mut dummy: u64;
    asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0",
         res = out(reg) dummy, options(nostack, nomem));
    black_box(dummy);
    black_box(elapsed_ns(&buffer.start_time));
}
```

2. **Warmup packets** — `WARMUP_PACKETS = 10`. Sequences 1–10 run the full hot path (NEON asm, `elapsed_ns`, ring buffer reads) but skip the `fetch_add` commit. Purpose: train branch predictor, warm all hot-path cache lines — without polluting the trade log. `WARMUP_PACKETS` is defined in `src/lib.rs` and used in both `engine.rs` and `market-simulator.rs`. They must stay in sync.

---

## Trading loop (spin-poll pattern)

```rust
loop {
    let current_seq = buffer.latest_idx.load(Ordering::Acquire);
    if current_seq > last_processed_seq {
        // hot path: NEON decision → trade log write → order ring push
    } else {
        std::hint::spin_loop(); // YIELD on ARM64
        // PRFM PSTL1KEEP for next trade log slot
    }
}
```
- **No sleep, no condvar, no mutex in the hot path.**
- `spin_loop()` emits `YIELD` (ARM64). Avoids burning full pipeline resources.
- **PRFM PSTL1KEEP** — in the idle branch, prefetches the next trade log write slot in exclusive (store-ready) cache state. Reduces cache penalty when the next tick arrives.
- The tick buffer is intentionally NOT prefetched from the strategy: the ingestor writes to it, so a load-prefetch from the strategy would cause cache coherence conflicts.

### Decision logic (placeholder)

```rust
asm!(
    "ld1 {v0.4s}, [{ptr}]",
    "fmul v1.4s, v0.4s, v0.4s",
    "fmov {res:w}, s1",
    "and {res:w}, {res:w}, #1",
    ptr = in(reg) tick_ptr,
    res = out(reg) decision,
    options(nostack, nomem)
);
let trigger = (black_box(decision) | (current_seq & 1)) != 0;
```
- Loads 16 bytes from the tick, squares the 4x f32 lanes, extracts bit 0.
- **This is not real signal logic.** Structural placeholder only. The `| (current_seq & 1)` ensures trades trigger on every odd sequence in simulation.
- `black_box(decision)` prevents the compiler from using the provably 0-or-1 range of `decision` to transform the branch. The branchless `|` (not `||`) eliminates short-circuit evaluation.

### Round-trip measurement (in-process)

1. Strategy fills a `TradeExecution` (all fields including `order_send_ns`) and commits with `fetch_add(Release)`.
2. Strategy writes an `OrderEntry` (sequence, slot, `order_send_ns`) to the `OrderRing` and commits with `fetch_add(Release)`.
3. Exchange thread (`run_in_process_exchange`) spin-polls `OrderRing`, reads `confirm_recv_ns = elapsed_ns(&buffer.start_time)`, writes `round_trip_ns = confirm_recv_ns - order_send_ns` directly into the trade log slot.
4. No kernel boundary crossings — the entire round trip is userspace shared memory.

---

## Run logging (`write_log` in `engine.rs`)

After each run the engine writes:
```
logs/v{version}/{date}/{HH-MM-SS}.json
```

- Version from `env!("CARGO_PKG_VERSION")` — tracks `Cargo.toml` automatically.
- Date/time computed from `SystemTime::now()` via a stdlib-only Gregorian calendar calculation (no chrono dependency).
- JSON is built with manual string formatting — no serde.
- Contains: version, timestamp, total_trades, signal_latency (avg/min/max), round_trip (avg/min/max), full trades array.

---

## Benchmarking tools (`src/testing_scripts/`)

### `one_threaded.rs`
- Runtime SIMD detection: `is_x86_feature_detected!` for AVX-512/AVX2/SSE; `cfg(target_arch = "aarch64")` + NEON for ARM64.
- Runs 1 billion float multiplies, reports Gops/s and ns/Mops.
- Used to establish a hardware ceiling: the trading loop cannot exceed this throughput.

### `multi_threaded.rs` ("The Kraken")
- Apple Silicon focused.
- Progressive thermal ramp: spins up threads 1..=N over 10 seconds.
- Full parallel execution across all available cores, 1 billion NEON ops each.
- Measures wall-clock aggregate throughput and per-op latency.

---

## Observed latency characteristics

| Metric | Typical range | Root cause |
|--------|--------------|------------|
| Signal latency (min) | 83–125 ns | L2 cache misses due to 10ms inter-packet gaps |
| Signal latency (typical) | 125–400 ns | Cache cold path + NEON + hardware timer |
| Signal latency (spike) | 500–800 ns | OS timer interrupt hitting the strategy thread |
| Round trip (in-process, min) | 83–125 ns | Shared memory write + atomic read, no syscalls |
| Round trip (in-process, typical) | 125–400 ns | Same cache-miss profile as signal latency |
| Round trip (in-process, spike) | 500–650 ns | Thread scheduling jitter |
| Round trip (external UDP, min) | 43–75 µs | 4 kernel boundary crossings + 2 loopback traversals |
| Round trip (external UDP, typical) | 50–135 µs | Above + OS scheduling for exchange/confirm threads |

**In-process vs external UDP (~163x difference):** in-process exchange has no kernel crossings. External UDP mandatorily crosses EL0→EL1 four times plus two process wakeups. The `trading-engine` binary uses the in-process path. The standalone `fake-exchange` binary exists for external testing when kernel-path measurement is needed.

---

## What needs to be built next

### 1. Real market data feed
Replace the UDP ingestor stub with real tick ingestion:
- Kernel-bypass networking (raw sockets or DPDK bindings) for the feed thread.
- UDP multicast reception (CME MDP 3.0 or similar), direct ring buffer writes.
- Single writer to `latest_idx` — maintain this invariant.
- Consider a separate `RingBuffer` per instrument.

### 2. Signal logic
Replace the NEON placeholder in `trading_strategy()`:
- Momentum / VWAP deviation are common starting points.
- All hot-path computation should stay in SIMD registers.
- Minimize branching — prefer branchless SIMD predicates.

### 3. Real order submission
The current submission path writes to an in-process `OrderRing`:
- Replace with FIX, OUCH, or exchange-native binary protocol over a real NIC.
- A dedicated submission thread can drain the `OrderRing` and send over the wire — the ring already exists for this pattern.

### 4. Position and risk management
- Flat position limits (max long/short)
- Loss limits (kill switch on drawdown)
- Sequence-number gap detection (detect missed ticks)

### 5. Multi-instrument support
- `HashMap<InstrumentId, Arc<RingBuffer>>` or flat array indexed by compact instrument ID.
- Pre-allocate all buffers at startup — no dynamic allocation in the hot path.

### 6. x86_64 portability
- Wrap `pthread_set_qos_class_self_np` in `#[cfg(target_os = "macos")]` with a Linux `sched_setaffinity` equivalent.
- Replace ARM64 NEON asm with AVX2/AVX-512 equivalents behind `#[cfg(target_arch = "x86_64")]`.
- `one_threaded.rs` already has the x86_64 path — use it as reference.

---

## Invariants to never break

1. **`MarketTick` must remain 64 bytes.** If fields are added, adjust `_unused` accordingly. Verify with `assert_eq!(std::mem::size_of::<MarketTick>(), 64)`.
2. **`latest_idx` and `start_time` must remain adjacent in `RingBuffer`.** They are in the same 64-byte cache line. Inserting any field between them breaks the co-location optimization that keeps `start_time` L1-warm for free.
3. **Only one thread writes `latest_idx`.** The lock-free reader pattern depends on single-writer semantics.
4. **Only one thread writes to any given `TradeLog` slot.** The strategy is the sole writer. The exchange thread writes only `round_trip_ns` on already-committed slots — after the strategy has moved on.
5. **No mutex or condvar in the trading loop.** Any blocking synchronization in the hot path defeats the purpose.
6. **No heap allocation in the hot path.** `Box`, `Vec`, `String` — none of these in `trading_strategy()` or the tick processing path.
7. **`Ordering::Acquire` on read, `Ordering::Release` on write.** Relaxed ordering on the ring buffer cursor introduces data races on weakly-ordered ARM.
8. **`WARMUP_PACKETS` must stay consistent across `engine.rs` and `market-simulator.rs`.** The engine gates trade log writes on `current_seq > WARMUP_PACKETS`; the simulator sends exactly that many warmup packets first.
9. **Do not replace `Instant::elapsed()` with `mach_absolute_time()` FFI.** `Instant` uses the commpage fast path and is empirically faster. Raw FFI to `mach_absolute_time()` also returns 24 MHz ticks (not nanoseconds), requiring a `* 125 / 3` conversion that makes the code fragile and slower.

---

## Key Rust features in use

| Feature | Where | Why |
|---------|-------|-----|
| `std::arch::asm!` | engine.rs, multi_threaded.rs, one_threaded.rs | Direct SIMD instruction control |
| `AtomicU64` with Acquire/Release | models.rs, engine.rs | Lock-free producer/consumer |
| `UnsafeCell<[T; N]>` | models.rs (TradeLog, OrderRing) | Interior mutability for lock-free buffers |
| `Arc<T>` | main.rs | Shared ownership across threads |
| `#[repr(C, align(64))]` | models.rs | Cache-line alignment |
| `std::hint::spin_loop()` | engine.rs, fake-exchange.rs | YIELD/PAUSE hint in busy-wait |
| `std::hint::black_box` | engine.rs | Prevent compiler from eliminating warmup work |
| `std::ptr::write_volatile` | main.rs (pre-touch) | Force OS page commitment, prevent compiler elision |
| `unsafe extern "C"` | engine.rs, fake-exchange.rs | Call macOS-private pthread QOS API |
| `#[inline(always)]` | engine.rs | Prevent hot-path function call overhead |
| `env!("CARGO_PKG_VERSION")` | engine.rs | Compile-time version for log file paths |
| `#[target_feature(enable = "...")]` | one_threaded.rs | Safe SIMD dispatch |

---

## Platform assumptions

- **Primary target:** macOS, Apple Silicon (M-series), ARM64.
- `pthread_set_qos_class_self_np` is macOS-only.
- ARM64 NEON inline assembly in `engine.rs` and `multi_threaded.rs` will not compile on x86.
- `one_threaded.rs` is the only file with cross-platform SIMD support.
- No Linux testing has been done. Linux support is a future concern.
