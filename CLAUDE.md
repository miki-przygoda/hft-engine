# CLAUDE.md — Context for LLM-assisted development

This document is the single source of truth for any LLM working on this codebase. Read this before touching anything.

---

## Project goal

Build a high-frequency trading (HFT) engine in Rust targeting Apple Silicon (ARM64 / M-series). The guiding constraint is latency: every architectural decision should be evaluated in terms of nanoseconds, not developer ergonomics. This is an educational and research project but the engineering standards are production-grade.

---

## Current codebase state (as of 2026-04-01)

### File map

```
src/
├── main.rs                          # Strategy, ingestor, confirm receiver, heartbeat, stats threads
├── models.rs                        # Core data structures: MarketTick, RingBuffer, TradeLog, OrderBook
├── bin/
│   ├── market-simulator.rs          # UDP packet sender: 10 warmup + 40 real packets
│   └── fake-exchange.rs             # Spin-poll UDP exchange simulator, echoes orders as confirms
└── testing_scripts/
    ├── mod.rs                       # Declares one_threaded and multi_threaded submodules
    ├── one_threaded.rs              # Single-threaded SIMD throughput benchmark
    └── multi_threaded.rs            # Multi-threaded all-core stress benchmark (Apple Silicon)
```

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
- `start_time` was moved here from `OrderBook` specifically for this co-location. It must stay here.

### `TradeExecution`
```rust
#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence: u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns: u64,
    pub latency_ns: u64,        // buy_time_ns - ingest_time_ns
    pub order_send_ns: u64,     // when order packet was dispatched
    pub round_trip_ns: u64,     // confirm_recv_ns - order_send_ns; written by confirm receiver
}
```
- 48 bytes. `Copy + Clone`.
- `round_trip_ns` is written by the confirm receiver thread after the trade log slot has been committed by the strategy. This is safe because: (1) strategy commits the slot with `fetch_add(Release)` before sending the order packet; (2) the confirmation arrives only after the order is sent; (3) stats thread reads 5 seconds later. There is no concurrent write to the same field.

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
- **Write protocol:** fill `entries[write_idx % SIZE]` with all fields, then `fetch_add(1, Release)` on `write_idx` to commit.
- **Read protocol:** `load(Acquire)` on `write_idx` to get committed count, then read `entries[0..count]`.
- Initialized via `TradeLog::new()` which calls `unsafe { std::mem::zeroed() }` on the entries array. After construction, all pages must be committed via volatile writes in the warmup phase (see below) to avoid OS page faults on first real write.

### `OrderBook`
```rust
#[repr(C, align(64))]
pub(crate) struct OrderBook {
    pub(crate) buy_count: AtomicU64,
    pub(crate) trade_log: TradeLog,
}
```
- `start_time` is NOT here — it lives in `RingBuffer`. Do not move it back.

---

## Trading strategy (`src/main.rs`)

### Thread model

| Thread | Function | Priority |
|--------|----------|----------|
| Main | `trading_strategy` — hot path | `QOS_USER_INTERACTIVE` (0x21) |
| Ingestor | `run_ingestor` — UDP recv → ring buffer | `QOS_USER_INTERACTIVE` (0x21) |
| Confirm receiver | `run_confirm_receiver` — spin-poll UDP → write `round_trip_ns` | engine process |
| Heartbeat | sends 1-byte UDP to exchange every 1ms | default |
| Stats monitor | sleeps 5s then prints report | default |

### Port assignments

| Port | Direction | Purpose |
|------|-----------|---------|
| 34254 | simulator → ingestor | market tick packets |
| 34255 | engine → exchange | order packets (24 bytes) + heartbeats (1 byte) |
| 34256 | exchange → engine | order confirmations (24 bytes echoed) |

### Thread priority
```rust
unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}
// called with qos = 0x21 (QOS_CLASS_USER_INTERACTIVE), relpri = 0
```
- macOS-only Apple-private API. Biases the thread toward P-cores.
- Applied to: main (strategy), ingestor, and fake-exchange process.

### Warmup sequence (before entering the trading loop)

1. **NEON warmup loop** — 10,000 iterations of `fmul v0.4s` + `elapsed()` call. Warms the vector execution units, instruction cache, and the `start_time` cache line.
2. **OS page commitment** — one `write_volatile` per 64 entries (= 3072 bytes < 4KB page) across the full trade log entries array. `zeroed()` on a fresh heap allocation does not commit physical pages on macOS (zero-fill-on-demand). Without this, the first real trade write triggers a page fault (~3–5µs).

```rust
for _ in 0..10_000 {
    asm!("fmul v0.4s, v0.4s, v0.4s", ...);
    let _ = buffer.start_time.elapsed();
}
// page pre-touch
let mut i = 0;
while i < TRADE_LOG_SIZE {
    write_volatile(&mut entries[i].sequence, 0);
    i += 64;
}
```

### Warmup packets

`WARMUP_PACKETS = 10` (must match the same constant in `market-simulator.rs`).

Sequences 1–10 are warmup. The strategy runs the **full hot path** (NEON asm, `elapsed()`, lock-free write code path, order socket send) but skips the `fetch_add` commit and does not send a real order. Purpose: train branch predictor, warm all hot-path cache lines, and verify the socket path is alive — without polluting the trade log.

### Trading loop (spin-poll pattern)

```rust
loop {
    let current_seq = buffer.latest_idx.load(Ordering::Acquire);
    if current_seq > last_processed_seq {
        // hot path: NEON decision → trade log write → order send
    } else {
        std::hint::spin_loop(); // YIELD on ARM64
        // PRFM PSTL1KEEP for next trade log slot
    }
}
```
- **No sleep, no condvar, no mutex in the hot path.**
- `spin_loop()` emits `YIELD` (ARM64). Avoids burning full pipeline resources.
- **PRFM PSTL1KEEP** — in the idle branch, prefetches the next trade log write slot in exclusive (store-ready) cache state. This is a hint (not guaranteed), but reduces the cache penalty when the next tick arrives.
- The tick buffer is intentionally NOT prefetched: the ingestor writes to it, so a load-prefetch from the strategy would create cache coherence conflicts.

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
let trigger = (decision == 1) || ((current_seq & 1) == 1);
```
- Loads 16 bytes from the tick, squares the 4x f32 lanes, extracts bit 0.
- **This is not real signal logic.** It is a structural placeholder. The skeleton (load tick → SIMD compute → binary decision) is the correct pattern; the computation needs replacing.
- The `|| ((current_seq & 1) == 1)` fallback ensures trades trigger on every odd sequence in the simulation regardless of the NEON result.

### Order submission and round-trip measurement

```
Order packet layout (24 bytes, little-endian):
  bytes  0– 7  sequence      u64
  bytes  8–15  slot          u64   (trade log index — enables O(1) confirm lookup)
  bytes 16–23  order_send_ns u64   (timestamp for round-trip calculation)
```

1. Strategy commits trade log entry (all fields including `order_send_ns`) with `fetch_add(Release)`.
2. Strategy sends 24-byte order packet to port 34255 (non-blocking socket).
3. Fake exchange spin-polls 34255, echoes real orders (≥ 24 bytes) to 34256.
4. Confirm receiver spin-polls 34256, uses `slot` from packet to index directly into trade log, writes `round_trip_ns = confirm_recv_ns - order_send_ns`.

**Heartbeat:** a separate thread sends 1-byte packets to port 34255 every 1ms. The exchange discards them (< 24 bytes) but stays awake and keeps the kernel networking path warm. Without heartbeats, the exchange process sleeps between real orders and pays a ~10–30µs OS wakeup penalty per order.

---

## Benchmarking tools (`src/testing_scripts/`)

### `one_threaded.rs`
- Runtime SIMD detection: `is_x86_feature_detected!` for AVX-512/AVX2/SSE; `cfg(target_arch = "aarch64")` + NEON for ARM64.
- Runs 1 billion float multiplies, reports Gops/s and ns/Mops.
- Used to establish a hardware ceiling: the trading loop cannot exceed this throughput.

### `multi_threaded.rs` ("The Kraken")
- Apple Silicon M3 Max focused.
- Progressive thermal ramp: spins up threads 1..=N over 10 seconds.
- Full parallel execution across all available cores, 1 billion NEON ops each.
- Measures wall-clock aggregate throughput and per-op latency.

---

## Observed latency characteristics

| Metric | Typical range | Root cause |
|--------|--------------|------------|
| Signal latency (min) | 125–208 ns | L2 cache misses due to 50ms inter-packet gaps |
| Signal latency (typical) | 200–600 ns | Cache cold path + NEON + hardware timer |
| Signal latency (spike) | 800–3500 ns | OS timer interrupt hitting the strategy thread |
| Round trip (min) | 43–75 µs | 4 kernel boundary crossings + 2 loopback traversals |
| Round trip (typical) | 50–100 µs | Above + OS scheduling for exchange/confirm threads |
| Round trip (spike) | 100–220 µs | Thread migration, first-packet cold path |

**Signal/round-trip ratio (~163x):** signal latency is pure userspace (no syscalls). Round trip mandatorily crosses EL0→EL1 four times plus two process wakeups — categorically different operations. Closing this gap requires kernel bypass (DPDK/shared memory), not further userspace optimization.

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
The current submission path is a UDP simulation to a local fake exchange:
- Replace with FIX, OUCH, or exchange-native binary protocol.
- Move order send to a dedicated submission thread — strategy writes to an order ring buffer, submission thread drains it.

### 4. Position and risk management
- Flat position limits (max long/short)
- Loss limits (kill switch on drawdown)
- Sequence-number gap detection (detect missed ticks)

### 5. Multi-instrument support
- `HashMap<InstrumentId, Arc<RingBuffer>>` or flat array indexed by compact instrument ID.
- Pre-allocate all buffers at startup — no dynamic allocation in the hot path.

### 6. Round-trip improvement below ~43µs
Current floor is the macOS BSD socket layer. To go lower:
- **Shared memory ring buffer** between engine and fake-exchange — eliminates all kernel involvement, expected ~1–5µs.
- **Kernel bypass NIC** (DPDK/Solarflare) — eliminates kernel from both feed and order paths.

### 7. x86_64 portability
- Wrap `pthread_set_qos_class_self_np` in `#[cfg(target_os = "macos")]` with a Linux `sched_setaffinity` equivalent.
- Replace ARM64 NEON asm with AVX2/AVX-512 equivalents behind `#[cfg(target_arch = "x86_64")]`.
- `one_threaded.rs` already has the x86_64 path — use it as reference.

---

## Invariants to never break

1. **`MarketTick` must remain 64 bytes.** If fields are added, adjust `_unused` accordingly. Verify with `assert_eq!(std::mem::size_of::<MarketTick>(), 64)`.
2. **`latest_idx` and `start_time` must remain adjacent in `RingBuffer`.** They are in the same 64-byte cache line. Inserting any field between them breaks the co-location optimization that keeps `start_time` L1-warm for free.
3. **Only one thread writes `latest_idx`.** The lock-free reader pattern depends on single-writer semantics.
4. **Only one thread writes to any given `TradeLog` slot.** The strategy is the sole writer. The confirm receiver writes only `round_trip_ns` on already-committed slots — after the strategy has moved on.
5. **No mutex or condvar in the trading loop.** Any blocking synchronization in the hot path defeats the purpose.
6. **No heap allocation in the hot path.** `Box`, `Vec`, `String` — none of these in `trading_strategy()` or the tick processing path.
7. **`Ordering::Acquire` on read, `Ordering::Release` on write.** Relaxed ordering on the ring buffer cursor introduces data races on weakly-ordered ARM.
8. **`WARMUP_PACKETS` must be consistent between `main.rs` and `market-simulator.rs`.** The engine gates trade log writes on `current_seq > WARMUP_PACKETS`; the simulator sends exactly that many warmup packets before real traffic.

---

## Key Rust features in use

| Feature | Where | Why |
|---------|-------|-----|
| `std::arch::asm!` | main.rs, multi_threaded.rs, one_threaded.rs | Direct SIMD instruction control |
| `AtomicU64` with Acquire/Release | models.rs, main.rs | Lock-free producer/consumer |
| `UnsafeCell<[T; N]>` | models.rs (TradeLog) | Interior mutability for lock-free trade log |
| `Arc<T>` | main.rs | Shared ownership across threads |
| `#[repr(C, align(64))]` | models.rs | Cache-line alignment |
| `std::hint::spin_loop()` | main.rs, fake-exchange.rs | YIELD/PAUSE hint in busy-wait |
| `std::ptr::write_volatile` | main.rs (warmup) | Force OS page commitment, prevent compiler elision |
| `unsafe extern "C"` | main.rs, fake-exchange.rs | Call macOS-private pthread QOS API |
| `#[inline(always)]` | main.rs | Prevent hot-path function call overhead |
| `#[target_feature(enable = "...")]` | one_threaded.rs | Safe SIMD dispatch |

---

## Platform assumptions

- **Primary target:** macOS, Apple Silicon (M-series), ARM64.
- `pthread_set_qos_class_self_np` is macOS-only.
- ARM64 NEON inline assembly in `main.rs` and `multi_threaded.rs` will not compile on x86.
- `one_threaded.rs` is the only file with cross-platform SIMD support.
- No Linux testing has been done. Linux support is a future concern.
