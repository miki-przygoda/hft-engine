# CLAUDE.md — Context for LLM-assisted development

This document is the single source of truth for any LLM working on this codebase. Read this before touching anything.

---

## Project goal

Build a high-frequency trading (HFT) engine in Rust targeting Apple Silicon (ARM64 / M-series). The guiding constraint is latency: every architectural decision should be evaluated in terms of nanoseconds, not developer ergonomics. This is an educational and research project but the engineering standards are production-grade.

---

## Current codebase state (as of 2026-03-31)

### File map

```
src/
├── main.rs                          # Trading strategy entry point, thread setup, QOS pinning
├── models.rs                        # Core data structures: MarketTick, RingBuffer
└── testing_scripts/
    ├── mod.rs                       # Declares one_threaded and multi_threaded submodules
    ├── one_threaded.rs              # Single-threaded SIMD throughput benchmark
    └── multi_threaded.rs           # Multi-threaded all-core stress benchmark (Apple Silicon)
```

No external dependencies. `Cargo.toml` has an empty `[dependencies]` section and uses `edition = "2024"`.

---

## Data structures (`src/models.rs`)

### `MarketTick`
```rust
#[repr(C, align(64))]
pub(crate) struct MarketTick {
    price: f32,      // 4 bytes
    volume: f32,     // 4 bytes
    sequence: u64,   // 8 bytes
    _unused: [u8; 44], // padding to 64 bytes
}
```
- Exactly one CPU cache line (64 bytes). This is non-negotiable for performance.
- `_unused` is intentional padding — do not remove it.
- Fields are `pub(crate)` — not exposed outside the crate.

### `RingBuffer`
```rust
pub(crate) const BUFFER_SIZE: usize = 1024;

#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks: [MarketTick; BUFFER_SIZE],
    pub(crate) latest_idx: AtomicU64,
}
```
- Lock-free single-producer / single-consumer ring buffer.
- `latest_idx` is the write cursor (sequence number, not a modulo index). Readers compute `idx = seq % BUFFER_SIZE`.
- `AtomicU64` with `Ordering::Release` on write, `Ordering::Acquire` on read — the acquire/release pair establishes the happens-before relationship.
- The buffer holds 1024 ticks. Overwrite semantics: old ticks are clobbered when the buffer wraps. This is intentional — HFT cares about the latest state, not history.

---

## Trading strategy (`src/main.rs`)

### Thread model
- **Main thread:** trading strategy. This is the hot path. Never block it.
- **Background thread:** market feed stub. Currently sleeps 1000ms and increments `latest_idx`. In production this would be a kernel-bypass network thread (e.g., DPDK or RDMA).

### Thread priority
```rust
unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}
// called with qos = 0x21 (QOS_CLASS_USER_INTERACTIVE), relpri = 0
```
- This is an Apple-private API. It biases the thread toward P-cores on Apple Silicon.
- `0x21` = `QOS_CLASS_USER_INTERACTIVE`. This is the highest QOS class available without root.
- This call is macOS/iOS only. Any cross-platform work needs a `#[cfg(target_os = "macos")]` guard.

### CPU warmup
```rust
for _ in 0..10_000 {
    asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0", res = out(reg) _dummy);
}
```
- 10,000 NEON fmul operations before entering the trading loop.
- Purpose: warm the vector execution units, pull trading_strategy into L1 instruction cache, eliminate first-iteration penalty.
- This pattern should be preserved and potentially expanded when new hot-path assembly is added.

### Trading loop (spin-poll pattern)
```rust
loop {
    let current_seq = buffer.latest_idx.load(Ordering::Acquire);
    if current_seq > last_processed_seq {
        // process tick
        last_processed_seq = current_seq;
    } else {
        std::hint::spin_loop(); // YIELD / WFE hint
    }
}
```
- **No sleep, no condvar, no mutex.** Busy-wait is intentional at this latency tier.
- `spin_loop()` emits a `YIELD` instruction on ARM64 (or `PAUSE` on x86). This avoids burning full pipeline resources while still polling at near-maximum frequency.
- `Ordering::Acquire` on load pairs with `Ordering::Release` on store to guarantee memory visibility.

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
```
- Loads 16 bytes from `tick_ptr` (the first 16 bytes of a `MarketTick`: price, volume, and the low 8 bytes of sequence).
- Squares the 4x f32 lane values, extracts bit 0 of the low f32 result as a binary buy signal.
- **This is not real signal logic.** It is a structural placeholder. The inline assembly skeleton (load tick → compute → emit binary decision) is the correct pattern; the computation itself needs replacing.
- `options(nostack, nomem)` tells LLVM not to assume memory effects or stack changes. This is correct here because we explicitly named the memory operand via `ptr`.

---

## Benchmarking tools (`src/testing_scripts/`)

### `one_threaded.rs`
- Runtime SIMD detection: `is_x86_feature_detected!` for AVX-512/AVX2/SSE; `cfg(target_arch = "aarch64")` + NEON for ARM64.
- Runs 1 billion float multiplies, reports Gops/s and ns/Mops.
- Used to establish a hardware ceiling: the trading loop cannot exceed this throughput.
- Architecture dispatch is via `select_mode()` returning an enum, then a match to call the appropriate `run_compute_block_*()` function.

### `multi_threaded.rs` ("The Kraken")
- Apple Silicon M3 Max focused.
- Progressive thermal ramp: spins up threads 1..=N over 10 seconds to avoid thermal throttling.
- Full parallel execution: `thread::available_parallelism()` cores, each running 1 billion NEON ops.
- Measures wall-clock aggregate throughput and per-op latency.
- Used to characterize multi-core headroom when the engine runs alongside other system processes.

---

## What needs to be built next (development roadmap)

### 1. Real market data feed
The background thread is a stub. Replace it with real tick ingestion:
- **Kernel bypass networking** — look at using raw sockets or DPDK bindings. The feed thread should receive UDP multicast (e.g., CME MDP 3.0 or similar) and write ticks directly into the ring buffer.
- The writer path must be lock-free. Only one thread writes `latest_idx` — maintain this invariant.
- Consider a separate `RingBuffer` per instrument. The current design is single-instrument.

### 2. Signal logic
Replace the NEON placeholder in `trading_strategy()` with real decision logic:
- Momentum / VWAP deviation signals are common starting points.
- All hot-path computation should stay in SIMD registers if possible. The inline asm pattern is already set up correctly for this.
- Any branching in the hot path should be minimized — prefer branchless SIMD predicates.

### 3. Order submission
No exchange connectivity exists yet. This is the other half of the engine:
- Need a low-latency order submission path (FIX, OUCH, or exchange-native binary protocol).
- The submission thread should be separate from the strategy thread — strategy writes to an order ring buffer, submission thread drains it.

### 4. Position and risk management
No position tracking, P&L, or risk limits exist. These must be added before any live trading:
- Flat position limits (max long/short)
- Loss limits (kill switch on drawdown)
- Sequence-number gap detection (detect missed ticks from the feed)

### 5. Multi-instrument support
`RingBuffer` is a fixed-size single-buffer. For multi-instrument:
- Consider a `HashMap<InstrumentId, Arc<RingBuffer>>` or a flat array of buffers indexed by a compact instrument ID.
- Avoid dynamic allocation in the hot path — pre-allocate all buffers at startup.

### 6. x86_64 portability
`main.rs` and `multi_threaded.rs` contain ARM64-only inline assembly and macOS-only APIs. To run on Linux/x86:
- Wrap `pthread_set_qos_class_self_np` in `#[cfg(target_os = "macos")]` with a Linux `sched_setaffinity` equivalent.
- Replace ARM64 NEON asm in the trading loop with AVX2/AVX-512 equivalents behind `#[cfg(target_arch = "x86_64")]`.
- `one_threaded.rs` already has the x86_64 path — use it as reference.

---

## Invariants to never break

1. **`MarketTick` must remain 64 bytes.** If fields are added, adjust `_unused` accordingly. Verify with `assert_eq!(std::mem::size_of::<MarketTick>(), 64)`.
2. **Only one thread writes `latest_idx`.** The lock-free reader pattern depends on single-writer semantics. Multi-writer requires a different synchronization primitive (e.g., a ticket lock or LMAX Disruptor-style sequence barrier).
3. **No mutex or condvar in the trading loop.** Any blocking synchronization in the hot path defeats the purpose. If you need to communicate to the strategy thread, use an atomic flag or another ring buffer.
4. **No heap allocation in the hot path.** `Box`, `Vec`, `String` — none of these in `trading_strategy()` or the tick processing path.
5. **`Ordering::Acquire` on read, `Ordering::Release` on write.** Relaxed ordering on the ring buffer cursor would introduce data races on weakly-ordered architectures (including ARM).

---

## Key Rust features in use

| Feature                             | Where                                       | Why                                           |
|-------------------------------------|---------------------------------------------|-----------------------------------------------|
| `std::arch::asm!`                   | main.rs, multi_threaded.rs, one_threaded.rs | Direct SIMD instruction control               |
| `AtomicU64` with Acquire/Release    | models.rs, main.rs                          | Lock-free producer/consumer                   |
| `Arc<T>`                            | main.rs                                     | Shared ownership of RingBuffer across threads |
| `#[repr(C, align(64))]`             | models.rs                                   | Cache-line alignment                          |
| `std::hint::spin_loop()`            | main.rs                                     | YIELD/PAUSE hint in busy-wait                 |
| `unsafe extern "C"`                 | main.rs                                     | Call macOS-private pthread QOS API            |
| `#[inline(always)]`                 | main.rs                                     | Prevent hot-path function call overhead       |
| `#[target_feature(enable = "...")]` | one_threaded.rs                             | Safe SIMD dispatch                            |

---

## Platform assumptions

- **Primary target:** macOS, Apple Silicon (M-series), ARM64.
- `pthread_set_qos_class_self_np` is macOS-only.
- ARM64 NEON inline assembly in `main.rs` and `multi_threaded.rs` will not compile on x86.
- `one_threaded.rs` is the only file with cross-platform SIMD support.
- No Linux testing has been done. Linux support is a future concern.
