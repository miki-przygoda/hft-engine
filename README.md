# rust-hft-software

An attempt at building a high-frequency trading engine from scratch in Rust, targeting Apple Silicon (ARM64). This is a starting point — the architecture is intentionally low-level and latency-focused from day one.

---

## What's been built so far

### Core data model (`src/models.rs`)

The foundation. Two structs with aggressive memory layout control:

- **`MarketTick`** — represents a single market data event: price (f32), volume (f32), and a sequence number (u64). Padded to exactly 64 bytes with `_unused: [u8; 44]` and `#[repr(C, align(64))]`. The goal is one tick per CPU cache line — no false sharing, no partial cache-line loads.

- **`RingBuffer`** — a lock-free circular buffer holding 1024 `MarketTick`s. The write head is an `AtomicU64` (`latest_idx`), so readers can poll it without a mutex. The whole struct is also 64-byte aligned to prevent the buffer itself from straddling cache lines.

No heap allocations, no dynamic sizing, no locks.

### Trading strategy entry point (`src/main.rs`)

The main thread is the trading strategy. The background thread is the "market feed" (currently a stub that increments a sequence number every second). Here's what's notable:

- **Thread pinning to performance cores** via Apple's `pthread_set_qos_class_self_np` with QOS class `0x21` (`QOS_CLASS_USER_INTERACTIVE`). On Apple Silicon this biases the thread toward P-cores.
- **CPU warmup** — before entering the trading loop, the strategy burns through 10,000 NEON `fmul v0.4s` operations to warm up the vector pipeline and pull hot code into instruction cache.
- **Spin-loop polling** — no blocking, no condition variables. The strategy tight-polls `latest_idx` with `Ordering::Acquire` and uses `std::hint::spin_loop()` when idle to yield the CPU pipeline without a context switch.
- **ARM64 NEON inline assembly** for the actual "decision" — loads 4x f32 from the tick pointer into a NEON register, multiplies, extracts the low bit as a buy/sell signal. This is a placeholder for real signal logic but the mechanical structure (load tick data directly from memory into SIMD registers, emit a decision in a handful of instructions) is intentional.

### Benchmarking suite (`src/testing_scripts/`)

Built to characterize the hardware before tuning the engine against it.

**`one_threaded.rs`** — single-threaded SIMD throughput benchmark. Detects the best available instruction set at runtime (AVX-512 → AVX2 → SSE on x86_64; NEON on ARM64) and runs 1 billion float multiply operations, reporting billions of ops/sec and nanoseconds per million ops. Used to establish peak single-core throughput as a ceiling for what the trading loop can realistically achieve.

**`multi_threaded.rs`** ("The Kraken") — all-core stress test targeting Apple Silicon M3 Max. Does a progressive thermal ramp (spins up cores one at a time over 10 seconds before launching the full benchmark) to avoid thermal throttling skewing results. Then runs 1 billion NEON operations per thread in parallel and measures aggregate throughput and per-op latency. Useful for understanding how much headroom exists when the engine is running alongside other processes.

---

## Architecture decisions made early

- **Zero external dependencies.** Everything is std + inline assembly. The goal is to understand and own every microsecond.
- **Cache-line alignment everywhere.** `#[repr(C, align(64))]` on structs that cross thread boundaries is not optional — false sharing is a real problem at HFT latencies.
- **Atomics over mutexes.** The ring buffer uses a single `AtomicU64` as a write cursor. Readers derive their read position from it. No lock contention.
- **Inline assembly for hot paths.** The decision logic is ARM64 NEON assembly. This is intentional — the JIT-like nature of LLVM means you can't always guarantee the instruction sequence you want without dropping to asm.

---

## What doesn't exist yet

- Real market data ingestion (the feed thread is a stub)
- Order submission / exchange connectivity
- Actual signal logic (the NEON decision is a bit-extraction placeholder)
- Position management, risk limits, P&L tracking
- Proper benchmarking framework (currently just printing to stdout)
- Any x86_64 path in the main trading loop (it's ARM64-only right now)

---

## Running it

```bash
cargo run
```

Requires macOS on Apple Silicon for the `pthread_set_qos_class_self_np` call and the ARM64 inline assembly in `main.rs` and `multi_threaded.rs`.
