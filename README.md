# rust-hft-software

A high-frequency trading engine built from scratch in Rust, targeting Apple Silicon (ARM64). Every architectural decision is evaluated in terms of nanoseconds. This is an educational and research project with production-grade engineering standards.

---

## What's been built

### Core data model (`src/models.rs`)

Five structs with aggressive memory layout control:

- **`MarketTick`** — one market data event: price (f32), volume (f32), sequence (u64), ingest timestamp (u64). Padded to exactly 64 bytes with `_unused: [u8; 36]` and `#[repr(C, align(64))]`. One tick per CPU cache line — no false sharing, no partial cache-line loads.

- **`RingBuffer`** — lock-free circular buffer holding 1024 `MarketTick`s. The write head is an `AtomicU64` (`latest_idx`). Critically, `start_time: Instant` sits immediately after `latest_idx` in the same 64-byte cache line — so the strategy's spin-poll `load(Acquire)` on `latest_idx` keeps `start_time` in L1 for free, eliminating the cold-start penalty on timestamp reads.

- **`TradeExecution`** — record of one completed trade: sequence, ingest time, buy time, signal latency, order send time, and round-trip time. Six `u64` fields (48 bytes). `Copy + Clone`.

- **`TradeLog`** — lock-free single-writer trade log backed by `UnsafeCell<[TradeExecution; 1024]>` + an `AtomicU64` write cursor. The strategy writes all fields then does `fetch_add(1, Release)` to commit. The stats thread reads with `load(Acquire)`. No mutex, no heap allocation in the hot path.

- **`OrderBook`** — holds `buy_count: AtomicU64` and the `TradeLog`. The `start_time` that was previously here has been moved into `RingBuffer` for the cache co-location optimization above.

### Trading strategy (`src/main.rs`)

Five threads with clearly separated responsibilities:

| Thread | Role | Priority |
|--------|------|----------|
| Main | Trading strategy — the hot path | `QOS_USER_INTERACTIVE` |
| Ingestor | UDP receive → ring buffer write | `QOS_USER_INTERACTIVE` |
| Confirm receiver | Exchange ack → trade log round-trip write | engine process |
| Heartbeat | Keep exchange path warm | default |
| Stats monitor | Print latency report after execution | default |

**Hot path design:**
- **NEON warmup** — 10,000 `fmul v0.4s` operations + `elapsed()` calls before entering the loop. Warms vector execution units, pulls code into L1 instruction cache, and commits OS pages for the trade log array (avoiding page faults on first write).
- **Spin-poll** — tight `load(Acquire)` loop with `spin_loop()` (YIELD on ARM64) in the idle branch. No blocking, no condvars.
- **PRFM prefetch** — idle branch emits `PRFM PSTL1KEEP` for the next trade log slot, fetching it into L1 in exclusive (store-ready) state before the next tick arrives.
- **Lock-free trade log** — replaced the original `Mutex<Vec<TradeExecution>>` with the `TradeLog` ring. Eliminated mutex acquisition from the hot path entirely.
- **Warmup packets** — the first 10 sequence numbers are treated as warmup. The strategy runs the full hot path (NEON asm, elapsed, lock-free write path) but does not commit to the trade log. This trains the branch predictor and warms all hot-path cache lines before real measurement begins.

**Round-trip path:**
- Strategy commits the trade log entry, then sends a 24-byte UDP order packet to the fake exchange (port 34255). The packet carries sequence, trade log slot index, and `order_send_ns`.
- A dedicated heartbeat thread sends 1-byte packets to the exchange every 1ms, keeping the exchange process awake and the kernel networking path warm between real orders.
- The fake exchange spin-polls port 34255 at `QOS_USER_INTERACTIVE`. Real orders (≥ 24 bytes) are echoed immediately to port 34256. Heartbeats (< 24 bytes) are discarded.
- The confirm receiver spin-polls port 34256. On receipt it reads `confirm_recv_ns`, looks up the trade log slot directly (no scan — slot index is in the packet), and writes `round_trip_ns`.

### Market simulator (`src/bin/market-simulator.rs`)

Sends packets in two phases:
1. **Warmup phase** — 10 packets with no inter-packet delay. These exercise the full hot path in the engine to warm caches and the branch predictor without polluting the latency measurements.
2. **Real trading phase** — 40 packets at 50ms intervals. Triggers fire on odd sequence numbers (sequences 11, 13, 15, … 49), producing 20 recorded trades.

### Fake exchange (`src/bin/fake-exchange.rs`)

Simulates the matching engine side of the round-trip:
- Binds to port 34255, spin-polls with `QOS_USER_INTERACTIVE`.
- Real order packets (24 bytes) are echoed immediately to port 34256.
- Heartbeat packets (< 24 bytes) are discarded — they exist only to keep the process awake and the kernel path warm.

### Benchmarking suite (`src/testing_scripts/`)

**`one_threaded.rs`** — single-threaded SIMD throughput benchmark. Detects AVX-512 → AVX2 → SSE on x86_64 or NEON on ARM64 at runtime, runs 1 billion float multiplies. Establishes peak single-core throughput as a ceiling for the trading loop.

**`multi_threaded.rs`** ("The Kraken") — all-core stress test for Apple Silicon. Progressive thermal ramp, then 1 billion NEON ops per thread in parallel. Measures aggregate throughput and per-op latency across all P-cores and E-cores.

---

## Architecture decisions

- **Zero external dependencies.** Everything is std + inline assembly.
- **Cache-line alignment everywhere.** `#[repr(C, align(64))]` on all structs that cross thread boundaries.
- **Atomics over mutexes.** The ring buffer, trade log, and buy counter all use atomics. No lock contention in the hot path.
- **`start_time` co-located with `latest_idx`** in `RingBuffer`. The spin-poll loop reads `latest_idx` on every iteration — `start_time` in the same cache line means it is always L1-hot at zero extra cost.
- **Lock-free trade log** over `Mutex<Vec>`. The original design paid a mutex acquisition on every triggered trade. The `TradeLog` ring eliminates this.
- **Warmup packets** over PRFM-only cache warming. PRFM is a hint the CPU can ignore. Warmup packets force the full hot path to actually execute, guaranteeing cache and branch predictor state.
- **Spin-poll exchange + heartbeat** for round-trip measurement. Eliminates OS wakeup latency (~10–30µs per wakeup) from the exchange and confirm paths.

---

## What doesn't exist yet

- Real market data ingestion (UDP ingestor exists, but no kernel-bypass networking)
- Actual signal logic (the NEON decision is a placeholder bit-extraction)
- Position management, risk limits, P&L tracking
- Multi-instrument support (currently single-instrument ring buffer)
- x86_64 path in the trading loop (ARM64 NEON only)
- Real exchange connectivity (FIX/OUCH/binary protocol)

---

## Running it

### Build everything

```bash
cargo build --release
```

### Run the full simulation (signal latency + round-trip)

```bash
./target/release/fake-exchange & ./target/release/trading-engine & sleep 1.5 && ./target/release/market-simulator & sleep 8 && killall trading-engine fake-exchange 2>/dev/null; sleep 0.5
```

All three processes must be running. The fake exchange must start before the trading engine attempts to send orders.

### Expected output

The system runs silently during execution. After 5 seconds, it prints the full latency report:

```
Total trades executed: 20

Sequence     Sig Latency (ns)     Round Trip (ns)
───────────────────────────────────────────────────────
11           208                  57625
13           167                  44458
15           250                  50750
17           208                  65958
...
───────────────────────────────────────────────────────
Signal latency — Avg:     389 ns  Min:     167 ns  Max:    1125 ns
Round trip     — Avg:   68000 ns  Min:   43167 ns  Max:  135666 ns
```

**Key metrics:**
- **Sig Latency** — nanoseconds from packet ingest to buy signal trigger. Pure userspace: spin-poll detection + NEON asm + hardware timer read. Typical range: 125–600ns.
- **Round Trip** — nanoseconds from order submission to confirmation received. Crosses the kernel 4 times (2 sends, 2 recvs) through macOS loopback UDP. Typical range: 43–135µs. The floor (~43µs) is the hard limit of the BSD socket layer without kernel bypass.

**Why the ~163x ratio between signal latency and round trip:** signal latency is entirely in userspace (no syscalls). Round trip mandatorily crosses EL0→EL1 four times plus two process wakeups — categorically different operations.

### Platform requirements

Requires macOS on Apple Silicon for `pthread_set_qos_class_self_np` and the ARM64 NEON inline assembly in `main.rs` and `multi_threaded.rs`.
