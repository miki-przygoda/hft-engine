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

- **`OrderBook`** — holds the `TradeLog`. Cache-line aligned with `#[repr(C, align(64))]`.

- **`OrderRing`** — lock-free SPSC ring connecting the strategy thread to the in-process exchange. Each `OrderEntry` carries sequence, trade log slot index, and `order_send_ns`.

### Engine (`src/engine.rs`)

All runtime logic lives here. Six functions:

| Function | Role |
|----------|------|
| `run_ingestor` | Binds UDP port 34254, spin-polls, writes ticks into the ring buffer |
| `run_in_process_exchange` | Consumes `OrderRing` entries, writes `round_trip_ns` back to the trade log |
| `run_watchdog` | 500ms poll loop — shuts down after 10s idle or 30s with no feed |
| `run_market_simulator` | Internal simulator — sends 10 warmup + 100 real UDP packets |
| `trading_strategy` | Hot path — NEON warmup, spin-poll, signal logic, trade log write |
| `print_stats` / `write_log` | Print latency report to stdout and save JSON to disk |

### Trading strategy (`src/main.rs`)

Five threads with clearly separated responsibilities:

| Thread | Role | Priority |
|--------|------|----------|
| Watchdog | Idle detection and shutdown | default |
| Exchange | In-process order confirmation | `QOS_USER_INTERACTIVE` |
| Ingestor | UDP receive → ring buffer write | `QOS_USER_INTERACTIVE` |
| Simulator | Internal market data source | default |
| Strategy | Trading hot path | `QOS_USER_INTERACTIVE` |

The strategy thread spins on two `Arc<AtomicBool>` greenlights (ingestor ready + exchange ready) before entering the trading loop. The main thread joins the strategy thread and blocks until the watchdog calls `process::exit`.

**Hot path design:**
- **NEON warmup** — 10,000 `fmul v0.4s` operations + `elapsed()` calls before entering the loop. Warms vector execution units, pulls code into L1 instruction cache, and commits OS pages for the trade log array (avoiding page faults on first write).
- **Spin-poll** — tight `load(Acquire)` loop with `spin_loop()` (YIELD on ARM64) in the idle branch. No blocking, no condvars.
- **PRFM prefetch** — idle branch emits `PRFM PSTL1KEEP` for the next trade log slot, fetching it into L1 in exclusive (store-ready) state before the next tick arrives.
- **`black_box` hints** — warmup loop outputs and the branchless trigger expression are wrapped in `black_box` to prevent the compiler from eliminating or transforming them.
- **Branchless trigger** — `(black_box(decision) | (current_seq & 1)) != 0` replaces short-circuit `||` to remove the branch predictor dependency.
- **Page pre-touch** — all three shared buffers (`RingBuffer`, `OrderRing`, `TradeLog`) are written with `write_volatile` before any threads are spawned, committing OS pages up front and eliminating demand-paging faults during trading.
- **Warmup packets** — the first 10 sequence numbers are treated as warmup. The strategy runs the full hot path but does not commit to the trade log.

**Round-trip path (in-process):**
- Strategy writes a completed `TradeExecution` to the trade log, then pushes an `OrderEntry` (sequence + slot + `order_send_ns`) onto the `OrderRing`.
- The exchange thread spin-polls the `OrderRing`, reads `confirm_recv_ns` via `elapsed()`, and writes `round_trip_ns = confirm_recv_ns - order_send_ns` directly into the trade log slot.
- No kernel boundary crossings — the entire round trip is userspace shared memory.

### Standalone binaries (`src/bin/`)

These exist for external testing and are not required for the standard run:

**`fake-exchange`** — binds port 34255, spin-polls at `QOS_USER_INTERACTIVE`, echoes real order packets (≥ 24 bytes) to port 34256, discards heartbeats.

**`market-simulator`** — sends 10 warmup + 100 real UDP packets to port 34254 with a configurable inter-packet interval.

### Run logging

After each run, the engine writes a JSON file to:
```
logs/v{version}/{date}/{HH-MM-SS}.json
```

Example: `logs/v0.1.1/2026-04-04/22-07-35.json`

The file contains version, timestamp, aggregate stats (avg/min/max for both signal latency and round trip), and the full per-trade record. Version is read from `Cargo.toml` at compile time via `env!("CARGO_PKG_VERSION")`.

### Benchmarking suite (`src/testing_scripts/`)

**`one_threaded.rs`** — single-threaded SIMD throughput benchmark. Detects AVX-512 → AVX2 → SSE on x86_64 or NEON on ARM64 at runtime, runs 1 billion float multiplies. Establishes peak single-core throughput as a ceiling for the trading loop.

**`multi_threaded.rs`** ("The Kraken") — all-core stress test for Apple Silicon. Progressive thermal ramp, then 1 billion NEON ops per thread in parallel. Measures aggregate throughput and per-op latency across all P-cores and E-cores.

---

## Architecture decisions

- **Zero external dependencies.** Everything is std + inline assembly.
- **Self-contained simulation.** The `trading-engine` binary spawns the market simulator and in-process exchange internally — no separate processes required.
- **Cache-line alignment everywhere.** `#[repr(C, align(64))]` on all structs that cross thread boundaries.
- **Atomics over mutexes.** The ring buffer, trade log, and order ring all use atomics. No lock contention in the hot path.
- **`start_time` co-located with `latest_idx`** in `RingBuffer`. The spin-poll loop reads `latest_idx` on every iteration — `start_time` in the same cache line means it is always L1-hot at zero extra cost.
- **In-process exchange** over external UDP echo. Eliminates all kernel crossings from the round-trip path, dropping round-trip from ~43–135µs to ~83–625ns.
- **`black_box` + branchless trigger** to prevent the compiler from eliminating warmup work or transforming the hot-path branch.
- **Power-of-2 buffer sizes** with bitmask indexing (`& MASK`) replacing `% SIZE` modulo operations.

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

### Run the full simulation

```bash
cargo run --release --bin trading-engine
```

The binary is self-contained — it spawns the market simulator and in-process exchange internally. No external processes needed.

### Expected output

```
[engine] starting — running full simulation in-process
[engine] all systems ready — entering trading loop
Total trades executed: 50

Sequence     Sig Latency (ns)     Round Trip (ns)
───────────────────────────────────────────────────────
11           583                  625
13           125                  83
15           167                  125
...
───────────────────────────────────────────────────────
Signal latency — Avg:     214 ns  Min:      83 ns  Max:     583 ns
Round trip     — Avg:     174 ns  Min:      83 ns  Max:     625 ns
[log] saved → logs/v0.1.1/2026-04-04/22-07-35.json
```

**Key metrics:**
- **Sig Latency** — nanoseconds from packet ingest to buy signal trigger. Pure userspace: spin-poll detection + NEON asm + hardware timer read. Typical range: 83–600ns.
- **Round Trip** — nanoseconds from order submission to in-process confirmation. Entirely userspace shared memory — no syscalls. Typical range: 83–625ns.

### Run the standalone binaries (optional)

```bash
# External exchange + engine + simulator (three-process mode)
./target/release/fake-exchange &
./target/release/trading-engine &
./target/release/market-simulator
```

In three-process mode the round trip crosses kernel UDP boundaries, producing latencies in the 43–135µs range.

### Run benchmarks

```bash
cargo run --release --bin bench-one-threaded
cargo run --release --bin bench-multi-threaded
```

### Platform requirements

Requires macOS on Apple Silicon for `pthread_set_qos_class_self_np` and the ARM64 NEON inline assembly in `engine.rs` and `multi_threaded.rs`.
