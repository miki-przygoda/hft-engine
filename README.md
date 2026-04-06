# rust-hft-software

A high-frequency trading engine built from scratch in Rust, targeting Apple Silicon (ARM64). Every architectural decision is evaluated in terms of nanoseconds. This is an educational and research project with production-grade engineering standards.

---

## What's been built

### Core data model (`src/models.rs`)

- **`MarketTick`** — one market data event: price (f32), volume (f32), sequence (u64), ingest timestamp (u64). Padded to exactly 64 bytes with `_unused: [u8; 36]` and `#[repr(C, align(64))]`. One tick per CPU cache line — no false sharing, no partial cache-line loads.

- **`RingBuffer`** — lock-free circular buffer holding 1024 `MarketTick`s. The write head is an `AtomicU64` (`latest_idx`). `start_time: Instant` sits immediately after `latest_idx` in the same 64-byte cache line — so the strategy's spin-poll `load(Acquire)` on `latest_idx` keeps `start_time` in L1 for free, eliminating the cold-start penalty on timestamp reads.

- **`TradeExecution`** — record of one completed trade: sequence, ingest time, buy time, signal latency, order send time, and round-trip time. Six `u64` fields (48 bytes). `Copy + Clone`.

- **`TradeLog`** — lock-free single-writer trade log backed by `UnsafeCell<[TradeExecution; 1024]>` + an `AtomicU64` write cursor. The strategy writes all fields then does `fetch_add(1, Release)` to commit. No mutex, no heap allocation.

- **`LatencyHistogram`** — fixed-bucket histogram covering 0–10,000 ns (one `u64` bucket per ns) plus an `AtomicU64` overflow counter. `record(ns)` is a single array-index increment. `percentile(p_num, p_den, total)` is an O(n) cumulative walk at shutdown — no sort, no allocation. Two instances live in `OrderBook`: one for signal latency (sole writer: strategy thread), one for round-trip (sole writer: exchange thread).

- **`OrderBook`** — cache-line aligned container holding the trade log, both histograms, and all shared counters:
  - `sig_hist` / `rt_hist` — latency histograms
  - `stall_count: AtomicU64` — incremented when the idle spin gap exceeds 500 ns (OS preemption detector)
  - `gap_count: AtomicU64` — incremented by the ingestor on sequence breaks
  - `dirty: AtomicBool` — set on gap, cleared by strategy after N consecutive clean ticks
  - `halt: AtomicBool` — permanent stop flag; set by the risk layer, never cleared in-session
  - `net_position: AtomicI64` — running long position; sole writer: strategy thread
  - `mem_total_ram` / `mem_rss_start` / `mem_rss_ready: AtomicU64` — memory snapshots written at startup (see below)

- **`OrderRing`** — lock-free SPSC ring connecting the strategy thread to the in-process exchange. Each `OrderEntry` carries sequence, trade log slot index, and `order_send_ns`. One cache line per entry (`#[repr(C, align(64))]`).

### Engine (`src/engine.rs`)

All runtime logic. Key functions:

| Function | Role |
|----------|------|
| `run_ingestor` | Binds UDP 34254, spin-polls, detects sequence gaps, writes ticks into the ring buffer |
| `run_in_process_exchange` | Consumes `OrderRing` entries, writes `round_trip_ns` back to the trade log |
| `run_watchdog` | Spin-based idle detector — shuts down after 10s idle or 30s with no feed |
| `run_market_simulator` | Burst-mode simulator — 10 warmup packets, then 20 bursts × 50 packets at 20µs spacing |
| `trading_strategy` | Hot path — NEON warmup, spin-poll, momentum signal, risk checks, trade log write |
| `print_stats` / `write_log` | Print latency report (with percentiles) to stdout and save JSON to disk |
| `collect_memory_stats` | POSIX `getrusage` + BSD `sysctl` — peak RSS and total physical RAM, no allocation |

### Trading strategy (`src/main.rs`)

Five threads with clearly separated responsibilities:

| Thread | Role | Priority |
|--------|------|----------|
| Watchdog | Idle detection and shutdown | default |
| Exchange | In-process order confirmation | `QOS_USER_INTERACTIVE` |
| Ingestor | UDP receive → ring buffer write | `QOS_USER_INTERACTIVE` |
| Simulator | Internal burst market data source | default |
| Strategy | Trading hot path | `QOS_USER_INTERACTIVE` |

The strategy thread spins on two `Arc<AtomicBool>` greenlights (ingestor ready + exchange ready) before entering the trading loop. It then calls `set_thread_affinity_tag(1)` to hint the scheduler to keep it on the same P-core cluster. The main thread joins the strategy thread and blocks until the watchdog calls `process::exit`.

**Hot path design:**
- **NEON warmup** — 10,000 `fmul v0.4s` operations + `elapsed()` calls before the loop. Warms vector execution units, pulls code into the instruction cache, and commits OS pages for the trade log array.
- **Spin-poll** — tight `load(Acquire)` loop with `spin_loop()` (YIELD on ARM64) in the idle branch.
- **PRFM prefetch** — idle branch emits `PRFM PSTL1KEEP` for the next trade log slot, fetching it into L1 in exclusive state before the next tick arrives.
- **Warmup packets** — the first 10 sequences run the full hot path but do not commit to the trade log, training the branch predictor and warming all hot-path cache lines.
- **Page pre-touch** — all three shared buffers (`RingBuffer`, `OrderRing`, `TradeLog`) are written with `write_volatile` before threads are spawned, eliminating demand-paging faults during trading.

**Momentum signal (ARM64):**

The 8-price momentum window lives entirely in two NEON registers (`v28`/`v29`) across loop iterations — no L1 access for window state between ticks. On each tick:

1. `LD1` loads 16 bytes from the new tick.
2. `EXT` shifts each register by one f32 lane — O(1) window slide, one instruction.
3. Two `FADDP` passes collapse the 8-lane sum; `FMUL` scales to the mean.
4. `FCMGT` compares current price to `mean * (1 + threshold)` — result is the trigger bit.

Total signal computation: ~6 NEON instructions, one tick load, zero window memory accesses.

**x86_64 fallback:** an equivalent scalar `[f32; 8]` window on the stack (L1-resident) with a horizontal sum and comparison. Same structure as the ARM64 path, no inline asm required.

**Risk layer:**
- Halt check at top of hot path (`halt.load(Relaxed)`) — `#[cold]` on the halt function biases the branch predictor toward the safe (not-taken) path after warmup.
- Position limit — if `net_position >= MAX_POSITION`, calls `halt_trading` (permanent stop).
- Gap kill switch — if `gap_count > MAX_GAP_COUNT`, calls `halt_trading`.
- Dirty-flag recovery — on gap, strategy skips trades until `CLEAN_SEQ_THRESHOLD` (5) consecutive clean ticks.
- `net_position` incremented with `fetch_add(1, Relaxed)` on each committed long trade.

**Stall detection:**
In the idle spin branch, `elapsed_ns` is called on each iteration. If the gap between consecutive `spin_loop()` calls exceeds 500 ns, `stall_count` is incremented. A high stall count at shutdown is direct evidence of OS preemption on the strategy thread. Measured 5,000–7,000 stalls per run during normal laptop use.

**Round-trip path (in-process):**
- Strategy writes a completed `TradeExecution` to the trade log, then pushes an `OrderEntry` onto the `OrderRing`.
- The exchange thread spin-polls the `OrderRing`, reads `confirm_recv_ns`, and writes `round_trip_ns` directly into the trade log slot via the `slot` index.
- No kernel boundary crossings — the entire round trip is userspace shared memory.

### Sequence gap detection (`run_ingestor`)

The ingestor tracks `last_ingest_seq`. On each received packet, if `recv_seq != last_ingest_seq + 1`:
- `gap_count.fetch_add(1, Relaxed)` — cumulative gap counter for stats and the kill switch.
- `dirty.store(true, Relaxed)` — signals the strategy to pause.

The strategy clears `dirty` only after `CLEAN_SEQ_THRESHOLD` consecutive clean ticks, preventing a single-packet recovery from masking a wider feed problem.

### Watchdog (spin-based)

The watchdog replaced `thread::sleep(500ms)` with a spin loop that checks elapsed time every 2^24 (~16M) iterations, amortising the `elapsed_ns` call cost. The check interval is 500ms by comparison, but the watchdog never surrenders its thread to the OS scheduler between checks. This prevents the OS from using the watchdog's wakeup as a pretext to reschedule the strategy thread.

### Memory snapshots

Four memory snapshots are taken across the process lifetime using POSIX `getrusage` (peak RSS) and BSD `sysctl` `[CTL_HW, HW_MEMSIZE]` (total physical RAM). No external dependencies, no heap allocation.

| Snapshot | When |
|----------|------|
| `[1] start` | Before any buffer allocation in `main()` |
| `[2] after ready` | After all buffers are pre-touched, before threads spawn |
| `[3] before log` | In the watchdog, immediately before `print_stats`/`write_log` |
| `[4] after log` | In the watchdog, immediately after `write_log` — printed to stdout only |

Snapshots 1–3 appear in both stdout and the JSON log. Snapshot 4 is a final stdout line showing whether the log write itself caused any RSS growth.

### Simulation

**`run_market_simulator`** sends bursts to stress-test the ring buffer and signal logic:
- 10 warmup packets (flat price, full hot path but no trade log commits).
- 20 bursts × 50 real packets at 20 µs intra-burst spacing.
- 500 ms silence between bursts to expose the full OS-jitter profile via stall counting.
- Price follows `100.0 + 5.0 * sin(seq * 0.1)` — oscillates between ~95 and ~105, giving the momentum signal non-trivial input.

Total real packets: 1000. Expected trade count with default threshold: ~750–800 per run.

### Standalone binaries (`src/bin/`)

**`fake-exchange`** — binds port 34255, spin-polls at `QOS_USER_INTERACTIVE`, echoes real order packets (≥ 24 bytes) to port 34256, discards heartbeats. Used when measuring external UDP round-trip latency across kernel boundaries.

**`market-simulator`** — sends 10 warmup + 100 real UDP packets to port 34254. Used with `fake-exchange` for three-process external testing.

### Run logging

After each run, the engine writes a JSON file to:
```
logs/v{version}/{date}/{HH-MM-SS}.json
```

Contains: version, timestamp, total trades, net position, halt/stall/gap counts, four-bucket memory snapshot, signal latency stats (avg/min/max/p50/p95/p99/p99.9), round-trip stats, and the full per-trade array. Version is read from `Cargo.toml` at compile time via `env!("CARGO_PKG_VERSION")`.

### Benchmarking suite (`src/testing_scripts/`)

**`one_threaded.rs`** — single-threaded SIMD throughput benchmark. Detects AVX-512 → AVX2 → SSE on x86_64 or NEON on ARM64, runs 1 billion float multiplies. Establishes peak single-core throughput as a ceiling for the trading loop.

**`multi_threaded.rs`** ("The Kraken") — all-core stress test for Apple Silicon. Progressive thermal ramp, then 1 billion NEON ops per thread in parallel. Measures aggregate throughput and per-op latency across all P-cores and E-cores.

---

## Architecture decisions

- **Zero external dependencies.** Everything is std + inline assembly.
- **Self-contained simulation.** The `trading-engine` binary spawns the market simulator and in-process exchange internally — no separate processes required.
- **Cache-line alignment everywhere.** `#[repr(C, align(64))]` on all structs that cross thread boundaries.
- **Atomics over mutexes.** The ring buffer, trade log, and order ring all use atomics. No lock contention in the hot path.
- **`start_time` co-located with `latest_idx`** in `RingBuffer`. The spin-poll loop reads `latest_idx` on every iteration — `start_time` in the same cache line is always L1-hot at zero extra cost.
- **In-process exchange** over external UDP echo. Eliminates all kernel crossings from the round-trip path, dropping round-trip from ~43–135µs to ~83–625ns.
- **Register-resident window** in the NEON signal path. Keeping the 8-price momentum window in `v28`/`v29` across loop iterations costs one `EXT` instruction per tick instead of an L1 load.
- **`#[cold]` on the halt path.** Biases the branch predictor toward the safe (not-taken) direction without requiring nightly intrinsics.
- **Spin-based watchdog** instead of `thread::sleep`. Prevents the OS from using the watchdog's wakeup event as a scheduling opportunity against the strategy thread.
- **`Instant::elapsed()` for timing**, not `mach_absolute_time()` FFI. The commpage fast path is empirically faster (~42x) and returns nanoseconds directly.
- **Power-of-2 buffer sizes** with bitmask indexing (`& MASK`) replacing `% SIZE` modulo.

---

## What doesn't exist yet

- Real market data ingestion (kernel-bypass networking, multicast feed)
- Actual profitable signal logic (current momentum signal is structural, not calibrated)
- Real exchange connectivity (FIX / OUCH / binary protocol)

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
Total trades executed: 762

Sequence     Sig Latency (ns)     Round Trip (ns)
───────────────────────────────────────────────
11           208                  250
12           125                  166
13           83                   83
...
───────────────────────────────────────────────
Signal latency — Avg:     174 ns  Min:      83 ns  Max:     583 ns
                p50:     166 ns  p95:     291 ns  p99:     416 ns  p99.9:     541 ns
Round trip     — Avg:     157 ns  Min:      83 ns  Max:     541 ns
                p50:     125 ns  p95:     250 ns  p99:     374 ns  p99.9:     499 ns
───────────────────────────────────────────────
OS stalls (>500ns spin gap): 5842  |  Sequence gaps: 0  |  Net position: 762  |  Halt: false
───────────────────────────────────────────────
Memory — Total RAM: 16384 MB
  [1] start          Peak RSS:   8 MB
  [2] after ready    Peak RSS:  14 MB
  [3] before log     Peak RSS:  18 MB
[log] saved → logs/v0.1.1/2026-04-06/14-22-07.json
[mem] snapshot [4] after log write  — Peak RSS:  18 MB
```

**Key metrics:**
- **Sig Latency** — nanoseconds from packet ingest to buy signal trigger. Pure userspace: spin-poll detection + NEON asm + hardware timer read. Typical range: 83–600 ns.
- **Round Trip** — nanoseconds from order submission to in-process confirmation. Entirely userspace shared memory — no syscalls. Typical range: 83–625 ns.
- **OS stalls** — count of idle-spin gaps > 500 ns. Direct proxy for OS preemption events on the strategy thread.
- **Memory snapshots** — peak RSS at four points in the process lifetime, in MB.

### Run the standalone binaries (optional)

```bash
# External exchange + engine + simulator (three-process mode)
./target/release/fake-exchange &
./target/release/trading-engine &
./target/release/market-simulator
```

In three-process mode the round trip crosses kernel UDP boundaries, producing latencies in the 43–135µs range (~163x higher than in-process).

### Run benchmarks

```bash
cargo run --release --bin bench-one-threaded
cargo run --release --bin bench-multi-threaded
```

### Platform requirements

Requires macOS on Apple Silicon for `pthread_set_qos_class_self_np`, `thread_policy_set`, and the ARM64 NEON inline assembly in `engine.rs` and `multi_threaded.rs`. x86_64 compilation is gated behind `#[cfg(target_arch = "aarch64")]` guards throughout; the x86_64 fallback path in `trading_strategy` uses scalar arithmetic only.
