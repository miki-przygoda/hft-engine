//! All engine runtime logic: the threads, the hot path, and reporting.
//!
//! This module holds every long-running routine the threads execute —
//! [`run_ingestor`], [`run_in_process_exchange`], [`run_watchdog`],
//! [`run_market_simulator`], and the hot path [`trading_strategy`] — plus the
//! cross-platform scheduling helpers ([`set_qos_interactive`],
//! [`set_thread_affinity_tag`]), memory-stat collection, and the latency
//! reporting / JSON logging. The signal computation has two register-resident
//! SIMD implementations selected at compile time: NEON on `aarch64`, AVX2 on
//! `x86_64`. See `CLAUDE.md` for the design rationale and invariants.

use std::arch::asm;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::float32x4_t;
use std::hint::black_box;
use std::io::Write;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{models, BUFFER_SIZE, ORDER_RING_SIZE, ROUND_TRIP_LOG_SIZE, SIGNAL_SERIES_LEN, TRADE_LOG_SIZE};
use rust_hft_software::config::{
    BURST_GAP_MS, BURST_SIZE, CLEAN_SEQ_THRESHOLD, INGESTOR_ADDR,
    MAX_GAP_COUNT, MAX_POSITION, NUM_BURSTS, SIGNAL_MOMENTUM_BPS, WARMUP_PACKETS,
};

const BUFFER_MASK:     u64   = (BUFFER_SIZE     - 1) as u64;
const TRADE_LOG_MASK:  usize = TRADE_LOG_SIZE   - 1;
const ORDER_RING_MASK: usize = ORDER_RING_SIZE   - 1;

// macOS-only: elevate to user-interactive QOS class (P-core scheduling bias).
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}

// macOS-only: Mach thread affinity API (item 6).
// On Apple Silicon, THREAD_AFFINITY_POLICY provides a grouping hint — the OS
// will try to co-schedule threads with the same tag on the same cluster. It is
// NOT a hard pin (unlike Linux CPU_SET), but it reduces cross-cluster migration.
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn mach_thread_self() -> u32;
    fn thread_policy_set(thread: u32, flavor: u32, policy_info: *const i32, count: u32) -> i32;
}

// Memory measurement APIs.
//   getrusage() — POSIX: peak RSS and other process resource usage.
//   sysctl()    — BSD: read kernel parameters via numeric MIB (used for hw.memsize).
// Both are always available on macOS and Linux; the sysctl MIBs differ by OS.
unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut RUsage) -> i32;
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn sysctl(name: *const i32, namelen: u32, oldp: *mut u8, oldlenp: *mut usize,
              newp: *const u8, newlen: usize) -> i32;
}

// Partial struct rusage layout for 64-bit macOS/Linux.
// Only the fields up to and including ru_maxrss are needed.
// On macOS: ru_maxrss is in bytes.
// On Linux: ru_maxrss is in kilobytes.
// Padding after each timeval accounts for the 8+4+[4] layout on 64-bit targets.
#[repr(C)]
struct RUsage {
    utime_sec:  i64,       // timeval tv_sec  (offset  0)
    utime_usec: i32,       // timeval tv_usec (offset  8)
    _pad0:      i32,       //                 (offset 12)
    stime_sec:  i64,       // timeval tv_sec  (offset 16)
    stime_usec: i32,       // timeval tv_usec (offset 24)
    _pad1:      i32,       //                 (offset 28)
    maxrss:     i64,       // peak RSS        (offset 32)
    _rest:      [i64; 13], // remaining fields (not read)
}

// Snapshot of memory figures at a single point in time.
// All values are in bytes. On unsupported targets all fields are 0.
pub(crate) struct MemoryStats {
    /// Total physical RAM installed in the system.
    pub total_ram:   u64,
    /// Peak resident set size since process start (bytes).
    pub peak_rss:    u64,
}

/// Collect memory statistics for the current process and the host system.
/// Uses only POSIX getrusage + BSD sysctl — no external dependencies.
pub(crate) fn collect_memory_stats() -> MemoryStats {
    let peak_rss: u64 = unsafe {
        const RUSAGE_SELF: i32 = 0;
        let mut ru: RUsage = core::mem::zeroed();
        getrusage(RUSAGE_SELF, &mut ru);
        // macOS reports bytes; Linux reports kB — normalise to bytes.
        #[cfg(target_os = "macos")]
        { ru.maxrss as u64 }
        #[cfg(not(target_os = "macos"))]
        { ru.maxrss as u64 * 1024 }
    };

    // Total physical RAM via sysctl(CTL_HW=6, HW_MEMSIZE=24) on macOS,
    // or sysconf(_SC_PHYS_PAGES * _SC_PAGE_SIZE) on Linux (no hw.memsize MIB).
    #[cfg(target_os = "macos")]
    let total_ram: u64 = unsafe {
        let mib: [i32; 2] = [6, 24]; // CTL_HW, HW_MEMSIZE
        let mut val: u64 = 0;
        let mut len = core::mem::size_of::<u64>();
        sysctl(mib.as_ptr(), 2, &mut val as *mut u64 as *mut u8,
               &mut len, core::ptr::null(), 0);
        val
    };

    #[cfg(target_os = "linux")]
    let total_ram: u64 = unsafe {
        unsafe extern "C" { fn sysconf(name: i32) -> i64; }
        const SC_PHYS_PAGES: i32 = 85; // _SC_PHYS_PAGES — GNU extension, glibc/musl
        const SC_PAGE_SIZE:  i32 = 30; // _SC_PAGE_SIZE  — POSIX
        let pages     = sysconf(SC_PHYS_PAGES);
        let page_size = sysconf(SC_PAGE_SIZE);
        if pages > 0 && page_size > 0 { pages as u64 * page_size as u64 } else { 0 }
    };

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let total_ram: u64 = 0;

    MemoryStats { total_ram, peak_rss }
}

#[inline(always)]
fn elapsed_ns(start: &std::time::Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

/// Elevate the *calling* thread's scheduling priority: `USER_INTERACTIVE` QOS
/// (P-core bias) on macOS, `SCHED_FIFO` priority 50 on Linux (needs
/// `CAP_SYS_NICE`; silently no-ops otherwise). Called by the strategy, ingestor,
/// and exchange threads only.
pub(crate) fn set_qos_interactive() {
    #[cfg(target_os = "macos")]
    unsafe { pthread_set_qos_class_self_np(0x21, 0); }

    // Linux: elevate the calling thread to SCHED_FIFO at priority 50.
    // Strategy/ingestor/exchange call this; watchdog and simulator do not,
    // so they stay on SCHED_OTHER and cannot block the hot path even if they
    // happen to land on the same core.
    // Requires CAP_SYS_NICE — run the binary with sudo.
    #[cfg(target_os = "linux")]
    unsafe { linux_set_fifo(50); }
}

// Linux: set the calling thread's scheduler to SCHED_FIFO at `priority`.
// Uses a raw syscall (sched_setscheduler, NR=144) to avoid a libc dependency.
// pid=0 targets the calling thread in a multi-threaded process.
#[cfg(target_os = "linux")]
unsafe fn linux_set_fifo(priority: i32) {
    // struct sched_param { int sched_priority; } — single i32 on all Linux ABIs.
    let param: i32 = priority;
    unsafe {
        core::arch::asm!(
            "syscall",
            inlateout("rax") 144i64 => _,        // NR_sched_setscheduler → ret (ignored)
            in("rdi") 0i64,                       // pid = 0 → calling thread
            in("rsi") 1i64,                       // SCHED_FIFO
            in("rdx") &param as *const i32,
            out("rcx") _, out("r11") _,           // clobbered by syscall ABI
            options(nostack),
        );
    }
}

// Linux: pin the calling thread to `core` via sched_setaffinity (NR=203).
// cpu_set_t is represented as a single u64 (sufficient for ≤ 64 cores).
#[cfg(target_os = "linux")]
unsafe fn linux_pin_to_core(core: usize) {
    let mask: u64 = 1u64 << core;
    unsafe {
        core::arch::asm!(
            "syscall",
            inlateout("rax") 203i64 => _,        // NR_sched_setaffinity → ret (ignored)
            in("rdi") 0i64,                       // pid = 0 → calling thread
            in("rsi") 8i64,                       // cpusetsize = sizeof(u64)
            in("rdx") &mask as *const u64,
            out("rcx") _, out("r11") _,
            options(nostack),
        );
    }
}

// Thread affinity tag → dedicated core mapping (i9-9900K layout).
//
// Tag | Thread   | Core | Rationale
// ----|----------|------|------------------------------------------
//  1  | strategy | 2    | isolated hot path, no sharing
//  2  | ingestor | 3    | feeds the ring buffer, needs its own core
//  3  | exchange | 4    | drains the order ring, needs its own core
//
// Cores 0-1 are left for the OS, watchdog, and simulator so they can't
// interfere with the critical threads even under load.
// Adjust the core numbers to match your actual CPU topology if needed.
pub(crate) fn set_thread_affinity_tag(tag: i32) {
    #[cfg(target_os = "macos")]
    unsafe {
        const THREAD_AFFINITY_POLICY: u32 = 4;
        const THREAD_AFFINITY_POLICY_COUNT: u32 = 1;
        let thread = mach_thread_self();
        thread_policy_set(thread, THREAD_AFFINITY_POLICY, &tag, THREAD_AFFINITY_POLICY_COUNT);
    }

    #[cfg(target_os = "linux")]
    {
        let core: usize = match tag {
            1 => 2, // strategy
            2 => 3, // ingestor
            3 => 4, // exchange
            _ => return,
        };
        unsafe { linux_pin_to_core(core); }
    }

    let _ = tag;
}

// Called on any risk-limit breach. #[cold] biases the branch predictor in the
// hot path toward the non-halting (not-taken) direction after the first few
// warmup iterations train it. The halt flag is permanent within a session.
#[cold]
fn halt_trading(order_book: &models::OrderBook, reason: &str) {
    order_book.halt.store(true, Ordering::Relaxed);
    eprintln!("[risk] HALT: {}", reason);
}

/// Ingestor thread: bind the UDP feed socket, spin-poll it, and publish each
/// received tick into the [`RingBuffer`](models::RingBuffer). Stamps every tick
/// with an ingest timestamp and flags sequence gaps via the `dirty` flag. Sole
/// writer of `latest_idx`.
// Emit an order for latency accounting (used by the trading model on each entry
// and exit). Records signal latency, writes a TradeExecution (target/fill left 0
// so it is excluded from the slippage stats — P&L is tracked via round-trips), and
// pushes the order ring so the exchange thread fills in the round-trip time.
#[inline(always)]
unsafe fn emit_latency_order(
    buffer: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_ring: &models::OrderRing,
    current_seq: u64,
    ingest_time_ns: u64,
    transit_est_ns: u64,
) {
    unsafe {
        let buy_time_ns = elapsed_ns(&buffer.start_time);
        let latency_ns  = buy_time_ns.saturating_sub(ingest_time_ns);
        order_book.sig_hist.record(latency_ns);

        let slot = order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize & TRADE_LOG_MASK;
        let entry = &mut (*order_book.trade_log.entries.get())[slot];
        let order_send_ns = elapsed_ns(&buffer.start_time);
        entry.sequence       = current_seq;
        entry.ingest_time_ns = ingest_time_ns;
        entry.buy_time_ns    = buy_time_ns;
        entry.latency_ns     = latency_ns;
        entry.order_send_ns  = order_send_ns;
        entry.round_trip_ns  = 0;
        entry.transit_est_ns = transit_est_ns;
        entry.target_price   = 0.0;
        entry.fill_price     = 0.0;
        order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

        let ring_slot = order_ring.write_idx.load(Ordering::Relaxed) as usize & ORDER_RING_MASK;
        let oe = &mut (*order_ring.entries.get())[ring_slot];
        oe.sequence      = current_seq;
        oe.slot          = slot as u64;
        oe.order_send_ns = order_send_ns;
        order_ring.write_idx.fetch_add(1, Ordering::Release);
    }
}

pub(crate) fn run_ingestor(
    traded: Arc<models::RingBuffer>,
    reference: Arc<models::RingBuffer>,
    order_book: Arc<models::OrderBook>,
    last_packet_ns: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
) {
    let socket = UdpSocket::bind(INGESTOR_ADDR).expect("ingestor: failed to bind");
    socket.set_nonblocking(true).expect("ingestor: failed to set non-blocking");
    ready.store(true, Ordering::Release);

    // Per-instrument write cursors: slot 0 = traded, slot 1 = reference. v3 packets
    // (>= 33 bytes) carry the instrument id at byte 32; older packets are slot 0.
    let mut seq:             [u64; 2] = [1, 1];
    let mut last_ingest_seq: [u64; 2] = [0, 0];
    let mut pkt = [0u8; 64];

    loop {
        match socket.recv_from(&mut pkt) {
            Ok((amt, _)) if amt >= 16 => {
                let id = if amt >= 33 && (pkt[32] as usize) < 2 { pkt[32] as usize } else { 0 };
                let buffer: &models::RingBuffer = if id == 0 { &traded } else { &reference };

                let recv_seq = u64::from_le_bytes(pkt[8..16].try_into().unwrap());

                // Sequence-gap detection. Only the TRADED instrument (slot 0) trips
                // the risk dirty flag — a reference-feed gap degrades the cross
                // signal but must not halt trading.
                if last_ingest_seq[id] > 0 && recv_seq != last_ingest_seq[id] + 1 && id == 0 {
                    order_book.gap_count.fetch_add(1, Ordering::Relaxed);
                    order_book.dirty.store(true, Ordering::Relaxed);
                }
                last_ingest_seq[id] = recv_seq;

                // Observed price range tracked for the traded instrument only.
                if id == 0 {
                    let px = f32::from_le_bytes(pkt[0..4].try_into().unwrap());
                    if px.is_finite() {
                        if px < f32::from_bits(order_book.price_lo_bits.load(Ordering::Relaxed)) {
                            order_book.price_lo_bits.store(px.to_bits(), Ordering::Relaxed);
                        }
                        if px > f32::from_bits(order_book.price_hi_bits.load(Ordering::Relaxed)) {
                            order_book.price_hi_bits.store(px.to_bits(), Ordering::Relaxed);
                        }
                    }
                    // v4: observed spread (bps) + latest funding from bid/ask/funding.
                    if amt >= 49 {
                        let bid = f32::from_le_bytes(pkt[33..37].try_into().unwrap());
                        let ask = f32::from_le_bytes(pkt[37..41].try_into().unwrap());
                        let m = (bid + ask) / 2.0;
                        if m > 0.0 && bid > 0.0 && ask >= bid {
                            let sp = (ask - bid) / m * 10_000.0;  // spread in bps
                            if sp < f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed)) {
                                order_book.spread_lo_bits.store(sp.to_bits(), Ordering::Relaxed);
                            }
                            if sp > f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed)) {
                                order_book.spread_hi_bits.store(sp.to_bits(), Ordering::Relaxed);
                            }
                        }
                        let funding = f32::from_le_bytes(pkt[45..49].try_into().unwrap());
                        if funding.is_finite() {
                            order_book.funding_bits.store(funding.to_bits(), Ordering::Relaxed);
                        }
                    }
                }

                let s = seq[id];
                let idx = (s & BUFFER_MASK) as usize;
                let ingest_time_ns = elapsed_ns(&buffer.start_time);

                unsafe {
                    let tick_ptr = &buffer.ticks[idx] as *const _ as *mut u8;
                    std::ptr::copy_nonoverlapping(pkt.as_ptr(), tick_ptr, 16);
                    *(tick_ptr.add(16) as *mut u64) = ingest_time_ns;
                    // v2+ packets (>= 32 bytes) carry origin_ts + transit_est. All
                    // writes precede the Release store, so the strategy sees them
                    // after its Acquire load (invariant #8).
                    if amt >= 32 {
                        *(tick_ptr.add(24) as *mut u64) =
                            u64::from_le_bytes(pkt[16..24].try_into().unwrap());
                        *(tick_ptr.add(32) as *mut u64) =
                            u64::from_le_bytes(pkt[24..32].try_into().unwrap());
                    } else {
                        *(tick_ptr.add(24) as *mut u64) = 0;
                        *(tick_ptr.add(32) as *mut u64) = 0;
                    }
                    // v4 packets (>= 49 bytes) carry bid/ask/mark/funding at [33..49].
                    if amt >= 49 {
                        *(tick_ptr.add(40) as *mut f32) = f32::from_le_bytes(pkt[33..37].try_into().unwrap());
                        *(tick_ptr.add(44) as *mut f32) = f32::from_le_bytes(pkt[37..41].try_into().unwrap());
                        *(tick_ptr.add(48) as *mut f32) = f32::from_le_bytes(pkt[41..45].try_into().unwrap());
                        *(tick_ptr.add(52) as *mut f32) = f32::from_le_bytes(pkt[45..49].try_into().unwrap());
                    } else {
                        *(tick_ptr.add(40) as *mut f32) = 0.0;
                        *(tick_ptr.add(44) as *mut f32) = 0.0;
                        *(tick_ptr.add(48) as *mut f32) = 0.0;
                        *(tick_ptr.add(52) as *mut f32) = 0.0;
                    }
                }

                buffer.latest_idx.store(s, Ordering::Release);
                last_packet_ns.store(ingest_time_ns, Ordering::Relaxed);
                seq[id] += 1;
            }
            _ => std::hint::spin_loop(),
        }
    }
}

/// Exchange thread: spin-poll the [`OrderRing`](models::OrderRing), and for each
/// order read a confirmation timestamp and write `round_trip_ns` back into the
/// referenced trade-log slot. Crosses zero kernel boundaries — this is what makes
/// the in-process round trip ~163× faster than the external UDP path.
pub(crate) fn run_in_process_exchange(
    order_ring: Arc<models::OrderRing>,
    buffer: Arc<models::RingBuffer>,
    order_book: Arc<models::OrderBook>,
    ready: Arc<AtomicBool>,
) {
    ready.store(true, Ordering::Release);
    let mut local_read_idx: u64 = 0;

    loop {
        let write_idx = order_ring.write_idx.load(Ordering::Acquire);
        if write_idx > local_read_idx {
            let ring_slot = local_read_idx as usize & ORDER_RING_MASK;
            let (slot, order_send_ns) = unsafe {
                let entry = &(*order_ring.entries.get())[ring_slot];
                (entry.slot as usize & TRADE_LOG_MASK, entry.order_send_ns)
            };
            let confirm_recv_ns = elapsed_ns(&buffer.start_time);
            let round_trip_ns = confirm_recv_ns.saturating_sub(order_send_ns);
            unsafe {
                (*order_book.trade_log.entries.get())[slot].round_trip_ns = round_trip_ns;
            }
            order_book.rt_hist.record(round_trip_ns);
            local_read_idx += 1;
        } else {
            std::hint::spin_loop();
        }
    }
}

// Spin-based watchdog (item 6): replaced thread::sleep(500ms) with a tight
// spin loop that checks elapsed time every ~16M iterations (~16ms). This avoids
// the OS sleep/wakeup cycle which can trigger the scheduler to preempt the
// strategy thread. The watchdog runs at default QOS (E-core), so spinning here
// does not compete with the strategy's P-core allocation.
pub(crate) fn run_watchdog(
    order_book: Arc<models::OrderBook>,
    buffer: Arc<models::RingBuffer>,
    last_packet_ns: Arc<AtomicU64>,
) {
    const IDLE_SHUTDOWN_NS:   u64 = 10_000_000_000;
    const NO_FEED_TIMEOUT_NS: u64 = 30_000_000_000;
    const CHECK_INTERVAL_NS:  u64 = 500_000_000;

    let mut last_check_ns: u64 = 0;
    let mut spin_count: u64 = 0;

    loop {
        spin_count = spin_count.wrapping_add(1);
        // Check elapsed time every 2^24 (~16M) spins to amortise the timer call.
        if spin_count & 0x00FF_FFFF != 0 {
            std::hint::spin_loop();
            continue;
        }

        let now_ns = elapsed_ns(&buffer.start_time);
        if now_ns.saturating_sub(last_check_ns) < CHECK_INTERVAL_NS {
            continue;
        }
        last_check_ns = now_ns;

        let last = last_packet_ns.load(Ordering::Acquire);

        if last == 0 {
            if now_ns >= NO_FEED_TIMEOUT_NS {
                eprintln!("[engine] no market data received within 30s — shutting down");
                std::process::exit(1);
            }
            continue;
        }

        if now_ns.saturating_sub(last) < IDLE_SHUTDOWN_NS {
            continue;
        }

        // Memory snapshot [3]: immediately before log output.
        let mem_pre_log = collect_memory_stats();
        print_stats(&order_book, &mem_pre_log);
        write_log(&order_book, &mem_pre_log);
        // Memory snapshot [4]: immediately after log write.
        let mem_post_log = collect_memory_stats();
        println!("[mem] snapshot [4] after log write  — Peak RSS: {} MB",
                 mem_post_log.peak_rss / 1_048_576);
        let _ = std::io::stdout().flush();
        std::process::exit(0);
    }
}

// Summary statistics for one latency stage, computed at shutdown.
struct Stat {
    avg:  u64,
    min:  u64,
    max:  u64,
    p50:  u64,
    p95:  u64,
    p99:  u64,
    p999: u64,
    count: usize,
}

// Compute avg/min/max/percentiles by sorting a copy of the samples. Used for the
// transit and end-to-end stages, whose values are millisecond-scale and therefore
// outside the LatencyHistogram's 0–10,000 ns range. Off the hot path (shutdown
// only), so the allocation + sort is fine. Nearest-rank percentiles, matching the
// histogram's ceil(total * p_num / p_den) convention.
fn summarize(mut v: Vec<u64>) -> Option<Stat> {
    if v.is_empty() { return None; }
    v.sort_unstable();
    let n = v.len();
    let sum: u128 = v.iter().map(|&x| x as u128).sum();
    let pct = |num: u64, den: u64| -> u64 {
        let rank = ((n as u64) * num).div_ceil(den).max(1) as usize;
        v[rank.min(n) - 1]
    };
    Some(Stat {
        avg:  (sum / n as u128) as u64,
        min:  v[0],
        max:  v[n - 1],
        p50:  pct(50, 100),
        p95:  pct(95, 100),
        p99:  pct(99, 100),
        p999: pct(999, 1000),
        count: n,
    })
}

// Per-trade transit samples (RTT/2 from the feed), in ns. Zero means the tick
// arrived on a legacy 16-byte packet with no transit estimate.
fn transit_samples(trades: &[models::TradeExecution]) -> Vec<u64> {
    trades.iter().filter(|t| t.transit_est_ns > 0).map(|t| t.transit_est_ns).collect()
}

// Per-trade end-to-end estimate = transit + signal + round trip, for confirmed
// trades that also carry a transit estimate.
fn end_to_end_samples(trades: &[models::TradeExecution]) -> Vec<u64> {
    trades.iter()
        .filter(|t| t.round_trip_ns > 0 && t.transit_est_ns > 0)
        .map(|t| t.transit_est_ns + t.latency_ns + t.round_trip_ns)
        .collect()
}

// Serialize a Stat (all values in ns) as a JSON object, or null when absent.
fn push_stat_json(json: &mut String, name: &str, s: &Option<Stat>, trailing_comma: bool) {
    let tail = if trailing_comma { "," } else { "" };
    match s {
        Some(s) => {
            json.push_str(&format!("  \"{}\": {{\n", name));
            json.push_str(&format!("    \"avg_ns\": {},\n", s.avg));
            json.push_str(&format!("    \"min_ns\": {},\n", s.min));
            json.push_str(&format!("    \"max_ns\": {},\n", s.max));
            json.push_str(&format!("    \"p50_ns\": {},\n", s.p50));
            json.push_str(&format!("    \"p95_ns\": {},\n", s.p95));
            json.push_str(&format!("    \"p99_ns\": {},\n", s.p99));
            json.push_str(&format!("    \"p999_ns\": {},\n", s.p999));
            json.push_str(&format!("    \"count\": {}\n", s.count));
            json.push_str(&format!("  }}{}\n", tail));
        }
        None => json.push_str(&format!("  \"{}\": null{}\n", name, tail)),
    }
}

// Per-filled-trade slippage in basis points: (fill - target)/target * 1e4.
// Positive = filled above the intended price (adverse for a buy).
fn slippage_bps_samples(trades: &[models::TradeExecution]) -> Vec<f64> {
    trades.iter()
        .filter(|t| t.fill_price > 0.0 && t.target_price > 0.0)
        .map(|t| (t.fill_price as f64 - t.target_price as f64) / t.target_price as f64 * 10_000.0)
        .collect()
}

struct SlipStat { mean: f64, min: f64, max: f64, abs_p50: f64, abs_p95: f64, n: usize }

// Signed mean/min/max plus |slippage| p50/p95 over a small sample (shutdown only).
fn summarize_slippage(v: Vec<f64>) -> Option<SlipStat> {
    if v.is_empty() { return None; }
    let n = v.len();
    let mean = v.iter().sum::<f64>() / n as f64;
    let min = v.iter().copied().fold(f64::INFINITY, f64::min);
    let max = v.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mut a: Vec<f64> = v.iter().map(|x| x.abs()).collect();
    a.sort_by(|x, y| x.partial_cmp(y).unwrap());
    let pick = |p: usize| a[((n * p).div_ceil(100).max(1) - 1).min(n - 1)];
    Some(SlipStat { mean, min, max, abs_p50: pick(50), abs_p95: pick(95), n })
}

// P&L scorecard computed from the completed round-trips at shutdown.
#[derive(Clone, Copy)]
struct Scorecard {
    n: usize, wins: usize, losses: usize, longs: usize, shorts: usize, liquidations: usize,
    gross_wins: usize, hit_rate: f64,
    win_rate: f64,
    total_pnl: f64, total_fees: f64,
    gross_bps_mean: f64, net_bps_mean: f64,
    avg_win_bps: f64, avg_loss_bps: f64,
    profit_factor: f64, max_drawdown: f64, max_dd_pct: f64, sharpe: f64, avg_hold_ms: f64,
    capital: f64, final_equity: f64, return_pct: f64, ruined: bool,
}

fn scorecard(rts: &[models::RoundTrip], capital: f64) -> Option<Scorecard> {
    if rts.is_empty() { return None; }
    let n = rts.len();
    let (mut wins, mut losses, mut longs, mut shorts, mut liquidations) = (0usize, 0usize, 0usize, 0usize, 0usize);
    let mut gross_wins = 0usize;
    let (mut total_pnl, mut total_fees) = (0.0f64, 0.0f64);
    let (mut gross_sum, mut net_sum) = (0.0f64, 0.0f64);
    let (mut win_bps_sum, mut loss_bps_sum) = (0.0f64, 0.0f64);
    let (mut win_pnl, mut loss_pnl, mut hold_sum) = (0.0f64, 0.0f64, 0.0f64);
    let (mut equity, mut peak, mut maxdd, mut min_eq) = (capital, capital, 0.0f64, capital);
    let mut nets: Vec<f64> = Vec::with_capacity(n);
    for t in rts {
        let net = t.net_bps as f64;
        net_sum += net; gross_sum += t.gross_bps as f64;
        total_pnl += t.pnl_quote as f64; total_fees += t.fees_quote as f64;
        hold_sum += t.hold_ns as f64;
        if t.side > 0 { longs += 1; } else { shorts += 1; }
        if t.flags >= 0.5 { liquidations += 1; }
        if t.gross_bps > 0.0 { gross_wins += 1; }   // signal accuracy (before fees)
        if net > 0.0 { wins += 1; win_bps_sum += net; win_pnl += t.pnl_quote as f64; }
        else { losses += 1; loss_bps_sum += net; loss_pnl += t.pnl_quote as f64; }
        nets.push(net);
        equity += t.pnl_quote as f64;
        if equity > peak { peak = equity; }
        if peak - equity > maxdd { maxdd = peak - equity; }
        if equity < min_eq { min_eq = equity; }
    }
    let mean = net_sum / n as f64;
    let std = (nets.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / n as f64).sqrt();
    let final_equity = (capital + total_pnl).max(0.0);
    Some(Scorecard {
        n, wins, losses, longs, shorts, liquidations,
        gross_wins, hit_rate: gross_wins as f64 / n as f64 * 100.0,
        win_rate: wins as f64 / n as f64 * 100.0,
        total_pnl, total_fees,
        gross_bps_mean: gross_sum / n as f64, net_bps_mean: mean,
        avg_win_bps:  if wins   > 0 { win_bps_sum  / wins as f64 }   else { 0.0 },
        avg_loss_bps: if losses > 0 { loss_bps_sum / losses as f64 } else { 0.0 },
        profit_factor: if loss_pnl < 0.0 { win_pnl / -loss_pnl }
                       else if win_pnl > 0.0 { f64::INFINITY } else { 0.0 },
        max_drawdown: maxdd,
        max_dd_pct: if peak > 0.0 { maxdd / peak * 100.0 } else { 0.0 },
        sharpe: if std > 0.0 { mean / std } else { 0.0 },
        avg_hold_ms: hold_sum / n as f64 / 1e6,
        capital, final_equity, return_pct: (final_equity - capital) / capital * 100.0,
        ruined: min_eq <= 0.0,
    })
}

// ── Backtest / parameter sweep (offline; no threads, UDP, or sleeps) ─────────
// Runs the SAME `AlphaModel` the live engine uses over a recorded capture, with an
// in-sample/out-of-sample split and a small parameter grid, ranked by OOS return.

#[derive(Copy, Clone)]
struct BtTick { id: u8, price: f32, vol: f32, t_ns: u64, bid: f32, ask: f32 }

fn load_capture(path: &str) -> std::io::Result<Vec<BtTick>> {
    let data = std::fs::read(path)?;
    if data.len() < 5 || &data[0..4] != b"KRKR" {
        return Err(std::io::Error::other("backtest: bad capture magic"));
    }
    let mut out = Vec::new();
    let (mut i, mut t) = (5usize, 0u64);
    while i + 10 <= data.len() {
        let delta = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
        let len = u16::from_le_bytes([data[i + 8], data[i + 9]]) as usize;
        i += 10;
        if i + len > data.len() || len < 8 { break; }
        let pkt = &data[i..i + len];
        i += len;
        t = t.wrapping_add(delta);
        out.push(BtTick {
            id:    if len >= 33 { pkt[32] } else { 0 },
            price: f32::from_le_bytes(pkt[0..4].try_into().unwrap()),
            vol:   f32::from_le_bytes(pkt[4..8].try_into().unwrap()),
            t_ns:  t,
            // v4 packets (>= 49 bytes) carry bid/ask at [33..41]; older packets 0.
            bid:   if len >= 49 { f32::from_le_bytes(pkt[33..37].try_into().unwrap()) } else { 0.0 },
            ask:   if len >= 49 { f32::from_le_bytes(pkt[37..41].try_into().unwrap()) } else { 0.0 },
        });
    }
    Ok(out)
}

// Run the model over a tick stream (id 1 = reference, 0 = traded) → round-trips.
// When `policy` is Some, the learned MLP supplies the signal instead of the
// hand-weighted composite (the gate / sizing / exit logic is identical).
fn run_model(
    ticks: &[BtTick], cfg: models::TradeCfg, policy: Option<crate::model::Policy>,
) -> Vec<models::RoundTrip> {
    let mut model = crate::model::AlphaModel::with_policy(cfg, policy);
    let mut rts = Vec::new();
    let mut seq: u64 = 0;
    for tk in ticks {
        if tk.id == 1 { model.on_reference_tick(tk.price); continue; }
        seq += 1;
        let warmed = seq > WARMUP_PACKETS;
        if let crate::model::Decision::Exit(rt) =
            model.on_traded_tick(tk.price, tk.bid, tk.ask, tk.vol, tk.t_ns, warmed, false)
        {
            rts.push(rt);
        }
    }
    rts
}

pub(crate) fn run_backtest(path: &str, base: models::TradeCfg) {
    let ticks = match load_capture(path) {
        Ok(t) => t,
        Err(e) => { eprintln!("[backtest] {e}"); return; }
    };
    let traded_n = ticks.iter().filter(|t| t.id == 0).count();
    println!("[backtest] {} ticks ({} traded) from {}", ticks.len(), traded_n, path);
    // Walk-forward split by TIME: the model runs continuously (warm EMAs, no future
    // leakage); round-trips are bucketed in-sample / out-of-sample by entry time.
    let split = ticks.len() * 70 / 100;
    let t_split = ticks.get(split).map(|t| t.t_ns).unwrap_or(u64::MAX);
    println!("  walk-forward split at {:.0}% of the capture  |  capital {:.0}, fee {:.1} bps/side, {:.0}x lev{}",
        70.0, base.capital, base.fee_bps, base.leverage,
        if base.normalize { ", normalized signal" } else { "" });

    let mut base = base;
    base.enabled = true;
    base.momentum = true;

    println!("{}", "─".repeat(86));
    println!("{:<6} {:<5} {:<6} {:<5} | {:>9} {:>9} {:>8} {:>6} | flag", "maker", "thr", "trail", "gate",
             "IS ret%", "OOS ret%", "OOS hit", "OOS n");
    println!("{}", "─".repeat(86));

    let mut rows: Vec<(f64, String)> = Vec::new();
    for &maker in &[false, true] {
        for &thr in &[3.0f32, 5.0, 10.0] {
            for &trail in &[6.0f32, 10.0] {
                for &gate in &[false, true] {
                    let mut cfg = base;
                    cfg.maker = maker;
                    cfg.signal_thr_bps = thr;
                    cfg.trail_bps = trail;
                    cfg.fee_gate = gate;
                    cfg.min_edge_bps = if gate { 2.0 } else { 0.0 };
                    // One continuous run; bucket round-trips by entry time.
                    let full = run_model(&ticks, cfg, None);
                    let is_rts:  Vec<models::RoundTrip> = full.iter().copied().filter(|r| r.entry_time_ns <  t_split).collect();
                    let oos_rts: Vec<models::RoundTrip> = full.iter().copied().filter(|r| r.entry_time_ns >= t_split).collect();
                    let is_sc  = scorecard(&is_rts, cfg.capital as f64);
                    let oos_sc = scorecard(&oos_rts, cfg.capital as f64);
                    let isr  = is_sc.as_ref().map(|s| s.return_pct).unwrap_or(0.0);
                    let oosr = oos_sc.as_ref().map(|s| s.return_pct).unwrap_or(0.0);
                    let oh   = oos_sc.as_ref().map(|s| s.hit_rate).unwrap_or(0.0);
                    let on   = oos_sc.as_ref().map(|s| s.n).unwrap_or(0);
                    let flag = if isr > 0.0 && oosr < 0.0 { "overfit" }
                               else if oosr > 0.0 { "OOS+" } else { "" };
                    rows.push((oosr, format!(
                        "{:<6} {:<5.0} {:<6.0} {:<5} | {:>8.2}% {:>8.2}% {:>7.1}% {:>6} | {}",
                        maker, thr, trail, gate, isr, oosr, oh, on, flag)));
                }
            }
        }
    }
    rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    for (_, r) in &rows { println!("{r}"); }
    println!("{}", "─".repeat(86));
    if let Some((oosr, _)) = rows.first() {
        println!("Best out-of-sample return: {:+.2}%  ({} configs; ranked by OOS, not in-sample)", oosr, rows.len());
    }
    println!("(maker fee = HFT_MAKER_BPS={:.1}; set HFT_NORMALIZE=1 to sweep the z-scored signal)", base.maker_bps);
}

// ── Evaluate a single (learned) policy over a whole capture, offline ─────────
// When HFT_MODEL is set, `--backtest` runs THIS instead of the hand-weighted grid
// sweep: it plays the trained policy over the entire capture at full speed (no
// sleeps), prints the full scorecard, and splits the capture into halves to show
// whether the edge is stable across the session (not just front-loaded). Point it
// at a capture the policy was NOT trained on for a true out-of-sample read.
pub(crate) fn run_eval(path: &str, base: models::TradeCfg, policy: crate::model::Policy) {
    let ticks = match load_capture(path) {
        Ok(t) => t,
        Err(e) => { eprintln!("[eval] {e}"); return; }
    };
    let traded_n = ticks.iter().filter(|t| t.id == 0).count();
    let mut cfg = base;
    cfg.enabled = true;
    cfg.momentum = true;
    let cap = cfg.capital as f64;

    println!("[eval] learned policy over {} ticks ({} traded) from {}", ticks.len(), traded_n, path);
    println!("       capital {:.0}, fee {:.1} bps/side, {:.0}x lev  (this capture is held out — train was on a different one)",
             cfg.capital, cfg.fee_bps, cfg.leverage);
    println!("{}", "─".repeat(78));

    let rts = run_model(&ticks, cfg, Some(policy));

    // Per-half stability: bucket round-trips by entry time into the first/second
    // half of the capture's time span.
    let t_lo = ticks.first().map(|t| t.t_ns).unwrap_or(0);
    let t_hi = ticks.last().map(|t| t.t_ns).unwrap_or(0);
    let t_mid = t_lo + (t_hi - t_lo) / 2;
    let h1: Vec<models::RoundTrip> = rts.iter().copied().filter(|r| r.entry_time_ns <  t_mid).collect();
    let h2: Vec<models::RoundTrip> = rts.iter().copied().filter(|r| r.entry_time_ns >= t_mid).collect();
    let line = |tag: &str, s: &Option<Scorecard>| match s {
        Some(sc) => println!("{:<11} n={:<4} hit={:>5.1}%  win={:>5.1}%  ret={:>+7.2}%  netP&L={:>+9.2}  PF={:>5.2}  Sharpe={:>5.2}  maxDD={:>4.1}%",
                             tag, sc.n, sc.hit_rate, sc.win_rate, sc.return_pct, sc.total_pnl, sc.profit_factor, sc.sharpe, sc.max_dd_pct),
        None => println!("{tag:<11} no round-trips"),
    };
    line("first-half",  &scorecard(&h1,  cap));
    line("second-half", &scorecard(&h2,  cap));
    println!("{}", "─".repeat(78));
    match scorecard(&rts, cap) {
        Some(s) => {
            line("FULL", &Some(s));
            println!("  {} long / {} short  |  liquidations {}  |  avg win {:+.2} / avg loss {:+.2} bps  |  avg hold {:.1} ms",
                     s.longs, s.shorts, s.liquidations, s.avg_win_bps, s.avg_loss_bps, s.avg_hold_ms);
            println!("  net {:+.2} bps/trade after fees  |  gross {:+.2} bps/trade  |  total fees {:.2}",
                     s.net_bps_mean, s.gross_bps_mean, s.total_fees);
            let verdict = if s.ruined { "BLOWN UP (ruined)" }
                          else if s.return_pct > 0.0 { "net PROFITABLE" } else { "net LOSS" };
            println!("→ {verdict} after fees over this held-out capture.");
        }
        None => println!("FULL        no round-trips (the policy never fired on this capture)."),
    }
}

// ── Train a learned policy by cross-entropy method (CEM) ─────────────────────
// CEM is a gradient-free, embarrassingly-simple evolutionary search: keep a
// Gaussian over the 65-param weight vector, sample a population, keep the elite
// (best-fitness) fraction, refit the Gaussian to them, repeat. No autodiff, no
// dependency — just the AlphaModel run forward over the in-sample ticks. The
// fitness is a Sharpe-like score (penalized below a minimum trade count) so the
// search prefers consistent edge over a few lucky outliers. Walk-forward: train
// on the first 70% by time, report the held-out last 30%.

// Deterministic splitmix64 → standard normal (Box–Muller), zero-dependency.
struct Rng(u64);
impl Rng {
    fn u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f32 { (self.u64() >> 11) as f32 / (1u64 << 53) as f32 }
    fn normal(&mut self) -> f32 {
        let u1 = self.unit().max(1e-9);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos()
    }
}

// Sharpe-like fitness with a floor on trade count (a high Sharpe over a handful
// of trades is noise, not edge). A run that never traded scores well below any
// real one but stays finite, so the elite mean is meaningful.
fn fitness(rts: &[models::RoundTrip], capital: f64) -> f64 {
    const MIN_TRADES: usize = 30;
    const NO_TRADE: f64 = -100.0;
    match scorecard(rts, capital) {
        None => NO_TRADE,
        Some(sc) => {
            if sc.ruined { return NO_TRADE; }
            // Quadratic ramp below the floor so the search is pushed to trade
            // enough to be statistically meaningful before chasing Sharpe.
            let penalty = if sc.n < MIN_TRADES {
                let r = sc.n as f64 / MIN_TRADES as f64;
                r * r
            } else { 1.0 };
            // Reward risk-adjusted edge; tie-break toward higher return.
            sc.sharpe * penalty + sc.return_pct * 1e-3
        }
    }
}

pub(crate) fn run_train(path: &str, base: models::TradeCfg) {
    use crate::model::{Policy, N_PARAMS};
    use rust_hft_software::config as cfgc;
    let env_usize = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let pop     = env_usize("HFT_POP", cfgc::TRAIN_POP_DEFAULT);
    let gens    = env_usize("HFT_GEN", cfgc::TRAIN_GEN_DEFAULT);
    let elite_n = (pop / 8).max(4);
    let seed    = std::env::var("HFT_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(cfgc::TRAIN_SEED_DEFAULT);
    let out     = std::env::var("HFT_MODEL").unwrap_or_else(|_| cfgc::MODEL_PATH_DEFAULT.to_string());

    let ticks = match load_capture(path) {
        Ok(t) => t,
        Err(e) => { eprintln!("[train] {e}"); return; }
    };
    let split   = ticks.len() * 70 / 100;
    let t_split = ticks.get(split).map(|t| t.t_ns).unwrap_or(u64::MAX);
    let is_ticks:  Vec<BtTick> = ticks.iter().copied().filter(|t| t.t_ns <  t_split).collect();
    let oos_ticks: Vec<BtTick> = ticks.iter().copied().filter(|t| t.t_ns >= t_split).collect();

    let mut cfg = base;
    cfg.enabled = true;
    cfg.momentum = true;
    let cap = cfg.capital as f64;

    println!("[train] CEM  |  {} ticks ({} IS / {} OOS)  pop={pop} gens={gens} elite={elite_n} seed={seed}",
             ticks.len(), is_ticks.len(), oos_ticks.len());
    println!("        {} params (tiny MLP {}→{}→1), fitness = Sharpe (penalized < 30 trades)",
             N_PARAMS, crate::model::N_FEATURES, 8);
    println!("{}", "─".repeat(64));
    println!("{:<5} {:>10} {:>10} {:>8}", "gen", "best fit", "elite μ", "IS n");
    println!("{}", "─".repeat(64));

    let mut rng = Rng(seed);
    let mut mean = [0.0f32; N_PARAMS];
    let mut std  = [0.5f32;  N_PARAMS];
    let mut best_p = Policy { p: mean };
    let mut best_fit = f64::MIN;

    let mut samples: Vec<([f32; N_PARAMS], f64)> = Vec::with_capacity(pop);
    for g in 0..gens {
        samples.clear();
        for _ in 0..pop {
            let mut p = [0.0f32; N_PARAMS];
            for k in 0..N_PARAMS { p[k] = mean[k] + std[k] * rng.normal(); }
            let rts = run_model(&is_ticks, cfg, Some(Policy { p }));
            samples.push((p, fitness(&rts, cap)));
        }
        samples.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        // Refit the Gaussian to the elite set.
        let mut nm = [0.0f32; N_PARAMS];
        let mut nv = [0.0f32; N_PARAMS];
        for (p, _) in &samples[..elite_n] {
            for k in 0..N_PARAMS { nm[k] += p[k]; }
        }
        for m in nm.iter_mut() { *m /= elite_n as f32; }
        for (p, _) in &samples[..elite_n] {
            for k in 0..N_PARAMS { let d = p[k] - nm[k]; nv[k] += d * d; }
        }
        for k in 0..N_PARAMS {
            mean[k] = nm[k];
            // Variance + a small floor so the search never fully collapses.
            std[k] = (nv[k] / elite_n as f32).sqrt().max(0.02);
        }
        let (bp, bf) = samples[0];
        if bf > best_fit { best_fit = bf; best_p = Policy { p: bp }; }
        let elite_n_trades = run_model(&is_ticks, cfg, Some(Policy { p: bp })).len();
        let mut mu = 0.0f64;
        for (_, f) in &samples[..elite_n] { mu += *f; }
        mu /= elite_n as f64;
        println!("{:<5} {:>10.4} {:>10.4} {:>8}", g, bf, mu, elite_n_trades);
    }
    println!("{}", "─".repeat(64));

    // Report the held-out OOS scorecard of the best policy.
    let is_rts  = run_model(&is_ticks,  cfg, Some(best_p));
    let oos_rts = run_model(&oos_ticks, cfg, Some(best_p));
    let is_sc  = scorecard(&is_rts,  cap);
    let oos_sc = scorecard(&oos_rts, cap);
    let fmt = |s: &Option<Scorecard>| match s {
        Some(sc) => format!("n={:<4} hit={:>5.1}%  ret={:>+7.2}%  sharpe={:>6.2}  PF={:>5.2}",
                             sc.n, sc.hit_rate, sc.return_pct, sc.sharpe, sc.profit_factor),
        None => "no trades".to_string(),
    };
    println!("  in-sample : {}", fmt(&is_sc));
    println!("  out-sample: {}", fmt(&oos_sc));
    let overfit = is_sc.as_ref().map(|s| s.return_pct).unwrap_or(0.0) > 0.0
               && oos_sc.as_ref().map(|s| s.return_pct).unwrap_or(0.0) < 0.0;
    if overfit { println!("  ⚠ overfit: in-sample positive, out-of-sample negative"); }

    // Persist the best policy (raw little-endian f32 weights).
    if let Some(dir) = std::path::Path::new(&out).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    match std::fs::write(&out, best_p.to_le_bytes()) {
        Ok(()) => println!("[train] wrote {} ({} bytes) — run with HFT_MODEL={}", out, N_PARAMS * 4, out),
        Err(e) => eprintln!("[train] could not write {out}: {e}"),
    }
}

fn print_stats(order_book: &models::OrderBook, mem_pre_log: &MemoryStats) {
    let count  = (order_book.trade_log.write_idx.load(Ordering::Acquire) as usize).min(TRADE_LOG_SIZE);
    let trades = unsafe { &*order_book.trade_log.entries.get() };

    println!("Total trades executed: {}\n", count);
    println!("{:<10} {:>14} {:>14} {:>16} {:>14}",
             "Sequence", "Sig Lat (ns)", "Round Trip(ns)", "Transit (µs)", "End-End (µs)");
    println!("{}", "─".repeat(72));

    for t in &trades[..count] {
        let rt = if t.round_trip_ns > 0 { t.round_trip_ns.to_string() } else { "—".to_string() };
        let tr = if t.transit_est_ns > 0 { (t.transit_est_ns / 1000).to_string() } else { "—".to_string() };
        let e2e = if t.round_trip_ns > 0 && t.transit_est_ns > 0 {
            ((t.transit_est_ns + t.latency_ns + t.round_trip_ns) / 1000).to_string()
        } else { "—".to_string() };
        println!("{:<10} {:>14} {:>14} {:>16} {:>14}", t.sequence, t.latency_ns, rt, tr, e2e);
    }

    if count > 0 {
        let mut sig_sum  = 0u64;
        let mut sig_min  = u64::MAX;
        let mut sig_max  = 0u64;
        let mut rt_sum   = 0u64;
        let mut rt_min   = u64::MAX;
        let mut rt_max   = 0u64;
        let mut rt_count = 0usize;

        for t in &trades[..count] {
            sig_sum += t.latency_ns;
            if t.latency_ns < sig_min { sig_min = t.latency_ns; }
            if t.latency_ns > sig_max { sig_max = t.latency_ns; }
            if t.round_trip_ns > 0 {
                rt_sum  += t.round_trip_ns;
                if t.round_trip_ns < rt_min { rt_min = t.round_trip_ns; }
                if t.round_trip_ns > rt_max { rt_max = t.round_trip_ns; }
                rt_count += 1;
            }
        }

        let sig_p50  = order_book.sig_hist.percentile(50,  100,  count as u64);
        let sig_p95  = order_book.sig_hist.percentile(95,  100,  count as u64);
        let sig_p99  = order_book.sig_hist.percentile(99,  100,  count as u64);
        let sig_p999 = order_book.sig_hist.percentile(999, 1000, count as u64);

        println!("{}", "─".repeat(55));
        println!("Signal latency — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                 sig_sum / count as u64, sig_min, sig_max);
        println!("                p50: {:>7} ns  p95: {:>7} ns  p99: {:>7} ns  p99.9: {:>7} ns",
                 sig_p50, sig_p95, sig_p99, sig_p999);

        if rt_count > 0 {
            let rt_p50  = order_book.rt_hist.percentile(50,  100,  rt_count as u64);
            let rt_p95  = order_book.rt_hist.percentile(95,  100,  rt_count as u64);
            let rt_p99  = order_book.rt_hist.percentile(99,  100,  rt_count as u64);
            let rt_p999 = order_book.rt_hist.percentile(999, 1000, rt_count as u64);
            println!("Round trip     — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                     rt_sum / rt_count as u64, rt_min, rt_max);
            println!("                p50: {:>7} ns  p95: {:>7} ns  p99: {:>7} ns  p99.9: {:>7} ns",
                     rt_p50, rt_p95, rt_p99, rt_p999);
        } else {
            println!("Round trip     — no confirmations received");
        }

        // ── Full-stack breakdown ───────────────────────────────────────────
        // Transit (source → local arrival) is the network leg, estimated as
        // RTT/2 by the feed adapter. It is millisecond-scale, so it's reported
        // in microseconds — in stark contrast to the nanosecond-scale signal and
        // round-trip stages above. End-to-end sums all three per trade.
        println!("{}", "─".repeat(55));
        if let Some(s) = summarize(transit_samples(&trades[..count])) {
            println!("Transit (RTT/2)— Avg: {:>7} µs  Min: {:>7} µs  Max: {:>7} µs  (n={})",
                     s.avg / 1000, s.min / 1000, s.max / 1000, s.count);
            println!("                p50: {:>7} µs  p95: {:>7} µs  p99: {:>7} µs  p99.9: {:>7} µs",
                     s.p50 / 1000, s.p95 / 1000, s.p99 / 1000, s.p999 / 1000);
        } else {
            println!("Transit (RTT/2)— no transit estimates (legacy/simulated feed without RTT)");
        }
        if let Some(s) = summarize(end_to_end_samples(&trades[..count])) {
            println!("End-to-end     — Avg: {:>7} µs  Min: {:>7} µs  Max: {:>7} µs  (n={})",
                     s.avg / 1000, s.min / 1000, s.max / 1000, s.count);
            println!("                p50: {:>7} µs  p95: {:>7} µs  p99: {:>7} µs  p99.9: {:>7} µs",
                     s.p50 / 1000, s.p95 / 1000, s.p99 / 1000, s.p999 / 1000);
            println!("  (end-to-end ≈ transit + signal + round-trip; the engine's own");
            println!("   reaction is the ns-scale signal+round-trip — a rounding error vs transit)");
        }
    }

    let lo = f32::from_bits(order_book.price_lo_bits.load(Ordering::Relaxed));
    let hi = f32::from_bits(order_book.price_hi_bits.load(Ordering::Relaxed));
    let spread_lo = f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed));
    let spread_hi = f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed));
    let funding   = f32::from_bits(order_book.funding_bits.load(Ordering::Relaxed));

    // ── Trading scorecard (HFT_TRADE) ───────────────────────────────────
    if order_book.trade_cfg.enabled {
        let rt_count = (order_book.round_trips.write_idx.load(Ordering::Acquire) as usize)
            .min(ROUND_TRIP_LOG_SIZE);
        let rts_all = unsafe { &*order_book.round_trips.entries.get() };
        let rts = &rts_all[..rt_count];
        let cfg = order_book.trade_cfg;
        let vol = f32::from_bits(order_book.vol_ema_bits.load(Ordering::Relaxed));
        let sig = f32::from_bits(order_book.latest_signal_bits.load(Ordering::Relaxed));
        println!("{}", "─".repeat(72));
        if cfg.momentum {
            println!("TRADING SCORECARD  ({} TREND-FOLLOWING + cross-market, {:.0}x lev, {:.1} bps/side fee)",
                     if cfg.allow_short { "long&short" } else { "long-only" }, cfg.leverage, cfg.fee_bps);
            println!("Signal: S = {:.1}·trend + {:.1}·flow + {:.1}·basket + {:.1}·leadlag   gate ±{:.1} bps   latest S {:+.2} bps",
                     cfg.w_trend, cfg.w_flow, cfg.w_basket, cfg.w_leadlag, cfg.signal_thr_bps, sig);
        } else {
            let rule = if cfg.adaptive { "ADAPTIVE (entry 1σ/TP 1.5σ/SL 2.5σ)".to_string() }
                       else { format!("entry {:.1}/TP {:.1}/SL {:.1} bps", cfg.entry_dip_bps, cfg.tp_bps, cfg.sl_bps) };
            println!("TRADING SCORECARD  ({} mean-reversion, {}{}, {:.0}x lev, {:.1} bps/side fee)",
                     if cfg.allow_short { "long&short" } else { "long-only" }, rule,
                     if cfg.use_flow { " +order-flow" } else { "" }, cfg.leverage, cfg.fee_bps);
        }
        if lo.is_finite() && hi.is_finite() {
            let range_bps = if lo > 0.0 { (hi - lo) / lo * 10_000.0 } else { 0.0 };
            println!("Observed price range: [{:.2}, {:.2}]  ({:.1} bps span)  |  volatility ~{:.2} bps/tick",
                     lo, hi, range_bps, vol);
            if spread_lo.is_finite() && spread_hi.is_finite() {
                println!("Market data: spread {:.2}–{:.2} bps  |  funding {:.8} (raw, latest)",
                         spread_lo, spread_hi, funding);
            }
        }
        match scorecard(rts, cfg.capital as f64) {
            Some(s) => {
                println!("Round-trips: {}  ({} long / {} short)  |  liquidations {}",
                         s.n, s.longs, s.shorts, s.liquidations);
                println!("Hit rate (signal accuracy, gross): {:.1}% ({}/{})   |   net-win rate (after fees): {:.1}% ({}W/{}L)",
                         s.hit_rate, s.gross_wins, s.n, s.win_rate, s.wins, s.losses);
                println!("Capital {:.2} → equity {:.2}   ({:+.2}% return on capital{})",
                         s.capital, s.final_equity, s.return_pct, if s.ruined { ", RUINED ☠" } else { "" });
                println!("Net P&L: {:+.2} quote   (gross {:+.2} bps/trade, net {:+.2} bps/trade after fees)",
                         s.total_pnl, s.gross_bps_mean, s.net_bps_mean);
                println!("Avg win {:+.2} bps  |  avg loss {:+.2} bps  |  profit factor {:.2}",
                         s.avg_win_bps, s.avg_loss_bps, s.profit_factor);
                println!("Max drawdown {:.2} quote ({:.1}%)  |  Sharpe(/trade) {:.2}  |  fees {:.2}  |  avg hold {:.1} ms",
                         s.max_drawdown, s.max_dd_pct, s.sharpe, s.total_fees, s.avg_hold_ms);
                let verdict = if s.ruined { "BLOWN UP (account ruined)" }
                              else if s.return_pct > 0.0 { "net PROFITABLE" } else { "net LOSS" };
                println!("→ {verdict} after fees over this run.");
            }
            None => println!("No round-trips closed (try a busier pair, longer run, or smaller HFT_ENTRY_BPS)."),
        }

        let stall_count  = order_book.stall_count.load(Ordering::Relaxed);
        let gap_count    = order_book.gap_count.load(Ordering::Relaxed);
        let net_position = order_book.net_position.load(Ordering::Relaxed);
        let halted       = order_book.halt.load(Ordering::Relaxed);
        println!("{}", "─".repeat(72));
        println!("OS stalls (>500ns spin gap): {}  |  Sequence gaps: {}  |  Net position: {}  |  Halt: {}",
                 stall_count, gap_count, net_position, halted);
        let _ = std::io::stdout().flush();
        let total_ram_mb   = order_book.mem_total_ram.load(Ordering::Relaxed) / 1_048_576;
        let rss_pre_log_mb = mem_pre_log.peak_rss / 1_048_576;
        println!("{}", "─".repeat(72));
        println!("Memory — Total RAM: {} MB  |  Peak RSS: {} MB", total_ram_mb, rss_pre_log_mb);
        let _ = std::io::stdout().flush();
        return;
    }

    // ── Execution / slippage ────────────────────────────────────────────
    let attempts = order_book.attempts.load(Ordering::Relaxed);
    let filled   = order_book.filled.load(Ordering::Relaxed);
    let pending  = attempts.saturating_sub(filled);
    println!("{}", "─".repeat(72));
    if order_book.buy_on_downtick {
        println!("Downtick buys  |  attempts: {}  filled: {}  pending: {}  (slippage vs the price we acted on)",
                 attempts, filled, pending);
    } else if order_book.target_dip_bps > 0.0 {
        println!("Dip buys ({:.1} bps)  |  attempts: {}  filled: {}  pending: {}  (slippage vs the price we acted on)",
                 order_book.target_dip_bps, attempts, filled, pending);
    } else if order_book.target_price > 0.0 {
        println!("Target buy @ {:.4}  |  attempts: {}  filled: {}  pending: {}",
                 order_book.target_price, attempts, filled, pending);
    } else {
        println!("Breakout buys  |  attempts: {}  filled: {}  pending: {}  (slippage measured vs entry price)",
                 attempts, filled, pending);
    }
    if lo.is_finite() && hi.is_finite() {
        let range_bps = if lo > 0.0 { (hi - lo) / lo * 10_000.0 } else { 0.0 };
        println!("Observed price range: [{:.4}, {:.4}]  ({:.2} bps span)", lo, hi, range_bps);
        if spread_lo.is_finite() && spread_hi.is_finite() {
            println!("Market data: spread {:.2}–{:.2} bps  |  funding {:.8} (raw, latest)",
                     spread_lo, spread_hi, funding);
        }
        if attempts == 0 {
            println!("  → no buys triggered. The market moved only {:.2} bps this run; try", range_bps);
            println!("    a smaller HFT_TARGET_DIP_BPS, HFT_DOWNTICK=1, a busier pair, or a longer run.");
        }
    }
    if let Some(s) = summarize_slippage(slippage_bps_samples(&trades[..count])) {
        println!("Slippage (fill−target) — mean: {:+.2} bps   |slip|: p50 {:.2}  p95 {:.2} bps   (n={})",
                 s.mean, s.abs_p50, s.abs_p95, s.n);
        println!("                         worst adverse: {:+.2} bps   best: {:+.2} bps",
                 s.max, s.min);
    } else if attempts > 0 {
        println!("Slippage: no fills resolved before shutdown (orders still in flight)");
    }

    let stall_count  = order_book.stall_count.load(Ordering::Relaxed);
    let gap_count    = order_book.gap_count.load(Ordering::Relaxed);
    let net_position = order_book.net_position.load(Ordering::Relaxed);
    let halted       = order_book.halt.load(Ordering::Relaxed);
    println!("{}", "─".repeat(55));
    println!("OS stalls (>500ns spin gap): {}  |  Sequence gaps: {}  |  Net position: {}  |  Halt: {}",
             stall_count, gap_count, net_position, halted);

    let _ = std::io::stdout().flush();

    let total_ram_mb   = order_book.mem_total_ram.load(Ordering::Relaxed) / 1_048_576;
    let rss_start_mb   = order_book.mem_rss_start.load(Ordering::Relaxed) / 1_048_576;
    let rss_ready_mb   = order_book.mem_rss_ready.load(Ordering::Relaxed) / 1_048_576;
    let rss_pre_log_mb = mem_pre_log.peak_rss / 1_048_576;
    println!("{}", "─".repeat(55));
    println!("Memory — Total RAM: {} MB", total_ram_mb);
    println!("  [1] start          Peak RSS: {} MB", rss_start_mb);
    println!("  [2] after ready    Peak RSS: {} MB", rss_ready_mb);
    println!("  [3] before log     Peak RSS: {} MB", rss_pre_log_mb);

    let _ = std::io::stdout().flush();
}

fn write_log(order_book: &models::OrderBook, mem_pre_log: &MemoryStats) {
    let count  = (order_book.trade_log.write_idx.load(Ordering::Acquire) as usize).min(TRADE_LOG_SIZE);
    let trades = unsafe { &*order_book.trade_log.entries.get() };

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let (date, time) = unix_to_date_time(secs);
    let version = env!("CARGO_PKG_VERSION");

    let dir = format!("logs/v{}/{}", version, date);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[log] failed to create log directory: {}", e);
        return;
    }
    let path = format!("{}/{}.json", dir, time);

    let mut json = String::with_capacity(8192);
    json.push_str("{\n");
    json.push_str(&format!("  \"version\": \"{}\",\n", version));
    json.push_str(&format!("  \"date\": \"{}\",\n", date));
    json.push_str(&format!("  \"timestamp\": \"{}T{}Z\",\n", date, time));
    json.push_str(&format!("  \"total_trades\": {},\n", count));
    json.push_str(&format!("  \"net_position\": {},\n", order_book.net_position.load(Ordering::Relaxed)));
    json.push_str(&format!("  \"halted\": {},\n", order_book.halt.load(Ordering::Relaxed)));
    json.push_str(&format!("  \"stall_count\": {},\n", order_book.stall_count.load(Ordering::Relaxed)));
    json.push_str(&format!("  \"gap_count\": {},\n", order_book.gap_count.load(Ordering::Relaxed)));

    json.push_str("  \"memory\": {\n");
    json.push_str(&format!("    \"total_ram_mb\": {},\n",
        order_book.mem_total_ram.load(Ordering::Relaxed) / 1_048_576));
    json.push_str(&format!("    \"peak_rss_start_mb\": {},\n",
        order_book.mem_rss_start.load(Ordering::Relaxed) / 1_048_576));
    json.push_str(&format!("    \"peak_rss_ready_mb\": {},\n",
        order_book.mem_rss_ready.load(Ordering::Relaxed) / 1_048_576));
    json.push_str(&format!("    \"peak_rss_pre_log_mb\": {}\n",
        mem_pre_log.peak_rss / 1_048_576));
    json.push_str("  },\n");

    if count > 0 {
        let mut sig_sum  = 0u64;
        let mut sig_min  = u64::MAX;
        let mut sig_max  = 0u64;
        let mut rt_sum   = 0u64;
        let mut rt_min   = u64::MAX;
        let mut rt_max   = 0u64;
        let mut rt_count = 0usize;

        for t in &trades[..count] {
            sig_sum += t.latency_ns;
            if t.latency_ns < sig_min { sig_min = t.latency_ns; }
            if t.latency_ns > sig_max { sig_max = t.latency_ns; }
            if t.round_trip_ns > 0 {
                rt_sum += t.round_trip_ns;
                if t.round_trip_ns < rt_min { rt_min = t.round_trip_ns; }
                if t.round_trip_ns > rt_max { rt_max = t.round_trip_ns; }
                rt_count += 1;
            }
        }

        let sig_p50  = order_book.sig_hist.percentile(50,  100,  count as u64);
        let sig_p95  = order_book.sig_hist.percentile(95,  100,  count as u64);
        let sig_p99  = order_book.sig_hist.percentile(99,  100,  count as u64);
        let sig_p999 = order_book.sig_hist.percentile(999, 1000, count as u64);

        json.push_str("  \"signal_latency\": {\n");
        json.push_str(&format!("    \"avg_ns\": {},\n", sig_sum / count as u64));
        json.push_str(&format!("    \"min_ns\": {},\n", sig_min));
        json.push_str(&format!("    \"max_ns\": {},\n", sig_max));
        json.push_str(&format!("    \"p50_ns\": {},\n", sig_p50));
        json.push_str(&format!("    \"p95_ns\": {},\n", sig_p95));
        json.push_str(&format!("    \"p99_ns\": {},\n", sig_p99));
        json.push_str(&format!("    \"p999_ns\": {}\n", sig_p999));
        json.push_str("  },\n");

        json.push_str("  \"round_trip\": {\n");
        if rt_count > 0 {
            let rt_p50  = order_book.rt_hist.percentile(50,  100,  rt_count as u64);
            let rt_p95  = order_book.rt_hist.percentile(95,  100,  rt_count as u64);
            let rt_p99  = order_book.rt_hist.percentile(99,  100,  rt_count as u64);
            let rt_p999 = order_book.rt_hist.percentile(999, 1000, rt_count as u64);
            json.push_str(&format!("    \"avg_ns\": {},\n", rt_sum / rt_count as u64));
            json.push_str(&format!("    \"min_ns\": {},\n", rt_min));
            json.push_str(&format!("    \"max_ns\": {},\n", rt_max));
            json.push_str(&format!("    \"p50_ns\": {},\n", rt_p50));
            json.push_str(&format!("    \"p95_ns\": {},\n", rt_p95));
            json.push_str(&format!("    \"p99_ns\": {},\n", rt_p99));
            json.push_str(&format!("    \"p999_ns\": {}\n", rt_p999));
        } else {
            json.push_str("    \"avg_ns\": null,\n    \"min_ns\": null,\n    \"max_ns\": null,\n");
            json.push_str("    \"p50_ns\": null,\n    \"p95_ns\": null,\n    \"p99_ns\": null,\n    \"p999_ns\": null\n");
        }
        json.push_str("  },\n");

        // Full-stack stages computed from the trade array (ms-scale; ns in JSON).
        push_stat_json(&mut json, "transit", &summarize(transit_samples(&trades[..count])), true);
        push_stat_json(&mut json, "end_to_end", &summarize(end_to_end_samples(&trades[..count])), true);
    } else {
        json.push_str("  \"signal_latency\": null,\n");
        json.push_str("  \"round_trip\": null,\n");
        json.push_str("  \"transit\": null,\n");
        json.push_str("  \"end_to_end\": null,\n");
    }

    // Execution / slippage.
    let attempts = order_book.attempts.load(Ordering::Relaxed);
    let filled   = order_book.filled.load(Ordering::Relaxed);
    let pending  = attempts.saturating_sub(filled);
    let lo = f32::from_bits(order_book.price_lo_bits.load(Ordering::Relaxed));
    let hi = f32::from_bits(order_book.price_hi_bits.load(Ordering::Relaxed));
    json.push_str(&format!("  \"target_price\": {},\n",
        if order_book.target_price > 0.0 { format!("{}", order_book.target_price) } else { "null".to_string() }));
    json.push_str(&format!("  \"attempts\": {},\n", attempts));
    json.push_str(&format!("  \"filled\": {},\n", filled));
    json.push_str(&format!("  \"pending\": {},\n", pending));
    if lo.is_finite() && hi.is_finite() {
        json.push_str(&format!("  \"price_range\": {{\"min\": {}, \"max\": {}}},\n", lo, hi));
    } else {
        json.push_str("  \"price_range\": null,\n");
    }
    let spread_lo = f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed));
    let spread_hi = f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed));
    let funding   = f32::from_bits(order_book.funding_bits.load(Ordering::Relaxed));
    if spread_lo.is_finite() && spread_hi.is_finite() {
        json.push_str(&format!("  \"spread_bps\": {{\"min\": {}, \"max\": {}}},\n", spread_lo, spread_hi));
    } else {
        json.push_str("  \"spread_bps\": null,\n");
    }
    json.push_str(&format!("  \"funding_rate\": {},\n", funding));
    match summarize_slippage(slippage_bps_samples(&trades[..count])) {
        Some(s) => {
            json.push_str("  \"slippage_bps\": {\n");
            json.push_str(&format!("    \"mean\": {:.4},\n", s.mean));
            json.push_str(&format!("    \"min\": {:.4},\n", s.min));
            json.push_str(&format!("    \"max\": {:.4},\n", s.max));
            json.push_str(&format!("    \"abs_p50\": {:.4},\n", s.abs_p50));
            json.push_str(&format!("    \"abs_p95\": {:.4},\n", s.abs_p95));
            json.push_str(&format!("    \"count\": {}\n", s.n));
            json.push_str("  },\n");
        }
        None => json.push_str("  \"slippage_bps\": null,\n"),
    }

    // Trading scorecard + equity curve (HFT_TRADE).
    if order_book.trade_cfg.enabled {
        let rt_count = (order_book.round_trips.write_idx.load(Ordering::Acquire) as usize)
            .min(ROUND_TRIP_LOG_SIZE);
        let rts_all = unsafe { &*order_book.round_trips.entries.get() };
        let rts = &rts_all[..rt_count];
        let cfg = order_book.trade_cfg;
        json.push_str("  \"trading\": {\n");
        json.push_str(&format!("    \"enabled\": true, \"momentum\": {}, \"allow_short\": {}, \"adaptive\": {}, \"use_flow\": {},\n",
            cfg.momentum, cfg.allow_short, cfg.adaptive, cfg.use_flow));
        json.push_str(&format!("    \"leverage\": {}, \"fee_bps\": {}, \"capital\": {}, \"risk_frac\": {},\n",
            cfg.leverage, cfg.fee_bps, cfg.capital, cfg.risk_frac));
        json.push_str(&format!("    \"entry_dip_bps\": {}, \"tp_bps\": {}, \"sl_bps\": {},\n",
            cfg.entry_dip_bps, cfg.tp_bps, cfg.sl_bps));
        if cfg.momentum {
            json.push_str(&format!("    \"w_trend\": {}, \"w_flow\": {}, \"w_basket\": {}, \"w_leadlag\": {}, \"signal_thr_bps\": {}, \"pullback_bps\": {}, \"trail_bps\": {}, \"latest_signal_bps\": {:.4},\n",
                cfg.w_trend, cfg.w_flow, cfg.w_basket, cfg.w_leadlag, cfg.signal_thr_bps, cfg.pullback_bps, cfg.trail_bps,
                f32::from_bits(order_book.latest_signal_bits.load(Ordering::Relaxed))));
        }
        match scorecard(rts, cfg.capital as f64) {
            Some(s) => {
                json.push_str(&format!("    \"round_trips\": {}, \"longs\": {}, \"shorts\": {}, \"liquidations\": {},\n",
                    s.n, s.longs, s.shorts, s.liquidations));
                json.push_str(&format!("    \"gross_wins\": {}, \"hit_rate_pct\": {:.2}, \"net_wins\": {}, \"net_win_rate_pct\": {:.2},\n",
                    s.gross_wins, s.hit_rate, s.wins, s.win_rate));
                json.push_str(&format!("    \"capital\": {:.4}, \"final_equity\": {:.4}, \"return_pct\": {:.4}, \"ruined\": {},\n",
                    s.capital, s.final_equity, s.return_pct, s.ruined));
                json.push_str(&format!("    \"net_pnl_quote\": {:.4}, \"total_fees_quote\": {:.4},\n",
                    s.total_pnl, s.total_fees));
                json.push_str(&format!("    \"gross_bps_mean\": {:.4}, \"net_bps_mean\": {:.4}, \"avg_win_bps\": {:.4}, \"avg_loss_bps\": {:.4},\n",
                    s.gross_bps_mean, s.net_bps_mean, s.avg_win_bps, s.avg_loss_bps));
                let pf = if s.profit_factor.is_finite() { format!("{:.4}", s.profit_factor) } else { "null".to_string() };
                json.push_str(&format!("    \"profit_factor\": {}, \"max_drawdown_quote\": {:.4}, \"max_dd_pct\": {:.4}, \"sharpe_per_trade\": {:.4}, \"avg_hold_ms\": {:.3}\n",
                    pf, s.max_drawdown, s.max_dd_pct, s.sharpe, s.avg_hold_ms));
            }
            None => json.push_str("    \"round_trips\": 0\n"),
        }
        json.push_str("  },\n");

        // Equity curve (account equity after each round-trip) + the round-trip array.
        json.push_str("  \"equity_curve\": [");
        let mut eq = cfg.capital as f64;
        for (i, t) in rts.iter().enumerate() {
            eq += t.pnl_quote as f64;
            json.push_str(&format!("{}{:.4}", if i > 0 { ", " } else { "" }, eq.max(0.0)));
        }
        json.push_str("],\n");

        let sig_at_entry = unsafe { &*order_book.signal.at_entry.get() };
        json.push_str("  \"round_trip_log\": [\n");
        for (i, t) in rts.iter().enumerate() {
            let comma = if i + 1 < rt_count { "," } else { "" };
            json.push_str(&format!(
                "    {{\"side\": {}, \"entry_price\": {}, \"exit_price\": {}, \"size\": {}, \"gross_bps\": {:.4}, \"net_bps\": {:.4}, \"pnl_quote\": {:.4}, \"hold_ns\": {}, \"signal_at_entry\": {:.4}}}{}\n",
                t.side, t.entry_price, t.exit_price, t.size, t.gross_bps, t.net_bps, t.pnl_quote, t.hold_ns, sig_at_entry[i], comma));
        }
        json.push_str("  ],\n");

        // Downsampled composite-signal series (the buy/sell signal over time).
        let scount = (order_book.signal.series_idx.load(Ordering::Acquire) as usize).min(SIGNAL_SERIES_LEN);
        let series = unsafe { &*order_book.signal.series.get() };
        json.push_str("  \"signal_series\": [");
        for (i, v) in series[..scount].iter().enumerate() {
            json.push_str(&format!("{}{:.3}", if i > 0 { ", " } else { "" }, v));
        }
        json.push_str("],\n");
    }

    json.push_str("  \"trades\": [\n");
    for (i, t) in trades[..count].iter().enumerate() {
        let rt = if t.round_trip_ns > 0 { t.round_trip_ns.to_string() } else { "null".to_string() };
        let tr = if t.transit_est_ns > 0 { t.transit_est_ns.to_string() } else { "null".to_string() };
        let e2e = if t.round_trip_ns > 0 && t.transit_est_ns > 0 {
            (t.transit_est_ns + t.latency_ns + t.round_trip_ns).to_string()
        } else { "null".to_string() };
        let fill = if t.fill_price > 0.0 { t.fill_price.to_string() } else { "null".to_string() };
        let slip = if t.fill_price > 0.0 && t.target_price > 0.0 {
            format!("{:.4}", (t.fill_price as f64 - t.target_price as f64) / t.target_price as f64 * 10_000.0)
        } else { "null".to_string() };
        let comma = if i + 1 < count { "," } else { "" };
        json.push_str(&format!(
            "    {{\"sequence\": {}, \"sig_latency_ns\": {}, \"round_trip_ns\": {}, \"transit_est_ns\": {}, \"end_to_end_ns\": {}, \"target_price\": {}, \"fill_price\": {}, \"slippage_bps\": {}}}{}\n",
            t.sequence, t.latency_ns, rt, tr, e2e, t.target_price, fill, slip, comma
        ));
    }
    json.push_str("  ]\n}\n");

    match std::fs::write(&path, &json) {
        Ok(_)  => println!("[log] saved → {}", path),
        Err(e) => eprintln!("[log] write failed: {}", e),
    }
}

fn unix_to_date_time(secs: u64) -> (String, String) {
    let days_since_epoch = secs / 86400;
    let time_of_day      = secs % 86400;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;
    let z    = days_since_epoch + 719468;
    let era  = z / 146097;
    let doe  = z - era * 146097;
    let yoe  = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y    = yoe + era * 400;
    let doy  = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp   = (5 * doy + 2) / 153;
    let d    = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year  = if month <= 2 { y + 1 } else { y };
    (
        format!("{:04}-{:02}-{:02}", year, month, d),
        format!("{:02}-{:02}-{:02}", h, m, s),
    )
}

// Burst-mode market simulator.
// Sends WARMUP_PACKETS warmup ticks, then NUM_BURSTS bursts of BURST_SIZE ticks
// with ~20µs intra-burst spacing and BURST_GAP_MS silence between bursts.
// Price follows a sine walk to give the signal logic non-trivial input.
//
// Emits the 32-byte v2 packet with a SIMULATED transit estimate (~30 ms ± jitter)
// so the engine's full-stack report renders end-to-end numbers in the default
// in-process run. The real RTT/2 comes from the kraken-feed adapter; here it is a
// stand-in clearly fabricated from the sequence number.
pub(crate) fn run_market_simulator(ingestor_ready: Arc<AtomicBool>) {
    while !ingestor_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    let socket = UdpSocket::bind("0.0.0.0:0").expect("simulator: failed to bind");
    let mut pkt = [0u8; 32];
    pkt[4..8].copy_from_slice(&1000.0_f32.to_le_bytes());

    // Simulated network transit (RTT/2): ~30 ms base with deterministic jitter.
    let sim_transit_ns = |seq: u64| -> u64 { 30_000_000 + (seq.wrapping_mul(1_618_033) % 6_000_000) };

    for seq in 1..=WARMUP_PACKETS {
        let price = 100.0_f32 + 5.0_f32 * (seq as f32 * 0.1_f32).sin();
        pkt[0..4].copy_from_slice(&price.to_le_bytes());
        pkt[8..16].copy_from_slice(&seq.to_le_bytes());
        pkt[16..24].copy_from_slice(&0u64.to_le_bytes());                 // origin_ts (sim: none)
        pkt[24..32].copy_from_slice(&sim_transit_ns(seq).to_le_bytes());  // transit_est (sim)
        socket.send_to(&pkt, INGESTOR_ADDR).expect("simulator: send failed");
    }

    thread::sleep(Duration::from_millis(50));

    for burst in 0..NUM_BURSTS {
        for i in 0..BURST_SIZE {
            let seq = WARMUP_PACKETS + 1 + burst * BURST_SIZE + i;
            let price = 100.0_f32 + 5.0_f32 * (seq as f32 * 0.1_f32).sin();
            pkt[0..4].copy_from_slice(&price.to_le_bytes());
            pkt[8..16].copy_from_slice(&seq.to_le_bytes());
            pkt[16..24].copy_from_slice(&0u64.to_le_bytes());
            pkt[24..32].copy_from_slice(&sim_transit_ns(seq).to_le_bytes());
            socket.send_to(&pkt, INGESTOR_ADDR).expect("simulator: send failed");
            thread::sleep(Duration::from_micros(20));
        }
        if burst + 1 < NUM_BURSTS {
            thread::sleep(Duration::from_millis(BURST_GAP_MS));
        }
    }
}

// The hot path.
//
// ARM64 signal logic (item 4):
//   The 8-price momentum window lives entirely in NEON registers v28/v29
//   (passed as `win_lo`/`win_hi` via inout(vreg)) across loop iterations.
//   On each tick: EXT shifts the window by one f32, FADDP computes the sum,
//   FCMGT compares current price to mean*(1+threshold). ~6 NEON instructions,
//   zero L1 accesses for window state beyond the single tick load.
//
//   CPU core monitoring (item 6):
//   tpidrro_el0 is the OS-managed thread pointer (TLS base), NOT a core ID.
//   True core ID is not accessible from EL0 on macOS ARM64 without kernel
//   assistance. The stall_count already serves as the jitter proxy — a spike
//   in stall_count at a given trade sequence indicates OS preemption.
//
// x86_64 signal logic (AVX2, targeting i9-9900K Coffee Lake):
//   Window is register-resident in a single YMM register (8×f32 = 256 bits).
//   vmaxps + vpermilps reduce the previous 8 prices to their max; vpalignr then
//   shifts the new price in; vucomiss + seta produce the branchless 0/1 trigger.
//
// Signal (both arches): a breakout — the new price must exceed the MAX of the
// previous 8 ticks by SIGNAL_MOMENTUM_BPS basis points. Still a demonstration
// signal, but a more defensible momentum rule than a bare mean comparison.
#[cfg_attr(target_arch = "x86_64", target_feature(enable = "avx2"))]
pub(crate) unsafe fn trading_strategy(
    buffer: &models::RingBuffer,
    reference: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_ring: &models::OrderRing,
    policy: Option<crate::model::Policy>,
) {
    unsafe {
        let mut last_processed_seq: u64 = 0;
        let mut last_spin_ns:       u64 = 0;
        let mut consecutive_clean:  u64 = 0;

        // Momentum window state.
        // ARM64: register-resident float32x4_t pair bound to NEON v-registers via
        //        inout(vreg). The compiler assigns them to vN registers and preserves
        //        them across the asm block as live Rust variables.
        // x86_64: register-resident __m256 (ymm register) holding the 8-price window.
        //         8×f32 = 256 bits fits exactly in one YMM register. Updated each tick
        //         via vpalignr (cross-128-bit shift) + vinsertf128 (lane rebuild).
        #[cfg(target_arch = "aarch64")]
        let (mut win_lo, mut win_hi): (float32x4_t, float32x4_t) =
            (core::mem::zeroed(), core::mem::zeroed());

        #[cfg(target_arch = "x86_64")]
        let mut win: std::arch::x86_64::__m256 = core::mem::zeroed();

        // Breakout signal threshold: trigger when the new price breaks above the
        // MAX of the previous 8-tick window by SIGNAL_MOMENTUM_BPS basis points.
        // A price exceeding the recent high is a more defensible momentum trigger
        // than a simple mean comparison. The scale (1 + bps/10_000) is computed
        // once at startup and loaded into a SIMD scalar each tick; the comparison
        // stays branchless and register-resident on both NEON and AVX2.
        let breakout_scale = (1.0_f32 + SIGNAL_MOMENTUM_BPS as f32 / 10_000.0).to_bits();

        // Target-price mode: when set, buy each time the price dips through the
        // target instead of on a breakout. `was_below` re-arms the cross detector.
        let target_price = order_book.target_price;
        let use_target   = target_price > 0.0;
        let mut was_below = false;

        // Relative-dip mode (takes priority): buy on a dip of `dip_mult` below a
        // rolling EMA reference. Adapts to any absolute price level. `armed`
        // prevents repeated fires until the price recovers back to the reference.
        let use_dip   = order_book.target_dip_bps > 0.0;
        let dip_mult  = 1.0_f32 - order_book.target_dip_bps / 10_000.0;
        const EMA_ALPHA: f32 = 1.0 / 64.0;
        let mut ref_px:   f32  = 0.0;
        let mut ref_init: bool = false;
        let mut armed:    bool = true;

        // Downtick mode: buy on any price decrease (fires on any feed that moves).
        let use_downtick = order_book.buy_on_downtick;
        let mut prev_px:   f32  = 0.0;
        let mut prev_init: bool = false;

        // ── Trading model state (HFT_TRADE) ─────────────────────────────────
        let tcfg = order_book.trade_cfg;
        const RT_MASK: usize = ROUND_TRIP_LOG_SIZE - 1;
        let mut pos_side:    i64 = 0;    // 0 flat, +1 long, -1 short
        let mut entry_price: f32 = 0.0;
        let mut entry_time:  u64 = 0;
        let mut pos_size:    f32 = 0.0;
        // Rolling volatility (EMA of |per-tick return| in bps). In adaptive mode the
        // entry/TP/SL thresholds are multiples of this, so they auto-scale to the
        // market instead of being fixed bps that may exceed the whole range.
        const VOL_ALPHA:  f32 = 1.0 / 32.0;
        const VOL_FLOOR:  f32 = 0.1;   // bps; avoids zero thresholds on a frozen tape
        let mut vol_ema:  f32 = 0.0;
        let mut vol_prev: f32 = 0.0;
        let mut vol_init: bool = false;
        // Order-flow imbalance: EMA of signed trade volume (buy +, sell −).
        const FLOW_ALPHA: f32 = 1.0 / 16.0;
        let mut flow_ema: f32 = 0.0;
        // Capital / equity (compounds across round-trips); drives position sizing.
        let mut equity:    f64  = tcfg.capital as f64;
        let mut entry_margin: f64 = 0.0;   // capital at risk on the open position
        let mut ruined:    bool = false;

        // ── Trend-following model (HFT_MOMENTUM) ─────────────────────────────
        // The signal + execution live in AlphaModel (shared verbatim with the
        // --backtest sweep); the strategy just feeds it ticks and applies the
        // resulting side effects (latency order, round-trip log, net position).
        let mut model = crate::model::AlphaModel::with_policy(tcfg, policy);
        let mut ref_last_seq: u64 = 0;           // last reference cursor we sampled
        const SIG_SERIES_MASK: usize = SIGNAL_SERIES_LEN - 1;
        let mut sig_tick: u64 = 0;

        // Pending simulated fills (FIFO ring). When we send an order we don't know
        // the fill price yet — it's the market price one transit later. Each entry
        // resolves when a later tick's timestamp passes its due time, at which
        // point that tick's price becomes the fill. The gap between fill and target
        // is the latency-induced slippage. CAP comfortably exceeds in-flight orders.
        const PCAP:  usize = 256;
        const PMASK: usize = PCAP - 1;
        let mut pend_slot = [0usize; PCAP];
        let mut pend_due  = [0u64;   PCAP];
        let mut p_head: usize = 0;
        let mut p_tail: usize = 0;

        // NEON warmup: exercise the vector execution units, pull hot-path code into
        // the instruction cache, and commit OS pages for start_time (via elapsed_ns).
        // black_box on both outputs prevents dead-code elimination.
        #[cfg(target_arch = "aarch64")]
        for _ in 0..10_000 {
            let mut dummy: u64;
            asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0",
                 res = out(reg) dummy, options(nostack, nomem));
            black_box(dummy);
            black_box(elapsed_ns(&buffer.start_time));
        }

        // AVX2 warmup: exercise 256-bit vector execution units (ymm registers),
        // pull hot-path code into the instruction cache, and commit OS pages for
        // start_time (via elapsed_ns). vmulps ymm operates on 8 f32 lanes — matches
        // the production window size and warms the same FP execution port.
        #[cfg(target_arch = "x86_64")]
        for _ in 0..10_000 {
            let mut dummy: u32;
            asm!(
                "vmulps ymm0, ymm0, ymm0",
                "vmovd {res:e}, xmm0",
                res = out(reg) dummy,
                out("ymm0") _,
                options(nostack, nomem)
            );
            black_box(dummy);
            black_box(elapsed_ns(&buffer.start_time));
        }

        loop {
            let current_seq = buffer.latest_idx.load(Ordering::Acquire);

            if current_seq > last_processed_seq {
                let idx      = (current_seq & BUFFER_MASK) as usize;
                let tick_ptr = &buffer.ticks[idx];

                // Gap / dirty-flag check: single Relaxed load, branch on register.
                // No memory barrier on the critical path (acquire is already handled
                // by the latest_idx load above).
                if order_book.dirty.load(Ordering::Relaxed) {
                    // Gap kill switch (item 5): halt if too many gaps accumulated.
                    if order_book.gap_count.load(Ordering::Relaxed) > MAX_GAP_COUNT {
                        halt_trading(order_book, "sequence gap limit exceeded");
                    }
                    // Wait for N consecutive clean ticks before resuming.
                    consecutive_clean += 1;
                    if consecutive_clean >= CLEAN_SEQ_THRESHOLD {
                        order_book.dirty.store(false, Ordering::Relaxed);
                        consecutive_clean = 0;
                    }
                } else {
                    consecutive_clean = 0;

                    let price       = tick_ptr.price;
                    let tick_now_ns = tick_ptr.timestamp;

                    // Resolve any pending simulated fills that have come due: this
                    // tick's price is the market price ~one transit after the order
                    // was sent, so it becomes that order's fill price.
                    while p_head != p_tail {
                        if tick_now_ns >= pend_due[p_head & PMASK] {
                            let fslot = pend_slot[p_head & PMASK];
                            (*order_book.trade_log.entries.get())[fslot].fill_price = price;
                            order_book.filled.fetch_add(1, Ordering::Relaxed);
                            p_head += 1;
                        } else {
                            break;
                        }
                    }

                    // ── Signal computation (breakout) ───────────────────────────────
                    //
                    // ARM64 (item 4): register-resident 8-price window.
                    //   win_lo = oldest 4 prices (f32 × 4), win_hi = newest 4 prices.
                    //   FMAX + FMAXV take the max of the previous 8; EXT then shifts the
                    //   new price in. Trigger: current_price > prev_window_max * (1+bps).
                    //
                    // x86_64 (item 7): register-resident AVX2 8-price window in a
                    //   single __m256 (ymm). Same breakout rule; full implementation in
                    //   the x86_64 asm block below.
                    // ────────────────────────────────────────────────────────────────

                    #[cfg(target_arch = "aarch64")]
                    let decision: u32 = {
                        let mut result: u32;
                        asm!(
                            // Load tick: [price, volume, seq_lo, seq_hi] → v0
                            "ld1 {{v0.4s}}, [{ptr}]",
                            // Max of the PREVIOUS 8-price window (before the shift):
                            //   v4 = lane-wise max(win_lo, win_hi) → 4 partial maxima
                            "fmax v4.4s, {wl:v}.4s, {wh:v}.4s",
                            //   s4 = horizontal max across the 4 lanes → max of 8 prices
                            "fmaxv s4, v4.4s",
                            // Shift window left by one f32 (bring in the new price):
                            //   win_lo = [win_lo[1], win_lo[2], win_lo[3], win_hi[0]]
                            "ext {wl:v}.16b, {wl:v}.16b, {wh:v}.16b, #4",
                            //   win_hi = [win_hi[1], win_hi[2], win_hi[3], price]
                            "ext {wh:v}.16b, {wh:v}.16b, v0.16b, #4",
                            // Threshold: s4 = prev_max * (1 + bps)
                            "fmov s3, {scale:w}",
                            "fmul s4, s4, s3",
                            // Compare: price (s0 = v0[0]) > prev_max * (1 + bps) (s4)
                            // FCMGT sets s2 = 0xFFFFFFFF if true, else 0
                            "fcmgt s2, s0, s4",
                            "fmov {res:w}, s2",
                            ptr   = in(reg)     tick_ptr as *const models::MarketTick as *const u8,
                            scale = in(reg)     breakout_scale,
                            wl    = inout(vreg) win_lo,
                            wh    = inout(vreg) win_hi,
                            res   = out(reg)    result,
                            out("v0") _, out("v2") _, out("v3") _, out("v4") _,
                            options(nostack)
                        );
                        result
                    };

                    // x86_64 AVX2 register-resident 8-price breakout window.
                    //
                    // Window layout in ymm register (one f32 per lane):
                    //   low  128-bit half (xmm): [p_oldest, p+1, p+2, p+3]
                    //   high 128-bit half:        [p+4,     p+5, p+6, p_newest]
                    //
                    // Each tick: take the MAX of the previous 8 prices, then shift the
                    // new price in, then trigger if new_price > prev_max * (1 + bps).
                    //
                    // Max reduction (8 f32 → scalar): vmaxps the two halves to 4 maxima,
                    // then vpermilps + vmaxss twice to reduce to lane 0.
                    //
                    // Shift protocol (vpalignr — cross-byte shift within 128-bit lanes):
                    //   new_lo = vpalignr(hi, lo, 4)    → [p+1, p+2, p+3, p+4]
                    //   new_hi = vpalignr(price, hi, 4) → [p+5, p+6, p_newest, new_price]
                    //   rebuild 256-bit win with vinsertf128
                    #[cfg(target_arch = "x86_64")]
                    let decision: u32 = {
                        let price = (tick_ptr as *const models::MarketTick as *const f32).read();
                        let threshold_scale = f32::from_bits(breakout_scale);
                        let mut result: u32;
                        asm!(
                            // Extract 128-bit halves: xmm0 = lo [p0..p3], xmm1 = hi [p4..p7]
                            "vextractf128 xmm0, {win}, 0",
                            "vextractf128 xmm1, {win}, 1",
                            // --- Max of the previous 8 prices (before the shift) ---
                            // xmm2 = lane-wise max of the two halves → 4 partial maxima
                            "vmaxps xmm2, xmm0, xmm1",
                            // reduce 4 → 1: max with lanes [2,3] then lane [1]
                            "vpermilps xmm3, xmm2, 0x0E",
                            "vmaxps xmm2, xmm2, xmm3",
                            "vpermilps xmm3, xmm2, 0x01",
                            "vmaxss xmm2, xmm2, xmm3",     // xmm2[0] = max of 8 prices
                            // --- Window shift (xmm0/xmm1 still hold the original halves) ---
                            // Shift lo: [p1, p2, p3, p4] = concat(hi, lo) >> 4 bytes
                            "vpalignr xmm0, xmm1, xmm0, 4",
                            // Shift hi: [p5, p6, p7, new_price] = concat(price, hi) >> 4 bytes
                            "vpalignr xmm1, {price}, xmm1, 4",
                            // Rebuild 256-bit window with updated halves
                            "vinsertf128 {win}, {win}, xmm0, 0",
                            "vinsertf128 {win}, {win}, xmm1, 1",
                            // --- Threshold: prev_max * (1 + bps) ---
                            "vmulss xmm2, xmm2, {scale}",
                            // --- Compare: new_price > threshold → result = 1 ---
                            // vucomiss sets ZF=CF=0 when src1 > src2 (ordered, no NaN)
                            "vucomiss {price}, xmm2",
                            "seta {res:l}",
                            "movzx {res:e}, {res:l}",
                            win   = inout(ymm_reg) win,
                            price = in(xmm_reg) price,
                            scale = in(xmm_reg) threshold_scale,
                            res   = lateout(reg) result,
                            out("xmm0") _, out("xmm1") _, out("xmm2") _, out("xmm3") _,
                            options(nostack, nomem)
                        );
                        result
                    };

                    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
                    let decision: u32 = 0; // unsupported arch: never trigger

                    // Buy trigger: a downward cross through the target price (target
                    // mode), or the SIMD breakout (default). The breakout asm runs
                    // either way to keep the window warm; its result is just ignored
                    // in target mode.
                    let trigger = if tcfg.enabled {
                        false   // trade mode runs its own entry/exit machine below
                    } else if use_downtick {
                        let fire = prev_init && price < prev_px;
                        prev_px = price;
                        prev_init = true;
                        fire
                    } else if use_dip {
                        if !ref_init { ref_px = price; ref_init = true; }
                        let thresh = ref_px * dip_mult;
                        let fire = armed && price <= thresh;
                        if fire {
                            armed = false;          // wait for recovery before firing again
                        } else if price >= ref_px {
                            armed = true;
                        }
                        ref_px += (price - ref_px) * EMA_ALPHA;
                        fire
                    } else if use_target {
                        let below = price <= target_price;
                        let fire  = below && !was_below;
                        was_below = below;
                        fire
                    } else {
                        black_box(decision) != 0
                    };

                    if trigger {
                        let buy_time_ns    = elapsed_ns(&buffer.start_time);
                        let ingest_time_ns = *(&tick_ptr.timestamp as *const u64);
                        let latency_ns     = buy_time_ns.saturating_sub(ingest_time_ns);

                        if current_seq > WARMUP_PACKETS {
                            // ── Risk checks (item 5) ─────────────────────────────────
                            // halt check — predicted-not-taken after first few iters.
                            if order_book.halt.load(Ordering::Relaxed) {
                                // Do nothing; halt is permanent.
                            } else {
                                let pos = order_book.net_position.load(Ordering::Relaxed);
                                if pos >= MAX_POSITION {
                                    halt_trading(order_book, "max position limit reached");
                                } else {
                                    // Commit trade.
                                    order_book.sig_hist.record(latency_ns);
                                    order_book.net_position.fetch_add(1, Ordering::Relaxed);

                                    let slot  = order_book.trade_log.write_idx
                                        .load(Ordering::Relaxed) as usize & TRADE_LOG_MASK;
                                    let entry = &mut (*order_book.trade_log.entries.get())[slot];
                                    let order_send_ns = elapsed_ns(&buffer.start_time);
                                    entry.sequence       = current_seq;
                                    entry.ingest_time_ns = ingest_time_ns;
                                    entry.buy_time_ns    = buy_time_ns;
                                    entry.latency_ns     = latency_ns;
                                    entry.order_send_ns  = order_send_ns;
                                    entry.round_trip_ns  = 0;
                                    // Carry the feed's RTT/2 transit estimate (L1-resident
                                    // field on the tick) so end-to-end latency can be
                                    // reconstructed at shutdown after the slot is reused.
                                    entry.transit_est_ns = tick_ptr.transit_est_ns;
                                    // Intended price (target, or entry price in breakout
                                    // mode); fill_price stays 0 until the deferred fill.
                                    entry.target_price   = if use_target { target_price } else { price };
                                    entry.fill_price     = 0.0;
                                    order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

                                    let ring_slot = order_ring.write_idx
                                        .load(Ordering::Relaxed) as usize & ORDER_RING_MASK;
                                    let oe = &mut (*order_ring.entries.get())[ring_slot];
                                    oe.sequence      = current_seq;
                                    oe.slot          = slot as u64;
                                    oe.order_send_ns = order_send_ns;
                                    order_ring.write_idx.fetch_add(1, Ordering::Release);

                                    order_book.attempts.fetch_add(1, Ordering::Relaxed);
                                    // Schedule the simulated market fill one transit
                                    // (RTT/2) after the order was sent.
                                    let due = order_send_ns + tick_ptr.transit_est_ns;
                                    if p_tail.wrapping_sub(p_head) < PCAP {
                                        let pi = p_tail & PMASK;
                                        pend_slot[pi] = slot;
                                        pend_due[pi]  = due;
                                        p_tail += 1;
                                    }
                                }
                            }
                        }
                    }

                    // ── Trend-following + cross-market signal (HFT_MOMENTUM) ─────
                    // Compute a composite buy/sell signal S each tick from own trend
                    // (fast vs slow EMA), own order flow, the reference market's trend
                    // (basket), and the reference's recent return (lead-lag). Trade
                    // WITH the trend but time entries on a pullback vs the fast EMA;
                    // exit on signal-flip or a trailing stop (TP/SL/liq as caps).
                    if tcfg.enabled && tcfg.momentum {
                        // Feed the reference market when its cursor advances (before the
                        // traded tick, matching the model's expected ordering).
                        let ref_seq = reference.latest_idx.load(Ordering::Acquire);
                        if ref_seq > 0 && ref_seq != ref_last_seq {
                            ref_last_seq = ref_seq;
                            model.on_reference_tick(reference.ticks[(ref_seq & BUFFER_MASK) as usize].price);
                        }
                        let warmed = current_seq > WARMUP_PACKETS;
                        let halted = order_book.halt.load(Ordering::Relaxed);
                        let decision = model.on_traded_tick(price, tick_ptr.bid, tick_ptr.ask, tick_ptr.volume, tick_now_ns, warmed, halted);

                        // Expose the signal: latest value, downsampled series, vol.
                        order_book.latest_signal_bits.store(model.latest_signal_bps().to_bits(), Ordering::Relaxed);
                        order_book.vol_ema_bits.store(model.vol_ema().to_bits(), Ordering::Relaxed);
                        sig_tick += 1;
                        if sig_tick & 7 == 0 {
                            let si = (order_book.signal.series_idx.load(Ordering::Relaxed) as usize) & SIG_SERIES_MASK;
                            (*order_book.signal.series.get())[si] = model.latest_signal_bps();
                            order_book.signal.series_idx.fetch_add(1, Ordering::Relaxed);
                        }

                        match decision {
                            crate::model::Decision::Enter { side, signal_bps } => {
                                let eidx = (order_book.round_trips.write_idx.load(Ordering::Relaxed) as usize) & RT_MASK;
                                (*order_book.signal.at_entry.get())[eidx] = signal_bps;
                                order_book.net_position.fetch_add(side, Ordering::Relaxed);
                                emit_latency_order(buffer, order_book, order_ring,
                                    current_seq, tick_now_ns, tick_ptr.transit_est_ns);
                            }
                            crate::model::Decision::Exit(rt) => {
                                let slot = order_book.round_trips.write_idx.load(Ordering::Relaxed) as usize & RT_MASK;
                                (*order_book.round_trips.entries.get())[slot] = rt;
                                order_book.round_trips.write_idx.fetch_add(1, Ordering::Release);
                                order_book.net_position.fetch_add(-rt.side, Ordering::Relaxed);
                                emit_latency_order(buffer, order_book, order_ring,
                                    current_seq, tick_now_ns, tick_ptr.transit_est_ns);
                                if model.ruined && !order_book.halt.load(Ordering::Relaxed) {
                                    halt_trading(order_book, "account ruined (equity ≤ 0)");
                                }
                            }
                            crate::model::Decision::None => {}
                        }
                    }

                    // ── Trading model: long & short mean-reversion ──────────────
                    // Enter on a dip/rip vs a rolling EMA reference (optionally
                    // confirmed by order flow); exit on take-profit, stop-loss, the
                    // opposite signal, or LIQUIDATION (adverse move ≥ 1/leverage).
                    // Sizing is capital-based: margin = risk_frac·equity, notional =
                    // margin·leverage, and equity compounds across round-trips.
                    if tcfg.enabled && !tcfg.momentum {
                        if !ref_init { ref_px = price; ref_init = true; }

                        // Rolling volatility estimate (bps/tick) and order-flow EMA.
                        if vol_init {
                            let ret = ((price - vol_prev) / vol_prev * 10_000.0).abs();
                            vol_ema += (ret - vol_ema) * VOL_ALPHA;
                        } else {
                            vol_init = true;
                        }
                        vol_prev = price;
                        order_book.vol_ema_bits.store(vol_ema.to_bits(), Ordering::Relaxed);
                        flow_ema += (tick_ptr.volume - flow_ema) * FLOW_ALPHA;  // signed volume

                        let (entry_bps, tp_bps, sl_bps) = if tcfg.adaptive {
                            let s = vol_ema.max(VOL_FLOOR);
                            (s * 1.0, s * 1.5, s * 2.5)
                        } else {
                            (tcfg.entry_dip_bps, tcfg.tp_bps, tcfg.sl_bps)
                        };
                        let liq_bps = 10_000.0 / tcfg.leverage.max(1.0);  // adverse move that wipes margin

                        let dip   = entry_bps / 10_000.0;
                        let long_sig  = price <= ref_px * (1.0 - dip);
                        let short_sig = price >= ref_px * (1.0 + dip);
                        ref_px += (price - ref_px) * EMA_ALPHA;

                        if current_seq > WARMUP_PACKETS && !ruined {
                            if pos_side == 0 {
                                // Order-flow confirmation: only buy dips into net buying,
                                // short rips into net selling (when HFT_USE_FLOW is set).
                                let long_ok  = long_sig  && (!tcfg.use_flow || flow_ema >= 0.0);
                                let short_ok = short_sig && tcfg.allow_short
                                                         && (!tcfg.use_flow || flow_ema <= 0.0);
                                let dir: i64 = if long_ok { 1 } else if short_ok { -1 } else { 0 };
                                if dir != 0 && !order_book.halt.load(Ordering::Relaxed) {
                                    let depth_frac = if dir == 1 { (ref_px - price) / ref_px }
                                                     else { (price - ref_px) / ref_px };
                                    let depth_mult = ((depth_frac * 10_000.0) / entry_bps.max(VOL_FLOOR))
                                                     .clamp(1.0, tcfg.max_size_mult) as f64;
                                    let risk     = (tcfg.risk_frac as f64 * depth_mult).min(1.0);
                                    entry_margin = equity * risk;
                                    let notional = entry_margin * tcfg.leverage as f64;
                                    // Cross the spread on entry: long buys the ask, short sells the bid.
                                    entry_price  = crate::model::taker_fill(price, tick_ptr.bid, tick_ptr.ask, dir == 1, tcfg.slippage_bps);
                                    pos_size     = (notional / entry_price as f64) as f32;  // units
                                    entry_time   = tick_now_ns;
                                    pos_side     = dir;
                                    order_book.net_position.fetch_add(dir, Ordering::Relaxed);
                                    emit_latency_order(buffer, order_book, order_ring,
                                        current_seq, tick_now_ns, tick_ptr.transit_est_ns);
                                }
                            } else {
                                let move_bps = (price - entry_price) / entry_price
                                    * 10_000.0 * pos_side as f32;
                                let opp        = if pos_side == 1 { short_sig } else { long_sig };
                                let liquidated = move_bps <= -liq_bps;
                                if move_bps >= tp_bps || move_bps <= -sl_bps || opp || liquidated {
                                    // Cross the spread on exit: long sells the bid, short buys the ask.
                                    // move_bps (mid-marked) drove the triggers; realized gross is fill-to-fill.
                                    let exit_fill  = crate::model::taker_fill(price, tick_ptr.bid, tick_ptr.ask, pos_side == -1, tcfg.slippage_bps);
                                    let gross_bps  = ((exit_fill - entry_price) / entry_price * 10_000.0 * pos_side as f32) as f64;
                                    let notional   = entry_margin * tcfg.leverage as f64;
                                    let fees_quote = notional * (2.0 * tcfg.fee_bps as f64 / 10_000.0);
                                    // Isolated margin: a loss can't exceed the posted margin.
                                    let raw_pnl    = notional * (gross_bps / 10_000.0) - fees_quote;
                                    let pnl_quote  = raw_pnl.max(-entry_margin);
                                    let was_liq    = liquidated || raw_pnl <= -entry_margin;

                                    equity += pnl_quote;
                                    if equity <= 0.0 {
                                        equity = 0.0;
                                        ruined = true;
                                        halt_trading(order_book, "account ruined (equity ≤ 0)");
                                    }

                                    let slot = order_book.round_trips.write_idx
                                        .load(Ordering::Relaxed) as usize & RT_MASK;
                                    let rt = &mut (*order_book.round_trips.entries.get())[slot];
                                    rt.entry_time_ns = entry_time;
                                    rt.exit_time_ns  = tick_now_ns;
                                    rt.hold_ns       = tick_now_ns.saturating_sub(entry_time);
                                    rt.side          = pos_side;
                                    rt.entry_price   = entry_price;
                                    rt.exit_price    = exit_fill;
                                    rt.size          = pos_size;
                                    rt.gross_bps     = gross_bps as f32;
                                    rt.net_bps       = (gross_bps - 2.0 * tcfg.fee_bps as f64) as f32;
                                    rt.pnl_quote     = pnl_quote as f32;
                                    rt.fees_quote    = fees_quote as f32;
                                    rt.flags         = if was_liq { 1.0 } else { 0.0 };
                                    order_book.round_trips.write_idx.fetch_add(1, Ordering::Release);

                                    order_book.net_position.fetch_add(-pos_side, Ordering::Relaxed);
                                    emit_latency_order(buffer, order_book, order_ring,
                                        current_seq, tick_now_ns, tick_ptr.transit_est_ns);
                                    pos_side = 0;
                                }
                            }
                        }
                    }
                }

                last_processed_seq = current_seq;
            } else {
                // Idle branch: stall detection (item 1) + prefetch (item 4).
                let now_ns = elapsed_ns(&buffer.start_time);
                if last_spin_ns > 0 && now_ns.saturating_sub(last_spin_ns) > 500 {
                    order_book.stall_count.fetch_add(1, Ordering::Relaxed);
                }
                last_spin_ns = now_ns;

                std::hint::spin_loop();

                // Prefetch next trade-log slot into L1 in exclusive (store-ready) state
                // so the cache line is hot when the next tick arrives.
                let next_entry_ptr = (*order_book.trade_log.entries.get())
                    .as_ptr()
                    .add(order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize
                        & TRADE_LOG_MASK);

                #[cfg(target_arch = "aarch64")]
                asm!(
                    "prfm pstl1keep, [{entry}]",
                    entry = in(reg) next_entry_ptr,
                    options(nostack, nomem)
                );

                #[cfg(target_arch = "x86_64")]
                std::arch::x86_64::_mm_prefetch(
                    next_entry_ptr as *const i8,
                    std::arch::x86_64::_MM_HINT_ET0,
                );
            }
        }
    }
}
