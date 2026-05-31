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

use crate::{models, BUFFER_SIZE, ORDER_RING_SIZE, TRADE_LOG_SIZE};
use rust_hft_software::config::{
    BURST_GAP_MS, BURST_SIZE, CLEAN_SEQ_THRESHOLD, INGESTOR_ADDR,
    MAX_GAP_COUNT, MAX_POSITION, NUM_BURSTS, WARMUP_PACKETS,
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
pub(crate) fn run_ingestor(
    buffer: Arc<models::RingBuffer>,
    order_book: Arc<models::OrderBook>,
    last_packet_ns: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
) {
    let socket = UdpSocket::bind(INGESTOR_ADDR).expect("ingestor: failed to bind");
    socket.set_nonblocking(true).expect("ingestor: failed to set non-blocking");
    ready.store(true, Ordering::Release);

    let mut seq: u64 = 1;
    let mut last_ingest_seq: u64 = 0;
    let mut pkt = [0u8; 64];

    loop {
        match socket.recv_from(&mut pkt) {
            Ok((amt, _)) if amt >= 16 => {
                let recv_seq = u64::from_le_bytes(pkt[8..16].try_into().unwrap());

                // Sequence gap detection: if the packet sequence is not the expected
                // next value, set the dirty flag so the strategy skips trading on
                // potentially stale or reordered data.
                if last_ingest_seq > 0 && recv_seq != last_ingest_seq + 1 {
                    order_book.gap_count.fetch_add(1, Ordering::Relaxed);
                    order_book.dirty.store(true, Ordering::Relaxed);
                }
                last_ingest_seq = recv_seq;

                let idx = (seq & BUFFER_MASK) as usize;
                let ingest_time_ns = elapsed_ns(&buffer.start_time);

                unsafe {
                    let tick_ptr = &buffer.ticks[idx] as *const _ as *mut u8;
                    std::ptr::copy_nonoverlapping(pkt.as_ptr(), tick_ptr, 16);
                    *(tick_ptr.add(16) as *mut u64) = ingest_time_ns;
                }

                buffer.latest_idx.store(seq, Ordering::Release);
                last_packet_ns.store(ingest_time_ns, Ordering::Relaxed);
                seq += 1;
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

fn print_stats(order_book: &models::OrderBook, mem_pre_log: &MemoryStats) {
    let count  = (order_book.trade_log.write_idx.load(Ordering::Acquire) as usize).min(TRADE_LOG_SIZE);
    let trades = unsafe { &*order_book.trade_log.entries.get() };

    println!("Total trades executed: {}\n", count);
    println!("{:<12} {:<20} {:<20}", "Sequence", "Sig Latency (ns)", "Round Trip (ns)");
    println!("{}", "─".repeat(55));

    for t in &trades[..count] {
        let rt = if t.round_trip_ns > 0 { t.round_trip_ns.to_string() } else { "—".to_string() };
        println!("{:<12} {:<20} {:<20}", t.sequence, t.latency_ns, rt);
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
    } else {
        json.push_str("  \"signal_latency\": null,\n");
        json.push_str("  \"round_trip\": null,\n");
    }

    json.push_str("  \"trades\": [\n");
    for (i, t) in trades[..count].iter().enumerate() {
        let rt = if t.round_trip_ns > 0 { t.round_trip_ns.to_string() } else { "null".to_string() };
        let comma = if i + 1 < count { "," } else { "" };
        json.push_str(&format!(
            "    {{\"sequence\": {}, \"sig_latency_ns\": {}, \"round_trip_ns\": {}}}{}\n",
            t.sequence, t.latency_ns, rt, comma
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
pub(crate) fn run_market_simulator(ingestor_ready: Arc<AtomicBool>) {
    while !ingestor_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    let socket = UdpSocket::bind("0.0.0.0:0").expect("simulator: failed to bind");
    let mut pkt = [0u8; 16];
    pkt[4..8].copy_from_slice(&1000.0_f32.to_le_bytes());

    for seq in 1..=WARMUP_PACKETS {
        let price = 100.0_f32 + 5.0_f32 * (seq as f32 * 0.1_f32).sin();
        pkt[0..4].copy_from_slice(&price.to_le_bytes());
        pkt[8..16].copy_from_slice(&seq.to_le_bytes());
        socket.send_to(&pkt, INGESTOR_ADDR).expect("simulator: send failed");
    }

    thread::sleep(Duration::from_millis(50));

    for burst in 0..NUM_BURSTS {
        for i in 0..BURST_SIZE {
            let seq = WARMUP_PACKETS + 1 + burst * BURST_SIZE + i;
            let price = 100.0_f32 + 5.0_f32 * (seq as f32 * 0.1_f32).sin();
            pkt[0..4].copy_from_slice(&price.to_le_bytes());
            pkt[8..16].copy_from_slice(&seq.to_le_bytes());
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
//   vpalignr shifts the window by one f32 across the 128-bit lane boundary.
//   vhaddps × 2 + vpermilps + vaddss reduce 8 floats to a scalar sum.
//   vucomiss + seta produce the 0/1 trigger with no branch in the signal path.
#[cfg_attr(target_arch = "x86_64", target_feature(enable = "avx2"))]
pub(crate) unsafe fn trading_strategy(
    buffer: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_ring: &models::OrderRing,
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

        // Scale factor for signal threshold: sum * (1/8 * 1.001) = mean * 1.001.
        // Computed once at startup; loaded into a SIMD scalar register each tick.
        let momentum_scale = (0.125125_f32).to_bits();

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

                    // ── Signal computation ──────────────────────────────────────────
                    //
                    // ARM64 (item 4): register-resident 8-price momentum window.
                    //   win_lo = oldest 4 prices (f32 × 4), win_hi = newest 4 prices.
                    //   EXT shifts window by one lane; FADDP tree sums all 8.
                    //   Trigger: current_price > window_mean * 1.001
                    //
                    // x86_64 (item 7): register-resident AVX2 8-price window in a
                    //   single __m256 (ymm). Functionally identical to the NEON path;
                    //   full implementation in the x86_64 asm block below.
                    // ────────────────────────────────────────────────────────────────

                    #[cfg(target_arch = "aarch64")]
                    let decision: u32 = {
                        let mut result: u32;
                        asm!(
                            // Load tick: [price, volume, seq_lo, seq_hi] → v0
                            "ld1 {{v0.4s}}, [{ptr}]",
                            // Shift window left by one f32:
                            //   win_lo = [win_lo[1], win_lo[2], win_lo[3], win_hi[0]]
                            "ext {wl:v}.16b, {wl:v}.16b, {wh:v}.16b, #4",
                            //   win_hi = [win_hi[1], win_hi[2], win_hi[3], price]
                            "ext {wh:v}.16b, {wh:v}.16b, v0.16b, #4",
                            // Sum 8 prices via FADDP tree:
                            //   v1 = [wl[0]+wl[1], wl[2]+wl[3], wh[0]+wh[1], wh[2]+wh[3]]
                            "faddp v1.4s, {wl:v}.4s, {wh:v}.4s",
                            //   v1 = [wl_sum, wh_sum, wl_sum, wh_sum]
                            "faddp v1.4s, v1.4s, v1.4s",
                            //   s1 = total sum of 8 prices
                            "faddp s1, v1.2s",
                            // Scale: s1 = sum * (1/8 * 1.001) = mean * 1.001
                            "fmov s3, {scale:w}",
                            "fmul s1, s1, s3",
                            // Compare: price (s0 = v0[0]) > mean * 1.001 (s1)
                            // FCMGT sets s2 = 0xFFFFFFFF if true, else 0
                            "fcmgt s2, s0, s1",
                            "fmov {res:w}, s2",
                            ptr   = in(reg)     tick_ptr as *const models::MarketTick as *const u8,
                            scale = in(reg)     momentum_scale,
                            wl    = inout(vreg) win_lo,
                            wh    = inout(vreg) win_hi,
                            res   = out(reg)    result,
                            out("v0") _, out("v1") _, out("v2") _, out("v3") _,
                            options(nostack)
                        );
                        result
                    };

                    // x86_64 AVX2 register-resident 8-price momentum window.
                    //
                    // Window layout in ymm register (one f32 per lane):
                    //   low  128-bit half (xmm): [p_oldest, p+1, p+2, p+3]
                    //   high 128-bit half:        [p+4,     p+5, p+6, p_newest]
                    //
                    // Shift protocol (vpalignr — AVX2 cross-byte shift within 128-bit lanes):
                    //   extract lo/hi halves with vextractf128
                    //   new_lo = vpalignr(hi, lo, 4)  → [p+1, p+2, p+3, p+4]
                    //   new_hi = vpalignr(price, hi, 4) → [p+5, p+6, p_newest, new_price]
                    //   rebuild 256-bit win with vinsertf128
                    //
                    // Horizontal sum (8 f32 → scalar):
                    //   two vhaddps passes reduce to 2 partial sums in xmm
                    //   vpermilps + vaddss for cross-element final reduction
                    //
                    // Trigger: new_price > (total_sum * momentum_scale)
                    //          where momentum_scale = 1/(8 * 1.001) ≈ 0.125125
                    #[cfg(target_arch = "x86_64")]
                    let decision: u32 = {
                        let price = (tick_ptr as *const models::MarketTick as *const f32).read();
                        let threshold_scale = f32::from_bits(momentum_scale);
                        let mut result: u32;
                        asm!(
                            // --- Window shift ---
                            // Extract 128-bit halves: xmm0 = lo [p0..p3], xmm1 = hi [p4..p7]
                            "vextractf128 xmm0, {win}, 0",
                            "vextractf128 xmm1, {win}, 1",
                            // Shift lo: [p1, p2, p3, p4] = concat(hi, lo) >> 4 bytes
                            "vpalignr xmm0, xmm1, xmm0, 4",
                            // Shift hi: [p5, p6, p7, new_price] = concat(price, hi) >> 4 bytes
                            // {price} is a compiler-allocated xmm holding the new price scalar
                            "vpalignr xmm1, {price}, xmm1, 4",
                            // Rebuild 256-bit window with updated halves
                            "vinsertf128 {win}, {win}, xmm0, 0",
                            "vinsertf128 {win}, {win}, xmm1, 1",
                            // --- Horizontal sum over all 8 updated prices ---
                            // [p1+p2, p3+p4, p5+p6, p7+new]
                            "vhaddps xmm0, xmm0, xmm1",
                            // [(p1+p2+p3+p4), (p5+p6+p7+new), same×2]
                            "vhaddps xmm0, xmm0, xmm0",
                            // Move high pair sum to xmm1[0] for final cross-element add
                            "vpermilps xmm1, xmm0, 1",
                            // xmm0[0] = total sum of all 8 prices
                            "vaddss xmm0, xmm0, xmm1",
                            // --- Threshold: total_sum * momentum_scale = mean * 1.001 ---
                            "vmulss xmm0, xmm0, {scale}",
                            // --- Compare: new_price > threshold → result = 1 ---
                            // vucomiss sets ZF=CF=0 when src1 > src2 (ordered, no NaN)
                            "vucomiss {price}, xmm0",
                            "seta {res:l}",
                            "movzx {res:e}, {res:l}",
                            win   = inout(ymm_reg) win,
                            price = in(xmm_reg) price,
                            scale = in(xmm_reg) threshold_scale,
                            res   = lateout(reg) result,
                            out("xmm0") _, out("xmm1") _,
                            options(nostack, nomem)
                        );
                        result
                    };

                    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
                    let decision: u32 = 0; // unsupported arch: never trigger

                    let trigger = black_box(decision) != 0;

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
                                    order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

                                    let ring_slot = order_ring.write_idx
                                        .load(Ordering::Relaxed) as usize & ORDER_RING_MASK;
                                    let oe = &mut (*order_ring.entries.get())[ring_slot];
                                    oe.sequence      = current_seq;
                                    oe.slot          = slot as u64;
                                    oe.order_send_ns = order_send_ns;
                                    order_ring.write_idx.fetch_add(1, Ordering::Release);
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
