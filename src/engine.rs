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

#[inline(always)]
fn elapsed_ns(start: &std::time::Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

pub(crate) fn set_qos_interactive() {
    #[cfg(target_os = "macos")]
    unsafe { pthread_set_qos_class_self_np(0x21, 0); }
    // Linux equivalent: sched_setaffinity / pthread_setaffinity_np — no-op here
    // until a production Linux target is added.
}

// Set a Mach thread affinity tag for the calling thread (item 6).
// Same tag = same cluster hint from the scheduler. Tag 1 reserved for the
// strategy thread; the OS will prefer keeping it on the same P-core cluster.
// No-op on non-macOS targets.
pub(crate) fn set_thread_affinity_tag(tag: i32) {
    #[cfg(target_os = "macos")]
    unsafe {
        const THREAD_AFFINITY_POLICY: u32 = 4;
        const THREAD_AFFINITY_POLICY_COUNT: u32 = 1;
        let thread = mach_thread_self();
        thread_policy_set(thread, THREAD_AFFINITY_POLICY, &tag, THREAD_AFFINITY_POLICY_COUNT);
    }
    let _ = tag; // suppress unused-variable warning on non-macOS
}

// Called on any risk-limit breach. #[cold] biases the branch predictor in the
// hot path toward the non-halting (not-taken) direction after the first few
// warmup iterations train it. The halt flag is permanent within a session.
#[cold]
fn halt_trading(order_book: &models::OrderBook, reason: &str) {
    order_book.halt.store(true, Ordering::Relaxed);
    eprintln!("[risk] HALT: {}", reason);
}

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

        print_stats(&order_book);
        write_log(&order_book);
        std::process::exit(0);
    }
}

fn print_stats(order_book: &models::OrderBook) {
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
}

fn write_log(order_book: &models::OrderBook) {
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
// x86_64 signal logic (item 7):
//   Window kept in a [f32; 8] stack array (L1-resident). Mean computed via
//   horizontal SSE add. Not register-resident; see one_threaded.rs for the
//   AVX2 reference. The structure is identical to the ARM64 path.
#[inline(always)]
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
        // x86_64: [f32; 8] stack array; not register-resident (register-resident
        //         AVX2 path is the next step — see TODO item 7 note in one_threaded.rs).
        #[cfg(target_arch = "aarch64")]
        let (mut win_lo, mut win_hi): (float32x4_t, float32x4_t) =
            (core::mem::zeroed(), core::mem::zeroed());

        #[cfg(target_arch = "x86_64")]
        let mut win_buf: [f32; 8] = [0.0; 8];

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

        #[cfg(target_arch = "x86_64")]
        for _ in 0..10_000 {
            let mut dummy: u32;
            asm!(
                "mulps xmm0, xmm0",
                "movd {res:e}, xmm0",
                res = out(reg) dummy,
                out("xmm0") _,
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
                    // x86_64 (item 7): scalar window array + SSE horizontal sum.
                    //   Functionally identical; register-resident AVX2 path deferred
                    //   (see one_threaded.rs for the reference implementation).
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

                    #[cfg(target_arch = "x86_64")]
                    let decision: u32 = {
                        // Shift window: drop oldest price, insert new price at end.
                        let price = (tick_ptr as *const models::MarketTick as *const f32).read();
                        for i in 0..7 { win_buf[i] = win_buf[i + 1]; }
                        win_buf[7] = price;
                        // Sum via scalar iteration; AVX2 horizontal-add path is
                        // the register-optimised version (see one_threaded.rs).
                        let sum: f32 = win_buf.iter().copied().sum();
                        let threshold = f32::from_bits(momentum_scale); // mean * 1.001
                        (price > sum * threshold) as u32
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
