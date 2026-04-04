use std::arch::asm;
use std::hint::black_box;
use std::io::Write;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{models, BUFFER_SIZE, ORDER_RING_SIZE, TRADE_LOG_SIZE};
use rust_hft_software::config::{INGESTOR_ADDR, PACKET_INTERVAL_MS, REAL_PACKETS, WARMUP_PACKETS};

const BUFFER_MASK:     u64   = (BUFFER_SIZE     - 1) as u64;
const TRADE_LOG_MASK:  usize = TRADE_LOG_SIZE   - 1;
const ORDER_RING_MASK: usize = ORDER_RING_SIZE   - 1;

unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}

#[inline(always)]
fn elapsed_ns(start: &std::time::Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

pub(crate) fn set_qos_interactive() {
    unsafe { pthread_set_qos_class_self_np(0x21, 0); }
}

pub(crate) fn run_ingestor(
    buffer: Arc<models::RingBuffer>,
    last_packet_ns: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
) {
    let socket = UdpSocket::bind(INGESTOR_ADDR).expect("ingestor: failed to bind");
    socket.set_nonblocking(true).expect("ingestor: failed to set non-blocking");
    ready.store(true, Ordering::Release);

    let mut seq = 1u64;
    let mut pkt = [0u8; 64];

    loop {
        match socket.recv_from(&mut pkt) {
            Ok((amt, _)) if amt >= 16 => {
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
            unsafe {
                (*order_book.trade_log.entries.get())[slot].round_trip_ns =
                    confirm_recv_ns.saturating_sub(order_send_ns);
            }
            local_read_idx += 1;
        } else {
            std::hint::spin_loop();
        }
    }
}

pub(crate) fn run_watchdog(
    order_book: Arc<models::OrderBook>,
    buffer: Arc<models::RingBuffer>,
    last_packet_ns: Arc<AtomicU64>,
) {
    const IDLE_SHUTDOWN_NS:   u64 = 10_000_000_000;
    const NO_FEED_TIMEOUT_NS: u64 = 30_000_000_000;

    loop {
        thread::sleep(Duration::from_millis(500));

        let now_ns = elapsed_ns(&buffer.start_time);
        let last   = last_packet_ns.load(Ordering::Acquire);

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

        println!("{}", "─".repeat(55));
        println!("Signal latency — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                 sig_sum / count as u64, sig_min, sig_max);
        if rt_count > 0 {
            println!("Round trip     — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                     rt_sum / rt_count as u64, rt_min, rt_max);
        } else {
            println!("Round trip     — no confirmations received");
        }
    }

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

    let mut json = String::with_capacity(4096);
    json.push_str("{\n");
    json.push_str(&format!("  \"version\": \"{}\",\n", version));
    json.push_str(&format!("  \"date\": \"{}\",\n", date));
    json.push_str(&format!("  \"timestamp\": \"{}T{}Z\",\n", date, time));
    json.push_str(&format!("  \"total_trades\": {},\n", count));

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

        json.push_str("  \"signal_latency\": {\n");
        json.push_str(&format!("    \"avg_ns\": {},\n", sig_sum / count as u64));
        json.push_str(&format!("    \"min_ns\": {},\n", sig_min));
        json.push_str(&format!("    \"max_ns\": {}\n", sig_max));
        json.push_str("  },\n");

        json.push_str("  \"round_trip\": {\n");
        if rt_count > 0 {
            json.push_str(&format!("    \"avg_ns\": {},\n", rt_sum / rt_count as u64));
            json.push_str(&format!("    \"min_ns\": {},\n", rt_min));
            json.push_str(&format!("    \"max_ns\": {}\n", rt_max));
        } else {
            json.push_str("    \"avg_ns\": null,\n");
            json.push_str("    \"min_ns\": null,\n");
            json.push_str("    \"max_ns\": null\n");
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
    json.push_str("  ]\n");
    json.push_str("}\n");

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

    let z       = days_since_epoch + 719468;
    let era      = z / 146097;
    let doe      = z - era * 146097;
    let yoe      = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y        = yoe + era * 400;
    let doy      = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp       = (5 * doy + 2) / 153;
    let d        = doy - (153 * mp + 2) / 5 + 1;
    let month    = if mp < 10 { mp + 3 } else { mp - 9 };
    let year     = if month <= 2 { y + 1 } else { y };

    (
        format!("{:04}-{:02}-{:02}", year, month, d),
        format!("{:02}-{:02}-{:02}", h, m, s),
    )
}

pub(crate) fn run_market_simulator(ingestor_ready: Arc<AtomicBool>) {
    while !ingestor_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    let socket = UdpSocket::bind("0.0.0.0:0").expect("simulator: failed to bind");

    let mut pkt = [0u8; 16];
    pkt[0..4].copy_from_slice(&100.5_f32.to_le_bytes());
    pkt[4..8].copy_from_slice(&1000.0_f32.to_le_bytes());

    for seq in 1..=WARMUP_PACKETS {
        pkt[8..16].copy_from_slice(&seq.to_le_bytes());
        socket.send_to(&pkt, INGESTOR_ADDR).expect("simulator: send failed");
    }

    thread::sleep(Duration::from_millis(50));

    for i in 0..REAL_PACKETS {
        let seq = WARMUP_PACKETS + 1 + i;
        pkt[8..16].copy_from_slice(&seq.to_le_bytes());
        socket.send_to(&pkt, INGESTOR_ADDR).expect("simulator: send failed");
        thread::sleep(Duration::from_millis(PACKET_INTERVAL_MS));
    }
}

#[inline(always)]
pub(crate) unsafe fn trading_strategy(
    buffer: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_ring: &models::OrderRing,
) {
    unsafe {
        let mut last_processed_seq = 0u64;

        // NEON warmup: black_box both outputs so the compiler cannot eliminate
        // the loop as dead code, ensuring vector units, icache, and the
        // start_ticks cache line are genuinely warmed.
        for _ in 0..10_000 {
            let mut dummy: u64;
            asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0",
                 res = out(reg) dummy, options(nostack, nomem));
            black_box(dummy);
            black_box(elapsed_ns(&buffer.start_time));
        }

        loop {
            let current_seq = buffer.latest_idx.load(Ordering::Acquire);

            if current_seq > last_processed_seq {
                let idx      = (current_seq & BUFFER_MASK) as usize;
                let tick_ptr = &buffer.ticks[idx];

                let decision: u64;
                asm!(
                    "ld1 {{v0.4s}}, [{ptr}]",
                    "fmul v1.4s, v0.4s, v0.4s",
                    "fmov {res:w}, s1",
                    "and {res:w}, {res:w}, #1",
                    ptr = in(reg) tick_ptr,
                    res = out(reg) decision,
                    options(nostack, nomem)
                );

                // black_box prevents the compiler from using the provably
                // 0-or-1 range of `decision` to transform the branch.
                // Branchless OR: evaluates both conditions without short-circuit.
                let trigger = (black_box(decision) | (current_seq & 1)) != 0;

                if trigger {
                    let buy_time_ns    = elapsed_ns(&buffer.start_time);
                    let ingest_time_ns = *(&tick_ptr.timestamp as *const u64);
                    let latency_ns     = buy_time_ns.saturating_sub(ingest_time_ns);

                    if current_seq > WARMUP_PACKETS {
                        let slot  = order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize
                            & TRADE_LOG_MASK;
                        let entry = &mut (*order_book.trade_log.entries.get())[slot];

                        let order_send_ns = elapsed_ns(&buffer.start_time);
                        entry.sequence       = current_seq;
                        entry.ingest_time_ns = ingest_time_ns;
                        entry.buy_time_ns    = buy_time_ns;
                        entry.latency_ns     = latency_ns;
                        entry.order_send_ns  = order_send_ns;
                        entry.round_trip_ns  = 0;
                        order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

                        let ring_slot = order_ring.write_idx.load(Ordering::Relaxed) as usize
                            & ORDER_RING_MASK;
                        let oe = &mut (*order_ring.entries.get())[ring_slot];
                        oe.sequence      = current_seq;
                        oe.slot          = slot as u64;
                        oe.order_send_ns = order_send_ns;
                        order_ring.write_idx.fetch_add(1, Ordering::Release);
                    }
                }

                last_processed_seq = current_seq;
            } else {
                std::hint::spin_loop();
                let next_entry_ptr = (*order_book.trade_log.entries.get())
                    .as_ptr()
                    .add(order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize
                        & TRADE_LOG_MASK);
                asm!(
                    "prfm pstl1keep, [{entry}]",
                    entry = in(reg) next_entry_ptr,
                    options(nostack, nomem)
                );
            }
        }
    }
}
