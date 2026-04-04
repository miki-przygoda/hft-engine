mod models;
#[cfg(feature = "testing")]
mod testing_scripts;

use std::arch::asm;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::net::UdpSocket;
use std::time::Duration;
use rust_hft_software::config::{
    WARMUP_PACKETS, STATS_MONITOR_TIMEOUT_SECS, INGESTOR_ADDR, BUFFER_SIZE,
    TRADE_LOG_SIZE, ORDER_RING_SIZE
};

unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}

fn main() {
    use std::time::Duration;

    let buffer = Arc::new(models::RingBuffer {
        ticks: unsafe { std::mem::zeroed() },
        latest_idx: AtomicU64::new(0),
        start_time: Instant::now(),
    });

    let order_book = Arc::new(models::OrderBook {
        buy_count: AtomicU64::new(0),
        trade_log: models::TradeLog::new(),
    });

    // Shared order ring: strategy writes, in-process exchange reads.
    // Replaces the UDP socket path entirely — no kernel crossings on the
    // order submission or confirmation path.
    let order_ring = Arc::new(models::OrderRing::new());

    let ingestor_buffer  = Arc::clone(&buffer);
    let exchange_buffer  = Arc::clone(&buffer);
    let exchange_ob      = Arc::clone(&order_book);
    let exchange_ring    = Arc::clone(&order_ring);
    let stats_order_book = Arc::clone(&order_book);
    let strategy_ring    = Arc::clone(&order_ring);

    // Stats monitor — wakes after all packets have been processed and all
    // confirmations have had time to arrive, then prints both latency tables.
    thread::spawn(move || {
        eprintln!("DEBUG: Stats monitor waiting for {} seconds...", STATS_MONITOR_TIMEOUT_SECS);
        thread::sleep(Duration::from_secs(STATS_MONITOR_TIMEOUT_SECS));
        eprintln!("DEBUG: Stats monitor woke up, reading trade log...");

        let count  = stats_order_book.trade_log.write_idx.load(Ordering::Acquire) as usize;
        let count  = count.min(TRADE_LOG_SIZE);
        eprintln!("DEBUG: Total trades in log: {}", count);

        let trades = unsafe { &*stats_order_book.trade_log.entries.get() };

        println!("Total trades executed: {}\n", count);
        println!("{:<12} {:<20} {:<20}", "Sequence", "Sig Latency (ns)", "Round Trip (ns)");
        println!("{}", "─".repeat(55));

        for i in 0..count {
            let t = &trades[i];
            let rt = if t.round_trip_ns > 0 {
                format!("{}", t.round_trip_ns)
            } else {
                "—".to_string()
            };
            println!("{:<12} {:<20} {:<20}", t.sequence, t.latency_ns, rt);
        }

        if count > 0 {
            let sig_total: u64 = trades[..count].iter().map(|t| t.latency_ns).sum();
            let sig_avg = sig_total / count as u64;
            let sig_min = trades[..count].iter().map(|t| t.latency_ns).min().unwrap_or(0);
            let sig_max = trades[..count].iter().map(|t| t.latency_ns).max().unwrap_or(0);

            let rt_trades: Vec<_> = trades[..count].iter()
                .filter(|t| t.round_trip_ns > 0)
                .collect();
            let rt_avg = if rt_trades.is_empty() { 0 }
                else { rt_trades.iter().map(|t| t.round_trip_ns).sum::<u64>() / rt_trades.len() as u64 };
            let rt_min = rt_trades.iter().map(|t| t.round_trip_ns).min().unwrap_or(0);
            let rt_max = rt_trades.iter().map(|t| t.round_trip_ns).max().unwrap_or(0);

            println!("{}", "─".repeat(55));
            println!("Signal latency — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                     sig_avg, sig_min, sig_max);
            if !rt_trades.is_empty() {
                println!("Round trip     — Avg: {:>7} ns  Min: {:>7} ns  Max: {:>7} ns",
                         rt_avg, rt_min, rt_max);
            } else {
                println!("Round trip     — no confirmations received");
            }
        }

        let _ = io::stdout().flush();
        eprintln!("DEBUG: About to exit(0)");
        std::process::exit(0);
    });

    // In-process exchange thread — replaces the external fake-exchange UDP process.
    // Spin-polls the shared order ring buffer. When the strategy commits an order
    // entry (write_idx Release), this thread detects it immediately via Acquire
    // load, reads order_send_ns + slot, captures confirm_recv_ns, and writes
    // round_trip_ns directly into the trade log.  Zero kernel boundary crossings.
    thread::spawn(move || {
        unsafe { pthread_set_qos_class_self_np(0x21, 0); }
        run_in_process_exchange(exchange_ring, exchange_buffer, exchange_ob);
    });

    thread::spawn(move || {
        unsafe { pthread_set_qos_class_self_np(0x21, 0); }
        run_ingestor(ingestor_buffer);
    });

    unsafe {
        pthread_set_qos_class_self_np(0x21, 0);
        trading_strategy(&buffer, &order_book, &strategy_ring);
    }
}

fn run_ingestor(buffer: Arc<models::RingBuffer>) {
    let socket = UdpSocket::bind(INGESTOR_ADDR).expect("Failed to bind socket");
    socket.set_nonblocking(true).expect("Failed to set non-blocking");

    let mut seq = 1u64;
    let mut packet_buffer = [0u8; 64];

    loop {
        match socket.recv_from(&mut packet_buffer) {
            Ok((amt, _src)) => {
                if amt >= 16 {
                    let idx = (seq & (BUFFER_SIZE as u64 - 1)) as usize;
                    let ingest_time_ns = buffer.start_time.elapsed().as_nanos() as u64;

                    unsafe {
                        let tick_ptr = &buffer.ticks[idx] as *const _ as *mut u8;
                        std::ptr::copy_nonoverlapping(packet_buffer.as_ptr(), tick_ptr, 16);
                        let timestamp_ptr = tick_ptr.add(16) as *mut u64;
                        *timestamp_ptr = ingest_time_ns;
                    }

                    buffer.latest_idx.store(seq, Ordering::Release);
                    seq += 1;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::hint::spin_loop();
            }
            Err(_) => {
                std::hint::spin_loop();
            }
        }
    }
}

// In-process exchange: spin-polls the order ring written by trading_strategy.
// SPSC consumer: maintains a local read index, spins until write_idx advances,
// then reads the entry and writes round_trip_ns into the trade log.
// Memory ordering: write_idx.load(Acquire) pairs with fetch_add(Release) in
// the strategy — guarantees all OrderEntry fields are visible before we read.
fn run_in_process_exchange(
    order_ring: Arc<models::OrderRing>,
    buffer: Arc<models::RingBuffer>,
    order_book: Arc<models::OrderBook>,
) {
    let mut local_read_idx: u64 = 0;
    loop {
        let write_idx = order_ring.write_idx.load(Ordering::Acquire);
        if write_idx > local_read_idx {
            let ring_slot = (local_read_idx as usize) % ORDER_RING_SIZE;
            let (slot, order_send_ns) = unsafe {
                let entry = &(*order_ring.entries.get())[ring_slot];
                (entry.slot as usize % TRADE_LOG_SIZE, entry.order_send_ns)
            };
            let confirm_recv_ns = buffer.start_time.elapsed().as_nanos() as u64;
            let round_trip_ns = confirm_recv_ns.saturating_sub(order_send_ns);
            unsafe {
                (*order_book.trade_log.entries.get())[slot].round_trip_ns = round_trip_ns;
            }
            local_read_idx += 1;
        } else {
            std::hint::spin_loop();
        }
    }
}

#[inline(always)]
unsafe fn trading_strategy(
    buffer: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_ring: &models::OrderRing,
) {
    unsafe {
        let mut last_processed_seq = 0u64;

        for _ in 0..10_000 {
            let mut _dummy: u64;
            asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0", res = out(reg) _dummy);
            let _ = buffer.start_time.elapsed();
        }

        // Force OS page commitment for all trade_log entry pages.
        // TradeExecution is 48 bytes; step of 64 entries = 3072 bytes < 4096,
        // so we touch at least once per page across the full 49152-byte array.
        {
            let entries = &mut *order_book.trade_log.entries.get();
            let mut i = 0;
            while i < TRADE_LOG_SIZE {
                std::ptr::write_volatile(&mut entries[i].sequence as *mut u64, 0);
                i += 64;
            }
        }

        loop {
            let current_seq = buffer.latest_idx.load(Ordering::Acquire);

            if current_seq > last_processed_seq {
                let idx      = (current_seq & (BUFFER_SIZE as u64 - 1)) as usize;
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

                let trigger = (decision == 1) || ((current_seq & 1) == 1);

                if trigger {
                    let buy_time_ns    = buffer.start_time.elapsed().as_nanos() as u64;
                    let ingest_time_ns = *(&tick_ptr.timestamp as *const u64);
                    let latency_ns     = buy_time_ns.saturating_sub(ingest_time_ns);

                    if current_seq > WARMUP_PACKETS {
                        let slot = (order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize)
                            % TRADE_LOG_SIZE;
                        let entry = &mut (*order_book.trade_log.entries.get())[slot];

                        let order_send_ns = buffer.start_time.elapsed().as_nanos() as u64;
                        entry.sequence       = current_seq;
                        entry.ingest_time_ns = ingest_time_ns;
                        entry.buy_time_ns    = buy_time_ns;
                        entry.latency_ns     = latency_ns;
                        entry.order_send_ns  = order_send_ns;
                        entry.round_trip_ns  = 0; // filled in by run_in_process_exchange
                        order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

                        // Submit order to in-process exchange via shared ring buffer.
                        // No syscall — exchange thread spin-polls write_idx.
                        let ring_slot = (order_ring.write_idx.load(Ordering::Relaxed) as usize)
                            % ORDER_RING_SIZE;
                        let order_entry = &mut (*order_ring.entries.get())[ring_slot];
                        order_entry.sequence      = current_seq;
                        order_entry.slot          = slot as u64;
                        order_entry.order_send_ns = order_send_ns;
                        order_ring.write_idx.fetch_add(1, Ordering::Release);
                    }

                    order_book.buy_count.fetch_add(1, Ordering::Release);
                }

                last_processed_seq = current_seq;
            } else {
                std::hint::spin_loop();
                let next_entry_ptr = (*order_book.trade_log.entries.get())
                    .as_ptr()
                    .add(order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize
                        % TRADE_LOG_SIZE);
                asm!(
                    "prfm pstl1keep, [{entry}]",
                    entry = in(reg) next_entry_ptr,
                    options(nostack, nomem)
                );
            }
        }
    }
}
