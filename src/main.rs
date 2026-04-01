mod models;
#[cfg(feature = "testing")]
mod testing_scripts;

use std::arch::asm;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::net::UdpSocket;

// Must match WARMUP_PACKETS in market-simulator.rs.
const WARMUP_PACKETS: u64 = 10;

const EXCHANGE_ADDR:    &str = "127.0.0.1:34255";
const CONFIRM_ADDR:     &str = "0.0.0.0:34256";
// Heartbeat interval: keeps the exchange process and kernel networking path
// warm between real orders so neither side pays an OS wakeup penalty.
const HEARTBEAT_US: u64 = 1_000; // 1 ms

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

    // UDP socket the strategy uses to submit orders to the exchange.
    // Non-blocking so a missing exchange never stalls the hot path.
    let order_socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind order socket");
    order_socket.set_nonblocking(true).expect("Failed to set non-blocking on order socket");

    // UDP socket the confirm receiver listens on for exchange acknowledgements.
    // Non-blocking: the receiver spin-polls so confirmations are detected in
    // nanoseconds rather than waiting for an OS wakeup event.
    let confirm_socket = UdpSocket::bind(CONFIRM_ADDR).expect("Failed to bind confirm socket");
    confirm_socket.set_nonblocking(true).expect("Failed to set non-blocking on confirm socket");

    // Heartbeat socket — separate from order_socket so the hot order path is
    // never delayed by heartbeat sends.
    let heartbeat_socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind heartbeat socket");

    let ingestor_buffer  = Arc::clone(&buffer);
    let confirm_buffer   = Arc::clone(&buffer);
    let confirm_ob       = Arc::clone(&order_book);
    let stats_order_book = Arc::clone(&order_book);

    // Stats monitor — wakes after all packets have been processed and all
    // confirmations have had time to arrive, then prints both latency tables.
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(5));

        let count  = stats_order_book.trade_log.write_idx.load(Ordering::Acquire) as usize;
        let count  = count.min(models::TRADE_LOG_SIZE);
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
                println!("Round trip     — no confirmations received (is fake-exchange running?)");
            }
        }

        std::process::exit(0);
    });

    // Heartbeat thread — sends a sub-24-byte packet to the exchange every
    // HEARTBEAT_US microseconds. The exchange discards these but the kernel
    // networking path (socket buffers, loopback routing, process wakeup
    // infrastructure) stays warm, so real orders find zero cold-start overhead.
    thread::spawn(move || {
        let hb = [0u8; 1]; // 1-byte packet — exchange identifies as heartbeat by size
        loop {
            let _ = heartbeat_socket.send_to(&hb, EXCHANGE_ADDR);
            thread::sleep(std::time::Duration::from_micros(HEARTBEAT_US));
        }
    });

    // Confirmation receiver — spin-polls the confirm socket so real order acks
    // are detected in nanoseconds rather than paying an OS wakeup cost.
    thread::spawn(move || {
        run_confirm_receiver(confirm_socket, confirm_buffer, confirm_ob);
    });

    thread::spawn(move || {
        unsafe { pthread_set_qos_class_self_np(0x21, 0); }
        run_ingestor(ingestor_buffer);
    });

    unsafe {
        pthread_set_qos_class_self_np(0x21, 0);
        trading_strategy(&buffer, &order_book, &order_socket);
    }
}

fn run_ingestor(buffer: Arc<models::RingBuffer>) {
    let socket = UdpSocket::bind("127.0.0.1:34254").expect("Failed to bind socket");
    socket.set_nonblocking(true).expect("Failed to set non-blocking");

    let mut seq = 1u64;
    let mut packet_buffer = [0u8; 64];

    loop {
        match socket.recv_from(&mut packet_buffer) {
            Ok((amt, _src)) => {
                if amt >= 16 {
                    let idx = (seq & (models::BUFFER_SIZE as u64 - 1)) as usize;
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

// Receives exchange confirmations and writes round_trip_ns into the matching
// trade log slot.  The slot index is carried in the echoed order packet so no
// scanning is required.
//
// Memory ordering note: the strategy commits the slot with fetch_add(Release)
// before sending the order packet.  The confirmation always arrives after the
// send, so the slot is guaranteed to be fully written before we touch it here.
// The stats thread reads 5 s later — far after all confirmations arrive — so
// the round_trip_ns writes are visible without an additional barrier.
// Spin-polls the confirm socket. Because the socket is non-blocking, this
// thread never sleeps — it detects incoming confirmations on the very next
// recv attempt (~sub-µs) rather than waiting for the OS to schedule a wakeup.
// The trade-off is 100% CPU on this thread, which is acceptable for a
// latency-critical confirmation path.
fn run_confirm_receiver(
    socket: UdpSocket,
    buffer: Arc<models::RingBuffer>,
    order_book: Arc<models::OrderBook>,
) {
    let mut buf = [0u8; 32];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((amt, _)) if amt >= 24 => {
                let confirm_recv_ns = buffer.start_time.elapsed().as_nanos() as u64;
                let slot = u64::from_le_bytes(buf[8..16].try_into().unwrap()) as usize
                    % models::TRADE_LOG_SIZE;
                let order_send_ns = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                let round_trip_ns = confirm_recv_ns.saturating_sub(order_send_ns);
                unsafe {
                    (*order_book.trade_log.entries.get())[slot].round_trip_ns = round_trip_ns;
                }
            }
            Ok(_) => {
                // Packet too small — not a real confirm, ignore.
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::hint::spin_loop();
            }
            Err(_) => {}
        }
    }
}

#[inline(always)]
unsafe fn trading_strategy(
    buffer: &models::RingBuffer,
    order_book: &models::OrderBook,
    order_socket: &UdpSocket,
) {
    unsafe {
        let mut last_processed_seq = 0u64;

        for _ in 0..10_000 {
            let mut _dummy: u64;
            asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0", res = out(reg) _dummy);
            let _ = buffer.start_time.elapsed();
        }

        // Force OS page commitment for all trade_log entry pages.
        // TradeExecution is now 48 bytes; step of 64 entries = 3072 bytes < 4096,
        // so we touch at least once per page across the full 49152-byte array.
        {
            let entries = &mut *order_book.trade_log.entries.get();
            let mut i = 0;
            while i < models::TRADE_LOG_SIZE {
                std::ptr::write_volatile(&mut entries[i].sequence as *mut u64, 0);
                i += 64;
            }
        }

        loop {
            let current_seq = buffer.latest_idx.load(Ordering::Acquire);

            if current_seq > last_processed_seq {
                let idx      = (current_seq & (models::BUFFER_SIZE as u64 - 1)) as usize;
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
                            % models::TRADE_LOG_SIZE;
                        let entry = &mut (*order_book.trade_log.entries.get())[slot];

                        // Capture order_send_ns as late as possible — right before the
                        // fetch_add so it reflects the true submission moment.
                        let order_send_ns = buffer.start_time.elapsed().as_nanos() as u64;
                        entry.sequence       = current_seq;
                        entry.ingest_time_ns = ingest_time_ns;
                        entry.buy_time_ns    = buy_time_ns;
                        entry.latency_ns     = latency_ns;
                        entry.order_send_ns  = order_send_ns;
                        entry.round_trip_ns  = 0; // filled in by run_confirm_receiver
                        order_book.trade_log.write_idx.fetch_add(1, Ordering::Release);

                        // Order packet: sequence (8) | slot (8) | order_send_ns (8)
                        // The slot lets the confirm receiver update the right entry
                        // directly without scanning.
                        let mut pkt = [0u8; 24];
                        pkt[0..8].copy_from_slice(&current_seq.to_le_bytes());
                        pkt[8..16].copy_from_slice(&(slot as u64).to_le_bytes());
                        pkt[16..24].copy_from_slice(&order_send_ns.to_le_bytes());
                        let _ = order_socket.send_to(&pkt, EXCHANGE_ADDR);
                    }

                    order_book.buy_count.fetch_add(1, Ordering::Release);
                }

                last_processed_seq = current_seq;
            } else {
                std::hint::spin_loop();
                let next_entry_ptr = (*order_book.trade_log.entries.get())
                    .as_ptr()
                    .add(order_book.trade_log.write_idx.load(Ordering::Relaxed) as usize
                        % models::TRADE_LOG_SIZE);
                asm!(
                    "prfm pstl1keep, [{entry}]",
                    entry = in(reg) next_entry_ptr,
                    options(nostack, nomem)
                );
            }
        }
    }
}
