mod engine;
mod models;
#[cfg(feature = "testing")]
mod testing_scripts;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use models::LatencyHistogram;

pub(crate) const BUFFER_SIZE:     usize = 1024;
pub(crate) const TRADE_LOG_SIZE:  usize = 1024;
pub(crate) const ORDER_RING_SIZE: usize = 1024;

fn main() {
    println!("[engine] starting — running full simulation in-process");

    // Memory snapshot [1]: very start, before any buffer allocation.
    let mem_start = engine::collect_memory_stats();

    let buffer = Arc::new(models::RingBuffer {
        ticks:      unsafe { std::mem::zeroed() },
        latest_idx: AtomicU64::new(0),
        start_time: Instant::now(),
    });

    let order_book = Arc::new(models::OrderBook {
        trade_log:     models::TradeLog::new(),
        sig_hist:      LatencyHistogram::new(),
        rt_hist:       LatencyHistogram::new(),
        stall_count:   AtomicU64::new(0),
        gap_count:     AtomicU64::new(0),
        dirty:         AtomicBool::new(false),
        halt:          AtomicBool::new(false),
        net_position:  AtomicI64::new(0),
        mem_total_ram: AtomicU64::new(mem_start.total_ram),
        mem_rss_start: AtomicU64::new(mem_start.peak_rss),
        mem_rss_ready: AtomicU64::new(0),  // filled after pre-touch below
    });

    let order_ring = Arc::new(models::OrderRing::new());

    // Pre-touch every cache line of every shared buffer before spawning threads.
    // std::mem::zeroed() allocates pages lazily on macOS (zero-fill-on-demand);
    // write_volatile forces physical commitment, eliminating page-fault spikes
    // during trading. Each struct is one cache line (64 bytes), so one write
    // per entry covers both page commitment and cache-line warming.
    unsafe {
        let ticks = buffer.ticks.as_ptr() as *mut u64;
        for i in (0..BUFFER_SIZE * 8).step_by(8) {
            std::ptr::write_volatile(ticks.add(i), 0);
        }
        let ring = (*order_ring.entries.get()).as_ptr() as *mut u64;
        for i in (0..ORDER_RING_SIZE * 8).step_by(8) {
            std::ptr::write_volatile(ring.add(i), 0);
        }
        let log = (*order_book.trade_log.entries.get()).as_ptr() as *mut u64;
        for i in (0..TRADE_LOG_SIZE * 6).step_by(6) {
            std::ptr::write_volatile(log.add(i), 0);
        }
    }

    // Memory snapshot [2]: after all buffers are pre-touched, before spawning threads.
    let mem_ready = engine::collect_memory_stats();
    order_book.mem_rss_ready.store(mem_ready.peak_rss, Ordering::Relaxed);

    let ingestor_ready = Arc::new(AtomicBool::new(false));
    let exchange_ready = Arc::new(AtomicBool::new(false));
    let last_packet_ns = Arc::new(AtomicU64::new(0));

    thread::spawn({
        let ob  = Arc::clone(&order_book);
        let buf = Arc::clone(&buffer);
        let lpn = Arc::clone(&last_packet_ns);
        move || engine::run_watchdog(ob, buf, lpn)
    });

    thread::spawn({
        let ring = Arc::clone(&order_ring);
        let buf  = Arc::clone(&buffer);
        let ob   = Arc::clone(&order_book);
        let rdy  = Arc::clone(&exchange_ready);
        move || {
            engine::set_qos_interactive();
            engine::set_thread_affinity_tag(3); // exchange → core 4
            engine::run_in_process_exchange(ring, buf, ob, rdy);
        }
    });

    thread::spawn({
        let buf = Arc::clone(&buffer);
        let ob  = Arc::clone(&order_book);
        let lpn = Arc::clone(&last_packet_ns);
        let rdy = Arc::clone(&ingestor_ready);
        move || {
            engine::set_qos_interactive();
            engine::set_thread_affinity_tag(2); // ingestor → core 3
            engine::run_ingestor(buf, ob, lpn, rdy);
        }
    });

    thread::spawn({
        let ir = Arc::clone(&ingestor_ready);
        move || engine::run_market_simulator(ir)
    });

    let strategy = thread::spawn({
        let buf  = Arc::clone(&buffer);
        let ob   = Arc::clone(&order_book);
        let ring = Arc::clone(&order_ring);
        let ir   = Arc::clone(&ingestor_ready);
        let er   = Arc::clone(&exchange_ready);
        move || {
            while !ir.load(Ordering::Acquire) || !er.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            println!("[engine] all systems ready — entering trading loop");
            engine::set_qos_interactive();
            // Affinity tag 1: hint the scheduler to keep the strategy thread on
            // the same P-core cluster throughout the session (item 6).
            engine::set_thread_affinity_tag(1);
            unsafe { engine::trading_strategy(&buf, &ob, &ring); }
        }
    });

    strategy.join().expect("trading strategy thread panicked");
}
