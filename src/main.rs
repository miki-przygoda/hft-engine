//! `trading-engine` — the self-contained in-process HFT simulation.
//!
//! This binary's `main` is the orchestrator: it allocates the three lock-free
//! shared buffers (`RingBuffer`, `OrderRing`, `OrderBook`/`TradeLog`),
//! **pre-touches** every cache line so the hot path never takes a page fault,
//! then spawns the five threads (watchdog, exchange, ingestor, simulator, and
//! the strategy on the main thread) and joins the strategy.
//!
//! See `CLAUDE.md` for the full architecture and the list of invariants this
//! startup sequence depends on.

mod engine;
mod models;
#[cfg(feature = "testing")]
mod testing_scripts;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use models::LatencyHistogram;

pub(crate) const BUFFER_SIZE:        usize = 1024;
pub(crate) const TRADE_LOG_SIZE:     usize = 1024;
pub(crate) const ORDER_RING_SIZE:    usize = 1024;
pub(crate) const ROUND_TRIP_LOG_SIZE: usize = 4096;

/// Parse an env var as an f32, falling back to `default` when unset/invalid.
fn env_f32(key: &str, default: f32) -> f32 {
    std::env::var(key).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(default)
}

fn main() {
    println!("[engine] starting — running full simulation in-process");

    // Memory snapshot [1]: very start, before any buffer allocation.
    let mem_start = engine::collect_memory_stats();

    // Optional target-price buy level. When HFT_TARGET_PRICE is set, the strategy
    // buys at market each time the price dips to/through the target and measures
    // the slippage vs the target caused by the latency gap. Unset / 0 → breakout.
    let target_price: f32 = std::env::var("HFT_TARGET_PRICE")
        .ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0.0);
    // Relative-dip mode: buy on any dip of this many bps below a rolling reference.
    // Adapts to any price level (no need to know the market price up front).
    let target_dip_bps: f32 = std::env::var("HFT_TARGET_DIP_BPS")
        .ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0.0);
    // Downtick mode: buy on any price decrease. Fires on any feed that moves at all.
    let buy_on_downtick = std::env::var_os("HFT_DOWNTICK").is_some();

    // Trading model: long & short mean-reversion with TP/SL + opposite-signal exits,
    // fees, leverage, and a P&L scorecard. Enabled by HFT_TRADE=1.
    let trade_cfg = models::TradeCfg {
        enabled:       std::env::var_os("HFT_TRADE").is_some(),
        allow_short:   std::env::var_os("HFT_NO_SHORT").is_none(),
        entry_dip_bps: env_f32("HFT_ENTRY_BPS", rust_hft_software::config::ENTRY_DIP_BPS_DEFAULT),
        tp_bps:        env_f32("HFT_TP_BPS",     rust_hft_software::config::TP_BPS_DEFAULT),
        sl_bps:        env_f32("HFT_SL_BPS",     rust_hft_software::config::SL_BPS_DEFAULT),
        fee_bps:       env_f32("HFT_FEE_BPS",    rust_hft_software::config::FEE_BPS_DEFAULT),
        leverage:      env_f32("HFT_LEVERAGE",   rust_hft_software::config::LEVERAGE_DEFAULT),
        base_size:     env_f32("HFT_BASE_SIZE",  rust_hft_software::config::BASE_SIZE_DEFAULT),
        max_size_mult: env_f32("HFT_MAX_SIZE_MULT", rust_hft_software::config::MAX_SIZE_MULT_DEFAULT),
    };

    if trade_cfg.enabled {
        println!("[engine] TRADING model: {} mean-reversion  entry {:.1}bps  TP {:.1}bps  SL {:.1}bps  fee {:.1}bps/side  lev {:.0}x",
            if trade_cfg.allow_short { "long&short" } else { "long-only" },
            trade_cfg.entry_dip_bps, trade_cfg.tp_bps, trade_cfg.sl_bps, trade_cfg.fee_bps, trade_cfg.leverage);
    } else if buy_on_downtick {
        println!("[engine] downtick mode: buy on any price decrease");
    } else if target_dip_bps > 0.0 {
        println!("[engine] dip mode: buy on a {target_dip_bps} bps dip below the rolling reference");
    } else if target_price > 0.0 {
        println!("[engine] target-price mode: buy when price ≤ {target_price}");
    }

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
        attempts:      AtomicU64::new(0),
        filled:        AtomicU64::new(0),
        price_lo_bits: AtomicU32::new(f32::INFINITY.to_bits()),
        price_hi_bits: AtomicU32::new(f32::NEG_INFINITY.to_bits()),
        target_price,
        target_dip_bps,
        buy_on_downtick,
        trade_cfg,
        round_trips:   models::RoundTripLog::new(),
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
        for i in (0..TRADE_LOG_SIZE * 8).step_by(8) {  // TradeExecution = 64 bytes / 8 × u64 (invariant #10)
            std::ptr::write_volatile(log.add(i), 0);
        }
        let rt = (*order_book.round_trips.entries.get()).as_ptr() as *mut u64;
        for i in (0..ROUND_TRIP_LOG_SIZE * 8).step_by(8) {  // RoundTrip = 64 bytes / 8 × u64
            std::ptr::write_volatile(rt.add(i), 0);
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

    // Internal burst simulator. Skipped when HFT_EXTERNAL_FEED is set, so a real
    // feed (the kraken-feed adapter, live or replay) can drive the ingestor alone
    // without synthetic ticks mixing in.
    let external_feed = std::env::var_os("HFT_EXTERNAL_FEED").is_some();
    if !external_feed {
        thread::spawn({
            let ir = Arc::clone(&ingestor_ready);
            move || engine::run_market_simulator(ir)
        });
    } else {
        println!("[engine] HFT_EXTERNAL_FEED set — internal simulator disabled, awaiting external feed");
    }

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
