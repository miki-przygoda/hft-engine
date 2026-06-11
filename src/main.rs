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
mod model;
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
pub(crate) const SIGNAL_SERIES_LEN:  usize = 2048;  // downsampled composite-signal ring

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
        max_size_mult: env_f32("HFT_MAX_SIZE_MULT", rust_hft_software::config::MAX_SIZE_MULT_DEFAULT),
        adaptive:      std::env::var_os("HFT_ADAPTIVE").is_some(),
        use_flow:      std::env::var_os("HFT_USE_FLOW").is_some(),
        capital:       env_f32("HFT_CAPITAL",   rust_hft_software::config::CAPITAL_DEFAULT),
        risk_frac:     env_f32("HFT_RISK_FRAC", rust_hft_software::config::RISK_FRAC_DEFAULT),
        momentum:      std::env::var_os("HFT_MOMENTUM").is_some(),
        w_trend:        env_f32("HFT_W_TREND",   rust_hft_software::config::W_TREND_DEFAULT),
        w_flow:         env_f32("HFT_W_FLOW",    rust_hft_software::config::W_FLOW_DEFAULT),
        w_basket:       env_f32("HFT_W_BASKET",  rust_hft_software::config::W_BASKET_DEFAULT),
        w_leadlag:      env_f32("HFT_W_LEADLAG", rust_hft_software::config::W_LEADLAG_DEFAULT),
        signal_thr_bps: env_f32("HFT_SIGNAL_THR_BPS", rust_hft_software::config::SIGNAL_THR_BPS_DEFAULT),
        pullback_bps:   env_f32("HFT_PULLBACK_BPS",   rust_hft_software::config::PULLBACK_BPS_DEFAULT),
        trail_bps:      env_f32("HFT_TRAIL_BPS",      rust_hft_software::config::TRAIL_BPS_DEFAULT),
        signal_exit_bps: env_f32("HFT_SIGNAL_EXIT_BPS", rust_hft_software::config::SIGNAL_EXIT_BPS_DEFAULT),
        beta:           env_f32("HFT_BETA",      rust_hft_software::config::BETA_DEFAULT),
        maker:          std::env::var_os("HFT_MAKER").is_some(),
        maker_bps:      env_f32("HFT_MAKER_BPS",  rust_hft_software::config::MAKER_BPS_DEFAULT),
        fee_gate:       std::env::var_os("HFT_FEE_GATE").is_some(),
        min_edge_bps:   env_f32("HFT_MIN_EDGE_BPS", rust_hft_software::config::MIN_EDGE_BPS_DEFAULT),
        normalize:      std::env::var_os("HFT_NORMALIZE").is_some(),
        slippage_bps:   env_f32("HFT_SLIPPAGE_BPS", rust_hft_software::config::SLIPPAGE_BPS_DEFAULT),
        funding_bps_per_hr: env_f32("HFT_FUNDING_BPS_PER_HR", rust_hft_software::config::FUNDING_BPS_PER_HR_DEFAULT),
        vol_target_bps:    env_f32("HFT_VOL_TARGET_BPS",    rust_hft_software::config::VOL_TARGET_BPS_DEFAULT),
        max_exposure_mult: env_f32("HFT_MAX_EXPOSURE_MULT", rust_hft_software::config::MAX_EXPOSURE_MULT_DEFAULT),
    };

    if trade_cfg.enabled && trade_cfg.momentum {
        println!("[engine] TRADING model: {} TREND-FOLLOWING + cross-market signal  fee {:.1}bps/side  {:.0}x lev",
            if trade_cfg.allow_short { "long&short" } else { "long-only" },
            trade_cfg.fee_bps, trade_cfg.leverage);
        println!("[engine] S = {:.1}·own_trend + {:.1}·flow + {:.1}·basket + {:.1}·leadlag   gate {:.1}bps  pullback {:.1}bps  trail {:.1}bps",
            trade_cfg.w_trend, trade_cfg.w_flow, trade_cfg.w_basket, trade_cfg.w_leadlag,
            trade_cfg.signal_thr_bps, trade_cfg.pullback_bps, trade_cfg.trail_bps);
    } else if trade_cfg.enabled {
        let rule = if trade_cfg.adaptive { "ADAPTIVE (1σ/1.5σ/2.5σ)".to_string() }
            else { format!("entry {:.1} / TP {:.1} / SL {:.1} bps",
                trade_cfg.entry_dip_bps, trade_cfg.tp_bps, trade_cfg.sl_bps) };
        println!("[engine] TRADING model: {} mean-reversion, {}{}  fee {:.1}bps/side  {:.0}x lev",
            if trade_cfg.allow_short { "long&short" } else { "long-only" },
            rule, if trade_cfg.use_flow { " +order-flow" } else { "" },
            trade_cfg.fee_bps, trade_cfg.leverage);
        println!("[engine] capital {:.2}  risk/trade {:.0}%  → notional/trade ≈ {:.2} at {:.0}x",
            trade_cfg.capital, trade_cfg.risk_frac * 100.0,
            trade_cfg.capital * trade_cfg.risk_frac * trade_cfg.leverage, trade_cfg.leverage);
    } else if buy_on_downtick {
        println!("[engine] downtick mode: buy on any price decrease");
    } else if target_dip_bps > 0.0 {
        println!("[engine] dip mode: buy on a {target_dip_bps} bps dip below the rolling reference");
    } else if target_price > 0.0 {
        println!("[engine] target-price mode: buy when price ≤ {target_price}");
    }

    // Perpetual cost stack (SP2–SP5): only echo the knobs that are actually engaged,
    // so a plain run stays quiet but a cost-aware run is self-documenting.
    if trade_cfg.slippage_bps != 0.0 || trade_cfg.funding_bps_per_hr != 0.0
        || trade_cfg.vol_target_bps != 0.0 || trade_cfg.max_exposure_mult != 0.0 {
        let mut parts: Vec<String> = Vec::new();
        if trade_cfg.slippage_bps != 0.0 {
            parts.push(format!("slippage {:.2}bps", trade_cfg.slippage_bps));
        }
        if trade_cfg.funding_bps_per_hr != 0.0 {
            parts.push(format!("funding {:.2}bps/hr", trade_cfg.funding_bps_per_hr));
        }
        if trade_cfg.vol_target_bps != 0.0 {
            parts.push(format!("vol-target {:.1}bps (size↓ as σ↑)", trade_cfg.vol_target_bps));
        }
        if trade_cfg.max_exposure_mult != 0.0 {
            parts.push(format!("exposure ≤ {:.1}×equity", trade_cfg.max_exposure_mult));
        }
        println!("[engine] cost stack: {}", parts.join("  "));
    }

    let args: Vec<String> = std::env::args().collect();

    // Train a learned policy (CEM) over a capture and write it to HFT_MODEL, then
    // exit — no threads or sockets. (Checked before loading, since HFT_MODEL is
    // the OUTPUT path here and need not exist yet.)
    if let Some(i) = args.iter().position(|a| a == "--train") {
        let path = args.get(i + 1).cloned().unwrap_or_else(|| "recordings/two.krkr".to_string());
        engine::run_train(&path, trade_cfg);
        return;
    }

    // Optional learned policy: if HFT_MODEL points at a weights file, load it and
    // let the tiny MLP supply the signal in place of the hand-weighted composite.
    let policy = std::env::var("HFT_MODEL").ok().and_then(|path| {
        match std::fs::read(&path) {
            Ok(bytes) => match model::Policy::from_le_bytes(&bytes) {
                Some(p) => { println!("[model] loaded learned policy from {path}"); Some(p) }
                None => { eprintln!("[model] {path}: too small for a policy; ignoring"); None }
            },
            Err(e) => { eprintln!("[model] could not read {path}: {e}; using hand-weighted signal"); None }
        }
    });

    // Backtest mode: run the model over a recorded capture offline and exit. With a
    // learned policy loaded (HFT_MODEL) → evaluate THAT policy over the whole
    // capture (run_eval); otherwise sweep the hand-weighted config grid.
    if let Some(i) = args.iter().position(|a| a == "--backtest") {
        let path = args.get(i + 1).cloned().unwrap_or_else(|| "recordings/two.krkr".to_string());
        match policy {
            Some(p) => engine::run_eval(&path, trade_cfg, p),
            None    => engine::run_backtest(&path, trade_cfg),
        }
        return;
    }

    // Two ring buffers sharing one clock origin: slot 0 = the traded instrument
    // (drives the hot path, order ring, exchange, trade log), slot 1 = the
    // cross-market reference (read-only by the strategy for the basket/lead-lag
    // signal). The dormant InstrumentBuffers scaffold generalizes this to N.
    let start = Instant::now();
    let mk_buffer = || models::RingBuffer {
        ticks:      unsafe { std::mem::zeroed() },
        latest_idx: AtomicU64::new(0),
        start_time: start,
    };
    let traded    = Arc::new(mk_buffer());
    let reference = Arc::new(mk_buffer());

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
        spread_lo_bits: AtomicU32::new(f32::INFINITY.to_bits()),
        spread_hi_bits: AtomicU32::new(f32::NEG_INFINITY.to_bits()),
        funding_bits:   AtomicU32::new(0f32.to_bits()),
        funding_quote_bits: AtomicU64::new(0f64.to_bits()),
        target_price,
        target_dip_bps,
        buy_on_downtick,
        trade_cfg,
        round_trips:   models::RoundTripLog::new(),
        vol_ema_bits:  AtomicU32::new(0),
        latest_signal_bits: AtomicU32::new(0),
        signal:        models::SignalLog::new(),
    });

    let order_ring = Arc::new(models::OrderRing::new());

    // Pre-touch every cache line of every shared buffer before spawning threads.
    // std::mem::zeroed() allocates pages lazily on macOS (zero-fill-on-demand);
    // write_volatile forces physical commitment, eliminating page-fault spikes
    // during trading. Each struct is one cache line (64 bytes), so one write
    // per entry covers both page commitment and cache-line warming.
    unsafe {
        for buf in [&traded, &reference] {
            let ticks = buf.ticks.as_ptr() as *mut u64;
            for i in (0..BUFFER_SIZE * 8).step_by(8) {
                std::ptr::write_volatile(ticks.add(i), 0);
            }
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
        let buf = Arc::clone(&traded);
        let lpn = Arc::clone(&last_packet_ns);
        move || engine::run_watchdog(ob, buf, lpn)
    });

    thread::spawn({
        let ring = Arc::clone(&order_ring);
        let buf  = Arc::clone(&traded);
        let ob   = Arc::clone(&order_book);
        let rdy  = Arc::clone(&exchange_ready);
        move || {
            engine::set_qos_interactive();
            engine::set_thread_affinity_tag(3); // exchange → core 4
            engine::run_in_process_exchange(ring, buf, ob, rdy);
        }
    });

    thread::spawn({
        let buf = Arc::clone(&traded);
        let rbuf = Arc::clone(&reference);
        let ob  = Arc::clone(&order_book);
        let lpn = Arc::clone(&last_packet_ns);
        let rdy = Arc::clone(&ingestor_ready);
        move || {
            engine::set_qos_interactive();
            engine::set_thread_affinity_tag(2); // ingestor → core 3
            engine::run_ingestor(buf, rbuf, ob, lpn, rdy);
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
        let buf  = Arc::clone(&traded);
        let rbuf = Arc::clone(&reference);
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
            unsafe { engine::trading_strategy(&buf, &rbuf, &ob, &ring, policy); }
        }
    });

    strategy.join().expect("trading strategy thread panicked");
}
