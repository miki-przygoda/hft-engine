//! Core data structures shared across the engine's threads.
//!
//! Every cross-thread type here is `#[repr(C, align(64))]` and built around a
//! lock-free protocol — single-producer/single-consumer (SPSC) ring buffers or
//! single-writer logs — using `AtomicU64` cursors with `Acquire`/`Release`
//! ordering. There are no mutexes. The cache-line layout (notably the
//! `latest_idx` / `start_time` co-location in [`RingBuffer`]) and the
//! single-writer rules are load-bearing; see the invariants in `CLAUDE.md`
//! before changing field order or sizes.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use crate::{BUFFER_SIZE, ORDER_RING_SIZE, ROUND_TRIP_LOG_SIZE, SIGNAL_SERIES_LEN, TRADE_LOG_SIZE};
use rust_hft_software::config::MAX_INSTRUMENTS;

/// A single market data tick — exactly one 64-byte cache line.
///
/// One tick per cache line means no false sharing and no partial-line loads.
/// `timestamp` (ns since engine start) is written by the ingestor *before* it
/// publishes the tick via [`RingBuffer::latest_idx`], so the strategy sees a
/// consistent value after the matching acquire load. Must stay 64 bytes
/// (invariant #1) — adjust `_unused` if fields change.
///
/// `origin_ts_ns` and `transit_est_ns` carry live-feed metadata from the v2
/// packet (see `config::INGEST_PACKET_SIZE_V2`). They sit in what used to be
/// padding, so the struct is still exactly 64 bytes and the first 16 bytes are
/// still price/volume/sequence — legacy 16-byte packets leave both fields zero.
#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price:          f32,  // offset  0
    pub(crate) volume:         f32,  // offset  4
    pub(crate) sequence:       u64,  // offset  8
    pub(crate) timestamp:      u64,  // offset 16 — ingest time (ns since engine start)
    pub(crate) origin_ts_ns:   u64,  // offset 24 — exchange trade time (ns since epoch)
    pub(crate) transit_est_ns: u64,  // offset 32 — RTT/2 one-way transit estimate
    pub(crate) bid:            f32,  // offset 40 — best bid (v4; 0 if not provided)
    pub(crate) ask:            f32,  // offset 44 — best ask (v4)
    pub(crate) mark_price:     f32,  // offset 48 — perp mark price (v4)
    pub(crate) funding_rate:   f32,  // offset 52 — current funding rate (v4)
    _unused: [u8; 8],                // offset 56 — padding to 64 bytes
}

/// Lock-free SPSC ring buffer delivering ticks from the ingestor to the strategy.
///
/// The ingestor is the sole writer of `latest_idx` (publish with `Release`); the
/// strategy is the sole reader (consume with `Acquire`). `start_time` is
/// deliberately placed in the *same* cache line as `latest_idx` so that the
/// strategy's per-iteration cursor load keeps the clock anchor L1-hot for free —
/// do not insert any field between them (invariants #2, #3).
#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks:      [MarketTick; BUFFER_SIZE],
    pub(crate) latest_idx: AtomicU64,  // offset 65536 — cache-line boundary
    pub(crate) start_time: Instant,    // offset 65544 — same cache line as latest_idx
}

/// One recorded trade: its full-stack latency breakdown and its fill/slippage
/// (64 bytes, 7×u64 + 2×f32).
///
/// The strategy fills every field except `round_trip_ns` (written by the exchange
/// thread) and `fill_price` (written later, when the simulated market fill becomes
/// due — one transit after the order is sent). The 64-byte size is assumed by the
/// trade-log pre-touch loop in `main` (invariant #10).
///
/// `target_price` is the price we intended to buy at (the configured target in
/// target mode, or the entry price in breakout mode); `fill_price` is the market
/// price one transit later (0.0 = still pending at shutdown). Their difference is
/// the latency-induced slippage.
#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence:       u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns:    u64,
    pub latency_ns:     u64,   // signal latency: buy_time_ns - ingest_time_ns
    pub order_send_ns:  u64,
    pub round_trip_ns:  u64,   // written by exchange thread
    pub transit_est_ns: u64,   // RTT/2 transit estimate, copied from the tick
    pub target_price:   f32,   // intended buy price (config target, or entry price)
    pub fill_price:     f32,   // market price one transit later (0.0 = unfilled)
}

// Layout invariants, checked at compile time.
//   #1:  MarketTick stays exactly 64 bytes (one cache line; pre-touch step = 8).
//   #10: TradeExecution stays 64 bytes (pre-touch step = 8 in main.rs).
const _: () = assert!(std::mem::size_of::<MarketTick>() == 64);
const _: () = assert!(std::mem::size_of::<TradeExecution>() == 64);

/// Single-writer lock-free latency log.
///
/// Write: fill `entries[write_idx & MASK]`, then `fetch_add(1, Release)` on
/// `write_idx` to publish. Read: `load(Acquire)` for the committed count, then
/// read `entries[0..count]`. The strategy is the only thread that advances
/// `write_idx`.
pub(crate) struct TradeLog {
    pub(crate) entries:   UnsafeCell<[TradeExecution; TRADE_LOG_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}

unsafe impl Sync for TradeLog {}

impl TradeLog {
    pub(crate) fn new() -> Self {
        TradeLog {
            entries:   UnsafeCell::new(unsafe { std::mem::zeroed() }),
            write_idx: AtomicU64::new(0),
        }
    }
}

// Fixed-bucket latency histogram covering 0–10,000 ns (one bucket per ns).
// Values above 10,000 ns land in the overflow counter.
//
// Single-writer semantics: the strategy thread is the sole writer of sig_hist;
// the exchange thread is the sole writer of rt_hist. No concurrent writes to
// the same histogram. Reads only happen at shutdown from the watchdog thread,
// after all hot-path activity has ceased.
pub(crate) struct LatencyHistogram {
    pub(crate) buckets:  UnsafeCell<[u64; 10_001]>,
    pub(crate) overflow: AtomicU64,
}

unsafe impl Sync for LatencyHistogram {}

impl LatencyHistogram {
    pub(crate) fn new() -> Self {
        LatencyHistogram {
            buckets:  UnsafeCell::new([0u64; 10_001]),
            overflow: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub(crate) fn record(&self, ns: u64) {
        if ns <= 10_000 {
            unsafe { (*self.buckets.get())[ns as usize] += 1; }
        } else {
            self.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Walk the bucket array to find the value at which `p_num/p_den` of all
    // observations fall at or below. O(n) in bucket count, no allocation.
    // Returns 10_001 if the percentile falls in the overflow bucket.
    pub(crate) fn percentile(&self, p_num: u64, p_den: u64, total: u64) -> u64 {
        if total == 0 { return 0; }
        let threshold = (total * p_num).div_ceil(p_den).max(1);
        let mut cum = 0u64;
        let buckets = unsafe { &*self.buckets.get() };
        for (i, &count) in buckets.iter().enumerate() {
            cum += count;
            if cum >= threshold {
                return i as u64;
            }
        }
        10_001
    }
}

/// Trading-model configuration (set once by `main` from env vars; read-only after).
/// Plain `Copy` data shared across threads. `enabled` gates the whole trade path.
#[derive(Copy, Clone)]
pub(crate) struct TradeCfg {
    pub enabled:       bool,
    pub allow_short:   bool,
    pub entry_dip_bps: f32,   // long when price ≤ ref·(1-x); short when ≥ ref·(1+x)
    pub tp_bps:        f32,   // take-profit (favourable move from entry)
    pub sl_bps:        f32,   // stop-loss (adverse move from entry)
    pub fee_bps:       f32,   // taker fee charged per side
    pub leverage:      f32,
    pub max_size_mult: f32,   // cap on dynamic size scaling
    pub adaptive:      bool,  // entry/TP/SL as multiples of rolling volatility
    pub use_flow:      bool,  // require order-flow (signed-volume) confirmation
    pub capital:       f32,   // starting capital (quote); equity compounds from here
    pub risk_frac:     f32,   // fraction of equity used as margin per trade
    // ── Trend-following + cross-market signal (HFT_MOMENTUM) ──
    pub momentum:        bool, // gate: trade WITH the trend (else mean-reversion)
    pub w_trend:         f32,  // composite-S weights
    pub w_flow:          f32,
    pub w_basket:        f32,
    pub w_leadlag:       f32,
    pub signal_thr_bps:  f32,  // |S| gate to call a trend
    pub pullback_bps:    f32,  // dip/rip vs fast EMA to time entry
    pub trail_bps:       f32,  // trailing-stop retrace
    pub signal_exit_bps: f32,  // exit when S weakens past this
    pub beta:            f32,  // lead-lag transfer coefficient
    // Cost-aware execution (default off → current behavior).
    pub maker:           bool, // entry pays maker fee (passive) instead of taker
    pub maker_bps:       f32,  // entry-side maker fee under `maker` (can be negative)
    pub fee_gate:        bool, // require expected move ≥ round-trip cost + min_edge
    pub min_edge_bps:    f32,  // edge buffer over cost for the fee gate
    pub normalize:       bool, // z-score the composite-signal terms
    // Realistic fills (SP2): entries/exits cross the real bid/ask spread; this adds
    // extra adverse slippage in bps on top of the spread (0 = spread only).
    pub slippage_bps:    f32,
    // Funding (SP3): manual relative funding-rate override in bps/hr for offline
    // testing; 0 = use the feed's per-tick relative_funding_rate.
    pub funding_bps_per_hr: f32,
}

/// One completed round-trip (entry → exit), the unit of the P&L scorecard.
/// 64 bytes (4×u64 + 8×f32). Single writer: the strategy thread, on each exit.
#[derive(Copy, Clone)]
pub(crate) struct RoundTrip {
    pub entry_time_ns: u64,
    pub exit_time_ns:  u64,
    pub spread_cost_bps: f32, // SP2: spread+slippage cost vs mid-to-mid (bps). Hold
                              // time is derived as exit_time_ns - entry_time_ns.
    pub side:          i64,   // +1 long, -1 short
    pub entry_price:   f32,
    pub exit_price:    f32,
    pub size:          f32,
    pub gross_bps:     f32,   // signed favourable move, before fees
    pub net_bps:       f32,   // gross_bps - 2·fee_bps
    pub pnl_quote:     f32,   // net P&L in quote currency, including leverage
    pub fees_quote:    f32,   // total fees paid (both sides), quote currency
    pub flags:         f32,   // 1.0 = closed by liquidation, else 0.0
}

/// Single-writer lock-free log of completed round-trips (same protocol as TradeLog).
pub(crate) struct RoundTripLog {
    pub(crate) entries:   UnsafeCell<[RoundTrip; ROUND_TRIP_LOG_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}

unsafe impl Sync for RoundTripLog {}

impl RoundTripLog {
    pub(crate) fn new() -> Self {
        RoundTripLog {
            entries:   UnsafeCell::new(unsafe { std::mem::zeroed() }),
            write_idx: AtomicU64::new(0),
        }
    }
}

const _: () = assert!(std::mem::size_of::<RoundTrip>() == 64);

/// Shared run state: the trade log, both latency histograms, and the run's
/// bookkeeping atomics (stall/gap counters, the dirty-feed flag, the risk
/// `halt`/`net_position`, and memory snapshots). One instance is shared by all
/// threads behind an `Arc`.
#[repr(C, align(64))]
pub(crate) struct OrderBook {
    pub(crate) trade_log:    TradeLog,
    pub(crate) sig_hist:     LatencyHistogram,  // sole writer: strategy thread
    pub(crate) rt_hist:      LatencyHistogram,  // sole writer: exchange thread
    pub(crate) stall_count:  AtomicU64,         // incremented by strategy on >500ns spin gaps
    pub(crate) gap_count:    AtomicU64,         // incremented by ingestor on sequence gaps
    pub(crate) dirty:        AtomicBool,        // set by ingestor, cleared by strategy after N clean seqs
    // Risk management (item 5)
    pub(crate) halt:         AtomicBool,        // permanent stop flag; set by halt_trading(), never cleared
    pub(crate) net_position: AtomicI64,         // sole writer: strategy thread; incremented on each long trade
    // Memory snapshots — written once by main before threads spawn; read at shutdown by watchdog.
    pub(crate) mem_total_ram:  AtomicU64,       // total physical RAM (bytes), snapshot [1]
    pub(crate) mem_rss_start:  AtomicU64,       // peak RSS before buffer allocation, snapshot [1]
    pub(crate) mem_rss_ready:  AtomicU64,       // peak RSS after pre-touch + process setup, snapshot [2]
    // Execution / slippage tracking.
    pub(crate) attempts:      AtomicU64,        // buy attempts (sole writer: strategy)
    pub(crate) filled:        AtomicU64,        // attempts whose simulated fill resolved (sole writer: strategy)
    pub(crate) price_lo_bits: AtomicU32,        // min observed price (f32 bits); sole writer: ingestor
    pub(crate) price_hi_bits: AtomicU32,        // max observed price (f32 bits); sole writer: ingestor
    pub(crate) spread_lo_bits: AtomicU32,       // min observed spread (f32 bps bits); sole writer: ingestor
    pub(crate) spread_hi_bits: AtomicU32,       // max observed spread (f32 bps bits); sole writer: ingestor
    pub(crate) funding_bits:   AtomicU32,       // latest funding rate (f32 bits); sole writer: ingestor
    // Target-price buy level, set once by main from HFT_TARGET_PRICE. 0.0 = breakout mode.
    pub(crate) target_price:  f32,
    // Relative-dip threshold in bps (HFT_TARGET_DIP_BPS). >0 = buy on a dip of this
    // many bps below a rolling reference; adapts to any price level. Takes priority
    // over target_price.
    pub(crate) target_dip_bps: f32,
    // Downtick mode (HFT_DOWNTICK): buy on any price decrease. Guaranteed to fire on
    // any feed that moves at all — the fallback for very flat/thin markets. Highest
    // priority among the buy triggers.
    pub(crate) buy_on_downtick: bool,
    // ── Trading model (HFT_TRADE) ───────────────────────────────────────────
    pub(crate) trade_cfg:   TradeCfg,        // set once by main
    pub(crate) round_trips: RoundTripLog,    // completed round-trips; sole writer: strategy
    pub(crate) vol_ema_bits: AtomicU32,      // final rolling volatility est (f32 bps bits); writer: strategy
    // ── Composite signal output (HFT_MOMENTUM) ──
    pub(crate) latest_signal_bits: AtomicU32,// latest S (f32 bps bits), written every tick by strategy
    pub(crate) signal:      SignalLog,       // downsampled S series + per-trade S; sole writer: strategy
}

/// Signal output log (single writer: strategy). `series` is a downsampled ring of
/// the composite signal S; `at_entry[k]` is S at the entry of round-trip k. Both
/// are read only at shutdown. Mirrors the lock-free single-writer pattern of TradeLog.
pub(crate) struct SignalLog {
    pub(crate) series:     UnsafeCell<[f32; SIGNAL_SERIES_LEN]>,
    pub(crate) series_idx: AtomicU64,
    pub(crate) at_entry:   UnsafeCell<[f32; ROUND_TRIP_LOG_SIZE]>,
}

unsafe impl Sync for SignalLog {}

impl SignalLog {
    pub(crate) fn new() -> Self {
        SignalLog {
            series:     UnsafeCell::new([0.0; SIGNAL_SERIES_LEN]),
            series_idx: AtomicU64::new(0),
            at_entry:   UnsafeCell::new([0.0; ROUND_TRIP_LOG_SIZE]),
        }
    }
}

// ── Multi-instrument scaffold (item 8) ──────────────────────────────────────
//
// A compact, zero-copy instrument identifier. The value is the ring buffer array
// index (0-based). A flat array is used rather than HashMap to guarantee O(1)
// access with no heap allocation in the hot path.
//
// When a real feed arrives:
//   1. Parse the instrument ID from the packet header.
//   2. Map it to a compact u8 (e.g. via a small LUT populated at startup).
//   3. Index into InstrumentBuffers::buffers[id as usize].
//   4. The ingestor writes to that slot; the strategy spins on all slots or on
//      a priority-ordered subset.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct InstrumentId(pub u8);

impl InstrumentId {
    #[allow(dead_code)]
    pub(crate) fn as_index(self) -> usize {
        self.0 as usize
    }
}

// Pre-allocated ring buffer array: one slot per instrument, indexed by
// InstrumentId. All slots are allocated at startup (no dynamic allocation in
// the hot path). Currently only slot 0 is active; slots 1..MAX_INSTRUMENTS
// are reserved for future instruments.
//
// The #[repr(C)] ensures the array layout is predictable; each RingBuffer is
// independently cache-line aligned (via its own #[repr(C, align(64))]).
#[allow(dead_code)]
#[repr(C)]
pub(crate) struct InstrumentBuffers {
    pub(crate) buffers: [RingBuffer; MAX_INSTRUMENTS],
}

// SAFETY: InstrumentBuffers is accessed across threads, but each RingBuffer
// slot has its own AtomicU64 write cursor with Acquire/Release ordering.
// InstrumentBuffers itself is not Sync by default because RingBuffer contains
// non-Sync fields (Instant). We assert Sync here; callers must ensure that
// at most one thread writes latest_idx for each slot.
unsafe impl Sync for InstrumentBuffers {}

// ────────────────────────────────────────────────────────────────────────────

/// One submitted order on the strategy→exchange ring (64 bytes / one cache line).
///
/// `slot` is the trade-log index this order corresponds to, letting the exchange
/// write `round_trip_ns` back in O(1) without scanning.
#[repr(C, align(64))]
pub(crate) struct OrderEntry {
    pub(crate) sequence:      u64,
    pub(crate) slot:          u64,
    pub(crate) order_send_ns: u64,
    _pad: [u8; 40],
}

/// Lock-free SPSC ring carrying orders from the strategy (sole writer) to the
/// in-process exchange (sole reader). Same publish/consume protocol as
/// [`TradeLog`]: `fetch_add(Release)` to submit, `load(Acquire)` to detect.
pub(crate) struct OrderRing {
    pub(crate) entries:   UnsafeCell<[OrderEntry; ORDER_RING_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}

unsafe impl Sync for OrderRing {}

impl OrderRing {
    pub(crate) fn new() -> Self {
        OrderRing {
            entries:   UnsafeCell::new(unsafe { std::mem::zeroed() }),
            write_idx: AtomicU64::new(0),
        }
    }
}
