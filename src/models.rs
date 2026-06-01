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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use crate::{BUFFER_SIZE, ORDER_RING_SIZE, TRADE_LOG_SIZE};
use rust_hft_software::config::MAX_INSTRUMENTS;

/// A single market data tick — exactly one 64-byte cache line.
///
/// One tick per cache line means no false sharing and no partial-line loads.
/// `timestamp` (ns since engine start) is written by the ingestor *before* it
/// publishes the tick via [`RingBuffer::latest_idx`], so the strategy sees a
/// consistent value after the matching acquire load. Must stay 64 bytes
/// (invariant #1) — adjust `_unused` if fields change.
#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price:     f32,
    pub(crate) volume:    f32,
    pub(crate) sequence:  u64,
    pub(crate) timestamp: u64,
    _unused: [u8; 36],
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

/// One recorded trade and its latency breakdown (48 bytes, 6 × u64).
///
/// The strategy fills every field except `round_trip_ns` and commits the slot;
/// the exchange thread later fills `round_trip_ns` on that already-committed
/// slot (invariant #4). The 6-u64 size is assumed by the trade-log pre-touch
/// loop in `main` (invariant #10).
#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence:       u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns:    u64,
    pub latency_ns:     u64,
    pub order_send_ns:  u64,
    pub round_trip_ns:  u64,
}

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
        let threshold = ((total * p_num + p_den - 1) / p_den).max(1);
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
