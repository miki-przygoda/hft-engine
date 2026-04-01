use std::sync::atomic::{AtomicU64};
use std::cell::UnsafeCell;
use std::time::Instant;

#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price: f32,
    pub(crate) volume: f32,
    pub(crate) sequence: u64,
    pub(crate) timestamp: u64, // nanoseconds since engine start
    _unused: [u8; 36],
}

pub(crate) const BUFFER_SIZE: usize = 1024;

#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks: [MarketTick; BUFFER_SIZE],
    // latest_idx lands at offset 65536, exactly on a cache line boundary.
    // start_time sits 8 bytes later in the same 64-byte line, so every
    // spin-poll load(Acquire) on latest_idx inherently keeps start_time
    // in L1 — no idle-loop warming code required.
    pub(crate) latest_idx: AtomicU64,
    pub(crate) start_time: Instant,
}

#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence: u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns: u64,
    pub latency_ns: u64,     // buy_time_ns - ingest_time_ns
    pub order_send_ns: u64,  // when the order packet was dispatched to the exchange
    pub round_trip_ns: u64,  // filled by confirm receiver: confirm_recv_ns - order_send_ns
}

pub(crate) const TRADE_LOG_SIZE: usize = 1024;

// Lock-free single-writer trade log. write_idx is the committed entry count.
// Writer: fill entry at write_idx % SIZE, then fetch_add(1, Release).
// Reader: load(Acquire) to get count, then read entries[0..count].
pub(crate) struct TradeLog {
    pub(crate) entries: UnsafeCell<[TradeExecution; TRADE_LOG_SIZE]>,
    pub(crate) write_idx: AtomicU64,
}

unsafe impl Sync for TradeLog {}

impl TradeLog {
    pub(crate) fn new() -> Self {
        TradeLog {
            entries: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            write_idx: AtomicU64::new(0),
        }
    }
}

#[repr(C, align(64))]
pub(crate) struct OrderBook {
    pub(crate) buy_count: AtomicU64,
    pub(crate) trade_log: TradeLog,
}
