use std::sync::atomic::{AtomicU64};
use std::cell::UnsafeCell;
use std::time::Instant;
use rust_hft_software::config::{BUFFER_SIZE, TRADE_LOG_SIZE, ORDER_RING_SIZE};

#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price: f32,
    pub(crate) volume: f32,
    pub(crate) sequence: u64,
    pub(crate) timestamp: u64, // nanoseconds since engine start
    _unused: [u8; 36],
}


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

// Order entry written by the strategy into the shared order ring.
// Padded to 64 bytes so each entry occupies exactly one cache line —
// no false sharing between adjacent entries under concurrent access.
#[repr(C, align(64))]
pub(crate) struct OrderEntry {
    pub(crate) sequence:      u64,
    pub(crate) slot:          u64,  // trade log slot index for O(1) confirm lookup
    pub(crate) order_send_ns: u64,
    _pad: [u8; 40],
}

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
