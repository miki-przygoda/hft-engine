use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use crate::{BUFFER_SIZE, ORDER_RING_SIZE, TRADE_LOG_SIZE};

#[repr(C, align(64))]
pub(crate) struct MarketTick {
    pub(crate) price:     f32,
    pub(crate) volume:    f32,
    pub(crate) sequence:  u64,
    pub(crate) timestamp: u64,
    _unused: [u8; 36],
}

#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks:      [MarketTick; BUFFER_SIZE],
    pub(crate) latest_idx: AtomicU64,  // offset 65536 — cache-line boundary
    pub(crate) start_time: Instant,    // offset 65544 — same cache line as latest_idx
}

#[derive(Copy, Clone)]
pub(crate) struct TradeExecution {
    pub sequence:       u64,
    pub ingest_time_ns: u64,
    pub buy_time_ns:    u64,
    pub latency_ns:     u64,
    pub order_send_ns:  u64,
    pub round_trip_ns:  u64,
}

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

#[repr(C, align(64))]
pub(crate) struct OrderBook {
    pub(crate) trade_log: TradeLog,
}

#[repr(C, align(64))]
pub(crate) struct OrderEntry {
    pub(crate) sequence:      u64,
    pub(crate) slot:          u64,
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
