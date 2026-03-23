use std::sync::atomic::{AtomicU64};

#[repr(C, align(64))]
pub(crate) struct MarketTick {
    price: f32,
    volume: f32,
    sequence: u64,
    _unused: [u8; 44],
}

pub(crate) const BUFFER_SIZE: usize = 1024;

#[repr(C, align(64))]
pub(crate) struct RingBuffer {
    pub(crate) ticks: [MarketTick; BUFFER_SIZE],
    pub(crate) latest_idx: AtomicU64,
}