pub mod config {
    pub const WARMUP_PACKETS:        u64   = 10;
    pub const REAL_PACKETS:          u64   = 100;
    pub const TOTAL_PACKETS:         u64   = WARMUP_PACKETS + REAL_PACKETS;
    pub const PACKET_INTERVAL_MS:    u64   = 10;
    pub const INGESTOR_ADDR:         &str  = "127.0.0.1:34254";
    pub const ORDER_ADDR:            &str  = "127.0.0.1:34255";
    pub const CONFIRM_ADDR:          &str  = "127.0.0.1:34256";
    pub const MIN_ORDER_PACKET_SIZE: usize = 24;
}
