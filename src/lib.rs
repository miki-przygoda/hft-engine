pub mod config {
    pub const WARMUP_PACKETS:        u64   = 10;
    pub const REAL_PACKETS:          u64   = 100;   // used by standalone market-simulator binary
    pub const TOTAL_PACKETS:         u64   = WARMUP_PACKETS + REAL_PACKETS;
    pub const PACKET_INTERVAL_MS:    u64   = 10;    // used by standalone market-simulator binary
    pub const INGESTOR_ADDR:         &str  = "127.0.0.1:34254";
    pub const ORDER_ADDR:            &str  = "127.0.0.1:34255";
    pub const CONFIRM_ADDR:          &str  = "127.0.0.1:34256";
    pub const MIN_ORDER_PACKET_SIZE: usize = 24;

    // In-engine burst simulator parameters (run_market_simulator in engine.rs).
    // One burst = BURST_SIZE packets sent ~20µs apart, followed by BURST_GAP_MS silence.
    // Total real packets = BURST_SIZE * NUM_BURSTS = 1000.
    pub const BURST_SIZE:    u64 = 50;
    pub const NUM_BURSTS:    u64 = 20;
    pub const BURST_GAP_MS:  u64 = 500;

    // Strategy gap-recovery: number of consecutive clean ticks required after a
    // dirty flag is set before the strategy resumes trading.
    pub const CLEAN_SEQ_THRESHOLD: u64 = 5;

    // Risk limits (item 5). In simulation these are set high enough not to trigger;
    // in production set to meaningful values based on capital and risk appetite.
    pub const MAX_POSITION:  i64 = 10_000; // max net long position before halting
    pub const MAX_GAP_COUNT: u64 = 10;     // max cumulative sequence gaps before halting

    // Multi-instrument scaffold (item 8).
    // Pre-allocated ring buffer slots; index by compact instrument ID (0-based).
    pub const MAX_INSTRUMENTS: usize = 8;
}
