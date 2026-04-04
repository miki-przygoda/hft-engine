//! HFT engine library — shared configuration and data structures.

/// Central configuration — single source of truth for all tunable parameters.
pub mod config {
    // === PACKET CONFIGURATION ===
    /// Warmup packets for cache priming (not logged).
    pub const WARMUP_PACKETS: u64 = 10;

    /// Real trading packets to process.
    pub const REAL_PACKETS: u64 = 100;

    /// Total packets (warmup + real).
    pub const TOTAL_PACKETS: u64 = WARMUP_PACKETS + REAL_PACKETS;

    /// Inter-packet interval (ms).
    pub const PACKET_INTERVAL_MS: u64 = 50;

    /// Stats monitor timeout: auto-calculated to (REAL_PACKETS * PACKET_INTERVAL_MS / 1000) + 2 sec.
    /// Automatically adjusts when REAL_PACKETS changes — no manual sync needed.
    pub const STATS_MONITOR_TIMEOUT_SECS: u64 = (REAL_PACKETS * PACKET_INTERVAL_MS / 1000) + 2;

    // === BUFFER SIZES ===
    /// Ring buffer size for market ticks (must be power of 2 for masking).
    pub const BUFFER_SIZE: usize = 1024;

    /// Trade log capacity.
    pub const TRADE_LOG_SIZE: usize = 1024;

    /// Order ring buffer size (strategy → in-process exchange).
    pub const ORDER_RING_SIZE: usize = 1024;

    // === NETWORK CONFIGURATION ===
    /// Market tick ingestion listen address.
    pub const INGESTOR_ADDR: &str = "127.0.0.1:34254";

    /// Order submission address (strategy → exchange).
    pub const ORDER_ADDR: &str = "127.0.0.1:34255";

    /// Order confirmation address (exchange → strategy).
    pub const CONFIRM_ADDR: &str = "127.0.0.1:34256";

    /// Minimum order packet size (bytes).
    pub const MIN_ORDER_PACKET_SIZE: usize = 24;
}

// Re-export commonly used config constants at root level
pub use config::{BUFFER_SIZE, TRADE_LOG_SIZE, ORDER_RING_SIZE};
