//! Shared configuration for the engine and its standalone binaries.
//!
//! This crate exists purely to share compile-time constants between the
//! self-contained `trading-engine` and the standalone `market-simulator` /
//! `fake-exchange` binaries (e.g. `WARMUP_PACKETS`, port addresses, packet
//! sizes). Keeping them in one place prevents the engine and the simulator from
//! drifting out of sync — see invariant #9 in `CLAUDE.md`.

/// Engine-wide tunable constants. All values are compile-time and dependency-free.
pub mod config {
    /// Warmup packets the engine runs through the full hot path *without*
    /// committing to the trade log (trains caches and the branch predictor).
    /// The standalone simulator must send exactly this many warmup packets first.
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

    // ── Live feed (kraken-feed adapter) ─────────────────────────────────────
    //
    // The v2 market-data packet is 32 bytes (LE). The first 16 bytes are
    // byte-identical to the legacy 16-byte packet (price f32, volume f32,
    // sequence u64) so old senders keep working; the extra 16 bytes carry the
    // exchange origin timestamp and the RTT/2 transit estimate:
    //   [ 0.. 4] price f32      [ 4.. 8] volume f32     [ 8..16] sequence u64
    //   [16..24] origin_ts_ns   [24..32] transit_est_ns
    // The ingestor parses the extra fields only when it receives >= 32 bytes.
    pub const INGEST_PACKET_SIZE_V2: usize = 32;

    // Kraken WebSocket v1 feed. TLS is terminated by a local stunnel instance
    // (STUNNEL_ADDR → KRAKEN_HOST:443); the adapter speaks plaintext TCP to
    // stunnel and never links a TLS library (zero-dependency invariant #13).
    pub const KRAKEN_HOST:  &str = "ws.kraken.com";
    pub const KRAKEN_PAIR:  &str = "XBT/USD";
    pub const STUNNEL_ADDR: &str = "127.0.0.1:8443";

    // RTT probe cadence: how often the adapter sends a WebSocket ping to refresh
    // its RTT/2 one-way transit estimate.
    pub const RTT_PING_INTERVAL_MS: u64 = 1000;

    // Default path for the record/replay capture file (relative to cwd).
    pub const RECORD_PATH_DEFAULT: &str = "recordings/kraken.krkr";

    // Buy-signal threshold in basis points above the 8-tick window (item: real
    // signal). The strategy triggers when current price exceeds the window
    // reference by this fraction. 10 bps = 0.10%. Loaded once at startup into
    // the SIMD scale constant; see trading_strategy in engine.rs.
    pub const SIGNAL_MOMENTUM_BPS: u64 = 10;
}
