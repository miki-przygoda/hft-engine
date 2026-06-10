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

    // v3 packet appends a 1-byte instrument id at [32] so a second market (the
    // cross-market reference) can be routed to its own ring buffer.
    // Back-compat: amt<32 legacy, 32<=amt<33 v2 (id 0), amt>=33 reads pkt[32].
    pub const INGEST_PACKET_SIZE_V3: usize = 33;
    // v4 packet appends bid/ask/mark_price/funding_rate (4×f32) at [33..49] for the
    // Kraken Futures feed. First 33 bytes stay byte-identical to v3 (amt>=49 reads them).
    pub const INGEST_PACKET_SIZE_V4: usize = 49;
    pub const N_INSTRUMENTS: usize = 2;  // traded (0) + reference (1); scaffold allows MAX_INSTRUMENTS

    // Kraken WebSocket v1 feed. TLS is terminated by a local stunnel instance
    // (STUNNEL_ADDR → KRAKEN_HOST:443); the adapter speaks plaintext TCP to
    // stunnel and never links a TLS library (zero-dependency invariant #13).
    pub const KRAKEN_HOST:  &str = "ws.kraken.com";
    pub const KRAKEN_PAIR:  &str = "XBT/USD";
    pub const STUNNEL_ADDR: &str = "127.0.0.1:8443";
    // Kraken REST (historical trades) via a second stunnel service → api.kraken.com:443.
    pub const KRAKEN_API_HOST: &str = "api.kraken.com";
    pub const API_STUNNEL_ADDR: &str = "127.0.0.1:8444";
    // Kraken Futures public market-data feed (no auth) via a third stunnel service
    // → futures.kraken.com:443. Distinct accept port from spot (8443) / api (8444).
    pub const KRAKEN_FUTURES_HOST: &str = "futures.kraken.com";
    pub const FUTURES_STUNNEL_ADDR: &str = "127.0.0.1:8445";
    pub const KRAKEN_FUTURES_PRODUCT: &str = "PF_XBTUSD";  // linear, USD-collateral perp

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

    // ── Trading model (HFT_TRADE) defaults; each is env-overridable in main ──
    // Long & short mean-reversion: enter on a dip/rip vs a rolling reference,
    // exit on take-profit / stop-loss / opposite signal. All values in bps unless
    // noted. These defaults are illustrative, not calibrated alpha.
    pub const ENTRY_DIP_BPS_DEFAULT: f32 = 3.0;   // long ≤ ref·(1-x), short ≥ ref·(1+x)
    pub const TP_BPS_DEFAULT:        f32 = 10.0;  // take-profit
    pub const SL_BPS_DEFAULT:        f32 = 10.0;  // stop-loss
    pub const FEE_BPS_DEFAULT:       f32 = 2.6;   // Kraken taker, per side
    pub const LEVERAGE_DEFAULT:      f32 = 1.0;
    pub const MAX_SIZE_MULT_DEFAULT: f32 = 4.0;   // cap on dynamic size scaling
    pub const CAPITAL_DEFAULT:       f32 = 10_000.0; // starting capital (quote)
    pub const RISK_FRAC_DEFAULT:     f32 = 0.10;  // margin per trade as a fraction of equity

    // ── Trend-following + cross-market signal (HFT_MOMENTUM) ─────────────────
    // A composite buy/sell signal S blends own trend + own order flow + a
    // reference market's trend (basket) + the reference's lead-lag. Trade WITH
    // the trend, time entries on pullbacks, exit on signal-flip / trailing stop.
    pub const KRAKEN_REF_PAIR:    &str = "ETH/USD";  // cross-market reference (adapter)
    pub const W_TREND_DEFAULT:    f32 = 1.0;   // weight: own fast-vs-slow EMA trend (bps)
    pub const W_FLOW_DEFAULT:     f32 = 0.5;   // weight: own order-flow (normalized)
    pub const W_BASKET_DEFAULT:   f32 = 0.5;   // weight: reference trend (market beta)
    pub const W_LEADLAG_DEFAULT:  f32 = 0.5;   // weight: reference recent return (leads us)
    pub const SIGNAL_THR_BPS_DEFAULT:  f32 = 5.0;  // |S| gate to call a trend
    pub const PULLBACK_BPS_DEFAULT:    f32 = 2.0;  // dip/rip vs fast EMA to time entry
    pub const TRAIL_BPS_DEFAULT:       f32 = 8.0;  // trailing-stop retrace from best price
    pub const SIGNAL_EXIT_BPS_DEFAULT: f32 = 0.0;  // exit when S weakens past this (0 = pure flip)
    pub const BETA_DEFAULT:            f32 = 1.0;  // lead-lag transfer coefficient

    // ── Cost-aware execution (all default to current behavior when off) ──────
    // Maker (passive) entry fee when HFT_MAKER=1 — you post a limit at the dip/
    // pullback and pay maker (often a rebate, so negative) instead of taker; the
    // exit still crosses (taker = FEE_BPS). Round-trip cost = maker + taker.
    pub const MAKER_BPS_DEFAULT:    f32 = 0.0;   // entry-side fee under HFT_MAKER (can be negative)
    // Fee-aware entry gate (HFT_FEE_GATE=1): require expected move ≥ round-trip
    // cost + this buffer before entering. Kills structurally-doomed trades.
    pub const MIN_EDGE_BPS_DEFAULT: f32 = 0.0;

    // ── Learned policy (HFT_MODEL / --train) ─────────────────────────────────
    // A tiny MLP (6→8→1, 65 f32 weights — see model::Policy) supplies the signal
    // S in place of the hand-weighted composite. Trained offline by cross-entropy
    // method (`trading-engine --train <capture>`) and persisted as raw LE f32.
    // The hyperparameters below are env-overridable (HFT_POP / HFT_GEN / HFT_SEED).
    pub const TRAIN_POP_DEFAULT:  usize = 256; // CEM population per generation
    pub const TRAIN_GEN_DEFAULT:  usize = 40;  // CEM generations
    pub const TRAIN_SEED_DEFAULT: u64   = 0xC0FFEE; // RNG seed (deterministic runs)
    pub const MODEL_PATH_DEFAULT: &str  = "models/policy.bin"; // weights file
}
