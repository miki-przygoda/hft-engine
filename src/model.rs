//! The alpha model — the trend-following + cross-market signal and its long/short
//! execution — factored out of the live strategy so the **backtester and the live
//! engine run identical logic** (single source of truth).
//!
//! `AlphaModel` owns all per-tick signal state (fast/slow EMAs per instrument,
//! order-flow and volatility EMAs, the reference lead-lag), the open position, and
//! the compounding equity. `on_traded_tick` returns a [`Decision`]; the caller
//! applies the side effects (the live loop emits latency orders / logs / updates
//! `net_position`; the backtester just accumulates the round-trip).
//!
//! New cost-aware behaviors (z-score normalization, maker fee, fee gate) all
//! default OFF in `TradeCfg`, so with the defaults this reproduces the previous
//! inline momentum logic bit-for-bit — the harness sweeps them on.

use crate::models::{RoundTrip, TradeCfg};

const FAST_ALPHA: f32 = 1.0 / 16.0;
const SLOW_ALPHA: f32 = 1.0 / 128.0;
const FLOW_ALPHA: f32 = 1.0 / 16.0;
const VOL_ALPHA:  f32 = 1.0 / 32.0;
const VOL_FLOOR:  f32 = 0.1;

// ── Learned policy (tiny MLP, L1-resident) ──────────────────────────────────
// 6 standardized features → 8-unit tanh hidden → 1 linear output (the signal S
// in bps). 65 f32 weights (260 bytes) — fits in L1; inference is ~56 MACs + 8
// tanh, branchless. Trained offline by CEM (see engine::run_train).
pub(crate) const N_FEATURES: usize = 6;
const N_HIDDEN: usize = 8;
pub(crate) const N_PARAMS: usize = N_FEATURES * N_HIDDEN + N_HIDDEN + N_HIDDEN + 1; // 65

#[derive(Copy, Clone)]
pub(crate) struct Policy {
    pub(crate) p: [f32; N_PARAMS],
}

impl Policy {
    // Layout: [0..48] w1 (hidden×features), [48..56] b1, [56..64] w2, [64] b2.
    #[inline(always)]
    pub(crate) fn forward(&self, x: &[f32; N_FEATURES]) -> f32 {
        let p = &self.p;
        let mut out = p[64];
        for j in 0..N_HIDDEN {
            let mut s = p[48 + j];
            let base = j * N_FEATURES;
            for i in 0..N_FEATURES { s += p[base + i] * x[i]; }
            out += p[56 + j] * s.tanh();
        }
        out
    }

    pub(crate) fn to_le_bytes(self) -> Vec<u8> {
        let mut b = Vec::with_capacity(N_PARAMS * 4);
        for w in &self.p { b.extend_from_slice(&w.to_le_bytes()); }
        b
    }

    pub(crate) fn from_le_bytes(b: &[u8]) -> Option<Policy> {
        if b.len() < N_PARAMS * 4 { return None; }
        let mut p = [0.0; N_PARAMS];
        for (i, w) in p.iter_mut().enumerate() {
            *w = f32::from_le_bytes(b[i * 4..i * 4 + 4].try_into().ok()?);
        }
        Some(Policy { p })
    }
}


/// What the model decided on a tick. The caller turns this into side effects.
pub(crate) enum Decision {
    None,
    Enter { side: i64, signal_bps: f32 },
    Exit(RoundTrip),
}

/// Taker fill price: a buy crosses up to the **ask**, a sell crosses down to the
/// **bid**, plus `slippage_bps` of extra adverse slippage (book-walking / impact).
/// Falls back to `mid` when there is no usable quote (bid/ask are 0 or degenerate,
/// e.g. the spot trade feed or legacy v1–v3 packets), so non-v4 feeds keep filling
/// at the observed price — SP2 spread cost applies only where a real spread exists.
pub(crate) fn taker_fill(mid: f32, bid: f32, ask: f32, is_buy: bool, slippage_bps: f32) -> f32 {
    let have_quote = bid > 0.0 && ask >= bid;
    let base = if is_buy {
        if have_quote { ask } else { mid }
    } else if have_quote { bid } else { mid };
    let slip = slippage_bps / 10_000.0;
    if is_buy { base * (1.0 + slip) } else { base * (1.0 - slip) }
}

pub(crate) struct AlphaModel {
    cfg: TradeCfg,
    // Signal EMAs: index 0 = traded instrument, 1 = reference.
    fast_ema: [f32; 2],
    slow_ema: [f32; 2],
    ema_init: [bool; 2],
    ref_prev_px: f32,
    ref_ret_ema: f32,
    flow_ema: f32,
    flow_norm: f32,
    vol_ema: f32,
    vol_prev: f32,
    vol_init: bool,
    // Rolling |term| scales for optional z-score normalization.
    scale_trend: f32,
    scale_basket: f32,
    scale_leadlag: f32,
    // Learned policy (when present, replaces the hand-weighted signal) + rolling
    // |feature| scales so the MLP sees standardized, well-conditioned inputs.
    policy: Option<Policy>,
    feat_scale: [f32; N_FEATURES],
    // Position / capital.
    pub(crate) pos_side: i64, // 0 flat, +1 long, -1 short
    entry_price: f32,         // adverse entry fill (ask for long, bid for short)
    entry_mid: f32,           // mid at entry, for the spread-cost accounting
    entry_time: u64,
    pos_size: f32,
    entry_margin: f64,
    best_price: f32,
    pub(crate) equity: f64,
    pub(crate) ruined: bool,
    latest_signal: f32,
}

impl AlphaModel {
    pub(crate) fn with_policy(cfg: TradeCfg, policy: Option<Policy>) -> Self {
        AlphaModel {
            cfg,
            fast_ema: [0.0; 2], slow_ema: [0.0; 2], ema_init: [false; 2],
            ref_prev_px: 0.0, ref_ret_ema: 0.0,
            flow_ema: 0.0, flow_norm: 1.0,
            vol_ema: 0.0, vol_prev: 0.0, vol_init: false,
            scale_trend: 1.0, scale_basket: 1.0, scale_leadlag: 1.0,
            policy, feat_scale: [1.0; N_FEATURES],
            pos_side: 0, entry_price: 0.0, entry_mid: 0.0, entry_time: 0, pos_size: 0.0,
            entry_margin: 0.0, best_price: 0.0,
            equity: cfg.capital as f64, ruined: false, latest_signal: 0.0,
        }
    }

    pub(crate) fn latest_signal_bps(&self) -> f32 { self.latest_signal }
    pub(crate) fn vol_ema(&self) -> f32 { self.vol_ema }

    /// Round-trip cost in bps: taker both sides, or maker entry + taker exit.
    fn round_trip_fee_bps(&self) -> f32 {
        if self.cfg.maker { self.cfg.maker_bps + self.cfg.fee_bps } else { 2.0 * self.cfg.fee_bps }
    }

    /// Update the reference-market EMAs (call when the reference cursor advances,
    /// before `on_traded_tick`, to match the live ordering).
    pub(crate) fn on_reference_tick(&mut self, rpx: f32) {
        if self.ema_init[1] {
            self.fast_ema[1] += (rpx - self.fast_ema[1]) * FAST_ALPHA;
            self.slow_ema[1] += (rpx - self.slow_ema[1]) * SLOW_ALPHA;
            let rret = if self.ref_prev_px > 0.0 {
                (rpx - self.ref_prev_px) / self.ref_prev_px * 10_000.0 } else { 0.0 };
            self.ref_ret_ema += (rret - self.ref_ret_ema) * FAST_ALPHA;
        } else {
            self.fast_ema[1] = rpx; self.slow_ema[1] = rpx; self.ema_init[1] = true;
        }
        self.ref_prev_px = rpx;
    }

    /// Process a traded-instrument tick. `warmed` gates trading until past warmup;
    /// `halted` blocks new entries (exits still proceed).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_traded_tick(
        &mut self, price: f32, bid: f32, ask: f32, signed_vol: f32, now_ns: u64, warmed: bool, halted: bool,
    ) -> Decision {
        // Own EMAs, order flow, volatility.
        if self.ema_init[0] {
            self.fast_ema[0] += (price - self.fast_ema[0]) * FAST_ALPHA;
            self.slow_ema[0] += (price - self.slow_ema[0]) * SLOW_ALPHA;
        } else { self.fast_ema[0] = price; self.slow_ema[0] = price; self.ema_init[0] = true; }
        self.flow_ema  += (signed_vol - self.flow_ema) * FLOW_ALPHA;
        self.flow_norm += (signed_vol.abs() - self.flow_norm) * SLOW_ALPHA;
        if self.vol_init {
            let ret = ((price - self.vol_prev) / self.vol_prev * 10_000.0).abs();
            self.vol_ema += (ret - self.vol_ema) * VOL_ALPHA;
        } else { self.vol_init = true; }
        self.vol_prev = price;

        // Composite signal terms (bps).
        let trend0 = if self.slow_ema[0] > 0.0 { (self.fast_ema[0]-self.slow_ema[0])/self.slow_ema[0]*10_000.0 } else { 0.0 };
        let trend1 = if self.slow_ema[1] > 0.0 { (self.fast_ema[1]-self.slow_ema[1])/self.slow_ema[1]*10_000.0 } else { 0.0 };
        let flow_term = (self.flow_ema / self.flow_norm.max(1e-6)).clamp(-3.0, 3.0) * 3.0;
        let leadlag = self.ref_ret_ema * self.cfg.beta;

        let s = if let Some(pol) = self.policy.as_ref() {
            // ── Learned policy ──────────────────────────────────────────────
            // Six raw features, each standardized by a rolling |value| EMA so
            // the MLP sees O(1)-scaled, well-conditioned inputs regardless of
            // the absolute price/vol regime. Output is the signal S in bps.
            let pull = if self.fast_ema[0] > 0.0 {
                (price - self.fast_ema[0]) / self.fast_ema[0] * 10_000.0 } else { 0.0 };
            let raw = [trend0, pull, flow_term, trend1, self.ref_ret_ema, self.vol_ema];
            let mut x = [0.0f32; N_FEATURES];
            for k in 0..N_FEATURES {
                self.feat_scale[k] += (raw[k].abs() - self.feat_scale[k]) * SLOW_ALPHA;
                x[k] = raw[k] / self.feat_scale[k].max(1e-6);
            }
            pol.forward(&x)
        } else {
            // ── Hand-weighted composite (default) ───────────────────────────
            // Optional z-score: divide each unbounded term by its rolling |value|
            // and re-express in bps via the realized-vol unit, so weights compare.
            let (t0, t1, ll) = if self.cfg.normalize {
                self.scale_trend   += (trend0.abs()  - self.scale_trend)   * SLOW_ALPHA;
                self.scale_basket  += (trend1.abs()  - self.scale_basket)  * SLOW_ALPHA;
                self.scale_leadlag += (leadlag.abs() - self.scale_leadlag) * SLOW_ALPHA;
                let unit = self.vol_ema.max(VOL_FLOOR);
                ( trend0  / self.scale_trend.max(1e-6)   * unit,
                  trend1  / self.scale_basket.max(1e-6)  * unit,
                  leadlag / self.scale_leadlag.max(1e-6) * unit )
            } else { (trend0, trend1, leadlag) };
            self.cfg.w_trend * t0 + self.cfg.w_flow * flow_term
                + self.cfg.w_basket * t1 + self.cfg.w_leadlag * ll
        };
        self.latest_signal = s;

        if !warmed || self.ruined { return Decision::None; }

        if self.pos_side == 0 {
            let bullish = s >  self.cfg.signal_thr_bps;
            let bearish = s < -self.cfg.signal_thr_bps;
            let pb = self.cfg.pullback_bps / 10_000.0;
            let long_ok  = bullish && price <= self.fast_ema[0] * (1.0 - pb);
            let short_ok = bearish && self.cfg.allow_short && price >= self.fast_ema[0] * (1.0 + pb);
            let dir: i64 = if long_ok { 1 } else if short_ok { -1 } else { 0 };
            if dir == 0 || halted { return Decision::None; }

            // Fee-aware gate: only act if the expected move clears the round trip.
            if self.cfg.fee_gate {
                let expected = (3.0 * self.vol_ema).max(self.cfg.tp_bps);
                if expected < self.round_trip_fee_bps() + self.cfg.min_edge_bps {
                    return Decision::None;
                }
            }

            let conv = ((s.abs() - self.cfg.signal_thr_bps).max(0.0)
                        / self.cfg.signal_thr_bps.max(0.1) + 1.0).min(self.cfg.max_size_mult) as f64;
            let risk = (self.cfg.risk_frac as f64 * conv).min(1.0);
            self.entry_margin = self.equity * risk;
            let notional = self.entry_margin * self.cfg.leverage as f64;
            // Cross the spread on entry: a long buys the ask, a short sells the bid.
            self.entry_price = taker_fill(price, bid, ask, dir == 1, self.cfg.slippage_bps);
            self.entry_mid  = price;
            self.pos_size   = (notional / self.entry_price as f64) as f32;
            self.entry_time = now_ns;
            self.pos_side = dir;
            self.best_price = price;   // mark at mid for the trailing stop
            Decision::Enter { side: dir, signal_bps: s }
        } else {
            if self.pos_side == 1 { if price > self.best_price { self.best_price = price; } }
            else if price < self.best_price { self.best_price = price; }
            let move_bps = (price - self.entry_price) / self.entry_price * 10_000.0 * self.pos_side as f32;
            let trail_hit = if self.pos_side == 1 {
                price <= self.best_price * (1.0 - self.cfg.trail_bps / 10_000.0)
            } else {
                price >= self.best_price * (1.0 + self.cfg.trail_bps / 10_000.0)
            };
            let flip = if self.pos_side == 1 { s < -self.cfg.signal_exit_bps }
                       else { s > self.cfg.signal_exit_bps };
            let liq_bps    = 10_000.0 / self.cfg.leverage.max(1.0);
            let liquidated = move_bps <= -liq_bps;
            let tp_hit = self.cfg.tp_bps > 0.0 && move_bps >=  self.cfg.tp_bps;
            let sl_hit = self.cfg.sl_bps > 0.0 && move_bps <= -self.cfg.sl_bps;
            if !(trail_hit || flip || tp_hit || sl_hit || liquidated) { return Decision::None; }

            // Cross the spread on exit: a long sells the bid, a short buys the ask.
            // move_bps (mid-marked) drove the triggers above; the realized gross is
            // measured fill-to-fill, so both half-spreads land in the P&L.
            let exit_fill  = taker_fill(price, bid, ask, self.pos_side == -1, self.cfg.slippage_bps);
            let gross_bps  = ((exit_fill - self.entry_price) / self.entry_price * 10_000.0 * self.pos_side as f32) as f64;
            let notional   = self.entry_margin * self.cfg.leverage as f64;
            let rt_fee     = self.round_trip_fee_bps() as f64;
            let fees_quote = notional * (rt_fee / 10_000.0);
            let raw_pnl    = notional * (gross_bps / 10_000.0) - fees_quote;
            let pnl_quote  = raw_pnl.max(-self.entry_margin);
            let was_liq    = liquidated || raw_pnl <= -self.entry_margin;
            self.equity += pnl_quote;
            if self.equity <= 0.0 { self.equity = 0.0; self.ruined = true; }
            let side = self.pos_side;
            self.pos_side = 0;
            // Spread+slippage cost = the mid-to-mid "ideal" move minus the realized
            // fill-to-fill gross. Positive = the spread/slippage ate this many bps.
            let ideal_gross = ((price - self.entry_mid) / self.entry_mid * 10_000.0 * side as f32) as f64;
            Decision::Exit(RoundTrip {
                entry_time_ns: self.entry_time,
                exit_time_ns: now_ns,
                spread_cost_bps: (ideal_gross - gross_bps) as f32,
                side,
                entry_price: self.entry_price,
                exit_price: exit_fill,
                size: self.pos_size,
                gross_bps: gross_bps as f32,
                net_bps: (gross_bps - rt_fee) as f32,
                pnl_quote: pnl_quote as f32,
                fees_quote: fees_quote as f32,
                flags: if was_liq { 1.0 } else { 0.0 },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taker_fill_crosses_spread() {
        // Buy crosses to the ask; sell crosses to the bid.
        assert_eq!(taker_fill(100.0, 99.95, 100.05, true, 0.0), 100.05);
        assert_eq!(taker_fill(100.0, 99.95, 100.05, false, 0.0), 99.95);
        // No usable quote (bid/ask 0, or ask<bid) → fall back to mid.
        assert_eq!(taker_fill(100.0, 0.0, 0.0, true, 0.0), 100.0);
        assert_eq!(taker_fill(100.0, 0.0, 0.0, false, 0.0), 100.0);
        assert_eq!(taker_fill(100.0, 101.0, 99.0, true, 0.0), 100.0); // crossed quote → mid
        // Slippage is adverse: buys fill higher, sells lower (10 bps).
        assert!((taker_fill(100.0, 0.0, 0.0, true,  10.0) - 100.1).abs() < 1e-3);
        assert!((taker_fill(100.0, 0.0, 0.0, false, 10.0) -  99.9).abs() < 1e-3);
        // Spread + slippage stack on entry: ask 100.05, +10bps → ~100.150.
        assert!((taker_fill(100.0, 99.95, 100.05, true, 10.0) - 100.15005).abs() < 1e-2);
    }
}
