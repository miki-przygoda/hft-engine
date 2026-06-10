# SP1 — Kraken Futures feed adapter (real spread + mark + funding)

**Status:** design / approved to spec
**Date:** 2026-06-10
**Branch:** `claude/kraken-futures-feed` (off `claude/live-kraken-feed`)
**Part of initiative:** "Real costs & risk — Kraken Futures pivot" (SP1 of 5 + README)

---

## 1. Goal

Bring **real Kraken Futures perpetual market data** — best bid/ask (spread), mark
price, and the live funding rate — into the engine over the existing
zero-dependency hand-rolled WebSocket, with **no authentication**, and surface it
in the shutdown report. This is the data foundation that later sub-projects
consume (SP2 realistic fills, SP3 funding accrual + real perp P&L).

**SP1 explicitly does *not* change trading behavior.** Its deliverable is "real
futures data flows in end-to-end and is visible," which also validates the
public-feed connectivity assumption against the live exchange.

### Success criteria
- `kraken-feed --futures PF_XBTUSD` streams the live public ticker and emits v4 packets.
- The engine ingests v4, stores bid/ask/mark/funding per tick, and the shutdown
  report + JSON show **observed spread (bps)** and **current funding rate**.
- `kraken-feed --synth` emits synthetic spread + funding so offline replay/backtest
  exercise the new fields deterministically with no network.
- Spot `--live` / `--synth` / `--replay` paths still work unchanged (additive).
- New pure logic (ticker parse, v4 codec, spread-bps) is unit-tested.

## 2. Background — Kraken Futures public feed

Kraken Futures market data is a **public** WebSocket; the challenge/sign auth
applies only to *private* feeds (orders/fills/positions), which a measurement
engine never uses. Endpoint: `futures.kraken.com` (TLS, terminated by stunnel as
today). Subscribe:

```json
{ "event": "subscribe", "feed": "ticker", "product_ids": ["PF_XBTUSD"] }
```

The `ticker` feed publishes (throttled ~1 s) a JSON **object** (not the array the
spot trade feed uses) including, per the docs:

| field          | meaning                                  |
|----------------|------------------------------------------|
| `bid`          | best bid price                           |
| `ask`          | best ask price                           |
| `markPrice`    | mark price (funding/liquidation ref)     |
| `last`         | last trade price                         |
| `funding_rate` | current funding rate (perps; **omitted when zero**) |
| `time`         | event time (ms since epoch)              |
| `product_id`   | e.g. `PF_XBTUSD`                          |

Sources: [Futures Ticker feed](https://docs.kraken.com/api/docs/futures-api/websocket/ticker/),
[Futures WebSockets guide](https://docs.kraken.com/api/docs/guides/futures-websockets/).

**Product:** `PF_XBTUSD` — the linear, USD-collateralised perpetual, so P&L is in
USD (clean accounting). Configurable via `--product`. (`PI_XBTUSD` is the inverse
perp; out of scope.)

## 3. Architecture

```
futures.kraken.com:443
        │ TLS
   [ stunnel ]  127.0.0.1:8443→futures host  (new conf section)
        │ plaintext TCP
  kraken-feed --futures PF_XBTUSD
        │  hand-rolled WS  →  subscribe public ticker
        │  parse {bid,ask,markPrice,funding_rate,time}
        │  build v4 packet (49 B)
        ▼  UDP :34254
  trading-engine ingestor
        │  parse v4 (amt>=49) → MarketTick{price,…,bid,ask,mark_price,funding_rate}
        ▼
  report/JSON: observed spread (bps), funding rate   ← SP1 visible deliverable
```

### 3.1 Adapter — new `--futures` mode (`src/bin/kraken-feed.rs`)
- New CLI: `--futures [PRODUCT]` (default `PF_XBTUSD`), `--product` alias. Connects
  to the futures stunnel listener; reuses the existing hand-rolled WS handshake,
  RFC6455 framing, ping/pong, and masking. Additive — does not touch `--live`
  (spot), `--synth`, `--replay`.
- Subscribes the public `ticker` feed for the product; on each ticker message,
  parses the needed fields and sends one v4 UDP packet.
- `--record` works as today (records the v4 packets + inter-arrival timing).

### 3.2 Futures ticker parser (hand-rolled, no serde)
- Input: a WS text frame holding a JSON object. Extract `bid`, `ask`, `markPrice`,
  `funding_rate`, `time` by scanning for each `"key":` and parsing the following
  number (reusing/extending the existing number-parse helpers).
- `funding_rate` **absent → 0.0** (docs: omitted when zero).
- Non-ticker frames (subscription acks, `info`, heartbeats) are ignored.
- **price reference:** the v4 packet's `price` field = **mid = (bid+ask)/2** (the
  executable reference the signal reads). `mark_price` is carried separately for
  funding/liquidation use in SP3.

### 3.3 Packet v4 (49 bytes, little-endian)
Extends v3 (33 B); first 33 bytes byte-identical, so v1/v2/v3 senders stay valid.

```
[ 0.. 4] price f32        (= mid for futures)
[ 4.. 8] volume f32       (0 for ticker; trade size later)
[ 8..16] sequence u64
[16..24] origin_ts_ns     (ticker `time`, ms→ns)
[24..32] transit_est_ns   (RTT/2)
[32..33] instrument u8    (v3)
[33..37] bid f32          ┐
[37..41] ask f32          │ v4 additions
[41..45] mark_price f32   │
[45..49] funding_rate f32 ┘
```
New const `INGEST_PACKET_SIZE_V4 = 49`. The ingestor parses the v4 tail only when
`amt >= 49`.

### 3.4 `MarketTick` — fields into the existing padding (stays 64 B)
Replace `_unused: [u8; 20]` (offset 40) with:
```
bid:          f32,  // offset 40
ask:          f32,  // offset 44
mark_price:   f32,  // offset 48
funding_rate: f32,  // offset 52
_unused:      [u8; 8], // offset 56 → 64 total
```
Single-writer (ingestor) semantics unchanged; the `assert!(size_of::<MarketTick>()==64)`
still holds (invariant #1). Carrying all four on the tick avoids extra atomic
plumbing and keeps the lock-free protocol intact.

### 3.5 Engine reporting (the visible deliverable)
- The ingestor (sole writer of the observed-range fields today) also tracks
  **observed spread**: min/mean/max of `(ask-bid)/mid` in bps, and the **latest
  funding rate**.
- Shutdown console + JSON gain a small **market block**: spread bps (min/mean/max)
  and current funding rate (+ as an annualised %, informational). No trading
  behavior changes.

### 3.6 Synth + replay (offline, deterministic)
- `run_synth` additionally emits, per tick, a synthetic spread (a few bps around
  mid, optionally vol-scaled) and a synthetic funding rate (small, slowly varying,
  seeded), written into the v4 packet. Deterministic (existing LCG).
- Replay re-emits v4 packets unchanged (the `.krkr` format already length-prefixes
  each packet; a larger packet is transparent). Old captures (v2/v3) still replay.

### 3.7 stunnel
- Add a documented futures section to `docs/stunnel.conf` (or a sibling
  `docs/stunnel-futures.conf`): `connect = futures.kraken.com:443`,
  `accept = 127.0.0.1:8443` (or a distinct port if running both spot + futures).

## 4. Error handling
- WS connect / handshake / subscribe failures → **fail loudly** with the cause
  (consistent with the spot live path; no silent fallback).
- Malformed / partial ticker JSON → skip that frame, count it, continue (don't
  crash the stream on one bad message).
- `bid`/`ask` missing or non-positive → skip emitting that tick (can't form a mid/
  spread); count skips so an empty run is explained.
- Subscription ack / error frames are logged; an explicit subscribe **error** from
  Kraken (e.g. bad product) → fail loudly with the message.

## 5. Testing (TDD the new pure logic)
- **Futures ticker parse:** golden message (from the docs) → `{bid,ask,mark,funding,time}`;
  a variant with `funding_rate` omitted → funding 0.0; a non-ticker frame → ignored.
- **v4 codec:** `build_packet_v4(...)` → bytes → parse → fields round-trip; a v3
  (33 B) buffer still parses (tail defaults to 0); `size==49`.
- **spread-bps:** `(ask-bid)/mid*1e4` for known bid/ask; mid computation.
- **MarketTick size:** the existing compile-time `assert!(size_of==64)` guards the
  layout change.
- Engine integration is covered by an offline `--synth → replay` smoke run showing
  the market block populated (manual/CI, not a unit test).

## 6. Invariants (CLAUDE.md) — touched & preserved
- **#1 MarketTick stays 64 B** — preserved (new fields consume `_unused`, assert holds).
- **#12 v-packet compatibility** — extended: v4 = 49 B, first 33 B identical; ingestor
  parses the tail only at `amt>=49`; v1/v2/v3 remain valid.
- **#13 stunnel terminates TLS; adapter is plaintext** — preserved (futures via stunnel).
- **#3 single writer of `latest_idx`**, **#8 Acquire/Release** — unchanged.

## 7. Risks & open questions (validate during implementation, before building out)
1. **Live ticker JSON shape** — confirm exact field names/types against a real
   `futures.kraken.com` ticker frame *first* (a connectivity spike) before writing
   the parser to a doc-derived example.
2. **Funding units/sign** — `funding_rate` magnitude/sign convention (per-interval
   vs annualised; long-pays-short sign) is **carried as-reported in SP1**; its
   interpretation for accrual is an SP3 concern, flagged there.
3. **Tick rate** — `ticker` is throttled ~1 s, much slower than the spot trade
   feed. Fine for SP1 (data-in + visible). If a higher rate is needed for strategy
   testing, adding the futures `trade` feed is an SP2 consideration, noted not done.
4. **Running spot + futures together** — if both stunnel listeners run at once they
   need distinct ports; documented in the conf.

## 8. Out of scope (later sub-projects)
- Consuming the spread in the fill model (SP2).
- Funding accrual over hold time + mark-price liquidation / perp contract re-model (SP3).
- Richer risk metrics (SP4); smarter sizing (SP5).
- The futures `trade` feed, full L2 book, private/auth feeds, real order submission.
- README rewrite for the futures engine (after the behavior-changing sub-projects land).
