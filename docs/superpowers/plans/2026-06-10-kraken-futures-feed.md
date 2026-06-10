# Kraken Futures Feed (SP1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring real Kraken Futures public-ticker data (bid/ask, mark price, funding rate) into the engine over the existing hand-rolled WebSocket with no auth, and surface it in the shutdown report — no trading-behavior change.

**Architecture:** A new `--futures` mode in `kraken-feed` subscribes the public `ticker` feed on `futures.kraken.com`, hand-parses the JSON, and emits a 49-byte **v4** packet (v3 + bid/ask/mark/funding). The engine ingestor stores the four f32s in `MarketTick`'s existing 20-byte padding (struct stays 64 B) and the report/JSON show observed spread (bps) + current funding. The synth emits a synthetic spread/funding so offline replay stays deterministic.

**Tech Stack:** Rust 2024, zero external deps, std-only. Tests via `cargo test` (unit tests live in `src/bin/kraken-feed.rs`'s `#[cfg(test)] mod tests`).

**Spec:** `docs/superpowers/specs/2026-06-10-kraken-futures-feed-design.md`
**Branch:** `claude/kraken-futures-feed`

---

## File map

| File | Responsibility | Change |
|------|----------------|--------|
| `src/lib.rs` | config consts | add `INGEST_PACKET_SIZE_V4`, futures host/addr/product consts |
| `src/models.rs` | `MarketTick`, `OrderBook` | add 4 f32 fields to tick (in padding); add spread/funding atomics to OrderBook |
| `src/main.rs` | OrderBook construction | init the new OrderBook atomics |
| `src/bin/kraken-feed.rs` | adapter | `build_packet_v4`, `parse_futures_ticker`, `json_num`, `mid`/`spread_bps`, `run_futures`, `--futures` CLI, synth→v4 |
| `src/engine.rs` | ingestor + report | parse v4 tail, track spread/funding, print + JSON the market block |
| `docs/stunnel.conf` | TLS terminator | add a futures service section |

---

## Task 1: v4 packet const + MarketTick layout (struct stays 64 B)

**Files:**
- Modify: `src/lib.rs` (near `INGEST_PACKET_SIZE_V3`, ~line 57)
- Modify: `src/models.rs:38` (the `_unused: [u8; 20]` field)

- [ ] **Step 1: Add the v4 size + futures consts in `src/lib.rs`**

Find the line `pub const INGEST_PACKET_SIZE_V3: usize = 33;` and add directly below it:

```rust
    pub const INGEST_PACKET_SIZE_V4: usize = 49;  // v3 (33) + bid/ask/mark/funding (4×f32)
```

Find the futures/kraken host block (where `KRAKEN_HOST`, `STUNNEL_ADDR`, `API_STUNNEL_ADDR` live, ~lines 63-68) and add below `API_STUNNEL_ADDR`:

```rust
    pub const KRAKEN_FUTURES_HOST: &str = "futures.kraken.com";
    pub const FUTURES_STUNNEL_ADDR: &str = "127.0.0.1:8445"; // distinct from spot 8443 / api 8444
    pub const KRAKEN_FUTURES_PRODUCT: &str = "PF_XBTUSD";     // linear USD-collateral perp
```

- [ ] **Step 2: Replace `MarketTick`'s padding with the four futures fields**

In `src/models.rs`, replace the single padding line (currently `    _unused: [u8; 20],               // offset 40 — padding to 64 bytes`) with:

```rust
    pub(crate) bid:            f32,  // offset 40 — best bid (v4; 0 if not provided)
    pub(crate) ask:            f32,  // offset 44 — best ask (v4)
    pub(crate) mark_price:     f32,  // offset 48 — perp mark price (v4)
    pub(crate) funding_rate:   f32,  // offset 52 — current funding rate (v4)
    _unused: [u8; 8],                // offset 56 — padding to 64 bytes
}
```

(Delete the old `_unused: [u8; 20],` and the closing `}` it preceded — the block above already closes the struct.)

- [ ] **Step 3: Build — the existing compile-time assert verifies 64 B**

Run: `cargo build --release 2>&1 | tail -5`
Expected: compiles clean. (`src/models.rs` has `const _: () = assert!(std::mem::size_of::<MarketTick>() == 64);` — a layout regression would fail the build here. Invariant #1 preserved.)

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs src/models.rs
git commit -m "Add v4 packet const + MarketTick bid/ask/mark/funding fields (stays 64B)"
```

---

## Task 2: `build_packet_v4` (TDD)

**Files:**
- Modify: `src/bin/kraken-feed.rs` (add fn after `build_packet`, ~line 63; add test in `mod tests`)

- [ ] **Step 1: Write the failing test**

Add inside `mod tests` (after `frame_roundtrip`, before the closing `}`):

```rust
    #[test]
    fn build_v4_packet_layout() {
        let p = build_packet_v4(60000.0, 0.0, 7, 1_700_000_000_000_000_000, 1234, 0,
                                59995.0, 60005.0, 60001.0, 1.2e-7);
        assert_eq!(p.len(), 49);
        // First 33 bytes are byte-identical to a v3 packet.
        let v3 = build_packet(60000.0, 0.0, 7, 1_700_000_000_000_000_000, 1234, 0);
        assert_eq!(&p[..33], &v3[..]);
        // v4 tail: bid/ask/mark/funding as little-endian f32.
        assert_eq!(f32::from_le_bytes(p[33..37].try_into().unwrap()), 59995.0);
        assert_eq!(f32::from_le_bytes(p[37..41].try_into().unwrap()), 60005.0);
        assert_eq!(f32::from_le_bytes(p[41..45].try_into().unwrap()), 60001.0);
        assert_eq!(f32::from_le_bytes(p[45..49].try_into().unwrap()), 1.2e-7);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin kraken-feed build_v4_packet_layout 2>&1 | tail -15`
Expected: FAIL — `cannot find function build_packet_v4`.

- [ ] **Step 3: Write the implementation**

Add directly after the existing `build_packet` fn (after line 63):

```rust
/// Build the 49-byte v4 packet: the 33-byte v3 layout plus bid/ask/mark_price/
/// funding_rate as four little-endian f32s. The first 33 bytes are byte-identical
/// to v3, so older ingestors stay valid (they parse only what they understand).
#[allow(clippy::too_many_arguments)]
fn build_packet_v4(
    price: f32, volume: f32, seq: u64, origin_ts_ns: u64, transit_est_ns: u64,
    instrument: u8, bid: f32, ask: f32, mark_price: f32, funding_rate: f32,
) -> [u8; 49] {
    let mut p = [0u8; 49];
    p[..33].copy_from_slice(&build_packet(price, volume, seq, origin_ts_ns, transit_est_ns, instrument));
    p[33..37].copy_from_slice(&bid.to_le_bytes());
    p[37..41].copy_from_slice(&ask.to_le_bytes());
    p[41..45].copy_from_slice(&mark_price.to_le_bytes());
    p[45..49].copy_from_slice(&funding_rate.to_le_bytes());
    p
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin kraken-feed build_v4_packet_layout 2>&1 | tail -8`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add src/bin/kraken-feed.rs
git commit -m "Add build_packet_v4 (v3 + bid/ask/mark/funding), TDD"
```

---

## Task 3: `json_num` + `parse_futures_ticker` (TDD)

**Files:**
- Modify: `src/bin/kraken-feed.rs` (add fns after `parse_kraken_ts`, ~line 260; add tests in `mod tests`)

- [ ] **Step 1: Write the failing tests**

Add inside `mod tests`:

```rust
    #[test]
    fn json_num_extracts_numbers() {
        let m = r#"{"a":12.5,"b":-3,"c":1.2e-7,"d":"x"}"#;
        assert_eq!(json_num(m, "a"), Some(12.5));
        assert_eq!(json_num(m, "b"), Some(-3.0));
        assert_eq!(json_num(m, "c"), Some(1.2e-7));
        assert_eq!(json_num(m, "missing"), None);
    }

    #[test]
    fn parse_futures_ticker_message() {
        let msg = r#"{"feed":"ticker","product_id":"PF_XBTUSD","bid":60234.0,"ask":60235.5,"markPrice":60234.8,"last":60234.5,"funding_rate":1.2e-7,"time":1718040000000}"#;
        let t = parse_futures_ticker(msg).expect("a ticker");
        assert!((t.0 - 60234.0).abs() < 0.01);   // bid
        assert!((t.1 - 60235.5).abs() < 0.01);   // ask
        assert!((t.2 - 60234.8).abs() < 0.01);   // mark
        assert!((t.3 - 1.2e-7).abs() < 1e-12);   // funding
        assert_eq!(t.4, 1_718_040_000_000_000_000); // time ms → ns
    }

    #[test]
    fn parse_futures_ticker_absent_funding_is_zero() {
        let msg = r#"{"feed":"ticker","product_id":"PF_XBTUSD","bid":60234.0,"ask":60235.5,"markPrice":60234.8,"time":1718040000000}"#;
        let t = parse_futures_ticker(msg).expect("a ticker");
        assert_eq!(t.3, 0.0);                    // funding omitted → 0.0
    }

    #[test]
    fn parse_futures_ticker_non_ticker_is_none() {
        assert!(parse_futures_ticker(r#"{"event":"subscribed","feed":"ticker"}"#).is_none());
        assert!(parse_futures_ticker(r#"{"feed":"heartbeat"}"#).is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin kraken-feed futures_ticker 2>&1 | tail -15`
Expected: FAIL — `cannot find function parse_futures_ticker` / `json_num`.

- [ ] **Step 3: Write the implementation**

Add after `parse_kraken_ts` (after line 260):

```rust
/// Extract the JSON number that follows `"key":` in `msg` (handles integers,
/// decimals, and scientific notation). `None` if the key is absent or the value
/// is not a bare number (e.g. a string). Good enough for Kraken's flat ticker
/// objects — not a general JSON parser.
fn json_num(msg: &str, key: &str) -> Option<f64> {
    let pat = format!("\"{key}\":");
    let start = msg.find(&pat)? + pat.len();
    let rest = msg[start..].trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E')))
        .unwrap_or(rest.len());
    rest[..end].parse::<f64>().ok()
}

/// Parse a Kraken **Futures** public `ticker` message → `(bid, ask, mark_price,
/// funding_rate, origin_ts_ns)`. The feed is a flat JSON object with `bid`, `ask`,
/// `markPrice`, `funding_rate` (omitted when zero → defaulted to 0.0), and `time`
/// (ms since epoch). Returns `None` for any frame that is not a populated ticker
/// (subscription acks, heartbeats, or missing bid/ask).
fn parse_futures_ticker(msg: &str) -> Option<(f32, f32, f32, f32, u64)> {
    if !msg.contains("\"feed\":\"ticker\"") || !msg.contains("\"bid\":") {
        return None;
    }
    let bid = json_num(msg, "bid")? as f32;
    let ask = json_num(msg, "ask")? as f32;
    if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0) {
        return None;
    }
    let mark = json_num(msg, "markPrice").map(|v| v as f32).unwrap_or((bid + ask) / 2.0);
    let funding = json_num(msg, "funding_rate").map(|v| v as f32).unwrap_or(0.0);
    let origin_ns = json_num(msg, "time").map(|ms| (ms as u64).wrapping_mul(1_000_000)).unwrap_or(0);
    Some((bid, ask, mark, funding, origin_ns))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin kraken-feed futures_ticker 2>&1 | tail -8 && cargo test --bin kraken-feed json_num 2>&1 | tail -5`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/bin/kraken-feed.rs
git commit -m "Add json_num + parse_futures_ticker (Kraken Futures ticker), TDD"
```

---

## Task 4: `mid` + `spread_bps` helpers (TDD)

**Files:**
- Modify: `src/bin/kraken-feed.rs` (add fns after `parse_futures_ticker`; add test in `mod tests`)

- [ ] **Step 1: Write the failing test**

Add inside `mod tests`:

```rust
    #[test]
    fn mid_and_spread_bps() {
        assert!((mid(59995.0, 60005.0) - 60000.0).abs() < 0.01);
        // spread = 10 / 60000 * 1e4 ≈ 1.667 bps
        assert!((spread_bps(59995.0, 60005.0) - 1.6667).abs() < 0.01);
        assert_eq!(spread_bps(0.0, 0.0), 0.0);   // degenerate → 0, no NaN
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin kraken-feed mid_and_spread_bps 2>&1 | tail -12`
Expected: FAIL — `cannot find function mid` / `spread_bps`.

- [ ] **Step 3: Write the implementation**

Add after `parse_futures_ticker`:

```rust
/// Mid price from best bid/ask.
fn mid(bid: f32, ask: f32) -> f32 { (bid + ask) / 2.0 }

/// Bid/ask spread in basis points of the mid. 0.0 for a degenerate quote.
fn spread_bps(bid: f32, ask: f32) -> f32 {
    let m = mid(bid, ask);
    if m > 0.0 { (ask - bid) / m * 10_000.0 } else { 0.0 }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin kraken-feed mid_and_spread_bps 2>&1 | tail -8`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/bin/kraken-feed.rs
git commit -m "Add mid + spread_bps helpers, TDD"
```

---

## Task 5: `run_futures` adapter + `--futures` CLI

**Files:**
- Modify: `src/bin/kraken-feed.rs` (import consts; add `run_futures` after `run_live`; CLI parse + dispatch; usage)

- [ ] **Step 1: Add the new consts to the import block**

At the top `use rust_hft_software::config::{ ... };` block (lines 41-44), add `FUTURES_STUNNEL_ADDR, KRAKEN_FUTURES_HOST, KRAKEN_FUTURES_PRODUCT,` to the import list.

- [ ] **Step 2: Add `run_futures` after `run_live` (after line 624)**

```rust
/// Stream the Kraken **Futures** public `ticker` feed (bid/ask/mark/funding) for
/// `product` and emit v4 packets. Public feed → no auth (challenge/sign is only
/// for private order feeds). Mirrors `run_live` but parses the futures ticker and
/// sets the packet price to the mid.
fn run_futures(
    endpoint: &str,
    product: &str,
    ingestor: &str,
    record: Option<&str>,
) -> io::Result<()> {
    println!("[kraken-feed] connecting to {endpoint} (stunnel → {KRAKEN_FUTURES_HOST}:443)");
    let mut stream = TcpStream::connect(endpoint)?;
    stream.set_nodelay(true).ok();
    let mut acc = ws_handshake(&mut stream, KRAKEN_FUTURES_HOST)?;
    println!("[kraken-feed] websocket connected; subscribing to futures ticker {product}");
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;

    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let mut recorder = match record {
        Some(p) => Some(Recorder::create(p)?),
        None => None,
    };

    let sub = format!(
        "{{\"event\":\"subscribe\",\"feed\":\"ticker\",\"product_ids\":[\"{product}\"]}}"
    );
    stream.write_all(&build_frame(0x1, sub.as_bytes()))?;

    let start = Instant::now();
    let mut seq: u64 = 1;
    let mut transit_est: u64 = 0;
    stream.write_all(&build_frame(0x9, &start.elapsed().as_nanos().to_le_bytes()[..8]))?;
    let mut last_ping = Instant::now();
    let mut tmp = [0u8; 8192];

    loop {
        while let Some((opcode, payload, consumed)) = parse_frame(&acc) {
            acc.drain(..consumed);
            match opcode {
                0x1 => {
                    let msg = String::from_utf8_lossy(&payload);
                    if let Some((bid, ask, mark, funding, origin_ns)) = parse_futures_ticker(&msg) {
                        let pkt = build_packet_v4(
                            mid(bid, ask), 0.0, seq, origin_ns, transit_est, 0,
                            bid, ask, mark, funding,
                        );
                        udp.send_to(&pkt, ingestor)?;
                        if let Some(r) = recorder.as_mut() { r.write(&pkt)?; }
                        seq += 1;
                    }
                }
                0x9 => { stream.write_all(&build_frame(0xA, &payload))?; }
                0xA => {
                    if payload.len() >= 8 {
                        let sent = u64::from_le_bytes(payload[..8].try_into().unwrap());
                        let rtt = (start.elapsed().as_nanos() as u64).saturating_sub(sent);
                        transit_est = rtt / 2;
                    }
                }
                0x8 => { println!("[kraken-feed] server closed connection"); return Ok(()); }
                _ => {}
            }
        }

        match stream.read(&mut tmp) {
            Ok(0) => { println!("[kraken-feed] connection closed"); return Ok(()); }
            Ok(n) => acc.extend_from_slice(&tmp[..n]),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock
                       || e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }

        if last_ping.elapsed().as_millis() as u64 >= RTT_PING_INTERVAL_MS {
            stream.write_all(&build_frame(0x9, &start.elapsed().as_nanos().to_le_bytes()[..8]))?;
            last_ping = Instant::now();
        }
    }
}
```

- [ ] **Step 3: Add CLI parsing + dispatch in `main`**

In `main()`, add these locals next to the others (after `let mut api = ...;`, ~line 640):

```rust
    let mut futures: Option<String> = None;
    let mut futures_endpoint = FUTURES_STUNNEL_ADDR.to_string();
```

In the `while i < args.len()` match, add two arms (before `"--help"`):

```rust
            "--futures"  => { futures = Some(args.get(i + 1).filter(|v| !v.starts_with("--")).cloned().unwrap_or_else(|| KRAKEN_FUTURES_PRODUCT.to_string())); if args.get(i + 1).map(|v| !v.starts_with("--")).unwrap_or(false) { i += 1; } }
            "--product"  => { if let Some(v) = args.get(i + 1) { futures = Some(v.clone()); i += 1; } }
            "--futures-endpoint" => { if let Some(v) = args.get(i + 1) { futures_endpoint = v.clone(); i += 1; } }
```

In the dispatch chain (the `let result = if history { ... }` block, ~line 662), add a `futures` branch **first** (so it takes priority when set):

```rust
    let result = if let Some(product) = futures.as_deref() {
        run_futures(&futures_endpoint, product, &ingestor, record.as_deref())
    } else if history {
        run_history(&api, &pair, &ref_pair, hours, &out)
    } else if let Some(path) = synth {
```

(Leave the rest of the chain unchanged.)

- [ ] **Step 4: Add the futures line to `print_usage`**

In `print_usage`, add a line after the `--synth` usage line:

```rust
         \x20 kraken-feed --futures [PRODUCT] [--futures-endpoint HOST:PORT] [--ingestor ADDR] [--record FILE]\n\
```

- [ ] **Step 5: Build + usage check**

Run: `cargo build --release 2>&1 | tail -5 && ./target/release/kraken-feed --help 2>&1 | grep futures`
Expected: builds clean; the `--futures` usage line prints.

- [ ] **Step 6: Commit**

```bash
git add src/bin/kraken-feed.rs src/lib.rs
git commit -m "Add --futures mode: Kraken Futures public ticker → v4 packets"
```

---

## Task 6: synth emits a v4 packet with synthetic spread + funding

**Files:**
- Modify: `src/bin/kraken-feed.rs` `run_synth` (the per-tick emit loop, lines 420-426)

- [ ] **Step 1: Replace the synth emit loop to use v4 with a synthetic spread + funding**

Replace the `for (price, vol, id) in [...] { ... }` block (lines 421-426) with:

```rust
        // Synthetic microstructure: a small spread around mid (≈1.5 bps) and a tiny
        // slowly-varying funding rate, so offline replay/backtest exercise the v4
        // fields deterministically. Funding only meaningful for the traded perp.
        let funding = (0.000_01 * (seq as f64 * 0.01).sin()) as f32;
        for (price, vol, id) in [(ref_price, ref_vol, 1u8), (traded_price, traded_vol, 0u8)] {
            let half = (price * 0.000_075) as f32;         // ≈0.75 bps each side → ~1.5 bps spread
            let bid = price as f32 - half;
            let ask = price as f32 + half;
            let pkt = build_packet_v4(
                price as f32, vol, seq, origin, transit, id,
                bid, ask, price as f32, if id == 0 { funding } else { 0.0 },
            );
            rec.file.write_all(&2_500_000u64.to_le_bytes())?;
            rec.file.write_all(&(pkt.len() as u16).to_le_bytes())?;
            rec.file.write_all(&pkt)?;
        }
```

- [ ] **Step 2: Build + verify synth emits 49-byte packets**

Run:
```bash
cargo build --release 2>&1 | tail -3
./target/release/kraken-feed --synth /tmp/v4.krkr
python3 -c "d=open('/tmp/v4.krkr','rb').read(); import struct; off=5; ln=struct.unpack('<H',d[off+8:off+10])[0]; print('first packet len =', ln)"
```
Expected: builds; prints `first packet len = 49`.

- [ ] **Step 3: Commit**

```bash
git add src/bin/kraken-feed.rs
git commit -m "Synth emits v4 packets with synthetic spread + funding"
```

---

## Task 7: OrderBook spread/funding atomics + init

**Files:**
- Modify: `src/models.rs` (OrderBook fields, after line 252)
- Modify: `src/main.rs:173` (OrderBook literal init)

- [ ] **Step 1: Add three atomics to `OrderBook`**

In `src/models.rs`, immediately after the `price_hi_bits` field (line 252), add:

```rust
    pub(crate) spread_lo_bits: AtomicU32,       // min observed spread (f32 bps bits); sole writer: ingestor
    pub(crate) spread_hi_bits: AtomicU32,       // max observed spread (f32 bps bits); sole writer: ingestor
    pub(crate) funding_bits:   AtomicU32,       // latest funding rate (f32 bits); sole writer: ingestor
```

- [ ] **Step 2: Initialize them in the OrderBook literal**

In `src/main.rs`, find `price_lo_bits: AtomicU32::new(f32::INFINITY.to_bits()),` (line 173) and the `price_hi_bits` line after it; add directly below them:

```rust
        spread_lo_bits: AtomicU32::new(f32::INFINITY.to_bits()),
        spread_hi_bits: AtomicU32::new(f32::NEG_INFINITY.to_bits()),
        funding_bits:   AtomicU32::new(0f32.to_bits()),
```

(Match the surrounding indentation. `price_hi_bits` initializes to `f32::NEG_INFINITY.to_bits()` — mirror that for `spread_hi_bits`.)

- [ ] **Step 3: Build**

Run: `cargo build --release 2>&1 | tail -5`
Expected: compiles clean (no missing-field error on the OrderBook literal).

- [ ] **Step 4: Commit**

```bash
git add src/models.rs src/main.rs
git commit -m "Add OrderBook spread/funding observed-market atomics"
```

---

## Task 8: ingestor parses v4 tail + tracks spread/funding

**Files:**
- Modify: `src/engine.rs` `run_ingestor` (the unsafe tick-write block, lines 330-346; the `id == 0` tracking block, lines 313-324)

- [ ] **Step 1: Write the v4 tail into the tick**

In the `unsafe { ... }` block (lines 330-346), after the existing `if amt >= 32 { ... } else { ... }` that writes offsets 24 and 32, add:

```rust
                    // v4 packets (>= 49 bytes) carry bid/ask/mark/funding at [33..49].
                    if amt >= 49 {
                        *(tick_ptr.add(40) as *mut f32) = f32::from_le_bytes(pkt[33..37].try_into().unwrap());
                        *(tick_ptr.add(44) as *mut f32) = f32::from_le_bytes(pkt[37..41].try_into().unwrap());
                        *(tick_ptr.add(48) as *mut f32) = f32::from_le_bytes(pkt[41..45].try_into().unwrap());
                        *(tick_ptr.add(52) as *mut f32) = f32::from_le_bytes(pkt[45..49].try_into().unwrap());
                    } else {
                        *(tick_ptr.add(40) as *mut f32) = 0.0;
                        *(tick_ptr.add(44) as *mut f32) = 0.0;
                        *(tick_ptr.add(48) as *mut f32) = 0.0;
                        *(tick_ptr.add(52) as *mut f32) = 0.0;
                    }
```

- [ ] **Step 2: Track observed spread + latest funding (traded instrument only)**

Inside the existing `if id == 0 { ... }` block (lines 314-324), after the price-range update (after the closing brace of `if px.is_finite() { ... }`, before the block's closing `}`), add:

```rust
                    if amt >= 49 {
                        let bid = f32::from_le_bytes(pkt[33..37].try_into().unwrap());
                        let ask = f32::from_le_bytes(pkt[37..41].try_into().unwrap());
                        let m = (bid + ask) / 2.0;
                        if m > 0.0 && bid > 0.0 && ask >= bid {
                            let sp = (ask - bid) / m * 10_000.0;  // spread in bps
                            if sp < f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed)) {
                                order_book.spread_lo_bits.store(sp.to_bits(), Ordering::Relaxed);
                            }
                            if sp > f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed)) {
                                order_book.spread_hi_bits.store(sp.to_bits(), Ordering::Relaxed);
                            }
                        }
                        let funding = f32::from_le_bytes(pkt[45..49].try_into().unwrap());
                        if funding.is_finite() {
                            order_book.funding_bits.store(funding.to_bits(), Ordering::Relaxed);
                        }
                    }
```

- [ ] **Step 3: Build**

Run: `cargo build --release 2>&1 | tail -5`
Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add src/engine.rs
git commit -m "Ingestor parses v4 tail + tracks observed spread/funding"
```

---

## Task 9: report + JSON market block

**Files:**
- Modify: `src/engine.rs` — trade-mode report (~line 1041), non-trade report (~line 1101), JSON (~line 1257)

- [ ] **Step 1: Add a helper to read the spread/funding (top of the shutdown report fn)**

In the shutdown report function, near where `lo`/`hi` are read (line 1014: `let lo = f32::from_bits(order_book.price_lo_bits...)`), add:

```rust
    let spread_lo = f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed));
    let spread_hi = f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed));
    let funding   = f32::from_bits(order_book.funding_bits.load(Ordering::Relaxed));
```

- [ ] **Step 2: Print the market block after the trade-mode price-range line (~line 1042)**

After the `println!("Observed price range: ...volatility...")` block (the one ending ~line 1042), add:

```rust
        if spread_lo.is_finite() && spread_hi.is_finite() {
            println!("Market data: spread {:.2}–{:.2} bps  |  funding {:.6}% (latest)",
                     spread_lo, spread_hi, funding * 100.0);
        }
```

- [ ] **Step 3: Print the market block after the non-trade price-range line (~line 1101)**

After `println!("Observed price range: [{:.4}, {:.4}]  ({:.2} bps span)", lo, hi, range_bps);` add:

```rust
    if spread_lo.is_finite() && spread_hi.is_finite() {
        println!("Market data: spread {:.2}–{:.2} bps  |  funding {:.6}% (latest)",
                 spread_lo, spread_hi, funding * 100.0);
    }
```

- [ ] **Step 4: Add the market block to the JSON (~line 1257)**

In `write_log`, after the `price_range` JSON block (the `if lo.is_finite() ... else ...` at lines 1256-1260), add:

```rust
    let spread_lo = f32::from_bits(order_book.spread_lo_bits.load(Ordering::Relaxed));
    let spread_hi = f32::from_bits(order_book.spread_hi_bits.load(Ordering::Relaxed));
    let funding   = f32::from_bits(order_book.funding_bits.load(Ordering::Relaxed));
    if spread_lo.is_finite() && spread_hi.is_finite() {
        json.push_str(&format!("  \"spread_bps\": {{\"min\": {}, \"max\": {}}},\n", spread_lo, spread_hi));
    } else {
        json.push_str("  \"spread_bps\": null,\n");
    }
    json.push_str(&format!("  \"funding_rate\": {},\n", funding));
```

- [ ] **Step 5: Build**

Run: `cargo build --release 2>&1 | tail -5`
Expected: compiles clean.

- [ ] **Step 6: Commit**

```bash
git add src/engine.rs
git commit -m "Report + JSON: observed spread (bps) + latest funding market block"
```

---

## Task 10: stunnel futures config + offline smoke test

**Files:**
- Modify: `docs/stunnel.conf` (add a futures service section)

- [ ] **Step 1: Add the futures service to `docs/stunnel.conf`**

Append:

```ini
# Kraken Futures public market data (no auth). Run alongside the spot service on a
# distinct port. kraken-feed --futures speaks plaintext to this; TLS terminates here.
[kraken-futures]
client = yes
accept = 127.0.0.1:8445
connect = futures.kraken.com:443
sni = futures.kraken.com
verifyChain = no
```

- [ ] **Step 2: Offline end-to-end smoke test (the SP1 visible deliverable)**

Run:
```bash
pkill -f 'target/release/trading-engine' 2>/dev/null; sleep 0.4
./target/release/kraken-feed --synth /tmp/v4.krkr
env HFT_EXTERNAL_FEED=1 ./target/release/trading-engine > /tmp/sp1.log 2>&1 &
ENG=$!; sleep 1.3
./target/release/kraken-feed --replay /tmp/v4.krkr >/dev/null 2>&1
wait "$ENG" 2>/dev/null || true
grep -E 'Market data: spread|Observed price range' /tmp/sp1.log
LOG=$(ls -t logs/v*/*/*.json | head -1); grep -E 'spread_bps|funding_rate' "$LOG"
```
Expected: console shows `Market data: spread ~1.50–1.50 bps  |  funding ...%`; JSON shows `"spread_bps": {"min": ..., "max": ...}` and `"funding_rate": ...`.

- [ ] **Step 3: Full test suite + clippy**

Run: `cargo test 2>&1 | tail -6 && cargo clippy --release 2>&1 | tail -3`
Expected: all tests pass (the original 8 + the new ~7); clippy clean.

- [ ] **Step 4: Commit**

```bash
git add docs/stunnel.conf
git commit -m "Add stunnel futures service + SP1 offline smoke test green"
```

---

## Live validation (manual, post-merge — needs network + stunnel)

Not a plan task (requires the live exchange), but the first thing to run before SP2:

```bash
stunnel docs/stunnel.conf &                       # starts the [kraken-futures] service on :8445
HFT_EXTERNAL_FEED=1 ./target/release/trading-engine &
./target/release/kraken-feed --futures PF_XBTUSD --record recordings/futures.krkr
```
Confirm: the engine's shutdown report shows a **non-zero, realistic** spread (sub-bp to a few bps on PF_XBTUSD) and a live funding rate. **This validates the doc-derived ticker field names against the real feed** (spec risk #1). If field names differ, fix `parse_futures_ticker` (Task 3) and re-run its unit tests with a captured real frame.

---

## Self-review notes

- **Spec coverage:** adapter+ticker (T3,T5) ✓; v4 packet (T1,T2) ✓; MarketTick 64 B (T1) ✓; engine ingest (T8) ✓; report/JSON market block (T9) ✓; synth spread/funding (T6) ✓; stunnel (T10) ✓; TDD on pure logic (T2,T3,T4) ✓; live-shape validation flagged (manual section) ✓.
- **Out of scope (unchanged):** no fill-model/funding-accrual/sizing/metrics — those are SP2–SP5; SP1 is data-in + visible only.
- **Type consistency:** `build_packet_v4` 10-arg signature is used identically in T5 (`run_futures`) and T6 (`run_synth`); `parse_futures_ticker` returns `(bid,ask,mark,funding,origin_ns)` consumed positionally in T5; tick offsets 40/44/48/52 match T1's field layout and T8's writes.
- **Invariants:** #1 (64 B, compile-assert in T1), #12 (v4=49 B, first 33 identical, parsed at `amt>=49` in T8), #13 (stunnel TLS, T10) — all preserved.
