//! `kraken-feed` — a pure zero-dependency live crypto feed adapter.
//!
//! Connects to the Kraken WebSocket v1 trade feed and re-emits each trade as the
//! engine's 32-byte v2 UDP packet (see `config::INGEST_PACKET_SIZE_V2`), so the
//! existing ingestor → strategy → exchange pipeline can react to real market data
//! and measure the full latency stack.
//!
//! ## Zero dependencies, TLS by proxy
//! Kraken requires `wss://` (TLS). Rather than link a TLS crate (which would break
//! the project's zero-dependency invariant), TLS is terminated by a local
//! **stunnel** instance and this adapter speaks plaintext TCP to it, implementing
//! the WebSocket protocol BY HAND: the HTTP/1.1 `Upgrade` handshake (with a
//! hand-rolled SHA-1 + base64 for `Sec-WebSocket-Accept`), RFC6455 frame parsing
//! and client-side masking, and ping/pong. See `CLAUDE.md` (invariant #13) and the
//! `docs/stunnel.conf` example.
//!
//! ## Transit measurement (RTT-based)
//! The adapter periodically sends a WebSocket ping carrying its monotonic send
//! time; the matching pong yields the round trip, and `transit_est_ns = RTT/2` is
//! stamped into every emitted packet as the one-way "distance from source"
//! estimate. This avoids cross-host clock comparison (the message's own timestamp
//! is carried too, but only as an informational cross-check).
//!
//! ## Modes
//!   --live [HOST:PORT]   connect via stunnel (default STUNNEL_ADDR) and stream
//!   --record FILE        (with --live) also capture packets + timing to FILE
//!   --replay FILE        re-emit a capture deterministically, offline, no network
//!   --synth [FILE]       fabricate a small capture for offline testing, then exit
//!   --pair SYMBOL        Kraken pair (default XBT/USD)
//!   --ingestor ADDR      engine ingestor address (default INGESTOR_ADDR)
//!
//! Warmup: the adapter streams real trades directly; the engine's existing
//! `current_seq > WARMUP_PACKETS` gate treats the first few trades as warmup
//! (run through the hot path but not logged), which also fills the price window.

use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpStream, UdpSocket};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rust_hft_software::config::{
    API_STUNNEL_ADDR, FUTURES_STUNNEL_ADDR, INGESTOR_ADDR, KRAKEN_API_HOST, KRAKEN_FUTURES_HOST,
    KRAKEN_FUTURES_PRODUCT, KRAKEN_HOST, KRAKEN_PAIR, KRAKEN_REF_PAIR, RECORD_PATH_DEFAULT,
    RTT_PING_INTERVAL_MS, STUNNEL_ADDR,
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const RECORD_MAGIC: &[u8; 4] = b"KRKR";
const RECORD_VERSION: u8 = 1;

// ── Packet ──────────────────────────────────────────────────────────────────

/// Build the engine's 33-byte v3 market-data packet (little-endian): the 32-byte
/// v2 layout plus a 1-byte instrument id at [32] (0 = traded, 1 = reference).
fn build_packet(price: f32, volume: f32, seq: u64, origin_ts_ns: u64, transit_est_ns: u64, instrument: u8) -> [u8; 33] {
    let mut p = [0u8; 33];
    p[0..4].copy_from_slice(&price.to_le_bytes());
    p[4..8].copy_from_slice(&volume.to_le_bytes());
    p[8..16].copy_from_slice(&seq.to_le_bytes());
    p[16..24].copy_from_slice(&origin_ts_ns.to_le_bytes());
    p[24..32].copy_from_slice(&transit_est_ns.to_le_bytes());
    p[32] = instrument;
    p
}

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

// ── base64 + SHA-1 (hand-rolled, no deps) ─────────────────────────────────────

fn base64_encode(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { T[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[(n & 63) as usize] as char } else { '=' });
    }
    out
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x6745_2301, 0xEFCD_AB89, 0x98BA_DCFE, 0x1032_5476, 0xC3D2_E1F0];
    let ml = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 { msg.push(0); }
    msg.extend_from_slice(&ml.to_be_bytes());

    for block in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([block[i * 4], block[i * 4 + 1], block[i * 4 + 2], block[i * 4 + 3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let tmp = a.rotate_left(5)
                .wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(wi);
            e = d; d = c; c = b.rotate_left(30); b = a; a = tmp;
        }
        h[0] = h[0].wrapping_add(a); h[1] = h[1].wrapping_add(b); h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d); h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for i in 0..5 { out[i * 4..i * 4 + 4].copy_from_slice(&h[i].to_be_bytes()); }
    out
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    if let Ok(mut f) = File::open("/dev/urandom")
        && f.read_exact(&mut v).is_ok()
    {
        return v;
    }
    // Fallback: SystemTime-seeded LCG. Sec-WebSocket-Key quality is not security
    // critical for a client (the server only echoes a derived accept value).
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0x9E37);
    for b in v.iter_mut() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *b = (seed >> 33) as u8;
    }
    v
}

// ── RFC6455 framing ───────────────────────────────────────────────────────────

/// Build a client→server frame (FIN set, payload masked, as the RFC requires).
fn build_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mask = random_bytes(4);
    let mut f = Vec::with_capacity(payload.len() + 14);
    f.push(0x80 | opcode);
    let len = payload.len();
    if len < 126 {
        f.push(0x80 | len as u8);
    } else if len <= 0xFFFF {
        f.push(0x80 | 126);
        f.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        f.push(0x80 | 127);
        f.extend_from_slice(&(len as u64).to_be_bytes());
    }
    f.extend_from_slice(&mask);
    for (i, &b) in payload.iter().enumerate() { f.push(b ^ mask[i & 3]); }
    f
}

/// Parse one frame from the front of `buf`. Returns `(opcode, payload, consumed)`
/// when a complete frame is present; `None` if more bytes are needed.
fn parse_frame(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 { return None; }
    let opcode = buf[0] & 0x0F;
    let masked = (buf[1] & 0x80) != 0;
    let len7 = (buf[1] & 0x7F) as usize;
    let mut off = 2;
    let payload_len = if len7 < 126 {
        len7
    } else if len7 == 126 {
        if buf.len() < 4 { return None; }
        let l = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        off = 4; l
    } else {
        if buf.len() < 10 { return None; }
        let mut l = 0u64;
        for &b in &buf[2..10] { l = (l << 8) | b as u64; }
        off = 10; l as usize
    };
    let mask = if masked {
        if buf.len() < off + 4 { return None; }
        let m = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
        off += 4; Some(m)
    } else { None };
    if buf.len() < off + payload_len { return None; }
    let mut payload = buf[off..off + payload_len].to_vec();
    if let Some(m) = mask {
        for (i, b) in payload.iter_mut().enumerate() { *b ^= m[i & 3]; }
    }
    Some((opcode, payload, off + payload_len))
}

// ── Kraken v1 trade parsing ───────────────────────────────────────────────────

/// Extract `(price, signed_volume, origin_ts_ns)` from a Kraken v1 trade message.
/// The shape is `[channelID, [["price","vol","time","side","ordType","misc"], ...],
/// "trade", "pair"]`. `signed_volume` is +vol for a buy (side "b") and −vol for a
/// sell (side "s") — the order-flow input. Non-trade frames yield an empty vec.
fn parse_trades(msg: &str) -> Vec<(f32, f32, u64)> {
    let mut out = Vec::new();
    if !msg.contains("\"trade\"") { return out; }
    let bytes = msg.as_bytes();
    // Start just inside the "[[" that opens the trades list.
    let mut i = match msg.find("[[") { Some(p) => p + 1, None => return out };
    let n = bytes.len();
    while i < n {
        while i < n && bytes[i] != b'[' { i += 1; }   // next trade entry
        if i >= n { break; }
        let mut j = i + 1;
        while j < n && bytes[j] != b']' { j += 1; }    // entries hold no nested arrays
        if j >= n { break; }
        let toks = quoted_tokens(&msg[i + 1..j]);
        if toks.len() >= 4
            && let (Ok(price), Ok(vol), Some(ts)) =
                (toks[0].parse::<f32>(), toks[1].parse::<f32>(), parse_kraken_ts(&toks[2]))
        {
            let signed = if toks[3].starts_with('s') { -vol } else { vol };
            out.push((price, signed, ts));
        }
        i = j + 1;
    }
    out
}

/// Extract the trading pair from a Kraken v1 trade frame's tail
/// (`...,"trade","XBT/USD"]` → `XBT/USD`). `None` for non-trade frames.
fn frame_pair(msg: &str) -> Option<String> {
    let i = msg.rfind("\"trade\"")?;
    quoted_tokens(&msg[i + 7..]).into_iter().next()
}

/// Collect the contents of double-quoted strings, in order.
fn quoted_tokens(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if in_q {
            match c {
                '\\' => { if let Some(&nc) = chars.peek() { cur.push(nc); chars.next(); } }
                '"'  => { out.push(std::mem::take(&mut cur)); in_q = false; }
                _    => cur.push(c),
            }
        } else if c == '"' {
            in_q = true;
        }
    }
    out
}

/// Parse a Kraken "seconds.microseconds" timestamp string to ns since epoch.
fn parse_kraken_ts(s: &str) -> Option<u64> {
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    let sec: u64 = sec.parse().ok()?;
    let mut frac9 = String::from(frac);
    while frac9.len() < 9 { frac9.push('0'); }
    frac9.truncate(9);
    let frac_ns: u64 = if frac9.is_empty() { 0 } else { frac9.parse().ok()? };
    Some(sec.wrapping_mul(1_000_000_000).wrapping_add(frac_ns))
}

// ── Kraken Futures ticker parsing ─────────────────────────────────────────────

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
    // The *relative* funding rate (per-hour fraction of spot) is the directly-usable
    // one for accrual; the absolute `funding_rate` is USD/contract/hr (needs contract
    // size). Omitted when zero → 0.0. Positive → longs pay shorts.
    let funding = json_num(msg, "relative_funding_rate").map(|v| v as f32).unwrap_or(0.0);
    let origin_ns = json_num(msg, "time").map(|ms| (ms as u64).wrapping_mul(1_000_000)).unwrap_or(0);
    Some((bid, ask, mark, funding, origin_ns))
}

/// Mid price from best bid/ask.
fn mid(bid: f32, ask: f32) -> f32 { (bid + ask) / 2.0 }

/// Bid/ask spread in basis points of the mid. 0.0 for a degenerate quote.
fn spread_bps(bid: f32, ask: f32) -> f32 {
    let m = mid(bid, ask);
    if m > 0.0 { (ask - bid) / m * 10_000.0 } else { 0.0 }
}

// ── WebSocket handshake ───────────────────────────────────────────────────────

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Perform the WebSocket opening handshake. Returns any bytes already read past
/// the response headers (the start of the frame stream).
fn ws_handshake(stream: &mut TcpStream, host: &str, path: &str) -> io::Result<Vec<u8>> {
    let key = base64_encode(&random_bytes(16));
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nOrigin: https://{host}\r\n\r\n"
    );
    stream.write_all(req.as_bytes())?;

    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "handshake: connection closed"));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&buf[..pos]);
            let first = headers.lines().next().unwrap_or("");
            if !first.contains(" 101") {
                return Err(io::Error::other(format!("handshake: expected 101, got: {first}")));
            }
            let expected = base64_encode(&sha1(format!("{key}{WS_GUID}").as_bytes()));
            let accept_ok = headers.lines().any(|l| {
                let l = l.to_ascii_lowercase();
                l.starts_with("sec-websocket-accept:") && l.contains(&expected.to_ascii_lowercase())
            });
            if !accept_ok {
                eprintln!("[kraken-feed] warning: Sec-WebSocket-Accept did not match (continuing)");
            }
            return Ok(buf[pos + 4..].to_vec());
        }
        if buf.len() > 16384 {
            return Err(io::Error::other("handshake: response headers too large"));
        }
    }
}

// ── Record / replay ───────────────────────────────────────────────────────────

struct Recorder {
    file: File,
    last: Instant,
    started: bool,
}

impl Recorder {
    fn create(path: &str) -> io::Result<Recorder> {
        if let Some(dir) = std::path::Path::new(path).parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = File::create(path)?;
        file.write_all(RECORD_MAGIC)?;
        file.write_all(&[RECORD_VERSION])?;
        Ok(Recorder { file, last: Instant::now(), started: false })
    }

    fn write(&mut self, pkt: &[u8]) -> io::Result<()> {
        let now = Instant::now();
        let delta_ns = if self.started { now.duration_since(self.last).as_nanos() as u64 } else { 0 };
        self.started = true;
        self.last = now;
        self.file.write_all(&delta_ns.to_le_bytes())?;
        self.file.write_all(&(pkt.len() as u16).to_le_bytes())?;
        self.file.write_all(pkt)?;
        Ok(())
    }
}

/// Replay a capture file to the ingestor, honoring the recorded inter-arrival
/// timing. No network — deterministic and offline.
fn run_replay(path: &str, ingestor: &str) -> io::Result<()> {
    let data = std::fs::read(path)?;
    if data.len() < 5 || &data[0..4] != RECORD_MAGIC {
        return Err(io::Error::other("replay: bad file magic"));
    }
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let mut i = 5usize;
    let mut count = 0u64;
    while i + 10 <= data.len() {
        let delta_ns = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
        let len = u16::from_le_bytes([data[i + 8], data[i + 9]]) as usize;
        i += 10;
        if i + len > data.len() { break; }
        let pkt = &data[i..i + len];
        i += len;
        sleep_ns(delta_ns);
        udp.send_to(pkt, ingestor)?;
        count += 1;
    }
    println!("[kraken-feed] replayed {count} packets from {path}");
    Ok(())
}

/// Fabricate a deterministic capture for offline testing. The price is a
/// **mean-reverting random walk with microstructure noise** (Ornstein-Uhlenbeck
/// pull toward a slowly drifting center + per-tick shocks), so per-tick moves are
/// a realistic few bps and the model's edge is genuinely uncertain — unlike a
/// pure sine, which mean-reversion trivially prints money on. Seeded LCG → fully
/// reproducible. Override the seed with HFT_SYNTH_SEED for a different path.
fn run_synth(path: &str) -> io::Result<()> {
    let mut rec = Recorder::create(path)?;
    // Per-market tick count (HFT_SYNTH_TICKS); default 3000. Use a large value for
    // a long "day"-length session, e.g. HFT_SYNTH_TICKS=100000.
    let n: u64 = std::env::var("HFT_SYNTH_TICKS").ok()
        .and_then(|s| s.trim().parse().ok()).unwrap_or(3000);

    let mut seed: u64 = std::env::var("HFT_SYNTH_SEED").ok()
        .and_then(|s| s.trim().parse().ok()).unwrap_or(0x5DEECE66D);
    // Uniform in [-1, 1) from a 48-bit LCG.
    let mut u = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 16) as f64 / (1u64 << 48) as f64) * 2.0 - 1.0
    };

    // Two correlated markets driven by a shared latent factor F (a trending random
    // walk, in bps). The REFERENCE (id 1, ~ETH scale) reacts to F now; the TRADED
    // market (id 0, ~BTC scale) reacts to F delayed by LAG ticks — so the reference
    // LEADS, giving the cross-market trend (basket) and lead-lag terms genuine
    // predictive value. Each market also gets idiosyncratic noise and flow ~ its
    // own recent move. Deterministic (seeded LCG).
    const LAG: usize = 5;
    let traded_base = 60_000.0_f64;
    let ref_base    = 3_000.0_f64;
    let mut f: f64 = 0.0;          // latent factor, cumulative bps
    let mut trend: f64 = 0.0;      // slowly meandering drift of F (bps/tick)
    let mut f_hist = [0.0f64; LAG + 1];
    let mut traded_prev = traded_base;
    let mut ref_prev = ref_base;

    for seq in 1..=n {
        // Persistent trend drift (AR(1), half-life ~70 ticks): directional regimes
        // come and go and persist long enough to ride — realistic trend + chop,
        // spread across the whole capture so both IS and OOS contain trends.
        trend = trend * 0.99 + 0.2 * u();
        f += trend + 5.0 * u();
        for k in (1..=LAG).rev() { f_hist[k] = f_hist[k - 1]; }
        f_hist[0] = f;

        let ref_price    = ref_base    * (1.0 + f / 10_000.0)          + 0.6 * u();
        let traded_price = traded_base * (1.0 + f_hist[LAG] / 10_000.0) + 8.0 * u();
        let ref_vol    = ((ref_price - ref_prev)    * 0.5  + 0.3 * u()) as f32;
        let traded_vol = ((traded_price - traded_prev) * 0.02 + 1.0 * u()) as f32;
        ref_prev = ref_price;
        traded_prev = traded_price;

        let transit = 33_000_000 + (seq.wrapping_mul(2_654_435) % 8_000_000);
        let origin = 1_700_000_000_000_000_000u64.wrapping_add(seq.wrapping_mul(5_000_000));
        // Emit reference (id 1) then traded (id 0), 2.5 ms apart (≈5 ms/round).
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
    }
    println!("[kraken-feed] wrote {n}×2 synthetic packets (two correlated markets, reference leads) to {path}");
    Ok(())
}

// ── Historical data collection (Kraken REST /0/public/Trades) ──────────────────
// Pulls past trades over `hours` and writes a .krkr the backtester/replay can use.
// TLS is terminated by a second stunnel service → api.kraken.com:443 (HTTP/1.0 +
// Connection-close so the body is a clean read-to-EOF, no chunked decoding).

/// One-shot HTTP/1.0 GET; returns the response body (headers stripped).
fn http_get(endpoint: &str, path: &str, host: &str) -> io::Result<String> {
    let mut s = TcpStream::connect(endpoint)?;
    s.set_read_timeout(Some(Duration::from_secs(20)))?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nUser-Agent: hft-engine\r\nAccept: */*\r\n\r\n");
    s.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    Ok(match text.find("\r\n\r\n") { Some(p) => text[p + 4..].to_string(), None => text })
}

/// Parse a Kraken `Trades` JSON body → (trades as (price, signed_vol, time_ns), `last` cursor).
/// Trade entry shape: `["price","vol",time,"side","ordtype","misc",id]` (time is unquoted).
fn parse_rest_trades(body: &str) -> (Vec<(f32, f32, u64)>, String) {
    let mut out = Vec::new();
    let last = body.find("\"last\":\"")
        .map(|i| body[i + 8..].split('"').next().unwrap_or("").to_string())
        .unwrap_or_default();
    let bytes = body.as_bytes();
    let mut i = match body.find("[[") { Some(p) => p + 1, None => return (out, last) };
    let n = bytes.len();
    while i < n {
        while i < n && bytes[i] != b'[' { i += 1; }
        if i >= n { break; }
        let mut j = i + 1;
        while j < n && bytes[j] != b']' { j += 1; }
        if j >= n { break; }
        let f: Vec<&str> = body[i + 1..j].split(',').collect();
        if f.len() >= 4 {
            let price = f[0].trim().trim_matches('"').parse::<f32>();
            let vol   = f[1].trim().trim_matches('"').parse::<f32>();
            let time  = f[2].trim().trim_matches('"').parse::<f64>();
            if let (Ok(price), Ok(vol), Ok(time)) = (price, vol, time) {
                let signed = if f[3].trim().trim_matches('"').starts_with('s') { -vol } else { vol };
                out.push((price, signed, (time * 1e9) as u64));
            }
        }
        i = j + 1;
    }
    (out, last)
}

fn run_history(api: &str, pair: &str, ref_pair: &str, hours: u64, out: &str) -> io::Result<()> {
    let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    let since0 = now_ns.saturating_sub(hours * 3_600 * 1_000_000_000);
    println!("[history] collecting ~{hours}h of {pair} (0) + {ref_pair} (1) via {api} → {out}");

    let mut all: Vec<(u64, u8, f32, f32)> = Vec::new();  // (time_ns, id, price, signed_vol)
    for (id, p) in [(0u8, pair), (1u8, ref_pair)] {
        let pair_q = p.replace('/', "");                  // XBT/USD → XBTUSD
        let (mut since, mut pages, mut total) = (since0, 0u32, 0usize);
        loop {
            let path = format!("/0/public/Trades?pair={pair_q}&since={since}");
            let body = match http_get(api, &path, KRAKEN_API_HOST) {
                Ok(b) => b,
                Err(e) => { eprintln!("\n[history] {p} fetch error: {e}"); break; }
            };
            let (trades, last) = parse_rest_trades(&body);
            if trades.is_empty() { break; }
            for (price, vol, t) in &trades { all.push((*t, id, *price, *vol)); }
            total += trades.len();
            pages += 1;
            print!("\r[history] {p}: {total} trades ({pages} pages)…");
            let _ = std::io::stdout().flush();
            let next: u64 = last.parse().unwrap_or(0);
            if next <= since || next >= now_ns || pages >= 500 { break; }
            since = next;
        }
        println!();
    }
    if all.is_empty() {
        return Err(io::Error::other("history: no trades (check the stunnel → api.kraken.com service)"));
    }
    all.sort_by_key(|t| t.0);

    let mut rec = Recorder::create(out)?;
    let mut seq = [1u64; 2];
    let mut prev_t = all[0].0;
    for (t, id, price, vol) in &all {
        let delta = t.saturating_sub(prev_t).min(1_000_000_000);  // cap idle gaps at 1s
        prev_t = *t;
        let s = seq[*id as usize];
        let pkt = build_packet(*price, *vol, s, *t, 0, *id);       // transit 0 (no live RTT)
        rec.file.write_all(&delta.to_le_bytes())?;
        rec.file.write_all(&(pkt.len() as u16).to_le_bytes())?;
        rec.file.write_all(&pkt)?;
        seq[*id as usize] = s + 1;
    }
    println!("[history] wrote {} trades to {out}", all.len());
    Ok(())
}

fn sleep_ns(ns: u64) {
    if ns == 0 { return; }
    // Sleep for the millisecond bulk, spin for the sub-ms remainder for fidelity.
    if ns >= 2_000_000 {
        std::thread::sleep(Duration::from_nanos(ns - 1_000_000));
    } else {
        let start = Instant::now();
        let target = Duration::from_nanos(ns);
        while start.elapsed() < target { std::hint::spin_loop(); }
    }
}

// ── Live streaming ────────────────────────────────────────────────────────────

fn run_live(
    endpoint: &str,
    pair: &str,
    ref_pair: &str,
    ingestor: &str,
    record: Option<&str>,
) -> io::Result<()> {
    println!("[kraken-feed] connecting to {endpoint} (stunnel → {KRAKEN_HOST}:443)");
    let mut stream = TcpStream::connect(endpoint)?;
    stream.set_nodelay(true).ok();
    let mut acc = ws_handshake(&mut stream, KRAKEN_HOST, "/")?;
    println!("[kraken-feed] websocket connected; subscribing to {pair} (0) + {ref_pair} (1) trades");
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;

    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let mut recorder = match record {
        Some(p) => Some(Recorder::create(p)?),
        None => None,
    };

    let sub = format!(
        "{{\"event\":\"subscribe\",\"pair\":[\"{pair}\",\"{ref_pair}\"],\"subscription\":{{\"name\":\"trade\"}}}}"
    );
    stream.write_all(&build_frame(0x1, sub.as_bytes()))?;

    let start = Instant::now();
    let mut seq: [u64; 2] = [1, 1];   // per-instrument sequence
    let mut transit_est: u64 = 0;
    // Send an initial ping so we get an RTT sample quickly.
    stream.write_all(&build_frame(0x9, &start.elapsed().as_nanos().to_le_bytes()[..8]))?;
    let mut last_ping = Instant::now();
    let mut tmp = [0u8; 8192];

    loop {
        // Drain complete frames already buffered.
        while let Some((opcode, payload, consumed)) = parse_frame(&acc) {
            acc.drain(..consumed);
            match opcode {
                0x1 => {
                    let msg = String::from_utf8_lossy(&payload);
                    // Map the frame's pair to an instrument id (0 traded, 1 reference).
                    let id: u8 = match frame_pair(&msg) {
                        Some(p) if p == pair     => 0,
                        Some(p) if p == ref_pair => 1,
                        _ => continue,   // unknown pair / non-trade frame
                    };
                    for (price, signed_vol, origin_ts_ns) in parse_trades(&msg) {
                        let s = seq[id as usize];
                        let pkt = build_packet(price, signed_vol, s, origin_ts_ns, transit_est, id);
                        udp.send_to(&pkt, ingestor)?;
                        if let Some(r) = recorder.as_mut() { r.write(&pkt)?; }
                        seq[id as usize] = s + 1;
                    }
                }
                0x9 => { stream.write_all(&build_frame(0xA, &payload))?; } // ping → pong
                0xA if payload.len() >= 8 => {
                    let sent = u64::from_le_bytes(payload[..8].try_into().unwrap());
                    let rtt = (start.elapsed().as_nanos() as u64).saturating_sub(sent);
                    transit_est = rtt / 2;
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
    let mut acc = ws_handshake(&mut stream, KRAKEN_FUTURES_HOST, "/ws/v1")?;
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
    let mut first = true;
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
                        if first {
                            println!("[kraken-feed] first ticker: mid {:.1}  spread {:.3} bps  funding {:.8} /hr (relative)",
                                     mid(bid, ask), spread_bps(bid, ask), funding);
                            first = false;
                        }
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
                0xA if payload.len() >= 8 => {
                    let sent = u64::from_le_bytes(payload[..8].try_into().unwrap());
                    let rtt = (start.elapsed().as_nanos() as u64).saturating_sub(sent);
                    transit_est = rtt / 2;
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

// ── CLI ───────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut endpoint = STUNNEL_ADDR.to_string();
    let mut pair = KRAKEN_PAIR.to_string();
    let mut ref_pair = std::env::var("HFT_REF_PAIR").unwrap_or_else(|_| KRAKEN_REF_PAIR.to_string());
    let mut ingestor = INGESTOR_ADDR.to_string();
    let mut record: Option<String> = None;
    let mut replay: Option<String> = None;
    let mut synth: Option<String> = None;
    let mut history = false;
    let mut hours: u64 = 24;
    let mut out = RECORD_PATH_DEFAULT.to_string();
    let mut api = API_STUNNEL_ADDR.to_string();
    let mut futures: Option<String> = None;
    let mut futures_endpoint = FUTURES_STUNNEL_ADDR.to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--live"     => { if let Some(v) = args.get(i + 1).filter(|v| !v.starts_with("--")) { endpoint = v.clone(); i += 1; } }
            "--pair"     => { if let Some(v) = args.get(i + 1) { pair = v.clone(); i += 1; } }
            "--ref-pair" => { if let Some(v) = args.get(i + 1) { ref_pair = v.clone(); i += 1; } }
            "--ingestor" => { if let Some(v) = args.get(i + 1) { ingestor = v.clone(); i += 1; } }
            "--record"   => { record = Some(args.get(i + 1).cloned().unwrap_or_else(|| RECORD_PATH_DEFAULT.to_string())); if record.as_deref().map(|s| !s.starts_with("--")).unwrap_or(false) { i += 1; } }
            "--replay"   => { replay = Some(args.get(i + 1).cloned().unwrap_or_else(|| RECORD_PATH_DEFAULT.to_string())); i += 1; }
            "--synth"    => { synth = Some(args.get(i + 1).filter(|v| !v.starts_with("--")).cloned().unwrap_or_else(|| RECORD_PATH_DEFAULT.to_string())); if synth.as_deref().map(|s| !s.starts_with("--")).unwrap_or(false) && args.get(i + 1).map(|v| !v.starts_with("--")).unwrap_or(false) { i += 1; } }
            "--history"  => { history = true; }
            "--hours"    => { if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) { hours = v; i += 1; } }
            "--out"      => { if let Some(v) = args.get(i + 1) { out = v.clone(); i += 1; } }
            "--api"      => { if let Some(v) = args.get(i + 1) { api = v.clone(); i += 1; } }
            "--futures"  => { futures = Some(args.get(i + 1).filter(|v| !v.starts_with("--")).cloned().unwrap_or_else(|| KRAKEN_FUTURES_PRODUCT.to_string())); if args.get(i + 1).map(|v| !v.starts_with("--")).unwrap_or(false) { i += 1; } }
            "--product"  => { if let Some(v) = args.get(i + 1) { futures = Some(v.clone()); i += 1; } }
            "--futures-endpoint" => { if let Some(v) = args.get(i + 1) { futures_endpoint = v.clone(); i += 1; } }
            "--help" | "-h" => { print_usage(); return; }
            other => { eprintln!("[kraken-feed] unknown argument: {other}"); print_usage(); std::process::exit(2); }
        }
        i += 1;
    }

    let result = if let Some(product) = futures.as_deref() {
        run_futures(&futures_endpoint, product, &ingestor, record.as_deref())
    } else if history {
        run_history(&api, &pair, &ref_pair, hours, &out)
    } else if let Some(path) = synth {
        run_synth(&path)
    } else if let Some(path) = replay {
        run_replay(&path, &ingestor)
    } else {
        run_live(&endpoint, &pair, &ref_pair, &ingestor, record.as_deref())
    };

    if let Err(e) = result {
        eprintln!("[kraken-feed] error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!(
        "kraken-feed — live Kraken trade feed → engine UDP\n\
         \n\
         USAGE:\n\
         \x20 kraken-feed [--live HOST:PORT] [--pair SYMBOL] [--ref-pair SYMBOL] [--ingestor ADDR] [--record FILE]\n\
         \x20 kraken-feed --replay FILE [--ingestor ADDR]\n\
         \x20 kraken-feed --synth [FILE]            (HFT_SYNTH_TICKS sets length)\n\
         \x20 kraken-feed --futures [PRODUCT] [--futures-endpoint HOST:PORT] [--ingestor ADDR] [--record FILE]\n\
         \x20 kraken-feed --history [--hours N] [--pair SYMBOL] [--ref-pair SYMBOL] [--out FILE] [--api HOST:PORT]\n\
         \n\
         Defaults: --live {STUNNEL_ADDR}  --api {API_STUNNEL_ADDR}  --pair {KRAKEN_PAIR}  --ref-pair {KRAKEN_REF_PAIR}\n\
         Live needs stunnel → {KRAKEN_HOST}:443; --history needs a stunnel service → {KRAKEN_API_HOST}:443 (see docs/stunnel.conf)."
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn sha1_known_vectors() {
        assert_eq!(hex(&sha1(b"abc")), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(hex(&sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn base64_known_vectors() {
        assert_eq!(base64_encode(b"abc"), "YWJj");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"a"), "YQ==");
    }

    #[test]
    fn websocket_accept_rfc_example() {
        // The canonical RFC6455 §1.3 example.
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = base64_encode(&sha1(format!("{key}{WS_GUID}").as_bytes()));
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn frame_roundtrip() {
        // build_frame masks (client side); parse_frame unmasks.
        let frame = build_frame(0x1, b"hello");
        let (opcode, payload, consumed) = parse_frame(&frame).expect("complete frame");
        assert_eq!(opcode, 0x1);
        assert_eq!(payload, b"hello");
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn frame_incomplete_returns_none() {
        let frame = build_frame(0x1, b"hello world");
        assert!(parse_frame(&frame[..4]).is_none());
    }

    #[test]
    fn parse_kraken_timestamp() {
        assert_eq!(parse_kraken_ts("1534614057.321597"), Some(1_534_614_057_321_597_000));
        assert_eq!(parse_kraken_ts("1700000000"), Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn parse_trade_message() {
        let msg = r#"[337,[["5541.20000","0.15850568","1534614057.321597","s","l",""],["5541.30000","0.10000000","1534614057.400000","b","l",""]],"trade","XBT/USD"]"#;
        let trades = parse_trades(msg);
        assert_eq!(trades.len(), 2);
        assert!((trades[0].0 - 5541.2).abs() < 0.01);
        assert!((trades[0].1 + 0.15850568).abs() < 1e-6);   // sell → negative signed volume
        assert_eq!(trades[0].2, 1_534_614_057_321_597_000);
        assert!((trades[1].0 - 5541.3).abs() < 0.01);
        assert!((trades[1].1 - 0.10000000).abs() < 1e-6);   // buy → positive
    }

    #[test]
    fn non_trade_message_ignored() {
        assert!(parse_trades(r#"{"event":"heartbeat"}"#).is_empty());
        assert!(parse_trades(r#"{"event":"systemStatus","status":"online"}"#).is_empty());
    }

    #[test]
    fn parse_rest_history() {
        // Kraken REST /0/public/Trades shape (time is an unquoted number).
        let body = r#"{"error":[],"result":{"XXBTZUSD":[["96000.1","0.5",1700000000.5,"b","l","",1],["95999.0","0.2",1700000001.0,"s","m","",2]],"last":"1700000001000000000"}}"#;
        let (trades, last) = parse_rest_trades(body);
        assert_eq!(trades.len(), 2);
        assert!((trades[0].0 - 96000.1).abs() < 0.1);
        assert!((trades[0].1 - 0.5).abs() < 1e-6);                 // buy → positive
        assert_eq!(trades[0].2, 1_700_000_000_500_000_000);        // 1700000000.5 s → ns
        assert!((trades[1].1 + 0.2).abs() < 1e-6);                 // sell → negative
        assert_eq!(last, "1700000001000000000");
    }

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
        let msg = r#"{"feed":"ticker","product_id":"PF_XBTUSD","bid":60234.0,"ask":60235.5,"markPrice":60234.8,"last":60234.5,"funding_rate":1.2e-7,"relative_funding_rate":3.5e-6,"time":1718040000000}"#;
        let t = parse_futures_ticker(msg).expect("a ticker");
        assert!((t.0 - 60234.0).abs() < 0.01);   // bid
        assert!((t.1 - 60235.5).abs() < 0.01);   // ask
        assert!((t.2 - 60234.8).abs() < 0.01);   // mark
        assert!((t.3 - 3.5e-6).abs() < 1e-10);   // relative_funding_rate (not the absolute one)
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

    #[test]
    fn mid_and_spread_bps() {
        assert!((mid(59995.0, 60005.0) - 60000.0).abs() < 0.01);
        // spread = 10 / 60000 * 1e4 ≈ 1.667 bps
        assert!((spread_bps(59995.0, 60005.0) - 1.6667).abs() < 0.01);
        assert_eq!(spread_bps(0.0, 0.0), 0.0);   // degenerate → 0, no NaN
    }
}
