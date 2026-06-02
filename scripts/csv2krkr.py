#!/usr/bin/env python3
"""Convert real market CSV(s) into a .krkr capture the engine can backtest/train on.

Dev utility only (not part of the zero-dependency Rust workspace) — it fabricates
the same capture format kraken-feed --record/--synth write, from real exchange CSV
dumps fetched off an allowlisted git host. Trades or OHLCV candles both work:

  capture = b"KRKR" + version(1) + records
  record  = [delta_ns u64 LE][len u16 LE][packet]
  packet  = price f32 | signed_vol f32 | seq u64 | origin_ns u64 | transit_ns u64 | instr u8  (33B v3)

Usage:
  csv2krkr.py OUT.krkr  traded=BTC.csv [reference=ETH.csv]  [--limit N]

Each input is parsed for (timestamp_ns, price, volume, signed?) with header
auto-detection; the two instruments are interleaved by timestamp (traded=id0,
reference=id1). If per-trade side is absent (OHLCV), volume is signed by the tick
rule (close-vs-previous-close) so the order-flow term still carries information.
"""
import csv, struct, sys, io

def _to_ns(ts: float) -> int:
    # Heuristic: seconds (1e9..1e10), millis (1e12..), micros, or already ns.
    t = float(ts)
    if t > 1e17:   return int(t)            # ns
    if t > 1e14:   return int(t * 1e3)      # micros
    if t > 1e11:   return int(t * 1e6)      # millis
    return int(t * 1e9)                     # seconds

def load(path, limit=None):
    """Return list of (ts_ns:int, price:float, signed_vol:float)."""
    with open(path, newline="") as f:
        sniff = f.read(4096); f.seek(0)
        delim = "\t" if sniff.count("\t") > sniff.count(",") else ","
        rd = csv.reader(f, delimiter=delim)
        rows = list(rd)
    if not rows:
        return []
    header = [c.strip().lower() for c in rows[0]]
    has_header = any(c and not _isnum(c) for c in header)
    body = rows[1:] if has_header else rows
    cols = header if has_header else [f"c{i}" for i in range(len(rows[0]))]

    def find(*names):
        for n in names:
            if n in cols: return cols.index(n)
        return None
    i_ts    = find("timestamp", "time", "date", "unix", "ts", "datetime", "open_time", "c0")
    i_price = find("price", "close", "last", "c4")
    i_vol   = find("volume", "amount", "size", "qty", "vol", "base_volume", "volume_base")
    i_side  = find("side", "type", "is_buyer_maker", "maker")
    if i_price is None:  # candle CSV without 'close' header but ohlcv order
        i_price = 4 if len(cols) > 4 else len(cols) - 1
    if i_ts is None:    i_ts = 0

    out, prev_px = [], None
    for r in body:
        if not r or len(r) <= max(i_ts, i_price): continue
        try:
            ts = _to_ns(_parse_ts(r[i_ts]))
            px = float(r[i_price])
        except (ValueError, IndexError):
            continue
        vol = 0.0
        if i_vol is not None and i_vol < len(r):
            try: vol = abs(float(r[i_vol]))
            except ValueError: vol = 0.0
        sign = 0.0
        if i_side is not None and i_side < len(r):
            s = r[i_side].strip().lower()
            if s in ("b", "buy", "bid", "true", "1"):  sign = 1.0
            elif s in ("s", "sell", "ask", "false", "0"): sign = -1.0
        if sign == 0.0:                       # tick rule fallback (OHLCV)
            if prev_px is not None: sign = 1.0 if px >= prev_px else -1.0
            else: sign = 1.0
        prev_px = px
        out.append((ts, px, sign * (vol if vol else 1.0)))
        if limit and len(out) >= limit: break
    return out

def _isnum(s):
    try: float(s); return True
    except ValueError: return False

def _parse_ts(s):
    s = s.strip()
    if _isnum(s): return float(s)
    # ISO-ish "YYYY-MM-DD HH:MM:SS" → epoch seconds (UTC), stdlib only.
    import datetime as dt
    s = s.replace("T", " ").replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try: return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc).timestamp()
        except ValueError: continue
    raise ValueError(f"bad ts {s!r}")

def main(argv):
    if len(argv) < 3:
        print(__doc__); return 1
    out_path = argv[1]
    inputs, limit = {}, None
    for a in argv[2:]:
        if a.startswith("--limit"): limit = int(a.split("=")[1]); continue
        k, _, v = a.partition("="); inputs[k] = v
    limit_each = limit
    traded = load(inputs["traded"], limit_each)
    ref = load(inputs["reference"], limit_each) if "reference" in inputs else []
    events = [(t, p, v, 0) for (t, p, v) in traded] + [(t, p, v, 1) for (t, p, v) in ref]
    events.sort(key=lambda e: e[0])
    if not events:
        print("no events parsed", file=sys.stderr); return 2

    buf = io.BytesIO()
    buf.write(b"KRKR"); buf.write(bytes([1]))
    prev_t = events[0][0]
    for seq, (t, px, vol, instr) in enumerate(events, 1):
        delta = max(0, t - prev_t); prev_t = t
        pkt = (struct.pack("<f", px) + struct.pack("<f", float(vol)) +
               struct.pack("<Q", seq) + struct.pack("<Q", int(t)) +
               struct.pack("<Q", 0) + bytes([instr]))
        buf.write(struct.pack("<Q", int(delta)))
        buf.write(struct.pack("<H", len(pkt)))
        buf.write(pkt)
    with open(out_path, "wb") as f:
        f.write(buf.getvalue())
    span_s = (events[-1][0] - events[0][0]) / 1e9
    print(f"wrote {len(events)} events ({len(traded)} traded / {len(ref)} ref) "
          f"spanning {span_s/3600:.1f}h → {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
