#!/usr/bin/env python3
"""
backfill_desktop_data_to_postgres.py

Two responsibilities:

1. IMPORT  – Discover legacy CSV files under DATA_DIR (default ~/Desktop/Data)
             and insert rows into PostgreSQL tables:
               • harvest_5s_bars        (*_5s_warmup*.csv)
               • harvest_live_ticks     (*_live_ticks*.csv)
               • harvest_news_events    (*_news_*.csv)
             Uses ON CONFLICT DO NOTHING so the script is safe to re-run.

2. VALIDATE – For every record in harvest_5s_bars, inspect the bid/ask and
              call/put volume fields in the JSON payload.  If any of those
              fields are missing or zero *and* the corresponding tick data
              exists in harvest_live_ticks for the same 5-second window,
              re-aggregate them from the ticks and repair the bar payload
              in-place.

              Aggregation semantics (matches harvester.py finalize_quote_window):
                Bid / Ask          → simple average of non-zero tick bid/ask values
                                     as a proxy for TWAP when bar data is absent
                BidSize / AskSize  → simple average of non-zero tick sizes
                BidLast / AskLast  → last non-zero tick bid/ask in window
                PutVol / CallVol   → last non-null level seen in tick window
                PutVolDelta5s      → max(0, last_put_vol − first_put_vol)
                CallVolDelta5s     → max(0, last_call_vol − first_call_vol)

Modes  (--mode flag or MODE env var):
  import    – import only
  validate  – validate/repair only
  both      – import then validate  (default)

Environment variables:
  DB_HOST       PostgreSQL host           (default: localhost)
  DB_PORT       PostgreSQL port           (default: 5432)
  DB_NAME       Database name             (default: trading_agent)
  DB_USER       Database user             (default: postgres)
  DB_PASSWORD   Database password         (default: '')
  DATA_DIR      Root directory to scan    (default: ~/Desktop/Data)
  DRY_RUN       1 = print only, no writes (default: 0)
  BATCH_SIZE    Bars per validation batch  (default: 500)
  LOG_EVERY     Log progress every N rows  (default: 1000)
"""

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit(
        "psycopg2 not found. Install with:  pip install psycopg2-binary"
    )

# ---------------------------------------------------------------------------
# Constants / configuration
# ---------------------------------------------------------------------------

MARKET_ZONE = ZoneInfo("America/New_York")

# Field names that constitute "bid/ask present" in a 5s bar payload.
BAR_BID_FIELDS  = ("Bid", "BidLast")
BAR_ASK_FIELDS  = ("Ask", "AskLast")
BAR_PUT_FIELDS  = ("PutVol", "PutVolDelta5s")
BAR_CALL_FIELDS = ("CallVol", "CallVolDelta5s")

# Legacy CSV format markers
_EPOCH_RE   = re.compile(r"^\d{9,13}(\.\d+)?$")
_YYYYMMDD_RE = re.compile(
    r"^(\d{8})\s{1,2}(\d{2}:\d{2}:\d{2})(\s+America/New_York)?$"
)
_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
)

# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(raw) -> datetime | None:
    """
    Normalise a raw timestamp value from CSV into a UTC-aware datetime.
    Handles:
      • numeric epoch (seconds)
      • 'YYYYMMDD HH:MM:SS' / 'YYYYMMDD  HH:MM:SS' [America/New_York]
      • ISO-ish strings (YYYY-MM-DD HH:MM:SS[...])
    Returns None when parsing fails.
    """
    if raw is None:
        return None

    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None

    # Numeric epoch (seconds; millisecond values are detected and converted)
    if _EPOCH_RE.match(s):
        try:
            epoch_val = float(s)
            if epoch_val > 1e12:          # milliseconds → seconds
                epoch_val /= 1000.0
            return datetime.fromtimestamp(epoch_val, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            pass

    # YYYYMMDD HH:MM:SS [America/New_York]
    m = _YYYYMMDD_RE.match(s)
    if m:
        date_part, time_part = m.group(1), m.group(2)
        try:
            naive = datetime.strptime(f"{date_part} {time_part}", "%Y%m%d %H:%M:%S")
            # Always treat as NY local → convert to UTC
            ny_aware = naive.replace(tzinfo=MARKET_ZONE)
            return ny_aware.astimezone(timezone.utc)
        except ValueError:
            pass

    # ISO-ish
    if _ISO_RE.match(s):
        s_clean = re.sub(r"\s+America/New_York$", "", s).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(s_clean, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=MARKET_ZONE).astimezone(timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                pass

    return None


def _market_day(dt_utc: datetime) -> str:
    """Return 'YYYY-MM-DD' in NY timezone for a UTC datetime."""
    return dt_utc.astimezone(MARKET_ZONE).date().isoformat()


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _safe_num(v, default=None):
    """Convert v to float; return default on failure."""
    if v is None:
        return default
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _event_key(*parts: str) -> str:
    """Stable SHA-1 hash of concatenated string parts → 40-char hex key."""
    blob = "|".join(str(p) for p in parts)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _infer_symbol(path: Path) -> str | None:
    """
    Try to derive a ticker symbol from file name or parent folder name.
    e.g. 'TSLA_5s_warmup_20260306.csv' → 'TSLA'
         'NVDA/NVDA_live_ticks_20260316.csv' → 'NVDA'
    """
    stem = path.stem.upper()
    # Filename starts with symbol
    m = re.match(r"^([A-Z]{1,6})[_\-\.]", stem)
    if m:
        return m.group(1)
    # Parent directory name is a symbol
    parent = path.parent.name.upper()
    if re.match(r"^[A-Z]{1,6}$", parent):
        return parent
    return None


def _bool_val(env_var: str, default: bool) -> bool:
    raw = os.getenv(env_var, "")
    if not raw:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "trading_agent"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


# ---------------------------------------------------------------------------
# IMPORT: individual file importers
# ---------------------------------------------------------------------------

_INSERT_BAR = """
INSERT INTO harvest_5s_bars
    (symbol, bar_time, market_day, payload, created_at, updated_at)
VALUES
    (%(symbol)s, %(bar_time)s, %(market_day)s, %(payload)s,
     NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC')
ON CONFLICT (symbol, bar_time) DO NOTHING;
"""

_INSERT_TICK = """
INSERT INTO harvest_live_ticks
    (event_key, symbol, tick_time, market_day, price, size, payload,
     created_at, updated_at)
VALUES
    (%(event_key)s, %(symbol)s, %(tick_time)s, %(market_day)s,
     %(price)s, %(size)s, %(payload)s,
     NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC')
ON CONFLICT (event_key) DO NOTHING;
"""

_INSERT_NEWS = """
INSERT INTO harvest_news_events
    (event_key, symbol, published_ts, market_day, provider, article_id,
     headline, payload, created_at, updated_at)
VALUES
    (%(event_key)s, %(symbol)s, %(published_ts)s, %(market_day)s,
     %(provider)s, %(article_id)s, %(headline)s, %(payload)s,
     NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC')
ON CONFLICT (event_key) DO NOTHING;
"""


def _import_bars_file(conn, path: Path, symbol: str, dry_run: bool,
                      log_every: int) -> dict:
    stats = {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0}
    rows_batch = []

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return stats

        header = [h.strip() for h in header]
        ts_col = next(
            (i for i, h in enumerate(header) if h in ("Timestamp", "time", "Time")),
            0,
        )

        for lineno, row in enumerate(reader, start=2):
            if not any(r.strip() for r in row):
                continue
            stats["seen"] += 1

            raw_ts = row[ts_col].strip() if ts_col < len(row) else ""
            dt_utc = _parse_timestamp(raw_ts)
            if dt_utc is None:
                stats["skipped"] += 1
                continue

            payload = {header[i]: (row[i].strip() if i < len(row) else None)
                       for i in range(len(header))}

            rows_batch.append({
                "symbol":     symbol,
                "bar_time":   dt_utc.isoformat(),
                "market_day": _market_day(dt_utc),
                "payload":    json.dumps(payload),
            })

            if len(rows_batch) >= log_every:
                _flush_bars(conn, rows_batch, stats, dry_run)
                rows_batch.clear()
                print(f"  [bars] {path.name}: {stats['seen']} seen / "
                      f"{stats['inserted']} inserted / {stats['dupes']} dupes")

    if rows_batch:
        _flush_bars(conn, rows_batch, stats, dry_run)

    return stats


def _flush_bars(conn, rows, stats, dry_run):
    if dry_run:
        stats["inserted"] += len(rows)
        return
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(_INSERT_BAR, row)
            if cur.rowcount == 1:
                stats["inserted"] += 1
            else:
                stats["dupes"] += 1
    conn.commit()


def _import_ticks_file(conn, path: Path, symbol: str, dry_run: bool,
                       log_every: int) -> dict:
    stats = {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0}
    rows_batch = []

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return stats

        header = [h.strip() for h in header]
        ts_col  = next((i for i, h in enumerate(header) if h == "time"), 0)
        price_col = next((i for i, h in enumerate(header) if h == "price"), -1)
        size_col  = next((i for i, h in enumerate(header) if h == "size"),  -1)

        for lineno, row in enumerate(reader, start=2):
            if not any(r.strip() for r in row):
                continue
            stats["seen"] += 1

            raw_ts = row[ts_col].strip() if ts_col < len(row) else ""
            dt_utc = _parse_timestamp(raw_ts)
            if dt_utc is None:
                stats["skipped"] += 1
                continue

            price = _safe_num(row[price_col]) if price_col >= 0 and price_col < len(row) else None
            size  = _safe_num(row[size_col])  if size_col  >= 0 and size_col  < len(row) else None

            payload = {header[i]: (row[i].strip() if i < len(row) else None)
                       for i in range(len(header))}

            key = _event_key(symbol, dt_utc.isoformat(),
                             str(price or ""), str(size or ""))

            rows_batch.append({
                "event_key":  key,
                "symbol":     symbol,
                "tick_time":  dt_utc.isoformat(),
                "market_day": _market_day(dt_utc),
                "price":      price,
                "size":       round(size) if size is not None else None,
                "payload":    json.dumps(payload),
            })

            if len(rows_batch) >= log_every:
                _flush_ticks(conn, rows_batch, stats, dry_run)
                rows_batch.clear()
                print(f"  [ticks] {path.name}: {stats['seen']} seen / "
                      f"{stats['inserted']} inserted / {stats['dupes']} dupes")

    if rows_batch:
        _flush_ticks(conn, rows_batch, stats, dry_run)

    return stats


def _flush_ticks(conn, rows, stats, dry_run):
    if dry_run:
        stats["inserted"] += len(rows)
        return
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(_INSERT_TICK, row)
            if cur.rowcount == 1:
                stats["inserted"] += 1
            else:
                stats["dupes"] += 1
    conn.commit()


def _import_news_file(conn, path: Path, symbol: str, dry_run: bool,
                      log_every: int) -> dict:
    stats = {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0}
    rows_batch = []

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return stats

        header = [h.strip() for h in header]

        def _col(names):
            for n in names:
                try:
                    return header.index(n)
                except ValueError:
                    pass
            return -1

        ts_col      = _col(["published_ts", "time", "Time"])
        prov_col    = _col(["provider", "Provider"])
        art_col     = _col(["article_id"])
        head_col    = _col(["headline", "Headline"])

        for lineno, row in enumerate(reader, start=2):
            if not any(r.strip() for r in row):
                continue
            stats["seen"] += 1

            raw_ts = row[ts_col].strip() if ts_col >= 0 and ts_col < len(row) else ""
            dt_utc = _parse_timestamp(raw_ts)
            if dt_utc is None:
                stats["skipped"] += 1
                continue

            provider   = row[prov_col].strip() if prov_col >= 0 and prov_col < len(row) else ""
            article_id = row[art_col].strip()  if art_col  >= 0 and art_col  < len(row) else ""
            headline   = row[head_col].strip() if head_col >= 0 and head_col < len(row) else ""

            payload = {header[i]: (row[i].strip() if i < len(row) else None)
                       for i in range(len(header))}

            key = _event_key(symbol, provider, article_id,
                             dt_utc.isoformat(), headline[:80])

            rows_batch.append({
                "event_key":    key,
                "symbol":       symbol,
                "published_ts": dt_utc.isoformat(),
                "market_day":   _market_day(dt_utc),
                "provider":     provider,
                "article_id":   article_id,
                "headline":     headline,
                "payload":      json.dumps(payload),
            })

            if len(rows_batch) >= log_every:
                _flush_news(conn, rows_batch, stats, dry_run)
                rows_batch.clear()
                print(f"  [news] {path.name}: {stats['seen']} seen / "
                      f"{stats['inserted']} inserted / {stats['dupes']} dupes")

    if rows_batch:
        _flush_news(conn, rows_batch, stats, dry_run)

    return stats


def _flush_news(conn, rows, stats, dry_run):
    if dry_run:
        stats["inserted"] += len(rows)
        return
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(_INSERT_NEWS, row)
            if cur.rowcount == 1:
                stats["inserted"] += 1
            else:
                stats["dupes"] += 1
    conn.commit()


# ---------------------------------------------------------------------------
# IMPORT: orchestrator
# ---------------------------------------------------------------------------

def run_import(conn, data_dir: Path, dry_run: bool, log_every: int) -> dict:
    """
    Discover all CSV files under data_dir and import them.
    Returns a summary dict.
    """
    all_stats = {
        "bars":  {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0, "files": 0},
        "ticks": {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0, "files": 0},
        "news":  {"seen": 0, "inserted": 0, "dupes": 0, "skipped": 0, "files": 0},
    }

    for path in sorted(data_dir.rglob("*.csv")):
        name_lower = path.name.lower()
        symbol = _infer_symbol(path)
        if symbol is None:
            print(f"[WARN] Cannot infer symbol from {path}, skipping.")
            continue

        try:
            if "_5s_warmup" in name_lower or "warmup" in name_lower:
                kind = "bars"
                stats = _import_bars_file(conn, path, symbol, dry_run, log_every)
            elif "_live_ticks" in name_lower or "live_tick" in name_lower:
                kind = "ticks"
                stats = _import_ticks_file(conn, path, symbol, dry_run, log_every)
            elif "_news_" in name_lower or "news" in name_lower:
                kind = "news"
                stats = _import_news_file(conn, path, symbol, dry_run, log_every)
            else:
                continue

            for k in ("seen", "inserted", "dupes", "skipped"):
                all_stats[kind][k] += stats[k]
            all_stats[kind]["files"] += 1
            print(f"[{kind.upper()}] {symbol} {path.name}: "
                  f"seen={stats['seen']} inserted={stats['inserted']} "
                  f"dupes={stats['dupes']} skipped={stats['skipped']}")

        except Exception as exc:
            print(f"[ERROR] Failed to import {path}: {exc}")
            try:
                conn.rollback()
            except Exception:
                pass

    return all_stats


# ---------------------------------------------------------------------------
# VALIDATE: helpers
# ---------------------------------------------------------------------------

def _is_missing_or_zero(payload: dict, keys: tuple) -> bool:
    """
    Return True if none of the given keys in payload has a positive finite value.
    """
    for k in keys:
        v = _safe_num(payload.get(k))
        if v is not None and v > 0:
            return False
    return True


def _compute_aggregates_from_ticks(tick_rows: list) -> dict:
    """
    Given a list of tick payload dicts for a 5-second window, compute the
    expected 5s bar aggregate values using the same semantics as
    harvester.py :: finalize_quote_window():

      Bid/Ask      → time-weighted average proxy (simple mean of non-zero ticks)
      BidSize/AskSize → simple mean of non-zero tick sizes
      BidLast/AskLast → last non-zero tick value
      PutVol/CallVol  → last non-null level
      PutVolDelta5s/CallVolDelta5s → max(0, last − first)
    """
    result = {}
    if not tick_rows:
        return result

    bids       = [v for r in tick_rows if (v := _safe_num(r.get("bid"))) is not None and v > 0]
    asks       = [v for r in tick_rows if (v := _safe_num(r.get("ask"))) is not None and v > 0]
    bid_sizes  = [v for r in tick_rows if (v := _safe_num(r.get("bid_size"))) is not None and v >= 0]
    ask_sizes  = [v for r in tick_rows if (v := _safe_num(r.get("ask_size"))) is not None and v >= 0]
    put_vols   = [v for r in tick_rows if (v := _safe_num(r.get("put_vol")))  is not None]
    call_vols  = [v for r in tick_rows if (v := _safe_num(r.get("call_vol"))) is not None]

    if bids:
        result["Bid"]     = sum(bids) / len(bids)
        result["BidLast"] = bids[-1]
    if asks:
        result["Ask"]     = sum(asks) / len(asks)
        result["AskLast"] = asks[-1]
    if bid_sizes:
        result["BidSize"]     = sum(bid_sizes) / len(bid_sizes)
        result["BidSizeLast"] = bid_sizes[-1]
    if ask_sizes:
        result["AskSize"]     = sum(ask_sizes) / len(ask_sizes)
        result["AskSizeLast"] = ask_sizes[-1]
    if put_vols:
        result["PutVol"]        = put_vols[-1]
        result["PutVolDelta5s"] = max(0.0, put_vols[-1] - put_vols[0])
    if call_vols:
        result["CallVol"]        = call_vols[-1]
        result["CallVolDelta5s"] = max(0.0, call_vols[-1] - call_vols[0])

    return result


# ---------------------------------------------------------------------------
# VALIDATE: main engine
# ---------------------------------------------------------------------------

_FETCH_BARS_BATCH = """
SELECT
    symbol,
    bar_time,
    market_day,
    payload
FROM harvest_5s_bars
ORDER BY bar_time, symbol
LIMIT %(limit)s OFFSET %(offset)s;
"""

_FETCH_TICKS_WINDOW = """
SELECT payload
FROM   harvest_live_ticks
WHERE  symbol    = %(symbol)s
  AND  tick_time >  %(window_start)s
  AND  tick_time <= %(window_end)s
ORDER BY tick_time;
"""

_UPDATE_BAR = """
UPDATE harvest_5s_bars
SET    payload    = %(payload)s,
       updated_at = NOW() AT TIME ZONE 'UTC'
WHERE  symbol   = %(symbol)s
  AND  bar_time = %(bar_time)s;
"""


def run_validation(conn, dry_run: bool, batch_size: int) -> dict:
    """
    Scan every row in harvest_5s_bars, validate bid/ask and call/put volume
    fields against harvest_live_ticks for the same 5-second window.

    For every bar where a required field is missing or zero:
      1. Query ticks in (bar_time − 5s, bar_time].
      2. Compute aggregates from those ticks.
      3. Merge computed values back into bar payload and UPDATE the row
         (unless dry_run=True).

    Returns a dict with per-symbol/day discrepancy counts and a global summary.
    """
    # per-symbol/per-day counters: {(symbol, market_day): {...}}
    report: dict[tuple, dict] = defaultdict(lambda: {
        "bars_checked":        0,
        "missing_bid_ask":     0,
        "missing_put_call":    0,
        "repaired_bid_ask":    0,
        "repaired_put_call":   0,
        "no_ticks_to_repair":  0,
        "sample_issues":       [],   # up to 5 sample rows
    })

    totals = {
        "bars_checked":       0,
        "bars_with_issues":   0,
        "bars_repaired":      0,
        "no_ticks_to_repair": 0,
    }

    offset = 0
    while True:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_FETCH_BARS_BATCH, {"limit": batch_size, "offset": offset})
            rows = cur.fetchall()

        if not rows:
            break

        offset += len(rows)
        totals["bars_checked"] += len(rows)

        for row in rows:
            symbol     = row["symbol"]
            bar_time   = row["bar_time"]   # already a datetime from psycopg2
            market_day = str(row["market_day"])

            payload: dict = row["payload"] if isinstance(row["payload"], dict) \
                else json.loads(row["payload"])

            key = (symbol, market_day)
            rpt = report[key]
            rpt["bars_checked"] += 1

            need_bid_ask  = _is_missing_or_zero(payload, BAR_BID_FIELDS + BAR_ASK_FIELDS)
            need_put_call = _is_missing_or_zero(payload, BAR_PUT_FIELDS + BAR_CALL_FIELDS)

            if not need_bid_ask and not need_put_call:
                continue

            totals["bars_with_issues"] += 1
            if need_bid_ask:
                rpt["missing_bid_ask"] += 1
            if need_put_call:
                rpt["missing_put_call"] += 1

            # ── fetch ticks for this 5-second window ──────────────────
            bar_time_utc = bar_time if bar_time.tzinfo else \
                bar_time.replace(tzinfo=timezone.utc)
            window_start = bar_time_utc - timedelta(seconds=5)

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as tcur:
                tcur.execute(_FETCH_TICKS_WINDOW, {
                    "symbol":       symbol,
                    "window_start": window_start.isoformat(),
                    "window_end":   bar_time_utc.isoformat(),
                })
                tick_rows = tcur.fetchall()

            tick_payloads = []
            for tr in tick_rows:
                tp = tr["payload"]
                if isinstance(tp, str):
                    try:
                        tp = json.loads(tp)
                    except (json.JSONDecodeError, TypeError):
                        tp = {}
                tick_payloads.append(tp if isinstance(tp, dict) else {})

            if not tick_payloads:
                rpt["no_ticks_to_repair"] += 1
                totals["no_ticks_to_repair"] += 1
                _record_sample(rpt, symbol, bar_time_utc, payload,
                               "missing_but_no_ticks")
                continue

            # ── compute aggregates and merge ──────────────────────────
            computed = _compute_aggregates_from_ticks(tick_payloads)
            repaired_payload = dict(payload)
            changed = False

            if need_bid_ask:
                filled = {}
                for k in ("Bid", "Ask", "BidLast", "AskLast",
                          "BidSize", "AskSize", "BidSizeLast", "AskSizeLast"):
                    if k in computed and _is_missing_or_zero(payload, (k,)):
                        filled[k] = computed[k]
                if filled:
                    repaired_payload.update(filled)
                    repaired_payload["_bid_ask_source"] = "tick_aggregate"
                    rpt["repaired_bid_ask"] += 1
                    changed = True
                    _record_sample(rpt, symbol, bar_time_utc, payload,
                                   f"bid_ask_repaired: {list(filled.keys())}")

            if need_put_call:
                filled = {}
                for k in ("PutVol", "CallVol",
                          "PutVolDelta5s", "CallVolDelta5s"):
                    if k in computed and _is_missing_or_zero(payload, (k,)):
                        filled[k] = computed[k]
                if filled:
                    repaired_payload.update(filled)
                    repaired_payload["_put_call_source"] = "tick_aggregate"
                    rpt["repaired_put_call"] += 1
                    changed = True
                    _record_sample(rpt, symbol, bar_time_utc, payload,
                                   f"put_call_repaired: {list(filled.keys())}")

            if changed:
                totals["bars_repaired"] += 1
                if not dry_run:
                    with conn.cursor() as ucur:
                        ucur.execute(_UPDATE_BAR, {
                            "symbol":   symbol,
                            "bar_time": bar_time_utc.isoformat(),
                            "payload":  json.dumps(repaired_payload),
                        })
                    conn.commit()

        print(f"[validate] processed {totals['bars_checked']} bars | "
              f"issues={totals['bars_with_issues']} "
              f"repaired={totals['bars_repaired']}")

    return {"by_symbol_day": dict(report), "totals": totals}


def _record_sample(rpt: dict, symbol: str, bar_time: datetime,
                   payload: dict, note: str):
    if len(rpt["sample_issues"]) >= 5:
        return
    rpt["sample_issues"].append({
        "symbol":   symbol,
        "bar_time": bar_time.isoformat(),
        "Bid":      payload.get("Bid"),
        "Ask":      payload.get("Ask"),
        "PutVol":   payload.get("PutVol"),
        "CallVol":  payload.get("CallVol"),
        "note":     note,
    })


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_import_report(stats: dict):
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    for kind in ("bars", "ticks", "news"):
        s = stats[kind]
        print(f"  {kind.upper():6s}  files={s['files']:4d}  "
              f"seen={s['seen']:10,d}  inserted={s['inserted']:10,d}  "
              f"dupes={s['dupes']:10,d}  skipped={s['skipped']:6,d}")
    print()


def print_validation_report(vr: dict):
    totals = vr["totals"]
    by_sd  = vr["by_symbol_day"]

    print("\n" + "=" * 60)
    print("VALIDATION / REPAIR SUMMARY")
    print("=" * 60)
    print(f"  Bars checked          : {totals['bars_checked']:,}")
    print(f"  Bars with issues      : {totals['bars_with_issues']:,}")
    print(f"  Bars repaired         : {totals['bars_repaired']:,}")
    print(f"  No ticks to repair    : {totals['no_ticks_to_repair']:,}")

    # Per-symbol/day breakdown (only rows that had issues)
    problem_rows = [
        (k, v) for k, v in sorted(by_sd.items())
        if v["missing_bid_ask"] or v["missing_put_call"]
    ]
    if problem_rows:
        print(f"\n  Per-symbol/day breakdown ({len(problem_rows)} impacted days):")
        print(f"  {'SYMBOL':<8} {'DATE':<12} {'miss_ba':>8} "
              f"{'miss_pc':>8} {'rep_ba':>8} {'rep_pc':>8} {'no_ticks':>9}")
        print("  " + "-" * 63)
        for (sym, day), v in problem_rows:
            print(f"  {sym:<8} {day:<12} {v['missing_bid_ask']:>8,} "
                  f"{v['missing_put_call']:>8,} "
                  f"{v['repaired_bid_ask']:>8,} "
                  f"{v['repaired_put_call']:>8,} "
                  f"{v['no_ticks_to_repair']:>9,}")

        print("\n  Sample issue rows (up to 5 per symbol/day):")
        for (sym, day), v in problem_rows[:10]:
            for sample in v["sample_issues"][:2]:
                print(f"    {sym} {day}  bar_time={sample['bar_time']}  "
                      f"Bid={sample['Bid']}  Ask={sample['Ask']}  "
                      f"PutVol={sample['PutVol']}  CallVol={sample['CallVol']}  "
                      f"→ {sample['note']}")
    else:
        print("\n  ✓ No issues found – all bars have bid/ask and put/call vol data.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--mode",
        choices=["import", "validate", "both"],
        default=os.getenv("MODE", "both"),
        help="Operation mode (default: both)",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("DATA_DIR", str(Path.home() / "Desktop" / "Data")),
        help="Root directory to scan for CSV files (import mode)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_bool_val("DRY_RUN", False),
        help="Print what would be done without writing to the DB",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "500")),
        help="Bars per validation batch (validate mode)",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=int(os.getenv("LOG_EVERY", "1000")),
        help="Log a progress line every N import rows",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()

    if args.dry_run:
        print("[DRY RUN] No data will be written to the database.\n")

    conn = _get_conn()
    try:
        if args.mode in ("import", "both"):
            if not data_dir.exists():
                print(f"[WARN] DATA_DIR does not exist: {data_dir}  (skipping import)")
            else:
                print(f"[import] Scanning {data_dir} …")
                import_stats = run_import(conn, data_dir, args.dry_run, args.log_every)
                print_import_report(import_stats)

        if args.mode in ("validate", "both"):
            print("[validate] Checking bid/ask and call/put volume in harvest_5s_bars …")
            val_report = run_validation(conn, args.dry_run, args.batch_size)
            print_validation_report(val_report)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
