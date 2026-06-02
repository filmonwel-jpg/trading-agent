#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import json
import math
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import databento as db

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from harvest_storage import load_postgres_settings

DEFAULT_DATASET = "DBEQ.BASIC"
DEFAULT_SCHEMA = "tbbo"
DEFAULT_STYPE_IN = "raw_symbol"
DEFAULT_MONTHS = 9


@dataclass(frozen=True)
class BackfillConfig:
    databento_api_key: str
    databento_user_id: str
    dataset: str
    schema: str
    stype_in: str
    symbols_file: Path
    symbols: list[str]
    start: datetime
    end: datetime
    chunk_days: int
    dry_run: bool
    root_dir: Path
    run_id: str | None
    resume_latest: bool
    max_retries: int
    retry_base_seconds: float
    retry_max_seconds: float
    pause_file: Path
    pause_on_network_error: bool


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Databento TBBO history for a symbol list and store into Postgres"
    )
    parser.add_argument("--symbols-file", default="databento_ibkr_bridge/config/symbols_100.txt")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS)
    parser.add_argument("--dataset", default=os.getenv("DATABENTO_DATASET", DEFAULT_DATASET))
    parser.add_argument("--schema", default=os.getenv("DATABENTO_SCHEMA", DEFAULT_SCHEMA))
    parser.add_argument("--stype-in", default=os.getenv("DATABENTO_STYPE_IN", DEFAULT_STYPE_IN))
    parser.add_argument("--chunk-days", type=int, default=14)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--resume-latest", action="store_true", default=True)
    parser.add_argument("--no-resume-latest", action="store_true")
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base-seconds", type=float, default=2.0)
    parser.add_argument("--retry-max-seconds", type=float, default=60.0)
    parser.add_argument("--pause-file", default="runtime/databento/state/pause_backfill.flag")
    parser.add_argument("--pause-on-network-error", action="store_true", default=True)
    parser.add_argument("--no-pause-on-network-error", action="store_true")
    args = parser.parse_args()
    if args.no_resume_latest:
        args.resume_latest = False
    if args.no_pause_on_network_error:
        args.pause_on_network_error = False
    return args


def _parse_symbols(symbols_file: Path, max_symbols: int) -> list[str]:
    if not symbols_file.exists():
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")

    seen: set[str] = set()
    symbols: list[str] = []
    for raw in symbols_file.read_text(encoding="utf-8").splitlines():
        symbol = raw.strip().upper()
        if not symbol or symbol.startswith("#"):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if max_symbols > 0:
        symbols = symbols[:max_symbols]

    if not symbols:
        raise ValueError("No symbols were loaded.")

    return symbols


def _month_window(months: int) -> tuple[datetime, datetime]:
    end = datetime.now(UTC)
    start = end - timedelta(days=months * 30)
    return start, end


def _load_config(root_dir: Path, args: argparse.Namespace) -> BackfillConfig:
    api_key = os.getenv("DATABENTO_API_KEY", "").strip()
    user_id = os.getenv("DATABENTO_USER_ID", "").strip()

    symbols_file = (root_dir / args.symbols_file).resolve()
    symbols = _parse_symbols(symbols_file, max_symbols=args.max_symbols)
    start, end = _month_window(args.months)

    if not args.dry_run and not api_key:
        raise ValueError("Set DATABENTO_API_KEY to run live backfill.")

    return BackfillConfig(
        databento_api_key=api_key,
        databento_user_id=user_id,
        dataset=args.dataset,
        schema=args.schema,
        stype_in=args.stype_in,
        symbols_file=symbols_file,
        symbols=symbols,
        start=start,
        end=end,
        chunk_days=args.chunk_days,
        dry_run=args.dry_run,
        root_dir=root_dir,
        run_id=args.run_id.strip() or None,
        resume_latest=args.resume_latest,
        max_retries=max(1, args.max_retries),
        retry_base_seconds=max(0.1, args.retry_base_seconds),
        retry_max_seconds=max(1.0, args.retry_max_seconds),
        pause_file=(root_dir / args.pause_file).resolve(),
        pause_on_network_error=args.pause_on_network_error,
    )


def _connect_db(root_dir: Path):
    settings = load_postgres_settings(root_dir)
    url = settings.get("url", "").strip()
    if not url:
        raise RuntimeError("Postgres URL is missing. Set HARVEST_DB_URL or spring.datasource.url")

    try:
        import psycopg  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError('Install psycopg first: pip install "psycopg[binary]"') from exc

    # Only pass explicit credentials when present. This preserves libpq fallbacks
    # such as URL credentials, PG* env vars, and ~/.pgpass.
    conn_kwargs: dict[str, Any] = {
        "autocommit": True,
        "application_name": "databento_tbbo_backfill",
    }
    username = settings.get("username", "").strip()
    password = settings.get("password")
    if username:
        conn_kwargs["user"] = username
    if password:
        conn_kwargs["password"] = password

    try:
        conn = psycopg.connect(url, **conn_kwargs)
    except psycopg.OperationalError as exc:
        msg = str(exc)
        if "fe_sendauth: no password supplied" in msg:
            raise RuntimeError(
                "Postgres authentication failed: no password supplied. "
                "Set HARVEST_DB_PASSWORD (or SPRING_DATASOURCE_PASSWORD), "
                "or embed credentials in HARVEST_DB_URL/spring.datasource.url, "
                "or configure ~/.pgpass."
            ) from exc
        raise
    return conn


def _ensure_table(conn) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS databento_tbbo (
        record_hash TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        ts_event TIMESTAMPTZ NOT NULL,
        market_day DATE NOT NULL,
        dataset TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        stype_in TEXT NOT NULL,
        publisher_id INTEGER,
        sequence BIGINT,
        bid_px DOUBLE PRECISION,
        bid_sz DOUBLE PRECISION,
        ask_px DOUBLE PRECISION,
        ask_sz DOUBLE PRECISION,
        trade_px DOUBLE PRECISION,
        trade_sz DOUBLE PRECISION,
        side TEXT,
        action TEXT,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
    idx = "CREATE INDEX IF NOT EXISTS idx_databento_tbbo_symbol_time ON databento_tbbo (symbol, ts_event)"
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(idx)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS databento_backfill_runs (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                dataset TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                stype_in TEXT NOT NULL,
                symbols_file TEXT NOT NULL,
                symbols_total INTEGER NOT NULL,
                start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL,
                chunk_days INTEGER NOT NULL,
                symbols_completed INTEGER NOT NULL DEFAULT 0,
                chunks_completed INTEGER NOT NULL DEFAULT 0,
                rows_upserted BIGINT NOT NULL DEFAULT 0,
                last_symbol TEXT,
                last_error TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS databento_backfill_progress (
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                chunk_start TIMESTAMPTZ NOT NULL,
                chunk_end TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                rows_upserted INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, symbol, chunk_start, chunk_end),
                FOREIGN KEY (run_id) REFERENCES databento_backfill_runs(run_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_databento_backfill_progress_run_symbol ON databento_backfill_progress (run_id, symbol, status)"
        )


def _make_run_id(cfg: BackfillConfig) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    sig = hashlib.sha1(
        f"{cfg.dataset}|{cfg.schema}|{cfg.stype_in}|{cfg.start.isoformat()}|{cfg.end.isoformat()}|{cfg.chunk_days}|{cfg.symbols_file}".encode(
            "utf-8"
        )
    ).hexdigest()[:10]
    return f"tbbo-{stamp}-{sig}"


def _find_latest_resumable_run(conn, cfg: BackfillConfig) -> str | None:
    sql = """
    SELECT run_id
    FROM databento_backfill_runs
    WHERE status IN ('running', 'paused', 'failed')
      AND dataset = %s
      AND schema_name = %s
      AND stype_in = %s
      AND symbols_file = %s
    ORDER BY updated_at DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                cfg.dataset,
                cfg.schema,
                cfg.stype_in,
                str(cfg.symbols_file),
            ),
        )
        row = cur.fetchone()
    return str(row[0]) if row else None


def _load_run_params(conn, run_id: str) -> tuple[datetime, datetime, int] | None:
    sql = """
    SELECT start_ts, end_ts, chunk_days
    FROM databento_backfill_runs
    WHERE run_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        row = cur.fetchone()
    if not row:
        return None
    return row[0], row[1], int(row[2])


def _resolve_run_id(conn, cfg: BackfillConfig) -> str:
    if cfg.run_id:
        return cfg.run_id
    if cfg.resume_latest:
        resumable = _find_latest_resumable_run(conn, cfg)
        if resumable:
            return resumable
    return _make_run_id(cfg)


def _upsert_run_header(conn, cfg: BackfillConfig, run_id: str) -> None:
    sql = """
    INSERT INTO databento_backfill_runs (
        run_id, status, dataset, schema_name, stype_in, symbols_file, symbols_total,
        start_ts, end_ts, chunk_days, started_at, updated_at
    )
    VALUES (%s, 'running', %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (run_id)
    DO UPDATE SET
        status = 'running',
        dataset = EXCLUDED.dataset,
        schema_name = EXCLUDED.schema_name,
        stype_in = EXCLUDED.stype_in,
        symbols_file = EXCLUDED.symbols_file,
        symbols_total = EXCLUDED.symbols_total,
        start_ts = EXCLUDED.start_ts,
        end_ts = EXCLUDED.end_ts,
        chunk_days = EXCLUDED.chunk_days,
        updated_at = CURRENT_TIMESTAMP,
        finished_at = NULL
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                run_id,
                cfg.dataset,
                cfg.schema,
                cfg.stype_in,
                str(cfg.symbols_file),
                len(cfg.symbols),
                cfg.start,
                cfg.end,
                cfg.chunk_days,
            ),
        )


def _update_run_stats(conn, run_id: str, last_symbol: str | None = None, last_error: str | None = None) -> None:
    sql = """
    WITH agg AS (
        SELECT
            COALESCE(SUM(rows_upserted), 0) AS rows_upserted,
            COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS chunks_completed,
            COALESCE(COUNT(DISTINCT CASE WHEN s.done THEN symbol END), 0) AS symbols_completed
        FROM databento_backfill_progress p
        LEFT JOIN (
            SELECT run_id AS r_id, symbol AS s_symbol,
                   BOOL_AND(status = 'completed') AS done
            FROM databento_backfill_progress
            WHERE run_id = %s
            GROUP BY run_id, symbol
        ) s ON p.run_id = s.r_id AND p.symbol = s.s_symbol
        WHERE p.run_id = %s
    )
    UPDATE databento_backfill_runs r
    SET rows_upserted = agg.rows_upserted,
        chunks_completed = agg.chunks_completed,
        symbols_completed = agg.symbols_completed,
        last_symbol = COALESCE(%s, r.last_symbol),
        last_error = %s,
        updated_at = CURRENT_TIMESTAMP
    FROM agg
    WHERE r.run_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, run_id, last_symbol, last_error, run_id))


def _set_run_status(conn, run_id: str, status: str, last_error: str | None = None) -> None:
    finished = status in {"completed", "failed", "paused"}
    sql = """
    UPDATE databento_backfill_runs
    SET status = %s,
        last_error = %s,
        updated_at = CURRENT_TIMESTAMP,
        finished_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finished_at END
    WHERE run_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, last_error, finished, run_id))


def _chunk_status(conn, run_id: str, symbol: str, chunk_start: datetime, chunk_end: datetime) -> str | None:
    sql = """
    SELECT status
    FROM databento_backfill_progress
    WHERE run_id = %s AND symbol = %s AND chunk_start = %s AND chunk_end = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, symbol, chunk_start, chunk_end))
        row = cur.fetchone()
    return str(row[0]) if row else None


def _mark_chunk_status(
    conn,
    run_id: str,
    symbol: str,
    chunk_start: datetime,
    chunk_end: datetime,
    status: str,
    rows_upserted: int = 0,
    last_error: str | None = None,
    increment_attempt: bool = True,
) -> None:
    sql = """
    INSERT INTO databento_backfill_progress (
        run_id, symbol, chunk_start, chunk_end, status, attempts, rows_upserted, last_error, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (run_id, symbol, chunk_start, chunk_end)
    DO UPDATE SET
        status = EXCLUDED.status,
        attempts = databento_backfill_progress.attempts + %s,
        rows_upserted = EXCLUDED.rows_upserted,
        last_error = EXCLUDED.last_error,
        updated_at = CURRENT_TIMESTAMP
    """
    inc = 1 if increment_attempt else 0
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (run_id, symbol, chunk_start, chunk_end, status, inc, rows_upserted, last_error, inc),
        )


def _safe_float(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _record_hash(symbol: str, ts_event: str, row: dict[str, Any]) -> str:
    pieces = [
        symbol,
        ts_event,
        str(row.get("publisher_id", "")),
        str(row.get("sequence", "")),
        str(row.get("bid_px_00", "")),
        str(row.get("ask_px_00", "")),
        str(row.get("price", "")),
        str(row.get("size", "")),
    ]
    return hashlib.sha1("|".join(pieces).encode("utf-8")).hexdigest()


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    # Handle pandas/NumPy scalar wrappers (e.g., Timestamp, int64, float64, NaT).
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _to_jsonable(item())
        except Exception:
            pass

    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass

    text = str(value)
    if text in {"nan", "NaN", "NaT", "<NA>"}:
        return None
    return text


def _normalize_rows(symbol: str, rows: Iterable[dict[str, Any]], cfg: BackfillConfig) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    for row in rows:
        ts_event = str(row.get("ts_event", ""))
        if not ts_event:
            continue

        try:
            ts_dt = datetime.fromisoformat(ts_event.replace("Z", "+00:00"))
        except ValueError:
            continue

        record_hash = _record_hash(symbol, ts_event, row)
        out.append(
            (
                record_hash,
                symbol,
                ts_dt,
                ts_dt.date(),
                cfg.dataset,
                cfg.schema,
                cfg.stype_in,
                row.get("publisher_id"),
                row.get("sequence"),
                _safe_float(row.get("bid_px_00")),
                _safe_float(row.get("bid_sz_00")),
                _safe_float(row.get("ask_px_00")),
                _safe_float(row.get("ask_sz_00")),
                _safe_float(row.get("price")),
                _safe_float(row.get("size")),
                str(row.get("side")) if row.get("side") is not None else None,
                str(row.get("action")) if row.get("action") is not None else None,
                json.dumps(_to_jsonable(row), separators=(",", ":")),
            )
        )
    return out


def _insert_rows(conn, records: list[tuple[Any, ...]]) -> int:
    if not records:
        return 0

    sql = """
    INSERT INTO databento_tbbo (
        record_hash, symbol, ts_event, market_day, dataset, schema_name, stype_in,
        publisher_id, sequence, bid_px, bid_sz, ask_px, ask_sz, trade_px, trade_sz,
        side, action, payload, created_at, updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (record_hash)
    DO UPDATE SET
        payload = EXCLUDED.payload,
        bid_px = EXCLUDED.bid_px,
        bid_sz = EXCLUDED.bid_sz,
        ask_px = EXCLUDED.ask_px,
        ask_sz = EXCLUDED.ask_sz,
        trade_px = EXCLUDED.trade_px,
        trade_sz = EXCLUDED.trade_sz,
        side = EXCLUDED.side,
        action = EXCLUDED.action,
        updated_at = CURRENT_TIMESTAMP
    """

    with conn.cursor() as cur:
        cur.executemany(sql, records)
    return len(records)


def _iter_ranges(start: datetime, end: datetime, chunk_days: int) -> Iterable[tuple[datetime, datetime]]:
    cursor = start
    step = timedelta(days=max(1, chunk_days))
    while cursor < end:
        chunk_end = min(cursor + step, end)
        yield cursor, chunk_end
        cursor = chunk_end


def _is_retryable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    retry_markers = [
        "429",
        "rate",
        "timeout",
        "timed out",
        "temporarily",
        "connection",
        "reset",
        "unavailable",
        "503",
        "502",
        "504",
    ]
    return any(marker in text for marker in retry_markers)


def _is_data_end_after_available_end(exc: Exception) -> bool:
    text = str(exc).lower()
    return "data_end_after_available_end" in text


def _is_data_start_after_available_end(exc: Exception) -> bool:
    text = str(exc).lower()
    return "data_start_after_available_end" in text


def _extract_available_end(exc: Exception) -> datetime | None:
    msg = str(exc)
    candidates: list[str] = []

    up_to = re.search(r"available up to '([^']+)'", msg)
    if up_to:
        candidates.append(up_to.group(1))

    available_end = re.search(r"available end[^\n]*\('([^']+)'\)", msg, flags=re.IGNORECASE)
    if available_end:
        candidates.append(available_end.group(1))

    for raw in candidates:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    return None


def _download_chunk_with_retry(
    cfg: BackfillConfig,
    client: db.Historical,
    symbol: str,
    chunk_start: datetime,
    chunk_end: datetime,
) -> Any:
    attempt = 0
    while True:
        try:
            return client.timeseries.get_range(
                dataset=cfg.dataset,
                schema=cfg.schema,
                stype_in=cfg.stype_in,
                symbols=[symbol],
                start=chunk_start,
                end=chunk_end,
            )
        except Exception as exc:
            attempt += 1
            if attempt >= cfg.max_retries or not _is_retryable_error(exc):
                raise
            wait_seconds = min(cfg.retry_max_seconds, cfg.retry_base_seconds * (2 ** (attempt - 1)))
            wait_seconds += random.uniform(0.0, 0.25 * wait_seconds)
            print(
                f"[TBBO] retry symbol={symbol} chunk={chunk_start.date()}->{chunk_end.date()} "
                f"attempt={attempt}/{cfg.max_retries} wait={wait_seconds:.1f}s error={exc}"
            )
            import time

            time.sleep(wait_seconds)


def _pause_requested(cfg: BackfillConfig) -> bool:
    return cfg.pause_file.exists()


def _download_symbol(cfg: BackfillConfig, client: db.Historical, conn, run_id: str, symbol: str) -> tuple[int, int, bool]:
    chunk_count = 0
    row_count = 0
    paused = False

    stop_at_dataset_tail = False
    for chunk_start, chunk_end in _iter_ranges(cfg.start, cfg.end, cfg.chunk_days):
        if _pause_requested(cfg):
            print(f"[TBBO] pause requested via file: {cfg.pause_file}")
            paused = True
            break

        prior_status = _chunk_status(conn, run_id, symbol, chunk_start, chunk_end)
        if prior_status == "completed":
            continue

        chunk_count += 1
        _mark_chunk_status(conn, run_id, symbol, chunk_start, chunk_end, status="running")
        try:
            effective_chunk_end = chunk_end
            while True:
                try:
                    data = _download_chunk_with_retry(cfg, client, symbol, chunk_start, effective_chunk_end)
                    break
                except Exception as exc:
                    is_end_error = _is_data_end_after_available_end(exc)
                    is_start_error = _is_data_start_after_available_end(exc)
                    if not (is_end_error or is_start_error):
                        raise

                    available_end = _extract_available_end(exc)
                    if available_end is None:
                        raise

                    if is_start_error or available_end <= chunk_start:
                        # No data is available for this chunk or any future chunk in this run window.
                        print(
                            f"[TBBO] {symbol} reached dataset tail at {available_end.isoformat()} "
                            f"(chunk start {chunk_start.isoformat()}); stopping symbol."
                        )
                        _mark_chunk_status(
                            conn,
                            run_id,
                            symbol,
                            chunk_start,
                            chunk_end,
                            status="completed",
                            rows_upserted=0,
                            increment_attempt=False,
                        )
                        stop_at_dataset_tail = True
                        data = None
                        break

                    if available_end >= effective_chunk_end:
                        raise

                    print(
                        f"[TBBO] clamping {symbol} chunk end {effective_chunk_end.isoformat()} "
                        f"-> {available_end.isoformat()} due to dataset availability"
                    )
                    effective_chunk_end = available_end

            if data is None:
                break

            frame = data.to_df().reset_index(drop=False)
            if frame.empty:
                _mark_chunk_status(
                    conn,
                    run_id,
                    symbol,
                    chunk_start,
                    chunk_end,
                    status="completed",
                    rows_upserted=0,
                    increment_attempt=False,
                )
                print(f"[TBBO] {symbol} {chunk_start.date()}->{chunk_end.date()} rows=0")
                continue

            records = _normalize_rows(symbol, frame.to_dict(orient="records"), cfg)
            inserted = _insert_rows(conn, records)
            row_count += inserted
            _mark_chunk_status(
                conn,
                run_id,
                symbol,
                chunk_start,
                chunk_end,
                status="completed",
                rows_upserted=inserted,
                increment_attempt=False,
            )
            print(f"[TBBO] {symbol} {chunk_start.date()}->{effective_chunk_end.date()} rows={inserted}")
        except Exception as exc:
            _mark_chunk_status(
                conn,
                run_id,
                symbol,
                chunk_start,
                chunk_end,
                status="failed",
                rows_upserted=0,
                last_error=str(exc)[:1000],
                increment_attempt=False,
            )
            _update_run_stats(conn, run_id=run_id, last_symbol=symbol, last_error=str(exc)[:1000])
            if cfg.pause_on_network_error and _is_retryable_error(exc):
                print(f"[TBBO] network error, pausing run for resume later: {exc}")
                paused = True
                break
            raise

        _update_run_stats(conn, run_id=run_id, last_symbol=symbol)

        if stop_at_dataset_tail:
            break

    return chunk_count, row_count, paused


def main() -> int:
    args = _parse_args()
    root_dir = ROOT_DIR
    cfg = _load_config(root_dir, args)

    print(f"[TBBO] symbols_file={cfg.symbols_file}")
    print(f"[TBBO] symbols_count={len(cfg.symbols)}")
    print(f"[TBBO] time_window={cfg.start.isoformat()} -> {cfg.end.isoformat()}")
    print(f"[TBBO] dataset={cfg.dataset} schema={cfg.schema} stype_in={cfg.stype_in}")
    print(f"[TBBO] databento_user_id={cfg.databento_user_id or '<unset>'}")
    print(f"[TBBO] pause_file={cfg.pause_file}")

    if cfg.dry_run:
        print("[TBBO] Dry run only. No Databento request and no DB writes were performed.")
        return 0

    conn = _connect_db(cfg.root_dir)
    _ensure_table(conn)
    run_id = _resolve_run_id(conn, cfg)
    prior = _load_run_params(conn, run_id)
    if prior is not None:
        cfg = replace(cfg, start=prior[0], end=prior[1], chunk_days=prior[2])
    _upsert_run_header(conn, cfg, run_id)
    _update_run_stats(conn, run_id=run_id)
    print(f"[TBBO] run_id={run_id}")

    total_chunks = 0
    total_rows = 0
    paused = False
    try:
        client = db.Historical(cfg.databento_api_key)
        for idx, symbol in enumerate(cfg.symbols, start=1):
            print(f"[TBBO] ({idx}/{len(cfg.symbols)}) downloading {symbol}")
            chunks, rows, paused = _download_symbol(cfg, client, conn, run_id, symbol)
            total_chunks += chunks
            total_rows += rows
            if paused:
                break

        if paused:
            _set_run_status(conn, run_id, status="paused")
        else:
            _update_run_stats(conn, run_id=run_id)
            _set_run_status(conn, run_id, status="completed")
    except Exception as exc:
        _set_run_status(conn, run_id, status="failed", last_error=str(exc)[:1000])
        print(f"[TBBO] FAILED run_id={run_id} error={exc}")
        raise
    finally:
        conn.close()

    final_status = "PAUSED" if paused else "COMPLETED"
    print(f"[TBBO] {final_status}. run_id={run_id} chunks={total_chunks} rows_upserted={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


