#!/usr/bin/env python3
"""Shared helpers for Databento Phase 1 silver normalizers."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


MARKET_TZ = "America/New_York"
RTH_OPEN_MINUTE = 9 * 60 + 30
RTH_CLOSE_MINUTE = 16 * 60
SESSION_SECONDS = (RTH_CLOSE_MINUTE - RTH_OPEN_MINUTE) * 60
OPTION_SYMBOL_RE = re.compile(r"^\s*([A-Z]{1,8})(?:\s+)?(\d{6}|\d{8})([CP])(\d{1,9})\s*$")


@dataclass(frozen=True)
class PilotSourceFile:
    date: str
    source_label: str
    dataset: str
    schema: str
    path: Path
    name: str
    bytes: int
    sha256_status: str = ""
    sha256: str = ""
    sha256_error: str = ""


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def non_empty_path_arg(raw: str) -> Path:
    if not str(raw or "").strip():
        raise argparse.ArgumentTypeError("path argument must not be empty; check the corresponding shell variable")
    return Path(raw).expanduser()


def normalize_date(raw: object) -> str:
    value = str(raw or "").strip().replace("-", "")
    if len(value) != 8 or not value.isdigit() or not value.startswith("20"):
        raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD, got {raw!r}")
    return value


def normalize_date_arg(raw: str) -> str:
    try:
        return normalize_date(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def iso_date(date: str) -> str:
    normalized = normalize_date(date)
    return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"


def split_symbols(raw: str | Iterable[str] | None) -> list[str]:
    if raw is None:
        return []
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = str(part or "").strip().upper()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _int(raw: object, default: int = 0) -> int:
    try:
        if raw is None or str(raw).strip() == "":
            return default
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _float(raw: object, default: float = 0.0) -> float:
    try:
        if raw is None or str(raw).strip() == "":
            return default
        value = float(str(raw).strip())
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def load_pilot_source_files(
    pilot_source_files_path: Path,
    *,
    source_label: str | None = None,
    source_labels: set[str] | None = None,
    dates: set[str] | None = None,
) -> list[PilotSourceFile]:
    wanted_labels = {source_label} if source_label else source_labels
    wanted_dates = {normalize_date(date) for date in dates} if dates else None
    files: list[PilotSourceFile] = []
    for row in read_csv_rows(pilot_source_files_path):
        label = str(row.get("source_label", "")).strip()
        if wanted_labels is not None and label not in wanted_labels:
            continue
        try:
            date = normalize_date(row.get("date", ""))
        except ValueError:
            continue
        if wanted_dates is not None and date not in wanted_dates:
            continue
        path = Path(str(row.get("checked_path") or row.get("path") or "").strip()).expanduser()
        files.append(
            PilotSourceFile(
                date=date,
                source_label=label,
                dataset=str(row.get("dataset", "")).strip(),
                schema=str(row.get("schema", "")).strip(),
                path=path,
                name=str(row.get("name", "")).strip() or path.name,
                bytes=_int(row.get("bytes")),
                sha256_status=str(row.get("sha256_status", "")).strip(),
                sha256=str(row.get("sha256", "")).strip(),
                sha256_error=str(row.get("sha256_error", "")).strip(),
            )
        )
    return sorted(files, key=lambda item: (item.date, item.source_label, str(item.path)))


def require_databento() -> Any:
    try:
        import databento as db
    except ImportError as exc:  # pragma: no cover - exercised only without dependency installed.
        raise RuntimeError("databento package is required; install requirements.txt first") from exc
    return db


def load_dbn_frame(path: Path) -> pd.DataFrame:
    db = require_databento()
    frame = db.DBNStore.from_file(path).to_df().reset_index(drop=False)
    return ensure_ts_event_column(frame)


def ensure_ts_event_column(frame: pd.DataFrame) -> pd.DataFrame:
    if "ts_event" not in frame.columns and "index" in frame.columns:
        return frame.rename(columns={"index": "ts_event"})
    return frame


def regular_session_mask_and_local_ts(ts_event: pd.Series) -> tuple[pd.Series, pd.Series]:
    local_ts = pd.to_datetime(ts_event, utc=True, errors="coerce").dt.tz_convert(MARKET_TZ)
    minute = local_ts.dt.hour * 60 + local_ts.dt.minute
    mask = local_ts.notna() & (minute >= RTH_OPEN_MINUTE) & (minute < RTH_CLOSE_MINUTE)
    return mask, local_ts


def session_index(date: str, freq: str = "1s") -> pd.DatetimeIndex:
    start = pd.Timestamp(f"{iso_date(date)} 09:30:00", tz=MARKET_TZ)
    end = pd.Timestamp(f"{iso_date(date)} 16:00:00", tz=MARKET_TZ)
    return pd.date_range(start=start, end=end - pd.Timedelta(seconds=1), freq=freq)


def local_timestamp_strings(index: pd.DatetimeIndex) -> pd.Index:
    return index.strftime("%Y%m%d %H:%M:%S") + f" {MARKET_TZ}"


def bar_epoch_seconds(index: pd.DatetimeIndex) -> np.ndarray:
    return np.array([int(ts.tz_convert("UTC").timestamp()) for ts in index], dtype=np.int64)


def session_bucket_values(index: pd.DatetimeIndex) -> np.ndarray:
    minute = index.hour * 60 + index.minute
    return np.where(minute < 11 * 60, "open", np.where(minute < 15 * 60, "midday", "close"))


def seconds_from_open(index: pd.DatetimeIndex) -> np.ndarray:
    minute = index.hour * 60 + index.minute
    return ((minute - RTH_OPEN_MINUTE) * 60 + index.second).astype(int)


def numeric_series(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def coalesce_numeric(frame: pd.DataFrame, candidates: list[str], default: float = np.nan) -> pd.Series:
    out = pd.Series(np.nan, index=frame.index, dtype="float64")
    for column in candidates:
        if column in frame.columns:
            out = out.combine_first(pd.to_numeric(frame[column], errors="coerce"))
    if not pd.isna(default):
        out = out.fillna(default)
    return out


def safe_divide(numerator: pd.Series, denominator: pd.Series, default: float = np.nan) -> pd.Series:
    out = numerator / denominator.replace(0.0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).fillna(default)


def parse_option_symbol(symbol: object) -> dict[str, Any]:
    text = str(symbol or "").strip().upper()
    match = OPTION_SYMBOL_RE.match(text)
    if not match:
        return {"underlying": "", "expiration": "", "option_right": "", "strike": np.nan}
    underlying, expiry_token, right, strike_token = match.groups()
    if len(expiry_token) == 6:
        expiration = f"20{expiry_token[:2]}-{expiry_token[2:4]}-{expiry_token[4:6]}"
    else:
        expiration = f"{expiry_token[:4]}-{expiry_token[4:6]}-{expiry_token[6:8]}"
    strike = int(strike_token) / 1000.0
    return {"underlying": underlying, "expiration": expiration, "option_right": right, "strike": strike}


def option_meta_for_symbols(symbols: Iterable[object]) -> dict[str, dict[str, Any]]:
    return {str(symbol or "").strip().upper(): parse_option_symbol(symbol) for symbol in set(symbols)}


def normalize_expiration(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    if len(digits) == 6:
        return f"20{digits[:2]}-{digits[2:4]}-{digits[4:6]}"
    try:
        parsed = pd.to_datetime(text, utc=True, errors="coerce")
        if pd.notna(parsed):
            return parsed.date().isoformat()
    except Exception:
        pass
    return text


def normalize_fixed_price(raw: object, fallback: float = np.nan) -> float:
    value = _float(raw, default=fallback)
    if pd.isna(value):
        return fallback
    if abs(value) > 1_000_000:
        return value / 1_000_000_000.0
    return value




