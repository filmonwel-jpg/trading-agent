#!/usr/bin/env python3
"""Train position-aware lifecycle exits and 5-second micro-entry/exit-guard models.

This script is intentionally separate from train_30s_models.py so the existing production
30-second training path remains stable while the upgraded lifecycle/micro architecture is
iterated. It consumes the combined 30s/5s Databento CSVs produced by
build_30s_from_5s_csv.py / scripts/run_parallel_databento_build_20260523.sh.
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

try:
    import onnx  # noqa: F401
except Exception:  # pragma: no cover - optional diagnostics only
    onnx = None

MARKET_TZ = "America/New_York"
DEFAULT_MODEL_EXPORTS_ROOT = Path(os.getenv("MODEL_EXPORTS_ROOT", "model_exports")).expanduser()

ENTRY_PROFIT_PCT = float(os.getenv("LIFECYCLE_ENTRY_PROFIT_PCT", "0.0035"))
ENTRY_RISK_PCT = float(os.getenv("LIFECYCLE_ENTRY_RISK_PCT", "0.0025"))
LIFECYCLE_HORIZON_30S = int(os.getenv("LIFECYCLE_HORIZON_30S", "20"))
MICRO_ARM_TTL_SECONDS = int(os.getenv("MICRO_ARM_TTL_SECONDS", "60"))
MICRO_FUTURE_WINDOW_5S = int(os.getenv("MICRO_FUTURE_WINDOW_5S", "24"))
MICRO_EXIT_LOOKAHEAD_5S = int(os.getenv("MICRO_EXIT_LOOKAHEAD_5S", "6"))
EXIT_DECISION_MARGIN_R = float(os.getenv("EXIT_DECISION_MARGIN_R", "0.10"))
MICRO_EXIT_MARGIN_R = float(os.getenv("MICRO_EXIT_MARGIN_R", "0.05"))
ENTRY_FILL_MODE = os.getenv("ENTRY_FILL_MODE", "next_open").strip().lower()
ENTRY_SLIPPAGE_BPS = float(os.getenv("ENTRY_SLIPPAGE_BPS", "2.0"))
EXIT_SLIPPAGE_BPS = float(os.getenv("EXIT_SLIPPAGE_BPS", "2.0"))

THRESHOLD_RANGES = {
    "lifecycle": np.arange(0.50, 0.82, 0.02),
    "micro_entry": np.arange(0.50, 0.82, 0.02),
    "micro_exit_guard": np.arange(0.60, 0.92, 0.02),
}

NON_FEATURE_COLUMNS = {
    "Symbol", "Timestamp", "Date", "MarketRegime", "RegimeLabel", "Side", "EntryTime",
    "Label_Long_Entry", "Label_Short_Entry", "Label_Long_Exit", "Label_Short_Exit",
    "Label_Long_MicroEntry", "Label_Short_MicroEntry", "Label_Long_MicroExitGuard", "Label_Short_MicroExitGuard",
    "Label_Long_ExitLifecycle", "Label_Short_ExitLifecycle",
}


@dataclass(frozen=True)
class TrainedModelResult:
    name: str
    filename: str
    threshold: float
    precision: float
    recall: float
    pred_pos_rate: float
    label_pos_rate: float
    rows: int
    positives: int
    feature_count: int
    feature_columns: list[str]
    exported_to: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train lifecycle and 5s micro models from Databento 30s/5s CSVs.")
    parser.add_argument("--input-30s-csv", required=True, help="Combined/per-symbol 30s CSV, or a directory of *_30s_training.csv files.")
    parser.add_argument("--input-5s-csv", required=True, help="Combined/per-symbol 5s CSV, or a directory of *_5s_training.csv files.")
    parser.add_argument("--output-dir", default="", help="Model export directory. Defaults to model_exports/lifecycle_micro_<timestamp>.")
    parser.add_argument("--max-entry-events", type=int, default=0, help="Optional cap for faster smoke runs.")
    parser.add_argument(
        "--max-train-rows-per-model",
        type=int,
        default=int(os.getenv("LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL", "750000")),
        help="Maximum staged rows loaded into memory per model. 0 means load all staged rows.",
    )
    parser.add_argument(
        "--max-staged-rows-per-symbol-per-model",
        type=int,
        default=int(os.getenv("LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL", "20000")),
        help="Maximum derived rows written per symbol/model dataset. 0 means stage all derived rows.",
    )
    parser.add_argument(
        "--max-entry-events-per-symbol-side",
        type=int,
        default=int(os.getenv("LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE", "500")),
        help="Maximum labeled setup events expanded per symbol/side. 0 means expand every setup event.",
    )
    parser.add_argument("--staging-dir", default="", help="Directory for streamed intermediate lifecycle/micro datasets.")
    parser.add_argument("--min-rows", type=int, default=200, help="Minimum rows required to train a model.")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--no-onnx", action="store_true", help="Train and score but do not export ONNX.")
    return parser.parse_args()


def parse_timestamp(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip()
    extracted = raw.str.extract(r"(\d{8}\s+\d{2}:\d{2}:\d{2})")[0]
    parsed = pd.to_datetime(extracted, format="%Y%m%d %H:%M:%S", errors="coerce")
    missing = parsed.isna()
    if missing.any():
        fallback = pd.to_datetime(raw[missing], errors="coerce")
        parsed.loc[missing] = fallback
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(MARKET_TZ, nonexistent="shift_forward", ambiguous="NaT")
    else:
        parsed = parsed.dt.tz_convert(MARKET_TZ)
    if parsed.isna().any():
        examples = raw[parsed.isna()].head(5).tolist()
        raise ValueError(f"Unable to parse timestamps. Examples={examples}")
    return parsed


def load_bar_csv(path: str, cadence: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Input {cadence} CSV is empty: {path}")
    if "Timestamp" not in df.columns:
        raise ValueError(f"Input {cadence} CSV missing Timestamp: {path}")
    if "Symbol" not in df.columns:
        df.insert(0, "Symbol", "SINGLE")
    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper().replace("", "SINGLE")
    df["_ts"] = parse_timestamp(df["Timestamp"])
    for col in ["Open", "High", "Low", "Close", "Volume", "WAP", "Count"]:
        if col not in df.columns:
            df[col] = df["Close"] if col in {"Open", "High", "Low", "WAP"} else 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Close"] = df["Close"].ffill().bfill()
    for col in ["Open", "High", "Low", "WAP"]:
        df[col] = df[col].fillna(df["Close"])
    df["Volume"] = df["Volume"].fillna(0.0)
    df = df.sort_values(["Symbol", "_ts"]).reset_index(drop=True)
    df["Date"] = df["_ts"].dt.strftime("%Y-%m-%d")
    return add_common_features(df, cadence)


def numeric_col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[name], errors="coerce").fillna(default)


def add_common_features(df: pd.DataFrame, cadence: str) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby("Symbol", sort=False)
    close = numeric_col(out, "Close")
    open_ = numeric_col(out, "Open")
    high = numeric_col(out, "High")
    low = numeric_col(out, "Low")
    volume = numeric_col(out, "Volume")
    wap = numeric_col(out, "WAP").replace(0.0, np.nan).fillna(close)
    prev_close = grouped["Close"].shift(1).replace(0.0, np.nan)

    prefix = f"f_{cadence}_"
    out[prefix + "ret_1"] = (close / prev_close - 1.0).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "ret_3"] = grouped["Close"].pct_change(3).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "range_pct"] = ((high - low) / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "body_pct"] = ((close - open_) / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "upper_wick_pct"] = ((high - np.maximum(open_, close)) / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "lower_wick_pct"] = ((np.minimum(open_, close) - low) / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "vwap_dist"] = ((close - wap) / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    out[prefix + "rel_volume_20"] = (volume / (grouped["Volume"].transform(lambda s: s.rolling(20, min_periods=1).mean()) + 1.0)).fillna(0.0)
    out[prefix + "realized_vol_20"] = grouped[prefix + "ret_1"].transform(lambda s: s.rolling(20, min_periods=2).std(ddof=0)).fillna(0.0)

    spread = numeric_col(out, "SpreadBps", np.nan)
    if spread.isna().all():
        bid = numeric_col(out, "Bid", 0.0)
        ask = numeric_col(out, "Ask", 0.0)
        mid = (bid + ask) / 2.0
        spread = ((ask - bid) / mid.replace(0.0, np.nan)) * 10000.0
    out[prefix + "spread_bps"] = spread.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    put_delta = numeric_col(out, "PutVolDelta5s", 0.0)
    call_delta = numeric_col(out, "CallVolDelta5s", 0.0)
    put_total = numeric_col(out, "PutVol", put_delta.cumsum())
    call_total = numeric_col(out, "CallVol", call_delta.cumsum())
    out[prefix + "option_put_delta"] = put_delta
    out[prefix + "option_call_delta"] = call_delta
    out[prefix + "option_put_call_ratio"] = put_total / (call_total + 1.0)
    out[prefix + "option_delta_put_call_ratio"] = put_delta / (call_delta + 1.0)
    option_flow = put_delta + call_delta
    option_flow_mean = option_flow.groupby(out["Symbol"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    out[prefix + "option_volume_burst"] = option_flow / (option_flow_mean + 1.0)

    minute_of_day = out["_ts"].dt.hour * 60 + out["_ts"].dt.minute
    out[prefix + "time_of_day"] = ((minute_of_day - (9 * 60 + 30)) / (6.5 * 60)).clip(0.0, 1.0)
    out[prefix + "is_open_hour"] = (minute_of_day < 10 * 60 + 30).astype(float)
    out[prefix + "is_close_hour"] = (minute_of_day >= 15 * 60).astype(float)

    for col in out.columns:
        if col.startswith("f_"):
            out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return out


def assign_simple_regime(df30: pd.DataFrame) -> pd.DataFrame:
    out = df30.copy()
    ret = numeric_col(out, "f_30s_ret_3", 0.0).abs()
    vol = numeric_col(out, "f_30s_realized_vol_20", 0.0)
    spread = numeric_col(out, "f_30s_spread_bps", 0.0)
    vol_cut = vol.quantile(0.70) if len(vol) else 0.0
    spread_cut = spread.quantile(0.75) if len(spread) else 0.0
    trend_cut = ret.quantile(0.65) if len(ret) else 0.0
    regime = np.where((vol >= vol_cut) | (spread >= spread_cut), "volatile", np.where(ret >= trend_cut, "trend", "choppy"))
    out["MarketRegime"] = regime
    for name in ["choppy", "trend", "volatile"]:
        out[f"f_regime_{name}"] = (out["MarketRegime"] == name).astype(float)
    return out


def ensure_entry_labels(df30: pd.DataFrame) -> pd.DataFrame:
    out = df30.copy()
    if {"Label_Long_Entry", "Label_Short_Entry"}.issubset(out.columns):
        out["Label_Long_Entry"] = pd.to_numeric(out["Label_Long_Entry"], errors="coerce").fillna(0).astype(int)
        out["Label_Short_Entry"] = pd.to_numeric(out["Label_Short_Entry"], errors="coerce").fillna(0).astype(int)
        return out

    out["Label_Long_Entry"] = 0
    out["Label_Short_Entry"] = 0
    for symbol, idx in out.groupby("Symbol", sort=False).groups.items():
        positions = list(idx)
        close = out.loc[positions, "Close"].to_numpy(dtype=float)
        high = out.loc[positions, "High"].to_numpy(dtype=float)
        low = out.loc[positions, "Low"].to_numpy(dtype=float)
        for local_i, row_i in enumerate(positions[:-LIFECYCLE_HORIZON_30S]):
            fill = close[local_i]
            long_tp = fill * (1.0 + ENTRY_PROFIT_PCT)
            long_sl = fill * (1.0 - ENTRY_RISK_PCT)
            short_tp = fill * (1.0 - ENTRY_PROFIT_PCT)
            short_sl = fill * (1.0 + ENTRY_RISK_PCT)
            for j in range(local_i + 1, min(local_i + LIFECYCLE_HORIZON_30S + 1, len(positions))):
                hit_tp = high[j] >= long_tp
                hit_sl = low[j] <= long_sl
                if hit_tp and hit_sl:
                    break
                if hit_tp:
                    out.at[row_i, "Label_Long_Entry"] = 1
                    break
                if hit_sl:
                    break
            for j in range(local_i + 1, min(local_i + LIFECYCLE_HORIZON_30S + 1, len(positions))):
                hit_tp = low[j] <= short_tp
                hit_sl = high[j] >= short_sl
                if hit_tp and hit_sl:
                    break
                if hit_tp:
                    out.at[row_i, "Label_Short_Entry"] = 1
                    break
                if hit_sl:
                    break
    return out


def side_pnl_r(side: str, entry: float, price: float) -> float:
    if entry <= 0.0:
        return 0.0
    pnl_pct = (price - entry) / entry if side == "long" else (entry - price) / entry
    return pnl_pct / ENTRY_RISK_PCT


def path_outcome_r(side: str, entry: float, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    target_r = ENTRY_PROFIT_PCT / ENTRY_RISK_PCT
    stop_r = -1.0
    target = entry * (1.0 + ENTRY_PROFIT_PCT) if side == "long" else entry * (1.0 - ENTRY_PROFIT_PCT)
    stop = entry * (1.0 - ENTRY_RISK_PCT) if side == "long" else entry * (1.0 + ENTRY_RISK_PCT)
    for h, l in zip(highs, lows):
        if side == "long":
            hit_tp = h >= target
            hit_sl = l <= stop
        else:
            hit_tp = l <= target
            hit_sl = h >= stop
        if hit_tp and hit_sl:
            return stop_r
        if hit_tp:
            return target_r
        if hit_sl:
            return stop_r
    return side_pnl_r(side, entry, float(closes[-1])) if len(closes) else 0.0


def select_feature_columns(df: pd.DataFrame, prefixes: Iterable[str]) -> list[str]:
    cols = []
    for col in df.columns:
        if col in NON_FEATURE_COLUMNS or col.startswith("_"):
            continue
        if any(col.startswith(prefix) for prefix in prefixes):
            cols.append(col)
    return sorted(cols)


def sample_entry_indices(entries: list[int], max_events: int, random_state: int) -> list[int]:
    if max_events <= 0 or len(entries) <= max_events:
        return entries
    rng = np.random.default_rng(random_state)
    sampled = rng.choice(np.asarray(entries, dtype=np.int64), size=max_events, replace=False)
    return sorted(int(x) for x in sampled)


def stable_seed(random_state: int, *parts: object) -> int:
    value = int(random_state)
    for part in parts:
        for ch in str(part):
            value = (value * 131 + ord(ch)) % (2**32 - 1)
    return value


def build_lifecycle_rows(df30: pd.DataFrame, max_entry_events: int = 0, max_entry_events_per_side: int = 0, random_state: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    long_rows: list[dict] = []
    short_rows: list[dict] = []
    entry_events = 0
    feature_cols = select_feature_columns(df30, ["f_30s_", "f_regime_"])

    for symbol, group in df30.groupby("Symbol", sort=False):
        group = group.sort_values("_ts").reset_index(drop=True)
        highs = group["High"].to_numpy(dtype=float)
        lows = group["Low"].to_numpy(dtype=float)
        closes = group["Close"].to_numpy(dtype=float)
        for side, label_col, sink in [
            ("long", "Label_Long_Entry", long_rows),
            ("short", "Label_Short_Entry", short_rows),
        ]:
            entries = group.index[group[label_col].astype(int).eq(1)].tolist()
            entries = sample_entry_indices(entries, max_entry_events_per_side, stable_seed(random_state, symbol, side, "lifecycle"))
            for entry_i in entries:
                if entry_i + 2 >= len(group):
                    continue
                entry_events += 1
                if max_entry_events and entry_events > max_entry_events:
                    break
                entry_price = float(closes[entry_i])
                mfe = 0.0
                mae = 0.0
                end_i = min(entry_i + LIFECYCLE_HORIZON_30S, len(group) - 1)
                entry_features = {"f_entry_score_proxy": 1.0, "f_entry_side_long": 1.0 if side == "long" else 0.0, "f_entry_side_short": 1.0 if side == "short" else 0.0}
                for t in range(entry_i + 1, end_i + 1):
                    cur_close = float(closes[t])
                    cur_r = side_pnl_r(side, entry_price, cur_close)
                    if side == "long":
                        fav_r = (float(highs[entry_i + 1:t + 1].max()) - entry_price) / entry_price / ENTRY_RISK_PCT
                        adv_r = (float(lows[entry_i + 1:t + 1].min()) - entry_price) / entry_price / ENTRY_RISK_PCT
                    else:
                        fav_r = (entry_price - float(lows[entry_i + 1:t + 1].min())) / entry_price / ENTRY_RISK_PCT
                        adv_r = (entry_price - float(highs[entry_i + 1:t + 1].max())) / entry_price / ENTRY_RISK_PCT
                    mfe = max(mfe, fav_r)
                    mae = min(mae, adv_r)
                    hold_end = min(t + LIFECYCLE_HORIZON_30S, len(group) - 1)
                    hold_r = path_outcome_r(side, entry_price, highs[t + 1:hold_end + 1], lows[t + 1:hold_end + 1], closes[t + 1:hold_end + 1]) if hold_end > t else cur_r
                    row = {col: float(group.at[t, col]) for col in feature_cols}
                    row.update(entry_features)
                    row.update({
                        "Symbol": symbol,
                        "Timestamp": group.at[t, "Timestamp"],
                        "Date": group.at[t, "Date"],
                        "EntryTime": group.at[entry_i, "Timestamp"],
                        "f_pos_side": 1.0 if side == "long" else -1.0,
                        "f_bars_since_entry": float(t - entry_i) / max(1.0, LIFECYCLE_HORIZON_30S),
                        "f_unrealized_pnl_r": cur_r,
                        "f_mfe_r": mfe,
                        "f_mae_r": mae,
                        "f_target_remaining_r": ENTRY_PROFIT_PCT / ENTRY_RISK_PCT - cur_r,
                        "f_stop_remaining_r": cur_r + 1.0,
                    })
                    label = int(cur_r >= hold_r + EXIT_DECISION_MARGIN_R)
                    row[f"Label_{'Long' if side == 'long' else 'Short'}_ExitLifecycle"] = label
                    sink.append(row)
            if max_entry_events and entry_events > max_entry_events:
                break
        if max_entry_events and entry_events > max_entry_events:
            break
    return pd.DataFrame(long_rows), pd.DataFrame(short_rows)


def build_micro_rows(df30: pd.DataFrame, df5: pd.DataFrame, max_entry_events: int = 0, max_entry_events_per_side: int = 0, random_state: int = 42) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    long_entry_rows: list[dict] = []
    short_entry_rows: list[dict] = []
    long_exit_rows: list[dict] = []
    short_exit_rows: list[dict] = []
    ctx_cols = select_feature_columns(df30, ["f_30s_", "f_regime_"])
    micro_cols = select_feature_columns(df5, ["f_5s_"])
    entry_events = 0

    for symbol, ctx_group in df30.groupby("Symbol", sort=False):
        micro = df5[df5["Symbol"].eq(symbol)].sort_values("_ts").reset_index(drop=True)
        if micro.empty:
            continue
        ctx_group = ctx_group.sort_values("_ts").reset_index(drop=True)
        # On the macOS/pyarrow-backed pandas stack used for this project, astype("int64")
        # on tz-aware datetimes yields microseconds while Timestamp.value is nanoseconds.
        # Normalize both sides to nanoseconds before searchsorted.
        micro_ts_ns = micro["_ts"].dt.tz_convert("UTC").astype("int64").to_numpy() * 1000
        micro_high = micro["High"].to_numpy(dtype=float)
        micro_low = micro["Low"].to_numpy(dtype=float)
        micro_close = micro["Close"].to_numpy(dtype=float)

        for side, label_col, entry_sink, exit_sink in [
            ("long", "Label_Long_Entry", long_entry_rows, long_exit_rows),
            ("short", "Label_Short_Entry", short_entry_rows, short_exit_rows),
        ]:
            if label_col not in ctx_group.columns:
                continue
            entry_indices = ctx_group.index[ctx_group[label_col].astype(int).eq(1)].tolist()
            entry_indices = sample_entry_indices(entry_indices, max_entry_events_per_side, stable_seed(random_state, symbol, side, "micro"))
            for ctx_i in entry_indices:
                ctx = ctx_group.loc[ctx_i]
                ctx_features = {col: float(ctx[col]) for col in ctx_cols}
                entry_events += 1
                if max_entry_events and entry_events > max_entry_events:
                    break
                start = ctx["_ts"]
                start_ns = int(start.tz_convert("UTC").value)
                end_ns = int((start + pd.Timedelta(seconds=MICRO_ARM_TTL_SECONDS)).tz_convert("UTC").value)
                start_pos = int(np.searchsorted(micro_ts_ns, start_ns, side="right"))
                end_pos = int(np.searchsorted(micro_ts_ns, end_ns, side="right"))
                candidate_idx = range(start_pos, end_pos)
                for mi in candidate_idx:
                    fill = float(micro_close[mi])
                    future_end = min(mi + MICRO_FUTURE_WINDOW_5S, len(micro) - 1)
                    outcome = path_outcome_r(side, fill, micro_high[mi + 1:future_end + 1], micro_low[mi + 1:future_end + 1], micro_close[mi + 1:future_end + 1]) if future_end > mi else 0.0
                    row = {col: float(micro.at[mi, col]) for col in micro_cols}
                    row.update(ctx_features)
                    row.update({
                        "Symbol": symbol,
                        "Timestamp": micro.at[mi, "Timestamp"],
                        "Date": micro.at[mi, "Date"],
                        "f_setup_score_proxy": 1.0,
                        "f_seconds_since_arm": (micro.at[mi, "_ts"] - start).total_seconds(),
                    })
                    row[f"Label_{'Long' if side == 'long' else 'Short'}_MicroEntry"] = int(outcome >= 1.0)
                    entry_sink.append(row)

                if candidate_idx:
                    entry_mi = candidate_idx[0]
                    entry_price = float(micro_close[entry_mi])
                    trade_end = min(entry_mi + MICRO_FUTURE_WINDOW_5S, len(micro) - 1)
                    mfe = 0.0
                    mae = 0.0
                    for mi in range(entry_mi + 1, trade_end + 1):
                        cur_r = side_pnl_r(side, entry_price, float(micro_close[mi]))
                        if side == "long":
                            mfe = max(mfe, (float(micro_high[entry_mi + 1:mi + 1].max()) - entry_price) / entry_price / ENTRY_RISK_PCT)
                            mae = min(mae, (float(micro_low[entry_mi + 1:mi + 1].min()) - entry_price) / entry_price / ENTRY_RISK_PCT)
                        else:
                            mfe = max(mfe, (entry_price - float(micro_low[entry_mi + 1:mi + 1].min())) / entry_price / ENTRY_RISK_PCT)
                            mae = min(mae, (entry_price - float(micro_high[entry_mi + 1:mi + 1].max())) / entry_price / ENTRY_RISK_PCT)
                        look_end = min(mi + MICRO_EXIT_LOOKAHEAD_5S, len(micro) - 1)
                        hold_r = path_outcome_r(side, entry_price, micro_high[mi + 1:look_end + 1], micro_low[mi + 1:look_end + 1], micro_close[mi + 1:look_end + 1]) if look_end > mi else cur_r
                        row = {col: float(micro.at[mi, col]) for col in micro_cols}
                        row.update(ctx_features)
                        row.update({
                            "Symbol": symbol,
                            "Timestamp": micro.at[mi, "Timestamp"],
                            "Date": micro.at[mi, "Date"],
                            "EntryTime": micro.at[entry_mi, "Timestamp"],
                            "f_pos_side": 1.0 if side == "long" else -1.0,
                            "f_bars_since_entry_5s": float(mi - entry_mi),
                            "f_unrealized_pnl_r": cur_r,
                            "f_mfe_r": mfe,
                            "f_mae_r": mae,
                        })
                        row[f"Label_{'Long' if side == 'long' else 'Short'}_MicroExitGuard"] = int(cur_r >= hold_r + MICRO_EXIT_MARGIN_R)
                        exit_sink.append(row)
                if max_entry_events and entry_events > max_entry_events:
                    break
            if max_entry_events and entry_events > max_entry_events:
                break
        if max_entry_events and entry_events > max_entry_events:
            break
    return (pd.DataFrame(long_entry_rows), pd.DataFrame(short_entry_rows), pd.DataFrame(long_exit_rows), pd.DataFrame(short_exit_rows))


def train_test_split_time(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    n = len(df)
    if n < 3:
        return np.arange(n), np.arange(n)
    split = max(1, int(n * 0.8))
    split = min(split, n - 1)
    return np.arange(split), np.arange(split, n)


def optimize_threshold(y_true: np.ndarray, probas: np.ndarray, model_kind: str) -> tuple[float, float, float, float]:
    best = (float(THRESHOLD_RANGES[model_kind][0]), -1.0, 0.0, 0.0, -1e9)
    for thr in THRESHOLD_RANGES[model_kind]:
        preds = (probas >= thr).astype(np.int8)
        pos_rate = float(preds.mean()) if len(preds) else 0.0
        if pos_rate <= 0.0:
            continue
        prec = precision_score(y_true, preds, zero_division=0)
        rec = recall_score(y_true, preds, zero_division=0)
        score = 0.60 * prec + 0.25 * rec + 0.15 * min(pos_rate, 0.20)
        if score > best[4]:
            best = (float(thr), float(prec), float(rec), pos_rate, float(score))
    return best[0], max(best[1], 0.0), best[2], best[3]


def export_onnx(model: RandomForestClassifier, feature_count: int, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    initial_type = [("float_input", FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type, options={id(model): {"zipmap": False}}, target_opset=12)
    if getattr(onnx_model, "ir_version", 0) > 9:
        onnx_model.ir_version = 9
    path.write_bytes(onnx_model.SerializeToString())


def train_binary_model(df: pd.DataFrame, label_col: str, name: str, filename: str, model_kind: str, output_dir: Path, min_rows: int, random_state: int, no_onnx: bool) -> TrainedModelResult | None:
    if df.empty or label_col not in df.columns:
        print(f"SKIP {name}: empty dataset or missing {label_col}")
        return None
    data = df.copy().sort_values(["Date", "Timestamp"]).reset_index(drop=True)
    y = pd.to_numeric(data[label_col], errors="coerce").fillna(0).astype(int).to_numpy()
    feature_cols = [c for c in data.columns if c not in NON_FEATURE_COLUMNS and not c.startswith("_") and pd.api.types.is_numeric_dtype(data[c])]
    if len(data) < min_rows or int(y.sum()) < 5 or len(np.unique(y)) < 2:
        print(f"SKIP {name}: rows={len(data)} positives={int(y.sum())} features={len(feature_cols)}")
        return None
    X = data[feature_cols].replace([np.inf, -np.inf], 0.0).fillna(0.0).to_numpy(dtype=np.float32)
    train_idx, test_idx = train_test_split_time(data)
    model = RandomForestClassifier(n_estimators=260, max_depth=9, min_samples_leaf=20, min_samples_split=40, max_features="sqrt", random_state=random_state, n_jobs=-1)
    model.fit(X[train_idx], y[train_idx])
    test_proba = model.predict_proba(X[test_idx])[:, 1]
    threshold, precision, recall, pred_pos_rate = optimize_threshold(y[test_idx], test_proba, model_kind)
    export_path = output_dir / filename
    if not no_onnx:
        export_onnx(model, len(feature_cols), export_path)
    print(f"TRAINED {name}: rows={len(data)} positives={int(y.sum())} threshold={threshold:.2f} precision={precision:.2%} recall={recall:.2%} export={export_path if not no_onnx else 'disabled'}")
    return TrainedModelResult(
        name=name,
        filename=filename,
        threshold=threshold,
        precision=precision,
        recall=recall,
        pred_pos_rate=pred_pos_rate,
        label_pos_rate=float(y.mean()),
        rows=len(data),
        positives=int(y.sum()),
        feature_count=len(feature_cols),
        feature_columns=feature_cols,
        exported_to=str(export_path) if not no_onnx else "",
    )


def symbol_from_training_path(path: Path, cadence: str) -> str:
    suffix = f"_{cadence}_training.csv"
    name = path.name
    return name[:-len(suffix)].upper() if name.endswith(suffix) else path.stem.upper()


def discover_symbol_pairs(input_30s: Path, input_5s: Path) -> list[tuple[str, Path, Path]]:
    if not input_30s.is_dir() or not input_5s.is_dir():
        return []
    five_by_symbol = {symbol_from_training_path(path, "5s"): path for path in input_5s.glob("*_5s_training.csv")}
    pairs: list[tuple[str, Path, Path]] = []
    missing: list[str] = []
    for path30 in sorted(input_30s.glob("*_30s_training.csv")):
        symbol = symbol_from_training_path(path30, "30s")
        path5 = five_by_symbol.get(symbol)
        if path5 is None:
            missing.append(symbol)
            continue
        pairs.append((symbol, path30, path5))
    if missing:
        print(f"WARN missing 5s files for {len(missing)} symbol(s): {','.join(missing[:20])}")
    if not pairs:
        raise ValueError(f"No 30s/5s symbol pairs found under {input_30s} and {input_5s}")
    return pairs


def append_frame_csv(frame: pd.DataFrame, path: Path) -> None:
    if frame.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)


def maybe_sample_frame(frame: pd.DataFrame, max_rows: int, random_state: int) -> pd.DataFrame:
    if max_rows <= 0 or len(frame) <= max_rows:
        return frame
    return frame.sample(n=max_rows, random_state=random_state).sort_values([c for c in ["Date", "Timestamp"] if c in frame.columns]).reset_index(drop=True)


def count_csv_rows(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open("rb") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def load_staged_training_frame(path: Path, max_rows: int, random_state: int) -> pd.DataFrame:
    total_rows = count_csv_rows(path)
    if total_rows == 0:
        return pd.DataFrame()
    if max_rows <= 0 or total_rows <= max_rows:
        print(f"Loading staged dataset {path} rows={total_rows}")
        return pd.read_csv(path)

    rng = np.random.default_rng(random_state)
    kept: pd.DataFrame | None = None
    chunk_size = 100_000
    for chunk in pd.read_csv(path, chunksize=chunk_size):
        chunk = chunk.copy()
        chunk["_sample_key"] = rng.random(len(chunk))
        if kept is None:
            kept = chunk
        else:
            kept = pd.concat([kept, chunk], ignore_index=True)
        if len(kept) > max_rows:
            kept = kept.nsmallest(max_rows, "_sample_key").reset_index(drop=True)
    if kept is None:
        return pd.DataFrame()
    kept = kept.drop(columns=["_sample_key"]).reset_index(drop=True)
    print(f"Loaded sampled staged dataset {path} rows={len(kept)} total_rows={total_rows} cap={max_rows}")
    return kept


def build_streamed_staging_datasets(
    pairs: list[tuple[str, Path, Path]],
    staging_dir: Path,
    max_entry_events: int,
    max_staged_rows_per_symbol_per_model: int,
    max_entry_events_per_symbol_side: int,
    random_state: int,
) -> dict[str, Path]:
    dataset_paths = {
        "long_lifecycle": staging_dir / "long_lifecycle_rows.csv",
        "short_lifecycle": staging_dir / "short_lifecycle_rows.csv",
        "long_micro_entry": staging_dir / "long_micro_entry_rows.csv",
        "short_micro_entry": staging_dir / "short_micro_entry_rows.csv",
        "long_micro_exit": staging_dir / "long_micro_exit_rows.csv",
        "short_micro_exit": staging_dir / "short_micro_exit_rows.csv",
    }
    staging_dir.mkdir(parents=True, exist_ok=True)
    for path in dataset_paths.values():
        if path.exists():
            path.unlink()

    total_entry_events = 0
    for index, (symbol, path30, path5) in enumerate(pairs, start=1):
        remaining = 0 if max_entry_events <= 0 else max(0, max_entry_events - total_entry_events)
        if max_entry_events > 0 and remaining <= 0:
            break
        print(f"STREAM symbol={symbol} pair={index}/{len(pairs)} 30s={path30.name} 5s={path5.name}", flush=True)
        df30 = assign_simple_regime(ensure_entry_labels(load_bar_csv(str(path30), "30s")))
        symbol_entries = int(df30["Label_Long_Entry"].sum()) + int(df30["Label_Short_Entry"].sum())
        per_symbol_cap = 0 if max_entry_events <= 0 else remaining

        long_lifecycle, short_lifecycle = build_lifecycle_rows(
            df30,
            max_entry_events=per_symbol_cap,
            max_entry_events_per_side=max_entry_events_per_symbol_side,
            random_state=random_state + index,
        )
        append_frame_csv(maybe_sample_frame(long_lifecycle, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 1), dataset_paths["long_lifecycle"])
        append_frame_csv(maybe_sample_frame(short_lifecycle, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 2), dataset_paths["short_lifecycle"])

        df5 = load_bar_csv(str(path5), "5s")
        long_micro_entry, short_micro_entry, long_micro_exit, short_micro_exit = build_micro_rows(
            df30,
            df5,
            max_entry_events=per_symbol_cap,
            max_entry_events_per_side=max_entry_events_per_symbol_side,
            random_state=random_state + index,
        )
        append_frame_csv(maybe_sample_frame(long_micro_entry, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 3), dataset_paths["long_micro_entry"])
        append_frame_csv(maybe_sample_frame(short_micro_entry, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 4), dataset_paths["short_micro_entry"])
        append_frame_csv(maybe_sample_frame(long_micro_exit, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 5), dataset_paths["long_micro_exit"])
        append_frame_csv(maybe_sample_frame(short_micro_exit, max_staged_rows_per_symbol_per_model, random_state + index * 10 + 6), dataset_paths["short_micro_exit"])

        total_entry_events += symbol_entries
        print(
            "STREAM_DONE "
            f"symbol={symbol} entry_events={symbol_entries} "
            f"lifecycle_rows=({len(long_lifecycle)},{len(short_lifecycle)}) "
            f"micro_rows=({len(long_micro_entry)},{len(short_micro_entry)},{len(long_micro_exit)},{len(short_micro_exit)})",
            flush=True,
        )
        del df30, df5, long_lifecycle, short_lifecycle, long_micro_entry, short_micro_entry, long_micro_exit, short_micro_exit
        gc.collect()

    for name, path in dataset_paths.items():
        print(f"STAGED {name} rows={count_csv_rows(path)} path={path}")
    return dataset_paths


def write_scorecards(output_dir: Path, results: list[TrainedModelResult]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    route = []
    for r in results:
        rows.append({
            "model": r.name,
            "filename": r.filename,
            "rows": r.rows,
            "positives": r.positives,
            "label_pos_rate": r.label_pos_rate,
            "threshold": r.threshold,
            "precision": r.precision,
            "recall": r.recall,
            "pred_pos_rate": r.pred_pos_rate,
            "feature_count": r.feature_count,
            "exported_to": r.exported_to,
        })
        route.append({
            "model": r.name,
            "model_path": r.exported_to,
            "threshold": r.threshold,
            "feature_count": r.feature_count,
            "feature_columns": r.feature_columns,
        })
    pd.DataFrame(rows).to_csv(output_dir / "lifecycle_micro_scorecard.csv", index=False)
    (output_dir / "lifecycle_micro_route_manifest.json").write_text(json.dumps(route, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else DEFAULT_MODEL_EXPORTS_ROOT / f"lifecycle_micro_{run_tag}"
    output_dir.mkdir(parents=True, exist_ok=True)

    input_30s = Path(args.input_30s_csv).expanduser()
    input_5s = Path(args.input_5s_csv).expanduser()
    pairs = discover_symbol_pairs(input_30s, input_5s)
    if pairs:
        staging_dir = Path(args.staging_dir).expanduser() if args.staging_dir else output_dir / "staged_lifecycle_micro_rows"
        print(f"Streaming per-symbol lifecycle/micro staging pairs={len(pairs)} staging_dir={staging_dir}")
        staged = build_streamed_staging_datasets(
            pairs,
            staging_dir,
            args.max_entry_events,
            args.max_staged_rows_per_symbol_per_model,
            args.max_entry_events_per_symbol_side,
            args.random_state,
        )
        datasets = [
            (load_staged_training_frame(staged["long_lifecycle"], args.max_train_rows_per_model, args.random_state + 101), "Label_Long_ExitLifecycle", "longExitLifecycleAi", "long_exit_lifecycle.onnx", "lifecycle"),
            (load_staged_training_frame(staged["short_lifecycle"], args.max_train_rows_per_model, args.random_state + 102), "Label_Short_ExitLifecycle", "shortExitLifecycleAi", "short_exit_lifecycle.onnx", "lifecycle"),
            (load_staged_training_frame(staged["long_micro_entry"], args.max_train_rows_per_model, args.random_state + 103), "Label_Long_MicroEntry", "longMicroEntryAi", "long_micro_entry_5s.onnx", "micro_entry"),
            (load_staged_training_frame(staged["short_micro_entry"], args.max_train_rows_per_model, args.random_state + 104), "Label_Short_MicroEntry", "shortMicroEntryAi", "short_micro_entry_5s.onnx", "micro_entry"),
            (load_staged_training_frame(staged["long_micro_exit"], args.max_train_rows_per_model, args.random_state + 105), "Label_Long_MicroExitGuard", "longMicroExitGuardAi", "long_micro_exit_guard_5s.onnx", "micro_exit_guard"),
            (load_staged_training_frame(staged["short_micro_exit"], args.max_train_rows_per_model, args.random_state + 106), "Label_Short_MicroExitGuard", "shortMicroExitGuardAi", "short_micro_exit_guard_5s.onnx", "micro_exit_guard"),
        ]
    else:
        print(f"Loading 30s data: {args.input_30s_csv}")
        df30 = assign_simple_regime(ensure_entry_labels(load_bar_csv(args.input_30s_csv, "30s")))
        print(f"Loading 5s data: {args.input_5s_csv}")
        df5 = load_bar_csv(args.input_5s_csv, "5s")

        long_lifecycle, short_lifecycle = build_lifecycle_rows(
            df30,
            max_entry_events=args.max_entry_events,
            max_entry_events_per_side=args.max_entry_events_per_symbol_side,
            random_state=args.random_state,
        )
        long_micro_entry, short_micro_entry, long_micro_exit, short_micro_exit = build_micro_rows(
            df30,
            df5,
            max_entry_events=args.max_entry_events,
            max_entry_events_per_side=args.max_entry_events_per_symbol_side,
            random_state=args.random_state,
        )

        datasets = [
            (long_lifecycle, "Label_Long_ExitLifecycle", "longExitLifecycleAi", "long_exit_lifecycle.onnx", "lifecycle"),
            (short_lifecycle, "Label_Short_ExitLifecycle", "shortExitLifecycleAi", "short_exit_lifecycle.onnx", "lifecycle"),
            (long_micro_entry, "Label_Long_MicroEntry", "longMicroEntryAi", "long_micro_entry_5s.onnx", "micro_entry"),
            (short_micro_entry, "Label_Short_MicroEntry", "shortMicroEntryAi", "short_micro_entry_5s.onnx", "micro_entry"),
            (long_micro_exit, "Label_Long_MicroExitGuard", "longMicroExitGuardAi", "long_micro_exit_guard_5s.onnx", "micro_exit_guard"),
            (short_micro_exit, "Label_Short_MicroExitGuard", "shortMicroExitGuardAi", "short_micro_exit_guard_5s.onnx", "micro_exit_guard"),
        ]

    results: list[TrainedModelResult] = []
    for i, (dataset, label_col, name, filename, kind) in enumerate(datasets):
        result = train_binary_model(dataset, label_col, name, filename, kind, output_dir, args.min_rows, args.random_state + i, args.no_onnx)
        if result is not None:
            results.append(result)

    write_scorecards(output_dir, results)
    print(f"WROTE {output_dir / 'lifecycle_micro_scorecard.csv'}")
    print(f"WROTE {output_dir / 'lifecycle_micro_route_manifest.json'}")


if __name__ == "__main__":
    main()



