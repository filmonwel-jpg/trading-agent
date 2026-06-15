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
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
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
# Must match strategy.micro.armTtlSeconds Java default (currently 30).
# Override via MICRO_ARM_TTL_SECONDS only when intentionally testing a non-standard TTL,
# and only with a matching Java -Dstrategy.micro.armTtlSeconds= flag.
MICRO_ARM_TTL_SECONDS = int(os.getenv("MICRO_ARM_TTL_SECONDS", "30"))
MICRO_FUTURE_WINDOW_5S = int(os.getenv("MICRO_FUTURE_WINDOW_5S", "24"))
MICRO_EXIT_LOOKAHEAD_5S = int(os.getenv("MICRO_EXIT_LOOKAHEAD_5S", "6"))
EXIT_DECISION_MARGIN_R = float(os.getenv("EXIT_DECISION_MARGIN_R", "0.10"))
MICRO_EXIT_MARGIN_R = float(os.getenv("MICRO_EXIT_MARGIN_R", "0.05"))
ENTRY_FILL_MODE = os.getenv("ENTRY_FILL_MODE", "next_open").strip().lower()
ENTRY_SLIPPAGE_BPS = float(os.getenv("ENTRY_SLIPPAGE_BPS", "2.0"))
EXIT_SLIPPAGE_BPS = float(os.getenv("EXIT_SLIPPAGE_BPS", "2.0"))
ENTRY_SCORE_PROXY_BOOTSTRAP = 1.0  # Remove once walk-forward 30s setup scoring is active.
ALLOW_BOOTSTRAP_SETUP_PROXY = os.getenv("ALLOW_BOOTSTRAP_SETUP_PROXY", "0").strip().lower() in {"1", "true", "yes", "on"}
MIN_SETUP_SCORE_UNIQUE_VALUES = int(os.getenv("MIN_SETUP_SCORE_UNIQUE_VALUES", "3"))
POSTHOC_CALIBRATION_MODE = os.getenv("LIFECYCLE_POSTHOC_CALIBRATION", "none").strip().lower()
POSTHOC_CALIBRATION_METHODS = {"none", "sigmoid", "isotonic", "both"}

_ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED = False

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
    brier_score: float
    ece: float
    calibration_rows: int
    calibration_bins: list[dict[str, object]]
    pred_pos_rate: float
    label_pos_rate: float
    rows: int
    positives: int
    feature_count: int
    feature_columns: list[str]
    exported_to: str
    posthoc_calibration: dict[str, object] | None = None


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
    parser.add_argument(
        "--setup-predictions-csv",
        default="",
        help="OOF 30s setup predictions from generate_walk_forward_setup_predictions.py. Required unless --allow-bootstrap-setup-proxy is used.",
    )
    parser.add_argument(
        "--allow-bootstrap-setup-proxy",
        action="store_true",
        help="Research-only override allowing constant bootstrap setup proxies when no OOF setup predictions are supplied.",
    )
    parser.add_argument(
        "--min-setup-score-unique-values",
        type=int,
        default=MIN_SETUP_SCORE_UNIQUE_VALUES,
        help="Minimum unique finite setup-score values required per side after joining OOF setup predictions.",
    )
    parser.add_argument("--min-rows", type=int, default=200, help="Minimum rows required to train a model.")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--no-onnx", action="store_true", help="Train and score but do not export ONNX.")
    parser.add_argument(
        "--posthoc-calibration",
        choices=sorted(POSTHOC_CALIBRATION_METHODS),
        default=POSTHOC_CALIBRATION_MODE if POSTHOC_CALIBRATION_MODE in POSTHOC_CALIBRATION_METHODS else "none",
        help="Fit post-hoc probability calibrators on a chronological calibration split and evaluate on a frozen holdout.",
    )
    parser.add_argument(
        "--posthoc-calibration-frac",
        type=float,
        default=float(os.getenv("LIFECYCLE_POSTHOC_CALIBRATION_FRAC", "0.20")),
        help="Fraction of rows reserved before the frozen holdout for post-hoc calibrator fitting.",
    )
    parser.add_argument(
        "--frozen-holdout-frac",
        type=float,
        default=float(os.getenv("LIFECYCLE_FROZEN_HOLDOUT_FRAC", "0.20")),
        help="Final chronological fraction reserved for frozen post-hoc evaluation.",
    )
    parser.add_argument(
        "--min-frozen-holdout-rows",
        type=int,
        default=int(os.getenv("LIFECYCLE_MIN_FROZEN_HOLDOUT_ROWS", "200")),
        help="Minimum frozen-holdout rows expected for promotion-gate discussion.",
    )
    parser.add_argument(
        "--min-holdout-predictions",
        type=int,
        default=int(os.getenv("LIFECYCLE_MIN_HOLDOUT_PREDICTIONS", "20")),
        help="Minimum predicted-positive count on frozen holdout for threshold-gate discussion.",
    )
    parser.add_argument(
        "--max-day-dominance-frac",
        type=float,
        default=float(os.getenv("LIFECYCLE_MAX_DAY_DOMINANCE_FRAC", "0.40")),
        help="Maximum fraction of frozen-holdout predictions allowed on one day before day-dominance warning.",
    )
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
    df = df.sort_values(["Symbol", "_ts"]).reset_index(drop=True)
    df["Close"] = df.groupby("Symbol", sort=False)["Close"].ffill()
    for col in ["Open", "High", "Low", "WAP"]:
        df[col] = df[col].fillna(df["Close"])
    df["Volume"] = df["Volume"].fillna(0.0)
    df["Date"] = df["_ts"].dt.strftime("%Y-%m-%d")
    return add_common_features(df, cadence)


def numeric_col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[name], errors="coerce").fillna(default)


def warn_bootstrap_score_proxy_once() -> None:
    global _ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED
    if _ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED:
        return
    print(
        "WARN f_setup_score_proxy and f_entry_score_proxy are using bootstrap value 1.0. "
        "Models trained with this placeholder must not be promoted to live trading.",
        file=sys.stderr,
    )
    _ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED = True


def setup_score_proxy(setup_prob: float | None) -> float:
    if setup_prob is None:
        if not ALLOW_BOOTSTRAP_SETUP_PROXY:
            raise ValueError(
                "Missing real out-of-fold setup probability. Run generate_walk_forward_setup_predictions.py "
                "and pass --setup-predictions-csv, or use --allow-bootstrap-setup-proxy for research-only smoke tests."
            )
        warn_bootstrap_score_proxy_once()
        return ENTRY_SCORE_PROXY_BOOTSTRAP
    value = float(setup_prob)
    if not np.isfinite(value):
        raise ValueError("Setup probability is not finite; refusing to stage lifecycle/micro rows.")
    return value


def extract_setup_probability(row: pd.Series, side: str) -> float | None:
    side_title = "Long" if side == "long" else "Short"
    candidate_columns = [
        "f_setup_prob",
        "f_entry_prob",
        f"f_{side}_setup_prob",
        f"f_{side}_entry_prob",
        f"{side_title}EntryProbability",
        f"Probability_{side_title}_Entry",
        f"Prob_{side_title}_Entry",
    ]
    for column in candidate_columns:
        if column not in row.index:
            continue
        value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
        if pd.notna(value) and np.isfinite(float(value)):
            return float(value)
    return None


def load_setup_predictions(path: str) -> pd.DataFrame:
    pred_path = Path(path).expanduser()
    if not pred_path.exists():
        raise FileNotFoundError(f"Setup predictions CSV not found: {pred_path}")
    pred = pd.read_csv(pred_path)
    required = {
        "Symbol",
        "Timestamp",
        "f_long_setup_prob",
        "f_short_setup_prob",
        "long_setup_fold_id",
        "short_setup_fold_id",
    }
    missing = sorted(required - set(pred.columns))
    if missing:
        raise ValueError(f"Setup predictions CSV missing required columns: {missing}")
    pred["Symbol"] = pred["Symbol"].astype(str).str.strip().str.upper().replace("", "SINGLE")
    pred["_setup_ts"] = parse_timestamp(pred["Timestamp"])
    for col in [
        "f_long_setup_prob",
        "f_short_setup_prob",
        "f_long_setup_threshold",
        "f_short_setup_threshold",
        "f_long_setup_threshold_margin",
        "f_short_setup_threshold_margin",
    ]:
        if col in pred.columns:
            pred[col] = pd.to_numeric(pred[col], errors="coerce")
    if "is_oof_setup_prediction" not in pred.columns:
        pred["is_oof_setup_prediction"] = (
            pred["f_long_setup_prob"].notna() & pred["f_short_setup_prob"].notna()
        ).astype(int)
    pred["is_oof_setup_prediction"] = pd.to_numeric(pred["is_oof_setup_prediction"], errors="coerce").fillna(0).astype(int)
    keep_cols = [
        "Symbol",
        "_setup_ts",
        "f_long_setup_prob",
        "f_short_setup_prob",
        "f_long_setup_threshold",
        "f_short_setup_threshold",
        "f_long_setup_threshold_margin",
        "f_short_setup_threshold_margin",
        "long_setup_fold_id",
        "short_setup_fold_id",
        "is_oof_setup_prediction",
    ]
    keep_cols = [col for col in keep_cols if col in pred.columns]
    pred = pred[keep_cols].drop_duplicates(["Symbol", "_setup_ts"], keep="last")
    return pred


def setup_prediction_side_summary(df30: pd.DataFrame, side: str) -> tuple[dict[str, object], pd.Series]:
    col = f"f_{side}_setup_prob"
    if col not in df30.columns:
        raise ValueError(f"Missing joined setup prediction column: {col}")
    values = pd.to_numeric(df30[col], errors="coerce")
    finite = values[np.isfinite(values)]
    return {
        "finite_count": int(len(finite)),
        "missing_count": int(len(df30) - len(finite)),
        "unique_values": int(finite.round(8).nunique()) if len(finite) else 0,
        "min": float(finite.min()) if len(finite) else None,
        "max": float(finite.max()) if len(finite) else None,
        "mean": float(finite.mean()) if len(finite) else None,
    }, finite


def validate_setup_prediction_columns(df30: pd.DataFrame, min_unique_values: int) -> dict[str, object]:
    summary: dict[str, object] = {"rows": int(len(df30)), "min_unique_values": int(min_unique_values), "sides": {}}
    errors: list[str] = []
    for side in ["long", "short"]:
        side_summary, finite = setup_prediction_side_summary(df30, side)
        summary["sides"][side] = side_summary
        if side_summary["missing_count"] > 0:
            errors.append(f"{side} setup predictions missing for {side_summary['missing_count']} retained rows")
        if side_summary["unique_values"] < min_unique_values:
            errors.append(f"{side} setup predictions have {side_summary['unique_values']} unique values, need >= {min_unique_values}")
        if side_summary["unique_values"] == 1 and len(finite) and float(finite.iloc[0]) == ENTRY_SCORE_PROXY_BOOTSTRAP:
            errors.append(f"{side} setup predictions look like bootstrap constant {ENTRY_SCORE_PROXY_BOOTSTRAP}")
    summary["errors"] = errors
    if errors:
        raise ValueError("Invalid OOF setup predictions: " + "; ".join(errors))
    return summary


def apply_setup_predictions(df30: pd.DataFrame, setup_predictions: pd.DataFrame | None, min_unique_values: int) -> pd.DataFrame:
    if setup_predictions is None:
        if ALLOW_BOOTSTRAP_SETUP_PROXY:
            warn_bootstrap_score_proxy_once()
            return df30
        raise ValueError(
            "--setup-predictions-csv is required by default. Generate OOF setup scores with "
            "generate_walk_forward_setup_predictions.py or pass --allow-bootstrap-setup-proxy for research-only smoke tests."
        )
    merged = df30.merge(
        setup_predictions,
        how="left",
        left_on=["Symbol", "_ts"],
        right_on=["Symbol", "_setup_ts"],
        validate="many_to_one",
    )
    matched = merged["is_oof_setup_prediction"].fillna(0).astype(int).eq(1)
    retained = merged[matched].copy()
    dropped = len(merged) - len(retained)
    if retained.empty:
        raise ValueError("No 30s rows retained after joining OOF setup predictions.")
    if dropped:
        print(f"OOF_SETUP_JOIN dropped_unscored_30s_rows={dropped} retained_rows={len(retained)}")
    retained = retained.drop(columns=[col for col in ["_setup_ts"] if col in retained.columns])
    summary = validate_setup_prediction_columns(retained, min_unique_values=min_unique_values)
    print("OOF_SETUP_JOIN summary=" + json.dumps(summary, sort_keys=True))
    return retained.reset_index(drop=True)


def first_available_price_array(df: pd.DataFrame, names: Iterable[str]) -> np.ndarray | None:
    for name in names:
        if name not in df.columns:
            continue
        values = pd.to_numeric(df[name], errors="coerce").to_numpy(dtype=float)
        if not np.isnan(values).all():
            return values
    return None


def bid_ask_arrays(df: pd.DataFrame) -> tuple[np.ndarray | None, np.ndarray | None]:
    bids = first_available_price_array(df, ["BidLast", "Bid", "bid", "BestBid", "best_bid"])
    asks = first_available_price_array(df, ["AskLast", "Ask", "ask", "BestAsk", "best_ask"])
    return bids, asks


def valid_price(value: float) -> bool:
    return np.isfinite(value) and value > 0.0


def side_aware_entry_fill(
    side: str,
    closes: np.ndarray,
    bids: np.ndarray | None,
    asks: np.ndarray | None,
    index: int,
    slippage_bps: float = ENTRY_SLIPPAGE_BPS,
) -> float:
    close = float(closes[index])
    slip = slippage_bps / 10_000.0
    if side == "long":
        raw = float(asks[index]) if asks is not None and valid_price(float(asks[index])) else close
        return raw * (1.0 + slip)
    raw = float(bids[index]) if bids is not None and valid_price(float(bids[index])) else close
    return raw * (1.0 - slip)


def timestamp_ns_array(series: pd.Series) -> np.ndarray:
    utc = series.dt.tz_convert("UTC")
    values = utc.astype("int64").to_numpy(dtype=np.int64)
    if len(values) == 0:
        return values
    expected_first = int(utc.iloc[0].value)
    actual_first = int(values[0])
    if actual_first == expected_first:
        return values
    if actual_first * 1000 == expected_first:
        return values * 1000
    return np.asarray([int(ts.value) for ts in utc], dtype=np.int64)


def feature_schema_hash(feature_columns: list[str]) -> str:
    return hashlib.sha256("\n".join(feature_columns).encode("utf-8")).hexdigest()


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
        bids, asks = bid_ask_arrays(group)
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
                entry_price = side_aware_entry_fill(side, closes, bids, asks, entry_i)
                setup_prob = extract_setup_probability(group.loc[entry_i], side)
                mfe = 0.0
                mae = 0.0
                end_i = min(entry_i + LIFECYCLE_HORIZON_30S, len(group) - 1)
                entry_features = {
                    # Legacy Java schema name; with --setup-predictions-csv this is the real OOF setup probability.
                    "f_entry_score_proxy": setup_score_proxy(setup_prob),
                    "f_entry_side_long": 1.0 if side == "long" else 0.0,
                    "f_entry_side_short": 1.0 if side == "short" else 0.0,
                }
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
        micro_ts_ns = timestamp_ns_array(micro["_ts"])
        micro_high = micro["High"].to_numpy(dtype=float)
        micro_low = micro["Low"].to_numpy(dtype=float)
        micro_close = micro["Close"].to_numpy(dtype=float)
        micro_bids, micro_asks = bid_ask_arrays(micro)

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
                setup_prob = extract_setup_probability(ctx, side)
                entry_events += 1
                if max_entry_events and entry_events > max_entry_events:
                    break
                # Arm starts at the 30-second bar close (bar_open + 30s), matching Java's
                # current30sAiDecisionEpoch = finalizedBucketStart + 30L.
                arm_start = ctx["_ts"] + pd.Timedelta(seconds=30)
                start_ns = int(arm_start.tz_convert("UTC").value)
                end_ns = int((arm_start + pd.Timedelta(seconds=MICRO_ARM_TTL_SECONDS)).tz_convert("UTC").value)
                start_pos = int(np.searchsorted(micro_ts_ns, start_ns, side="left"))
                end_pos = int(np.searchsorted(micro_ts_ns, end_ns, side="right"))
                candidate_idx = range(start_pos, end_pos)
                confirmed_entry_mi: int | None = None
                confirmed_entry_price = 0.0
                for mi in candidate_idx:
                    fill = side_aware_entry_fill(side, micro_close, micro_bids, micro_asks, mi)
                    future_end = min(mi + MICRO_FUTURE_WINDOW_5S, len(micro) - 1)
                    outcome = path_outcome_r(side, fill, micro_high[mi + 1:future_end + 1], micro_low[mi + 1:future_end + 1], micro_close[mi + 1:future_end + 1]) if future_end > mi else 0.0
                    row = {col: float(micro.at[mi, col]) for col in micro_cols}
                    row.update(ctx_features)
                    row.update({
                        "Symbol": symbol,
                        "Timestamp": micro.at[mi, "Timestamp"],
                        "Date": micro.at[mi, "Date"],
                        # Legacy Java schema name; with --setup-predictions-csv this is the real OOF setup probability.
                        "f_setup_score_proxy": setup_score_proxy(setup_prob),
                        "f_seconds_since_arm": (micro.at[mi, "_ts"] - arm_start).total_seconds(),
                    })
                    label = int(outcome >= 1.0)
                    row[f"Label_{'Long' if side == 'long' else 'Short'}_MicroEntry"] = label
                    entry_sink.append(row)
                    if label == 1 and confirmed_entry_mi is None:
                        confirmed_entry_mi = mi
                        confirmed_entry_price = fill

                if confirmed_entry_mi is not None:
                    entry_mi = confirmed_entry_mi
                    entry_price = confirmed_entry_price
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


def calibration_report(y_true: np.ndarray, probas: np.ndarray, *, bins: int = 10) -> dict[str, object]:
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(probas, dtype=float)
    mask = np.isfinite(y) & np.isfinite(p)
    y = y[mask]
    p = np.clip(p[mask], 0.0, 1.0)
    if len(y) == 0:
        return {"rows": 0, "brier_score": math.nan, "ece": math.nan, "bins": []}

    brier = float(np.mean((p - y) ** 2))
    edges = np.linspace(0.0, 1.0, int(bins) + 1)
    reliability_bins: list[dict[str, object]] = []
    ece = 0.0
    for index in range(int(bins)):
        low = float(edges[index])
        high = float(edges[index + 1])
        if index == int(bins) - 1:
            bin_mask = (p >= low) & (p <= high)
        else:
            bin_mask = (p >= low) & (p < high)
        count = int(bin_mask.sum())
        if count:
            mean_pred = float(p[bin_mask].mean())
            observed = float(y[bin_mask].mean())
            abs_error = abs(mean_pred - observed)
            ece += (count / len(y)) * abs_error
        else:
            mean_pred = math.nan
            observed = math.nan
            abs_error = math.nan
        reliability_bins.append({
            "bin_index": index,
            "prob_min": low,
            "prob_max": high,
            "rows": count,
            "mean_predicted_probability": mean_pred,
            "observed_positive_rate": observed,
            "abs_calibration_error": abs_error,
        })
    return {"rows": int(len(y)), "brier_score": brier, "ece": float(ece), "bins": reliability_bins}


def chronological_three_way_split(n: int, calibration_frac: float, holdout_frac: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return base-train, calibrator-fit, and frozen-holdout indices.

    The final segment is never used to fit either the model or the calibrator.
    This is the Phase-5 frozen-holdout discipline used for post-hoc calibration.
    """
    if n < 3:
        idx = np.arange(n)
        return idx, idx[:0], idx[:0]
    calibration_frac = float(np.clip(calibration_frac, 0.05, 0.40))
    holdout_frac = float(np.clip(holdout_frac, 0.05, 0.40))
    holdout_rows = max(1, int(round(n * holdout_frac)))
    calibration_rows = max(1, int(round(n * calibration_frac)))
    if holdout_rows + calibration_rows >= n:
        holdout_rows = max(1, n // 5)
        calibration_rows = max(1, n // 5)
    base_end = max(1, n - holdout_rows - calibration_rows)
    calibration_end = max(base_end + 1, n - holdout_rows)
    calibration_end = min(calibration_end, n - 1)
    return np.arange(0, base_end), np.arange(base_end, calibration_end), np.arange(calibration_end, n)


def holdout_fingerprint(data: pd.DataFrame, holdout_idx: np.ndarray, label_col: str) -> str:
    cols = [c for c in ["Symbol", "Date", "Timestamp", label_col] if c in data.columns]
    if len(holdout_idx) == 0:
        return hashlib.sha256(b"").hexdigest()
    if not cols:
        payload = "\n".join(str(int(i)) for i in holdout_idx)
    else:
        payload = data.iloc[holdout_idx][cols].astype(str).to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def predict_sigmoid_probability(raw_probability: np.ndarray, coef: float, intercept: float) -> np.ndarray:
    z = np.clip(coef * np.asarray(raw_probability, dtype=float) + intercept, -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-z))


def fit_sigmoid_calibrator(raw_probability: np.ndarray, y_true: np.ndarray, random_state: int) -> dict[str, object]:
    if len(np.unique(y_true)) < 2:
        raise ValueError("sigmoid calibrator requires both classes in calibration split")
    lr = LogisticRegression(solver="lbfgs", random_state=random_state)
    lr.fit(np.asarray(raw_probability, dtype=float).reshape(-1, 1), np.asarray(y_true, dtype=int))
    coef = float(lr.coef_[0][0])
    intercept = float(lr.intercept_[0])
    return {
        "method": "sigmoid",
        "type": "platt_logistic_on_raw_probability",
        "coef": coef,
        "intercept": intercept,
    }


def apply_calibrator(raw_probability: np.ndarray, calibrator: dict[str, object]) -> np.ndarray:
    method = str(calibrator.get("method", "raw"))
    raw = np.asarray(raw_probability, dtype=float)
    if method == "raw":
        return np.clip(raw, 0.0, 1.0)
    if method == "sigmoid":
        return np.clip(predict_sigmoid_probability(raw, float(calibrator["coef"]), float(calibrator["intercept"])), 0.0, 1.0)
    if method == "isotonic":
        x = np.asarray(calibrator["x_thresholds"], dtype=float)
        y = np.asarray(calibrator["y_thresholds"], dtype=float)
        return np.clip(np.interp(raw, x, y, left=y[0], right=y[-1]), 0.0, 1.0)
    raise ValueError(f"Unknown calibrator method: {method}")


def fit_isotonic_calibrator(raw_probability: np.ndarray, y_true: np.ndarray) -> dict[str, object]:
    if len(np.unique(y_true)) < 2:
        raise ValueError("isotonic calibrator requires both classes in calibration split")
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    iso.fit(np.asarray(raw_probability, dtype=float), np.asarray(y_true, dtype=float))
    return {
        "method": "isotonic",
        "type": "sklearn_isotonic_regression_on_raw_probability",
        "x_thresholds": [float(x) for x in iso.X_thresholds_],
        "y_thresholds": [float(y) for y in iso.y_thresholds_],
    }


def model_positive_proba(model: RandomForestClassifier, X: np.ndarray) -> np.ndarray:
    probas = model.predict_proba(X)
    classes = list(model.classes_)
    if 1 not in classes:
        return np.zeros(len(X), dtype=float)
    return np.asarray(probas[:, classes.index(1)], dtype=float)


def evaluate_probability_candidate(
    *,
    method: str,
    y_select: np.ndarray,
    prob_select: np.ndarray,
    y_holdout: np.ndarray,
    prob_holdout: np.ndarray,
    holdout_dates: pd.Series | None,
    model_kind: str,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    threshold, _cal_precision, _cal_recall, _cal_pos_rate = optimize_threshold(y_select, prob_select, model_kind)
    preds = (prob_holdout >= threshold).astype(np.int8)
    calibration = calibration_report(y_holdout, prob_holdout, bins=10)
    pred_count = int(np.sum(preds, dtype=np.int64))
    pred_pos_rate = float(preds.mean()) if len(preds) else 0.0
    if holdout_dates is not None and pred_count > 0:
        day_counts = pd.Series(holdout_dates).reset_index(drop=True)[preds.astype(bool)].value_counts()
        max_day_fraction = float(day_counts.max() / pred_count) if len(day_counts) else 0.0
    else:
        max_day_fraction = 0.0
    row = {
        "calibration_method": method,
        "threshold": float(threshold),
        "precision": float(precision_score(y_holdout, preds, zero_division=0)),
        "recall": float(recall_score(y_holdout, preds, zero_division=0)),
        "pred_pos_rate": pred_pos_rate,
        "predicted_positive_count": pred_count,
        "brier_score": calibration["brier_score"],
        "ece": calibration["ece"],
        "calibration_rows": calibration["rows"],
        "max_predicted_day_fraction": max_day_fraction,
    }
    reliability_rows = []
    for bin_row in calibration.get("bins", []):
        reliability_rows.append({"calibration_method": method, **bin_row})
    return row, reliability_rows


def fit_posthoc_calibration(
    *,
    model: RandomForestClassifier,
    x_calibration: np.ndarray,
    y_calibration: np.ndarray,
    x_holdout: np.ndarray,
    y_holdout: np.ndarray,
    holdout_dates: pd.Series | None,
    data: pd.DataFrame,
    holdout_idx: np.ndarray,
    label_col: str,
    model_kind: str,
    mode: str,
    random_state: int,
    min_frozen_holdout_rows: int,
    min_holdout_predictions: int,
    max_day_dominance_frac: float,
) -> dict[str, object]:
    mode = (mode or "none").strip().lower()
    if mode == "none":
        return {}
    methods = ["sigmoid", "isotonic"] if mode == "both" else [mode]
    raw_cal = model_positive_proba(model, x_calibration)
    raw_holdout = model_positive_proba(model, x_holdout)
    comparison_rows: list[dict[str, object]] = []
    reliability_rows: list[dict[str, object]] = []
    calibrator_candidates: list[dict[str, object]] = []
    fitted_calibrator_methods: list[str] = []
    warnings: list[str] = []

    raw_row, raw_rel = evaluate_probability_candidate(
        method="raw",
        y_select=y_calibration,
        prob_select=raw_cal,
        y_holdout=y_holdout,
        prob_holdout=raw_holdout,
        holdout_dates=holdout_dates,
        model_kind=model_kind,
    )
    comparison_rows.append(raw_row)
    reliability_rows.extend(raw_rel)
    calibrator_candidates.append({
        "method": "raw",
        "parameters": {
            "method": "raw",
            "type": "identity_raw_probability_no_posthoc_calibrator",
        },
        "metrics": raw_row,
    })

    for method in methods:
        try:
            calibrator = fit_sigmoid_calibrator(raw_cal, y_calibration, random_state) if method == "sigmoid" else fit_isotonic_calibrator(raw_cal, y_calibration)
            cal_prob_select = apply_calibrator(raw_cal, calibrator)
            cal_prob_holdout = apply_calibrator(raw_holdout, calibrator)
            candidate_row, candidate_rel = evaluate_probability_candidate(
                method=method,
                y_select=y_calibration,
                prob_select=cal_prob_select,
                y_holdout=y_holdout,
                prob_holdout=cal_prob_holdout,
                holdout_dates=holdout_dates,
                model_kind=model_kind,
            )
            comparison_rows.append(candidate_row)
            reliability_rows.extend(candidate_rel)
            calibrator_candidates.append({"method": method, "parameters": calibrator, "metrics": candidate_row})
            fitted_calibrator_methods.append(method)
        except Exception as exc:
            warnings.append(f"{method} post-hoc calibrator skipped: {exc}")

    selected = min(
        calibrator_candidates,
        key=lambda c: (
            float(c["metrics"].get("brier_score", math.inf)),
            float(c["metrics"].get("ece", math.inf)),
        ),
    )
    if selected["method"] == "raw" and fitted_calibrator_methods:
        warnings.append("Raw/no-op probabilities outperformed fitted post-hoc calibrators on frozen holdout; selected raw.")
    if not fitted_calibrator_methods:
        warnings.append("No post-hoc calibrator could be fitted; raw probabilities remain the only candidate.")

    gate_warnings = []
    frozen_rows = int(len(y_holdout))
    if frozen_rows < int(min_frozen_holdout_rows):
        gate_warnings.append(f"frozen_holdout_rows {frozen_rows} < minimum {int(min_frozen_holdout_rows)}")
    selected_metrics = selected["metrics"]
    if int(selected_metrics.get("predicted_positive_count", 0)) < int(min_holdout_predictions):
        gate_warnings.append(
            f"selected predicted_positive_count {selected_metrics.get('predicted_positive_count', 0)} < minimum {int(min_holdout_predictions)}"
        )
    if float(selected_metrics.get("max_predicted_day_fraction", 0.0)) > float(max_day_dominance_frac):
        gate_warnings.append(
            f"selected max_predicted_day_fraction {selected_metrics.get('max_predicted_day_fraction')} > maximum {float(max_day_dominance_frac):.3f}"
        )

    return {
        "enabled": True,
        "requested_mode": mode,
        "split_convention": "chronological_base_train_then_calibration_then_frozen_holdout",
        "calibration_fit_rows": int(len(y_calibration)),
        "frozen_holdout_rows": frozen_rows,
        "holdout_fingerprint_sha256": holdout_fingerprint(data, holdout_idx, label_col),
        "selected_method": selected["method"],
        "selected_metrics": selected_metrics,
        "raw_metrics": raw_row,
        "comparison_rows": comparison_rows,
        "reliability_rows": reliability_rows,
        "calibrator_candidates": calibrator_candidates,
        "promotion_gate": {
            "promotion_ready": False,
            "warnings": gate_warnings + warnings + [
                "Post-hoc calibration artifacts are research-only until runtime applies calibration hashes and full promotion gates pass."
            ],
        },
    }


def export_onnx(model: RandomForestClassifier, feature_count: int, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    initial_type = [("float_input", FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type, options={id(model): {"zipmap": False}}, target_opset=12)
    if getattr(onnx_model, "ir_version", 0) > 9:
        onnx_model.ir_version = 9
    path.write_bytes(onnx_model.SerializeToString())


def train_binary_model(
    df: pd.DataFrame,
    label_col: str,
    name: str,
    filename: str,
    model_kind: str,
    output_dir: Path,
    min_rows: int,
    random_state: int,
    no_onnx: bool,
    posthoc_calibration: str = "none",
    posthoc_calibration_frac: float = 0.20,
    frozen_holdout_frac: float = 0.20,
    min_frozen_holdout_rows: int = 200,
    min_holdout_predictions: int = 20,
    max_day_dominance_frac: float = 0.40,
) -> TrainedModelResult | None:
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
    posthoc_mode = (posthoc_calibration or "none").strip().lower()
    if posthoc_mode not in POSTHOC_CALIBRATION_METHODS:
        raise ValueError(f"Unsupported posthoc_calibration={posthoc_calibration}")
    if posthoc_mode == "none":
        train_idx, test_idx = train_test_split_time(data)
        calibration_idx = np.asarray([], dtype=np.int64)
        holdout_idx = test_idx
    else:
        train_idx, calibration_idx, holdout_idx = chronological_three_way_split(
            len(data),
            calibration_frac=posthoc_calibration_frac,
            holdout_frac=frozen_holdout_frac,
        )
        test_idx = holdout_idx
        if len(calibration_idx) == 0 or len(holdout_idx) == 0:
            print(f"WARN {name}: post-hoc calibration requested but split is too small; falling back to raw 80/20 split")
            posthoc_mode = "none"
            train_idx, test_idx = train_test_split_time(data)
            calibration_idx = np.asarray([], dtype=np.int64)
            holdout_idx = test_idx
    model = RandomForestClassifier(n_estimators=260, max_depth=9, min_samples_leaf=20, min_samples_split=40, max_features="sqrt", random_state=random_state, n_jobs=-1)
    model.fit(X[train_idx], y[train_idx])
    test_proba = model_positive_proba(model, X[test_idx])
    threshold, precision, recall, pred_pos_rate = optimize_threshold(y[test_idx], test_proba, model_kind)
    calibration = calibration_report(y[test_idx], test_proba, bins=10)
    posthoc_result: dict[str, object] | None = None
    if posthoc_mode != "none" and len(calibration_idx) and len(holdout_idx):
        holdout_dates = data.iloc[holdout_idx]["Date"] if "Date" in data.columns else None
        posthoc_result = fit_posthoc_calibration(
            model=model,
            x_calibration=X[calibration_idx],
            y_calibration=y[calibration_idx],
            x_holdout=X[holdout_idx],
            y_holdout=y[holdout_idx],
            holdout_dates=holdout_dates,
            data=data,
            holdout_idx=holdout_idx,
            label_col=label_col,
            model_kind=model_kind,
            mode=posthoc_mode,
            random_state=random_state,
            min_frozen_holdout_rows=min_frozen_holdout_rows,
            min_holdout_predictions=min_holdout_predictions,
            max_day_dominance_frac=max_day_dominance_frac,
        )
    export_path = output_dir / filename
    if not no_onnx:
        export_onnx(model, len(feature_cols), export_path)
    posthoc_summary = ""
    if posthoc_result:
        posthoc_summary = (
            f" posthoc={posthoc_result.get('selected_method')}"
            f" posthoc_brier={posthoc_result.get('selected_metrics', {}).get('brier_score')}"
            f" frozen_rows={posthoc_result.get('frozen_holdout_rows')}"
        )
    print(f"TRAINED {name}: rows={len(data)} positives={int(y.sum())} threshold={threshold:.2f} precision={precision:.2%} recall={recall:.2%}{posthoc_summary} export={export_path if not no_onnx else 'disabled'}")
    return TrainedModelResult(
        name=name,
        filename=filename,
        threshold=threshold,
        precision=precision,
        recall=recall,
        brier_score=float(calibration["brier_score"]),
        ece=float(calibration["ece"]),
        calibration_rows=int(calibration["rows"]),
        calibration_bins=list(calibration["bins"]),
        pred_pos_rate=pred_pos_rate,
        label_pos_rate=float(y.mean()),
        rows=len(data),
        positives=int(y.sum()),
        feature_count=len(feature_cols),
        feature_columns=feature_cols,
        exported_to=str(export_path) if not no_onnx else "",
        posthoc_calibration=posthoc_result,
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
    setup_predictions: pd.DataFrame | None,
    min_setup_score_unique_values: int,
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
        df30 = apply_setup_predictions(df30, setup_predictions, min_unique_values=min_setup_score_unique_values)
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
    feature_schema = {}
    reliability_rows = []
    posthoc_comparison_rows = []
    posthoc_reliability_rows = []
    posthoc_calibrator_models = []
    calibration_metrics = []
    for r in results:
        schema_hash = feature_schema_hash(r.feature_columns)
        posthoc = r.posthoc_calibration or {}
        selected_posthoc_metrics = posthoc.get("selected_metrics", {}) if posthoc else {}
        rows.append({
            "model": r.name,
            "filename": r.filename,
            "rows": r.rows,
            "positives": r.positives,
            "label_pos_rate": r.label_pos_rate,
            "threshold": r.threshold,
            "precision": r.precision,
            "recall": r.recall,
            "brier_score": r.brier_score,
            "ece": r.ece,
            "calibration_rows": r.calibration_rows,
            "posthoc_selected_method": posthoc.get("selected_method", ""),
            "posthoc_brier_score": selected_posthoc_metrics.get("brier_score", ""),
            "posthoc_ece": selected_posthoc_metrics.get("ece", ""),
            "posthoc_threshold": selected_posthoc_metrics.get("threshold", ""),
            "posthoc_calibration_rows": selected_posthoc_metrics.get("calibration_rows", ""),
            "pred_pos_rate": r.pred_pos_rate,
            "feature_count": r.feature_count,
            "feature_schema_sha256": schema_hash,
            "exported_to": r.exported_to,
        })
        route.append({
            "model": r.name,
            "model_path": r.exported_to,
            "threshold": r.threshold,
            "calibration": {
                "method": "raw_random_forest_probability_no_posthoc_calibrator",
                "brier_score": r.brier_score,
                "ece": r.ece,
                "rows": r.calibration_rows,
                "reliability_artifact": "calibration_reliability.csv",
                "posthoc": {
                    "enabled": bool(posthoc),
                    "selected_method": posthoc.get("selected_method", ""),
                    "selected_metrics": selected_posthoc_metrics,
                    "calibrator_artifact": "posthoc_calibrators.json" if posthoc else "",
                    "comparison_artifact": "posthoc_calibration_comparison.csv" if posthoc else "",
                    "promotion_gate": posthoc.get("promotion_gate", {}) if posthoc else {},
                },
            },
            "feature_count": r.feature_count,
            "feature_schema_sha256": schema_hash,
            "feature_columns": r.feature_columns,
        })
        feature_schema[r.name] = {
            "feature_count": r.feature_count,
            "feature_schema_sha256": schema_hash,
            "feature_columns": r.feature_columns,
        }
        calibration_metrics.append({
            "model": r.name,
            "filename": r.filename,
            "method": "raw_random_forest_probability_no_posthoc_calibrator",
            "rows": r.calibration_rows,
            "calibration_rows": r.calibration_rows,
            "brier_score": r.brier_score,
            "ece": r.ece,
            "threshold": r.threshold,
        })
        if posthoc:
            for row in posthoc.get("comparison_rows", []):
                posthoc_comparison_rows.append({"model": r.name, "filename": r.filename, **row})
            for row in posthoc.get("reliability_rows", []):
                posthoc_reliability_rows.append({"model": r.name, "filename": r.filename, **row})
            for candidate in posthoc.get("calibrator_candidates", []):
                posthoc_calibrator_models.append({
                    "model": r.name,
                    "filename": r.filename,
                    "feature_schema_sha256": schema_hash,
                    "holdout_fingerprint_sha256": posthoc.get("holdout_fingerprint_sha256"),
                    "selected": candidate.get("method") == posthoc.get("selected_method"),
                    "method": candidate.get("method"),
                    "parameters": candidate.get("parameters"),
                    "metrics": candidate.get("metrics"),
                })
        for bin_row in r.calibration_bins:
            row = {"model": r.name, "filename": r.filename, "calibration_method": "raw"}
            row.update(bin_row)
            reliability_rows.append(row)
        calibration_metric = calibration_metrics[-1]
        if posthoc:
            calibration_metric["posthoc"] = {
                "enabled": True,
                "requested_mode": posthoc.get("requested_mode"),
                "selected_method": posthoc.get("selected_method"),
                "selected_metrics": selected_posthoc_metrics,
                "raw_metrics": posthoc.get("raw_metrics", {}),
                "calibration_fit_rows": posthoc.get("calibration_fit_rows"),
                "frozen_holdout_rows": posthoc.get("frozen_holdout_rows"),
                "holdout_fingerprint_sha256": posthoc.get("holdout_fingerprint_sha256"),
                "promotion_gate": posthoc.get("promotion_gate", {}),
            }
        else:
            calibration_metric["posthoc"] = {"enabled": False}
    pd.DataFrame(rows).to_csv(output_dir / "lifecycle_micro_scorecard.csv", index=False)
    pd.DataFrame(reliability_rows).to_csv(output_dir / "calibration_reliability.csv", index=False)
    posthoc_comparison_cols = [
        "model", "filename", "calibration_method", "threshold", "precision", "recall",
        "pred_pos_rate", "predicted_positive_count", "brier_score", "ece",
        "calibration_rows", "max_predicted_day_fraction",
    ]
    posthoc_reliability_cols = [
        "model", "filename", "calibration_method", "bin_index", "prob_min", "prob_max",
        "rows", "mean_predicted_probability", "observed_positive_rate", "abs_calibration_error",
    ]
    pd.DataFrame(posthoc_comparison_rows, columns=posthoc_comparison_cols).to_csv(output_dir / "posthoc_calibration_comparison.csv", index=False)
    pd.DataFrame(posthoc_reliability_rows, columns=posthoc_reliability_cols).to_csv(output_dir / "posthoc_calibration_reliability.csv", index=False)
    posthoc_calibrators = {
        "schema_version": "lifecycle_micro_posthoc_calibrators_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "models": posthoc_calibrator_models,
        "warnings": [
            "These calibrators are research-only until Java/runtime applies calibration hashes and full promotion gates pass."
        ],
    }
    (output_dir / "posthoc_calibrators.json").write_text(json.dumps(posthoc_calibrators, indent=2), encoding="utf-8")
    posthoc_enabled = any(bool(r.posthoc_calibration) for r in results)
    manifest_method = (
        "raw_random_forest_probability_with_posthoc_sigmoid_isotonic_comparison"
        if posthoc_enabled else
        "raw_random_forest_probability_no_posthoc_calibrator"
    )
    manifest_holdout_split = (
        "chronological_base_train_then_calibration_then_frozen_holdout"
        if posthoc_enabled else
        "chronological_last_20_percent_from_train_test_split_time"
    )
    manifest_warnings = [
        "Raw scorecard fields still describe raw RandomForest predict_proba outputs; use per-model posthoc fields and posthoc artifacts for calibrated-probability analysis.",
        "Post-hoc calibrators are research-only until runtime applies calibration hashes and full promotion gates pass.",
    ] if posthoc_enabled else [
        "Metrics describe raw RandomForest predict_proba outputs; no isotonic/Platt post-hoc calibrator is exported yet. Treat bundles as research-only until calibration and holdout gates pass."
    ]
    calibration_manifest = {
        "schema_version": "lifecycle_micro_calibration_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "method": manifest_method,
        "holdout_split": manifest_holdout_split,
        "bins": 10,
        "errors": [],
        "warnings": manifest_warnings,
        "artifacts": {
            "scorecard_csv": "lifecycle_micro_scorecard.csv",
            "reliability_csv": "calibration_reliability.csv",
            "posthoc_comparison_csv": "posthoc_calibration_comparison.csv",
            "posthoc_reliability_csv": "posthoc_calibration_reliability.csv",
            "posthoc_calibrators_json": "posthoc_calibrators.json",
            "route_manifest_json": "lifecycle_micro_route_manifest.json",
        },
        "models": calibration_metrics,
    }
    (output_dir / "calibration_manifest.json").write_text(json.dumps(calibration_manifest, indent=2), encoding="utf-8")
    (output_dir / "lifecycle_micro_route_manifest.json").write_text(json.dumps(route, indent=2), encoding="utf-8")
    feature_schema_json = json.dumps(feature_schema, indent=2, sort_keys=True)
    (output_dir / "feature_schema.json").write_text(feature_schema_json, encoding="utf-8")
    (output_dir / "feature_schema.sha256").write_text(hashlib.sha256(feature_schema_json.encode("utf-8")).hexdigest() + "\n", encoding="utf-8")


def main() -> None:
    global ALLOW_BOOTSTRAP_SETUP_PROXY
    args = parse_args()
    ALLOW_BOOTSTRAP_SETUP_PROXY = bool(args.allow_bootstrap_setup_proxy or ALLOW_BOOTSTRAP_SETUP_PROXY)
    run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else DEFAULT_MODEL_EXPORTS_ROOT / f"lifecycle_micro_{run_tag}"
    output_dir.mkdir(parents=True, exist_ok=True)

    input_30s = Path(args.input_30s_csv).expanduser()
    input_5s = Path(args.input_5s_csv).expanduser()
    setup_predictions = load_setup_predictions(args.setup_predictions_csv) if args.setup_predictions_csv else None
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
            setup_predictions,
            args.min_setup_score_unique_values,
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
        df30 = apply_setup_predictions(df30, setup_predictions, min_unique_values=args.min_setup_score_unique_values)
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
        result = train_binary_model(
            dataset,
            label_col,
            name,
            filename,
            kind,
            output_dir,
            args.min_rows,
            args.random_state + i,
            args.no_onnx,
            posthoc_calibration=args.posthoc_calibration,
            posthoc_calibration_frac=args.posthoc_calibration_frac,
            frozen_holdout_frac=args.frozen_holdout_frac,
            min_frozen_holdout_rows=args.min_frozen_holdout_rows,
            min_holdout_predictions=args.min_holdout_predictions,
            max_day_dominance_frac=args.max_day_dominance_frac,
        )
        if result is not None:
            results.append(result)

    write_scorecards(output_dir, results)
    print(f"WROTE {output_dir / 'lifecycle_micro_scorecard.csv'}")
    print(f"WROTE {output_dir / 'lifecycle_micro_route_manifest.json'}")
    print(f"WROTE {output_dir / 'calibration_manifest.json'}")
    print(f"WROTE {output_dir / 'calibration_reliability.csv'}")
    print(f"WROTE {output_dir / 'posthoc_calibration_comparison.csv'}")
    print(f"WROTE {output_dir / 'posthoc_calibration_reliability.csv'}")
    print(f"WROTE {output_dir / 'posthoc_calibrators.json'}")
    print(f"WROTE {output_dir / 'feature_schema.json'}")


if __name__ == "__main__":
    main()

