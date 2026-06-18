#!/usr/bin/env python3
"""Build 30s training CSVs enriched with QA-passed Databento silver features.

This script is intentionally a feature-join/build step only. It does not train,
backtest, promote, or overwrite existing model artifacts. It consumes:

* an existing baseline 30s training CSV or directory of ``*_30s_training.csv``
  files, usually from the fixed-quality Databento pilot build; and
* Phase 1 silver CSVs under ``equs_mbp1_1s`` and ``opra_tcbbo_1s`` after
  ``verify_databento_silver_outputs.py`` has passed.

The join is exact on the left edge of each 30-second regular-session bucket.
No forward/asof future fill is used. Missing silver rows are fatal by default.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from databento_silver_utils import MARKET_TZ, normalize_date_arg, split_symbols, utc_now, write_json


SCHEMA_VERSION = "databento_enriched_30s_from_silver_v1"
EQUS_SUMMARY = "equs_mbp1_silver_summary.csv"
OPRA_SUMMARY = "opra_tcbbo_silver_summary.csv"
ENRICHED_SUMMARY = "enriched_feature_join_summary.csv"
SECONDS_PER_30S_BAR = 30.0

EQUS_FEATURE_COLUMNS = [
    "EqMbp1SilverRows30s",
    "EqMbp1JoinCoverage30s",
    "EqMbp1SourceMaxLagSec30s",
    "EqMbp1BidMean30s",
    "EqMbp1AskMean30s",
    "EqMbp1BidLast30s",
    "EqMbp1AskLast30s",
    "EqMbp1BidSizeMean30s",
    "EqMbp1AskSizeMean30s",
    "EqMbp1BidSizeLast30s",
    "EqMbp1AskSizeLast30s",
    "EqMbp1BidCountMean30s",
    "EqMbp1AskCountMean30s",
    "EqMbp1MidMean30s",
    "EqMbp1MidLast30s",
    "EqMbp1MicropriceMean30s",
    "EqMbp1MicropriceLast30s",
    "EqMbp1SpreadMean30s",
    "EqMbp1SpreadBpsMean30s",
    "EqMbp1SpreadBpsLast30s",
    "EqMbp1RawSpreadMinBps30s",
    "EqMbp1RawSpreadMaxBps30s",
    "EqMbp1L1ImbalanceMean30s",
    "EqMbp1L1ImbalanceLast30s",
    "EqMbp1QuoteUpdateCount30s",
    "EqMbp1EventCount30s",
    "EqMbp1QuoteUpdateCoverage30s",
    "EqMbp1QuoteStateValidCoverage30s",
    "EqMbp1ValidSpreadCoverage30s",
    "EqMbp1LockedCrossedSeconds30s",
    "EqMbp1LockedCrossedCoverage30s",
    "EqMbp1QuoteAgeMsMean30s",
    "EqMbp1QuoteAgeMsMax30s",
]

OPRA_FEATURE_COLUMNS = [
    "OpraTcbboSilverRows30s",
    "OpraTcbboJoinCoverage30s",
    "OpraTcbboSourceMaxLagSec30s",
    "OpraTcbboCallTradeCount30s",
    "OpraTcbboPutTradeCount30s",
    "OpraTcbboTotalTradeCount30s",
    "OpraTcbboCallContractVolume30s",
    "OpraTcbboPutContractVolume30s",
    "OpraTcbboTotalContractVolume30s",
    "OpraTcbboCallPremiumNotional30s",
    "OpraTcbboPutPremiumNotional30s",
    "OpraTcbboTotalPremiumNotional30s",
    "OpraTcbboCallQuoteContextCount30s",
    "OpraTcbboPutQuoteContextCount30s",
    "OpraTcbboTotalQuoteContextCount30s",
    "OpraTcbboCallActiveContractSeconds30s",
    "OpraTcbboPutActiveContractSeconds30s",
    "OpraTcbboCallActiveSeconds30s",
    "OpraTcbboPutActiveSeconds30s",
    "OpraTcbboAnyActiveSeconds30s",
    "OpraTcbboAnyActiveCoverage30s",
    "OpraTcbboCallAvgSpreadBpsMean30s",
    "OpraTcbboPutAvgSpreadBpsMean30s",
    "OpraTcbboCallMedianSpreadBpsMean30s",
    "OpraTcbboPutMedianSpreadBpsMean30s",
    "OpraTcbboCallMinSpreadBps30s",
    "OpraTcbboPutMinSpreadBps30s",
    "OpraTcbboCallAtBidVolume30s",
    "OpraTcbboPutAtBidVolume30s",
    "OpraTcbboCallAtAskVolume30s",
    "OpraTcbboPutAtAskVolume30s",
    "OpraTcbboCallMinusPutVolume30s",
    "OpraTcbboOptionVolumeImbalance30s",
    "OpraTcbboPutCallVolumeRatio30s",
]

ALL_FEATURE_COLUMNS = EQUS_FEATURE_COLUMNS + OPRA_FEATURE_COLUMNS
ZERO_FILL_FEATURE_COLUMNS = {
    column
    for column in ALL_FEATURE_COLUMNS
    if (
        "Rows30s" in column
        or "Coverage30s" in column
        or "Count30s" in column
        or "Volume30s" in column
        or "Notional30s" in column
        or "Seconds30s" in column
        or "JoinCoverage30s" in column
        or "LockedCrossed" in column
        or "Imbalance30s" in column
        or "Ratio30s" in column
    )
}
ZERO_FILL_FEATURE_COLUMNS.update(
    {
        "EqMbp1SourceMaxLagSec30s",
        "OpraTcbboSourceMaxLagSec30s",
        "OpraTcbboCallMinusPutVolume30s",
    }
)


@dataclass
class EnrichedSummaryRow:
    symbol: str
    date: str
    baseline_rows: int
    output_rows: int
    expected_rows: int
    row_count_ok: bool
    equs_feature_rows: int
    opra_feature_rows: int
    missing_equs_rows: int
    missing_opra_rows: int
    mean_equs_join_coverage: float
    mean_opra_join_coverage: float
    max_equs_source_lag_sec: float
    max_opra_source_lag_sec: float
    total_opra_contract_volume_30s: float
    output_path: str


class CsvAppender:
    def __init__(self, path: Path):
        self.path = path
        self.columns: list[str] | None = None

    def append(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        out = frame.copy()
        if self.columns is None:
            self.columns = list(out.columns)
        else:
            extra = [column for column in out.columns if column not in self.columns]
            if extra:
                raise ValueError(f"schema changed while writing {self.path}; unexpected columns={extra}")
            for column in self.columns:
                if column not in out.columns:
                    out[column] = pd.NA
            out = out[self.columns]
        out.to_csv(self.path, mode="a", header=not self.path.exists(), index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _normalize_dates(dates: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in dates or []:
        date = str(raw or "").strip().replace("-", "")
        if len(date) != 8 or not date.isdigit():
            raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD, got {raw!r}")
        if date not in seen:
            seen.add(date)
            out.append(date)
    return out


def _safe_divide(numerator: pd.Series | float, denominator: pd.Series | float, default: float = 0.0) -> pd.Series | float:
    if isinstance(numerator, pd.Series) or isinstance(denominator, pd.Series):
        num = numerator if isinstance(numerator, pd.Series) else pd.Series(numerator, index=denominator.index)  # type: ignore[union-attr]
        den = denominator if isinstance(denominator, pd.Series) else pd.Series(denominator, index=num.index)
        out = num.astype(float) / den.replace(0.0, np.nan).astype(float)
        return out.replace([np.inf, -np.inf], np.nan).fillna(default)
    if not denominator:
        return default
    return numerator / denominator


def _numeric(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def parse_timestamp_epoch_seconds(timestamp: pd.Series) -> pd.Series:
    """Parse repository-local timestamp strings into UTC epoch seconds.

    Existing training CSVs use strings such as
    ``20260521 09:30:00 America/New_York``. We extract the local wall-clock
    component and localize it to the market timezone so joins are independent of
    the machine's local timezone.
    """

    raw = timestamp.astype(str).str.strip()
    extracted = raw.str.extract(r"(\d{8}\s+\d{2}:\d{2}:\d{2})", expand=False)
    parsed = pd.to_datetime(extracted, format="%Y%m%d %H:%M:%S", errors="coerce")
    if parsed.isna().any():
        fallback = pd.to_datetime(raw.where(parsed.isna()), errors="coerce")
        if fallback.notna().any():
            fallback_naive = fallback.dt.tz_localize(None) if getattr(fallback.dt, "tz", None) is not None else fallback
            parsed = parsed.fillna(fallback_naive)

    localized = parsed.dt.tz_localize(MARKET_TZ, ambiguous="NaT", nonexistent="NaT")
    epochs = pd.Series(np.nan, index=timestamp.index, dtype="float64")
    valid = localized.notna()
    if valid.any():
        epochs.loc[valid] = localized.loc[valid].map(lambda ts: int(ts.tz_convert("UTC").timestamp())).astype(float)
    return epochs


def timestamp_dates(timestamp: pd.Series) -> pd.Series:
    raw = timestamp.astype(str).str.strip()
    ymd = raw.str.extract(r"(\d{8})", expand=False)
    return ymd.where(ymd.str.match(r"\d{8}", na=False), "")


def _bucket_epoch_seconds(frame: pd.DataFrame) -> pd.Series:
    epoch = pd.to_numeric(frame.get("BarEpochSec"), errors="coerce")
    if epoch.isna().any():
        raise ValueError("silver frame has missing/non-numeric BarEpochSec values")
    if "SecondsFromOpen" in frame.columns:
        seconds = pd.to_numeric(frame["SecondsFromOpen"], errors="coerce").fillna(0.0)
        return (epoch - (seconds % SECONDS_PER_30S_BAR)).astype("int64")
    return ((epoch // SECONDS_PER_30S_BAR) * SECONDS_PER_30S_BAR).astype("int64")


def _sum(grouped: pd.core.groupby.DataFrameGroupBy, column: str) -> pd.Series:
    return grouped[column].sum(min_count=1).fillna(0.0)


def _mean(grouped: pd.core.groupby.DataFrameGroupBy, column: str) -> pd.Series:
    return grouped[column].mean()


def _last(grouped: pd.core.groupby.DataFrameGroupBy, column: str) -> pd.Series:
    return grouped[column].last()


def _min(grouped: pd.core.groupby.DataFrameGroupBy, column: str) -> pd.Series:
    return grouped[column].min()


def _max(grouped: pd.core.groupby.DataFrameGroupBy, column: str) -> pd.Series:
    return grouped[column].max()


def aggregate_equs_1s_to_30s(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["_BucketEpochSec", *EQUS_FEATURE_COLUMNS])

    working = pd.DataFrame(index=frame.index)
    working["_BucketEpochSec"] = _bucket_epoch_seconds(frame)
    working["BarEpochSec"] = _numeric(frame, "BarEpochSec", default=np.nan)
    for column in [
        "Bid",
        "Ask",
        "BidSize",
        "AskSize",
        "BidCount",
        "AskCount",
        "Mid",
        "Microprice",
        "Spread",
        "SpreadBps",
        "RawSpreadMinBps1s",
        "RawSpreadMaxBps1s",
        "L1Imbalance",
        "QuoteUpdateCount1s",
        "EventCount1s",
        "QuoteCoverage1s",
        "QuoteStateValid1s",
        "ValidSpread1s",
        "LockedCrossed1s",
        "QuoteAgeMs",
    ]:
        default = 0.0 if column.endswith("Count1s") or column in {"QuoteCoverage1s", "QuoteStateValid1s", "ValidSpread1s", "LockedCrossed1s"} else np.nan
        working[column] = _numeric(frame, column, default=default)

    grouped = working.groupby("_BucketEpochSec", sort=True)
    out = pd.DataFrame(index=grouped.size().index)
    out["EqMbp1SilverRows30s"] = grouped.size().astype(float)
    out["EqMbp1JoinCoverage30s"] = (out["EqMbp1SilverRows30s"] / SECONDS_PER_30S_BAR).clip(0.0, 1.0)
    out["EqMbp1SourceMaxLagSec30s"] = (_max(grouped, "BarEpochSec") - out.index.to_series()).clip(lower=0.0)
    out["EqMbp1BidMean30s"] = _mean(grouped, "Bid")
    out["EqMbp1AskMean30s"] = _mean(grouped, "Ask")
    out["EqMbp1BidLast30s"] = _last(grouped, "Bid")
    out["EqMbp1AskLast30s"] = _last(grouped, "Ask")
    out["EqMbp1BidSizeMean30s"] = _mean(grouped, "BidSize")
    out["EqMbp1AskSizeMean30s"] = _mean(grouped, "AskSize")
    out["EqMbp1BidSizeLast30s"] = _last(grouped, "BidSize")
    out["EqMbp1AskSizeLast30s"] = _last(grouped, "AskSize")
    out["EqMbp1BidCountMean30s"] = _mean(grouped, "BidCount")
    out["EqMbp1AskCountMean30s"] = _mean(grouped, "AskCount")
    out["EqMbp1MidMean30s"] = _mean(grouped, "Mid")
    out["EqMbp1MidLast30s"] = _last(grouped, "Mid")
    out["EqMbp1MicropriceMean30s"] = _mean(grouped, "Microprice")
    out["EqMbp1MicropriceLast30s"] = _last(grouped, "Microprice")
    out["EqMbp1SpreadMean30s"] = _mean(grouped, "Spread")
    out["EqMbp1SpreadBpsMean30s"] = _mean(grouped, "SpreadBps")
    out["EqMbp1SpreadBpsLast30s"] = _last(grouped, "SpreadBps")
    out["EqMbp1RawSpreadMinBps30s"] = _min(grouped, "RawSpreadMinBps1s")
    out["EqMbp1RawSpreadMaxBps30s"] = _max(grouped, "RawSpreadMaxBps1s")
    out["EqMbp1L1ImbalanceMean30s"] = _mean(grouped, "L1Imbalance")
    out["EqMbp1L1ImbalanceLast30s"] = _last(grouped, "L1Imbalance")
    out["EqMbp1QuoteUpdateCount30s"] = _sum(grouped, "QuoteUpdateCount1s")
    out["EqMbp1EventCount30s"] = _sum(grouped, "EventCount1s")
    out["EqMbp1QuoteUpdateCoverage30s"] = _mean(grouped, "QuoteCoverage1s").fillna(0.0)
    out["EqMbp1QuoteStateValidCoverage30s"] = _mean(grouped, "QuoteStateValid1s").fillna(0.0)
    out["EqMbp1ValidSpreadCoverage30s"] = _mean(grouped, "ValidSpread1s").fillna(0.0)
    out["EqMbp1LockedCrossedSeconds30s"] = _sum(grouped, "LockedCrossed1s")
    out["EqMbp1LockedCrossedCoverage30s"] = _mean(grouped, "LockedCrossed1s").fillna(0.0)
    out["EqMbp1QuoteAgeMsMean30s"] = _mean(grouped, "QuoteAgeMs")
    out["EqMbp1QuoteAgeMsMax30s"] = _max(grouped, "QuoteAgeMs")
    return out.reset_index().rename(columns={"_BucketEpochSec": "_BucketEpochSec"})[["_BucketEpochSec", *EQUS_FEATURE_COLUMNS]]


def aggregate_opra_1s_to_30s(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["_BucketEpochSec", *OPRA_FEATURE_COLUMNS])

    working = pd.DataFrame(index=frame.index)
    working["_BucketEpochSec"] = _bucket_epoch_seconds(frame)
    working["BarEpochSec"] = _numeric(frame, "BarEpochSec", default=np.nan)
    source_columns = [
        "CallOptionTradeCount1s",
        "PutOptionTradeCount1s",
        "CallOptionContractVolume1s",
        "PutOptionContractVolume1s",
        "CallOptionPremiumNotional1s",
        "PutOptionPremiumNotional1s",
        "CallOptionQuoteContextCount1s",
        "PutOptionQuoteContextCount1s",
        "CallOptionActiveContracts1s",
        "PutOptionActiveContracts1s",
        "CallOptionAvgSpreadBps1s",
        "PutOptionAvgSpreadBps1s",
        "CallOptionMedianSpreadBps1s",
        "PutOptionMedianSpreadBps1s",
        "CallOptionMinSpreadBps1s",
        "PutOptionMinSpreadBps1s",
        "CallOptionAtBidVolume1s",
        "PutOptionAtBidVolume1s",
        "CallOptionAtAskVolume1s",
        "PutOptionAtAskVolume1s",
    ]
    flow_like = {column for column in source_columns if column.endswith(("Count1s", "Volume1s", "Notional1s", "Contracts1s"))}
    for column in source_columns:
        working[column] = _numeric(frame, column, default=0.0 if column in flow_like else np.nan)

    grouped = working.groupby("_BucketEpochSec", sort=True)
    out = pd.DataFrame(index=grouped.size().index)
    out["OpraTcbboSilverRows30s"] = grouped.size().astype(float)
    out["OpraTcbboJoinCoverage30s"] = (out["OpraTcbboSilverRows30s"] / SECONDS_PER_30S_BAR).clip(0.0, 1.0)
    out["OpraTcbboSourceMaxLagSec30s"] = (_max(grouped, "BarEpochSec") - out.index.to_series()).clip(lower=0.0)
    out["OpraTcbboCallTradeCount30s"] = _sum(grouped, "CallOptionTradeCount1s")
    out["OpraTcbboPutTradeCount30s"] = _sum(grouped, "PutOptionTradeCount1s")
    out["OpraTcbboCallContractVolume30s"] = _sum(grouped, "CallOptionContractVolume1s")
    out["OpraTcbboPutContractVolume30s"] = _sum(grouped, "PutOptionContractVolume1s")
    out["OpraTcbboCallPremiumNotional30s"] = _sum(grouped, "CallOptionPremiumNotional1s")
    out["OpraTcbboPutPremiumNotional30s"] = _sum(grouped, "PutOptionPremiumNotional1s")
    out["OpraTcbboCallQuoteContextCount30s"] = _sum(grouped, "CallOptionQuoteContextCount1s")
    out["OpraTcbboPutQuoteContextCount30s"] = _sum(grouped, "PutOptionQuoteContextCount1s")
    out["OpraTcbboCallActiveContractSeconds30s"] = _sum(grouped, "CallOptionActiveContracts1s")
    out["OpraTcbboPutActiveContractSeconds30s"] = _sum(grouped, "PutOptionActiveContracts1s")
    out["OpraTcbboCallActiveSeconds30s"] = grouped["CallOptionActiveContracts1s"].apply(lambda series: float(pd.to_numeric(series, errors="coerce").fillna(0.0).gt(0.0).sum()))
    out["OpraTcbboPutActiveSeconds30s"] = grouped["PutOptionActiveContracts1s"].apply(lambda series: float(pd.to_numeric(series, errors="coerce").fillna(0.0).gt(0.0).sum()))
    active_any = working["CallOptionActiveContracts1s"].fillna(0.0).add(working["PutOptionActiveContracts1s"].fillna(0.0)).gt(0.0).astype(float)
    out["OpraTcbboAnyActiveSeconds30s"] = active_any.groupby(working["_BucketEpochSec"]).sum()
    out["OpraTcbboAnyActiveCoverage30s"] = (out["OpraTcbboAnyActiveSeconds30s"] / SECONDS_PER_30S_BAR).clip(0.0, 1.0)
    out["OpraTcbboCallAvgSpreadBpsMean30s"] = _mean(grouped, "CallOptionAvgSpreadBps1s")
    out["OpraTcbboPutAvgSpreadBpsMean30s"] = _mean(grouped, "PutOptionAvgSpreadBps1s")
    out["OpraTcbboCallMedianSpreadBpsMean30s"] = _mean(grouped, "CallOptionMedianSpreadBps1s")
    out["OpraTcbboPutMedianSpreadBpsMean30s"] = _mean(grouped, "PutOptionMedianSpreadBps1s")
    out["OpraTcbboCallMinSpreadBps30s"] = _min(grouped, "CallOptionMinSpreadBps1s")
    out["OpraTcbboPutMinSpreadBps30s"] = _min(grouped, "PutOptionMinSpreadBps1s")
    out["OpraTcbboCallAtBidVolume30s"] = _sum(grouped, "CallOptionAtBidVolume1s")
    out["OpraTcbboPutAtBidVolume30s"] = _sum(grouped, "PutOptionAtBidVolume1s")
    out["OpraTcbboCallAtAskVolume30s"] = _sum(grouped, "CallOptionAtAskVolume1s")
    out["OpraTcbboPutAtAskVolume30s"] = _sum(grouped, "PutOptionAtAskVolume1s")

    out["OpraTcbboTotalTradeCount30s"] = out["OpraTcbboCallTradeCount30s"] + out["OpraTcbboPutTradeCount30s"]
    out["OpraTcbboTotalContractVolume30s"] = out["OpraTcbboCallContractVolume30s"] + out["OpraTcbboPutContractVolume30s"]
    out["OpraTcbboTotalPremiumNotional30s"] = out["OpraTcbboCallPremiumNotional30s"] + out["OpraTcbboPutPremiumNotional30s"]
    out["OpraTcbboTotalQuoteContextCount30s"] = out["OpraTcbboCallQuoteContextCount30s"] + out["OpraTcbboPutQuoteContextCount30s"]
    out["OpraTcbboCallMinusPutVolume30s"] = out["OpraTcbboCallContractVolume30s"] - out["OpraTcbboPutContractVolume30s"]
    out["OpraTcbboOptionVolumeImbalance30s"] = _safe_divide(out["OpraTcbboCallMinusPutVolume30s"], out["OpraTcbboTotalContractVolume30s"] + 1.0, default=0.0)
    out["OpraTcbboPutCallVolumeRatio30s"] = _safe_divide(out["OpraTcbboPutContractVolume30s"], out["OpraTcbboCallContractVolume30s"] + 1.0, default=0.0)
    return out.reset_index().rename(columns={"_BucketEpochSec": "_BucketEpochSec"})[["_BucketEpochSec", *OPRA_FEATURE_COLUMNS]]


def _resolve_baseline_files(path: Path, symbols: list[str]) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"baseline 30s path does not exist: {path}")
    data_dir = path / "data_30s" if (path / "data_30s").is_dir() else path
    files = []
    wanted = set(symbols)
    for csv_path in sorted(data_dir.glob("*_30s_training.csv")):
        symbol = csv_path.name.removesuffix("_30s_training.csv").upper()
        if not wanted or symbol in wanted:
            files.append(csv_path)
    if not files:
        raise FileNotFoundError(f"no *_30s_training.csv files found for requested symbols under {data_dir}")
    return files


def load_baseline_30s(path: Path, symbols: list[str], expected_dates: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for csv_path in _resolve_baseline_files(path, symbols):
        frame = pd.read_csv(csv_path)
        if "Symbol" not in frame.columns:
            symbol = csv_path.name.removesuffix("_30s_training.csv").upper()
            frame.insert(0, "Symbol", symbol)
        frames.append(frame)
    if not frames:
        raise ValueError("baseline 30s input produced no rows")
    baseline = pd.concat(frames, ignore_index=True)
    baseline["_BaselineOrder"] = np.arange(len(baseline), dtype=np.int64)
    if "Timestamp" not in baseline.columns:
        raise ValueError("baseline 30s CSV must contain Timestamp")
    baseline["Symbol"] = baseline["Symbol"].astype(str).str.strip().str.upper()
    baseline["_Date"] = timestamp_dates(baseline["Timestamp"])
    baseline["_BucketEpochSec"] = parse_timestamp_epoch_seconds(baseline["Timestamp"])
    bad_ts = int(baseline["_BucketEpochSec"].isna().sum())
    if bad_ts:
        raise ValueError(f"baseline 30s input has {bad_ts} unparseable Timestamp values")
    baseline["_BucketEpochSec"] = baseline["_BucketEpochSec"].astype("int64")
    symbol_set = set(symbols)
    if symbol_set:
        baseline = baseline[baseline["Symbol"].isin(symbol_set)].copy()
    if expected_dates:
        baseline = baseline[baseline["_Date"].isin(set(expected_dates))].copy()
    if baseline.empty:
        raise ValueError("baseline 30s input has no rows after symbol/date filtering")
    return baseline.sort_values("_BaselineOrder").reset_index(drop=True)


def _summary_index(
    silver_root: Path,
    *,
    artifact: str,
    summary_name: str,
    key_column: str,
    symbols: list[str],
    expected_dates: list[str],
    errors: list[str],
) -> dict[tuple[str, str], Path]:
    path = silver_root / artifact / summary_name
    if not path.exists():
        errors.append(f"missing {artifact} summary: {path}")
        return {}
    wanted_symbols = set(symbols)
    wanted_dates = set(expected_dates)
    index: dict[tuple[str, str], Path] = {}
    for row in _read_csv_rows(path):
        date = str(row.get("date", "")).strip().replace("-", "")
        key = str(row.get(key_column, "")).strip().upper()
        if wanted_dates and date not in wanted_dates:
            continue
        if wanted_symbols and key not in wanted_symbols:
            continue
        output_path = Path(str(row.get("output_path", "")).strip()).expanduser()
        if not output_path.exists():
            errors.append(f"{artifact} summary lists missing output for {key} {date}: {output_path}")
            continue
        index[(key, date)] = output_path
    return index


def _check_quality_manifest(
    silver_root: Path,
    quality_manifest: Path | None,
    *,
    allow_missing: bool,
    allow_warnings: bool,
    errors: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    manifest_path = quality_manifest or (silver_root / "quality_check" / "silver_quality_manifest.json")
    if not manifest_path.exists():
        message = f"missing silver quality manifest: {manifest_path}"
        if allow_missing:
            warnings.append(message)
        else:
            errors.append(message)
        return {}
    payload = _read_json(manifest_path)
    manifest_errors = list(payload.get("errors") or [])
    manifest_warnings = list(payload.get("warnings") or [])
    if manifest_errors:
        errors.append(f"silver quality manifest has errors: {manifest_errors}")
    if manifest_warnings and not allow_warnings:
        errors.append(f"silver quality manifest has warnings and --allow-quality-warnings was not set: {manifest_warnings}")
    elif manifest_warnings:
        warnings.append(f"silver quality manifest warnings allowed: {manifest_warnings}")
    if payload.get("strict_quality") is False:
        warnings.append("silver quality manifest was generated with strict_quality=false")
    return payload


def _remove_existing_outputs(output_dir: Path, force: bool) -> None:
    targets = [
        output_dir / "manifest.json",
        output_dir / "combined" / "combined_30s.csv",
        output_dir / "reports" / ENRICHED_SUMMARY,
        output_dir / "reports" / "databento_enriched_30s_summary.csv",
    ]
    if (output_dir / "data_30s").is_dir():
        targets.extend(sorted((output_dir / "data_30s").glob("*_30s_training.csv")))
    existing = [path for path in targets if path.exists()]
    if existing and not force:
        preview = "; ".join(str(path) for path in existing[:8])
        raise FileExistsError(f"enriched output already exists; pass --force to overwrite. Existing: {preview}")
    for path in existing:
        path.unlink()


def _feature_frame(path: Path | None, aggregate_fn) -> pd.DataFrame:
    if path is None:
        return aggregate_fn(pd.DataFrame())
    return aggregate_fn(pd.read_csv(path))


def _finalize_feature_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ALL_FEATURE_COLUMNS:
        if column not in out.columns:
            out[column] = 0.0 if column in ZERO_FILL_FEATURE_COLUMNS else np.nan
    for column in ZERO_FILL_FEATURE_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)
    return out


def _duplicate_count(frame: pd.DataFrame, columns: list[str]) -> int:
    if not all(column in frame.columns for column in columns):
        return 0
    return int(frame.duplicated(columns, keep=False).sum())


def _format_symbol_date_pairs(pairs: list[tuple[str, str]], limit: int = 12) -> str:
    preview = ", ".join(f"{symbol}/{date}" for symbol, date in pairs[:limit])
    if len(pairs) > limit:
        preview += f", ... (+{len(pairs) - limit} more)"
    return preview


def _baseline_integrity_errors(
    baseline: pd.DataFrame,
    *,
    symbols: list[str],
    expected_dates: list[str],
) -> tuple[list[str], dict[str, int]]:
    errors: list[str] = []
    duplicate_symbol_timestamp_rows = _duplicate_count(baseline, ["Symbol", "Timestamp"])
    duplicate_symbol_bucket_rows = _duplicate_count(baseline, ["Symbol", "_BucketEpochSec"])
    if duplicate_symbol_timestamp_rows:
        errors.append(f"baseline has {duplicate_symbol_timestamp_rows} duplicate Symbol/Timestamp rows")
    if duplicate_symbol_bucket_rows:
        errors.append(f"baseline has {duplicate_symbol_bucket_rows} duplicate Symbol/30s-bucket rows")

    collisions = [column for column in ALL_FEATURE_COLUMNS if column in baseline.columns]
    if collisions:
        preview = ", ".join(collisions[:12])
        suffix = f", ... (+{len(collisions) - 12} more)" if len(collisions) > 12 else ""
        errors.append(f"baseline already contains enriched Databento feature columns: {preview}{suffix}")

    present_symbols = set(baseline["Symbol"].dropna().astype(str).str.upper())
    missing_symbols = [symbol for symbol in symbols if symbol not in present_symbols]
    if missing_symbols:
        errors.append(f"baseline missing requested symbols: {', '.join(missing_symbols)}")

    if expected_dates:
        present_pairs = set(zip(baseline["Symbol"].astype(str), baseline["_Date"].astype(str)))
        missing_pairs = [
            (symbol, date)
            for symbol in symbols
            for date in expected_dates
            if (symbol, date) not in present_pairs
        ]
        if missing_pairs:
            errors.append(f"baseline missing expected symbol/date rows: {_format_symbol_date_pairs(missing_pairs)}")

    return errors, {
        "baseline_duplicate_symbol_timestamp_rows": duplicate_symbol_timestamp_rows,
        "baseline_duplicate_symbol_bucket_rows": duplicate_symbol_bucket_rows,
    }


def _date_row_summary(symbol: str, date: str, frame: pd.DataFrame, expected_rows: int, output_path: Path) -> EnrichedSummaryRow:
    eq_cov = pd.to_numeric(frame.get("EqMbp1JoinCoverage30s", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    opra_cov = pd.to_numeric(frame.get("OpraTcbboJoinCoverage30s", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    opra_volume = pd.to_numeric(frame.get("OpraTcbboTotalContractVolume30s", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    eq_lag = pd.to_numeric(frame.get("EqMbp1SourceMaxLagSec30s", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    opra_lag = pd.to_numeric(frame.get("OpraTcbboSourceMaxLagSec30s", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    return EnrichedSummaryRow(
        symbol=symbol,
        date=date,
        baseline_rows=int(len(frame)),
        output_rows=int(len(frame)),
        expected_rows=int(expected_rows),
        row_count_ok=bool(expected_rows <= 0 or len(frame) == expected_rows),
        equs_feature_rows=int(eq_cov.gt(0.0).sum()),
        opra_feature_rows=int(opra_cov.gt(0.0).sum()),
        missing_equs_rows=int(eq_cov.le(0.0).sum()),
        missing_opra_rows=int(opra_cov.le(0.0).sum()),
        mean_equs_join_coverage=round(float(eq_cov.mean()), 6) if len(eq_cov) else 0.0,
        mean_opra_join_coverage=round(float(opra_cov.mean()), 6) if len(opra_cov) else 0.0,
        max_equs_source_lag_sec=round(float(eq_lag.max()), 6) if len(eq_lag) else 0.0,
        max_opra_source_lag_sec=round(float(opra_lag.max()), 6) if len(opra_lag) else 0.0,
        total_opra_contract_volume_30s=round(float(opra_volume.sum()), 6),
        output_path=str(output_path),
    )


def write_summary_csv(path: Path, rows: list[EnrichedSummaryRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(EnrichedSummaryRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _manifest_payload(
    *,
    silver_root: Path,
    baseline_30s: Path,
    output_dir: Path,
    symbols: list[str],
    expected_dates: list[str],
    baseline_rows: int,
    output_rows: int,
    summary_rows: list[EnrichedSummaryRow],
    quality_manifest: dict[str, Any],
    errors: list[str],
    warnings: list[str],
    baseline_duplicate_symbol_timestamp_rows: int = 0,
    baseline_duplicate_symbol_bucket_rows: int = 0,
    output_duplicate_symbol_timestamp_rows: int = 0,
    output_duplicate_symbol_bucket_rows: int = 0,
) -> dict[str, Any]:
    dates = sorted({row.date for row in summary_rows} or set(expected_dates))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "silver_root": str(silver_root),
        "baseline_30s": str(baseline_30s),
        "output_dir": str(output_dir),
        "symbols": symbols,
        "expected_dates": expected_dates,
        "dates": dates,
        "baseline_rows": int(baseline_rows),
        "output_rows": int(output_rows),
        "row_count_preserved": int(baseline_rows) == int(output_rows),
        "baseline_duplicate_symbol_timestamp_rows": int(baseline_duplicate_symbol_timestamp_rows),
        "baseline_duplicate_symbol_bucket_rows": int(baseline_duplicate_symbol_bucket_rows),
        "output_duplicate_symbol_timestamp_rows": int(output_duplicate_symbol_timestamp_rows),
        "output_duplicate_symbol_bucket_rows": int(output_duplicate_symbol_bucket_rows),
        "summary_row_count": len(summary_rows),
        "feature_column_count": len(ALL_FEATURE_COLUMNS),
        "feature_columns": list(ALL_FEATURE_COLUMNS),
        "quality_manifest_errors": list(quality_manifest.get("errors") or []) if quality_manifest else [],
        "quality_manifest_warnings": list(quality_manifest.get("warnings") or []) if quality_manifest else [],
        "outputs": {
            "combined_30s_csv": "combined/combined_30s.csv",
            "data_30s_dir": "data_30s",
            "summary_csv": f"reports/{ENRICHED_SUMMARY}",
        },
        "errors": errors,
        "warnings": warnings,
    }


def build_enriched_30s(
    *,
    silver_root: Path,
    baseline_30s: Path,
    output_dir: Path,
    symbols: list[str],
    expected_dates: list[str],
    quality_manifest: Path | None = None,
    allow_missing_quality_manifest: bool = False,
    allow_quality_warnings: bool = False,
    fail_on_missing_silver: bool = True,
    expected_rows_per_symbol_day: int = 0,
    force: bool = False,
) -> int:
    silver_root = silver_root.expanduser().resolve()
    baseline_30s = baseline_30s.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    symbols = [symbol.upper() for symbol in symbols]
    expected_dates = _normalize_dates(expected_dates)
    errors: list[str] = []
    warnings: list[str] = []

    output_dir.mkdir(parents=True, exist_ok=True)
    _remove_existing_outputs(output_dir, force=force)

    quality_payload = _check_quality_manifest(
        silver_root,
        quality_manifest.expanduser().resolve() if quality_manifest else None,
        allow_missing=allow_missing_quality_manifest,
        allow_warnings=allow_quality_warnings,
        errors=errors,
        warnings=warnings,
    )

    if not silver_root.is_dir():
        errors.append(f"silver root does not exist: {silver_root}")
    if not baseline_30s.exists():
        errors.append(f"baseline 30s input does not exist: {baseline_30s}")

    eq_index = _summary_index(
        silver_root,
        artifact="equs_mbp1_1s",
        summary_name=EQUS_SUMMARY,
        key_column="symbol",
        symbols=symbols,
        expected_dates=expected_dates,
        errors=errors,
    )
    opra_index = _summary_index(
        silver_root,
        artifact="opra_tcbbo_1s",
        summary_name=OPRA_SUMMARY,
        key_column="underlying",
        symbols=symbols,
        expected_dates=expected_dates,
        errors=errors,
    )

    if errors:
        manifest = _manifest_payload(
            silver_root=silver_root,
            baseline_30s=baseline_30s,
            output_dir=output_dir,
            symbols=symbols,
            expected_dates=expected_dates,
            baseline_rows=0,
            output_rows=0,
            summary_rows=[],
            quality_manifest=quality_payload,
            errors=errors,
            warnings=warnings,
        )
        write_json(output_dir / "manifest.json", manifest)
        print(f"wrote {output_dir / 'manifest.json'}")
        print("DATABENTO_ENRICHED_30S_BUILD=FAIL")
        return 2

    baseline = load_baseline_30s(baseline_30s, symbols, expected_dates)
    baseline_integrity_errors, duplicate_stats = _baseline_integrity_errors(
        baseline,
        symbols=symbols,
        expected_dates=expected_dates,
    )
    errors.extend(baseline_integrity_errors)
    if errors:
        summary_path = output_dir / "reports" / ENRICHED_SUMMARY
        write_summary_csv(summary_path, [])
        manifest = _manifest_payload(
            silver_root=silver_root,
            baseline_30s=baseline_30s,
            output_dir=output_dir,
            symbols=symbols,
            expected_dates=expected_dates,
            baseline_rows=int(len(baseline)),
            output_rows=0,
            summary_rows=[],
            quality_manifest=quality_payload,
            errors=errors,
            warnings=warnings,
            **duplicate_stats,
        )
        write_json(output_dir / "manifest.json", manifest)
        print(f"wrote {summary_path}")
        print(f"wrote {output_dir / 'manifest.json'}")
        print("DATABENTO_ENRICHED_30S_BUILD=FAIL")
        return 2

    dates_to_process = expected_dates or sorted(baseline["_Date"].dropna().astype(str).unique())
    output_rows: list[pd.DataFrame] = []
    combined_chunks: list[pd.DataFrame] = []
    summary_rows: list[EnrichedSummaryRow] = []

    for symbol in symbols:
        symbol_frame = baseline[baseline["Symbol"].eq(symbol)].copy()
        if symbol_frame.empty:
            warnings.append(f"baseline has no rows for requested symbol {symbol}")
            continue
        symbol_chunks: list[pd.DataFrame] = []
        symbol_output_path = output_dir / "data_30s" / f"{symbol}_30s_training.csv"
        for date in dates_to_process:
            base_day = symbol_frame[symbol_frame["_Date"].eq(date)].copy()
            if base_day.empty:
                warnings.append(f"baseline has no rows for {symbol} {date}")
                continue

            eq_path = eq_index.get((symbol, date))
            opra_path = opra_index.get((symbol, date))
            if eq_path is None:
                message = f"missing EQUS silver features for {symbol} {date}"
                errors.append(message) if fail_on_missing_silver else warnings.append(message)
            if opra_path is None:
                message = f"missing OPRA silver features for {symbol} {date}"
                errors.append(message) if fail_on_missing_silver else warnings.append(message)

            eq_features = _feature_frame(eq_path, aggregate_equs_1s_to_30s)
            opra_features = _feature_frame(opra_path, aggregate_opra_1s_to_30s)
            enriched = base_day.merge(eq_features, on="_BucketEpochSec", how="left", validate="one_to_one")
            enriched = enriched.merge(opra_features, on="_BucketEpochSec", how="left", validate="one_to_one")
            enriched = _finalize_feature_defaults(enriched)

            missing_eq = int(pd.to_numeric(enriched["EqMbp1JoinCoverage30s"], errors="coerce").fillna(0.0).le(0.0).sum())
            missing_opra = int(pd.to_numeric(enriched["OpraTcbboJoinCoverage30s"], errors="coerce").fillna(0.0).le(0.0).sum())
            if missing_eq:
                message = f"{symbol} {date} has {missing_eq} baseline rows without EQUS silver features"
                errors.append(message) if fail_on_missing_silver else warnings.append(message)
            if missing_opra:
                message = f"{symbol} {date} has {missing_opra} baseline rows without OPRA silver features"
                errors.append(message) if fail_on_missing_silver else warnings.append(message)

            max_eq_lag = float(pd.to_numeric(enriched["EqMbp1SourceMaxLagSec30s"], errors="coerce").fillna(0.0).max())
            max_opra_lag = float(pd.to_numeric(enriched["OpraTcbboSourceMaxLagSec30s"], errors="coerce").fillna(0.0).max())
            if max_eq_lag > SECONDS_PER_30S_BAR - 1.0:
                errors.append(f"{symbol} {date} EQUS silver source lag exceeds a causal 30s bucket: max_lag_sec={max_eq_lag:.6f}")
            if max_opra_lag > SECONDS_PER_30S_BAR - 1.0:
                errors.append(f"{symbol} {date} OPRA silver source lag exceeds a causal 30s bucket: max_lag_sec={max_opra_lag:.6f}")

            expected_rows = int(expected_rows_per_symbol_day or 0)
            if expected_rows > 0 and len(enriched) != expected_rows:
                errors.append(f"{symbol} {date} enriched rows={len(enriched)} expected={expected_rows}")

            cleaned = enriched.drop(columns=["_Date"], errors="ignore")
            symbol_chunks.append(cleaned)
            summary_rows.append(_date_row_summary(symbol, date, enriched, expected_rows, symbol_output_path))

        if symbol_chunks:
            symbol_out_internal = pd.concat(symbol_chunks, ignore_index=True).sort_values("_BaselineOrder")
            symbol_out = symbol_out_internal.drop(columns=["_BaselineOrder", "_BucketEpochSec"], errors="ignore")
            symbol_output_path.parent.mkdir(parents=True, exist_ok=True)
            symbol_out.to_csv(symbol_output_path, index=False)
            combined_chunks.append(symbol_out_internal)
            output_rows.append(symbol_out)

    combined_path = output_dir / "combined" / "combined_30s.csv"
    output_duplicate_stats = {
        "output_duplicate_symbol_timestamp_rows": 0,
        "output_duplicate_symbol_bucket_rows": 0,
    }
    if combined_chunks:
        combined_internal = pd.concat(combined_chunks, ignore_index=True).sort_values("_BaselineOrder")
        output_duplicate_stats["output_duplicate_symbol_timestamp_rows"] = _duplicate_count(combined_internal, ["Symbol", "Timestamp"])
        output_duplicate_stats["output_duplicate_symbol_bucket_rows"] = _duplicate_count(combined_internal, ["Symbol", "_BucketEpochSec"])
        combined = combined_internal.drop(columns=["_BaselineOrder", "_BucketEpochSec"], errors="ignore")
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(combined_path, index=False)

    total_output_rows = int(sum(len(frame) for frame in output_rows))
    if total_output_rows != len(baseline):
        errors.append(f"row count not preserved: baseline_rows={len(baseline)} output_rows={total_output_rows}")
    if output_duplicate_stats["output_duplicate_symbol_timestamp_rows"]:
        errors.append(f"output has {output_duplicate_stats['output_duplicate_symbol_timestamp_rows']} duplicate Symbol/Timestamp rows")
    if output_duplicate_stats["output_duplicate_symbol_bucket_rows"]:
        errors.append(f"output has {output_duplicate_stats['output_duplicate_symbol_bucket_rows']} duplicate Symbol/30s-bucket rows")

    summary_path = output_dir / "reports" / ENRICHED_SUMMARY
    write_summary_csv(summary_path, summary_rows)
    manifest = _manifest_payload(
        silver_root=silver_root,
        baseline_30s=baseline_30s,
        output_dir=output_dir,
        symbols=symbols,
        expected_dates=expected_dates,
        baseline_rows=int(len(baseline)),
        output_rows=total_output_rows,
        summary_rows=summary_rows,
        quality_manifest=quality_payload,
        errors=errors,
        warnings=warnings,
        **duplicate_stats,
        **output_duplicate_stats,
    )
    write_json(output_dir / "manifest.json", manifest)
    print(f"wrote {summary_path}")
    print(f"wrote {combined_path}")
    print(f"wrote {output_dir / 'manifest.json'}")
    if errors:
        print("DATABENTO_ENRICHED_30S_BUILD=FAIL")
        return 2
    print("DATABENTO_ENRICHED_30S_BUILD=PASS")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", required=True, type=Path, help="Phase 1 silver root that passed verify_databento_silver_outputs.py")
    parser.add_argument("--baseline-30s", required=True, type=Path, help="Baseline combined 30s CSV, data_30s dir, or build root containing data_30s")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output build root for enriched 30s CSVs")
    parser.add_argument("--symbols", default="TSLA,TQQQ,NVDA,SPY,QQQ", help="Comma-separated symbols to build")
    parser.add_argument("--expected-date", action="append", type=normalize_date_arg, default=[], help="Expected market date. Repeatable; accepts YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--quality-manifest", type=Path, help="Optional explicit silver_quality_manifest.json path")
    parser.add_argument("--allow-missing-quality-manifest", action="store_true", help="Allow building without silver_quality_manifest.json; intended only for local smoke tests")
    parser.add_argument("--allow-quality-warnings", action="store_true", help="Allow non-empty warnings in silver_quality_manifest.json")
    parser.add_argument("--allow-missing-silver", action="store_true", help="Warn instead of failing when baseline rows have no matching silver features")
    parser.add_argument("--expected-rows-per-symbol-day", type=int, default=0, help="Optional exact row-count check per symbol/date, e.g. 780 for full RTH 30s bars")
    parser.add_argument("--force", action="store_true", help="Overwrite existing enriched output files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = split_symbols(args.symbols)
    if not symbols:
        raise SystemExit("--symbols must contain at least one symbol")
    return build_enriched_30s(
        silver_root=args.silver_root,
        baseline_30s=args.baseline_30s,
        output_dir=args.output_dir,
        symbols=symbols,
        expected_dates=list(args.expected_date or []),
        quality_manifest=args.quality_manifest,
        allow_missing_quality_manifest=bool(args.allow_missing_quality_manifest),
        allow_quality_warnings=bool(args.allow_quality_warnings),
        fail_on_missing_silver=not bool(args.allow_missing_silver),
        expected_rows_per_symbol_day=max(0, int(args.expected_rows_per_symbol_day or 0)),
        force=bool(args.force),
    )


if __name__ == "__main__":
    raise SystemExit(main())
