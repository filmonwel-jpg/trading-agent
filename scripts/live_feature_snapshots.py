#!/usr/bin/env python3
"""Live Databento feature-snapshot enrichment helpers.

This module is intentionally dependency-free. It decorates normalized live equity
bar payloads with a bounded set of 30-second, downstream setup-filter features so
Java can exercise the same event-carried feature-snapshot path in paper/live that
historical recovered replays use.

The snapshots are not historical research rows. They are live-computed carrier
snapshots from the normalizer and are identified with a distinct
FeatureSnapshotSource/FeatureSnapshotSchemaVersion.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from databento_event_contract import attach_feature_snapshot

DEFAULT_LIVE_FEATURE_SNAPSHOT_SOURCE = "live_normalizer_30s_v1"
DEFAULT_LIVE_FEATURE_SNAPSHOT_SCHEMA_VERSION = "live_normalizer_30s_v1"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        return default
    return parsed


def _bucket_start(epoch_sec: int, bucket_seconds: int) -> int:
    return int(epoch_sec) - (int(epoch_sec) % max(1, int(bucket_seconds)))


def _microprice(bid: float, ask: float, bid_size: float, ask_size: float) -> float:
    total = bid_size + ask_size
    if bid > 0.0 and ask > 0.0 and total > 0.0:
        return ((bid * ask_size) + (ask * bid_size)) / total
    if bid > 0.0 and ask > 0.0:
        return (bid + ask) / 2.0
    return ask if ask > 0.0 else bid


def _std(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(max(0.0, variance))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _finite_features(features: dict[str, float]) -> dict[str, float]:
    return {
        key: float(value)
        for key, value in features.items()
        if key and math.isfinite(float(value))
    }


@dataclass
class EquityFeatureBucket:
    symbol: str
    bucket_start_epoch: int
    bucket_seconds: int = 30
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0
    trade_count: int = 0
    quote_count: int = 0
    at_bid_vol: int = 0
    at_ask_vol: int = 0
    wap_px_volume: float = 0.0
    bid_values: list[float] = field(default_factory=list)
    ask_values: list[float] = field(default_factory=list)
    bid_size_values: list[float] = field(default_factory=list)
    ask_size_values: list[float] = field(default_factory=list)
    mid_values: list[float] = field(default_factory=list)
    microprice_values: list[float] = field(default_factory=list)
    spread_values: list[float] = field(default_factory=list)
    spread_bps_values: list[float] = field(default_factory=list)
    l1_imbalance_values: list[float] = field(default_factory=list)
    quote_age_ms_values: list[float] = field(default_factory=list)
    quality_values: list[float] = field(default_factory=list)
    observed_seconds: set[int] = field(default_factory=set)
    trade_seconds_present: float = 0.0
    quote_update_seconds_present: float = 0.0
    quote_state_seconds_valid: float = 0.0
    synthetic_seconds: float = 0.0
    valid_spread_seconds: float = 0.0
    locked_crossed_seconds: float = 0.0

    def observe(self, payload: dict[str, Any]) -> None:
        epoch = _safe_int(payload.get("barEpochSec"), self.bucket_start_epoch)
        self.observed_seconds.add(epoch)
        open_px = _safe_float(payload.get("open"))
        high_px = _safe_float(payload.get("high"))
        low_px = _safe_float(payload.get("low"))
        close_px = _safe_float(payload.get("close"))
        volume = max(0, _safe_int(payload.get("volume")))
        wap = _safe_float(payload.get("wap"), close_px)

        if self.open <= 0.0:
            self.open = open_px if open_px > 0.0 else close_px
        if high_px > 0.0:
            self.high = max(self.high, high_px)
        if low_px > 0.0:
            self.low = low_px if self.low <= 0.0 else min(self.low, low_px)
        if close_px > 0.0:
            self.close = close_px
        self.volume += volume
        if volume > 0 and wap > 0.0:
            self.wap_px_volume += wap * volume
        trade_count = max(0, _safe_int(payload.get("tradeCount")))
        quote_count = max(0, _safe_int(payload.get("quoteCount")))
        self.trade_count += trade_count
        self.quote_count += quote_count
        self.at_bid_vol += max(0, _safe_int(payload.get("atBidVol")))
        self.at_ask_vol += max(0, _safe_int(payload.get("atAskVol")))

        bid = _safe_float(payload.get("bid"))
        ask = _safe_float(payload.get("ask"))
        bid_size = max(0.0, _safe_float(payload.get("bidSize")))
        ask_size = max(0.0, _safe_float(payload.get("askSize")))
        quote_valid = bid > 0.0 and ask > bid
        if bid > 0.0:
            self.bid_values.append(bid)
        if ask > 0.0:
            self.ask_values.append(ask)
        self.bid_size_values.append(bid_size)
        self.ask_size_values.append(ask_size)
        if bid > 0.0 and ask > 0.0:
            mid = (bid + ask) / 2.0
            self.mid_values.append(mid)
            self.microprice_values.append(_microprice(bid, ask, bid_size, ask_size))
            spread = max(0.0, ask - bid)
            self.spread_values.append(spread)
            if mid > 0.0:
                self.spread_bps_values.append((spread / mid) * 10_000.0)
            denom = bid_size + ask_size + 1.0
            self.l1_imbalance_values.append((bid_size - ask_size) / denom)

        self.trade_seconds_present += _safe_float(payload.get("TradeSecondsPresent"), 1.0 if trade_count > 0 or volume > 0 else 0.0)
        self.quote_update_seconds_present += _safe_float(payload.get("QuoteUpdateSecondsPresent"), 1.0 if quote_count > 0 else 0.0)
        self.quote_state_seconds_valid += _safe_float(payload.get("QuoteStateSecondsValid"), 1.0 if quote_valid else 0.0)
        self.synthetic_seconds += _safe_float(payload.get("SyntheticSeconds"), 0.0 if volume > 0 else 1.0)
        self.valid_spread_seconds += _safe_float(payload.get("ValidSpreadCoverage"), 1.0 if quote_valid else 0.0)
        self.locked_crossed_seconds += _safe_float(payload.get("LockedCrossedSeconds"), 1.0 if bid > 0.0 and ask > 0.0 and ask <= bid else 0.0)
        self.quote_age_ms_values.append(_safe_float(payload.get("QuoteAgeMsMean"), 0.0 if quote_valid else 999_999.0))
        self.quality_values.append(_safe_float(payload.get("QualityScore"), 1.0 if quote_valid and close_px > 0.0 else 0.0))

    def snapshot_epoch(self) -> int:
        return self.bucket_start_epoch + self.bucket_seconds

    def features(self) -> dict[str, float]:
        close = self.close if self.close > 0.0 else self.open
        high = self.high if self.high > 0.0 else close
        low = self.low if self.low > 0.0 else close
        wap = (self.wap_px_volume / self.volume) if self.volume > 0 else close
        bid_mean = _mean(self.bid_values)
        ask_mean = _mean(self.ask_values)
        bid_last = self.bid_values[-1] if self.bid_values else 0.0
        ask_last = self.ask_values[-1] if self.ask_values else 0.0
        bid_size_mean = _mean(self.bid_size_values)
        ask_size_mean = _mean(self.ask_size_values)
        bid_size_last = self.bid_size_values[-1] if self.bid_size_values else 0.0
        ask_size_last = self.ask_size_values[-1] if self.ask_size_values else 0.0
        mid_mean = _mean(self.mid_values)
        mid_last = self.mid_values[-1] if self.mid_values else ((bid_last + ask_last) / 2.0 if bid_last > 0.0 and ask_last > 0.0 else close)
        microprice_mean = _mean(self.microprice_values)
        microprice_last = self.microprice_values[-1] if self.microprice_values else mid_last
        spread_mean = _mean(self.spread_values)
        spread_bps_mean = _mean(self.spread_bps_values)
        spread_bps_min = min(self.spread_bps_values) if self.spread_bps_values else 0.0
        spread_bps_max = max(self.spread_bps_values) if self.spread_bps_values else 0.0
        l1_mean = _mean(self.l1_imbalance_values)
        l1_last = self.l1_imbalance_values[-1] if self.l1_imbalance_values else 0.0
        quality_mean = _mean(self.quality_values)
        quote_age_mean = _mean(self.quote_age_ms_values)
        quote_age_max = max(self.quote_age_ms_values) if self.quote_age_ms_values else 999_999.0
        observed = max(1, len(self.observed_seconds))
        bucket = max(1, self.bucket_seconds)

        features = {
            "FeatureSnapshotEpochSec": float(self.snapshot_epoch()),
            "LiveFeatureSnapshotAvailable": 1.0,
            "LiveObservedSeconds30s": float(observed),
            "LiveObservedCoverage30s": min(1.0, observed / bucket),
            "Open30s": self.open,
            "High30s": high,
            "Low30s": low,
            "Close30s": close,
            "Wap30s": wap,
            "Volume30s": float(self.volume),
            "TradeCount30s": float(self.trade_count),
            "QuoteCount30s": float(self.quote_count),
            "AtBidVol30s": float(self.at_bid_vol),
            "AtAskVol30s": float(self.at_ask_vol),
            "RangeBps30s": ((high - low) / close) * 10_000.0 if close > 0.0 else 0.0,
            "FeatureCompleteness": quality_mean,
            "QualityScore": quality_mean,
            "TradeCoverage": min(1.0, self.trade_seconds_present / bucket),
            "QuoteUpdateCoverage": min(1.0, self.quote_update_seconds_present / bucket),
            "QuoteStateCoverage": min(1.0, self.quote_state_seconds_valid / bucket),
            "SyntheticCoverage": min(1.0, self.synthetic_seconds / bucket),
            "ValidSpreadCoverage": min(1.0, self.valid_spread_seconds / bucket),
            "LockedCrossedSeconds": self.locked_crossed_seconds,
            "TradeSecondsPresent": self.trade_seconds_present,
            "QuoteUpdateSecondsPresent": self.quote_update_seconds_present,
            "QuoteStateSecondsValid": self.quote_state_seconds_valid,
            "SyntheticSeconds": self.synthetic_seconds,
            "QuoteAgeMs": quote_age_mean,
            "QuoteAgeMsMean": quote_age_mean,
            "QuoteAgeMsMax": quote_age_max,
            "QuoteCoverage5s": min(1.0, self.quote_update_seconds_present / bucket),
            "QuoteUpdateCount5s": float(self.quote_count),
            "TradePrintCount5s": float(self.trade_count),
            "SpreadBps": spread_bps_mean,
            "SpreadMinBps5s": spread_bps_min,
            "SpreadMaxBps5s": spread_bps_max,
            "L1Imbalance": l1_mean,
            "ImbalanceStd5s": _std(self.l1_imbalance_values),
            "EqMbp1BidMean30s": bid_mean,
            "EqMbp1AskMean30s": ask_mean,
            "EqMbp1BidLast30s": bid_last,
            "EqMbp1AskLast30s": ask_last,
            "EqMbp1BidSizeMean30s": bid_size_mean,
            "EqMbp1AskSizeMean30s": ask_size_mean,
            "EqMbp1BidSizeLast30s": bid_size_last,
            "EqMbp1AskSizeLast30s": ask_size_last,
            "EqMbp1MidMean30s": mid_mean,
            "EqMbp1MidLast30s": mid_last,
            "EqMbp1MicropriceMean30s": microprice_mean,
            "EqMbp1MicropriceLast30s": microprice_last,
            "EqMbp1SpreadMean30s": spread_mean,
            "EqMbp1SpreadBpsMean30s": spread_bps_mean,
            "EqMbp1SpreadBpsLast30s": self.spread_bps_values[-1] if self.spread_bps_values else 0.0,
            "EqMbp1RawSpreadMinBps30s": spread_bps_min,
            "EqMbp1RawSpreadMaxBps30s": spread_bps_max,
            "EqMbp1L1ImbalanceMean30s": l1_mean,
            "EqMbp1L1ImbalanceLast30s": l1_last,
            "EqMbp1QuoteUpdateCount30s": float(self.quote_count),
            "EqMbp1EventCount30s": float(self.quote_count + self.trade_count),
            "EqMbp1QuoteUpdateCoverage30s": min(1.0, self.quote_update_seconds_present / bucket),
            "EqMbp1QuoteStateValidCoverage30s": min(1.0, self.quote_state_seconds_valid / bucket),
            "EqMbp1ValidSpreadCoverage30s": min(1.0, self.valid_spread_seconds / bucket),
            "EqMbp1LockedCrossedSeconds30s": self.locked_crossed_seconds,
            "EqMbp1LockedCrossedCoverage30s": min(1.0, self.locked_crossed_seconds / bucket),
            "EqMbp1QuoteAgeMsMean30s": quote_age_mean,
            "EqMbp1QuoteAgeMsMax30s": quote_age_max,
        }
        return _finite_features(features)


class LiveEquityFeatureSnapshotEmitter:
    def __init__(
        self,
        *,
        enabled: bool = False,
        bucket_seconds: int = 30,
        source: str = DEFAULT_LIVE_FEATURE_SNAPSHOT_SOURCE,
        schema_version: str = DEFAULT_LIVE_FEATURE_SNAPSHOT_SCHEMA_VERSION,
    ) -> None:
        self.enabled = bool(enabled)
        self.bucket_seconds = max(1, int(bucket_seconds or 30))
        self.source = str(source or DEFAULT_LIVE_FEATURE_SNAPSHOT_SOURCE).strip() or DEFAULT_LIVE_FEATURE_SNAPSHOT_SOURCE
        self.schema_version = str(schema_version or DEFAULT_LIVE_FEATURE_SNAPSHOT_SCHEMA_VERSION).strip() or DEFAULT_LIVE_FEATURE_SNAPSHOT_SCHEMA_VERSION
        self._buckets: dict[str, EquityFeatureBucket] = {}

    def decorate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return payload
        if str(payload.get("event") or "").lower() != "equity_bar":
            return payload
        symbol = str(payload.get("symbol") or "").strip().upper()
        epoch = _safe_int(payload.get("barEpochSec"))
        if not symbol or epoch <= 0:
            return payload
        bucket_start = _bucket_start(epoch, self.bucket_seconds)
        current = self._buckets.get(symbol)
        snapshot: tuple[int, dict[str, float]] | None = None
        if current is not None and current.bucket_start_epoch != bucket_start:
            snapshot = (current.snapshot_epoch(), current.features())
            current = None
            self._buckets.pop(symbol, None)
        if current is None:
            current = EquityFeatureBucket(symbol=symbol, bucket_start_epoch=bucket_start, bucket_seconds=self.bucket_seconds)
            self._buckets[symbol] = current
        current.observe(payload)
        if epoch >= bucket_start + self.bucket_seconds - 1:
            snapshot = (current.snapshot_epoch(), current.features())
            self._buckets.pop(symbol, None)
        if snapshot is None:
            return payload
        snapshot_epoch, features = snapshot
        return attach_feature_snapshot(
            payload,
            epoch_sec=snapshot_epoch,
            features=features,
            source=self.source,
            schema_version=self.schema_version,
        )
