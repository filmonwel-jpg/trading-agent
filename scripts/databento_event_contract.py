#!/usr/bin/env python3
"""Shared Databento NDJSON event-contract helpers.

The live normalizer and historical streamer both emit the same Java-consumed
NDJSON shape.  This module adds the Step-10 sanity fields to every emitted bar
without tying tests to Databento client packages.
"""
from __future__ import annotations

import math
import time
from typing import Any

EVENT_SCHEMA_VERSION = "databento_ndjson_v2"
MISSING_QUOTE_AGE_MS = 999_999.0


def _safe_float(value: Any, fallback: float = 0.0) -> float:
	try:
		parsed = float(value)
	except (TypeError, ValueError):
		return fallback
	return parsed if math.isfinite(parsed) else fallback


def _safe_int(value: Any, fallback: int = 0) -> int:
	try:
		if value is None:
			return fallback
		return int(round(float(value)))
	except (TypeError, ValueError):
		return fallback


def _finite_feature_map(features: dict[str, Any] | None) -> dict[str, float]:
	cleaned: dict[str, float] = {}
	if not features:
		return cleaned
	for raw_key, raw_value in features.items():
		key = str(raw_key or "").strip()
		if not key:
			continue
		try:
			value = float(raw_value)
		except (TypeError, ValueError):
			continue
		if math.isfinite(value):
			cleaned[key] = value
	return cleaned


def attach_feature_snapshot(
	payload: dict[str, Any],
	*,
	epoch_sec: int,
	features: dict[str, Any] | None,
	source: str = "",
	schema_version: str = "",
) -> dict[str, Any]:
	"""Attach an event-carried downstream feature snapshot in-place.

	Java accepts the numeric map from ``enriched_features`` and caches it by
	``FeatureSnapshotEpochSec`` before processing the carrier source bar.  This
	helper intentionally skips empty/non-numeric maps so callers do not create a
	misleading ``featureSnapshot=hit`` with no usable values.
	"""
	cleaned = _finite_feature_map(features)
	if not cleaned or int(epoch_sec or 0) <= 0:
		return payload
	payload["FeatureSnapshotEpochSec"] = int(epoch_sec)
	if schema_version:
		payload["FeatureSnapshotSchemaVersion"] = str(schema_version)
	if source:
		payload["FeatureSnapshotSource"] = str(source)
	payload["enriched_features"] = cleaned
	return payload


def _quality_score(
	trade_coverage: float,
	quote_state_coverage: float,
	valid_spread_coverage: float,
	synthetic_coverage: float,
	locked_crossed_seconds: float,
) -> float:
	score = (
		0.35 * max(0.0, min(1.0, trade_coverage))
		+ 0.35 * max(0.0, min(1.0, quote_state_coverage))
		+ 0.20 * max(0.0, min(1.0, valid_spread_coverage))
		+ 0.10 * (1.0 - max(0.0, min(1.0, synthetic_coverage)))
	)
	if locked_crossed_seconds > 0.0:
		score *= 0.5
	return round(max(0.0, min(1.0, score)), 6)


def _parent_quality_flags(
	trade_coverage: float,
	quote_state_coverage: float,
	synthetic_coverage: float,
	quote_age_ms_max: float,
	locked_crossed_seconds: float,
) -> str:
	tokens: list[str] = []
	if trade_coverage <= 0.0:
		tokens.append("no_trade")
	if quote_state_coverage < 1.0:
		tokens.append("no_quote")
	if synthetic_coverage >= 1.0:
		tokens.append("synthetic_ohlc")
	elif synthetic_coverage > 0.0:
		tokens.append("partial_synthetic_ohlc")
	if quote_state_coverage >= 1.0 and quote_age_ms_max > 5_000.0:
		tokens.append("stale_quote")
	if locked_crossed_seconds > 0.0:
		tokens.append("locked_crossed")
	return "none" if not tokens else "|".join(tokens)


def attach_contract_metadata(
	payload: dict[str, Any],
	*,
	event_source: str,
	dataset: str = "",
	schema: str = "",
	stype_in: str = "",
	ts_event_ns: int | None = None,
	generated_at_ms: int | None = None,
) -> dict[str, Any]:
	"""Attach schema/version and timestamp-provenance metadata in-place."""
	if generated_at_ms is None:
		generated_at_ms = int(time.time() * 1000)
	payload["EventSchemaVersion"] = EVENT_SCHEMA_VERSION
	payload["EventSource"] = str(event_source or "").strip() or "unknown"
	payload["EventDataset"] = str(dataset or "").strip()
	payload["EventSchema"] = str(schema or "").strip()
	payload["EventStypeIn"] = str(stype_in or "").strip()
	if ts_event_ns is not None:
		payload["tsEventNs"] = int(ts_event_ns)
		payload["EventTsEventNs"] = int(ts_event_ns)
	payload["EventGeneratedAtMs"] = int(generated_at_ms)
	return payload


def decorate_equity_bar(
	payload: dict[str, Any],
	*,
	event_source: str,
	dataset: str = "",
	schema: str = "",
	stype_in: str = "raw_symbol",
	ts_event_ns: int | None = None,
	generated_at_ms: int | None = None,
	expected_seconds: int = 1,
) -> dict[str, Any]:
	"""Attach offline-compatible quality fields to an equity bar payload."""
	expected = max(1, int(expected_seconds or 1))
	volume = max(0, _safe_int(payload.get("volume")))
	trade_count = max(0, _safe_int(payload.get("tradeCount")))
	quote_count = max(0, _safe_int(payload.get("quoteCount")))
	close = _safe_float(payload.get("close"))
	bid = _safe_float(payload.get("bid"))
	ask = _safe_float(payload.get("ask"))

	trade_present = 1.0 if trade_count > 0 or volume > 0 else 0.0
	quote_update_present = 1.0 if quote_count > 0 else 0.0
	quote_state_valid = 1.0 if bid > 0.0 and ask > 0.0 else 0.0
	locked_crossed = 1.0 if quote_state_valid and ask <= bid else 0.0
	valid_spread = 1.0 if quote_state_valid and ask > bid else 0.0
	synthetic = 1.0 if trade_present <= 0.0 and close > 0.0 else 0.0
	quote_age_ms = 0.0 if quote_state_valid else MISSING_QUOTE_AGE_MS

	trade_coverage = trade_present / expected
	quote_update_coverage = quote_update_present / expected
	quote_state_coverage = quote_state_valid / expected
	synthetic_coverage = synthetic / expected
	valid_spread_coverage = valid_spread / expected
	locked_crossed_seconds = locked_crossed
	flags = _parent_quality_flags(
		trade_coverage,
		quote_state_coverage,
		synthetic_coverage,
		quote_age_ms,
		locked_crossed_seconds,
	)

	payload.update({
		"DataQualityFlags": flags,
		"ChildDataQualityFlagUnion": flags,
		"TradeSecondsPresent": trade_present,
		"QuoteUpdateSecondsPresent": quote_update_present,
		"QuoteStateSecondsValid": quote_state_valid,
		"SyntheticSeconds": synthetic,
		"TradeCoverage": round(trade_coverage, 6),
		"QuoteUpdateCoverage": round(quote_update_coverage, 6),
		"QuoteStateCoverage": round(quote_state_coverage, 6),
		"SyntheticCoverage": round(synthetic_coverage, 6),
		"QuoteAgeMsMean": quote_age_ms,
		"QuoteAgeMsMax": quote_age_ms,
		"ValidSpreadCoverage": round(valid_spread_coverage, 6),
		"LockedCrossedSeconds": locked_crossed_seconds,
		"QualityScore": _quality_score(
			trade_coverage,
			quote_state_coverage,
			valid_spread_coverage,
			synthetic_coverage,
			locked_crossed_seconds,
		),
	})
	return attach_contract_metadata(
		payload,
		event_source=event_source,
		dataset=dataset,
		schema=schema,
		stype_in=stype_in,
		ts_event_ns=ts_event_ns,
		generated_at_ms=generated_at_ms,
	)


def decorate_option_bar(
	payload: dict[str, Any],
	*,
	event_source: str,
	dataset: str = "",
	schema: str = "ohlcv-1s",
	stype_in: str = "parent",
	ts_event_ns: int | None = None,
	generated_at_ms: int | None = None,
) -> dict[str, Any]:
	"""Attach the same contract keys to option OHLCV bars.

	OPRA `ohlcv-1s` bars are trade-volume bars, not quote-state bars.  They
	therefore get option-specific high quality when positive volume is present.
	"""
	volume = max(0, _safe_int(payload.get("volume")))
	flags = "none" if volume > 0 else "no_trade"
	quality = 1.0 if volume > 0 else 0.0
	payload.update({
		"DataQualityFlags": flags,
		"ChildDataQualityFlagUnion": flags,
		"TradeSecondsPresent": 1.0 if volume > 0 else 0.0,
		"QuoteUpdateSecondsPresent": 0.0,
		"QuoteStateSecondsValid": 0.0,
		"SyntheticSeconds": 0.0,
		"TradeCoverage": 1.0 if volume > 0 else 0.0,
		"QuoteUpdateCoverage": 0.0,
		"QuoteStateCoverage": 0.0,
		"SyntheticCoverage": 0.0,
		"QuoteAgeMsMean": MISSING_QUOTE_AGE_MS,
		"QuoteAgeMsMax": MISSING_QUOTE_AGE_MS,
		"ValidSpreadCoverage": 0.0,
		"LockedCrossedSeconds": 0.0,
		"QualityScore": quality,
	})
	return attach_contract_metadata(
		payload,
		event_source=event_source,
		dataset=dataset,
		schema=schema,
		stype_in=stype_in,
		ts_event_ns=ts_event_ns,
		generated_at_ms=generated_at_ms,
	)
