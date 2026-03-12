import asyncio

# Python 3.14 compatibility: ensure a current event loop exists before ib_insync import
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import *
import csv
import os
import math
import re
import hashlib
import numpy as np
from collections import deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ib = IB()
IB_HOST = os.getenv('IB_HOST', '127.0.0.1')
IB_PORT = int(os.getenv('IB_PORT', '7497'))
IB_CLIENT_ID = int(os.getenv('IB_CLIENT_ID', '10'))
RECONNECT_RETRY_SECONDS = float(os.getenv('RECONNECT_RETRY_SECONDS', '3'))
STREAM_WATCHDOG_INTERVAL_SECONDS = float(os.getenv('STREAM_WATCHDOG_INTERVAL_SECONDS', '10'))
BAR_STALE_SECONDS = float(os.getenv('BAR_STALE_SECONDS', '90'))
TICK_STALE_SECONDS = float(os.getenv('TICK_STALE_SECONDS', '90'))

# Connect to Gateway/TWS. Using ClientID 10 to avoid conflicts with Java bots.
ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)

def _parse_symbol_list(raw_value, fallback):
    text = '' if raw_value is None else str(raw_value).strip()
    if not text:
        return list(fallback)

    parsed = []
    for token in text.split(','):
        sym = token.strip().upper()
        if sym and sym not in parsed:
            parsed.append(sym)
    return parsed if parsed else list(fallback)


# Add all symbols you want to harvest (env override: HARVEST_SYMBOLS=TSLA,QQQ,NVDA,AMD).
DEFAULT_SYMBOLS = ['TSLA', 'QQQ', 'NVDA', 'AMD']
symbols = _parse_symbol_list(os.getenv('HARVEST_SYMBOLS', ''), DEFAULT_SYMBOLS)
# Market-context symbols default to all harvested symbols (optional override: MARKET_CONTEXT_SYMBOLS).
market_context_symbols = _parse_symbol_list(os.getenv('MARKET_CONTEXT_SYMBOLS', ''), symbols)
MAX_TICK_BY_TICK_STREAMS = int(os.getenv('MAX_TICK_BY_TICK_STREAMS', '4'))
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test'))
MARKET_ZONE = ZoneInfo('America/New_York')

# Dictionaries to keep track of active subscriptions and data state
contracts = {}
tickers = {}
bars_dict = {}
ticks_dict = {}
bar_state_by_symbol = {}
quote_window_state_by_symbol = {}
market_context_state_by_symbol = {}
symbol_runtime = {}
stream_resubscribe_lock = set()
WARMUP_CORE_HEADER = [
    'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'Count', 'YesterdayClose',
    'Bid', 'Ask', 'BidSize', 'AskSize', 'PutVol', 'CallVol', 'ShortableShares',
    'NewsCount300s', 'SentimentMean300s', 'SentimentMin300s', 'SentimentMax300s', 'MinutesSinceNews'
]
WARMUP_ENRICHMENT_HEADER = [
    'AsOfTs', 'BarEpochSec', 'SessionBucket', 'MinuteOfDay', 'SecondsFromOpen',
    'SpreadBps', 'L1Imbalance', 'NewsCount60s', 'NewsUniqueProviders300s',
    'SentimentStd300s', 'SentimentLatest', 'SentimentModel',
    'SentimentConfidenceMean300s', 'SentimentConfidenceLatest',
    'NewsAsOfLagSec', 'NewsCoverage300s', 'FeatureCompleteness', 'DataQualityFlags'
]
WARMUP_WINDOW_HEADER = [
    'BidLast', 'AskLast', 'BidSizeLast', 'AskSizeLast',
    'PutVolDelta5s', 'CallVolDelta5s', 'ShortableDelta5s',
    'ShortableMin5s', 'ShortableMax5s',
    'QuoteUpdateCount5s', 'QuoteCoverage5s', 'QuoteAgeMs',
    'SpreadMinBps5s', 'SpreadMaxBps5s', 'ImbalanceStd5s',
    'AtBidVol', 'AtAskVol', 'TradePrintCount5s'
]
MARKET_CONTEXT_FIELD_SUFFIXES = [
    'Close5s', 'Ret5s', 'Ret30s', 'SpreadBps',
    'L1Imbalance', 'NewsCount300s', 'SentimentMean300s', 'AsOfLagSec'
]
MARKET_CONTEXT_HEADER = [
    f'Mkt_{ctx_symbol}_{suffix}'
    for ctx_symbol in market_context_symbols
    for suffix in MARKET_CONTEXT_FIELD_SUFFIXES
]
MARKET_CONTEXT_SUMMARY_HEADER = [
    'MktReadyCount', 'MktBreadthUp5s', 'MktMeanRet5s', 'MktDispersion5s'
]
WARMUP_HEADER = WARMUP_CORE_HEADER + WARMUP_ENRICHMENT_HEADER + WARMUP_WINDOW_HEADER + MARKET_CONTEXT_HEADER + MARKET_CONTEXT_SUMMARY_HEADER
TICK_HEADER = [
    'time', 'price', 'size',
    'bid', 'ask', 'bid_size', 'ask_size',
    'last', 'last_size', 'mid', 'spread',
    'put_vol', 'call_vol', 'shortable_shares',
    'volume', 'vwap', 'bbo_exchange', 'last_exchange'
]
NEWS_HEADER = [
    'time', 'provider', 'provider_name', 'article_id', 'headline',
    'sentiment_score', 'sentiment_confidence', 'sentiment_label', 'sentiment_model',
    'published_ts', 'received_ts', 'tradable_ts', 'is_historical_seed',
    'source_raw', 'source_site',
    'dup_cluster_id', 'dup_seq_asof', 'dup_provider_count_asof', 'dup_first_seen_ts', 'dup_is_repeat'
]
NEWS_LOOKBACK_SECONDS = int(os.getenv('NEWS_LOOKBACK_SECONDS', '300'))
SENTIMENT_MODEL = os.getenv('SENTIMENT_MODEL', 'finbert').strip().lower()
FINBERT_MODEL_NAME = os.getenv('FINBERT_MODEL_NAME', 'ProsusAI/finbert').strip()
FINBERT_MAX_LENGTH = int(os.getenv('FINBERT_MAX_LENGTH', '128'))
HF_TOKEN = os.getenv('HF_TOKEN', os.getenv('HUGGINGFACE_HUB_TOKEN', '')).strip()
SENTIMENT_SELF_TEST_ENABLED = os.getenv('SENTIMENT_SELF_TEST', '1').strip().lower() not in ('0', 'false', 'no', 'off')
SENTIMENT_SELF_TEST_STRICT = os.getenv('SENTIMENT_SELF_TEST_STRICT', '0').strip().lower() in ('1', 'true', 'yes', 'on')

POSITIVE_WORDS = {
    'beat', 'beats', 'strong', 'up', 'upgrade', 'surge', 'growth', 'profit', 'bullish', 'outperform'
}
NEGATIVE_WORDS = {
    'miss', 'misses', 'weak', 'down', 'downgrade', 'drop', 'loss', 'bearish', 'underperform', 'lawsuit'
}

news_state_by_symbol = {}
seen_news_ids_by_symbol = {}
news_provider_name_by_code = {}
news_cluster_state_by_symbol = {}
finbert_pipeline = None
finbert_label_map = {}


def safe_num(val, default=0.0):
    """Safely handle IBKR's nan values before writing to CSV"""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return val


def safe_str(val, default=''):
    if val is None:
        return default
    text = str(val).strip()
    return text if text else default


def _now_utc():
    return datetime.now(timezone.utc)


def mark_stream_alive(sym, stream_name):
    runtime = symbol_runtime.get(sym)
    if not runtime:
        return
    runtime[f'last_{stream_name}_event_ts'] = _now_utc()


def to_market_dt(raw_dt):
    if raw_dt is None:
        return datetime.now(MARKET_ZONE)
    if raw_dt.tzinfo is None:
        return raw_dt.replace(tzinfo=timezone.utc).astimezone(MARKET_ZONE)
    return raw_dt.astimezone(MARKET_ZONE)


def format_market_timestamp(raw_dt):
    market_dt = to_market_dt(raw_dt)
    return market_dt.strftime('%Y%m%d %H:%M:%S') + ' America/New_York'


def format_market_timestamp_from_epoch(epoch_value):
    try:
        epoch_int = int(float(epoch_value))
    except (TypeError, ValueError):
        epoch_int = 0
    raw_dt = datetime.fromtimestamp(epoch_int, tz=timezone.utc)
    return format_market_timestamp(raw_dt)


def parse_news_time(raw_time):
    if isinstance(raw_time, datetime):
        return to_market_dt(raw_time)

    if isinstance(raw_time, (int, float)):
        return to_market_dt(datetime.fromtimestamp(int(raw_time), tz=timezone.utc))

    text = safe_str(raw_time, '')
    if not text:
        return datetime.now(MARKET_ZONE)

    if text.isdigit():
        return to_market_dt(datetime.fromtimestamp(int(text), tz=timezone.utc))

    for fmt in ('%Y%m%d  %H:%M:%S', '%Y%m%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc).astimezone(MARKET_ZONE)
        except ValueError:
            continue

    return datetime.now(MARKET_ZONE)


def score_headline_sentiment_lexicon(headline):
    tokens = re.findall(r"[a-zA-Z']+", safe_str(headline, '').lower())
    if not tokens:
        return 0.0

    pos_hits = sum(1 for token in tokens if token in POSITIVE_WORDS)
    neg_hits = sum(1 for token in tokens if token in NEGATIVE_WORDS)
    raw_score = float(pos_hits - neg_hits)
    return max(-1.0, min(1.0, raw_score / 3.0))


def init_sentiment_model():
    global finbert_pipeline, finbert_label_map

    if SENTIMENT_MODEL != 'finbert':
        print(f"[SENTIMENT] Using lexicon scorer (SENTIMENT_MODEL={SENTIMENT_MODEL}).")
        return

    try:
        from transformers import pipeline

        pipeline_kwargs = {
            'task': 'text-classification',
            'model': FINBERT_MODEL_NAME,
            'tokenizer': FINBERT_MODEL_NAME,
            'truncation': True,
        }

        if HF_TOKEN:
            try:
                finbert_pipeline = pipeline(token=HF_TOKEN, **pipeline_kwargs)
            except TypeError:
                finbert_pipeline = pipeline(use_auth_token=HF_TOKEN, **pipeline_kwargs)
        else:
            finbert_pipeline = pipeline(**pipeline_kwargs)

        id2label = getattr(getattr(finbert_pipeline, 'model', None), 'config', None)
        raw_map = getattr(id2label, 'id2label', {}) if id2label is not None else {}
        finbert_label_map = {str(k): str(v).lower() for k, v in raw_map.items()}
        auth_status = 'configured' if HF_TOKEN else 'not_configured'
        print(f"[SENTIMENT] FinBERT enabled: model={FINBERT_MODEL_NAME} hf_auth={auth_status}")
    except Exception as exc:
        finbert_pipeline = None
        finbert_label_map = {}
        print(f"[SENTIMENT] FinBERT unavailable ({exc}); using lexicon fallback.")


def run_finbert_self_test():
    global finbert_pipeline, finbert_label_map

    if SENTIMENT_MODEL != 'finbert':
        return True

    if not SENTIMENT_SELF_TEST_ENABLED:
        print('[SENTIMENT] FinBERT self-test skipped (SENTIMENT_SELF_TEST disabled).')
        return True

    if finbert_pipeline is None:
        print('[SENTIMENT] FinBERT self-test skipped (pipeline unavailable; lexicon fallback active).')
        return False

    test_cases = [
        ('positive', 'Tesla shares surge after record deliveries and strong profit guidance'),
        ('negative', 'Tesla shares plunge after weak deliveries and a profit warning'),
        ('neutral', 'Tesla scheduled its annual shareholder meeting for next month'),
    ]

    try:
        outputs = []
        for expected_label, headline in test_cases:
            score, confidence, label = score_headline_sentiment(headline)
            if label not in ('positive', 'negative', 'neutral'):
                raise ValueError(f'unexpected label={label!r}')
            if not (-1.0 <= float(score) <= 1.0):
                raise ValueError(f'score out of range={score!r}')
            if not (0.0 <= float(confidence) <= 1.0):
                raise ValueError(f'confidence out of range={confidence!r}')
            outputs.append((expected_label, label, float(score), float(confidence)))

        directional_failures = [
            (expected, actual, score, confidence)
            for expected, actual, score, confidence in outputs[:2]
            if actual != expected
        ]
        if directional_failures:
            raise ValueError(f'directional mismatch={directional_failures}')

        summary = ', '.join(
            f"{expected}->{actual} score={score:.4f} conf={confidence:.4f}"
            for expected, actual, score, confidence in outputs
        )
        print(f"[SENTIMENT] FinBERT self-test passed: {summary}")
        return True
    except Exception as exc:
        print(f"[SENTIMENT] FinBERT self-test failed ({exc}); using lexicon fallback.")
        finbert_pipeline = None
        finbert_label_map = {}
        if SENTIMENT_SELF_TEST_STRICT:
            raise
        return False


def get_active_sentiment_engine():
    return 'finbert' if finbert_pipeline is not None else 'lexicon'


def get_hf_auth_status():
    return 'configured' if HF_TOKEN else 'not_configured'


def _normalize_finbert_label(label):
    normalized = safe_str(label, '').lower()
    if normalized in ('positive', 'negative', 'neutral'):
        return normalized

    mapped = finbert_label_map.get(normalized, '')
    if mapped in ('positive', 'negative', 'neutral'):
        return mapped

    if normalized.startswith('label_'):
        # Common FinBERT mappings: 0=negative, 1=neutral, 2=positive.
        if normalized == 'label_0':
            return 'negative'
        if normalized == 'label_1':
            return 'neutral'
        if normalized == 'label_2':
            return 'positive'

    return 'neutral'


def _normalize_sentiment_label_from_score(score):
    if score > 0:
        return 'positive'
    if score < 0:
        return 'negative'
    return 'neutral'


def score_headline_sentiment(headline):
    clean_headline = safe_str(headline, '')
    if not clean_headline:
        return 0.0, 0.0, 'neutral'

    if finbert_pipeline is None:
        score = score_headline_sentiment_lexicon(clean_headline)
        confidence = min(1.0, abs(score))
        return score, confidence, _normalize_sentiment_label_from_score(score)

    try:
        result = finbert_pipeline(clean_headline, truncation=True, max_length=FINBERT_MAX_LENGTH)
        if not result:
            score = score_headline_sentiment_lexicon(clean_headline)
            confidence = min(1.0, abs(score))
            return score, confidence, _normalize_sentiment_label_from_score(score)

        top = result[0]
        label = _normalize_finbert_label(getattr(top, 'get', lambda *_: '')('label', ''))
        confidence = float(getattr(top, 'get', lambda *_: 0.0)('score', 0.0))
        confidence = max(0.0, min(1.0, confidence))

        if label == 'positive':
            return confidence, confidence, label
        if label == 'negative':
            return -confidence, confidence, label
        return 0.0, confidence, label
    except Exception:
        score = score_headline_sentiment_lexicon(clean_headline)
        confidence = min(1.0, abs(score))
        return score, confidence, _normalize_sentiment_label_from_score(score)


def _session_bucket(market_dt):
    hour = market_dt.hour
    minute = market_dt.minute
    if hour == 9 and minute < 30:
        return 'PRE'
    if (hour > 9 or (hour == 9 and minute >= 30)) and hour < 16:
        return 'REGULAR'
    return 'POST'


def _news_coverage_score(news_count_300s):
    return max(0.0, min(1.0, float(news_count_300s) / 5.0))


def _new_quote_window_state():
    return {
        'window_start_dt': None,
        'last_quote_dt': None,
        'last_valid_quote_dt': None,
        'last_bid': 0.0,
        'last_ask': 0.0,
        'last_bid_size': 0.0,
        'last_ask_size': 0.0,
        'last_put_vol': None,
        'last_call_vol': None,
        'last_shortable': None,
        'bid_time_sum': 0.0,
        'ask_time_sum': 0.0,
        'bid_size_time_sum': 0.0,
        'ask_size_time_sum': 0.0,
        'spread_bps_time_sum': 0.0,
        'imbalance_time_sum': 0.0,
        'imbalance_sq_time_sum': 0.0,
        'quote_duration_sec': 0.0,
        'quote_updates': 0,
        'spread_min_bps': None,
        'spread_max_bps': None,
        'window_put_first': None,
        'window_call_first': None,
        'window_shortable_first': None,
        'window_shortable_min': None,
        'window_shortable_max': None,
        'at_bid_vol': 0.0,
        'at_ask_vol': 0.0,
        'trade_print_count': 0,
    }


def get_quote_window_state(sym):
    return quote_window_state_by_symbol.setdefault(sym, _new_quote_window_state())


def _has_valid_quote_levels(state):
    return state['last_bid'] > 0 and state['last_ask'] > 0 and state['last_ask'] >= state['last_bid']


def _reset_quote_window_stats(state, next_start_dt, carry_forward=True):
    state['window_start_dt'] = next_start_dt
    state['last_quote_dt'] = next_start_dt if carry_forward and _has_valid_quote_levels(state) else None
    state['bid_time_sum'] = 0.0
    state['ask_time_sum'] = 0.0
    state['bid_size_time_sum'] = 0.0
    state['ask_size_time_sum'] = 0.0
    state['spread_bps_time_sum'] = 0.0
    state['imbalance_time_sum'] = 0.0
    state['imbalance_sq_time_sum'] = 0.0
    state['quote_duration_sec'] = 0.0
    state['quote_updates'] = 0
    state['spread_min_bps'] = None
    state['spread_max_bps'] = None
    state['window_put_first'] = state['last_put_vol'] if carry_forward and state['last_put_vol'] is not None else None
    state['window_call_first'] = state['last_call_vol'] if carry_forward and state['last_call_vol'] is not None else None
    state['window_shortable_first'] = state['last_shortable'] if carry_forward and state['last_shortable'] is not None else None
    carried_shortable = state['last_shortable'] if carry_forward and state['last_shortable'] is not None else None
    state['window_shortable_min'] = carried_shortable
    state['window_shortable_max'] = carried_shortable
    state['at_bid_vol'] = 0.0
    state['at_ask_vol'] = 0.0
    state['trade_print_count'] = 0


def _accumulate_quote_duration(state, until_dt):
    if until_dt is None or state['last_quote_dt'] is None:
        return

    delta_sec = (until_dt - state['last_quote_dt']).total_seconds()
    if delta_sec <= 0:
        state['last_quote_dt'] = until_dt
        return

    if _has_valid_quote_levels(state):
        bid = state['last_bid']
        ask = state['last_ask']
        bid_size = max(0.0, state['last_bid_size'])
        ask_size = max(0.0, state['last_ask_size'])
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid * 10000.0) if mid > 0 else 0.0
        imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1.0)

        state['bid_time_sum'] += bid * delta_sec
        state['ask_time_sum'] += ask * delta_sec
        state['bid_size_time_sum'] += bid_size * delta_sec
        state['ask_size_time_sum'] += ask_size * delta_sec
        state['spread_bps_time_sum'] += spread_bps * delta_sec
        state['imbalance_time_sum'] += imbalance * delta_sec
        state['imbalance_sq_time_sum'] += (imbalance ** 2) * delta_sec
        state['quote_duration_sec'] += delta_sec
        state['spread_min_bps'] = spread_bps if state['spread_min_bps'] is None else min(state['spread_min_bps'], spread_bps)
        state['spread_max_bps'] = spread_bps if state['spread_max_bps'] is None else max(state['spread_max_bps'], spread_bps)

    state['last_quote_dt'] = until_dt


def capture_quote_window_from_ticker(sym, ticker_obj, event_dt=None):
    state = get_quote_window_state(sym)
    event_market_dt = to_market_dt(event_dt if event_dt is not None else getattr(ticker_obj, 'time', None) or _now_utc())

    if state['window_start_dt'] is None:
        _reset_quote_window_stats(state, event_market_dt, carry_forward=False)

    _accumulate_quote_duration(state, event_market_dt)

    bid = safe_num(getattr(ticker_obj, 'bid', None), None)
    ask = safe_num(getattr(ticker_obj, 'ask', None), None)
    bid_size = safe_num(getattr(ticker_obj, 'bidSize', None), None)
    ask_size = safe_num(getattr(ticker_obj, 'askSize', None), None)

    if bid is not None and bid > 0:
        state['last_bid'] = float(bid)
    if ask is not None and ask > 0:
        state['last_ask'] = float(ask)
    if bid_size is not None and bid_size >= 0:
        state['last_bid_size'] = float(bid_size)
    if ask_size is not None and ask_size >= 0:
        state['last_ask_size'] = float(ask_size)

    if _has_valid_quote_levels(state):
        state['quote_updates'] += 1
        state['last_valid_quote_dt'] = event_market_dt
        if state['last_quote_dt'] is None:
            state['last_quote_dt'] = event_market_dt

    put_vol = safe_num(getattr(ticker_obj, 'putVolume', None), None)
    call_vol = safe_num(getattr(ticker_obj, 'callVolume', None), None)
    shortable = safe_num(getattr(ticker_obj, 'shortableShares', None), None)

    if put_vol is not None:
        state['last_put_vol'] = float(put_vol)
        if state['window_put_first'] is None:
            state['window_put_first'] = float(put_vol)

    if call_vol is not None:
        state['last_call_vol'] = float(call_vol)
        if state['window_call_first'] is None:
            state['window_call_first'] = float(call_vol)

    if shortable is not None:
        shortable = float(shortable)
        state['last_shortable'] = shortable
        if state['window_shortable_first'] is None:
            state['window_shortable_first'] = shortable
        state['window_shortable_min'] = shortable if state['window_shortable_min'] is None else min(state['window_shortable_min'], shortable)
        state['window_shortable_max'] = shortable if state['window_shortable_max'] is None else max(state['window_shortable_max'], shortable)


def record_trade_window_volume(sym, trade_dt, trade_price, trade_size, bid=None, ask=None):
    if trade_price <= 0 or trade_size <= 0:
        return

    state = get_quote_window_state(sym)
    market_dt = to_market_dt(trade_dt if trade_dt is not None else _now_utc())
    if state['window_start_dt'] is None:
        _reset_quote_window_stats(state, market_dt, carry_forward=False)

    state['trade_print_count'] += 1

    eff_bid = bid if bid is not None and bid > 0 else state['last_bid']
    eff_ask = ask if ask is not None and ask > 0 else state['last_ask']
    if eff_bid <= 0 or eff_ask <= 0 or eff_ask < eff_bid:
        return

    spread = eff_ask - eff_bid
    mid = (eff_bid + eff_ask) / 2.0
    epsilon = max(0.0001, min(0.02, spread * 0.35 if spread > 0 else 0.0001))

    if trade_price >= (eff_ask - epsilon) or trade_price > mid:
        state['at_ask_vol'] += float(trade_size)
    elif trade_price <= (eff_bid + epsilon) or trade_price < mid:
        state['at_bid_vol'] += float(trade_size)


def _level_delta(last_level, first_level, clamp_negative=False):
    if last_level is None and first_level is None:
        return 0.0, 0.0, False, False

    level = float(last_level if last_level is not None else first_level)
    if last_level is None or first_level is None:
        return level, 0.0, True, False

    delta = float(last_level - first_level)
    reset = clamp_negative and delta < 0
    if reset:
        delta = 0.0
    return level, delta, True, reset


def finalize_quote_window(sym, bar_dt):
    state = get_quote_window_state(sym)
    bar_market_dt = to_market_dt(bar_dt)

    if state['window_start_dt'] is None:
        _reset_quote_window_stats(state, bar_market_dt, carry_forward=False)

    window_start_dt = state['window_start_dt']
    _accumulate_quote_duration(state, bar_market_dt)

    duration_sec = state['quote_duration_sec']
    window_span_sec = (bar_market_dt - window_start_dt).total_seconds()
    if window_span_sec <= 0:
        window_span_sec = 5.0

    if duration_sec > 0:
        bid = state['bid_time_sum'] / duration_sec
        ask = state['ask_time_sum'] / duration_sec
        bid_size = state['bid_size_time_sum'] / duration_sec
        ask_size = state['ask_size_time_sum'] / duration_sec
        spread_bps = state['spread_bps_time_sum'] / duration_sec
        imbalance = state['imbalance_time_sum'] / duration_sec
        imbalance_var = max(0.0, (state['imbalance_sq_time_sum'] / duration_sec) - (imbalance ** 2))
        imbalance_std = math.sqrt(imbalance_var)
    else:
        bid = state['last_bid']
        ask = state['last_ask']
        bid_size = state['last_bid_size']
        ask_size = state['last_ask_size']
        if _has_valid_quote_levels(state):
            mid = (state['last_bid'] + state['last_ask']) / 2.0
            spread_bps = ((state['last_ask'] - state['last_bid']) / mid * 10000.0) if mid > 0 else 0.0
            imbalance = (state['last_bid_size'] - state['last_ask_size']) / (state['last_bid_size'] + state['last_ask_size'] + 1.0)
        else:
            spread_bps = 0.0
            imbalance = 0.0
        imbalance_std = 0.0

    put_vol, put_delta, has_put, put_reset = _level_delta(state['last_put_vol'], state['window_put_first'], clamp_negative=True)
    call_vol, call_delta, has_call, call_reset = _level_delta(state['last_call_vol'], state['window_call_first'], clamp_negative=True)
    shortable, shortable_delta, has_shortable, _ = _level_delta(state['last_shortable'], state['window_shortable_first'], clamp_negative=False)

    last_valid_quote_dt = state['last_valid_quote_dt']
    quote_age_ms = 999999.0 if last_valid_quote_dt is None else max(0.0, (bar_market_dt - last_valid_quote_dt).total_seconds() * 1000.0)

    quote_metrics = {
        'bid_twap': float(bid),
        'ask_twap': float(ask),
        'bid_size_twap': float(bid_size),
        'ask_size_twap': float(ask_size),
        'bid_last': float(state['last_bid']),
        'ask_last': float(state['last_ask']),
        'bid_size_last': float(state['last_bid_size']),
        'ask_size_last': float(state['last_ask_size']),
        'spread_bps_twap': float(spread_bps),
        'spread_min_bps_5s': float(state['spread_min_bps'] if state['spread_min_bps'] is not None else spread_bps),
        'spread_max_bps_5s': float(state['spread_max_bps'] if state['spread_max_bps'] is not None else spread_bps),
        'l1_imbalance_twap': float(imbalance),
        'l1_imbalance_std_5s': float(imbalance_std),
        'put_vol_last': int(round(put_vol)),
        'call_vol_last': int(round(call_vol)),
        'put_vol_delta_5s': int(round(put_delta)),
        'call_vol_delta_5s': int(round(call_delta)),
        'has_option_levels': has_put or has_call,
        'put_call_reset': put_reset or call_reset,
        'shortable_last': float(shortable),
        'shortable_delta_5s': float(shortable_delta),
        'shortable_min_5s': float(state['window_shortable_min'] if state['window_shortable_min'] is not None else shortable),
        'shortable_max_5s': float(state['window_shortable_max'] if state['window_shortable_max'] is not None else shortable),
        'has_shortable': has_shortable,
        'quote_update_count_5s': int(state['quote_updates']),
        'quote_coverage_5s': max(0.0, min(1.0, duration_sec / window_span_sec)),
        'quote_age_ms': float(quote_age_ms),
        'at_bid_vol': float(state['at_bid_vol']),
        'at_ask_vol': float(state['at_ask_vol']),
        'trade_print_count_5s': int(state['trade_print_count']),
    }

    _reset_quote_window_stats(state, bar_market_dt, carry_forward=True)
    return quote_metrics


def get_news_state(sym):
    return news_state_by_symbol.setdefault(sym, {
        'events': deque(),
        'last_news_dt': None,
    })


def record_news_event(sym, news_csv_path, news_dt, provider_code, article_id, headline, received_dt=None, is_historical_seed=False, provider_name='', source_raw=''):
    sentiment_score, sentiment_confidence, sentiment_label = score_headline_sentiment(headline)
    state = get_news_state(sym)
    published_dt = to_market_dt(news_dt)
    received_market_dt = to_market_dt(received_dt if received_dt is not None else _now_utc())
    tradable_dt = max(published_dt, received_market_dt)
    duplicate_meta = compute_news_duplicate_metadata(sym, headline, provider_code, published_dt)
    source_raw = safe_str(source_raw, '')
    source_site = _extract_source_site(source_raw)

    state['events'].append({
        'published_dt': published_dt,
        'received_dt': received_market_dt,
        'tradable_dt': tradable_dt,
        'provider': safe_str(provider_code, ''),
        'article_id': safe_str(article_id, ''),
        'sentiment_score': sentiment_score,
        'sentiment_confidence': sentiment_confidence,
        'sentiment_label': sentiment_label,
        'sentiment_model': SENTIMENT_MODEL,
    })
    state['last_news_dt'] = published_dt

    cutoff = published_dt - timedelta(seconds=NEWS_LOOKBACK_SECONDS)
    while state['events'] and state['events'][0]['published_dt'] < cutoff:
        state['events'].popleft()

    with open(news_csv_path, 'a', newline='') as f:
        csv.writer(f).writerow([
            format_market_timestamp(published_dt),
            safe_str(provider_code, ''),
            safe_str(provider_name or news_provider_name_by_code.get(provider_code, ''), ''),
            safe_str(article_id, ''),
            safe_str(headline, ''),
            f"{sentiment_score:.4f}",
            f"{sentiment_confidence:.4f}",
            sentiment_label,
            SENTIMENT_MODEL,
            format_market_timestamp(published_dt),
            format_market_timestamp(received_market_dt),
            format_market_timestamp(tradable_dt),
            '1' if is_historical_seed else '0',
            source_raw,
            source_site,
            duplicate_meta['dup_cluster_id'],
            duplicate_meta['dup_seq_asof'],
            duplicate_meta['dup_provider_count_asof'],
            duplicate_meta['dup_first_seen_ts'],
            duplicate_meta['dup_is_repeat'],
        ])


def get_news_features(sym, bar_dt):
    state = get_news_state(sym)
    cutoff_300 = bar_dt - timedelta(seconds=NEWS_LOOKBACK_SECONDS)
    cutoff_60 = bar_dt - timedelta(seconds=60)

    while state['events'] and state['events'][0]['published_dt'] < cutoff_300:
        state['events'].popleft()

    eligible_events = [
        event for event in state['events']
        if event['published_dt'] <= bar_dt and event['received_dt'] <= bar_dt and event['tradable_dt'] <= bar_dt
    ]

    if not eligible_events:
        minutes_since_news = 9999.0
        news_lag_sec = 999999.0
        if state['last_news_dt'] is not None:
            delta = bar_dt - state['last_news_dt']
            minutes_since_news = max(0.0, delta.total_seconds() / 60.0)
        return {
            'news_count_300s': 0,
            'news_count_60s': 0,
            'sentiment_mean_300s': 0.0,
            'sentiment_min_300s': 0.0,
            'sentiment_max_300s': 0.0,
            'sentiment_std_300s': 0.0,
            'sentiment_latest': 0.0,
            'sentiment_conf_mean_300s': 0.0,
            'sentiment_conf_latest': 0.0,
            'news_unique_providers_300s': 0,
            'minutes_since_news': minutes_since_news,
            'news_asof_lag_sec': news_lag_sec,
            'news_coverage_300s': 0.0,
        }

    scores = [event['sentiment_score'] for event in eligible_events]
    confidences = [event['sentiment_confidence'] for event in eligible_events]
    providers = {event['provider'] for event in eligible_events if event['provider']}
    latest_event = eligible_events[-1]

    std_score = float(np.std(scores, ddof=1)) if len(scores) > 1 else 0.0
    mean_score = sum(scores) / len(scores)
    mean_conf = sum(confidences) / len(confidences)

    delta = bar_dt - latest_event['published_dt']
    minutes_since_news = max(0.0, delta.total_seconds() / 60.0)
    news_asof_lag_sec = max(0.0, delta.total_seconds())

    news_count_60s = sum(1 for event in eligible_events if event['published_dt'] >= cutoff_60)

    return {
        'news_count_300s': len(eligible_events),
        'news_count_60s': int(news_count_60s),
        'sentiment_mean_300s': float(mean_score),
        'sentiment_min_300s': float(min(scores)),
        'sentiment_max_300s': float(max(scores)),
        'sentiment_std_300s': std_score,
        'sentiment_latest': float(latest_event['sentiment_score']),
        'sentiment_conf_mean_300s': float(mean_conf),
        'sentiment_conf_latest': float(latest_event['sentiment_confidence']),
        'news_unique_providers_300s': int(len(providers)),
        'minutes_since_news': minutes_since_news,
        'news_asof_lag_sec': news_asof_lag_sec,
        'news_coverage_300s': _news_coverage_score(len(eligible_events)),
    }


def create_news_callback(sym, news_csv_path, ticker_obj):
    def onNewsUpdate(_):
        news_ticks = getattr(ticker_obj, 'tickNews', None) or []
        if not news_ticks:
            return

        latest = news_ticks[-1]
        article_id = safe_str(getattr(latest, 'articleId', None), '')
        provider_code = safe_str(getattr(latest, 'providerCode', None), '')
        headline = safe_str(getattr(latest, 'headline', None), '')
        raw_ts = getattr(latest, 'timeStamp', None)
        source_raw = safe_str(getattr(latest, 'extraData', None), '')
        dedupe_key = f"{provider_code}:{article_id}:{raw_ts}"

        seen = seen_news_ids_by_symbol.setdefault(sym, set())
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)

        news_dt = parse_news_time(raw_ts)
        record_news_event(
            sym,
            news_csv_path,
            news_dt,
            provider_code,
            article_id,
            headline,
            received_dt=_now_utc(),
            is_historical_seed=False,
            provider_name=safe_str(news_provider_name_by_code.get(provider_code, ''), ''),
            source_raw=source_raw,
        )
        print(f"[NEWS] {sym} {format_market_timestamp(news_dt)} | {provider_code} | {headline}")

    return onNewsUpdate


def resolve_news_provider_codes():
    try:
        providers = ib.reqNewsProviders()
    except Exception as exc:
        print(f"[NEWS] Provider lookup failed: {exc}")
        return ''

    codes = [safe_str(getattr(provider, 'code', None), '') for provider in providers]
    names = {safe_str(getattr(provider, 'code', None), ''): safe_str(getattr(provider, 'name', None), '') for provider in providers}
    news_provider_name_by_code.clear()
    news_provider_name_by_code.update({code: name for code, name in names.items() if code})
    codes = [code for code in codes if code]
    if not codes:
        print('[NEWS] No provider codes found; skipping historical news seed.')
        return ''

    joined = '+'.join(codes)
    print(f"[NEWS] Enabled providers: {joined}")
    return joined


def seed_historical_news(sym, contract, provider_codes, news_csv_path):
    if not provider_codes:
        return

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=6)

    try:
        historical_news = ib.reqHistoricalNews(contract.conId, provider_codes, start_utc, now_utc, 50)
    except Exception as exc:
        print(f"[NEWS] {sym} historical seed failed: {exc}")
        return


    _ingest_historical_news_seed(sym, historical_news, news_csv_path)


async def seed_historical_news_async(sym, contract, provider_codes, news_csv_path):
    if not provider_codes:
        return

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=6)

    try:
        historical_news = await ib.reqHistoricalNewsAsync(contract.conId, provider_codes, start_utc, now_utc, 50)
    except Exception as exc:
        print(f"[NEWS] {sym} historical seed failed: {exc}")
        return

    _ingest_historical_news_seed(sym, historical_news, news_csv_path)


def _ingest_historical_news_seed(sym, historical_news, news_csv_path):
    seeded = 0
    for item in historical_news or []:
        news_time = parse_news_time(getattr(item, 'time', None))
        provider_code = safe_str(getattr(item, 'providerCode', None), '')
        article_id = safe_str(getattr(item, 'articleId', None), '')
        headline = safe_str(getattr(item, 'headline', None), '')

        dedupe_key = f"{provider_code}:{article_id}:{getattr(item, 'time', None)}"
        seen = seen_news_ids_by_symbol.setdefault(sym, set())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        record_news_event(
            sym,
            news_csv_path,
            news_time,
            provider_code,
            article_id,
            headline,
            received_dt=_now_utc(),
            is_historical_seed=True,
            provider_name=safe_str(news_provider_name_by_code.get(provider_code, ''), ''),
            source_raw='',
        )
        seeded += 1

    if seeded > 0:
        print(f"[NEWS] {sym} seeded {seeded} historical headlines.")


def _build_header_index(header_row):
    return {name: idx for idx, name in enumerate(header_row or []) if name}


def _overlay_row_values_by_header(target_cols, row, source_index, defaults):
    merged = dict(defaults)
    if not source_index:
        return merged

    for col in target_cols:
        idx = source_index.get(col)
        if idx is None or idx >= len(row):
            continue
        value = row[idx]
        if value not in (None, ''):
            merged[col] = value
    return merged


def normalize_news_csv(csv_path):
    if not os.path.exists(csv_path):
        return

    with open(csv_path, 'r', newline='') as f:
        rows = list(csv.reader(f))

    if not rows:
        with open(csv_path, 'w', newline='') as f:
            csv.writer(f).writerow(NEWS_HEADER)
        return

    first = rows[0]
    has_header = bool(first) and first[0].strip().lower() == 'time'
    source_index = _build_header_index(first if has_header else [])
    data_rows = rows[1:] if has_header else rows
    needs_upgrade = (not has_header) or (first != NEWS_HEADER) or any(len(r) < len(NEWS_HEADER) for r in data_rows)
    if not needs_upgrade:
        return

    upgraded_rows = []
    for row in data_rows:
        if not row:
            continue

        defaults = {
            'time': row[0] if len(row) > 0 else '',
            'provider': row[1] if len(row) > 1 else '',
            'provider_name': news_provider_name_by_code.get(row[1], '') if len(row) > 1 else '',
            'article_id': row[2] if len(row) > 2 else '',
            'headline': row[3] if len(row) > 3 else '',
            'sentiment_score': row[4] if len(row) > 4 else '0',
            'sentiment_confidence': '0',
            'sentiment_label': 'neutral',
            'sentiment_model': SENTIMENT_MODEL,
            'published_ts': row[0] if len(row) > 0 else '',
            'received_ts': row[0] if len(row) > 0 else '',
            'tradable_ts': row[0] if len(row) > 0 else '',
            'is_historical_seed': '0',
            'source_raw': '',
            'source_site': '',
            'dup_cluster_id': '',
            'dup_seq_asof': '0',
            'dup_provider_count_asof': '0',
            'dup_first_seen_ts': row[0] if len(row) > 0 else '',
            'dup_is_repeat': '0',
        }
        merged = _overlay_row_values_by_header(NEWS_HEADER, row, source_index, defaults)
        upgraded_rows.append([merged[col] for col in NEWS_HEADER])

    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(NEWS_HEADER)
        writer.writerows(upgraded_rows)


def normalize_warmup_csv(csv_path):
    if not os.path.exists(csv_path):
        return

    with open(csv_path, 'r', newline='') as f:
        rows = list(csv.reader(f))

    if not rows:
        with open(csv_path, 'w', newline='') as f:
            csv.writer(f).writerow(WARMUP_HEADER)
        return

    first = rows[0]
    first_cell = first[0].strip().lower() if first else ''
    has_header = first_cell in ('epoch', 'timestamp')
    source_index = _build_header_index(first if has_header and first_cell == 'timestamp' else [])
    data_rows = rows[1:] if has_header else rows

    needs_upgrade = (not has_header) or (first != WARMUP_HEADER) or any(len(r) < len(WARMUP_HEADER) for r in data_rows)
    if not needs_upgrade:
        return

    upgraded_rows = []
    current_day = None
    last_close = 0.0
    yesterday_close = 0.0

    for row in data_rows:
        if not row:
            continue

        legacy_epoch_layout = len(row) >= 7 and (not has_header or first_cell == 'epoch')

        if legacy_epoch_layout:
            epoch_raw = row[0] if len(row) > 0 else '0'
            try:
                epoch_int = int(float(epoch_raw))
            except (TypeError, ValueError):
                continue

            open_raw = row[1] if len(row) > 1 else '0'
            high_raw = row[2] if len(row) > 2 else open_raw
            low_raw = row[3] if len(row) > 3 else open_raw
            close_raw = row[4] if len(row) > 4 else open_raw
            volume_raw = row[5] if len(row) > 5 else '0'
            wap_raw = row[6] if len(row) > 6 else close_raw
            timestamp_raw = row[7] if len(row) > 7 else format_market_timestamp_from_epoch(epoch_int)
            count_raw = row[8] if len(row) > 8 else '0'
            yclose_raw = row[9] if len(row) > 9 else ''
            bar_dt = datetime.fromtimestamp(epoch_int, tz=timezone.utc).astimezone(MARKET_ZONE)
        else:
            timestamp_raw = row[0] if len(row) > 0 else ''
            open_raw = row[1] if len(row) > 1 else '0'
            high_raw = row[2] if len(row) > 2 else open_raw
            low_raw = row[3] if len(row) > 3 else open_raw
            close_raw = row[4] if len(row) > 4 else open_raw
            volume_raw = row[5] if len(row) > 5 else '0'
            wap_raw = row[6] if len(row) > 6 else close_raw
            count_raw = row[7] if len(row) > 7 else '0'
            yclose_raw = row[8] if len(row) > 8 else ''
            try:
                bar_dt = datetime.strptime(timestamp_raw.replace(' America/New_York', ''), '%Y%m%d %H:%M:%S').replace(tzinfo=MARKET_ZONE)
            except Exception:
                bar_dt = datetime.now(MARKET_ZONE)
            if not timestamp_raw:
                timestamp_raw = format_market_timestamp(bar_dt)

        try:
            open_val = float(open_raw)
        except (TypeError, ValueError):
            open_val = 0.0
        try:
            high_val = float(high_raw)
        except (TypeError, ValueError):
            high_val = open_val
        try:
            low_val = float(low_raw)
        except (TypeError, ValueError):
            low_val = open_val
        try:
            close_val = float(close_raw)
        except (TypeError, ValueError):
            close_val = open_val
        try:
            wap_val = float(wap_raw)
        except (TypeError, ValueError):
            wap_val = close_val

        bar_day = bar_dt.date()
        if current_day is None:
            current_day = bar_day
        elif bar_day != current_day:
            if last_close > 0:
                yesterday_close = last_close
            current_day = bar_day

        if yesterday_close <= 0:
            yesterday_close = open_val

        if not yclose_raw:
            yclose_raw = f"{yesterday_close:.4f}"

        if not count_raw:
            count_raw = '0'

        upgraded_row = [
            timestamp_raw,
            f"{open_val:.4f}",
            f"{high_val:.4f}",
            f"{low_val:.4f}",
            f"{close_val:.4f}",
            f"{safe_num(float(volume_raw) if str(volume_raw).strip() else 0.0):.18f}",
            f"{(wap_val if wap_val > 0 else close_val):.18f}",
            str(int(float(count_raw))) if str(count_raw).strip() else '0',
            yclose_raw,
            '0', '0', '0', '0', '0', '0', '0',
            '0', '0', '0', '0', '9999',
        ] + build_warmup_tail_defaults(bar_dt, quality_flag='migrated_legacy')

        if source_index:
            merged = _overlay_row_values_by_header(
                WARMUP_HEADER,
                row,
                source_index,
                dict(zip(WARMUP_HEADER, upgraded_row))
            )
            upgraded_row = [merged[col] for col in WARMUP_HEADER]

        upgraded_rows.append(upgraded_row)

        last_close = close_val

    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(WARMUP_HEADER)
        writer.writerows(upgraded_rows)

    print(f"[MIGRATE] Normalized warmup CSV schema: {os.path.basename(csv_path)} rows={len(upgraded_rows)}")


def ensure_csv_files(bar_csv, tick_csv, news_csv):
    normalize_warmup_csv(bar_csv)
    normalize_news_csv(news_csv)

    if not os.path.exists(bar_csv):
        with open(bar_csv, 'w', newline='') as f:
            csv.writer(f).writerow(WARMUP_HEADER)

    if not os.path.exists(tick_csv):
        with open(tick_csv, 'w', newline='') as f:
            csv.writer(f).writerow(TICK_HEADER)

    if not os.path.exists(news_csv):
        with open(news_csv, 'w', newline='') as f:
            csv.writer(f).writerow(NEWS_HEADER)


def cancel_symbol_streams(sym):
    runtime = symbol_runtime.get(sym)
    if not runtime:
        return

    contract = contracts.get(sym)
    bars = bars_dict.get(sym)

    if bars is not None:
        try:
            ib.cancelHistoricalData(bars)
        except Exception as exc:
            print(f"[RECONNECT] {sym} cancelHistoricalData warning: {exc}")

    if contract is not None:
        try:
            ib.cancelMktData(contract)
        except Exception as exc:
            print(f"[RECONNECT] {sym} cancelMktData warning: {exc}")

        if runtime.get('uses_tbt', False):
            try:
                ib.cancelTickByTickData(contract, 'AllLast')
            except Exception as exc:
                print(f"[RECONNECT] {sym} cancelTickByTickData warning: {exc}")

    bars_dict.pop(sym, None)
    tickers.pop(sym, None)
    ticks_dict.pop(sym, None)


def create_quote_capture_callback(sym, ticker_obj):
    def on_quote_update(_):
        mark_stream_alive(sym, 'tick')
        capture_quote_window_from_ticker(sym, ticker_obj)

    return on_quote_update


def _begin_symbol_subscription(sym):
    runtime = symbol_runtime[sym]
    contract = contracts[sym]

    ensure_csv_files(runtime['bar_csv'], runtime['tick_csv'], runtime['news_csv'])
    runtime['last_bar_event_ts'] = _now_utc()
    runtime['last_tick_event_ts'] = _now_utc()

    ticker_obj = ib.reqMktData(contract, '100,104,236,292', snapshot=False, regulatorySnapshot=False)
    ticker_obj.updateEvent += create_quote_capture_callback(sym, ticker_obj)
    ticker_obj.updateEvent += create_news_callback(sym, runtime['news_csv'], ticker_obj)
    tickers[sym] = ticker_obj
    return runtime, contract, ticker_obj


def _attach_symbol_bar_stream(sym, bars, ticker_obj):
    bars.updateEvent += create_bar_callback(sym, symbol_runtime[sym]['bar_csv'], ticker_obj)
    bars_dict[sym] = bars


def _attach_symbol_tick_stream(sym, contract, runtime, ticker_obj):
    if runtime['uses_tbt']:
        live_ticks = ib.reqTickByTickData(contract, 'AllLast')
        live_ticks.updateEvent += create_tick_callback(sym, runtime['tick_csv'], ticker_obj)
        ticks_dict[sym] = live_ticks
    else:
        ticker_obj.updateEvent += create_mktdata_tick_callback(sym, runtime['tick_csv'], ticker_obj)
        ticks_dict[sym] = ticker_obj
        print(f"[!] {sym}: using mktData fallback (tick-by-tick cap reached: {MAX_TICK_BY_TICK_STREAMS})")


def subscribe_symbol_streams(sym, provider_codes):
    runtime, contract, ticker_obj = _begin_symbol_subscription(sym)

    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='1800 S',
        barSizeSetting='5 secs', whatToShow='TRADES', useRTH=False, keepUpToDate=True
    )
    _attach_symbol_bar_stream(sym, bars, ticker_obj)
    _attach_symbol_tick_stream(sym, contract, runtime, ticker_obj)

    if not runtime.get('news_seeded', False):
        seed_historical_news(sym, contract, provider_codes, runtime['news_csv'])
        runtime['news_seeded'] = True


async def subscribe_symbol_streams_async(sym, provider_codes):
    runtime, contract, ticker_obj = _begin_symbol_subscription(sym)

    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime='',
        durationStr='1800 S',
        barSizeSetting='5 secs',
        whatToShow='TRADES',
        useRTH=False,
        keepUpToDate=True,
    )
    _attach_symbol_bar_stream(sym, bars, ticker_obj)
    _attach_symbol_tick_stream(sym, contract, runtime, ticker_obj)

    if not runtime.get('news_seeded', False):
        await seed_historical_news_async(sym, contract, provider_codes, runtime['news_csv'])
        runtime['news_seeded'] = True


async def resubscribe_symbol_streams(sym, provider_codes, reason='stale stream'):
    if sym in stream_resubscribe_lock:
        return

    stream_resubscribe_lock.add(sym)
    try:
        print(f"[STREAM-WATCHDOG] Re-arming {sym} ({reason})")
        cancel_symbol_streams(sym)
        await subscribe_symbol_streams_async(sym, provider_codes)
        print(f"[STREAM-WATCHDOG] {sym} streams resumed.")
    except Exception as exc:
        cancel_symbol_streams(sym)
        print(f"[STREAM-WATCHDOG] {sym} resume failed: {exc}")
    finally:
        stream_resubscribe_lock.discard(sym)


async def resubscribe_all_symbols(provider_codes):
    if not ib.isConnected():
        return

    print('[RECONNECT] Re-arming all symbol streams...')
    for sym in symbols:
        await resubscribe_symbol_streams(sym, provider_codes, reason='post reconnect')


async def stream_heartbeat_watchdog(provider_codes):
    while True:
        await asyncio.sleep(STREAM_WATCHDOG_INTERVAL_SECONDS)

        if not ib.isConnected():
            continue

        now_utc = _now_utc()
        for sym in symbols:
            runtime = symbol_runtime.get(sym)
            if not runtime:
                continue

            bar_age = (now_utc - runtime.get('last_bar_event_ts', now_utc)).total_seconds()
            tick_age = (now_utc - runtime.get('last_tick_event_ts', now_utc)).total_seconds()

            stale_bar = BAR_STALE_SECONDS > 0 and bar_age > BAR_STALE_SECONDS
            stale_tick = TICK_STALE_SECONDS > 0 and tick_age > TICK_STALE_SECONDS

            if stale_bar or stale_tick:
                reason = f"bar_age={bar_age:.1f}s tick_age={tick_age:.1f}s"
                await resubscribe_symbol_streams(sym, provider_codes, reason)


async def reconnect_watchdog(provider_codes):
    while True:
        await asyncio.sleep(RECONNECT_RETRY_SECONDS)
        if ib.isConnected():
            continue

        print('[RECONNECT] IBKR disconnected. Attempting reconnect...')
        try:
            await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
            print('[RECONNECT] Connected to IBKR. Re-subscribing streams...')
            await resubscribe_all_symbols(provider_codes)
        except Exception as exc:
            print(f"[RECONNECT] connectAsync failed: {exc}")


# --- MAIN LOGIC ---
def _extract_source_site(raw_source):
    text = safe_str(raw_source, '')
    if not text:
        return ''

    url_match = re.search(r'https?://([^/\s]+)', text, flags=re.IGNORECASE)
    if url_match:
        return url_match.group(1).lower()

    domain_match = re.search(r'([A-Za-z0-9.-]+\.[A-Za-z]{2,})', text)
    if domain_match:
        return domain_match.group(1).lower()

    return text[:64].lower()


def _normalize_news_cluster_text(headline):
    normalized = re.sub(r'[^a-z0-9]+', ' ', safe_str(headline, '').lower()).strip()
    return re.sub(r'\s+', ' ', normalized)


def get_news_cluster_state(sym):
    return news_cluster_state_by_symbol.setdefault(sym, {})


def _cleanup_news_clusters(sym, published_dt):
    state = get_news_cluster_state(sym)
    cutoff = published_dt - timedelta(minutes=15)
    stale_keys = [key for key, meta in state.items() if meta['first_seen_dt'] < cutoff]
    for key in stale_keys:
        state.pop(key, None)


def compute_news_duplicate_metadata(sym, headline, provider_code, published_dt):
    normalized = _normalize_news_cluster_text(headline)
    if not normalized:
        cluster_id = ''
        return {
            'dup_cluster_id': cluster_id,
            'dup_seq_asof': 0,
            'dup_provider_count_asof': 0,
            'dup_first_seen_ts': '',
            'dup_is_repeat': 0,
        }

    _cleanup_news_clusters(sym, published_dt)
    bucket = published_dt.strftime('%Y%m%d %H:%M')
    cluster_key = f"{bucket}|{normalized[:160]}"
    cluster_state = get_news_cluster_state(sym)
    cluster = cluster_state.get(cluster_key)
    if cluster is None:
        cluster = {
            'cluster_id': hashlib.sha1(cluster_key.encode('utf-8')).hexdigest()[:16],
            'first_seen_dt': published_dt,
            'count': 0,
            'providers': set(),
        }
        cluster_state[cluster_key] = cluster

    cluster['count'] += 1
    if provider_code:
        cluster['providers'].add(provider_code)

    return {
        'dup_cluster_id': cluster['cluster_id'],
        'dup_seq_asof': cluster['count'],
        'dup_provider_count_asof': len(cluster['providers']),
        'dup_first_seen_ts': format_market_timestamp(cluster['first_seen_dt']),
        'dup_is_repeat': 1 if cluster['count'] > 1 else 0,
    }


def _market_context_columns_for_symbol(ctx_symbol):
    return [f'Mkt_{ctx_symbol}_{suffix}' for suffix in MARKET_CONTEXT_FIELD_SUFFIXES]


def _default_market_context_values():
    defaults = {}
    for ctx_symbol in market_context_symbols:
        defaults.update({
            f'Mkt_{ctx_symbol}_Close5s': '0',
            f'Mkt_{ctx_symbol}_Ret5s': '0',
            f'Mkt_{ctx_symbol}_Ret30s': '0',
            f'Mkt_{ctx_symbol}_SpreadBps': '0',
            f'Mkt_{ctx_symbol}_L1Imbalance': '0',
            f'Mkt_{ctx_symbol}_NewsCount300s': '0',
            f'Mkt_{ctx_symbol}_SentimentMean300s': '0',
            f'Mkt_{ctx_symbol}_AsOfLagSec': '999999',
        })
    defaults.update({
        'MktReadyCount': '0',
        'MktBreadthUp5s': '0',
        'MktMeanRet5s': '0',
        'MktDispersion5s': '0',
    })
    return defaults


def build_warmup_tail_defaults(bar_dt, quality_flag='none'):
    market_ts = format_market_timestamp(bar_dt)
    minute_of_day = bar_dt.hour * 60 + bar_dt.minute
    defaults = {
        'AsOfTs': market_ts,
        'BarEpochSec': str(int(bar_dt.timestamp())),
        'SessionBucket': _session_bucket(bar_dt),
        'MinuteOfDay': str(minute_of_day),
        'SecondsFromOpen': str(max(0, int((minute_of_day - 570) * 60 + bar_dt.second))),
        'SpreadBps': '0',
        'L1Imbalance': '0',
        'NewsCount60s': '0',
        'NewsUniqueProviders300s': '0',
        'SentimentStd300s': '0',
        'SentimentLatest': '0',
        'SentimentModel': SENTIMENT_MODEL,
        'SentimentConfidenceMean300s': '0',
        'SentimentConfidenceLatest': '0',
        'NewsAsOfLagSec': '999999',
        'NewsCoverage300s': '0',
        'FeatureCompleteness': '0',
        'DataQualityFlags': quality_flag,
        'BidLast': '0',
        'AskLast': '0',
        'BidSizeLast': '0',
        'AskSizeLast': '0',
        'PutVolDelta5s': '0',
        'CallVolDelta5s': '0',
        'ShortableDelta5s': '0',
        'ShortableMin5s': '0',
        'ShortableMax5s': '0',
        'QuoteUpdateCount5s': '0',
        'QuoteCoverage5s': '0',
        'QuoteAgeMs': '999999',
        'SpreadMinBps5s': '0',
        'SpreadMaxBps5s': '0',
        'ImbalanceStd5s': '0',
        'AtBidVol': '0',
        'AtAskVol': '0',
        'TradePrintCount5s': '0',
    }
    defaults.update(_default_market_context_values())
    return [defaults[col] for col in WARMUP_HEADER[len(WARMUP_CORE_HEADER):]]


def _new_market_context_state():
    return {'snapshots': deque(maxlen=16)}


def get_market_context_state(sym):
    return market_context_state_by_symbol.setdefault(sym, _new_market_context_state())


def update_market_context_snapshot(sym, bar_dt, close_price, quote_metrics, news_features):
    state = get_market_context_state(sym)
    snapshots = state['snapshots']
    prev_snapshot = snapshots[-1] if snapshots else None
    close_price = float(close_price)
    prev_close = prev_snapshot['close_price'] if prev_snapshot is not None else None
    close_30s_ago = snapshots[-6]['close_price'] if len(snapshots) >= 6 else None

    snapshot = {
        'bar_dt': bar_dt,
        'close_price': close_price,
        'ret_5s': ((close_price / prev_close) - 1.0) if prev_close and prev_close > 0 else 0.0,
        'ret_30s': ((close_price / close_30s_ago) - 1.0) if close_30s_ago and close_30s_ago > 0 else 0.0,
        'spread_bps': float(quote_metrics['spread_bps_twap']),
        'l1_imbalance': float(quote_metrics['l1_imbalance_twap']),
        'news_count_300s': int(news_features['news_count_300s']),
        'sentiment_mean_300s': float(news_features['sentiment_mean_300s']),
    }
    snapshots.append(snapshot)


def _latest_market_snapshot_asof(sym, asof_dt):
    snapshots = get_market_context_state(sym)['snapshots']
    for snapshot in reversed(snapshots):
        if snapshot['bar_dt'] <= asof_dt:
            return snapshot
    return None


def build_market_context_features(asof_dt):
    features = _default_market_context_values()
    ready_returns = []
    breadth_up = 0
    ready_count = 0

    for ctx_symbol in market_context_symbols:
        snapshot = _latest_market_snapshot_asof(ctx_symbol, asof_dt)
        if snapshot is None:
            continue

        lag_sec = max(0.0, (asof_dt - snapshot['bar_dt']).total_seconds())
        features.update({
            f'Mkt_{ctx_symbol}_Close5s': f"{snapshot['close_price']:.4f}",
            f'Mkt_{ctx_symbol}_Ret5s': f"{snapshot['ret_5s']:.6f}",
            f'Mkt_{ctx_symbol}_Ret30s': f"{snapshot['ret_30s']:.6f}",
            f'Mkt_{ctx_symbol}_SpreadBps': f"{snapshot['spread_bps']:.4f}",
            f'Mkt_{ctx_symbol}_L1Imbalance': f"{snapshot['l1_imbalance']:.4f}",
            f'Mkt_{ctx_symbol}_NewsCount300s': str(int(snapshot['news_count_300s'])),
            f'Mkt_{ctx_symbol}_SentimentMean300s': f"{snapshot['sentiment_mean_300s']:.4f}",
            f'Mkt_{ctx_symbol}_AsOfLagSec': f"{lag_sec:.2f}",
        })

        if lag_sec <= 15.0:
            ready_count += 1
            ready_returns.append(snapshot['ret_5s'])
            if snapshot['ret_5s'] > 0:
                breadth_up += 1

    mean_ret = float(np.mean(ready_returns)) if ready_returns else 0.0
    dispersion = float(np.std(ready_returns, ddof=1)) if len(ready_returns) > 1 else 0.0
    features['MktReadyCount'] = str(int(ready_count))
    features['MktBreadthUp5s'] = str(int(breadth_up))
    features['MktMeanRet5s'] = f"{mean_ret:.6f}"
    features['MktDispersion5s'] = f"{dispersion:.6f}"
    return features


def capture_market_context_features(sym, bar_dt, quote_metrics, news_features):
    close_price = safe_num(quote_metrics.get('close_price', None), 0.0)
    if close_price <= 0:
        close_price = safe_num(quote_metrics['bid_last'] + quote_metrics['ask_last']) / 2.0
    update_market_context_snapshot(sym, bar_dt, close_price, quote_metrics, news_features)
    market_context_features = build_market_context_features(bar_dt)
    return market_context_features


def _new_bar_state():
    return {
        'current_day': None,
        'last_close': 0.0,
        'yesterday_close': 0.0,
    }


def create_bar_callback(sym, csv_path, ticker_obj):
    def onBarUpdate(bars, hasNewBar):
        mark_stream_alive(sym, 'bar')
        if hasNewBar:
            bar = bars[-1]
            market_dt = to_market_dt(getattr(bar, 'date', None))
            market_ts = format_market_timestamp(getattr(bar, 'date', None))
            bar_wap = safe_num(getattr(bar, 'wap', getattr(bar, 'average', None)))

            state = bar_state_by_symbol.setdefault(sym, _new_bar_state())

            bar_day = market_dt.date()
            previous_day = state['current_day']
            if previous_day is None:
                state['current_day'] = bar_day
            elif bar_day != previous_day:
                if state['last_close'] > 0:
                    state['yesterday_close'] = state['last_close']
                state['current_day'] = bar_day

            if state['yesterday_close'] <= 0:
                fallback_close = safe_num(getattr(ticker_obj, 'close', None), 0.0)
                state['yesterday_close'] = fallback_close if fallback_close > 0 else safe_num(bar.open, 0.0)

            bar_volume = safe_num(getattr(bar, 'volume', None), 0.0)
            bar_count = int(safe_num(getattr(bar, 'barCount', getattr(bar, 'count', None)), 0))
            y_close = safe_num(getattr(ticker_obj, 'close', None), 0.0)
            news_features = get_news_features(sym, market_dt)
            quote_metrics = finalize_quote_window(sym, market_dt)

            market_context_features = capture_market_context_features(
                sym,
                market_dt,
                {
                    **quote_metrics,
                    'close_price': safe_num(bar.close),
                },
                news_features,
            )

            bid = quote_metrics['bid_twap']
            ask = quote_metrics['ask_twap']
            bid_size = quote_metrics['bid_size_twap']
            ask_size = quote_metrics['ask_size_twap']
            put_vol = quote_metrics['put_vol_last']
            call_vol = quote_metrics['call_vol_last']
            shortable = quote_metrics['shortable_last']

            spread_bps = quote_metrics['spread_bps_twap']
            l1_imbalance = quote_metrics['l1_imbalance_twap']
            minute_of_day = market_dt.hour * 60 + market_dt.minute
            seconds_from_open = max(0, int((minute_of_day - 570) * 60 + market_dt.second))
            session_bucket = _session_bucket(market_dt)

            available_flags = {
                'bid_ask': quote_metrics['quote_coverage_5s'] > 0.0,
                'sizes': (bid_size + ask_size) > 0,
                'options': quote_metrics['has_option_levels'],
                'shortable': quote_metrics['has_shortable'],
                'news': news_features['news_count_300s'] > 0,
            }
            feature_completeness = sum(1 for ok in available_flags.values() if ok) / float(len(available_flags))
            quality_flags = [name for name, ok in available_flags.items() if not ok]
            if quote_metrics['quote_coverage_5s'] < 0.50:
                quality_flags.append('sparse_quote')
            if quote_metrics['quote_age_ms'] > 10000:
                quality_flags.append('stale_quote')
            if quote_metrics['put_call_reset']:
                quality_flags.append('options_reset')
            if int(market_context_features['MktReadyCount']) < len(market_context_symbols):
                quality_flags.append('partial_market_context')
            data_quality_flags = 'none' if not quality_flags else '|'.join(dict.fromkeys(quality_flags))

            with open(csv_path, 'a', newline='') as f:
                csv.writer(f).writerow([
                    market_ts,
                    f"{safe_num(bar.open):.4f}",
                    f"{safe_num(bar.high):.4f}",
                    f"{safe_num(bar.low):.4f}",
                    f"{safe_num(bar.close):.4f}",
                    f"{bar_volume:.18f}",
                    f"{safe_num(bar_wap if bar_wap > 0 else bar.close):.18f}",
                    bar_count,
                    f"{safe_num(y_close if y_close > 0 else state['yesterday_close']):.4f}",
                    f"{bid:.4f}",
                    f"{ask:.4f}",
                    f"{bid_size:.0f}",
                    f"{ask_size:.0f}",
                    put_vol,
                    call_vol,
                    f"{shortable:.0f}",
                    news_features['news_count_300s'],
                    f"{news_features['sentiment_mean_300s']:.4f}",
                    f"{news_features['sentiment_min_300s']:.4f}",
                    f"{news_features['sentiment_max_300s']:.4f}",
                    f"{news_features['minutes_since_news']:.4f}",
                    market_ts,
                    int(market_dt.timestamp()),
                    session_bucket,
                    minute_of_day,
                    seconds_from_open,
                    f"{spread_bps:.4f}",
                    f"{l1_imbalance:.4f}",
                    news_features['news_count_60s'],
                    news_features['news_unique_providers_300s'],
                    f"{news_features['sentiment_std_300s']:.4f}",
                    f"{news_features['sentiment_latest']:.4f}",
                    SENTIMENT_MODEL,
                    f"{news_features['sentiment_conf_mean_300s']:.4f}",
                    f"{news_features['sentiment_conf_latest']:.4f}",
                    f"{news_features['news_asof_lag_sec']:.2f}",
                    f"{news_features['news_coverage_300s']:.4f}",
                    f"{feature_completeness:.4f}",
                    data_quality_flags,
                    f"{quote_metrics['bid_last']:.4f}",
                    f"{quote_metrics['ask_last']:.4f}",
                    f"{quote_metrics['bid_size_last']:.0f}",
                    f"{quote_metrics['ask_size_last']:.0f}",
                    quote_metrics['put_vol_delta_5s'],
                    quote_metrics['call_vol_delta_5s'],
                    f"{quote_metrics['shortable_delta_5s']:.0f}",
                    f"{quote_metrics['shortable_min_5s']:.0f}",
                    f"{quote_metrics['shortable_max_5s']:.0f}",
                    quote_metrics['quote_update_count_5s'],
                    f"{quote_metrics['quote_coverage_5s']:.4f}",
                    f"{quote_metrics['quote_age_ms']:.1f}",
                    f"{quote_metrics['spread_min_bps_5s']:.4f}",
                    f"{quote_metrics['spread_max_bps_5s']:.4f}",
                    f"{quote_metrics['l1_imbalance_std_5s']:.4f}",
                    f"{quote_metrics['at_bid_vol']:.4f}",
                    f"{quote_metrics['at_ask_vol']:.4f}",
                    quote_metrics['trade_print_count_5s'],
                ] + [market_context_features[col] for col in MARKET_CONTEXT_HEADER + MARKET_CONTEXT_SUMMARY_HEADER])

            state['last_close'] = safe_num(bar.close)
            print(f"[BAR] {sym} {market_ts} | Close: {bar.close} | Vol: {bar.volume}")
    return onBarUpdate


def create_tick_callback(sym, csv_path, ticker_obj):
    skipped_invalid_ticks = 0

    def onTickUpdate(ticker):
        nonlocal skipped_invalid_ticks
        mark_stream_alive(sym, 'tick')

        if not ticker.ticks:
            return

        tick = ticker.ticks[-1]
        current_bid = safe_num(ticker_obj.bid)
        current_ask = safe_num(ticker_obj.ask)
        current_bid_size = safe_num(getattr(ticker_obj, 'bidSize', None), 0.0)
        current_ask_size = safe_num(getattr(ticker_obj, 'askSize', None), 0.0)
        put_vol = int(safe_num(ticker_obj.putVolume, 0))
        call_vol = int(safe_num(ticker_obj.callVolume, 0))
        shortable = safe_num(ticker_obj.shortableShares, 0.0)

        last_price = safe_num(getattr(ticker_obj, 'last', None), 0.0)
        last_size = safe_num(getattr(ticker_obj, 'lastSize', None), 0.0)
        mid = (current_bid + current_ask) / 2.0 if current_bid > 0 and current_ask > 0 else safe_num(last_price, 0.0)
        spread = (current_ask - current_bid) if current_bid > 0 and current_ask > 0 else 0.0
        volume = safe_num(getattr(ticker_obj, 'volume', None), 0.0)
        vwap = safe_num(getattr(ticker_obj, 'vwap', None), 0.0)
        bbo_exchange = safe_str(getattr(ticker_obj, 'bboExchange', ''), '')
        last_exchange = safe_str(getattr(ticker_obj, 'lastExchange', ''), '')

        tick_time = tick.time if tick.time is not None else ticker.time
        tick_price = safe_num(getattr(tick, 'price', None))
        tick_size = safe_num(getattr(tick, 'size', None))
        exchange = safe_str(getattr(tick, 'exchange', ''), '')

        if tick_price <= 0 or tick_size <= 0:
            skipped_invalid_ticks += 1
            if skipped_invalid_ticks % 100 == 0:
                print(f"[TICK] {sym} skipped invalid ticks: {skipped_invalid_ticks}")
            return

        record_trade_window_volume(
            sym,
            tick_time,
            tick_price,
            tick_size,
            bid=current_bid,
            ask=current_ask,
        )

        with open(csv_path, 'a', newline='') as f:
            csv.writer(f).writerow([
                tick_time,
                tick_price,
                tick_size,
                current_bid,
                current_ask,
                current_bid_size,
                current_ask_size,
                last_price,
                last_size,
                mid,
                spread,
                put_vol,
                call_vol,
                shortable,
                volume,
                vwap,
                bbo_exchange,
                exchange or last_exchange
            ])
    return onTickUpdate


def create_mktdata_tick_callback(sym, csv_path, ticker_obj):
    skipped_invalid_ticks = 0
    last_emitted = None

    def onMktDataUpdate(_):
        nonlocal skipped_invalid_ticks, last_emitted
        mark_stream_alive(sym, 'tick')

        tick_time = ticker_obj.time
        tick_price = safe_num(ticker_obj.last)
        tick_size = safe_num(ticker_obj.lastSize)
        current_bid = safe_num(ticker_obj.bid)
        current_ask = safe_num(ticker_obj.ask)
        current_bid_size = safe_num(getattr(ticker_obj, 'bidSize', None), 0.0)
        current_ask_size = safe_num(getattr(ticker_obj, 'askSize', None), 0.0)
        put_vol = int(safe_num(ticker_obj.putVolume, 0))
        call_vol = int(safe_num(ticker_obj.callVolume, 0))
        shortable = safe_num(ticker_obj.shortableShares, 0.0)
        last_price = safe_num(getattr(ticker_obj, 'last', None), 0.0)
        last_size = safe_num(getattr(ticker_obj, 'lastSize', None), 0.0)
        mid = (current_bid + current_ask) / 2.0 if current_bid > 0 and current_ask > 0 else safe_num(last_price, 0.0)
        spread = (current_ask - current_bid) if current_bid > 0 and current_ask > 0 else 0.0
        volume = safe_num(getattr(ticker_obj, 'volume', None), 0.0)
        vwap = safe_num(getattr(ticker_obj, 'vwap', None), 0.0)
        bbo_exchange = safe_str(getattr(ticker_obj, 'bboExchange', ''), '')
        last_exchange = safe_str(getattr(ticker_obj, 'lastExchange', ''), '')

        if tick_time is None or tick_price <= 0 or tick_size <= 0:
            skipped_invalid_ticks += 1
            if skipped_invalid_ticks % 100 == 0:
                print(f"[TICK-FALLBACK] {sym} skipped invalid ticks: {skipped_invalid_ticks}")
            return

        current_key = (tick_time, tick_price, tick_size)
        if current_key == last_emitted:
            return
        last_emitted = current_key

        record_trade_window_volume(
            sym,
            tick_time,
            tick_price,
            tick_size,
            bid=current_bid,
            ask=current_ask,
        )

        with open(csv_path, 'a', newline='') as f:
            csv.writer(f).writerow([
                tick_time,
                tick_price,
                tick_size,
                current_bid,
                current_ask,
                current_bid_size,
                current_ask_size,
                last_price,
                last_size,
                mid,
                spread,
                put_vol,
                call_vol,
                shortable,
                volume,
                vwap,
                bbo_exchange,
                last_exchange
            ])

    return onMktDataUpdate


# --- SUBSCRIPTION LOOP ---
init_sentiment_model()
run_finbert_self_test()
print(f"[SENTIMENT] Startup summary: active={get_active_sentiment_engine()} hf_auth={get_hf_auth_status()}")
news_provider_codes = resolve_news_provider_codes()

for idx, sym in enumerate(symbols):
    print(f"[*] Setting up data streams for {sym}...")
    contract = Stock(sym, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    contracts[sym] = contract

    warmup_date = datetime.now(MARKET_ZONE).strftime('%Y%m%d')
    symbol_dir = os.path.join(OUTPUT_DIR, sym)
    os.makedirs(symbol_dir, exist_ok=True)

    symbol_runtime[sym] = {
        'bar_csv': os.path.join(symbol_dir, f'{sym}_5s_warmup_{warmup_date}.csv'),
        'tick_csv': os.path.join(symbol_dir, f'{sym}_live_ticks_{warmup_date}.csv'),
        'news_csv': os.path.join(symbol_dir, f'{sym}_news_{warmup_date}.csv'),
        'uses_tbt': idx < MAX_TICK_BY_TICK_STREAMS,
        'news_seeded': False,
        'last_bar_event_ts': _now_utc(),
        'last_tick_event_ts': _now_utc(),
    }

    subscribe_symbol_streams(sym, news_provider_codes)

if MAX_TICK_BY_TICK_STREAMS < len(symbols):
    print(f"[!] Tick-by-tick cap active: {MAX_TICK_BY_TICK_STREAMS} of {len(symbols)} symbols use AllLast stream.")

ib.disconnectedEvent += lambda: print('[RECONNECT] disconnectedEvent received from IBKR.')
ib.connectedEvent += lambda: print('[RECONNECT] connectedEvent received from IBKR.')

watchdog_task = asyncio.get_event_loop().create_task(reconnect_watchdog(news_provider_codes))
stream_watchdog_task = asyncio.get_event_loop().create_task(stream_heartbeat_watchdog(news_provider_codes))

print("\n[+] Harvester fully armed. Streaming all symbols concurrently...")
try:
    ib.run()
except KeyboardInterrupt:
    print("\n[*] Harvester stopped.")
finally:
    watchdog_task.cancel()
    stream_watchdog_task.cancel()
