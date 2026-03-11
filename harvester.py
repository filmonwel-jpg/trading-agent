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
from collections import deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ib = IB()
# Connect to Gateway/TWS. Using ClientID 10 to avoid conflicts with Java bots.
ib.connect('127.0.0.1', 7497, clientId=10)

# Add all the symbols you want to harvest here.
symbols = ['TSLA', 'QQQ', 'NVDA','AMD']
MAX_TICK_BY_TICK_STREAMS = int(os.getenv('MAX_TICK_BY_TICK_STREAMS', '4'))
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test'))
MARKET_ZONE = ZoneInfo('America/New_York')

# Dictionaries to keep track of active subscriptions and data state
contracts = {}
tickers = {}
bars_dict = {}
ticks_dict = {}
bar_state_by_symbol = {}
WARMUP_HEADER = [
    'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'Count', 'YesterdayClose',
    'Bid', 'Ask', 'BidSize', 'AskSize', 'PutVol', 'CallVol', 'ShortableShares',
    'NewsCount300s', 'SentimentMean300s', 'SentimentMin300s', 'SentimentMax300s', 'MinutesSinceNews'
]
TICK_HEADER = [
    'time', 'price', 'size',
    'bid', 'ask', 'bid_size', 'ask_size',
    'last', 'last_size', 'mid', 'spread',
    'put_vol', 'call_vol', 'shortable_shares',
    'volume', 'vwap', 'bbo_exchange', 'last_exchange'
]
NEWS_HEADER = ['time', 'provider', 'article_id', 'headline', 'sentiment_score']
NEWS_LOOKBACK_SECONDS = int(os.getenv('NEWS_LOOKBACK_SECONDS', '300'))
SENTIMENT_MODEL = os.getenv('SENTIMENT_MODEL', 'finbert').strip().lower()
FINBERT_MODEL_NAME = os.getenv('FINBERT_MODEL_NAME', 'ProsusAI/finbert').strip()
FINBERT_MAX_LENGTH = int(os.getenv('FINBERT_MAX_LENGTH', '128'))

POSITIVE_WORDS = {
    'beat', 'beats', 'strong', 'up', 'upgrade', 'surge', 'growth', 'profit', 'bullish', 'outperform'
}
NEGATIVE_WORDS = {
    'miss', 'misses', 'weak', 'down', 'downgrade', 'drop', 'loss', 'bearish', 'underperform', 'lawsuit'
}

news_state_by_symbol = {}
seen_news_ids_by_symbol = {}
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

        finbert_pipeline = pipeline(
            'text-classification',
            model=FINBERT_MODEL_NAME,
            tokenizer=FINBERT_MODEL_NAME,
            truncation=True,
        )
        id2label = getattr(getattr(finbert_pipeline, 'model', None), 'config', None)
        raw_map = getattr(id2label, 'id2label', {}) if id2label is not None else {}
        finbert_label_map = {str(k): str(v).lower() for k, v in raw_map.items()}
        print(f"[SENTIMENT] FinBERT enabled: model={FINBERT_MODEL_NAME}")
    except Exception as exc:
        finbert_pipeline = None
        finbert_label_map = {}
        print(f"[SENTIMENT] FinBERT unavailable ({exc}); using lexicon fallback.")


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


def score_headline_sentiment(headline):
    clean_headline = safe_str(headline, '')
    if not clean_headline:
        return 0.0

    if finbert_pipeline is None:
        return score_headline_sentiment_lexicon(clean_headline)

    try:
        result = finbert_pipeline(clean_headline, truncation=True, max_length=FINBERT_MAX_LENGTH)
        if not result:
            return score_headline_sentiment_lexicon(clean_headline)

        top = result[0]
        label = _normalize_finbert_label(getattr(top, 'get', lambda *_: '')('label', ''))
        confidence = float(getattr(top, 'get', lambda *_: 0.0)('score', 0.0))
        confidence = max(0.0, min(1.0, confidence))

        if label == 'positive':
            return confidence
        if label == 'negative':
            return -confidence
        return 0.0
    except Exception:
        return score_headline_sentiment_lexicon(clean_headline)


def get_news_state(sym):
    return news_state_by_symbol.setdefault(sym, {
        'events': deque(),
        'last_news_dt': None,
    })


def record_news_event(sym, news_csv_path, news_dt, provider_code, article_id, headline):
    sentiment_score = score_headline_sentiment(headline)
    state = get_news_state(sym)
    state['events'].append((news_dt, sentiment_score))
    state['last_news_dt'] = news_dt

    cutoff = news_dt - timedelta(seconds=NEWS_LOOKBACK_SECONDS)
    while state['events'] and state['events'][0][0] < cutoff:
        state['events'].popleft()

    with open(news_csv_path, 'a', newline='') as f:
        csv.writer(f).writerow([
            format_market_timestamp(news_dt),
            safe_str(provider_code, ''),
            safe_str(article_id, ''),
            safe_str(headline, ''),
            f"{sentiment_score:.4f}",
        ])


def get_news_features(sym, bar_dt):
    state = get_news_state(sym)
    cutoff = bar_dt - timedelta(seconds=NEWS_LOOKBACK_SECONDS)

    while state['events'] and state['events'][0][0] < cutoff:
        state['events'].popleft()

    if not state['events']:
        minutes_since_news = 9999.0
        if state['last_news_dt'] is not None:
            delta = bar_dt - state['last_news_dt']
            minutes_since_news = max(0.0, delta.total_seconds() / 60.0)
        return 0, 0.0, 0.0, 0.0, minutes_since_news

    scores = [event[1] for event in state['events']]
    mean_score = sum(scores) / len(scores)
    min_score = min(scores)
    max_score = max(scores)
    delta = bar_dt - state['last_news_dt'] if state['last_news_dt'] is not None else timedelta(seconds=0)
    minutes_since_news = max(0.0, delta.total_seconds() / 60.0)
    return len(scores), mean_score, min_score, max_score, minutes_since_news


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
        dedupe_key = f"{provider_code}:{article_id}:{raw_ts}"

        seen = seen_news_ids_by_symbol.setdefault(sym, set())
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)

        news_dt = parse_news_time(raw_ts)
        record_news_event(sym, news_csv_path, news_dt, provider_code, article_id, headline)
        print(f"[NEWS] {sym} {format_market_timestamp(news_dt)} | {provider_code} | {headline}")

    return onNewsUpdate


def resolve_news_provider_codes():
    try:
        providers = ib.reqNewsProviders()
    except Exception as exc:
        print(f"[NEWS] Provider lookup failed: {exc}")
        return ''

    codes = [safe_str(getattr(provider, 'code', None), '') for provider in providers]
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

        record_news_event(sym, news_csv_path, news_time, provider_code, article_id, headline)
        seeded += 1

    if seeded > 0:
        print(f"[NEWS] {sym} seeded {seeded} historical headlines.")


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

        upgraded_rows.append([
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
            '0', '0', '0', '0', '9999'
        ])

        last_close = close_val

    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(WARMUP_HEADER)
        writer.writerows(upgraded_rows)

    print(f"[MIGRATE] Normalized warmup CSV schema: {os.path.basename(csv_path)} rows={len(upgraded_rows)}")


# --- CALLBACK FACTORIES ---
# These functions generate unique event listeners for EACH symbol.
def create_bar_callback(sym, csv_path, ticker_obj):
    def onBarUpdate(bars, hasNewBar):
        if hasNewBar:
            bar = bars[-1]
            market_dt = to_market_dt(getattr(bar, 'date', None))
            market_ts = format_market_timestamp(getattr(bar, 'date', None))
            bar_wap = safe_num(getattr(bar, 'wap', getattr(bar, 'average', None)))

            state = bar_state_by_symbol.setdefault(sym, {
                'current_day': None,
                'last_close': 0.0,
                'yesterday_close': 0.0,
            })

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
            news_count, sent_mean, sent_min, sent_max, mins_since_news = get_news_features(sym, market_dt)

            bid = safe_num(getattr(ticker_obj, 'bid', None), 0.0)
            ask = safe_num(getattr(ticker_obj, 'ask', None), 0.0)
            bid_size = safe_num(getattr(ticker_obj, 'bidSize', None), 0.0)
            ask_size = safe_num(getattr(ticker_obj, 'askSize', None), 0.0)
            put_vol = int(safe_num(getattr(ticker_obj, 'putVolume', None), 0))
            call_vol = int(safe_num(getattr(ticker_obj, 'callVolume', None), 0))
            shortable = safe_num(getattr(ticker_obj, 'shortableShares', None), 0.0)

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
                    news_count,
                    f"{sent_mean:.4f}",
                    f"{sent_min:.4f}",
                    f"{sent_max:.4f}",
                    f"{mins_since_news:.4f}",
                ])

            state['last_close'] = safe_num(bar.close)
            print(f"[BAR] {sym} {market_ts} | Close: {bar.close} | Vol: {bar.volume}")
    return onBarUpdate


def create_tick_callback(sym, csv_path, ticker_obj):
    skipped_invalid_ticks = 0

    def onTickUpdate(ticker):
        nonlocal skipped_invalid_ticks

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
tbt_subscriptions = 0
init_sentiment_model()
news_provider_codes = resolve_news_provider_codes()

for sym in symbols:
    print(f"[*] Setting up data streams for {sym}...")
    contract = Stock(sym, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    contracts[sym] = contract

    warmup_date = datetime.now(MARKET_ZONE).strftime('%Y%m%d')
    symbol_dir = os.path.join(OUTPUT_DIR, sym)
    os.makedirs(symbol_dir, exist_ok=True)
    bar_csv = os.path.join(symbol_dir, f'{sym}_5s_warmup_{warmup_date}.csv')
    tick_csv = os.path.join(symbol_dir, f'{sym}_live_ticks_{warmup_date}.csv')
    news_csv = os.path.join(symbol_dir, f'{sym}_news_{warmup_date}.csv')

    # Init CSVs with Headers
    normalize_warmup_csv(bar_csv)

    if not os.path.exists(bar_csv):
        with open(bar_csv, 'w', newline='') as f:
            csv.writer(f).writerow(WARMUP_HEADER)

    if not os.path.exists(tick_csv):
        with open(tick_csv, 'w', newline='') as f:
            csv.writer(f).writerow(TICK_HEADER)

    if not os.path.exists(news_csv):
        with open(news_csv, 'w', newline='') as f:
            csv.writer(f).writerow(NEWS_HEADER)

    # 1. State Tracker (BBO, Options, Shortable Shares)
    tickers[sym] = ib.reqMktData(contract, '100,104,236,292', snapshot=False, regulatorySnapshot=False)
    tickers[sym].updateEvent += create_news_callback(sym, news_csv, tickers[sym])
    seed_historical_news(sym, contract, news_provider_codes, news_csv)

    # 2. 5-Second Bars
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='1800 S',
        barSizeSetting='5 secs', whatToShow='TRADES', useRTH=False, keepUpToDate=True
    )
    bars.updateEvent += create_bar_callback(sym, bar_csv, tickers[sym])
    bars_dict[sym] = bars

    # 3. Live Trade Stream (tick-by-tick when capacity allows, else mktData fallback)
    if tbt_subscriptions < MAX_TICK_BY_TICK_STREAMS:
        live_ticks = ib.reqTickByTickData(contract, 'AllLast')
        live_ticks.updateEvent += create_tick_callback(sym, tick_csv, tickers[sym])
        ticks_dict[sym] = live_ticks
        tbt_subscriptions += 1
    else:
        tickers[sym].updateEvent += create_mktdata_tick_callback(sym, tick_csv, tickers[sym])
        ticks_dict[sym] = tickers[sym]
        print(f"[!] {sym}: using mktData fallback (tick-by-tick cap reached: {MAX_TICK_BY_TICK_STREAMS})")

print("\n[+] Harvester fully armed. Streaming all symbols concurrently...")
try:
    ib.run()
except KeyboardInterrupt:
    print("\n[*] Harvester stopped.")