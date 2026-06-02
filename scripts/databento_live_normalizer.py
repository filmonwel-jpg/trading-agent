#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import databento as db
import databento_dbn as dbn

PRICE_SCALE = 1_000_000_000.0
OPRA_SYMBOL_RE = re.compile(r'^([A-Z]+)\s+(\d{6,8})([CP])\d+$')
SNAPSHOT_UNSUPPORTED_EQUITY_DATASETS = {'EQUS.MINI'}
STREAM_FAILURE_EXIT_CODE = 3
VALID_PARENT_SUFFIXES = {'FUT', 'OPT', 'SPOT'}
DEFAULT_STARTUP_HISTORY_SECONDS = 12 * 30
DEFAULT_STARTUP_HISTORY_SCHEMA = 'ohlcv-1s'
DEFAULT_EQUITY_FLUSH_LAG_MS = 250.0
DEFAULT_EQUITY_FLUSH_POLL_SECONDS = 0.10
AVAILABLE_UP_TO_RE = re.compile(r"available up to '([^']+)'", flags=re.IGNORECASE)
AVAILABLE_END_RE = re.compile(r"available end[^\n]*\('([^']+)'\)", flags=re.IGNORECASE)


def _normalize_option_parent(value: str) -> str:
    token = str(value or '').strip().upper()
    if not token:
        return ''
    if '.' not in token:
        return f'{token}.OPT'
    root, suffix = token.rsplit('.', 1)
    root = root.strip().upper()
    suffix = suffix.strip().upper()
    if root and suffix in VALID_PARENT_SUFFIXES:
        return f'{root}.{suffix}'
    return token


def _normalize_option_parents(values: str | Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in _split_csv(values):
        token = _normalize_option_parent(raw)
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


@dataclass
class EquitySecondBar:
    epoch_sec: int
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0
    px_x_sz: float = 0.0
    trade_count: int = 0
    quote_count: int = 0
    bid: float = 0.0
    ask: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    at_bid_vol: int = 0
    at_ask_vol: int = 0
    saw_trade: bool = False

    def update_trade(self, price: float, size: int, side_text: str) -> None:
        if price <= 0.0:
            return
        if not self.saw_trade:
            self.open = self.high = self.low = self.close = price
            self.saw_trade = True
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
            self.close = price
        if size > 0:
            self.volume += size
            self.trade_count += 1
            self.px_x_sz += price * size
            side_letter = (side_text or '').strip().upper()[:1]
            if side_letter == 'B':
                self.at_bid_vol += size
            elif side_letter in {'A', 'S'}:
                self.at_ask_vol += size

    def update_quote(self, bid: float, ask: float, bid_size: int, ask_size: int) -> None:
        if bid > 0.0:
            self.bid = bid
        if ask > 0.0:
            self.ask = ask
        if bid_size >= 0:
            self.bid_size = bid_size
        if ask_size >= 0:
            self.ask_size = ask_size
        if bid > 0.0 or ask > 0.0:
            self.quote_count += 1
        if not self.saw_trade:
            fallback = ask if ask > 0.0 else bid
            if fallback > 0.0:
                self.open = self.high = self.low = self.close = fallback

    def to_payload(self, symbol: str) -> dict:
        wap = (self.px_x_sz / self.volume) if self.volume > 0 else self.close
        return {
            'event': 'equity_bar',
            'symbol': symbol,
            'barEpochSec': int(self.epoch_sec),
            'open': round(self.open, 6),
            'high': round(self.high, 6),
            'low': round(self.low, 6),
            'close': round(self.close, 6),
            'wap': round(wap, 6),
            'volume': int(self.volume),
            'tradeCount': int(self.trade_count),
            'quoteCount': int(self.quote_count),
            'bid': round(self.bid, 6),
            'ask': round(self.ask, 6),
            'bidSize': int(self.bid_size),
            'askSize': int(self.ask_size),
            'atBidVol': int(self.at_bid_vol),
            'atAskVol': int(self.at_ask_vol),
        }


class JsonWriter:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def emit(self, payload: dict) -> None:
        with self._lock:
            sys.stdout.write(json.dumps(payload, separators=(',', ':')) + '\n')
            sys.stdout.flush()


class DatabentoNormalizer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.args.option_parents = _normalize_option_parents(getattr(self.args, 'option_parents', []) or getattr(self.args, 'symbols', []))
        self.writer = JsonWriter()
        self.stop_event = threading.Event()
        self.instrument_to_symbol: dict[int, str] = {}
        self.instrument_to_option_meta: dict[int, tuple[str, str]] = {}
        self.clients: list[db.Live] = []
        self.clients_lock = threading.Lock()
        self.failure_lock = threading.Lock()
        self.failure_exit_code = 0
        self.failure_reason = ''

    def _live_gateway(self) -> str:
        return str(getattr(self.args, 'live_gateway', '') or '').strip()

    def _create_live_client(self, api_key: str) -> db.Live:
        kwargs: dict[str, object] = {
            'key': api_key,
            'reconnect_policy': 'reconnect',
        }
        live_gateway = self._live_gateway()
        if live_gateway:
            kwargs['gateway'] = live_gateway
        return db.Live(**kwargs)

    def _equity_flush_lag_seconds(self) -> float:
        lag_ms = max(0.0, float(getattr(self.args, 'equity_flush_lag_ms', DEFAULT_EQUITY_FLUSH_LAG_MS) or 0.0))
        return lag_ms / 1000.0

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc).replace(microsecond=0)

    def _flush_equity_bars(
        self,
        bars: dict[str, EquitySecondBar],
        bars_lock: threading.Lock,
        *,
        force: bool = False,
        wall_clock: float | None = None,
    ) -> int:
        ready: list[tuple[str, EquitySecondBar]] = []
        with bars_lock:
            if force:
                ready = sorted(bars.items(), key=lambda item: (item[1].epoch_sec, item[0]))
                bars.clear()
            else:
                now_ts = time.time() if wall_clock is None else wall_clock
                cutoff_epoch = int(now_ts - self._equity_flush_lag_seconds())
                if cutoff_epoch <= 0:
                    return 0
                flushable = [
                    (symbol, bar)
                    for symbol, bar in bars.items()
                    if bar.epoch_sec < cutoff_epoch
                ]
                if not flushable:
                    return 0
                ready = sorted(flushable, key=lambda item: (item[1].epoch_sec, item[0]))
                for symbol, _bar in ready:
                    bars.pop(symbol, None)

        for symbol, bar in ready:
            self.writer.emit(bar.to_payload(symbol))
        return len(ready)

    def run(self) -> int:
        if self.args.dry_run:
            self.writer.emit({
                'event': 'status',
                'message': 'dry-run',
                'symbols': self.args.symbols,
                'optionParents': self.args.option_parents,
                'equityDataset': self.args.equity_dataset,
                'equitySchema': self.args.equity_schema,
                'liveGateway': self._live_gateway(),
                'startupHistorySeconds': self.args.startup_history_seconds,
                'startupHistorySchema': self.args.startup_history_schema,
                'optionsDataset': self.args.options_dataset,
                'optionsSchema': self.args.options_schema,
            })
            return 0

        api_key = os.environ.get('DATABENTO_API_KEY', '').strip()
        if not api_key:
            self.writer.emit({'event': 'status', 'message': 'missing DATABENTO_API_KEY'})
            return 2

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        startup_delay_seconds = max(0.0, float(self.args.startup_delay_seconds))
        if startup_delay_seconds > 0.0:
            self.writer.emit({'event': 'status', 'message': f'startup-delay-seconds={startup_delay_seconds:.1f}'})
            if self.stop_event.wait(timeout=startup_delay_seconds):
                self.writer.emit({'event': 'status', 'message': 'startup-delay-interrupted'})
                return 0
            self.writer.emit({'event': 'status', 'message': 'startup-delay-complete'})

        self._emit_startup_equity_history(api_key)

        threads: list[threading.Thread] = []
        threads.append(threading.Thread(target=self._run_equity_stream, args=(api_key,), name='databento-equities', daemon=True))
        threads.append(threading.Thread(target=self._run_option_stream, args=(api_key,), name='databento-options', daemon=True))
        for thread in threads:
            thread.start()

        heartbeat_seconds = max(1, int(self.args.heartbeat_seconds))
        while not self.stop_event.wait(timeout=heartbeat_seconds):
            self.writer.emit({'event': 'status', 'message': f'heartbeat symbols={"|".join(self.args.symbols)}'})

        for thread in threads:
            thread.join(timeout=5.0)
        if self.failure_exit_code != 0:
            self.writer.emit({'event': 'status', 'message': f'normalizer-fatal exitCode={self.failure_exit_code} reason={self.failure_reason}'})
            return self.failure_exit_code
        return 0

    def _run_equity_stream(self, api_key: str) -> None:
        bars: dict[str, EquitySecondBar] = {}
        bars_lock = threading.Lock()
        flush_stop = threading.Event()
        client = self._create_live_client(api_key)
        self._register_client(client)
        iterator_exhausted = False

        def flush_worker() -> None:
            while not flush_stop.wait(timeout=DEFAULT_EQUITY_FLUSH_POLL_SECONDS):
                self._flush_equity_bars(bars, bars_lock, wall_clock=time.time())

        flusher = threading.Thread(target=flush_worker, name='databento-equity-flusher', daemon=True)
        flusher.start()
        try:
            client.subscribe(
                dataset=self.args.equity_dataset,
                schema=self.args.equity_schema,
                symbols=self.args.symbols,
                stype_in='raw_symbol',
                snapshot=self._equity_snapshot_enabled(),
            )
            self.writer.emit({'event': 'status', 'message': f'equity-subscribe dataset={self.args.equity_dataset} schema={self.args.equity_schema}'})
            for record in client:
                if self.stop_event.is_set():
                    break
                self._handle_symbol_mapping(record)
                if isinstance(record, dbn.ErrorMsg):
                    error_code = getattr(record, 'code', '')
                    error_text = str(getattr(record, 'err', '') or repr(record))
                    self.writer.emit({'event': 'status', 'message': f'equity-error code={error_code} err={error_text}'})
                    if not self.stop_event.is_set():
                        self._fail_stream('equity', f'error-code={error_code} err={error_text}')
                    break
                if isinstance(record, dbn.SystemMsg):
                    self.writer.emit({'event': 'status', 'message': f'equity-system code={getattr(record, "code", "")} msg={getattr(record, "msg", "")}'})
                    continue
                if not isinstance(record, (dbn.MBP1Msg, dbn.CMBP1Msg, dbn.TradeMsg, dbn.BBOMsg, dbn.CBBOMsg)):
                    continue
                symbol = self.instrument_to_symbol.get(int(getattr(record, 'instrument_id', 0) or 0), '')
                if not symbol:
                    continue
                epoch_sec = int(getattr(record, 'ts_event', 0) or 0) // 1_000_000_000
                if epoch_sec <= 0:
                    continue
                ready_bar = None
                with bars_lock:
                    current = bars.get(symbol)
                    if current is not None and epoch_sec != current.epoch_sec:
                        ready_bar = current
                        bars.pop(symbol, None)
                        current = None
                    if current is None:
                        current = EquitySecondBar(epoch_sec=epoch_sec)
                        bars[symbol] = current
                    bid, ask, bid_size, ask_size = self._extract_quote(record)
                    current.update_quote(bid, ask, bid_size, ask_size)
                    trade_price = self._normalize_price(getattr(record, 'price', 0))
                    trade_size = int(getattr(record, 'size', 0) or 0)
                    current.update_trade(trade_price, trade_size, str(getattr(record, 'side', '') or ''))

                if ready_bar is not None:
                    self.writer.emit(ready_bar.to_payload(symbol))

                # Timer-based flushing does not synthesize silent seconds; it only releases already-seen bars
                # once their second is definitely closed.
                self._flush_equity_bars(bars, bars_lock, wall_clock=time.time())
            else:
                iterator_exhausted = True
        except Exception as exc:  # pragma: no cover - live path
            self.writer.emit({'event': 'status', 'message': f'equity-stream-error {exc}'})
            if not self.stop_event.is_set():
                self._fail_stream('equity', f'exception={exc}')
        finally:
            flush_stop.set()
            flusher.join(timeout=1.0)
            self._flush_equity_bars(bars, bars_lock, force=True)
            self._terminate_client(client)
        if iterator_exhausted and not self.stop_event.is_set():
            self._fail_stream('equity', 'stream-ended')

    def _emit_startup_equity_history(self, api_key: str) -> None:
        history_seconds = max(0, int(round(float(getattr(self.args, 'startup_history_seconds', 0.0) or 0.0))))
        if history_seconds <= 0 or self.stop_event.is_set():
            return

        history_schema = str(getattr(self.args, 'startup_history_schema', DEFAULT_STARTUP_HISTORY_SCHEMA) or DEFAULT_STARTUP_HISTORY_SCHEMA).strip()
        end_time = self._utc_now()
        start_time = end_time - timedelta(seconds=history_seconds)
        self.writer.emit({
            'event': 'status',
            'message': (
                f'startup-history-begin dataset={self.args.equity_dataset} schema={history_schema} '
                f'start={start_time.isoformat()} end={end_time.isoformat()} symbols={"|".join(self.args.symbols)}'
            ),
        })

        emitted_total = 0
        try:
            client = db.Historical(api_key)
        except Exception as exc:
            self.writer.emit({'event': 'status', 'message': f'startup-history-skip reason=historical-client-init-failed error={exc}'})
            return

        for symbol in self.args.symbols:
            if self.stop_event.is_set():
                break
            try:
                current_start = start_time
                current_end = end_time
                attempted_windows: set[tuple[datetime, datetime]] = set()
                while True:
                    window_key = (current_start, current_end)
                    if window_key in attempted_windows:
                        raise RuntimeError(
                            f'startup-history-window-loop symbol={symbol} '
                            f'start={current_start.isoformat()} end={current_end.isoformat()}'
                        )
                    attempted_windows.add(window_key)
                    try:
                        data = client.timeseries.get_range(
                            dataset=self.args.equity_dataset,
                            schema=history_schema,
                            stype_in='raw_symbol',
                            symbols=[symbol],
                            start=current_start,
                            end=current_end,
                        )
                        break
                    except Exception as exc:
                        adjusted_window = self._adjust_history_window_from_exception(
                            symbol,
                            history_seconds,
                            current_start,
                            current_end,
                            exc,
                        )
                        if adjusted_window is None:
                            raise
                        current_start, current_end = adjusted_window
                frame = data.to_df().reset_index(drop=False)
                records = self._frame_records(frame)
                emitted_for_symbol = 0
                for row in records:
                    payload = self._historical_row_to_payload(symbol, row)
                    if payload is None:
                        continue
                    self.writer.emit(payload)
                    emitted_for_symbol += 1
                emitted_total += emitted_for_symbol
                self.writer.emit({'event': 'status', 'message': f'startup-history-symbol symbol={symbol} emittedBars={emitted_for_symbol}'})
            except Exception as exc:
                self.writer.emit({'event': 'status', 'message': f'startup-history-error symbol={symbol} error={exc}'})

        self.writer.emit({'event': 'status', 'message': f'startup-history-complete emittedBars={emitted_total}'})

    def _adjust_history_window_from_exception(
        self,
        symbol: str,
        history_seconds: int,
        current_start: datetime,
        current_end: datetime,
        exc: Exception,
    ) -> tuple[datetime, datetime] | None:
        if not self._is_data_after_available_end(exc):
            return None
        available_end = self._extract_available_end(exc)
        if available_end is None:
            return None
        adjusted_end = available_end.replace(microsecond=0)
        if adjusted_end.tzinfo is None:
            adjusted_end = adjusted_end.replace(tzinfo=timezone.utc)
        else:
            adjusted_end = adjusted_end.astimezone(timezone.utc)
        if adjusted_end >= current_end:
            return None
        adjusted_start = adjusted_end - timedelta(seconds=max(0, history_seconds))
        self.writer.emit({
            'event': 'status',
            'message': (
                f'startup-history-clamped symbol={symbol} '
                f'start={adjusted_start.isoformat()} end={adjusted_end.isoformat()} '
                f'availableEnd={adjusted_end.isoformat()}'
            ),
        })
        return adjusted_start, adjusted_end

    def _is_data_after_available_end(self, exc: Exception) -> bool:
        text = str(exc).lower()
        return 'data_start_after_available_end' in text or 'data_end_after_available_end' in text

    def _extract_available_end(self, exc: Exception) -> datetime | None:
        message = str(exc)
        candidates: list[str] = []
        up_to = AVAILABLE_UP_TO_RE.search(message)
        if up_to:
            candidates.append(up_to.group(1))
        available_end = AVAILABLE_END_RE.search(message)
        if available_end:
            candidates.append(available_end.group(1))
        for raw in candidates:
            try:
                parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None

    def _frame_records(self, frame: object) -> list[dict]:
        if frame is None:
            return []
        if bool(getattr(frame, 'empty', False)):
            return []
        ordered = frame
        if hasattr(frame, 'sort_values'):
            try:
                ordered = frame.sort_values(by=['symbol', 'ts_event'])
            except Exception:
                try:
                    ordered = frame.sort_values(by=['ts_event'])
                except Exception:
                    ordered = frame
        if hasattr(ordered, 'to_dict'):
            try:
                rows = ordered.to_dict(orient='records')
            except TypeError:
                rows = ordered.to_dict()
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []

    def _historical_row_to_payload(self, requested_symbol: str, row: dict) -> dict | None:
        epoch_sec = self._coerce_epoch_seconds(row.get('ts_event'))
        if epoch_sec <= 0:
            return None
        symbol = str(row.get('symbol') or requested_symbol or '').strip().upper()
        if not symbol:
            return None
        open_px = self._safe_float(row.get('open'))
        high_px = self._safe_float(row.get('high'))
        low_px = self._safe_float(row.get('low'))
        close_px = self._safe_float(row.get('close'))
        if close_px <= 0.0:
            return None
        volume = max(0, int(round(self._safe_float(row.get('volume')))))
        wap = self._safe_float(row.get('vwap'))
        return {
            'event': 'equity_bar',
            'symbol': symbol,
            'barEpochSec': epoch_sec,
            'open': round(open_px if open_px > 0.0 else close_px, 6),
            'high': round(high_px if high_px > 0.0 else close_px, 6),
            'low': round(low_px if low_px > 0.0 else close_px, 6),
            'close': round(close_px, 6),
            'wap': round(wap if wap > 0.0 else close_px, 6),
            'volume': volume,
            'tradeCount': 0,
            'quoteCount': 0,
            'bid': 0.0,
            'ask': 0.0,
            'bidSize': 0,
            'askSize': 0,
            'atBidVol': 0,
            'atAskVol': 0,
            'historical': True,
        }

    def _safe_float(self, raw_value: object) -> float:
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return 0.0

    def _coerce_epoch_seconds(self, raw_value: object) -> int:
        if raw_value is None:
            return 0
        if hasattr(raw_value, 'to_pydatetime'):
            try:
                raw_value = raw_value.to_pydatetime()
            except Exception:
                pass
        if isinstance(raw_value, datetime):
            dt = raw_value if raw_value.tzinfo is not None else raw_value.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        text = str(raw_value).strip()
        if not text:
            return 0
        try:
            return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp())
        except ValueError:
            pass
        try:
            numeric = float(text)
        except ValueError:
            return 0
        if numeric > 1_000_000_000_000:
            numeric /= 1_000_000_000.0
        elif numeric > 1_000_000_000_0:
            numeric /= 1_000_000.0
        return int(numeric)

    def _run_option_stream(self, api_key: str) -> None:
        client = self._create_live_client(api_key)
        self._register_client(client)
        iterator_exhausted = False
        try:
            client.subscribe(
                dataset=self.args.options_dataset,
                schema=self.args.options_schema,
                symbols=self.args.option_parents,
                stype_in='parent',
                snapshot=False,
            )
            self.writer.emit({'event': 'status', 'message': f'options-subscribe dataset={self.args.options_dataset} schema={self.args.options_schema}'})
            for record in client:
                if self.stop_event.is_set():
                    break
                self._handle_symbol_mapping(record)
                if isinstance(record, dbn.ErrorMsg):
                    error_code = getattr(record, 'code', '')
                    error_text = str(getattr(record, 'err', '') or repr(record))
                    self.writer.emit({'event': 'status', 'message': f'options-error code={error_code} err={error_text}'})
                    if not self.stop_event.is_set():
                        self._fail_stream('options', f'error-code={error_code} err={error_text}')
                    break
                if isinstance(record, dbn.SystemMsg):
                    self.writer.emit({'event': 'status', 'message': f'options-system code={getattr(record, "code", "")} msg={getattr(record, "msg", "")}'})
                    continue
                if not isinstance(record, dbn.OHLCVMsg):
                    continue
                option_meta = self.instrument_to_option_meta.get(int(getattr(record, 'instrument_id', 0) or 0))
                if not option_meta:
                    continue
                underlying, right = option_meta
                epoch_sec = int(getattr(record, 'ts_event', 0) or 0) // 1_000_000_000
                volume = int(getattr(record, 'volume', 0) or 0)
                if epoch_sec <= 0 or volume <= 0:
                    continue
                self.writer.emit({
                    'event': 'option_bar',
                    'underlying': underlying,
                    'right': right,
                    'barEpochSec': epoch_sec,
                    'volume': volume,
                })
            else:
                iterator_exhausted = True
        except Exception as exc:  # pragma: no cover - live path
            self.writer.emit({'event': 'status', 'message': f'options-stream-error {exc}'})
            if not self.stop_event.is_set():
                self._fail_stream('options', f'exception={exc}')
        finally:
            self._terminate_client(client)
        if iterator_exhausted and not self.stop_event.is_set():
            self._fail_stream('options', 'stream-ended')

    def _handle_symbol_mapping(self, record: object) -> None:
        if not isinstance(record, dbn.SymbolMappingMsg):
            return
        instrument_id = int(getattr(record, 'instrument_id', 0) or 0)
        if instrument_id <= 0:
            return
        out_symbol = str(getattr(record, 'stype_out_symbol', '') or '').strip().upper()
        in_symbol = str(getattr(record, 'stype_in_symbol', '') or '').strip().upper()
        resolved_symbol = out_symbol or in_symbol
        if not resolved_symbol:
            return
        self.instrument_to_symbol[instrument_id] = resolved_symbol
        option_meta = self._extract_option_meta(resolved_symbol)
        if option_meta is not None:
            self.instrument_to_option_meta[instrument_id] = option_meta
        self.writer.emit({'event': 'status', 'message': f'symbol-map instrumentId={instrument_id} symbol={resolved_symbol}'})

    def _extract_option_meta(self, raw_symbol: str) -> tuple[str, str] | None:
        match = OPRA_SYMBOL_RE.match(str(raw_symbol or '').strip().upper())
        if not match:
            return None
        return match.group(1), match.group(3)

    def _extract_quote(self, record: object) -> tuple[float, float, int, int]:
        levels = getattr(record, 'levels', None)
        if not levels:
            return 0.0, 0.0, 0, 0
        level = levels[0]
        bid = self._normalize_price(getattr(level, 'bid_px', 0))
        ask = self._normalize_price(getattr(level, 'ask_px', 0))
        bid_size = int(getattr(level, 'bid_sz', 0) or 0)
        ask_size = int(getattr(level, 'ask_sz', 0) or 0)
        return bid, ask, bid_size, ask_size

    def _normalize_price(self, raw_value: object) -> float:
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return 0.0
        if value <= 0.0:
            return 0.0
        if abs(value) >= 1_000_000.0:
            return value / PRICE_SCALE
        return value

    def _equity_snapshot_enabled(self) -> bool:
        return str(self.args.equity_dataset or '').strip().upper() not in SNAPSHOT_UNSUPPORTED_EQUITY_DATASETS

    def _register_client(self, client: db.Live) -> None:
        with self.clients_lock:
            self.clients.append(client)

    def _fail_stream(self, stream_name: str, reason: str) -> None:
        with self.failure_lock:
            if self.failure_exit_code != 0:
                return
            self.failure_exit_code = STREAM_FAILURE_EXIT_CODE
            self.failure_reason = f'{stream_name}:{reason}'
        self.writer.emit({'event': 'status', 'message': f'{stream_name}-stream-fatal {reason}'})
        self.stop_event.set()
        self._terminate_all_clients()


    def _terminate_all_clients(self) -> None:
        with self.clients_lock:
            clients = list(self.clients)
        for client in clients:
            self._terminate_client(client)

    def _terminate_client(self, client: db.Live) -> None:
        try:
            client.terminate()
        except Exception:
            pass

    def _handle_signal(self, *_args: object) -> None:
        self.stop_event.set()
        self._terminate_all_clients()


def _split_csv(values: str | Iterable[str]) -> list[str]:
    if isinstance(values, str):
        raw_parts = values.split(',')
    else:
        raw_parts = list(values)
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_parts:
        token = str(raw or '').strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Normalize live Databento TBBO + OPRA OHLCV-1s into NDJSON for the Java trader.')
    parser.add_argument('--symbols', type=str, required=True, help='Comma-separated equity symbols, e.g. TSLA,NVDA')
    parser.add_argument('--option-parents', type=str, default='', help='Comma-separated OPRA parent symbols. Defaults to --symbols.')
    parser.add_argument('--live-gateway', type=str, default=os.environ.get('DATABENTO_LIVE_GATEWAY', ''), help='Optional Databento live gateway hostname override passed to databento.Live(gateway=...).')
    parser.add_argument('--equity-dataset', type=str, default='EQUS.MINI')
    parser.add_argument('--equity-schema', type=str, default='tbbo')
    parser.add_argument('--startup-history-seconds', type=float, default=float(DEFAULT_STARTUP_HISTORY_SECONDS))
    parser.add_argument('--startup-history-schema', type=str, default=DEFAULT_STARTUP_HISTORY_SCHEMA)
    parser.add_argument('--options-dataset', type=str, default='OPRA.PILLAR')
    parser.add_argument('--options-schema', type=str, default='ohlcv-1s')
    parser.add_argument('--equity-flush-lag-ms', type=float, default=float(DEFAULT_EQUITY_FLUSH_LAG_MS))
    parser.add_argument('--heartbeat-seconds', type=int, default=15)
    parser.add_argument('--startup-delay-seconds', type=float, default=0.0)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    args.symbols = _split_csv(args.symbols)
    args.option_parents = _normalize_option_parents(args.option_parents) if args.option_parents else _normalize_option_parents(args.symbols)
    if not args.symbols:
        raise SystemExit('No symbols supplied.')
    if not args.option_parents:
        args.option_parents = _normalize_option_parents(args.symbols)
    return args



def main() -> int:
    args = parse_args()
    return DatabentoNormalizer(args).run()


if __name__ == '__main__':
    raise SystemExit(main())

