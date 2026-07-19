#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent
RUN_DIR = BASE / 'run'
LATEST_ENV = RUN_DIR / 'latest_run.env'
SLICE_MANIFEST = BASE / 'databento-20260427-20260522-core5-4week-daily-prevclose.manifest.json'
OUT_JSON = BASE / 'four_week_results_summary.json'
OUT_MD = BASE / 'four_week_results_summary.md'
SYMBOLS = ['TSLA', 'TQQQ', 'NVDA', 'SPY', 'QQQ']


def read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        if '=' in raw:
            key, value = raw.split('=', 1)
            out[key] = value
    return out


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def to_float(raw: Any, default: float = 0.0) -> float:
    try:
        value = float(raw)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def to_int(raw: Any, default: int = 0) -> int:
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def latest(pattern: str) -> Path | None:
    files = sorted(RUN_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def symbol_from_path(path: Path) -> str:
    return path.name.split('-', 1)[0].upper()


def parse_console_log(log_path: Path) -> dict[str, Any]:
    current_symbol = ''
    console: dict[str, dict[str, Any]] = defaultdict(dict)
    previous_close_counts: Counter[str] = Counter()
    previous_close_values: dict[str, dict[str, float]] = defaultdict(dict)
    counters = Counter()
    first_markers: dict[str, str] = {}
    symbol_pattern = re.compile(r'\[BACKTEST\] symbol=([A-Z0-9]+)\b')
    prev_pattern = re.compile(r'PREVIOUS_CLOSE_AVAILABLE=PASS \| symbol=([A-Z0-9]+) previousClose=([0-9.]+) sessionDate=([0-9-]+)')
    int_patterns = {
        'equity_bars': re.compile(r'Equity bars: ([-0-9]+)'),
        'option_bars': re.compile(r'Option bars: ([-0-9]+)'),
        'skipped_events': re.compile(r'Skipped events: ([-0-9]+)'),
        'final_position': re.compile(r'Final position: ([-0-9]+)'),
        'broker_position': re.compile(r'Simulated broker position: ([-0-9]+)'),
        'total_trades': re.compile(r'Total trades: ([-0-9]+)'),
        'current_session_trade_counter': re.compile(r'Current session trade counter: ([-0-9]+)'),
    }
    float_patterns = {'total_pnl': re.compile(r'Total PnL: ([-0-9.Ee]+)')}
    lifecycle_patterns = {
        'arms_total': re.compile(r'arms_total: ([-0-9]+)'),
        'arms_long': re.compile(r'arms_long: ([-0-9]+)'),
        'arms_short': re.compile(r'arms_short: ([-0-9]+)'),
        'arm_confirmations': re.compile(r'arm_confirmations: ([-0-9]+)'),
        'arm_expirations': re.compile(r'arm_expirations: ([-0-9]+)'),
        'guard_evaluations': re.compile(r'guard_evaluations: ([-0-9]+)'),
        'guard_fires': re.compile(r'guard_fires: ([-0-9]+)'),
        'lifecycle_exits': re.compile(r'lifecycle_exits: ([-0-9]+)'),
        'hard_risk_exits': re.compile(r'hard_risk_exits: ([-0-9]+)'),
        'eod_exits': re.compile(r'eod_exits: ([-0-9]+)'),
    }
    no_trade_diag = re.compile(r'BACKTEST\.NO_TRADE_DIAG\] (.*)$')
    if not log_path.is_file():
        return {'missing_log': str(log_path)}
    with log_path.open('r', encoding='utf-8', errors='replace') as handle:
        for line in handle:
            if 'FEATURE_COUNT_SUPPORTED=PASS' in line:
                counters['feature_count_supported_pass'] += 1
            if 'FEATURE_COUNT_SUPPORTED=FAIL' in line:
                counters['feature_count_supported_fail'] += 1
            if 'FEATURE_COUNT_MATCH=FAIL' in line:
                counters['feature_count_match_fail'] += 1
            if 'Loaded setup_manifest.json' in line:
                counters['setup_manifest_loaded'] += 1
                first_markers.setdefault('setup_manifest_loaded', line.strip())
            if 'Validated lifecycle/micro route manifest' in line:
                counters['lifecycle_route_manifest_validated'] += 1
                first_markers.setdefault('lifecycle_route_manifest_validated', line.strip())
            if 'setup_thresholds_file=' in line:
                counters['setup_thresholds_file_logged'] += 1
                first_markers.setdefault('setup_thresholds_file', line.strip())
            if 'thresholds base{' in line:
                counters['strategy_threshold_config_logged'] += 1
                first_markers.setdefault('threshold_config', line.strip())
            if '[FLOW][ERROR]' in line or '[BACKTEST][ERROR]' in line:
                counters['error_lines'] += 1
                first_markers.setdefault('first_error_line', line.strip())
            if 'Exception' in line:
                counters['exception_lines'] += 1
                first_markers.setdefault('first_exception_line', line.strip())
            if 'completed=5 failed=0 requested=5' in line:
                counters['wrapper_completed_5_failed_0'] += 1
            if 'BACKTEST_RC=0' in line:
                counters['backtest_rc_0'] += 1

            match = symbol_pattern.search(line)
            if match:
                current_symbol = match.group(1)
                console[current_symbol]['start_line'] = line.strip()
                continue
            prev_match = prev_pattern.search(line)
            if prev_match:
                symbol, value, day = prev_match.groups()
                previous_close_counts[symbol] += 1
                previous_close_values[symbol][day] = to_float(value)
                continue
            if not current_symbol:
                continue
            for key, pattern in int_patterns.items():
                match = pattern.search(line)
                if match:
                    console[current_symbol][key] = to_int(match.group(1))
            for key, pattern in float_patterns.items():
                match = pattern.search(line)
                if match:
                    console[current_symbol][key] = to_float(match.group(1))
            for key, pattern in lifecycle_patterns.items():
                match = pattern.search(line)
                if match:
                    console[current_symbol][key] = to_int(match.group(1))
            diag_match = no_trade_diag.search(line)
            if diag_match:
                console[current_symbol].setdefault('no_trade_diag', []).append(diag_match.group(1).strip())
    return {
        'counters': dict(counters),
        'first_markers': first_markers,
        'previous_close_counts': dict(previous_close_counts),
        'previous_close_values': previous_close_values,
        'symbol_console_summary': console,
    }


def parse_artifacts() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for symbol in SYMBOLS:
        entry: dict[str, Any] = {}
        sanity_path = latest(f'{symbol}-*-stream-sanity.json')
        lifecycle_path = latest(f'{symbol}-*-trade-lifecycle-summary.csv')
        orders_path = latest(f'{symbol}-*-orders.csv')
        trades_path = latest(f'{symbol}-*-trades.csv')
        if sanity_path and sanity_path.is_file():
            entry['stream_sanity_file'] = str(sanity_path)
            entry['stream_sanity'] = json.loads(sanity_path.read_text())
        if lifecycle_path and lifecycle_path.is_file():
            rows = read_csv_rows(lifecycle_path)
            entry['lifecycle_file'] = str(lifecycle_path)
            entry['lifecycle_rows'] = len(rows)
            closed_rows = [row for row in rows if row.get('ClosedTradeIndex', '').strip()]
            metrics_row = rows[0] if rows else {}
            metric_keys = [
                'ArmsTotal', 'ArmsLong', 'ArmsShort', 'ArmConfirmations', 'ArmExpirations', 'ArmConversionRate',
                'GuardEvaluations', 'GuardFires', 'LifecycleExits', 'HardRiskExits', 'EodExits', 'AvgSetupToFillSeconds',
                'AvgMfeR', 'AvgMaeR', 'ExitReasonDistribution'
            ]
            entry['lifecycle_metrics'] = {key: metrics_row.get(key, '') for key in metric_keys}
            entry['closed_trades'] = len(closed_rows)
            entry['closed_trade_rows'] = closed_rows
            entry['trade_pnl_sum'] = sum(to_float(row.get('TradePnL')) for row in closed_rows)
        orders = read_csv_rows(orders_path) if orders_path else []
        trades = read_csv_rows(trades_path) if trades_path else []
        if orders_path:
            entry['orders_file'] = str(orders_path)
        if trades_path:
            entry['trades_file'] = str(trades_path)
        entry['order_rows'] = len(orders)
        entry['filled_order_rows'] = sum(1 for row in orders if row.get('EventType') == 'FILLED')
        entry['trade_log_rows'] = len(trades)
        entry['trade_log_pnl_sum'] = sum(to_float(row.get('TradePnL')) for row in trades)
        out[symbol] = entry
    return out


def summarize_day_dominance(artifacts: dict[str, Any]) -> dict[str, Any]:
    pnl_by_day: Counter[str] = Counter()
    trades_by_day: Counter[str] = Counter()
    pnl_by_symbol: Counter[str] = Counter()
    for symbol, entry in artifacts.items():
        for row in entry.get('closed_trade_rows', []):
            day = (row.get('EntryTime') or row.get('ExitTime') or 'unknown')[:10]
            pnl = to_float(row.get('TradePnL'))
            trades_by_day[day] += 1
            pnl_by_day[day] += pnl
            pnl_by_symbol[symbol] += pnl
    total_abs_day_pnl = sum(abs(value) for value in pnl_by_day.values())
    total_trades = sum(trades_by_day.values())
    return {
        'total_closed_trades': total_trades,
        'total_pnl': sum(pnl_by_symbol.values()),
        'pnl_by_day': dict(sorted(pnl_by_day.items())),
        'trades_by_day': dict(sorted(trades_by_day.items())),
        'pnl_by_symbol': dict(sorted(pnl_by_symbol.items())),
        'max_abs_day_pnl_fraction': max((abs(v) for v in pnl_by_day.values()), default=0.0) / total_abs_day_pnl if total_abs_day_pnl else 0.0,
        'max_trade_day_fraction': max(trades_by_day.values(), default=0) / total_trades if total_trades else 0.0,
    }


def pct(num: float, den: float) -> float:
    return (num / den * 100.0) if den else 0.0


def markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    for row in rows:
        lines.append('| ' + ' | '.join(str(item) for item in row) + ' |')
    return lines


def write_markdown(summary: dict[str, Any]) -> None:
    artifacts = summary['artifacts']
    lines = [
        '# Four-week Databento latest-model replay summary',
        '',
        f"Run log: `{summary['env'].get('RUN_LOG', '')}`",
        f"Recorded events: `{summary['env'].get('RECORDED_EVENTS', '')}`",
        f"Setup dir: `{summary['env'].get('SETUP_DIR', '')}`",
        f"Lifecycle/micro dir: `{summary['env'].get('LIFECYCLE_DIR', '')}`",
        '',
        '## Slice validation',
        '',
    ]
    manifest = summary['slice_manifest']
    lines.extend(markdown_table(
        ['Metric', 'Value'],
        [
            ['output_events', manifest.get('output_events')],
            ['equity_bar', manifest.get('counts', {}).get('equity_bar')],
            ['option_bar', manifest.get('counts', {}).get('option_bar')],
            ['previous_close', manifest.get('counts', {}).get('previous_close')],
            ['day_count', manifest.get('day_count')],
            ['missing_previous_close', manifest.get('missing_previous_close')],
            ['malformed', manifest.get('malformed')],
        ]
    ))
    lines += ['', '## Per-symbol stream/data and decision output', '']
    rows = []
    for symbol in SYMBOLS:
        entry = artifacts.get(symbol, {})
        sanity = entry.get('stream_sanity', {})
        rows.append([
            symbol,
            sanity.get('processedEquityBars', 0),
            sanity.get('processedOptionBars', 0),
            sanity.get('skippedEvents', 0),
            sanity.get('lowQualityEquityBars', 0),
            f"{pct(to_float(sanity.get('lowQualityEquityBars')), to_float(sanity.get('processedEquityBars'))):.4f}%",
            entry.get('closed_trades', 0),
            entry.get('order_rows', 0),
            f"{entry.get('trade_pnl_sum', 0.0):.2f}",
            entry.get('lifecycle_metrics', {}).get('ArmsTotal', ''),
            entry.get('lifecycle_metrics', {}).get('ArmConfirmations', ''),
            entry.get('lifecycle_metrics', {}).get('ArmConversionRate', ''),
        ])
    lines.extend(markdown_table(['Symbol', 'Equity bars', 'Option bars', 'Skipped', 'Low quality', 'Low-quality %', 'Closed trades', 'Order rows', 'PnL', 'Arms', 'Confirmations', 'Conversion'], rows))
    lines += ['', '## Closed trades', '']
    trade_rows = []
    for symbol in SYMBOLS:
        for row in artifacts.get(symbol, {}).get('closed_trade_rows', []):
            trade_rows.append([
                symbol,
                row.get('TradeSide'),
                row.get('EntryTime'),
                row.get('ExitTime'),
                row.get('Quantity'),
                row.get('EntryPrice'),
                row.get('ExitPrice'),
                row.get('TradePnL'),
                row.get('RealizedR'),
                row.get('ExitReason'),
                row.get('EntryProb'),
                row.get('EntryThreshold'),
            ])
    lines.extend(markdown_table(['Symbol', 'Side', 'Entry', 'Exit', 'Qty', 'EntryPx', 'ExitPx', 'PnL', 'R', 'Exit', 'Prob', 'Threshold'], trade_rows or [['<none>', '', '', '', '', '', '', '', '', '', '', '']]))
    lines += ['', '## Log validation', '']
    log_validation = summary['log_validation']
    lines.append('```json')
    lines.append(json.dumps(log_validation.get('counters', {}), indent=2, sort_keys=True))
    lines.append('```')
    lines += ['', 'Previous-close event counts from Java log:', '']
    lines.extend(markdown_table(['Symbol', 'Previous-close PASS count'], [[s, log_validation.get('previous_close_counts', {}).get(s, 0)] for s in SYMBOLS]))
    lines += ['', '## Day dominance', '']
    lines.append('```json')
    lines.append(json.dumps(summary['day_dominance'], indent=2, sort_keys=True))
    lines.append('```')
    OUT_MD.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    env = read_env(LATEST_ENV)
    log_path = Path(env.get('RUN_LOG', ''))
    manifest = json.loads(SLICE_MANIFEST.read_text())
    artifacts = parse_artifacts()
    summary = {
        'env': env,
        'slice_manifest': manifest,
        'artifacts': artifacts,
        'day_dominance': summarize_day_dominance(artifacts),
        'log_validation': parse_console_log(log_path),
    }
    OUT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    write_markdown(summary)
    print(f'SUMMARY_JSON={OUT_JSON}')
    print(f'SUMMARY_MD={OUT_MD}')
    print(json.dumps({
        'total_closed_trades': summary['day_dominance']['total_closed_trades'],
        'total_pnl': summary['day_dominance']['total_pnl'],
        'symbols': {symbol: {
            'closed_trades': artifacts[symbol].get('closed_trades', 0),
            'pnl': artifacts[symbol].get('trade_pnl_sum', 0.0),
            'arms': artifacts[symbol].get('lifecycle_metrics', {}).get('ArmsTotal', ''),
            'confirmations': artifacts[symbol].get('lifecycle_metrics', {}).get('ArmConfirmations', ''),
        } for symbol in SYMBOLS},
        'log_counters': summary['log_validation'].get('counters', {}),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

