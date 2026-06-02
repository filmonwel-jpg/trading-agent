#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./reset_shared_capital.sh [--check] [--reset] [--force]

Examples:
  ./reset_shared_capital.sh --check
  ./reset_shared_capital.sh --reset
  ./reset_shared_capital.sh --reset --force

Behavior:
  - --check prints symbol ports, live status (if reachable), and the current shared-capital file contents.
  - --reset clears all reservation.* entries while preserving total.notional.
  - without --force, reset is blocked if any reachable bot reports a non-flat position, open orders, or an order in flight.
  - if at least one bot is reachable, the script uses the HTTP reset endpoint on the first reachable bot.
  - if no bots are reachable, the script edits the shared-capital file directly for pre-open recovery.
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"

check_mode=0
reset_mode=0
force_mode=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --check)
      check_mode=1
      ;;
    --reset)
      reset_mode=1
      ;;
    --force)
      force_mode=1
      ;;
    *)
      echo "[RESET][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ $check_mode -eq 0 && $reset_mode -eq 0 ]]; then
  check_mode=1
fi

python3 - <<'PY' "$repo_root" "$check_mode" "$reset_mode" "$force_mode"
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

repo_root = Path(sys.argv[1])
check_mode = bool(int(sys.argv[2]))
reset_mode = bool(int(sys.argv[3]))
force_mode = bool(int(sys.argv[4]))
runtime_dir = repo_root / 'runtime'
prop_files = sorted((runtime_dir / 'databento' / 'bots').glob('trading-*.properties'))
if not prop_files:
    prop_files = sorted(runtime_dir.glob('trading-*.properties'))

if not prop_files:
    print('[RESET][ERROR] No bot properties files found under runtime/databento/bots or runtime/.', file=sys.stderr)
    sys.exit(1)


def load_props(path: Path):
    props = {}
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        props[key.strip()] = value.strip()
    return props

symbols = []
shared_capital_file = None
for path in prop_files:
    props = load_props(path)
    symbol = (props.get('trading.symbol') or path.stem.replace('trading-', '')).upper()
    port = int(props.get('server.port', '0') or '0')
    shared_file = props.get('trading.shared-capital.file', 'runtime/shared-capital.properties')
    if shared_capital_file is None:
        candidate = Path(shared_file)
        shared_capital_file = candidate if candidate.is_absolute() else (repo_root / candidate)
    symbols.append({'symbol': symbol, 'port': port, 'path': path})

shared_capital_file = shared_capital_file or (runtime_dir / 'shared-capital.properties')

reachable = []
unsafe = []
print('[RESET] discovered symbols:')
for item in symbols:
    symbol = item['symbol']
    port = item['port']
    url = f'http://127.0.0.1:{port}/api/control/status'
    status = None
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            status = json.loads(response.read().decode('utf-8'))
        reachable.append((item, status))
        current_position = int(status.get('currentPosition', 0) or 0)
        open_orders = int(status.get('openOrders', 0) or 0)
        order_in_flight = bool(status.get('orderInFlight', False))
        shared_enabled = bool(status.get('sharedCapitalEnabled', False))
        print(
            f"  - {symbol} port={port} reachable=yes connected={status.get('connected')} "
            f"position={current_position} openOrders={open_orders} orderInFlight={order_in_flight} "
            f"sharedCapitalEnabled={shared_enabled}"
        )
        if current_position != 0 or open_orders != 0 or order_in_flight:
            unsafe.append(symbol)
    except Exception:
        print(f'  - {symbol} port={port} reachable=no')

print(f'[RESET] shared_capital_file={shared_capital_file}')
if shared_capital_file.exists():
    print('[RESET] current shared-capital file contents:')
    print(shared_capital_file.read_text(encoding='utf-8').rstrip())
else:
    print('[RESET] shared-capital file does not exist yet.')

if not reset_mode:
    sys.exit(0)

if unsafe and not force_mode:
    print(
        '[RESET][ERROR] Refusing reset because these reachable bots are not flat: ' + ', '.join(sorted(unsafe)),
        file=sys.stderr,
    )
    sys.exit(2)

if reachable:
    first_item, _ = reachable[0]
    force_segment = '/true' if force_mode else ''
    reset_url = f"http://127.0.0.1:{first_item['port']}/api/control/shared-capital/reset{force_segment}"
    req = urllib.request.Request(reset_url, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=5.0) as response:
            payload = json.loads(response.read().decode('utf-8'))
        print(f"[RESET] endpoint reset via {first_item['symbol']} port={first_item['port']}")
        print(json.dumps(payload, indent=2, sort_keys=True))
        sys.exit(0)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode('utf-8', errors='replace')
        print(f'[RESET][ERROR] Reset endpoint failed status={exc.code} body={body}', file=sys.stderr)
        sys.exit(3)
    except Exception as exc:
        print(f'[RESET][ERROR] Reset endpoint call failed: {exc}', file=sys.stderr)
        sys.exit(3)

shared_capital_file.parent.mkdir(parents=True, exist_ok=True)
props = load_props(shared_capital_file) if shared_capital_file.exists() else {}
total_notional = props.get('total.notional', '70000')
next_props = {k: v for k, v in props.items() if not k.startswith('reservation.')}
next_props['total.notional'] = total_notional

lines = [
    '# Shared capital reservation file for multi-symbol live trading.',
    '# This file is updated automatically by the running bots.',
    '# Manual reset cleared all reservation.* entries.',
]
for key in sorted(next_props):
    lines.append(f'{key}={next_props[key]}')
shared_capital_file.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('[RESET] no reachable bots; shared-capital file reset directly.')
print(shared_capital_file.read_text(encoding='utf-8').rstrip())
PY

