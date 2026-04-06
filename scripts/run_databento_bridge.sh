#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
bridge_root="$repo_root/databento_ibkr_bridge"

cd "$bridge_root"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python3 -m pip install --upgrade pip >/dev/null
python3 -m pip install -r requirements.txt >/dev/null

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "[RUN] Created databento_ibkr_bridge/.env from template"
fi

python3 src/databento_ibkr_bridge/bridge_runner.py --dry-run

