#!/usr/bin/env bash
set -euo pipefail
cd /Users/filmonghezehey/trading-agent/worktrees/databento

echo '--- per chunk latest ---'
for log in training_data/databento_30s_20260523_build_chunks/chunk_*/build.log; do
  [[ -f "$log" ]] || continue
  chunk=$(/usr/bin/basename "$(/usr/bin/dirname "$log")")
  latest=$(/usr/bin/grep -E '^\[[0-9]+/213\] Processing ' "$log" | /usr/bin/tail -1 || true)
  echo "$chunk: $latest"
done

echo '--- outputs ---'
for path in \
  training_data/databento_30s_20260523_combined.csv \
  training_data/databento_5s_20260523_combined.csv \
  training_data/databento_1s_20260523_combined.csv \
  training_data/databento_30s_20260523/symbol_model_plan.csv; do
  if [[ -e "$path" ]]; then
    /usr/bin/du -sh "$path"
  else
    echo "MISSING $path"
  fi
done

echo '--- disk ---'
/bin/df -h /Users/filmonghezehey/trading-agent/worktrees/databento

echo '--- processes ---'
/usr/bin/pgrep -af 'recover_20260523_pipeline|run_20260523_pipeline_after_bars|run_parallel_databento_build_20260523|build_30s_from_5s_csv.py|caffeinate -dimsu -w' || true

echo '--- recovery heartbeat ---'
latest_rec=$(/bin/ls -td runtime/logs/20260523_pipeline_recovery/* 2>/dev/null | /usr/bin/head -1 || true)
echo "recovery_log=$latest_rec"
[[ -n "$latest_rec" ]] && /usr/bin/tail -5 "$latest_rec/recovery.log" || true

