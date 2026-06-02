# Monday Runbook

This runbook is for starting the live symbol bots with the symbol-specific runtime property files and filesystem-backed ONNX bundles already prepared under `runtime/models/<SYMBOL>`.

## 1. Pre-open checklist

Before launching any symbol process:

- Confirm TWS / IB Gateway is up and reachable on the configured host and port.
- Optional auto-launch preflight: copy `runtime/ibkr-launch.env.example` to `runtime/ibkr-launch.env` and set either `IBKR_APP_PATH`, `IBKR_APP_NAME`, or `IBKR_LAUNCH_CMD`.
- Optional Keychain launcher: copy `runtime/ibkr-keychain.env.example` to `runtime/ibkr-keychain.env`, run `./set_ibkr_keychain_credentials.sh`, then set `IBKR_LAUNCH_CMD='/Users/filmonghezehey/trading-agent/ibkr_keychain_launch.sh'` in `runtime/ibkr-launch.env`.
- Optional phone confirmations: copy `runtime/notifications.env.example` to `runtime/notifications.env` and set either `STACK_NOTIFY_NTFY_URL` or `STACK_NOTIFY_WEBHOOK_URL`.
- Confirm the packaged jar exists:
  - `target/trading-agent-0.0.1-SNAPSHOT.jar`
- Confirm runtime secrets exist:
  - `runtime/postgres-local.properties`
- Confirm the symbol property files exist:
  - `runtime/trading-tsla.properties`
  - `runtime/trading-nvda.properties`
  - `runtime/trading-amd.properties`
- Confirm the runtime model bundles exist:
  - `runtime/models/TSLA`
  - `runtime/models/NVDA`
  - `runtime/models/AMD`
- Confirm the shared capital file exists:
  - `runtime/shared-capital.properties`
- Confirm each symbol folder contains 21 `.onnx` files.
- Confirm the target ports are not already occupied:
  - TSLA -> `8081`
  - NVDA -> `8082`
  - AMD -> `8083`

## 2. Fast preflight preview

You can test the new IBKR morning preflight manually before the open:

```bash
cp runtime/ibkr-launch.env.example runtime/ibkr-launch.env
./ensure_ibkr_workstation.sh --status-only
./ensure_ibkr_workstation.sh
```

Notes:
- The script can auto-launch TWS / IB Gateway, but it does **not** hardcode IBKR usernames/passwords.
- The local Keychain wrapper can prefill credentials for TWS / Gateway, but macOS Accessibility permission is required and IBKR 2FA may still need manual approval.
- If you need fully unattended login, point `IBKR_LAUNCH_CMD` at a secure external launcher (for example IBC or your own Keychain-backed wrapper).
- The afternoon shutdown now includes an IBKR quit step after all bots are flat and the harvester is stopped.
- Startup and shutdown scripts can also send a simple confirmation to your phone or another LAN endpoint via `./send_stack_notification.sh`.
- If the preflight fails during the weekday schedule, the launchd job will retry on the next minute inside the `07:00`–`07:04` MT window.

Use the launcher in preview mode first. This prints the resolved config and exact `java -jar` command without starting the app.

The preview also shows the effective per-symbol AI thresholds that will be passed to the process.

```bash
./run_symbol.sh TSLA
./run_symbol.sh NVDA
./run_symbol.sh AMD
```

If shared capital ever looks stale before the open, inspect and clear it with:

```bash
./reset_shared_capital.sh --check
./reset_shared_capital.sh --reset
```

Use `--force` only if you have already confirmed manually that all bots are flat and the lock file is stale.

## 3. Safe startup verification

If you want a startup-only verification that blocks new entries, start with `--max-trades=0`.

```bash
./run_symbol.sh TSLA --start --max-trades=0
./run_symbol.sh NVDA --start --max-trades=0
./run_symbol.sh AMD --start --max-trades=0
```

Then verify:

```bash
curl -fsS http://127.0.0.1:8081/actuator/health
curl -fsS http://127.0.0.1:8082/actuator/health
curl -fsS http://127.0.0.1:8083/actuator/health
```

And:

```bash
curl -fsS http://127.0.0.1:8081/api/control/status
curl -fsS http://127.0.0.1:8082/api/control/status
curl -fsS http://127.0.0.1:8083/api/control/status
```

Expected minimum signals:
- health returns `{"status":"UP"}`
- control status returns the correct symbol and `connected=true`
- log file shows `Loading model from filesystem path=runtime/models/<SYMBOL>/...`
- control status includes `sharedCapitalEnabled=true`

## Shared capital behavior

The current live runtime files are configured with:

```properties
trading.shared-capital.enabled=true
trading.shared-capital.file=runtime/shared-capital.properties
trading.shared-capital.total-notional=70000
```

That means the symbol bots coordinate through one shared reservation file.

Practical effect:
- when one symbol opens an entry that consumes the shared budget, the other symbol bots must wait
- exit orders are still allowed so positions can always be closed
- once the active position is closed and the reservation is released, other symbols can enter again

Manual reset options:
- HTTP endpoint on a running bot: `POST /api/control/shared-capital/reset`
- force variant: `POST /api/control/shared-capital/reset/true`
- operator helper: `./reset_shared_capital.sh --reset`

## 4. Live launch commands

If the safe startup check is clean, run live launches.

### TSLA

```bash
./run_symbol.sh TSLA --start --tee
```

### NVDA

```bash
./run_symbol.sh NVDA --start --tee
```

### AMD

```bash
./run_symbol.sh AMD --start --tee
```

Default tee files:
- `runtime/tsla_live_trade_logs.txt`
- `runtime/nvda_live_trade_logs.txt`
- `runtime/amd_live_trade_logs.txt`

You can also override a runtime property at launch time:

```bash
./run_symbol.sh TSLA --start --tee -- --trading.risk.max-order-notional=100000
```

## 5. Log and state locations

Per-symbol runtime files currently point to:

- TSLA
  - app log: `runtime/trading-agent-TSLA.log`
  - trade log: `runtime/trades-TSLA.csv`
  - state file: `runtime/trader-state-TSLA.properties`
- NVDA
  - app log: `runtime/trading-agent-NVDA.log`
  - trade log: `runtime/trades-NVDA.csv`
  - state file: `runtime/trader-state-NVDA.properties`
- AMD
  - app log: `runtime/trading-agent-AMD.log`
  - trade log: `runtime/trades-AMD.csv`
  - state file: `runtime/trader-state-AMD.properties`

## 5b. Live process log DB recovery

Use this when the mirrored live log files exist under `runtime/*_live_trade_logs.txt`, but the PostgreSQL live-process tables are missing rows for the prior session.

Typical trigger:

- `python3 check_morning_stack.py` reports missing or incomplete live-process rows for one or more symbols
- the text files such as `runtime/tsla_live_trade_logs.txt` or `runtime/qqq_live_trade_logs.txt` still contain the missed session output

Default one-command recovery (targets yesterday automatically):

```bash
python3 scripts/backfill_live_process_logs.py
```

Useful variants:

```bash
python3 scripts/backfill_live_process_logs.py --day 2026-04-14
python3 scripts/backfill_live_process_logs.py --day 2026-04-14 --symbols=TSLA,NVDA
python3 scripts/backfill_live_process_logs.py --day 2026-04-14 --dry-run
```

What it does:

- scans `runtime/*_live_trade_logs.txt`
- reconstructs `run_id` values from each process startup
- inserts rows into per-symbol live-process tables such as `tsla_databento_live_process_logs` with the original `log_ts`
- skips already completed files and resumes partially imported files safely

### One-time migration from the old shared table

If you already have historical rows in the old shared live-process table and want to split them into the new per-symbol tables, run this one-time migration:

```bash
python3 scripts/migrate_shared_live_process_logs.py --dry-run
python3 scripts/migrate_shared_live_process_logs.py
```

Useful variants:

```bash
python3 scripts/migrate_shared_live_process_logs.py --symbols=TSLA,NVDA
```

What it does:

- reads from `databento_live_process_logs`
- writes into per-symbol tables such as `tsla_databento_live_process_logs`
- skips rows already present in the destination table, so it is safe to rerun

Quick verification after recovery:

```bash
python3 check_morning_stack.py
python3 scripts/backfill_live_process_logs.py --day 2026-04-14 --symbols=QQQ
```

Expected result:

- `check_morning_stack.py` no longer reports missing live-process DB rows for the recovered symbols/day
- a repeat backfill run reports `SKIP-EXISTING` for already completed files

Prerequisites:

- run from the repo root
- `runtime/postgres-local.properties` is present
- the target `runtime/*_live_trade_logs.txt` files still exist

Prevention note:

- for future live sessions, launch with DB tee enabled so logs are persisted immediately
- single symbol: `./run_symbol.sh TSLA --start --tee --tee-db`
- bulk launcher: `./start_all_databento_bots.sh --start --tee --tee-db`

## 6. Abort conditions

Do **not** continue to live launch if any of these occur:

- preview reports missing jar, missing property file, or missing model bundle
- any symbol runtime folder has fewer than 21 ONNX files
- port conflict on `8081`, `8082`, or `8083`
- health endpoint does not return `UP`
- control status does not show the expected symbol
- logs do not show filesystem-backed model loading
- repeated IBKR connection or order-routing errors at boot

## 6b. Afternoon shutdown behavior

The scheduled afternoon flatten script now runs in this order:

1. pause/cancel/flatten each symbol bot
2. confirm positions are flat and open orders are zero
3. stop each symbol bot
4. stop `harvester.py`
5. quit TWS / IB Gateway

If any symbol does **not** confirm flat in time, the script leaves IBKR running so you can inspect and flatten manually.

### Manual shutdown sequence

If the scheduled shutdown ever stalls, use this manual sequence for each live bot port:

```bash
curl -fsS http://127.0.0.1:8081/api/control/status
curl -fsS -X POST http://127.0.0.1:8081/api/control/pause
curl -fsS -X POST http://127.0.0.1:8081/api/control/cancel-open-orders
curl -fsS http://127.0.0.1:8081/api/control/status
curl -fsS -X POST http://127.0.0.1:8081/api/control/kill-switch/true
```

If `currentPosition` is non-zero, call flatten before the kill-switch step:

```bash
curl -fsS -X POST http://127.0.0.1:8081/api/control/flatten
```

After all bots show `currentPosition=0` and `openOrders=0`, stop the stack:

```bash
pkill -f '/Users/filmonghezehey/trading-agent/harvester.py'
./ensure_ibkr_workstation.sh --shutdown
```

## 7. Fresh model promotion before the session

If you retrain before the open, promote the latest bundle explicitly before launch.

```bash
python3 promote_onnx_bundle.py --latest --symbol TSLA --clean
python3 promote_onnx_bundle.py --latest --symbol NVDA --clean
python3 promote_onnx_bundle.py --latest --symbol AMD --clean
```

Or use a known run tag:

```bash
python3 promote_onnx_bundle.py --run-tag 20260320_192113 --symbol NVDA --clean
python3 promote_onnx_bundle.py --run-tag 20260320_192304 --symbol AMD --clean
```

## 8. Notes from the latest verification

Verified in this workspace:
- `NVDA` startup passed with the freshly trained bundle in `runtime/models/NVDA`
- `AMD` startup passed with the freshly trained bundle in `runtime/models/AMD`
- both served health and control endpoints successfully
- both logged filesystem-backed ONNX loading from their symbol-specific runtime model folders




