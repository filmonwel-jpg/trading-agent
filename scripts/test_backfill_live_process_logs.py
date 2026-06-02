#!/usr/bin/env python3
from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backfill_live_process_logs import extract_rows_for_day


SAMPLE_LOG = """  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \\ \\ \\
( ( )\\___  '_  '_  '_ \\/ _` | \\ \\ \\
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\\__, | / / / /
 =========|_|==============|___/=/_/_/_/

 :: Spring Boot ::               (v3.5.11)
2026-04-14T09:30:00.123-04:00  INFO 123 --- [main] app : Starting TradingAgentApplication
2026-04-14T09:30:01.000-04:00  INFO 123 --- [main] app : First timestamped line
continuation without timestamp
2026-04-14T09:31:00.000-04:00  INFO 123 --- [main] app : Another line
2026-04-15T09:30:00.123-04:00  INFO 999 --- [main] app : Starting TradingAgentApplication
2026-04-15T09:30:01.000-04:00  INFO 999 --- [main] app : Next day line
"""


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_root = Path(tmp_dir)
        runtime_dir = repo_root / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        log_path = runtime_dir / "AMD_live_trade_logs.txt"
        log_path.write_text(SAMPLE_LOG, encoding="utf-8")

        extraction = extract_rows_for_day(
            repo_root=repo_root,
            file_path=log_path,
            target_day=date(2026, 4, 14),
            source="run_symbol.sh",
        )

        assert extraction.symbol == "AMD"
        assert extraction.source_file == "runtime/AMD_live_trade_logs.txt"
        assert len(extraction.rows) == 12, extraction.rows
        assert extraction.rows[0].log_line.startswith("  .   ____")
        assert extraction.rows[0].run_id == "AMD_20260414_093000"
        assert extraction.rows[7].log_line.startswith(" :: Spring Boot ::")
        assert extraction.rows[8].log_line.startswith("2026-04-14T09:30:00.123-04:00")
        assert extraction.rows[9].log_line.startswith("2026-04-14T09:30:01.000-04:00")
        assert extraction.rows[10].log_line == "continuation without timestamp"
        assert extraction.rows[11].log_line.startswith("2026-04-14T09:31:00.000-04:00")
        assert all(row.log_ts.isoformat().startswith("2026-04-14") for row in extraction.rows)
        assert not any("2026-04-15" in row.log_line for row in extraction.rows)

    print("backfill-live-process-logs-parser-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



