from __future__ import annotations

import gzip
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "analyze_live_no_trade_day.py"
SPEC = importlib.util.spec_from_file_location("analyze_live_no_trade_day", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
analyzer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analyzer
SPEC.loader.exec_module(analyzer)


def write_log(log_dir: Path, symbol: str, lines: list[str]) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"trading-agent-{symbol}.log"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class AnalyzeLiveNoTradeDayTest(unittest.TestCase):
    def test_gateway_executions_request_ids_do_not_count_as_order_activity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_dir = tmp_path / "logs"
            trade_dir = tmp_path / "output"
            write_log(
                log_dir,
                "NVDA",
                [
                    "2026-08-03T13:59:45.568-06:00  INFO 50973 --- [trading-agent] [shared-ibkr-gateway-reader] c.c.fili.trader.bot.trader.IBKRTrader    : >>> [FLOW][INFO][IBKR.GATEWAY] eventType=symbol_registered detail=strategy_id=NVDA:client-266 request_ids={'positions': 210000000, 'open_orders': 210000001, 'executions': 210000002, 'account_updates': 210000003}",
                    "2026-08-03T13:59:45.581-06:00  INFO 50973 --- [trading-agent] [shared-ibkr-gateway-reader] c.c.fili.trader.bot.trader.IBKRTrader    : >>> [FLOW][DATA][RISK] Shared capital reconcile symbol=NVDA position=0 referencePrice=0.0 fallbackNotional=60000.0 allowed=true message=already-released available=300000.0",
                    "2026-08-03T13:59:45.595-06:00  INFO 50973 --- [trading-agent] [pool-4-thread-1] c.c.fili.trader.bot.trader.IBKRTrader    : >>> [FLOW][INFO][SCHEDULE] EOD flatten workflow completed date=2026-08-03 cancelledOpenOrders=0 flattenResult=already-flat positionSyncComplete=true",
                ],
            )

            summary = analyzer.analyze_symbol(log_dir, trade_dir, "NVDA", "2026-08-03")
            result = analyzer.build_result([summary], "2026-08-03", log_dir, trade_dir)

            self.assertEqual(analyzer.real_order_activity_count(summary.counts), 0)
            self.assertEqual(summary.counts["strategy_order_fill_positive"], 0)
            self.assertEqual(summary.counts["strategy_order_fill_analyze"], 0)
            self.assertEqual(summary.counts["order_send"], 0)
            self.assertEqual(result["verdict"], "NO_TRADE_NO_TODAY_ACTIVITY_SEEN")

    def test_strict_order_and_fill_markers_count_as_real_activity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_dir = tmp_path / "logs"
            trade_dir = tmp_path / "output"
            write_log(
                log_dir,
                "TSLA",
                [
                    "2026-08-03T10:01:00.000-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy : >>> [FLOW][INFO][AI.LONG.ENTRY] Dip buyer firing order symbol=TSLA rsi=35.00",
                    "2026-08-03T10:01:00.001-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.fili.trader.bot.trader.IBKRTrader : >>> [FLOW][DATA][RISK] Shared capital reserved symbol=TSLA orderId=101 requested=59999.0 availableAfter=240001.0",
                    "2026-08-03T10:01:00.002-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.fili.trader.bot.trader.IBKRTrader : >>> [FLOW][DATA][ORDER.SEND] sharedGateway orderId=101 action=BUY type=FAST_LMT requestedPrice=322.23 executionReferencePrice=322.23",
                    "2026-08-03T10:01:00.003-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy : >>> [FLOW][DATA][STRATEGY.ORDER] submitted orderId=101 action=BUY qty=186 symbol=TSLA",
                    "2026-08-03T10:01:01.000-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy : >>> [FLOW][COND][STRATEGY.ORDER] FILLED_DELTA_POSITIVE=PASS | orderId=101 filledDelta=186 remaining=0 avgFillPrice=322.25",
                    "2026-08-03T10:01:01.001-06:00  INFO 1 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy : >>> [FLOW][ANALYZE][STRATEGY.ORDER] fill orderId=101 action=BUY newPos=186 netPnL=0.00 dailyPnL=0.00",
                ],
            )

            summary = analyzer.analyze_symbol(log_dir, trade_dir, "TSLA", "2026-08-03")
            result = analyzer.build_result([summary], "2026-08-03", log_dir, trade_dir)

            self.assertEqual(summary.counts["order_send"], 1)
            self.assertEqual(summary.counts["shared_capital_reserved"], 1)
            self.assertEqual(summary.counts["strategy_order_submitted"], 1)
            self.assertEqual(summary.counts["strategy_order_fill_positive"], 1)
            self.assertEqual(summary.counts["strategy_order_fill_analyze"], 1)
            self.assertEqual(analyzer.real_order_activity_count(summary.counts), 5)
            self.assertEqual(result["verdict"], "TRADES_OR_REAL_ORDER_ACTIVITY_DETECTED")

    def test_trade_csv_rows_are_scoped_to_requested_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_dir = tmp_path / "logs"
            trade_dir = tmp_path / "output"
            trade_dir.mkdir(parents=True, exist_ok=True)
            (trade_dir / "trades-NVDA.csv").write_text(
                "Timestamp,Symbol,Action,Quantity,EntryPrice,ExitPrice,TradePnL,CumulativePnL\n"
                "20260802 15:59:00 America/New_York,NVDA,SELL,10,100.0,101.0,10.0,10.0\n"
                "20260803 10:00:00 America/New_York,NVDA,SELL,10,100.0,101.0,10.0,20.0\n"
                "2026-08-03T11:00:00-04:00,NVDA,SELL,10,100.0,101.0,10.0,30.0\n",
                encoding="utf-8",
            )

            summary = analyzer.analyze_symbol(log_dir, trade_dir, "NVDA", "2026-08-03")
            result = analyzer.build_result([summary], "2026-08-03", log_dir, trade_dir)

            self.assertEqual(summary.counts["trade_csv_rows"], 2)
            self.assertEqual(summary.counts["trade_csv_rows_total"], 3)
            self.assertEqual(summary.counts["trade_csv_rows_other_dates"], 1)
            self.assertEqual(analyzer.real_order_activity_count(summary.counts), 2)
            self.assertEqual(result["verdict"], "TRADES_OR_REAL_ORDER_ACTIVITY_DETECTED")

    def test_old_trade_csv_rows_do_not_create_today_trade_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_dir = tmp_path / "logs"
            trade_dir = tmp_path / "output"
            trade_dir.mkdir(parents=True, exist_ok=True)
            (trade_dir / "trades-QQQ.csv").write_text(
                "Timestamp,Symbol,Action,Quantity,EntryPrice,ExitPrice,TradePnL,CumulativePnL\n"
                "20260802 15:59:00 America/New_York,QQQ,SELL,10,100.0,101.0,10.0,10.0\n",
                encoding="utf-8",
            )

            summary = analyzer.analyze_symbol(log_dir, trade_dir, "QQQ", "2026-08-03")
            result = analyzer.build_result([summary], "2026-08-03", log_dir, trade_dir)

            self.assertEqual(summary.counts["trade_csv_rows"], 0)
            self.assertEqual(summary.counts["trade_csv_rows_total"], 1)
            self.assertEqual(summary.counts["trade_csv_rows_other_dates"], 1)
            self.assertEqual(analyzer.real_order_activity_count(summary.counts), 0)
            self.assertEqual(result["verdict"], "NO_TRADE_NO_TODAY_ACTIVITY_SEEN")

    def test_rolled_gzip_logs_are_read_and_best_entry_margin_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_dir = tmp_path / "logs"
            trade_dir = tmp_path / "output"
            log_dir.mkdir(parents=True, exist_ok=True)
            gz_path = log_dir / "trading-agent-TSLA.log.2026-08-03.0.gz"
            lines = [
                "2026-08-03T13:47:33.324-06:00  INFO 51117 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy    : >>> [FLOW][COND][AI.LONG.ENTRY] AI_PREDICTS_ENTRY=FAIL | symbol=TSLA rsi=35.84817692248001 askOrFallback=324.2 qty=185 prob=0.6139 threshold=0.6560",
                "2026-08-03T13:47:33.324-06:00  INFO 51117 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy    : >>> [FLOW][COND][AI.SHORT.ENTRY] AI_PREDICTS_ENTRY=FAIL | symbol=TSLA rsi=35.84817692248001 bidOrFallback=322.0 qty=186 prob=0.2726 threshold=0.6440",
                "2026-08-03T13:47:33.324-06:00  INFO 51117 --- [trading-agent] [Strategy-Actor-Thread-TSLA] c.c.f.t.bot.strategy.PingPongStrategy    : >>> [FLOW][COND][AI.ENTRY.ARBITRATION] ENTRY_SIDE_SELECTED=FAIL | symbol=TSLA reason=no_passing_setup_with_positive_qty",
            ]
            with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
                handle.write("\n".join(lines) + "\n")

            summary = analyzer.analyze_symbol(log_dir, trade_dir, "TSLA", "2026-08-03")
            result = analyzer.build_result([summary], "2026-08-03", log_dir, trade_dir)

            self.assertEqual(summary.counts["ai_long_entry_fail"], 1)
            self.assertEqual(summary.counts["ai_short_entry_fail"], 1)
            self.assertEqual(summary.counts["entry_arbitration_fail"], 1)
            self.assertEqual(summary.reject_reasons["no_passing_setup_with_positive_qty"], 1)
            self.assertEqual(round(summary.best_entry_margin.margin, 4), -0.0421)
            self.assertEqual(summary.best_entry_margin.side, "long")
            self.assertEqual(result["verdict"], "NO_TRADE_MODEL_OR_ENTRY_GATES_REJECTED")


if __name__ == "__main__":
    unittest.main()


