from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from qtbot.decision_log import DecisionCsvLogger
from qtbot.strategy.signals import Decision
from qtbot.trade_log import TradeCsvLogger, TradeFillRecord


class CsvLogTests(unittest.TestCase):
    def test_decision_csv_logger_writes_header_once(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "decisions.csv"
            logger = DecisionCsvLogger(csv_path)
            decision = Decision(
                timestamp_utc="2026-03-05T00:00:00+00:00",
                symbol="SOLCAD",
                close=100.0,
                ema_fast=101.0,
                ema_slow=99.0,
                atr=2.0,
                signal="HOLD",
                reason="test",
            )
            logger.append_many([decision])
            logger.append_many([decision])
            lines = csv_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0], "timestamp,symbol,close,ema_fast,ema_slow,atr,signal,reason")
            self.assertEqual(len(lines), 3)

    def test_trade_csv_logger_writes_fill_record(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "trades.csv"
            logger = TradeCsvLogger(csv_path)
            fill = TradeFillRecord(
                timestamp_utc="2026-03-05T00:00:00+00:00",
                symbol="SOLCAD",
                side="BUY",
                qty=1.25,
                avg_price=100.0,
                fee_cad=0.5,
                order_id=1234,
            )
            logger.append(fill)
            lines = csv_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0], "timestamp,symbol,side,qty,avg_price,notional_cad,fee_cad,order_id")
            self.assertIn("SOLCAD", lines[1])
            self.assertIn("1234", lines[1])


if __name__ == "__main__":
    unittest.main()
