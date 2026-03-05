from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.ndax_client import NdaxError
from qtbot.strategy.engine import StrategyEngine
from qtbot.strategy.indicators import IndicatorSnapshot
from qtbot.strategy.signals import PositionSnapshot
from tests._helpers import make_runtime_config


class _FakeDecisionLogger:
    def __init__(self) -> None:
        self.rows = []

    def append_many(self, decisions) -> None:
        self.rows.extend(decisions)


class _FakeStateStore:
    def __init__(self, positions: dict[str, PositionSnapshot] | None = None) -> None:
        self._positions = positions or {}

    def get_positions(self):
        return self._positions


class _FakeNdaxClient:
    def __init__(self, instruments):
        self._instruments = instruments

    def get_instruments(self):
        return self._instruments

    def get_recent_ticker_history(self, **kwargs):
        raise AssertionError("get_recent_ticker_history should be mocked in this test")


class StrategyEngineTests(unittest.TestCase):
    def test_raises_when_no_tradable_universe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            engine = StrategyEngine(
                config=cfg,
                ndax_client=_FakeNdaxClient([]),
                state_store=_FakeStateStore(),
                decision_logger=_FakeDecisionLogger(),
            )
            with self.assertRaises(NdaxError):
                engine.evaluate_cycle(now_utc=datetime.now(timezone.utc))

    def test_entry_cap_applies_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), max_new_entries_per_cycle=2)
            instruments = [
                {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 1},
                {"Product1Symbol": "ADA", "Product2Symbol": "CAD", "Symbol": "ADACAD", "InstrumentId": 2},
                {"Product1Symbol": "DOGE", "Product2Symbol": "CAD", "Symbol": "DOGECAD", "InstrumentId": 3},
                {"Product1Symbol": "AVAX", "Product2Symbol": "CAD", "Symbol": "AVAXCAD", "InstrumentId": 4},
            ]
            logger = _FakeDecisionLogger()
            engine = StrategyEngine(
                config=cfg,
                ndax_client=_FakeNdaxClient(instruments),
                state_store=_FakeStateStore(),
                decision_logger=logger,
            )

            indicators = {
                "SOL": IndicatorSnapshot(close=100, ema_fast=110, ema_slow=90, atr=1),   # score 0.2
                "ADA": IndicatorSnapshot(close=100, ema_fast=108, ema_slow=90, atr=1),   # score 0.18
                "DOGE": IndicatorSnapshot(close=100, ema_fast=106, ema_slow=90, atr=1),  # score 0.16
                "AVAX": IndicatorSnapshot(close=100, ema_fast=104, ema_slow=90, atr=1),  # score 0.14
            }

            with mock.patch.object(engine, "_load_indicator", side_effect=lambda entry, lookback_hours: indicators[entry.ticker]):
                summary = engine.evaluate_cycle(now_utc=datetime(2026, 3, 5, tzinfo=timezone.utc))

            self.assertEqual(summary.enter_count, 2)
            self.assertEqual(summary.hold_count, 2)
            self.assertEqual(summary.exit_count, 0)
            self.assertEqual(len(summary.decisions), 4)
            reasons = {d.symbol: d.reason for d in summary.decisions}
            self.assertEqual(reasons["DOGECAD"], "entry_cap_reached")
            self.assertEqual(reasons["AVAXCAD"], "entry_cap_reached")
            self.assertEqual(len(logger.rows), 4)

    def test_generates_exit_for_open_position(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), max_new_entries_per_cycle=2)
            instruments = [
                {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 1},
            ]
            logger = _FakeDecisionLogger()
            positions = {
                "SOL": PositionSnapshot(
                    symbol="SOL",
                    qty=1.0,
                    avg_entry_price=100.0,
                    entry_time="2026-03-03T00:00:00+00:00",
                    last_exit_time=None,
                )
            }
            engine = StrategyEngine(
                config=cfg,
                ndax_client=_FakeNdaxClient(instruments),
                state_store=_FakeStateStore(positions),
                decision_logger=logger,
            )
            with mock.patch.object(
                engine,
                "_load_indicator",
                return_value=IndicatorSnapshot(close=100.0, ema_fast=90.0, ema_slow=95.0, atr=2.0),
            ):
                summary = engine.evaluate_cycle(now_utc=datetime(2026, 3, 5, tzinfo=timezone.utc))
            self.assertEqual(summary.exit_count, 1)
            self.assertEqual(summary.decisions[0].reason, "trend_break")


if __name__ == "__main__":
    unittest.main()
