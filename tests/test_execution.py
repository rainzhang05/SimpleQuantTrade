from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.control import Command, write_control
from qtbot.execution import LiveExecutionEngine
from qtbot.ndax_client import NdaxBalance, NdaxOrderAcceptance, NdaxOrderFill
from qtbot.strategy.signals import Decision, PositionSnapshot
from qtbot.trade_log import TradeCsvLogger
from qtbot.universe import UniverseEntry
from tests._helpers import make_runtime_config


@dataclass
class _FakeStateStore:
    bot_cash_cad: float
    positions: dict[str, PositionSnapshot]
    buy_calls: list[dict]
    sell_calls: list[dict]

    def get_positions(self):
        return dict(self.positions)

    def get_bot_cash_cad(self):
        return self.bot_cash_cad

    def apply_buy_fill(self, **kwargs):
        self.buy_calls.append(kwargs)

    def apply_sell_fill(self, **kwargs):
        self.sell_calls.append(kwargs)


class _FakeNdaxClient:
    def __init__(self, balances):
        self.balances = balances
        self.send_calls = []
        self.wait_calls = []
        self.next_order_id = 100
        self.next_fill_qty = 1.0
        self.next_fill_price = 100.0

    def fetch_balances(self, *, credentials):
        return 1, self.balances

    def send_market_order(self, **kwargs):
        self.send_calls.append(kwargs)
        self.next_order_id += 1
        return NdaxOrderAcceptance(
            order_id=self.next_order_id,
            client_order_id=int(kwargs["client_order_id"]),
            instrument_id=int(kwargs["instrument_id"]),
            side=kwargs["side"],
            raw_status="Working",
        )

    def wait_for_fill(self, **kwargs):
        self.wait_calls.append(kwargs)
        return NdaxOrderFill(
            order_id=int(kwargs["order_id"]),
            qty_executed=self.next_fill_qty,
            avg_price=self.next_fill_price,
            order_state="FullyExecuted",
            raw={},
        )


class ExecutionEngineTests(unittest.TestCase):
    def _logger(self) -> logging.Logger:
        logger = logging.getLogger("qtbot-test-execution")
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    def test_dry_run_mode_short_circuits_execution(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=False)
            state_store = _FakeStateStore(1000.0, {}, [], [])
            client = _FakeNdaxClient([NdaxBalance("CAD", 500.0, 0.0)])
            engine = LiveExecutionEngine(
                config=cfg,
                ndax_client=client,
                state_store=state_store,
                trade_logger=TradeCsvLogger(cfg.runtime_dir / "logs" / "trades.csv"),
                logger=self._logger(),
            )
            summary = engine.execute_decisions(
                now_utc=datetime.now(timezone.utc),
                decisions=[],
                tradable=[],
            )
            self.assertEqual(summary.message, "execution_disabled_dry_run")
            self.assertEqual(client.send_calls, [])

    def test_live_mode_skips_entry_when_notional_below_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(
                Path(td),
                enable_live_trading=True,
                min_order_notional_cad=1000.0,
            )
            write_control(cfg.control_file, Command.RUN, updated_by="test", reason="run")
            state_store = _FakeStateStore(100.0, {}, [], [])
            client = _FakeNdaxClient([NdaxBalance("CAD", 100.0, 0.0)])
            engine = LiveExecutionEngine(
                config=cfg,
                ndax_client=client,
                state_store=state_store,
                trade_logger=TradeCsvLogger(cfg.runtime_dir / "logs" / "trades.csv"),
                logger=self._logger(),
            )
            decision = Decision(
                timestamp_utc="2026-03-05T00:00:00+00:00",
                symbol="SOLCAD",
                close=100.0,
                ema_fast=101.0,
                ema_slow=99.0,
                atr=2.0,
                signal="ENTER",
                reason="entry_conditions_met",
                score=0.02,
            )
            tradable = [UniverseEntry(ticker="SOL", ndax_symbol="SOLCAD", instrument_id=99)]

            with mock.patch("qtbot.execution.load_credentials_from_env", return_value=mock.Mock()):
                summary = engine.execute_decisions(
                    now_utc=datetime.now(timezone.utc),
                    decisions=[decision],
                    tradable=tradable,
                )
            self.assertEqual(summary.enter_filled, 0)
            self.assertEqual(summary.skipped, 1)
            self.assertEqual(client.send_calls, [])

    def test_live_mode_executes_exit_fill(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(
                Path(td),
                enable_live_trading=True,
                min_order_notional_cad=10.0,
            )
            write_control(cfg.control_file, Command.RUN, updated_by="test", reason="run")
            positions = {
                "SOL": PositionSnapshot(
                    symbol="SOL",
                    qty=1.0,
                    avg_entry_price=100.0,
                    entry_time="2026-03-04T00:00:00+00:00",
                    last_exit_time=None,
                )
            }
            state_store = _FakeStateStore(100.0, positions, [], [])
            client = _FakeNdaxClient(
                [
                    NdaxBalance("CAD", 100.0, 0.0),
                    NdaxBalance("SOL", 1.0, 0.0),
                ]
            )
            client.next_fill_qty = 1.0
            client.next_fill_price = 110.0
            engine = LiveExecutionEngine(
                config=cfg,
                ndax_client=client,
                state_store=state_store,
                trade_logger=TradeCsvLogger(cfg.runtime_dir / "logs" / "trades.csv"),
                logger=self._logger(),
            )
            decision = Decision(
                timestamp_utc="2026-03-05T00:00:00+00:00",
                symbol="SOLCAD",
                close=95.0,
                ema_fast=90.0,
                ema_slow=95.0,
                atr=2.0,
                signal="EXIT",
                reason="trend_break",
            )
            tradable = [UniverseEntry(ticker="SOL", ndax_symbol="SOLCAD", instrument_id=99)]

            with mock.patch("qtbot.execution.load_credentials_from_env", return_value=mock.Mock()):
                summary = engine.execute_decisions(
                    now_utc=datetime.now(timezone.utc),
                    decisions=[decision],
                    tradable=tradable,
                )
            self.assertEqual(summary.exit_filled, 1)
            self.assertEqual(len(client.send_calls), 1)
            self.assertEqual(len(state_store.sell_calls), 1)
            self.assertEqual(state_store.sell_calls[0]["symbol"], "SOL")

    def test_live_mode_allows_btc_and_eth_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(
                Path(td),
                enable_live_trading=True,
                min_order_notional_cad=10.0,
            )
            write_control(cfg.control_file, Command.RUN, updated_by="test", reason="run")
            state_store = _FakeStateStore(500.0, {}, [], [])
            client = _FakeNdaxClient([NdaxBalance("CAD", 500.0, 0.0)])
            client.next_fill_qty = 0.1
            client.next_fill_price = 1000.0
            engine = LiveExecutionEngine(
                config=cfg,
                ndax_client=client,
                state_store=state_store,
                trade_logger=TradeCsvLogger(cfg.runtime_dir / "logs" / "trades.csv"),
                logger=self._logger(),
            )
            decisions = [
                Decision(
                    timestamp_utc="2026-03-05T00:00:00+00:00",
                    symbol="BTCCAD",
                    close=100000.0,
                    ema_fast=100100.0,
                    ema_slow=99900.0,
                    atr=500.0,
                    signal="ENTER",
                    reason="entry_conditions_met",
                    score=0.01,
                ),
                Decision(
                    timestamp_utc="2026-03-05T00:00:00+00:00",
                    symbol="ETHCAD",
                    close=3000.0,
                    ema_fast=3010.0,
                    ema_slow=2990.0,
                    atr=25.0,
                    signal="ENTER",
                    reason="entry_conditions_met",
                    score=0.009,
                ),
            ]
            tradable = [
                UniverseEntry(ticker="BTC", ndax_symbol="BTCCAD", instrument_id=1),
                UniverseEntry(ticker="ETH", ndax_symbol="ETHCAD", instrument_id=2),
            ]

            with mock.patch("qtbot.execution.load_credentials_from_env", return_value=mock.Mock()):
                summary = engine.execute_decisions(
                    now_utc=datetime.now(timezone.utc),
                    decisions=decisions,
                    tradable=tradable,
                )

            self.assertEqual(summary.enter_filled, 2)
            self.assertEqual(len(client.send_calls), 2)
            submitted_symbols = {call["instrument_id"] for call in client.send_calls}
            self.assertIn(1, submitted_symbols)
            self.assertIn(2, submitted_symbols)

    def test_live_mode_slippage_breach_stops_further_orders(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(
                Path(td),
                enable_live_trading=True,
                min_order_notional_cad=10.0,
                max_slippage_pct=0.01,
            )
            write_control(cfg.control_file, Command.RUN, updated_by="test", reason="run")
            state_store = _FakeStateStore(300.0, {}, [], [])
            client = _FakeNdaxClient([NdaxBalance("CAD", 300.0, 0.0)])
            client.next_fill_qty = 1.0
            client.next_fill_price = 120.0
            engine = LiveExecutionEngine(
                config=cfg,
                ndax_client=client,
                state_store=state_store,
                trade_logger=TradeCsvLogger(cfg.runtime_dir / "logs" / "trades.csv"),
                logger=self._logger(),
            )
            decisions = [
                Decision(
                    timestamp_utc="2026-03-05T00:00:00+00:00",
                    symbol="SOLCAD",
                    close=100.0,
                    ema_fast=101.0,
                    ema_slow=99.0,
                    atr=2.0,
                    signal="ENTER",
                    reason="entry_conditions_met",
                    score=0.02,
                ),
                Decision(
                    timestamp_utc="2026-03-05T00:00:00+00:00",
                    symbol="ADACAD",
                    close=100.0,
                    ema_fast=101.0,
                    ema_slow=99.0,
                    atr=2.0,
                    signal="ENTER",
                    reason="entry_conditions_met",
                    score=0.01,
                ),
            ]
            tradable = [
                UniverseEntry(ticker="SOL", ndax_symbol="SOLCAD", instrument_id=99),
                UniverseEntry(ticker="ADA", ndax_symbol="ADACAD", instrument_id=100),
            ]

            with mock.patch("qtbot.execution.load_credentials_from_env", return_value=mock.Mock()):
                summary = engine.execute_decisions(
                    now_utc=datetime.now(timezone.utc),
                    decisions=decisions,
                    tradable=tradable,
                )

            self.assertEqual(summary.slippage_breaches, 1)
            self.assertGreaterEqual(summary.failed, 1)
            self.assertEqual(len(client.send_calls), 1)
            self.assertIn("slippage_breaches=1", summary.message)


if __name__ == "__main__":
    unittest.main()
