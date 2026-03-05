from __future__ import annotations

import logging
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.ndax_client import NdaxBalance
from qtbot.reconciliation import StartupReconciler
from qtbot.state import StateStore
from tests._helpers import make_runtime_config


class _FakeNdaxClient:
    def __init__(self, *, instruments, balances, candles_by_instrument=None) -> None:
        self._instruments = instruments
        self._balances = balances
        self._candles_by_instrument = candles_by_instrument or {}

    def get_instruments(self):
        return self._instruments

    def fetch_balances(self, *, credentials):
        del credentials
        return 999, self._balances

    def get_recent_ticker_history(self, *, instrument_id, interval_seconds, lookback_hours):
        del interval_seconds, lookback_hours
        return self._candles_by_instrument.get(instrument_id, [])


class ReconciliationTests(unittest.TestCase):
    def _logger(self) -> logging.Logger:
        logger = logging.getLogger("qtbot-test-reconcile")
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    def test_reconcile_updates_positions_and_caps_cash(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=True)
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            store.apply_buy_fill(
                symbol="SOL",
                qty=1.0,
                avg_price=100.0,
                fee_cad=0.4,
                filled_at_utc="2026-03-05T00:00:00+00:00",
                order_id=1,
                ndax_symbol="SOLCAD",
            )
            # Stale internal position that no longer exists on exchange.
            store.reconcile_position(
                symbol="DOGE",
                ndax_qty=1.0,
                reference_price=10.0,
                reconciled_at_utc="2026-03-05T00:01:00+00:00",
                reason="seed",
            )

            client = _FakeNdaxClient(
                instruments=[
                    {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 99},
                    {"Product1Symbol": "DOGE", "Product2Symbol": "CAD", "Symbol": "DOGECAD", "InstrumentId": 77},
                ],
                balances=[
                    NdaxBalance(product_symbol="CAD", amount=25.0, hold=0.0),
                    NdaxBalance(product_symbol="SOL", amount=2.0, hold=0.0),
                    NdaxBalance(product_symbol="DOGE", amount=0.0, hold=0.0),
                ],
            )
            reconciler = StartupReconciler(
                config=cfg,
                ndax_client=client,
                state_store=store,
                logger=self._logger(),
            )

            with mock.patch("qtbot.reconciliation.load_credentials_from_env", return_value=mock.Mock()):
                summary = reconciler.reconcile()

            self.assertEqual(summary.account_id, 999)
            self.assertEqual(summary.changed_symbols, 2)
            self.assertTrue(summary.capped_bot_cash)
            snapshot = store.get_snapshot()
            assert snapshot is not None
            self.assertEqual(float(snapshot["bot_cash_cad"]), 25.0)
            positions = store.get_positions()
            self.assertEqual(positions["SOL"].qty, 2.0)
            self.assertEqual(positions["DOGE"].qty, 0.0)

    def test_reconcile_uses_reference_price_for_new_position(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=True)
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            client = _FakeNdaxClient(
                instruments=[
                    {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 99},
                ],
                balances=[
                    NdaxBalance(product_symbol="CAD", amount=100.0, hold=0.0),
                    NdaxBalance(product_symbol="SOL", amount=1.5, hold=0.0),
                ],
                candles_by_instrument={
                    99: [
                        [1, 0, 0, 0, 111.0, 0],
                        [2, 0, 0, 0, 123.45, 0],
                    ]
                },
            )
            reconciler = StartupReconciler(
                config=cfg,
                ndax_client=client,
                state_store=store,
                logger=self._logger(),
            )
            with mock.patch("qtbot.reconciliation.load_credentials_from_env", return_value=mock.Mock()):
                reconciler.reconcile()
            pos = store.get_positions()["SOL"]
            self.assertEqual(pos.qty, 1.5)
            self.assertAlmostEqual(pos.avg_entry_price, 123.45)


if __name__ == "__main__":
    unittest.main()
