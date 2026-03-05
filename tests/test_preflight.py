from __future__ import annotations

import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.control import Command, write_control
from qtbot.ndax_client import NdaxAuthenticationError, NdaxError
from qtbot.preflight import GoLivePreflight
from qtbot.state import StateStore
from tests._helpers import make_runtime_config


def _build_candles(count: int) -> list[list[float]]:
    return [[float(index), 10.0, 9.0, 9.5, 9.8, 100.0] for index in range(count)]


class _FakeNdaxClient:
    def __init__(
        self,
        *,
        auth_exc: Exception | None = None,
        reachability_exc: Exception | None = None,
        instruments: list[dict[str, object]] | None = None,
        candles_by_instrument: dict[int, list[list[float]]] | None = None,
        candle_exc_by_instrument: dict[int, Exception] | None = None,
    ) -> None:
        self._auth_exc = auth_exc
        self._reachability_exc = reachability_exc
        self._instruments = instruments or [
            {
                "Product1Symbol": "SOL",
                "Product2Symbol": "CAD",
                "Symbol": "SOLCAD",
                "InstrumentId": 99,
            }
        ]
        self._candles_by_instrument = candles_by_instrument or {99: _build_candles(500)}
        self._candle_exc_by_instrument = candle_exc_by_instrument or {}

    def authenticate(self, *, credentials) -> None:
        del credentials
        if self._auth_exc is not None:
            raise self._auth_exc

    def get_instruments(self) -> list[dict[str, object]]:
        if self._reachability_exc is not None:
            raise self._reachability_exc
        return self._instruments

    def get_recent_ticker_history(self, *, instrument_id: int, interval_seconds: int, lookback_hours: int):
        del interval_seconds, lookback_hours
        exc = self._candle_exc_by_instrument.get(instrument_id)
        if exc is not None:
            raise exc
        return self._candles_by_instrument.get(instrument_id, [])


class PreflightTests(unittest.TestCase):
    def _make_logger(self) -> logging.Logger:
        logger = logging.getLogger("qtbot-preflight-test")
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    def _make_env(self) -> dict[str, str]:
        return {
            "NDAX_API_KEY": "k",
            "NDAX_API_SECRET": "s",
            "NDAX_USER_ID": "123",
        }

    def test_preflight_passes_when_all_checks_are_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            write_control(
                cfg.control_file,
                Command.STOP,
                updated_by="test",
                reason="setup",
            )
            checker = GoLivePreflight(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                state_store=store,
                logger=self._make_logger(),
            )

            with mock.patch.dict(os.environ, self._make_env(), clear=True):
                summary = checker.run()

            self.assertTrue(summary.passed)
            self.assertEqual(summary.failed_checks, [])
            self.assertIn("go_live_preflight_passed", summary.message)
            self.assertTrue(summary.to_dict()["passed"])

    def test_preflight_reports_auth_control_and_warmup_failures(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            cfg.control_file.parent.mkdir(parents=True, exist_ok=True)
            cfg.control_file.write_text("{ not-json", encoding="utf-8")
            checker = GoLivePreflight(
                config=cfg,
                ndax_client=_FakeNdaxClient(
                    auth_exc=NdaxAuthenticationError("auth rejected"),
                    candles_by_instrument={99: _build_candles(5)},
                ),
                state_store=store,
                logger=self._make_logger(),
            )

            with mock.patch.dict(os.environ, self._make_env(), clear=True):
                summary = checker.run()

            self.assertFalse(summary.passed)
            failed_names = {item.name for item in summary.failed_checks}
            self.assertIn("credentials_auth", failed_names)
            self.assertIn("control_file_integrity", failed_names)
            self.assertIn("candle_warmup_sufficiency", failed_names)

    def test_preflight_flags_reachability_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            checker = GoLivePreflight(
                config=cfg,
                ndax_client=_FakeNdaxClient(reachability_exc=NdaxError("network down")),
                state_store=store,
                logger=self._make_logger(),
            )

            with mock.patch.dict(os.environ, self._make_env(), clear=True):
                summary = checker.run()

            self.assertFalse(summary.passed)
            failed_names = {item.name for item in summary.failed_checks}
            self.assertIn("ndax_api_reachability", failed_names)
            self.assertIn("cad_market_coverage", failed_names)
            self.assertIn("candle_warmup_sufficiency", failed_names)

    def test_preflight_allows_partial_warmup_when_coverage_threshold_met(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), preflight_min_warmup_coverage=0.5)
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            instruments = [
                {
                    "Product1Symbol": "SOL",
                    "Product2Symbol": "CAD",
                    "Symbol": "SOLCAD",
                    "InstrumentId": 99,
                },
                {
                    "Product1Symbol": "ADA",
                    "Product2Symbol": "CAD",
                    "Symbol": "ADACAD",
                    "InstrumentId": 100,
                },
            ]
            checker = GoLivePreflight(
                config=cfg,
                ndax_client=_FakeNdaxClient(
                    instruments=instruments,
                    candles_by_instrument={
                        99: _build_candles(500),
                        100: _build_candles(5),
                    },
                ),
                state_store=store,
                logger=self._make_logger(),
            )

            with mock.patch.dict(os.environ, self._make_env(), clear=True):
                summary = checker.run()

            self.assertTrue(summary.passed)
            warmup_check = next(item for item in summary.checks if item.name == "candle_warmup_sufficiency")
            self.assertTrue(warmup_check.passed)
            self.assertIn("coverage=0.500", warmup_check.detail)

    def test_preflight_detects_state_db_health_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            checker = GoLivePreflight(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                state_store=store,
                logger=self._make_logger(),
            )

            with mock.patch.dict(os.environ, self._make_env(), clear=True):
                summary = checker.run()

            self.assertFalse(summary.passed)
            failed_names = {item.name for item in summary.failed_checks}
            self.assertIn("state_db_health", failed_names)


if __name__ == "__main__":
    unittest.main()
