from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot import cli
from qtbot.control import Command
from qtbot.ndax_client import NdaxAuthenticationError, NdaxError
from qtbot.staging import StagingValidationReport, StagingValidationStep
from qtbot.state import StateStore
from tests._helpers import make_runtime_config


class _FakeClient:
    def __init__(self) -> None:
        self.instruments = [
            {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 99},
            {"Product1Symbol": "ADA", "Product2Symbol": "CAD", "Symbol": "ADACAD", "InstrumentId": 78},
        ]

    def get_instruments(self):
        return self.instruments

    def get_ticker_history(self, **kwargs):
        return [[1, 2, 3, 4, 5, 6]]

    def fetch_balances(self, **kwargs):
        from qtbot.ndax_client import NdaxBalance

        return 123, [NdaxBalance(product_symbol="CAD", amount=100.0, hold=10.0)]


def _capture_output(func, *args, **kwargs):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with mock.patch("sys.stdout", stdout), mock.patch("sys.stderr", stderr):
        code = func(*args, **kwargs)
    return code, stdout.getvalue(), stderr.getvalue()


class CliHandlerTests(unittest.TestCase):
    def test_handle_control_write_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            store.initialize(initial_budget_cad=1000.0)
            code, out, _ = _capture_output(
                cli._handle_control_write,
                config=cfg,
                command=Command.PAUSE,
                reason="pause",
            )
            self.assertEqual(code, 0)
            self.assertIn("command=PAUSE", out)

            code, out, _ = _capture_output(cli._handle_status, config=cfg)
            self.assertEqual(code, 0)
            payload = json.loads(out)
            self.assertEqual(payload["control_command"], "PAUSE")

    def test_handle_ndax_pairs_success_and_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()):
                code, out, err = _capture_output(cli._handle_ndax_pairs, config=cfg)
                self.assertEqual(code, 0)
                self.assertEqual(err, "")
                payload = json.loads(out)
                self.assertGreaterEqual(payload["tradable_count"], 1)

            with mock.patch("qtbot.cli._make_ndax_client", side_effect=NdaxError("boom")):
                code, _, err = _capture_output(cli._handle_ndax_pairs, config=cfg)
                self.assertEqual(code, 1)
                self.assertIn("NDAX pair discovery failed", err)

    def test_handle_ndax_candles(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()):
                code, out, _ = _capture_output(
                    cli._handle_ndax_candles,
                    config=cfg,
                    symbol="SOLCAD",
                    interval=60,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                )
                self.assertEqual(code, 0)
                payload = json.loads(out)
                self.assertEqual(payload["symbol"], "SOLCAD")
                self.assertEqual(payload["candle_count"], 1)

                code, _, err = _capture_output(
                    cli._handle_ndax_candles,
                    config=cfg,
                    symbol="UNKNOWN",
                    interval=60,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                )
                self.assertEqual(code, 1)
                self.assertIn("NDAX candle fetch failed", err)

    def test_handle_ndax_balances(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()), mock.patch(
                "qtbot.cli.load_credentials_from_env", return_value=mock.Mock()
            ):
                code, out, _ = _capture_output(cli._handle_ndax_balances, config=cfg)
                self.assertEqual(code, 0)
                payload = json.loads(out)
                self.assertEqual(payload["account_id"], 123)

            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()), mock.patch(
                "qtbot.cli.load_credentials_from_env",
                side_effect=NdaxAuthenticationError("auth failed"),
            ):
                code, _, err = _capture_output(cli._handle_ndax_balances, config=cfg)
                self.assertEqual(code, 1)
                self.assertIn("NDAX authentication failed", err)

    def test_handle_ndax_check(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()), mock.patch(
                "qtbot.cli.load_credentials_from_env", return_value=mock.Mock()
            ):
                code, out, _ = _capture_output(
                    cli._handle_ndax_check,
                    config=cfg,
                    symbol="SOLCAD",
                    interval=60,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                    skip_balances=False,
                    require_balances=True,
                )
                self.assertEqual(code, 0)
                payload = json.loads(out)
                self.assertFalse(payload["balance_check_skipped"])

            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()), mock.patch(
                "qtbot.cli.load_credentials_from_env",
                side_effect=NdaxAuthenticationError("bad"),
            ):
                code, out, _ = _capture_output(
                    cli._handle_ndax_check,
                    config=cfg,
                    symbol="SOLCAD",
                    interval=60,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                    skip_balances=False,
                    require_balances=False,
                )
                self.assertEqual(code, 0)
                payload = json.loads(out)
                self.assertTrue(payload["balance_check_skipped"])

    def test_handle_start_and_main_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with mock.patch("qtbot.cli.BotRunner") as runner_cls:
                runner_cls.return_value.run.return_value = mock.Mock(loop_count=3)
                code, out, _ = _capture_output(cli._handle_start, config=cfg, budget_cad=1000.0)
                self.assertEqual(code, 0)
                self.assertIn("loop_count=3", out)

            with mock.patch("qtbot.cli.load_runtime_config", return_value=cfg), mock.patch(
                "qtbot.cli._handle_status", return_value=0
            ) as status:
                result = cli.main(["status"])
                self.assertEqual(result, 0)
                status.assert_called_once()

    def test_handle_staging_validate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            report = StagingValidationReport(
                started_at_utc="2026-03-05T00:00:00+00:00",
                completed_at_utc="2026-03-05T00:00:10+00:00",
                runtime_dir=cfg.runtime_dir / "staging_validation",
                report_file=cfg.runtime_dir / "staging_validation" / "logs" / "staging_validation_report.json",
                steps=[
                    StagingValidationStep(name="offline_control_plane_drill", passed=True, detail="ok"),
                ],
                passed=True,
                message="staging_validation_passed steps=1",
            )
            with mock.patch("qtbot.cli.StagingValidator") as validator_cls:
                validator_cls.return_value.run.return_value = report
                code, out, _ = _capture_output(
                    cli._handle_staging_validate,
                    config=cfg,
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=60,
                    offline_only=True,
                )
            self.assertEqual(code, 0)
            payload = json.loads(out)
            self.assertTrue(payload["passed"])


if __name__ == "__main__":
    unittest.main()
