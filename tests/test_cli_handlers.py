from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot import cli
from qtbot.control import Command
from qtbot.cutover import CutoverCheckResult, ProductionCutoverReport
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
    def test_main_dispatches_all_commands(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            with (
                mock.patch("qtbot.cli.load_runtime_config", return_value=cfg),
                mock.patch("qtbot.cli._handle_start", return_value=0) as h_start,
                mock.patch("qtbot.cli._handle_control_write", return_value=0) as h_control,
                mock.patch("qtbot.cli._handle_status", return_value=0) as h_status,
                mock.patch("qtbot.cli._handle_ndax_pairs", return_value=0) as h_pairs,
                mock.patch("qtbot.cli._handle_ndax_candles", return_value=0) as h_candles,
                mock.patch("qtbot.cli._handle_ndax_balances", return_value=0) as h_balances,
                mock.patch("qtbot.cli._handle_ndax_check", return_value=0) as h_check,
                mock.patch("qtbot.cli._handle_data_backfill", return_value=0) as h_backfill,
                mock.patch("qtbot.cli._handle_data_status", return_value=0) as h_status_data,
                mock.patch("qtbot.cli._handle_data_build_combined", return_value=0) as h_build_combined,
                mock.patch("qtbot.cli._handle_data_calibrate_weights", return_value=0) as h_calibrate,
                mock.patch("qtbot.cli._handle_data_weight_status", return_value=0) as h_weight_status,
                mock.patch("qtbot.cli._handle_build_snapshot", return_value=0) as h_build_snapshot,
                mock.patch("qtbot.cli._handle_train", return_value=0) as h_train,
                mock.patch("qtbot.cli._handle_eval", return_value=0) as h_eval,
                mock.patch("qtbot.cli._handle_staging_validate", return_value=0) as h_staging,
                mock.patch("qtbot.cli._handle_cutover_checklist", return_value=0) as h_cutover,
            ):
                self.assertEqual(cli.main(["start", "--budget", "100"]), 0)
                h_start.assert_called_once()

                self.assertEqual(cli.main(["pause"]), 0)
                self.assertEqual(cli.main(["resume"]), 0)
                self.assertEqual(cli.main(["stop"]), 0)
                self.assertEqual(h_control.call_count, 3)

                self.assertEqual(cli.main(["status"]), 0)
                h_status.assert_called_once()

                self.assertEqual(cli.main(["ndax-pairs"]), 0)
                h_pairs.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "ndax-candles",
                            "--symbol",
                            "SOLCAD",
                            "--from-date",
                            "2026-03-01",
                            "--to-date",
                            "2026-03-02",
                        ]
                    ),
                    0,
                )
                h_candles.assert_called_once()

                self.assertEqual(cli.main(["ndax-balances"]), 0)
                h_balances.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "ndax-check",
                            "--symbol",
                            "SOLCAD",
                            "--from-date",
                            "2026-03-01",
                            "--to-date",
                            "2026-03-02",
                        ]
                    ),
                    0,
                )
                h_check.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "data-backfill",
                            "--from",
                            "2026-01-01",
                            "--to",
                            "2026-01-03",
                            "--sources",
                            "ndax,binance",
                        ]
                    ),
                    0,
                )
                h_backfill.assert_called_once()

                self.assertEqual(cli.main(["data-status", "--dataset", "combined"]), 0)
                h_status_data.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "data-build-combined",
                            "--from",
                            "2026-01-01",
                            "--to",
                            "2026-01-03",
                        ]
                    ),
                    0,
                )
                h_build_combined.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "data-calibrate-weights",
                            "--from",
                            "2026-01-01",
                            "--to",
                            "2026-01-03",
                            "--refresh",
                            "monthly",
                        ]
                    ),
                    0,
                )
                h_calibrate.assert_called_once()

                self.assertEqual(cli.main(["data-weight-status"]), 0)
                h_weight_status.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "build-snapshot",
                            "--asof",
                            "2026-03-05T12:00:00Z",
                        ]
                    ),
                    0,
                )
                h_build_snapshot.assert_called_once()

                self.assertEqual(
                    cli.main(
                        [
                            "train",
                            "--snapshot",
                            "snap123",
                            "--folds",
                            "4",
                            "--universe",
                            "V1",
                        ]
                    ),
                    0,
                )
                h_train.assert_called_once()

                self.assertEqual(cli.main(["eval", "--run", "run123"]), 0)
                h_eval.assert_called_once()

                self.assertEqual(cli.main(["staging-validate", "--offline-only"]), 0)
                h_staging.assert_called_once()

                self.assertEqual(cli.main(["cutover-checklist", "--offline-only"]), 0)
                h_cutover.assert_called_once()

    def test_main_returns_2_when_config_load_fails(self) -> None:
        with mock.patch("qtbot.cli.load_runtime_config", side_effect=RuntimeError("bad config")):
            code, _, err = _capture_output(cli.main, ["status"])
        self.assertEqual(code, 2)
        self.assertIn("Failed to load runtime config", err)

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

                code, _, err = _capture_output(
                    cli._handle_ndax_candles,
                    config=cfg,
                    symbol="SOLCAD",
                    interval=0,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                )
                self.assertEqual(code, 2)
                self.assertIn("--interval must be > 0", err)

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

            bad_client = _FakeClient()
            bad_client.fetch_balances = mock.Mock(side_effect=NdaxError("down"))  # type: ignore[method-assign]
            with mock.patch("qtbot.cli._make_ndax_client", return_value=bad_client), mock.patch(
                "qtbot.cli.load_credentials_from_env", return_value=mock.Mock()
            ):
                code, _, err = _capture_output(cli._handle_ndax_balances, config=cfg)
                self.assertEqual(code, 1)
                self.assertIn("NDAX balance fetch failed", err)

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

            with mock.patch("qtbot.cli._make_ndax_client", return_value=_FakeClient()):
                code, _, err = _capture_output(
                    cli._handle_ndax_check,
                    config=cfg,
                    symbol="SOLCAD",
                    interval=0,
                    from_date="2026-03-04",
                    to_date="2026-03-05",
                    skip_balances=True,
                    require_balances=False,
                )
                self.assertEqual(code, 2)
                self.assertIn("--interval must be > 0", err)

    def test_handle_data_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.symbols_with_errors = 0
            summary.to_payload.return_value = {
                "timeframe": "15m",
                "symbols_processed": 2,
                "symbols_with_errors": 0,
            }
            service = mock.Mock()
            service.backfill.return_value = summary
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_data_backfill,
                    config=cfg,
                    from_date="2026-01-01",
                    to_date="2026-01-31",
                    timeframe="15m",
                    sources="ndax,binance",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["timeframe"], "15m")
            self.assertIn("progress_log_file", payload)
            service.backfill.assert_called_once()

            service.backfill.side_effect = ValueError("bad window")
            code, _, err = _capture_output(
                cli._handle_data_backfill,
                config=cfg,
                from_date="2026-01-10",
                to_date="2026-01-01",
                timeframe="15m",
                sources="ndax",
            )
            self.assertEqual(code, 1)
            self.assertIn("Data backfill failed", err)

    def test_handle_data_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "timeframe": "15m",
                "symbols_total": 5,
            }
            service = mock.Mock()
            service.data_status.return_value = summary
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_data_status,
                    config=cfg,
                    timeframe="15m",
                    dataset="combined",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["symbols_total"], 5)
            service.data_status.assert_called_once_with(timeframe="15m", dataset="combined")

            service.data_status.side_effect = ValueError("bad dataset")
            code, _, err = _capture_output(
                cli._handle_data_status,
                config=cfg,
                timeframe="15m",
                dataset="nope",
            )
            self.assertEqual(code, 1)
            self.assertIn("Data status failed", err)

    def test_handle_data_build_combined(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.symbols_with_errors = 0
            summary.to_payload.return_value = {
                "timeframe": "15m",
                "symbols_total": 2,
                "symbols_with_errors": 0,
            }
            service = mock.Mock()
            service.build_combined.return_value = summary
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_data_build_combined,
                    config=cfg,
                    from_date="2026-01-01",
                    to_date="2026-01-31",
                    timeframe="15m",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["symbols_total"], 2)
            service.build_combined.assert_called_once()

            service.build_combined.side_effect = ValueError("boom")
            code, _, err = _capture_output(
                cli._handle_data_build_combined,
                config=cfg,
                from_date="2026-01-03",
                to_date="2026-01-01",
                timeframe="15m",
            )
            self.assertEqual(code, 1)
            self.assertIn("Combined dataset build failed", err)

    def test_handle_data_calibrate_weights(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "timeframe": "15m",
                "rows_total": 10,
                "refresh": "monthly",
            }
            service = mock.Mock()
            service.calibrate_weights.return_value = summary
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_data_calibrate_weights,
                    config=cfg,
                    from_date="2026-01-01",
                    to_date="2026-01-31",
                    timeframe="15m",
                    refresh="monthly",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["rows_total"], 10)
            service.calibrate_weights.assert_called_once()

            service.calibrate_weights.side_effect = ValueError("boom")
            code, _, err = _capture_output(
                cli._handle_data_calibrate_weights,
                config=cfg,
                from_date="2026-01-03",
                to_date="2026-01-01",
                timeframe="15m",
                refresh="monthly",
            )
            self.assertEqual(code, 1)
            self.assertIn("Weight calibration failed", err)

    def test_handle_data_weight_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "timeframe": "15m",
                "row_count": 3,
            }
            service = mock.Mock()
            service.weight_status.return_value = summary
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_data_weight_status,
                    config=cfg,
                    timeframe="15m",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["row_count"], 3)
            service.weight_status.assert_called_once_with(timeframe="15m")

            service.weight_status.side_effect = ValueError("boom")
            with mock.patch("qtbot.cli._make_data_service", return_value=service):
                code, _, err = _capture_output(
                    cli._handle_data_weight_status,
                    config=cfg,
                    timeframe="15m",
                )
            self.assertEqual(code, 1)
            self.assertIn("Weight status failed", err)

    def test_handle_build_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "snapshot_id": "20260305T120000Z_combined_15m_hash",
                "dataset_hash": "hash",
                "row_count": 42,
            }
            service = mock.Mock()
            service.build_snapshot.return_value = summary
            with mock.patch("qtbot.cli._make_snapshot_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_build_snapshot,
                    config=cfg,
                    asof="2026-03-05T12:00:00Z",
                    timeframe="15m",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["row_count"], 42)
            service.build_snapshot.assert_called_once()

            service.build_snapshot.side_effect = ValueError("bad asof")
            with mock.patch("qtbot.cli._make_snapshot_service", return_value=service):
                code, _, err = _capture_output(
                    cli._handle_build_snapshot,
                    config=cfg,
                    asof="not-a-date",
                    timeframe="15m",
                )
            self.assertEqual(code, 1)
            self.assertIn("Snapshot build failed", err)

    def test_handle_train(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "run_id": "run123",
                "snapshot_id": "snap123",
                "status": "trained",
            }
            service = mock.Mock()
            service.train.return_value = summary
            with mock.patch("qtbot.cli._make_training_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_train,
                    config=cfg,
                    snapshot_id="snap123",
                    folds=4,
                    universe="V1",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["run_id"], "run123")
            service.train.assert_called_once_with(snapshot_id="snap123", folds=4, universe="V1")

            service.train.side_effect = ValueError("bad snapshot")
            with mock.patch("qtbot.cli._make_training_service", return_value=service):
                code, _, err = _capture_output(
                    cli._handle_train,
                    config=cfg,
                    snapshot_id="snap123",
                    folds=4,
                    universe="V1",
                )
            self.assertEqual(code, 1)
            self.assertIn("Training failed", err)

    def test_handle_eval(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            summary = mock.Mock()
            summary.to_payload.return_value = {
                "run_id": "run123",
                "primary_scenario": "weighted_combined",
                "status": "evaluated",
            }
            service = mock.Mock()
            service.evaluate.return_value = summary
            with mock.patch("qtbot.cli._make_evaluation_service", return_value=service):
                code, out, err = _capture_output(
                    cli._handle_eval,
                    config=cfg,
                    run_id="run123",
                )
            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            payload = json.loads(out)
            self.assertEqual(payload["primary_scenario"], "weighted_combined")
            service.evaluate.assert_called_once_with(run_id="run123")

            service.evaluate.side_effect = ValueError("bad run")
            with mock.patch("qtbot.cli._make_evaluation_service", return_value=service):
                code, _, err = _capture_output(
                    cli._handle_eval,
                    config=cfg,
                    run_id="run123",
                )
            self.assertEqual(code, 1)
            self.assertIn("Evaluation failed", err)

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

            code, _, err = _capture_output(
                cli._handle_staging_validate,
                config=cfg,
                budget_cad=1000.0,
                cadence_seconds=0,
                min_loops=1,
                timeout_seconds=60,
                offline_only=True,
            )
            self.assertEqual(code, 2)
            self.assertIn("--cadence-seconds must be > 0", err)

            with mock.patch("qtbot.cli.StagingValidator") as validator_cls:
                validator_cls.return_value.run.side_effect = RuntimeError("bad")
                code, _, err = _capture_output(
                    cli._handle_staging_validate,
                    config=cfg,
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=60,
                    offline_only=True,
                )
            self.assertEqual(code, 1)
            self.assertIn("Staging validation failed", err)

    def test_handle_cutover_checklist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            report = ProductionCutoverReport(
                started_at_utc="2026-03-05T00:00:00+00:00",
                completed_at_utc="2026-03-05T00:01:00+00:00",
                runtime_dir=cfg.runtime_dir / "production_cutover",
                report_file=cfg.runtime_dir / "production_cutover" / "logs" / "production_cutover_report.json",
                checks=[
                    CutoverCheckResult(name="staging_validation_report", passed=True, detail="ok"),
                ],
                passed=True,
                message="production_cutover_ready checks=1",
                start_budget_cad=250.0,
                launch_commands=["qtbot start --budget 250"],
                manual_verification_checklist=["verify trade"],
                rollback_commands=["qtbot stop"],
            )
            with mock.patch("qtbot.cli.ProductionCutoverChecklist") as cutover_cls:
                cutover_cls.return_value.run.return_value = report
                code, out, _ = _capture_output(
                    cli._handle_cutover_checklist,
                    config=cfg,
                    start_budget_cad=250.0,
                    staging_max_age_hours=48,
                    offline_only=True,
                    require_discord=False,
                )
            self.assertEqual(code, 0)
            payload = json.loads(out)
            self.assertTrue(payload["passed"])

            code, _, err = _capture_output(
                cli._handle_cutover_checklist,
                config=cfg,
                start_budget_cad=250.0,
                staging_max_age_hours=0,
                offline_only=True,
                require_discord=False,
            )
            self.assertEqual(code, 2)
            self.assertIn("--staging-max-age-hours must be > 0", err)

            with mock.patch("qtbot.cli.ProductionCutoverChecklist") as cutover_cls:
                cutover_cls.return_value.run.side_effect = RuntimeError("bad")
                code, _, err = _capture_output(
                    cli._handle_cutover_checklist,
                    config=cfg,
                    start_budget_cad=250.0,
                    staging_max_age_hours=48,
                    offline_only=True,
                    require_discord=False,
                )
            self.assertEqual(code, 1)
            self.assertIn("Cutover checklist failed", err)


if __name__ == "__main__":
    unittest.main()
