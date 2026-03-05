from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from qtbot.staging import (
    StagingValidationStep,
    StagingValidator,
    _csv_data_rows,
    _derive_runtime_config,
    _tail,
    _temporary_env,
    _wait_for_exit,
)
from tests._helpers import make_runtime_config


class StagingValidatorTests(unittest.TestCase):
    def test_reconciliation_fault_simulation_passes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            step = validator._run_reconciliation_fault_simulation(
                runtime_dir=cfg.runtime_dir / "staging_validation"
            )
            self.assertTrue(step.passed)
            self.assertIn("changed_symbols=", step.detail)

    def test_risk_fault_simulation_passes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            step = validator._run_risk_fault_simulation(runtime_dir=cfg.runtime_dir / "staging_validation")
            self.assertTrue(step.passed)
            self.assertIn("risk simulation passed", step.detail)

    def test_run_offline_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            with mock.patch.object(
                StagingValidator,
                "_run_offline_control_plane_drill",
                return_value=StagingValidationStep(
                    name="offline_control_plane_drill",
                    passed=True,
                    detail="ok",
                ),
            ), mock.patch.object(
                StagingValidator,
                "_run_reconciliation_fault_simulation",
                return_value=StagingValidationStep(
                    name="reconciliation_fault_simulation",
                    passed=True,
                    detail="ok",
                ),
            ), mock.patch.object(
                StagingValidator,
                "_run_risk_fault_simulation",
                return_value=StagingValidationStep(
                    name="risk_fault_simulation",
                    passed=True,
                    detail="ok",
                ),
            ):
                report = validator.run(
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=30,
                    offline_only=True,
                )

            self.assertTrue(report.passed)
            self.assertTrue(report.report_file.exists())
            payload = json.loads(report.report_file.read_text(encoding="utf-8"))
            self.assertTrue(payload["passed"])
            self.assertEqual(payload["message"], "staging_validation_passed steps=3")

    def test_run_offline_marks_failed_when_any_step_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            with mock.patch.object(
                StagingValidator,
                "_run_offline_control_plane_drill",
                return_value=StagingValidationStep(
                    name="offline_control_plane_drill",
                    passed=True,
                    detail="ok",
                ),
            ), mock.patch.object(
                StagingValidator,
                "_run_reconciliation_fault_simulation",
                return_value=StagingValidationStep(
                    name="reconciliation_fault_simulation",
                    passed=False,
                    detail="broken",
                ),
            ), mock.patch.object(
                StagingValidator,
                "_run_risk_fault_simulation",
                return_value=StagingValidationStep(
                    name="risk_fault_simulation",
                    passed=True,
                    detail="ok",
                ),
            ):
                report = validator.run(
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=30,
                    offline_only=True,
                )

            self.assertFalse(report.passed)
            self.assertIn("reconciliation_fault_simulation", report.message)

    def test_run_non_offline_uses_live_step_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            with mock.patch.object(
                StagingValidator,
                "_run_public_ndax_health_check",
                return_value=StagingValidationStep("public_ndax_health_check", True, "ok"),
            ) as public_check, mock.patch.object(
                StagingValidator,
                "_run_dry_run_lifecycle_drill",
                return_value=StagingValidationStep("dry_run_lifecycle_drill", True, "ok"),
            ) as lifecycle_check, mock.patch.object(
                StagingValidator,
                "_run_cli_failure_scenario",
                return_value=StagingValidationStep("cli_failure_scenario_invalid_symbol", True, "ok"),
            ) as failure_check, mock.patch.object(
                StagingValidator,
                "_run_reconciliation_fault_simulation",
                return_value=StagingValidationStep("reconciliation_fault_simulation", True, "ok"),
            ), mock.patch.object(
                StagingValidator,
                "_run_risk_fault_simulation",
                return_value=StagingValidationStep("risk_fault_simulation", True, "ok"),
            ):
                report = validator.run(
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=20,
                    offline_only=False,
                )
            self.assertTrue(report.passed)
            public_check.assert_called_once()
            lifecycle_check.assert_called_once()
            failure_check.assert_called_once()

    def test_run_rejects_invalid_numeric_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            with self.assertRaises(ValueError):
                validator.run(
                    budget_cad=0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=20,
                    offline_only=True,
                )
            with self.assertRaises(ValueError):
                validator.run(
                    budget_cad=1,
                    cadence_seconds=0,
                    min_loops=2,
                    timeout_seconds=20,
                    offline_only=True,
                )
            with self.assertRaises(ValueError):
                validator.run(
                    budget_cad=1,
                    cadence_seconds=1,
                    min_loops=0,
                    timeout_seconds=20,
                    offline_only=True,
                )
            with self.assertRaises(ValueError):
                validator.run(
                    budget_cad=1,
                    cadence_seconds=1,
                    min_loops=1,
                    timeout_seconds=0,
                    offline_only=True,
                )

    def test_public_ndax_health_check_parsing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            runtime_dir = cfg.runtime_dir / "staging_validation"
            success = subprocess.CompletedProcess(
                args=["ndax-pairs"],
                returncode=0,
                stdout=json.dumps({"instrument_count": 5, "tradable_count": 3}),
                stderr="",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=success):
                step = validator._run_public_ndax_health_check(runtime_dir=runtime_dir)
            self.assertTrue(step.passed)

            bad_json = subprocess.CompletedProcess(
                args=["ndax-pairs"],
                returncode=0,
                stdout="{not-json}",
                stderr="",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=bad_json):
                step = validator._run_public_ndax_health_check(runtime_dir=runtime_dir)
            self.assertFalse(step.passed)

            bad_counts = subprocess.CompletedProcess(
                args=["ndax-pairs"],
                returncode=0,
                stdout=json.dumps({"instrument_count": 0, "tradable_count": 0}),
                stderr="",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=bad_counts):
                step = validator._run_public_ndax_health_check(runtime_dir=runtime_dir)
            self.assertFalse(step.passed)

            cmd_failed = subprocess.CompletedProcess(
                args=["ndax-pairs"],
                returncode=2,
                stdout="",
                stderr="boom",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=cmd_failed):
                step = validator._run_public_ndax_health_check(runtime_dir=runtime_dir)
            self.assertFalse(step.passed)

    def test_cli_failure_scenario_branches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            runtime_dir = cfg.runtime_dir / "staging_validation"

            expected_fail = subprocess.CompletedProcess(
                args=["ndax-check"],
                returncode=1,
                stdout="",
                stderr="error",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=expected_fail):
                step = validator._run_cli_failure_scenario(runtime_dir=runtime_dir)
            self.assertTrue(step.passed)

            unexpected_success = subprocess.CompletedProcess(
                args=["ndax-check"],
                returncode=0,
                stdout="{}",
                stderr="",
            )
            with mock.patch.object(StagingValidator, "_run_cli_command", return_value=unexpected_success):
                step = validator._run_cli_failure_scenario(runtime_dir=runtime_dir)
            self.assertFalse(step.passed)

    def test_offline_control_plane_drill_real_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            step = validator._run_offline_control_plane_drill(
                runtime_dir=cfg.runtime_dir / "staging_validation_offline"
            )
            self.assertTrue(step.passed)

    def test_dry_run_lifecycle_drill_mocked_success_and_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            validator = StagingValidator(config=cfg)
            runtime_dir = cfg.runtime_dir / "staging_validation"
            (runtime_dir / "logs").mkdir(parents=True, exist_ok=True)
            (runtime_dir / "logs" / "decisions.csv").write_text(
                "timestamp,symbol,close,ema_fast,ema_slow,atr,signal,reason\n"
                "2026-03-05T00:00:00+00:00,SOLCAD,1,1,1,1,HOLD,test\n",
                encoding="utf-8",
            )

            start_proc = mock.Mock()
            start_proc.poll.return_value = 0
            start_proc.communicate.return_value = ("qtbot stopped gracefully", "")
            start_proc.returncode = 0

            cli_ok = subprocess.CompletedProcess(args=["pause"], returncode=0, stdout="", stderr="")
            with mock.patch("qtbot.staging.subprocess.Popen", return_value=start_proc), mock.patch(
                "qtbot.staging._wait_for_exit", return_value=True
            ), mock.patch.object(
                StagingValidator, "_wait_for_condition", side_effect=[True, True, True]
            ), mock.patch.object(
                StagingValidator, "_run_cli_command", return_value=cli_ok
            ):
                step = validator._run_dry_run_lifecycle_drill(
                    runtime_dir=runtime_dir,
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=10,
                )
            self.assertTrue(step.passed)

            start_proc_timeout = mock.Mock()
            start_proc_timeout.poll.return_value = 0
            start_proc_timeout.communicate.return_value = ("", "")
            with mock.patch("qtbot.staging.subprocess.Popen", return_value=start_proc_timeout), mock.patch(
                "qtbot.staging._wait_for_exit", return_value=True
            ), mock.patch.object(
                StagingValidator, "_wait_for_condition", return_value=False
            ), mock.patch.object(
                StagingValidator, "_ensure_stop"
            ):
                step = validator._run_dry_run_lifecycle_drill(
                    runtime_dir=runtime_dir,
                    budget_cad=1000.0,
                    cadence_seconds=3,
                    min_loops=2,
                    timeout_seconds=1,
                )
            self.assertFalse(step.passed)

    def test_helper_functions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            csv_path = root / "rows.csv"
            csv_path.write_text("h\n1\n2\n", encoding="utf-8")
            self.assertEqual(_csv_data_rows(csv_path), 2)
            self.assertEqual(_csv_data_rows(root / "missing.csv"), 0)
            self.assertEqual(_tail("abc", limit=10), "abc")
            self.assertEqual(_tail("x" * 20, limit=5), "xxxxx")

            cfg = make_runtime_config(root)
            derived = _derive_runtime_config(cfg, root / "sim")
            self.assertEqual(derived.runtime_dir, (root / "sim"))
            self.assertEqual(derived.control_file, root / "sim" / "control.json")

            with _temporary_env(TEST_STAGING_ENV="1"):
                self.assertEqual("1", os.environ.get("TEST_STAGING_ENV"))
            self.assertNotIn("TEST_STAGING_ENV", os.environ)

            proc = mock.Mock()
            proc.wait.return_value = None
            self.assertTrue(_wait_for_exit(proc, timeout_seconds=1))
            proc_timeout = mock.Mock()
            proc_timeout.wait.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=1)
            self.assertFalse(_wait_for_exit(proc_timeout, timeout_seconds=1))


if __name__ == "__main__":
    unittest.main()
