from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.control import Command, ControlState
from qtbot.ndax_client import NdaxError
from qtbot.preflight import PreflightCheckResult, PreflightSummary
from qtbot.runner import BotRunner
from qtbot.strategy.engine import StrategySummary
from tests._helpers import make_runtime_config


class _FakeStateStore:
    def __init__(self, *_args, **_kwargs) -> None:
        self.loop_count = 0

    def initialize(self, *, initial_budget_cad: float) -> None:
        self.initial_budget_cad = initial_budget_cad

    def set_status(self, *, run_status: str, last_command: str, event_detail: str) -> None:
        self.last_status = (run_status, last_command, event_detail)

    def record_loop(
        self,
        *,
        last_command: str,
        loop_started_at_utc: str,
        loop_completed_at_utc: str,
        event_detail: str,
    ) -> int:
        self.loop_count += 1
        return self.loop_count


class RunnerLoopTests(unittest.TestCase):
    def test_runner_processes_single_cycle_then_stops(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), cadence_seconds=1)
            logger = logging.getLogger("runner-test")
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())

            strategy_summary = StrategySummary(
                symbol_count=1,
                enter_count=0,
                exit_count=0,
                hold_count=1,
                skipped_count=0,
                message="decisions_persisted symbols=1 enter=0 exit=0 hold=1 skipped=0",
                decisions=[],
                tradable=[],
            )
            fake_strategy = mock.Mock()
            fake_strategy.evaluate_cycle.return_value = strategy_summary
            fake_execution = mock.Mock()
            fake_execution.execute_decisions.return_value = mock.Mock(
                message="execution_disabled_dry_run"
            )

            control_states = [
                ControlState(command=Command.RUN, updated_at_utc=None, updated_by=None, reason=None),
                ControlState(command=Command.STOP, updated_at_utc=None, updated_by=None, reason=None),
            ]

            with mock.patch("qtbot.runner.configure_logging", return_value=logger), mock.patch(
                "qtbot.runner.StateStore", _FakeStateStore
            ), mock.patch("qtbot.runner.NdaxClient", return_value=mock.Mock()), mock.patch(
                "qtbot.runner.DecisionCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.TradeCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StrategyEngine", return_value=fake_strategy
            ), mock.patch(
                "qtbot.runner.LiveExecutionEngine", return_value=fake_execution
            ), mock.patch(
                "qtbot.runner.StartupReconciler"
            ) as reconciler_cls, mock.patch(
                "qtbot.runner.signal.signal"
            ), mock.patch(
                "qtbot.runner.read_control", side_effect=control_states
            ), mock.patch(
                "qtbot.runner.write_control", return_value=control_states[0]
            ):
                reconciler_cls.return_value.reconcile.return_value = mock.Mock(
                    message="reconciliation_complete"
                )
                result = BotRunner(config=cfg, budget_cad=1000.0).run()

            self.assertEqual(result.loop_count, 1)
            fake_strategy.evaluate_cycle.assert_called_once()
            fake_execution.execute_decisions.assert_called_once()

    def test_runner_blocks_start_when_live_reconciliation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=True)
            logger = logging.getLogger("runner-test-live-block")
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())

            with mock.patch("qtbot.runner.configure_logging", return_value=logger), mock.patch(
                "qtbot.runner.StateStore", _FakeStateStore
            ), mock.patch("qtbot.runner.NdaxClient", return_value=mock.Mock()), mock.patch(
                "qtbot.runner.DecisionCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.TradeCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StrategyEngine", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.LiveExecutionEngine", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StartupReconciler"
            ) as reconciler_cls, mock.patch(
                "qtbot.runner.signal.signal"
            ):
                reconciler_cls.return_value.reconcile.side_effect = NdaxError("reconcile failed")
                with self.assertRaises(NdaxError):
                    BotRunner(config=cfg, budget_cad=1000.0).run()

    def test_runner_blocks_start_when_live_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=True)
            logger = logging.getLogger("runner-test-preflight-block")
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())

            failed_summary = PreflightSummary(
                checks=[
                    PreflightCheckResult(
                        name="credentials_auth",
                        passed=False,
                        detail="missing credentials",
                    )
                ],
                message="go_live_preflight_failed failed_checks=credentials_auth",
            )

            with mock.patch("qtbot.runner.configure_logging", return_value=logger), mock.patch(
                "qtbot.runner.StateStore", _FakeStateStore
            ), mock.patch("qtbot.runner.NdaxClient", return_value=mock.Mock()), mock.patch(
                "qtbot.runner.DecisionCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.TradeCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StrategyEngine", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.LiveExecutionEngine", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StartupReconciler"
            ) as reconciler_cls, mock.patch(
                "qtbot.runner.GoLivePreflight"
            ) as preflight_cls, mock.patch(
                "qtbot.runner.signal.signal"
            ):
                reconciler_cls.return_value.reconcile.return_value = mock.Mock(
                    message="reconciliation_complete"
                )
                preflight_cls.return_value.run.return_value = failed_summary
                with self.assertRaises(NdaxError):
                    BotRunner(config=cfg, budget_cad=1000.0).run()

    def test_runner_honors_pre_cycle_risk_pause(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), enable_live_trading=True)
            logger = logging.getLogger("runner-test-risk-pause")
            if not logger.handlers:
                logger.addHandler(logging.NullHandler())

            control_states = [
                ControlState(command=Command.RUN, updated_at_utc=None, updated_by=None, reason=None),
                ControlState(command=Command.STOP, updated_at_utc=None, updated_by=None, reason=None),
            ]

            with mock.patch("qtbot.runner.configure_logging", return_value=logger), mock.patch(
                "qtbot.runner.StateStore", _FakeStateStore
            ), mock.patch("qtbot.runner.NdaxClient", return_value=mock.Mock()), mock.patch(
                "qtbot.runner.DecisionCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.TradeCsvLogger", return_value=mock.Mock()
            ), mock.patch(
                "qtbot.runner.StrategyEngine", return_value=mock.Mock()
            ) as strategy_cls, mock.patch(
                "qtbot.runner.LiveExecutionEngine", return_value=mock.Mock()
            ) as execution_cls, mock.patch(
                "qtbot.runner.StartupReconciler"
            ) as reconciler_cls, mock.patch(
                "qtbot.runner.GoLivePreflight"
            ) as preflight_cls, mock.patch(
                "qtbot.runner.RiskManager"
            ) as risk_cls, mock.patch(
                "qtbot.runner.signal.signal"
            ), mock.patch(
                "qtbot.runner.read_control", side_effect=control_states
            ), mock.patch(
                "qtbot.runner.write_control", return_value=control_states[0]
            ):
                reconciler_cls.return_value.reconcile.return_value = mock.Mock(
                    message="reconciliation_complete"
                )
                preflight_cls.return_value.run.return_value = PreflightSummary(
                    checks=[PreflightCheckResult(name="ok", passed=True, detail="ok")],
                    message="go_live_preflight_passed checks=6",
                )
                risk_cls.return_value.enforce_pre_cycle.return_value = mock.Mock(
                    triggered=True,
                    reason="daily_loss_cap_breached",
                )
                result = BotRunner(config=cfg, budget_cad=1000.0).run()

            self.assertEqual(result.loop_count, 1)
            strategy_cls.return_value.evaluate_cycle.assert_not_called()
            execution_cls.return_value.execute_decisions.assert_not_called()


if __name__ == "__main__":
    unittest.main()
