"""M10 staging validation orchestration."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control, write_control
from qtbot.ndax_client import NdaxBalance
from qtbot.reconciliation import StartupReconciler
from qtbot.risk import RiskManager
from qtbot.state import StateStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class StagingValidationStep:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class StagingValidationReport:
    started_at_utc: str
    completed_at_utc: str
    runtime_dir: Path
    report_file: Path
    steps: list[StagingValidationStep]
    passed: bool
    message: str

    def to_payload(self) -> dict[str, object]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "runtime_dir": str(self.runtime_dir),
            "report_file": str(self.report_file),
            "passed": self.passed,
            "message": self.message,
            "steps": [
                {
                    "name": step.name,
                    "passed": step.passed,
                    "detail": step.detail,
                }
                for step in self.steps
            ],
        }


class StagingValidator:
    """Runs M10 staging checks and writes a machine-readable report."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        working_dir: Path | None = None,
    ) -> None:
        self._config = config
        self._working_dir = (working_dir or Path.cwd()).resolve()
        self._logger = logging.getLogger("qtbot.staging")
        if not self._logger.handlers:
            self._logger.addHandler(logging.NullHandler())

    def run(
        self,
        *,
        budget_cad: float,
        cadence_seconds: int,
        min_loops: int,
        timeout_seconds: int,
        offline_only: bool,
    ) -> StagingValidationReport:
        if budget_cad <= 0:
            raise ValueError("budget_cad must be > 0.")
        if cadence_seconds <= 0:
            raise ValueError("cadence_seconds must be > 0.")
        if min_loops <= 0:
            raise ValueError("min_loops must be > 0.")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0.")

        started_at = _utc_now_iso()
        runtime_dir = (self._config.runtime_dir / "staging_validation").resolve()
        self._reset_runtime_dir(runtime_dir)

        steps: list[StagingValidationStep] = []
        if offline_only:
            steps.append(self._run_offline_control_plane_drill(runtime_dir=runtime_dir))
        else:
            steps.append(self._run_public_ndax_health_check(runtime_dir=runtime_dir))
            steps.append(
                self._run_dry_run_lifecycle_drill(
                    runtime_dir=runtime_dir,
                    budget_cad=budget_cad,
                    cadence_seconds=cadence_seconds,
                    min_loops=min_loops,
                    timeout_seconds=timeout_seconds,
                )
            )
            steps.append(self._run_cli_failure_scenario(runtime_dir=runtime_dir))
        steps.append(self._run_reconciliation_fault_simulation(runtime_dir=runtime_dir))
        steps.append(self._run_risk_fault_simulation(runtime_dir=runtime_dir))

        failed = [step.name for step in steps if not step.passed]
        passed = len(failed) == 0
        message = (
            f"staging_validation_passed steps={len(steps)}"
            if passed
            else f"staging_validation_failed failed_steps={','.join(failed)}"
        )
        completed_at = _utc_now_iso()

        report_file = runtime_dir / "logs" / "staging_validation_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report = StagingValidationReport(
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            runtime_dir=runtime_dir,
            report_file=report_file,
            steps=steps,
            passed=passed,
            message=message,
        )
        report_file.write_text(
            json.dumps(report.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return report

    def _run_public_ndax_health_check(self, *, runtime_dir: Path) -> StagingValidationStep:
        result = self._run_cli_command(
            runtime_dir=runtime_dir,
            args=["ndax-pairs"],
            env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
            timeout_seconds=30,
        )
        if result.returncode != 0:
            return StagingValidationStep(
                name="public_ndax_health_check",
                passed=False,
                detail=f"ndax-pairs failed rc={result.returncode} stderr={_tail(result.stderr)}",
            )
        try:
            payload = json.loads(result.stdout)
            tradable_count = int(payload.get("tradable_count", 0))
            instrument_count = int(payload.get("instrument_count", 0))
        except (ValueError, TypeError, json.JSONDecodeError):
            return StagingValidationStep(
                name="public_ndax_health_check",
                passed=False,
                detail="ndax-pairs returned invalid JSON payload",
            )
        if tradable_count <= 0 or instrument_count <= 0:
            return StagingValidationStep(
                name="public_ndax_health_check",
                passed=False,
                detail=(
                    "ndax-pairs payload invalid "
                    f"instrument_count={instrument_count} tradable_count={tradable_count}"
                ),
            )
        return StagingValidationStep(
            name="public_ndax_health_check",
            passed=True,
            detail=(
                "ndax-pairs succeeded "
                f"instrument_count={instrument_count} tradable_count={tradable_count}"
            ),
        )

    def _run_dry_run_lifecycle_drill(
        self,
        *,
        runtime_dir: Path,
        budget_cad: float,
        cadence_seconds: int,
        min_loops: int,
        timeout_seconds: int,
    ) -> StagingValidationStep:
        env = self._cli_env(
            runtime_dir=runtime_dir,
            overrides={
                "QTBOT_ENABLE_LIVE_TRADING": "false",
                "QTBOT_CADENCE_SECONDS": str(cadence_seconds),
            },
        )
        start_proc = subprocess.Popen(
            [sys.executable, "-m", "qtbot", "start", "--budget", f"{budget_cad:.12g}"],
            cwd=self._working_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            loop_ready = self._wait_for_condition(
                timeout_seconds=timeout_seconds,
                predicate=lambda: _loop_count(runtime_dir / "state.sqlite") >= min_loops,
            )
            if not loop_ready:
                self._ensure_stop(runtime_dir=runtime_dir)
                _wait_for_exit(start_proc, timeout_seconds=10)
                stdout, stderr = start_proc.communicate(timeout=5)
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail=(
                        f"loop_count did not reach {min_loops} within {timeout_seconds}s "
                        f"stdout={_tail(stdout)} stderr={_tail(stderr)}"
                    ),
                )

            pause_result = self._run_cli_command(
                runtime_dir=runtime_dir,
                args=["pause"],
                env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
                timeout_seconds=20,
            )
            if pause_result.returncode != 0:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail=f"pause failed rc={pause_result.returncode} stderr={_tail(pause_result.stderr)}",
                )
            paused = self._wait_for_condition(
                timeout_seconds=20,
                predicate=lambda: read_control(runtime_dir / "control.json").command == Command.PAUSE
                and _run_status(runtime_dir / "state.sqlite") == "PAUSED",
            )
            if not paused:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail="runner did not transition to PAUSED after pause command",
                )

            resume_result = self._run_cli_command(
                runtime_dir=runtime_dir,
                args=["resume"],
                env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
                timeout_seconds=20,
            )
            if resume_result.returncode != 0:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail=f"resume failed rc={resume_result.returncode} stderr={_tail(resume_result.stderr)}",
                )
            resumed = self._wait_for_condition(
                timeout_seconds=20,
                predicate=lambda: read_control(runtime_dir / "control.json").command == Command.RUN
                and _run_status(runtime_dir / "state.sqlite") == "RUNNING",
            )
            if not resumed:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail="runner did not transition to RUNNING after resume command",
                )

            stop_result = self._run_cli_command(
                runtime_dir=runtime_dir,
                args=["stop"],
                env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
                timeout_seconds=20,
            )
            if stop_result.returncode != 0:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail=f"stop failed rc={stop_result.returncode} stderr={_tail(stop_result.stderr)}",
                )

            exited = _wait_for_exit(start_proc, timeout_seconds=30)
            stdout, stderr = start_proc.communicate(timeout=5)
            if not exited:
                start_proc.kill()
                start_proc.wait(timeout=5)
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail="runner did not exit after stop command",
                )
            if start_proc.returncode != 0:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail=(
                        f"runner exited with rc={start_proc.returncode} "
                        f"stdout={_tail(stdout)} stderr={_tail(stderr)}"
                    ),
                )

            decisions_file = runtime_dir / "logs" / "decisions.csv"
            decisions_rows = _csv_data_rows(decisions_file)
            if decisions_rows <= 0:
                return StagingValidationStep(
                    name="dry_run_lifecycle_drill",
                    passed=False,
                    detail="decisions.csv has no data rows after dry-run staging drill",
                )
            return StagingValidationStep(
                name="dry_run_lifecycle_drill",
                passed=True,
                detail=(
                    "dry-run lifecycle drill passed "
                    f"loop_count={_loop_count(runtime_dir / 'state.sqlite')} "
                    f"decisions_rows={decisions_rows}"
                ),
            )
        finally:
            if start_proc.poll() is None:
                self._ensure_stop(runtime_dir=runtime_dir)
                try:
                    _wait_for_exit(start_proc, timeout_seconds=10)
                finally:
                    if start_proc.poll() is None:
                        start_proc.kill()
                        start_proc.wait(timeout=5)

    def _run_cli_failure_scenario(self, *, runtime_dir: Path) -> StagingValidationStep:
        result = self._run_cli_command(
            runtime_dir=runtime_dir,
            args=["ndax-check", "--symbol", "INVALIDCAD", "--skip-balances"],
            env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
            timeout_seconds=30,
        )
        if result.returncode == 0:
            return StagingValidationStep(
                name="cli_failure_scenario_invalid_symbol",
                passed=False,
                detail="ndax-check unexpectedly succeeded for invalid symbol",
            )
        return StagingValidationStep(
            name="cli_failure_scenario_invalid_symbol",
            passed=True,
            detail=f"ndax-check failed as expected rc={result.returncode}",
        )

    def _run_offline_control_plane_drill(self, *, runtime_dir: Path) -> StagingValidationStep:
        status = self._run_cli_command(
            runtime_dir=runtime_dir,
            args=["status"],
            env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
            timeout_seconds=20,
        )
        if status.returncode != 0:
            return StagingValidationStep(
                name="offline_control_plane_drill",
                passed=False,
                detail=f"status failed rc={status.returncode} stderr={_tail(status.stderr)}",
            )

        for command in (Command.PAUSE, Command.RUN, Command.STOP):
            cli_name = command.value.lower() if command != Command.RUN else "resume"
            args = [cli_name]
            result = self._run_cli_command(
                runtime_dir=runtime_dir,
                args=args,
                env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
                timeout_seconds=20,
            )
            if result.returncode != 0:
                return StagingValidationStep(
                    name="offline_control_plane_drill",
                    passed=False,
                    detail=f"{cli_name} failed rc={result.returncode} stderr={_tail(result.stderr)}",
                )
            observed = read_control(runtime_dir / "control.json").command
            if observed != command:
                return StagingValidationStep(
                    name="offline_control_plane_drill",
                    passed=False,
                    detail=f"{cli_name} wrote command={observed.value} expected={command.value}",
                )

        return StagingValidationStep(
            name="offline_control_plane_drill",
            passed=True,
            detail="status/pause/resume/stop control-plane commands validated offline",
        )

    def _run_reconciliation_fault_simulation(self, *, runtime_dir: Path) -> StagingValidationStep:
        sim_runtime = runtime_dir / "reconciliation_sim"
        self._reset_runtime_dir(sim_runtime)
        sim_config = _derive_runtime_config(self._config, sim_runtime)
        store = StateStore(sim_config.state_db)
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
        store.reconcile_position(
            symbol="DOGE",
            ndax_qty=1.0,
            reference_price=10.0,
            reconciled_at_utc="2026-03-05T00:01:00+00:00",
            reason="staging_seed",
        )

        reconciler = StartupReconciler(
            config=sim_config,
            ndax_client=_SimulatedReconciliationClient(),
            state_store=store,
            logger=self._logger,
            alerter=None,
        )
        with _temporary_env(
            NDAX_API_KEY="staging-key",
            NDAX_API_SECRET="staging-secret",
            NDAX_USER_ID="1",
        ):
            summary = reconciler.reconcile()

        positions = store.get_positions()
        doge_qty = positions.get("DOGE").qty if "DOGE" in positions else 0.0
        sol_qty = positions.get("SOL").qty if "SOL" in positions else 0.0
        if summary.changed_symbols < 1 or doge_qty > 1e-9 or abs(sol_qty - 2.0) > 1e-9:
            return StagingValidationStep(
                name="reconciliation_fault_simulation",
                passed=False,
                detail=(
                    "reconciliation simulation mismatch "
                    f"changed_symbols={summary.changed_symbols} "
                    f"SOL_qty={sol_qty:.12g} DOGE_qty={doge_qty:.12g}"
                ),
            )
        return StagingValidationStep(
            name="reconciliation_fault_simulation",
            passed=True,
            detail=(
                "reconciliation simulation passed "
                f"changed_symbols={summary.changed_symbols} capped_bot_cash={summary.capped_bot_cash}"
            ),
        )

    def _run_risk_fault_simulation(self, *, runtime_dir: Path) -> StagingValidationStep:
        sim_runtime = runtime_dir / "risk_sim"
        self._reset_runtime_dir(sim_runtime)
        sim_config = _derive_runtime_config(
            self._config,
            sim_runtime,
            daily_loss_cap_cad=50.0,
            consecutive_error_limit=2,
            max_slippage_pct=0.02,
        )
        store = StateStore(sim_config.state_db)
        store.initialize(initial_budget_cad=1000.0)
        store.apply_buy_fill(
            symbol="SOL",
            qty=1.0,
            avg_price=100.0,
            fee_cad=0.0,
            filled_at_utc="2026-03-05T00:00:00+00:00",
            order_id=10,
            ndax_symbol="SOLCAD",
        )
        store.apply_sell_fill(
            symbol="SOL",
            qty=1.0,
            avg_price=20.0,
            fee_cad=0.0,
            filled_at_utc="2026-03-05T00:02:00+00:00",
            order_id=11,
            ndax_symbol="SOLCAD",
        )
        write_control(
            sim_config.control_file,
            Command.RUN,
            updated_by="staging",
            reason="risk simulation start",
        )

        manager = RiskManager(
            config=sim_config,
            state_store=store,
            control_file=sim_config.control_file,
            logger=self._logger,
            alerter=None,
        )
        now = datetime.now(timezone.utc)

        action_loss = manager.enforce_pre_cycle(now_utc=now)
        loss_paused = read_control(sim_config.control_file).command == Command.PAUSE

        write_control(
            sim_config.control_file,
            Command.RUN,
            updated_by="staging",
            reason="risk simulation reset after daily-loss test",
        )
        action_errors = manager.record_cycle_errors(
            now_utc=now,
            error_count=2,
            reason="staging simulated consecutive errors",
        )
        error_paused = read_control(sim_config.control_file).command == Command.PAUSE

        write_control(
            sim_config.control_file,
            Command.RUN,
            updated_by="staging",
            reason="risk simulation reset after error-limit test",
        )
        action_slippage = manager.handle_slippage_breach(
            now_utc=now,
            breach_count=1,
            max_slippage_seen=0.05,
        )
        slippage_paused = read_control(sim_config.control_file).command == Command.PAUSE

        if not (action_loss.triggered and loss_paused and action_errors.triggered and error_paused):
            return StagingValidationStep(
                name="risk_fault_simulation",
                passed=False,
                detail=(
                    "risk simulation failed "
                    f"daily_loss_triggered={action_loss.triggered} daily_loss_paused={loss_paused} "
                    f"error_triggered={action_errors.triggered} error_paused={error_paused}"
                ),
            )
        if not (action_slippage.triggered and slippage_paused):
            return StagingValidationStep(
                name="risk_fault_simulation",
                passed=False,
                detail=(
                    "slippage simulation failed "
                    f"slippage_triggered={action_slippage.triggered} slippage_paused={slippage_paused}"
                ),
            )
        return StagingValidationStep(
            name="risk_fault_simulation",
            passed=True,
            detail=(
                "risk simulation passed "
                f"loss_triggered={action_loss.triggered} "
                f"errors_triggered={action_errors.triggered} "
                f"slippage_triggered={action_slippage.triggered}"
            ),
        )

    def _run_cli_command(
        self,
        *,
        runtime_dir: Path,
        args: list[str],
        env_overrides: dict[str, str],
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        env = self._cli_env(runtime_dir=runtime_dir, overrides=env_overrides)
        return subprocess.run(
            [sys.executable, "-m", "qtbot", *args],
            cwd=self._working_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )

    def _cli_env(self, *, runtime_dir: Path, overrides: dict[str, str]) -> dict[str, str]:
        env = os.environ.copy()
        env["QTBOT_RUNTIME_DIR"] = str(runtime_dir)
        env.update(overrides)
        return env

    def _wait_for_condition(self, *, timeout_seconds: int, predicate) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    def _ensure_stop(self, *, runtime_dir: Path) -> None:
        try:
            self._run_cli_command(
                runtime_dir=runtime_dir,
                args=["stop"],
                env_overrides={"QTBOT_ENABLE_LIVE_TRADING": "false"},
                timeout_seconds=10,
            )
        except Exception:
            pass

    def _reset_runtime_dir(self, runtime_dir: Path) -> None:
        if runtime_dir.exists():
            shutil.rmtree(runtime_dir)
        runtime_dir.mkdir(parents=True, exist_ok=True)


def _derive_runtime_config(
    base: RuntimeConfig,
    runtime_dir: Path,
    *,
    daily_loss_cap_cad: float | None = None,
    consecutive_error_limit: int | None = None,
    max_slippage_pct: float | None = None,
) -> RuntimeConfig:
    return replace(
        base,
        runtime_dir=runtime_dir,
        control_file=runtime_dir / "control.json",
        state_db=runtime_dir / "state.sqlite",
        log_file=runtime_dir / "logs" / "qtbot.log",
        pid_file=runtime_dir / "runner.pid",
        daily_loss_cap_cad=daily_loss_cap_cad if daily_loss_cap_cad is not None else base.daily_loss_cap_cad,
        consecutive_error_limit=(
            consecutive_error_limit
            if consecutive_error_limit is not None
            else base.consecutive_error_limit
        ),
        max_slippage_pct=max_slippage_pct if max_slippage_pct is not None else base.max_slippage_pct,
    )


class _SimulatedReconciliationClient:
    def get_instruments(self) -> list[dict[str, object]]:
        return [
            {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 99},
            {"Product1Symbol": "DOGE", "Product2Symbol": "CAD", "Symbol": "DOGECAD", "InstrumentId": 77},
        ]

    def fetch_balances(self, *, credentials):
        del credentials
        return 999, [
            NdaxBalance(product_symbol="CAD", amount=25.0, hold=0.0),
            NdaxBalance(product_symbol="SOL", amount=2.0, hold=0.0),
            NdaxBalance(product_symbol="DOGE", amount=0.0, hold=0.0),
        ]

    def get_recent_ticker_history(self, *, instrument_id, interval_seconds, lookback_hours):
        del interval_seconds, lookback_hours
        if instrument_id == 99:
            return [[1, 0, 0, 0, 111.0, 0], [2, 0, 0, 0, 123.45, 0]]
        return []


def _loop_count(state_db: Path) -> int:
    snapshot = _read_snapshot(state_db)
    if snapshot is None:
        return 0
    return int(snapshot.get("loop_count", 0))


def _run_status(state_db: Path) -> str | None:
    snapshot = _read_snapshot(state_db)
    if snapshot is None:
        return None
    value = snapshot.get("run_status")
    return str(value) if value is not None else None


def _read_snapshot(state_db: Path) -> dict[str, object] | None:
    try:
        return StateStore(state_db).get_snapshot()
    except Exception:
        return None


def _csv_data_rows(csv_file: Path) -> int:
    if not csv_file.exists():
        return 0
    lines = csv_file.read_text(encoding="utf-8").splitlines()
    return max(0, len(lines) - 1)


def _wait_for_exit(proc: subprocess.Popen[str], *, timeout_seconds: int) -> bool:
    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        return False
    return True


def _tail(value: str, *, limit: int = 220) -> str:
    raw = value.strip()
    if len(raw) <= limit:
        return raw
    return raw[-limit:]


@contextmanager
def _temporary_env(**overrides: str):
    previous: dict[str, str | None] = {}
    for key, value in overrides.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
