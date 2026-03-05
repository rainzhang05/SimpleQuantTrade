"""M11 production cutover checklist orchestration."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import shutil
from typing import Any

from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control, write_control
from qtbot.ndax_client import NdaxAuthenticationError, NdaxClient, NdaxError, load_credentials_from_env
from qtbot.preflight import GoLivePreflight
from qtbot.state import StateStore

_RUNBOOK_REQUIRED_MARKERS = (
    "## Rollback Procedure",
    "## Incident Response",
)

_LIVE_STAGING_REQUIRED_STEPS = {
    "public_ndax_health_check",
    "dry_run_lifecycle_drill",
    "cli_failure_scenario_invalid_symbol",
    "reconciliation_fault_simulation",
    "risk_fault_simulation",
}

_OFFLINE_STAGING_REQUIRED_STEPS = {
    "offline_control_plane_drill",
    "reconciliation_fault_simulation",
    "risk_fault_simulation",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class CutoverCheckResult:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class ProductionCutoverReport:
    started_at_utc: str
    completed_at_utc: str
    runtime_dir: Path
    report_file: Path
    checks: list[CutoverCheckResult]
    passed: bool
    message: str
    start_budget_cad: float
    launch_commands: list[str]
    manual_verification_checklist: list[str]
    rollback_commands: list[str]

    def to_payload(self) -> dict[str, object]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "runtime_dir": str(self.runtime_dir),
            "report_file": str(self.report_file),
            "passed": self.passed,
            "message": self.message,
            "checks": [
                {
                    "name": item.name,
                    "passed": item.passed,
                    "detail": item.detail,
                }
                for item in self.checks
            ],
            "start_budget_cad": self.start_budget_cad,
            "launch_commands": self.launch_commands,
            "manual_verification_checklist": self.manual_verification_checklist,
            "rollback_commands": self.rollback_commands,
        }


class ProductionCutoverChecklist:
    """Runs production cutover readiness checks and produces an operator report."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        working_dir: Path | None = None,
    ) -> None:
        self._config = config
        self._working_dir = (working_dir or Path.cwd()).resolve()
        self._logger = logging.getLogger("qtbot.cutover")
        if not self._logger.handlers:
            self._logger.addHandler(logging.NullHandler())

    def run(
        self,
        *,
        start_budget_cad: float,
        staging_max_age_hours: int,
        offline_only: bool,
        require_discord: bool,
    ) -> ProductionCutoverReport:
        if start_budget_cad <= 0:
            raise ValueError("start_budget_cad must be > 0.")
        if staging_max_age_hours <= 0:
            raise ValueError("staging_max_age_hours must be > 0.")

        started_at = _utc_now_iso()
        runtime_dir = (self._config.runtime_dir / "production_cutover").resolve()
        self._reset_runtime_dir(runtime_dir)

        checks: list[CutoverCheckResult] = []
        checks.append(
            self._check_staging_report(
                staging_max_age_hours=staging_max_age_hours,
                offline_only=offline_only,
            )
        )
        checks.append(self._check_runbook_presence())
        checks.append(self._check_local_state_and_control_health(runtime_dir=runtime_dir))
        checks.append(
            self._check_discord_configuration(require_discord=require_discord, offline_only=offline_only)
        )

        if not offline_only:
            checks.append(self._check_credentials_presence())
            checks.append(self._check_ndax_private_connectivity())
            checks.append(
                self._check_preflight_gate(
                    runtime_dir=runtime_dir,
                    start_budget_cad=start_budget_cad,
                )
            )

        failed = [item.name for item in checks if not item.passed]
        passed = len(failed) == 0
        message = (
            f"production_cutover_ready checks={len(checks)}"
            if passed
            else f"production_cutover_blocked failed_checks={','.join(failed)}"
        )
        completed_at = _utc_now_iso()

        report_file = runtime_dir / "logs" / "production_cutover_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report = ProductionCutoverReport(
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            runtime_dir=runtime_dir,
            report_file=report_file,
            checks=checks,
            passed=passed,
            message=message,
            start_budget_cad=start_budget_cad,
            launch_commands=_launch_commands(start_budget_cad=start_budget_cad),
            manual_verification_checklist=_manual_verification_checklist(),
            rollback_commands=_rollback_commands(),
        )
        report_file.write_text(
            json.dumps(report.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return report

    def _check_staging_report(
        self,
        *,
        staging_max_age_hours: int,
        offline_only: bool,
    ) -> CutoverCheckResult:
        report_path = self._config.runtime_dir / "staging_validation" / "logs" / "staging_validation_report.json"
        if not report_path.exists():
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=f"missing report at {report_path}",
            )

        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=f"invalid report payload: {exc}",
            )

        if not bool(payload.get("passed")):
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=f"staging report not passed: {payload.get('message')}",
            )

        completed_raw = payload.get("completed_at_utc")
        completed = _parse_utc_iso(completed_raw)
        if completed is None:
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=f"invalid completed_at_utc value: {completed_raw!r}",
            )
        age_hours = (datetime.now(timezone.utc) - completed).total_seconds() / 3600.0
        if age_hours > float(staging_max_age_hours):
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=(
                    "staging report too old "
                    f"age_hours={age_hours:.2f} max_age_hours={staging_max_age_hours}"
                ),
            )

        required_step_sets = (
            [_OFFLINE_STAGING_REQUIRED_STEPS, _LIVE_STAGING_REQUIRED_STEPS]
            if offline_only
            else [_LIVE_STAGING_REQUIRED_STEPS]
        )
        step_status = {
            str(item.get("name")): bool(item.get("passed"))
            for item in payload.get("steps", [])
            if isinstance(item, dict)
        }
        matched_required_count = 0
        validation_error: str | None = None
        for required_steps in required_step_sets:
            missing = sorted(step for step in required_steps if step not in step_status)
            failed = sorted(step for step in required_steps if step_status.get(step) is False)
            if not missing and not failed:
                matched_required_count = len(required_steps)
                validation_error = None
                break
            validation_error = f"missing_steps={missing} failed_steps={failed}"
        if validation_error is not None:
            return CutoverCheckResult(
                name="staging_validation_report",
                passed=False,
                detail=validation_error,
            )

        return CutoverCheckResult(
            name="staging_validation_report",
            passed=True,
            detail=(
                f"staging report fresh age_hours={age_hours:.2f} "
                f"required_steps={matched_required_count}"
            ),
        )

    def _check_runbook_presence(self) -> CutoverCheckResult:
        runbook_path = self._working_dir / "docs" / "PRODUCTION_RUNBOOK.md"
        if not runbook_path.exists():
            return CutoverCheckResult(
                name="production_runbook_presence",
                passed=False,
                detail=f"missing runbook at {runbook_path}",
            )
        content = runbook_path.read_text(encoding="utf-8")
        missing = [marker for marker in _RUNBOOK_REQUIRED_MARKERS if marker not in content]
        if missing:
            return CutoverCheckResult(
                name="production_runbook_presence",
                passed=False,
                detail=f"runbook missing required sections: {missing}",
            )
        return CutoverCheckResult(
            name="production_runbook_presence",
            passed=True,
            detail=f"runbook present at {runbook_path}",
        )

    def _check_local_state_and_control_health(self, *, runtime_dir: Path) -> CutoverCheckResult:
        health_runtime = runtime_dir / "local_healthcheck"
        self._reset_runtime_dir(health_runtime)
        health_config = _derive_runtime_config(self._config, health_runtime)

        write_control(
            health_config.control_file,
            Command.PAUSE,
            updated_by="cutover_check",
            reason="healthcheck pause write",
        )
        if read_control(health_config.control_file).command != Command.PAUSE:
            return CutoverCheckResult(
                name="local_state_control_health",
                passed=False,
                detail="control write/read mismatch for PAUSE",
            )
        write_control(
            health_config.control_file,
            Command.RUN,
            updated_by="cutover_check",
            reason="healthcheck run write",
        )
        if read_control(health_config.control_file).command != Command.RUN:
            return CutoverCheckResult(
                name="local_state_control_health",
                passed=False,
                detail="control write/read mismatch for RUN",
            )

        state_store = StateStore(health_config.state_db)
        state_store.initialize(initial_budget_cad=100.0)
        snapshot = state_store.get_snapshot()
        if snapshot is None:
            return CutoverCheckResult(
                name="local_state_control_health",
                passed=False,
                detail="state snapshot missing after initialization",
            )

        return CutoverCheckResult(
            name="local_state_control_health",
            passed=True,
            detail=(
                "control/state health passed "
                f"run_status={snapshot.get('run_status')} loop_count={snapshot.get('loop_count')}"
            ),
        )

    def _check_discord_configuration(self, *, require_discord: bool, offline_only: bool) -> CutoverCheckResult:
        if offline_only:
            return CutoverCheckResult(
                name="discord_alerting_configuration",
                passed=True,
                detail="offline-only mode: discord requirement skipped",
            )

        webhook = (self._config.discord_webhook_url or "").strip()
        if webhook:
            return CutoverCheckResult(
                name="discord_alerting_configuration",
                passed=True,
                detail="discord webhook configured",
            )
        if require_discord:
            return CutoverCheckResult(
                name="discord_alerting_configuration",
                passed=False,
                detail="QTBOT_DISCORD_WEBHOOK_URL is required but unset",
            )
        return CutoverCheckResult(
            name="discord_alerting_configuration",
            passed=True,
            detail="discord webhook unset (allowed; manual alerting verification still required)",
        )

    def _check_credentials_presence(self) -> CutoverCheckResult:
        try:
            load_credentials_from_env()
        except (NdaxAuthenticationError, ValueError) as exc:
            return CutoverCheckResult(
                name="credentials_presence",
                passed=False,
                detail=f"missing/invalid credentials: {exc}",
            )
        return CutoverCheckResult(
            name="credentials_presence",
            passed=True,
            detail="NDAX credentials loaded from environment",
        )

    def _check_ndax_private_connectivity(self) -> CutoverCheckResult:
        client = NdaxClient(
            base_url=self._config.ndax_base_url,
            oms_id=self._config.ndax_oms_id,
            timeout_seconds=self._config.ndax_timeout_seconds,
            max_retries=self._config.ndax_max_retries,
        )
        try:
            credentials = load_credentials_from_env()
            account_id, balances = client.fetch_balances(credentials=credentials)
            cad_available = 0.0
            for item in balances:
                if item.product_symbol == "CAD":
                    cad_available = item.available
                    break
        except (NdaxAuthenticationError, NdaxError, ValueError) as exc:
            return CutoverCheckResult(
                name="ndax_private_connectivity",
                passed=False,
                detail=f"private NDAX check failed: {exc}",
            )
        return CutoverCheckResult(
            name="ndax_private_connectivity",
            passed=True,
            detail=(
                f"private NDAX check passed account_id={account_id} "
                f"balance_count={len(balances)} cad_available={cad_available:.12g}"
            ),
        )

    def _check_preflight_gate(self, *, runtime_dir: Path, start_budget_cad: float) -> CutoverCheckResult:
        preflight_runtime = runtime_dir / "preflight_runtime"
        self._reset_runtime_dir(preflight_runtime)
        preflight_config = _derive_runtime_config(self._config, preflight_runtime)
        state_store = StateStore(preflight_config.state_db)
        state_store.initialize(initial_budget_cad=start_budget_cad)
        write_control(
            preflight_config.control_file,
            Command.RUN,
            updated_by="cutover_check",
            reason="preflight cutover validation",
        )
        client = NdaxClient(
            base_url=preflight_config.ndax_base_url,
            oms_id=preflight_config.ndax_oms_id,
            timeout_seconds=preflight_config.ndax_timeout_seconds,
            max_retries=preflight_config.ndax_max_retries,
        )
        summary = GoLivePreflight(
            config=preflight_config,
            ndax_client=client,
            state_store=state_store,
            logger=self._logger,
        ).run()
        if not summary.passed:
            return CutoverCheckResult(
                name="go_live_preflight_check",
                passed=False,
                detail=summary.message,
            )
        return CutoverCheckResult(
            name="go_live_preflight_check",
            passed=True,
            detail=summary.message,
        )

    def _reset_runtime_dir(self, runtime_dir: Path) -> None:
        if runtime_dir.exists():
            shutil.rmtree(runtime_dir)
        runtime_dir.mkdir(parents=True, exist_ok=True)


def _parse_utc_iso(raw_value: object) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _derive_runtime_config(base: RuntimeConfig, runtime_dir: Path) -> RuntimeConfig:
    return replace(
        base,
        runtime_dir=runtime_dir,
        control_file=runtime_dir / "control.json",
        state_db=runtime_dir / "state.sqlite",
        log_file=runtime_dir / "logs" / "qtbot.log",
        pid_file=runtime_dir / "runner.pid",
    )


def _launch_commands(*, start_budget_cad: float) -> list[str]:
    return [
        f"PYTHONPATH=src python3 -m qtbot start --budget {start_budget_cad:.12g}",
        f"docker compose up -d qtbot --scale qtbot=1 "
        f"(set QTBOT_START_BUDGET_CAD={start_budget_cad:.12g} in environment)",
    ]


def _manual_verification_checklist() -> list[str]:
    return [
        "Confirm first live ENTER/EXIT fills appear in runtime/logs/trades.csv with expected fees.",
        "Confirm runtime/state.sqlite totals (bot_cash_cad, realized_pnl_cad, fees_paid_cad) are consistent with fills.",
        "Confirm Discord receives lifecycle and risk/reconciliation alerts (or manual operator alert path is active).",
        "Confirm initial live cycles match expected strategy decisions and risk constraints.",
    ]


def _rollback_commands() -> list[str]:
    return [
        "PYTHONPATH=src python3 -m qtbot stop",
        "docker compose exec qtbot qtbot stop",
        "Verify status transitions to STOPPED before restart attempts.",
    ]
