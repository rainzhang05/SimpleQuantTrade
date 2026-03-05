"""Long-running bot process for M1 control and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import os
from pathlib import Path
import signal
import time

from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control, write_control
from qtbot.decision_log import DecisionCsvLogger
from qtbot.execution import LiveExecutionEngine
from qtbot.logging_setup import configure_logging
from qtbot.ndax_client import NdaxAuthenticationError, NdaxClient, NdaxError
from qtbot.preflight import GoLivePreflight
from qtbot.reconciliation import StartupReconciler
from qtbot.state import StateStore
from qtbot.strategy.engine import StrategyEngine
from qtbot.trade_log import TradeCsvLogger


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class RunResult:
    loop_count: int


class RunnerAlreadyRunningError(RuntimeError):
    """Raised when a second runner instance is started."""


class RunnerPidLock:
    """Simple PID lock file to avoid duplicate runners."""

    def __init__(self, pid_file: Path) -> None:
        self._pid_file = pid_file
        self._pid = os.getpid()

    def __enter__(self) -> "RunnerPidLock":
        self._pid_file.parent.mkdir(parents=True, exist_ok=True)
        existing_pid = read_runner_pid(self._pid_file)
        if existing_pid is not None and is_pid_alive(existing_pid):
            raise RunnerAlreadyRunningError(
                f"qtbot runner already active with pid={existing_pid}."
            )
        self._pid_file.write_text(f"{self._pid}\n", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        existing_pid = read_runner_pid(self._pid_file)
        if existing_pid == self._pid:
            self._pid_file.unlink(missing_ok=True)


def read_runner_pid(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    try:
        raw_value = pid_file.read_text(encoding="utf-8").strip()
        if not raw_value:
            return None
        return int(raw_value)
    except (OSError, ValueError):
        return None


def is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        return exc.errno == errno.EPERM
    return True


class BotRunner:
    """Implements M1 lifecycle loop with control polling and persistence."""

    def __init__(self, *, config: RuntimeConfig, budget_cad: float) -> None:
        self._config = config
        self._budget_cad = budget_cad
        self._shutdown_requested = False

    def run(self) -> RunResult:
        self._config.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._config.log_file.parent.mkdir(parents=True, exist_ok=True)

        with RunnerPidLock(self._config.pid_file):
            logger = configure_logging(self._config.log_file)
            state_store = StateStore(self._config.state_db)
            state_store.initialize(initial_budget_cad=self._budget_cad)
            ndax_client = NdaxClient(
                base_url=self._config.ndax_base_url,
                oms_id=self._config.ndax_oms_id,
                timeout_seconds=self._config.ndax_timeout_seconds,
                max_retries=self._config.ndax_max_retries,
            )
            decision_logger = DecisionCsvLogger(self._config.runtime_dir / "logs" / "decisions.csv")
            trade_logger = TradeCsvLogger(self._config.runtime_dir / "logs" / "trades.csv")
            strategy_engine = StrategyEngine(
                config=self._config,
                ndax_client=ndax_client,
                state_store=state_store,
                decision_logger=decision_logger,
            )
            execution_engine = LiveExecutionEngine(
                config=self._config,
                ndax_client=ndax_client,
                state_store=state_store,
                trade_logger=trade_logger,
                logger=logger,
            )
            reconciler = StartupReconciler(
                config=self._config,
                ndax_client=ndax_client,
                state_store=state_store,
                logger=logger,
            )

            state_store.set_status(
                run_status="RECONCILING",
                last_command=Command.STOP.value,
                event_detail="startup reconciliation started",
            )
            try:
                reconciliation = reconciler.reconcile()
                logger.info("Startup reconciliation completed. %s", reconciliation.message)
                startup_event = reconciliation.message
            except (NdaxAuthenticationError, NdaxError) as exc:
                event_detail = f"startup_reconciliation_failed: {exc}"
                if self._config.enable_live_trading:
                    state_store.set_status(
                        run_status="ERROR",
                        last_command=Command.STOP.value,
                        event_detail=event_detail,
                    )
                    logger.error(
                        "Startup reconciliation failed in live mode. Blocking bot start: %s",
                        exc,
                    )
                    raise
                logger.warning(
                    "Startup reconciliation skipped in dry-run mode due to NDAX error: %s",
                    exc,
                )
                state_store.add_event(
                    event_type="RECONCILIATION_SKIPPED",
                    detail=event_detail,
                )
                startup_event = f"startup_reconciliation_skipped: {exc}"

            if self._config.enable_live_trading:
                state_store.set_status(
                    run_status="PREFLIGHT",
                    last_command=Command.STOP.value,
                    event_detail="go-live preflight started",
                )
                preflight = GoLivePreflight(
                    config=self._config,
                    ndax_client=ndax_client,
                    state_store=state_store,
                    logger=logger,
                )
                preflight_summary = preflight.run()
                if not preflight_summary.passed:
                    event_detail = f"go_live_preflight_failed: {preflight_summary.message}"
                    state_store.set_status(
                        run_status="ERROR",
                        last_command=Command.STOP.value,
                        event_detail=event_detail,
                    )
                    logger.error(
                        "Go-live preflight failed in live mode. Blocking bot start: %s",
                        preflight_summary.message,
                    )
                    raise NdaxError(preflight_summary.message)
                logger.info("Go-live preflight completed. %s", preflight_summary.message)
                startup_event = f"{startup_event}; {preflight_summary.message}"

            write_control(
                self._config.control_file,
                Command.RUN,
                updated_by="cli:start",
                reason="start command",
            )
            state_store.set_status(
                run_status="RUNNING",
                last_command=Command.RUN.value,
                event_detail=f"runner startup; {startup_event}",
            )

            self._install_signal_handlers(logger=logger)
            logger.info(
                "qtbot runner started with cadence=%ss budget_cad=%.2f",
                self._config.cadence_seconds,
                self._budget_cad,
            )

            paused = False
            next_loop_at = time.monotonic()
            loop_count = 0

            while True:
                command = read_control(self._config.control_file).command
                if self._shutdown_requested:
                    command = Command.STOP

                if command == Command.STOP:
                    state_store.set_status(
                        run_status="STOPPED",
                        last_command=Command.STOP.value,
                        event_detail="stop transition",
                    )
                    logger.info("Stop requested. Exiting gracefully.")
                    break

                if command == Command.PAUSE:
                    if not paused:
                        paused = True
                        state_store.set_status(
                            run_status="PAUSED",
                            last_command=Command.PAUSE.value,
                            event_detail="pause transition",
                        )
                        logger.info("Pause requested. Trading loop suspended.")
                    time.sleep(1.0)
                    continue

                if paused:
                    paused = False
                    state_store.set_status(
                        run_status="RUNNING",
                        last_command=Command.RUN.value,
                        event_detail="resume transition",
                    )
                    logger.info("Resume requested. Trading loop resumed.")
                    next_loop_at = time.monotonic()

                now = time.monotonic()
                if now < next_loop_at:
                    time.sleep(min(1.0, next_loop_at - now))
                    continue

                loop_started_dt = datetime.now(timezone.utc)
                loop_started_at = loop_started_dt.replace(microsecond=0).isoformat()
                event_detail = "cycle_completed"
                try:
                    summary = strategy_engine.evaluate_cycle(now_utc=loop_started_dt)
                    execution_summary = execution_engine.execute_decisions(
                        now_utc=loop_started_dt,
                        decisions=summary.decisions,
                        tradable=summary.tradable,
                    )
                    event_detail = f"{summary.message}; {execution_summary.message}"
                    logger.info("Strategy cycle completed. %s", summary.message)
                    if self._config.enable_live_trading:
                        logger.info("Execution cycle completed. %s", execution_summary.message)
                except NdaxError as exc:
                    event_detail = f"cycle_failed: {exc}"
                    logger.error("Cycle failed: %s", exc)
                except Exception as exc:  # pragma: no cover - defensive safety
                    event_detail = f"cycle_unexpected_error: {exc}"
                    logger.exception("Unexpected cycle failure: %s", exc)

                loop_completed_at = utc_now_iso()
                loop_count = state_store.record_loop(
                    last_command=Command.RUN.value,
                    loop_started_at_utc=loop_started_at,
                    loop_completed_at_utc=loop_completed_at,
                    event_detail=event_detail,
                )
                logger.info("Loop persisted. loop_count=%s", loop_count)

                now_after = time.monotonic()
                next_loop_at = max(next_loop_at + self._config.cadence_seconds, now_after)

            return RunResult(loop_count=loop_count)

    def _install_signal_handlers(self, *, logger) -> None:
        def _handler(signum, _frame) -> None:
            self._shutdown_requested = True
            logger.info("Signal received signum=%s. STOP requested.", signum)

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
