"""Risk controls for M7 hardening."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path

from qtbot.alerts import DiscordAlerter
from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control, write_control
from qtbot.state import StateStore


@dataclass(frozen=True)
class RiskAction:
    triggered: bool
    reason: str | None = None
    consecutive_error_count: int = 0


class RiskManager:
    """Enforces daily loss cap, slippage guard pauses, and error kill-switch."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
        control_file: Path,
        logger: logging.Logger,
        alerter: DiscordAlerter | None = None,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._control_file = control_file
        self._logger = logger
        self._alerter = alerter

    def enforce_pre_cycle(self, *, now_utc: datetime) -> RiskAction:
        daily_realized_pnl = self._state_store.get_daily_realized_pnl(now_utc=now_utc)
        if daily_realized_pnl <= -self._config.daily_loss_cap_cad:
            reason = (
                "daily_loss_cap_breached "
                f"daily_realized_pnl_cad={daily_realized_pnl:.12g} "
                f"loss_cap_cad={self._config.daily_loss_cap_cad:.12g}"
            )
            self._pause_trading(
                reason=reason,
                event_type="RISK_DAILY_LOSS_CAP_BREACHED",
            )
            return RiskAction(triggered=True, reason=reason, consecutive_error_count=0)
        return RiskAction(triggered=False)

    def record_cycle_success(self, *, now_utc: datetime, reason: str) -> None:
        reset = self._state_store.reset_consecutive_errors(
            now_utc=now_utc,
            reason=reason,
        )
        if reset:
            self._logger.info("Risk counter reset after successful cycle. reason=%s", reason)

    def record_cycle_errors(self, *, now_utc: datetime, error_count: int, reason: str) -> RiskAction:
        if error_count <= 0:
            return RiskAction(triggered=False)

        consecutive = self._state_store.increment_consecutive_errors(
            now_utc=now_utc,
            by_count=error_count,
            reason=reason,
        )
        self._logger.warning(
            "Risk error counter incremented. consecutive_error_count=%s reason=%s",
            consecutive,
            reason,
        )
        if consecutive >= self._config.consecutive_error_limit:
            kill_reason = (
                "consecutive_error_limit_breached "
                f"consecutive_error_count={consecutive} "
                f"limit={self._config.consecutive_error_limit}"
            )
            self._pause_trading(
                reason=kill_reason,
                event_type="RISK_CONSECUTIVE_ERROR_LIMIT_BREACHED",
            )
            return RiskAction(triggered=True, reason=kill_reason, consecutive_error_count=consecutive)
        if consecutive >= 2:
            self._notify(
                category="REPEATED_API_FAILURES",
                summary="repeated execution/api failures detected",
                severity="WARNING",
                detail=f"consecutive_error_count={consecutive} reason={reason}",
            )
        return RiskAction(triggered=False, consecutive_error_count=consecutive)

    def handle_slippage_breach(
        self,
        *,
        now_utc: datetime,
        breach_count: int,
        max_slippage_seen: float,
    ) -> RiskAction:
        if breach_count <= 0:
            return RiskAction(triggered=False)

        self._state_store.increment_consecutive_errors(
            now_utc=now_utc,
            by_count=breach_count,
            reason="slippage_guard_breach",
        )
        reason = (
            "slippage_guard_breached "
            f"breach_count={breach_count} "
            f"max_slippage_pct={max_slippage_seen:.6g} "
            f"allowed_slippage_pct={self._config.max_slippage_pct:.6g}"
        )
        self._pause_trading(
            reason=reason,
            event_type="RISK_SLIPPAGE_GUARD_BREACHED",
        )
        return RiskAction(triggered=True, reason=reason, consecutive_error_count=breach_count)

    def _pause_trading(self, *, reason: str, event_type: str) -> None:
        if read_control(self._control_file).command != Command.PAUSE:
            write_control(
                self._control_file,
                Command.PAUSE,
                updated_by="risk_guard",
                reason=reason,
            )
        self._state_store.add_event(
            event_type=event_type,
            detail=reason,
        )
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self._state_store.set_status(
            run_status="PAUSED",
            last_command=Command.PAUSE.value,
            event_detail=f"{reason} at={now}",
        )
        self._logger.error("Risk manager paused trading. %s", reason)
        self._notify(
            category="RISK_HALT",
            summary="trading paused by risk manager",
            severity="ERROR",
            detail=f"{event_type}: {reason}",
        )

    def _notify(
        self,
        *,
        category: str,
        summary: str,
        severity: str,
        detail: str,
    ) -> None:
        if self._alerter is None:
            return
        self._alerter.send(
            category=category,
            summary=summary,
            severity=severity,
            detail=detail,
        )
