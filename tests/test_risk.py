from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.risk import RiskManager
from tests._helpers import make_runtime_config


@dataclass
class _FakeStateStore:
    daily_realized_pnl: float = 0.0
    consecutive_errors: int = 0
    events: list[tuple[str, str]] = field(default_factory=list)
    status_updates: list[tuple[str, str, str]] = field(default_factory=list)

    def get_daily_realized_pnl(self, *, now_utc: datetime) -> float:
        del now_utc
        return self.daily_realized_pnl

    def increment_consecutive_errors(self, *, now_utc: datetime, by_count: int, reason: str) -> int:
        del now_utc, reason
        self.consecutive_errors += by_count
        return self.consecutive_errors

    def reset_consecutive_errors(self, *, now_utc: datetime, reason: str) -> bool:
        del now_utc, reason
        had_errors = self.consecutive_errors > 0
        self.consecutive_errors = 0
        return had_errors

    def add_event(self, *, event_type: str, detail: str) -> None:
        self.events.append((event_type, detail))

    def set_status(self, *, run_status: str, last_command: str, event_detail: str) -> None:
        self.status_updates.append((run_status, last_command, event_detail))


class RiskManagerTests(unittest.TestCase):
    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def test_daily_loss_cap_breach_pauses_trading(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), daily_loss_cap_cad=100.0)
            store = _FakeStateStore(daily_realized_pnl=-120.0)
            manager = RiskManager(
                config=cfg,
                state_store=store,  # type: ignore[arg-type]
                control_file=cfg.control_file,
                logger=mock.Mock(),
            )

            with mock.patch("qtbot.risk.read_control", return_value=mock.Mock(command="STOP")), mock.patch(
                "qtbot.risk.write_control"
            ) as write_control_mock:
                action = manager.enforce_pre_cycle(now_utc=self._now())

            self.assertTrue(action.triggered)
            write_control_mock.assert_called_once()
            self.assertTrue(any(event[0] == "RISK_DAILY_LOSS_CAP_BREACHED" for event in store.events))

    def test_consecutive_error_limit_pause(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), consecutive_error_limit=3)
            store = _FakeStateStore(consecutive_errors=2)
            manager = RiskManager(
                config=cfg,
                state_store=store,  # type: ignore[arg-type]
                control_file=cfg.control_file,
                logger=mock.Mock(),
            )

            with mock.patch("qtbot.risk.read_control", return_value=mock.Mock(command="STOP")), mock.patch(
                "qtbot.risk.write_control"
            ) as write_control_mock:
                action = manager.record_cycle_errors(
                    now_utc=self._now(),
                    error_count=1,
                    reason="cycle_failed",
                )

            self.assertTrue(action.triggered)
            self.assertEqual(store.consecutive_errors, 3)
            write_control_mock.assert_called_once()
            self.assertTrue(any(event[0] == "RISK_CONSECUTIVE_ERROR_LIMIT_BREACHED" for event in store.events))

    def test_repeated_error_alerts_before_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), consecutive_error_limit=5)
            store = _FakeStateStore(consecutive_errors=1)
            alerter = mock.Mock()
            manager = RiskManager(
                config=cfg,
                state_store=store,  # type: ignore[arg-type]
                control_file=cfg.control_file,
                logger=mock.Mock(),
                alerter=alerter,
            )
            action = manager.record_cycle_errors(
                now_utc=self._now(),
                error_count=1,
                reason="cycle_failed",
            )
            self.assertFalse(action.triggered)
            self.assertEqual(action.consecutive_error_count, 2)
            alerter.send.assert_called_once()

    def test_slippage_breach_pauses_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td), max_slippage_pct=0.02)
            store = _FakeStateStore(consecutive_errors=0)
            manager = RiskManager(
                config=cfg,
                state_store=store,  # type: ignore[arg-type]
                control_file=cfg.control_file,
                logger=mock.Mock(),
            )

            with mock.patch("qtbot.risk.read_control", return_value=mock.Mock(command="STOP")), mock.patch(
                "qtbot.risk.write_control"
            ) as write_control_mock:
                action = manager.handle_slippage_breach(
                    now_utc=self._now(),
                    breach_count=1,
                    max_slippage_seen=0.05,
                )

            self.assertTrue(action.triggered)
            self.assertEqual(store.consecutive_errors, 1)
            write_control_mock.assert_called_once()
            self.assertTrue(any(event[0] == "RISK_SLIPPAGE_GUARD_BREACHED" for event in store.events))


if __name__ == "__main__":
    unittest.main()
