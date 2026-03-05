from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from qtbot.strategy.indicators import IndicatorSnapshot
from qtbot.strategy.signals import (
    EntryCandidate,
    candidate_to_decision,
    empty_position,
    evaluate_entry_or_hold,
    evaluate_exit_or_hold,
)


class SignalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc)
        self.up_indicator = IndicatorSnapshot(close=100.0, ema_fast=101.0, ema_slow=99.0, atr=2.0)
        self.down_indicator = IndicatorSnapshot(close=100.0, ema_fast=98.0, ema_slow=99.0, atr=2.0)

    def test_entry_candidate_created_when_all_conditions_met(self) -> None:
        result = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=empty_position("SOL"),
            cooldown_minutes=30,
        )
        self.assertIsInstance(result, EntryCandidate)
        assert isinstance(result, EntryCandidate)
        self.assertEqual(result.reason, "entry_conditions_met")

    def test_entry_hold_reasons(self) -> None:
        no_data = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=None,
            position=empty_position("SOL"),
            cooldown_minutes=30,
        )
        self.assertEqual(no_data.reason, "insufficient_data")

        opened = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=empty_position("SOL").__class__(
                symbol="SOL",
                qty=1.0,
                avg_entry_price=100.0,
                entry_time=self.now.isoformat(),
                last_exit_time=None,
            ),
            cooldown_minutes=30,
        )
        self.assertEqual(opened.reason, "position_open")

        cooldown = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=empty_position("SOL").__class__(
                symbol="SOL",
                qty=0.0,
                avg_entry_price=0.0,
                entry_time=None,
                last_exit_time=(self.now - timedelta(minutes=5)).isoformat(),
            ),
            cooldown_minutes=30,
        )
        self.assertEqual(cooldown.reason, "cooldown_active")

        trend_not_up = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.down_indicator,
            position=empty_position("SOL"),
            cooldown_minutes=30,
        )
        self.assertEqual(trend_not_up.reason, "trend_not_up")

        no_pullback_indicator = IndicatorSnapshot(close=102.0, ema_fast=101.0, ema_slow=99.0, atr=2.0)
        no_pullback = evaluate_entry_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=no_pullback_indicator,
            position=empty_position("SOL"),
            cooldown_minutes=30,
        )
        self.assertEqual(no_pullback.reason, "no_pullback")

    def test_exit_conditions(self) -> None:
        no_position = evaluate_exit_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=empty_position("SOL"),
            stop_k=2.5,
            max_hold_hours=48,
        )
        self.assertEqual(no_position.reason, "no_position")

        position = empty_position("SOL").__class__(
            symbol="SOL",
            qty=1.0,
            avg_entry_price=100.0,
            entry_time=(self.now - timedelta(hours=1)).isoformat(),
            last_exit_time=None,
        )
        trend_break = evaluate_exit_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.down_indicator,
            position=position,
            stop_k=2.5,
            max_hold_hours=48,
        )
        self.assertEqual(trend_break.reason, "trend_break")

        atr_stop_indicator = IndicatorSnapshot(close=94.0, ema_fast=101.0, ema_slow=99.0, atr=2.0)
        atr_stop = evaluate_exit_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=atr_stop_indicator,
            position=position,
            stop_k=2.5,
            max_hold_hours=48,
        )
        self.assertEqual(atr_stop.reason, "atr_stop")

        time_stop = evaluate_exit_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=position.__class__(
                symbol="SOL",
                qty=1.0,
                avg_entry_price=100.0,
                entry_time=(self.now - timedelta(hours=49)).isoformat(),
                last_exit_time=None,
            ),
            stop_k=2.5,
            max_hold_hours=48,
        )
        self.assertEqual(time_stop.reason, "time_stop")

        hold = evaluate_exit_or_hold(
            now_utc=self.now,
            symbol="SOLCAD",
            indicator=self.up_indicator,
            position=position,
            stop_k=2.5,
            max_hold_hours=48,
        )
        self.assertEqual(hold.reason, "hold_position")

    def test_candidate_to_decision_copies_fields(self) -> None:
        candidate = EntryCandidate(
            symbol="SOLCAD",
            close=100.0,
            ema_fast=101.0,
            ema_slow=99.0,
            atr=2.0,
            score=0.02,
            reason="entry_conditions_met",
        )
        decision = candidate_to_decision(
            candidate,
            now_utc=self.now,
            signal="ENTER",
            reason="entry_conditions_met",
        )
        self.assertEqual(decision.signal, "ENTER")
        self.assertAlmostEqual(decision.score or 0.0, 0.02)


if __name__ == "__main__":
    unittest.main()
