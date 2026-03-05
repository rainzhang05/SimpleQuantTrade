"""Strategy cycle execution for signals and decision generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any

from qtbot.config import RuntimeConfig
from qtbot.decision_log import DecisionCsvLogger
from qtbot.ndax_client import NdaxClient, NdaxError
from qtbot.state import StateStore
from qtbot.strategy.indicators import latest_snapshot
from qtbot.strategy.signals import (
    Decision,
    EntryCandidate,
    PositionSnapshot,
    candidate_to_decision,
    empty_position,
    evaluate_entry_or_hold,
    evaluate_exit_or_hold,
)
from qtbot.universe import UniverseEntry, resolve_tradable_universe


@dataclass(frozen=True)
class StrategySummary:
    symbol_count: int
    enter_count: int
    exit_count: int
    hold_count: int
    skipped_count: int
    message: str
    decisions: list[Decision]
    tradable: list[UniverseEntry]


class StrategyEngine:
    """Runs a full strategy evaluation cycle and writes decisions.csv."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        decision_logger: DecisionCsvLogger,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._state_store = state_store
        self._decision_logger = decision_logger

    def evaluate_cycle(self, *, now_utc: datetime | None = None) -> StrategySummary:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        instruments = self._ndax_client.get_instruments()
        resolution = resolve_tradable_universe(instruments)
        tradable = resolution.tradable
        if not tradable:
            raise NdaxError("No tradable NDAX CAD symbols available for strategy cycle.")

        positions = self._state_store.get_positions()
        lookback_hours = self._calculate_lookback_hours()
        provisional: list[Decision] = []
        entry_candidates: list[EntryCandidate] = []

        for entry in tradable:
            position = positions.get(entry.ticker, empty_position(entry.ticker))
            indicator = self._load_indicator(entry=entry, lookback_hours=lookback_hours)
            if position.qty > 0:
                decision = evaluate_exit_or_hold(
                    now_utc=now,
                    symbol=entry.ndax_symbol,
                    indicator=indicator,
                    position=position,
                    stop_k=self._config.stop_k,
                    max_hold_hours=self._config.max_hold_hours,
                )
                provisional.append(decision)
                continue

            candidate_or_decision = evaluate_entry_or_hold(
                now_utc=now,
                symbol=entry.ndax_symbol,
                indicator=indicator,
                position=position,
                cooldown_minutes=self._config.cooldown_minutes,
            )
            if isinstance(candidate_or_decision, EntryCandidate):
                entry_candidates.append(candidate_or_decision)
            else:
                provisional.append(candidate_or_decision)

        selected_enters, capped_holds = self._apply_entry_cap(
            candidates=entry_candidates,
            now_utc=now,
            max_entries=self._config.max_new_entries_per_cycle,
        )
        decisions = sorted(provisional + selected_enters + capped_holds, key=lambda item: item.symbol)
        self._decision_logger.append_many(decisions)

        enter_count = sum(1 for item in decisions if item.signal == "ENTER")
        exit_count = sum(1 for item in decisions if item.signal == "EXIT")
        hold_count = sum(1 for item in decisions if item.signal == "HOLD")
        skipped_count = len(resolution.skipped)
        message = (
            "decisions_persisted "
            f"symbols={len(tradable)} enter={enter_count} exit={exit_count} hold={hold_count} skipped={skipped_count}"
        )
        return StrategySummary(
            symbol_count=len(tradable),
            enter_count=enter_count,
            exit_count=exit_count,
            hold_count=hold_count,
            skipped_count=skipped_count,
            message=message,
            decisions=decisions,
            tradable=tradable,
        )

    def _load_indicator(self, *, entry: UniverseEntry, lookback_hours: int):
        try:
            candles = self._ndax_client.get_recent_ticker_history(
                instrument_id=entry.instrument_id,
                interval_seconds=self._config.signal_interval_seconds,
                lookback_hours=lookback_hours,
            )
        except NdaxError:
            return None

        normalized = _normalize_candles(candles)
        highs = [row["high"] for row in normalized]
        lows = [row["low"] for row in normalized]
        closes = [row["close"] for row in normalized]
        return latest_snapshot(
            highs=highs,
            lows=lows,
            closes=closes,
            ema_fast_period=self._config.ema_fast_period,
            ema_slow_period=self._config.ema_slow_period,
            atr_period=self._config.atr_period,
        )

    def _apply_entry_cap(
        self,
        *,
        candidates: list[EntryCandidate],
        now_utc: datetime,
        max_entries: int,
    ) -> tuple[list[Decision], list[Decision]]:
        if not candidates:
            return [], []

        sorted_candidates = sorted(candidates, key=lambda item: (-item.score, item.symbol))
        chosen = sorted_candidates[:max_entries]
        deferred = sorted_candidates[max_entries:]
        enters = [
            candidate_to_decision(candidate, now_utc=now_utc, signal="ENTER", reason=candidate.reason)
            for candidate in chosen
        ]
        holds = [
            candidate_to_decision(candidate, now_utc=now_utc, signal="HOLD", reason="entry_cap_reached")
            for candidate in deferred
        ]
        return enters, holds

    def _calculate_lookback_hours(self) -> int:
        needed_minutes = max(self._config.ema_slow_period, self._config.atr_period) + 120
        needed_hours = math.ceil(needed_minutes / 60)
        return max(12, needed_hours)


def _normalize_candles(candles: list[list[Any]]) -> list[dict[str, float]]:
    by_ts: dict[int, dict[str, float]] = {}
    for row in candles:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            ts = int(row[0])
            high = float(row[1])
            low = float(row[2])
            close = float(row[4])
        except (TypeError, ValueError):
            continue
        by_ts[ts] = {"high": high, "low": low, "close": close}

    result: list[dict[str, float]] = []
    for ts in sorted(by_ts):
        result.append(by_ts[ts])
    return result
