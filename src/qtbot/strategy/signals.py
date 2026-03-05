"""Signal generation rules for M3 dry-run mode."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from qtbot.strategy.indicators import IndicatorSnapshot

Signal = Literal["ENTER", "EXIT", "HOLD"]


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    qty: float
    avg_entry_price: float
    entry_time: str | None
    last_exit_time: str | None


@dataclass(frozen=True)
class Decision:
    timestamp_utc: str
    symbol: str
    close: float
    ema_fast: float
    ema_slow: float
    atr: float
    signal: Signal
    reason: str
    score: float | None = None


@dataclass(frozen=True)
class EntryCandidate:
    symbol: str
    close: float
    ema_fast: float
    ema_slow: float
    atr: float
    score: float
    reason: str


def evaluate_entry_or_hold(
    *,
    now_utc: datetime,
    symbol: str,
    indicator: IndicatorSnapshot | None,
    position: PositionSnapshot,
    cooldown_minutes: int,
) -> Decision | EntryCandidate:
    timestamp = now_utc.replace(microsecond=0).isoformat()
    if indicator is None:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=0.0,
            ema_fast=0.0,
            ema_slow=0.0,
            atr=0.0,
            signal="HOLD",
            reason="insufficient_data",
        )

    if position.qty > 0:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="HOLD",
            reason="position_open",
        )

    if not _cooldown_satisfied(now_utc=now_utc, last_exit_time=position.last_exit_time, cooldown_minutes=cooldown_minutes):
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="HOLD",
            reason="cooldown_active",
        )

    if not (indicator.ema_fast > indicator.ema_slow):
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="HOLD",
            reason="trend_not_up",
        )

    if not (indicator.close <= indicator.ema_fast):
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="HOLD",
            reason="no_pullback",
        )

    score = _safe_score(indicator.ema_fast, indicator.ema_slow, indicator.close)
    return EntryCandidate(
        symbol=symbol,
        close=indicator.close,
        ema_fast=indicator.ema_fast,
        ema_slow=indicator.ema_slow,
        atr=indicator.atr,
        score=score,
        reason="entry_conditions_met",
    )


def evaluate_exit_or_hold(
    *,
    now_utc: datetime,
    symbol: str,
    indicator: IndicatorSnapshot | None,
    position: PositionSnapshot,
    stop_k: float,
    max_hold_hours: int,
) -> Decision:
    timestamp = now_utc.replace(microsecond=0).isoformat()
    if indicator is None:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=0.0,
            ema_fast=0.0,
            ema_slow=0.0,
            atr=0.0,
            signal="HOLD",
            reason="insufficient_data",
        )

    if position.qty <= 0:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="HOLD",
            reason="no_position",
        )

    if indicator.ema_fast < indicator.ema_slow:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="EXIT",
            reason="trend_break",
        )

    stop_price = position.avg_entry_price - (stop_k * indicator.atr)
    if indicator.close < stop_price:
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="EXIT",
            reason="atr_stop",
        )

    if _is_time_stop_triggered(now_utc=now_utc, entry_time=position.entry_time, max_hold_hours=max_hold_hours):
        return Decision(
            timestamp_utc=timestamp,
            symbol=symbol,
            close=indicator.close,
            ema_fast=indicator.ema_fast,
            ema_slow=indicator.ema_slow,
            atr=indicator.atr,
            signal="EXIT",
            reason="time_stop",
        )

    return Decision(
        timestamp_utc=timestamp,
        symbol=symbol,
        close=indicator.close,
        ema_fast=indicator.ema_fast,
        ema_slow=indicator.ema_slow,
        atr=indicator.atr,
        signal="HOLD",
        reason="hold_position",
    )


def candidate_to_decision(candidate: EntryCandidate, *, now_utc: datetime, signal: Signal, reason: str) -> Decision:
    return Decision(
        timestamp_utc=now_utc.replace(microsecond=0).isoformat(),
        symbol=candidate.symbol,
        close=candidate.close,
        ema_fast=candidate.ema_fast,
        ema_slow=candidate.ema_slow,
        atr=candidate.atr,
        signal=signal,
        reason=reason,
        score=candidate.score,
    )


def empty_position(symbol: str) -> PositionSnapshot:
    return PositionSnapshot(
        symbol=symbol,
        qty=0.0,
        avg_entry_price=0.0,
        entry_time=None,
        last_exit_time=None,
    )


def _cooldown_satisfied(*, now_utc: datetime, last_exit_time: str | None, cooldown_minutes: int) -> bool:
    if not last_exit_time:
        return True
    exit_dt = _parse_iso_utc(last_exit_time)
    if exit_dt is None:
        return True
    return now_utc >= exit_dt + timedelta(minutes=cooldown_minutes)


def _is_time_stop_triggered(*, now_utc: datetime, entry_time: str | None, max_hold_hours: int) -> bool:
    if not entry_time:
        return False
    entry_dt = _parse_iso_utc(entry_time)
    if entry_dt is None:
        return False
    return now_utc > entry_dt + timedelta(hours=max_hold_hours)


def _parse_iso_utc(raw: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _safe_score(ema_fast: float, ema_slow: float, close: float) -> float:
    if close == 0:
        return 0.0
    return (ema_fast - ema_slow) / close
