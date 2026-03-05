"""Indicator calculations for M3 dry-run strategy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class IndicatorSnapshot:
    close: float
    ema_fast: float
    ema_slow: float
    atr: float


def ema(values: Sequence[float], period: int) -> list[float]:
    if period <= 0:
        raise ValueError("EMA period must be > 0.")
    if not values:
        return []

    alpha = 2.0 / (period + 1.0)
    result: list[float] = []
    current = float(values[0])
    result.append(current)
    for value in values[1:]:
        current = (float(value) * alpha) + (current * (1.0 - alpha))
        result.append(current)
    return result


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int) -> list[float]:
    if period <= 0:
        raise ValueError("ATR period must be > 0.")
    if not highs or len(highs) != len(lows) or len(highs) != len(closes):
        return []

    true_ranges: list[float] = []
    prev_close = float(closes[0])
    for index in range(len(highs)):
        high = float(highs[index])
        low = float(lows[index])
        close = float(closes[index])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
        prev_close = close

    result: list[float] = []
    running_atr = true_ranges[0]
    result.append(running_atr)
    for tr in true_ranges[1:]:
        running_atr = ((running_atr * (period - 1)) + tr) / period
        result.append(running_atr)
    return result


def latest_snapshot(
    *,
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    ema_fast_period: int,
    ema_slow_period: int,
    atr_period: int,
) -> IndicatorSnapshot | None:
    needed = max(ema_fast_period, ema_slow_period, atr_period)
    if len(closes) < needed or len(highs) < needed or len(lows) < needed:
        return None

    ema_fast_values = ema(closes, ema_fast_period)
    ema_slow_values = ema(closes, ema_slow_period)
    atr_values = atr(highs, lows, closes, atr_period)
    if not ema_fast_values or not ema_slow_values or not atr_values:
        return None

    return IndicatorSnapshot(
        close=float(closes[-1]),
        ema_fast=float(ema_fast_values[-1]),
        ema_slow=float(ema_slow_values[-1]),
        atr=float(atr_values[-1]),
    )
