"""UTC calendar walk-forward fold construction."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class FoldDefinition:
    fold_index: int
    train_start_month: str
    train_end_month: str
    valid_start_month: str
    valid_end_month: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def build_walk_forward_folds(
    *,
    rows: pd.DataFrame,
    requested_folds: int,
    train_window_months: int,
    valid_window_months: int,
    step_months: int,
    interval_seconds: int,
) -> list[FoldDefinition]:
    if requested_folds <= 0:
        raise ValueError("requested_folds must be > 0")
    if train_window_months <= 0 or valid_window_months <= 0 or step_months <= 0:
        raise ValueError("walk-forward month windows must be > 0")
    if rows.empty:
        raise ValueError("cannot build folds from empty dataset")

    timestamp_min = int(rows["timestamp_ms"].min())
    timestamp_max = int(rows["timestamp_ms"].max())
    months = _full_month_range(
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
        interval_seconds=interval_seconds,
    )
    minimum_months = train_window_months + valid_window_months
    if len(months) < minimum_months:
        raise ValueError(
            f"insufficient full months for walk-forward folds: have={len(months)} need>={minimum_months}"
        )

    all_folds: list[FoldDefinition] = []
    last_valid_start = len(months) - valid_window_months
    fold_counter = 1
    for valid_start_idx in range(train_window_months, last_valid_start + 1, step_months):
        valid_end_idx = valid_start_idx + valid_window_months - 1
        all_folds.append(
            FoldDefinition(
                fold_index=fold_counter,
                train_start_month=months[valid_start_idx - train_window_months],
                train_end_month=months[valid_start_idx - 1],
                valid_start_month=months[valid_start_idx],
                valid_end_month=months[valid_end_idx],
            )
        )
        fold_counter += 1

    if not all_folds:
        raise ValueError("no eligible walk-forward folds available")

    selected = all_folds[-requested_folds:]
    return [
        FoldDefinition(
            fold_index=index,
            train_start_month=fold.train_start_month,
            train_end_month=fold.train_end_month,
            valid_start_month=fold.valid_start_month,
            valid_end_month=fold.valid_end_month,
        )
        for index, fold in enumerate(selected, start=1)
    ]


def month_mask(*, months: pd.Series, start_month: str, end_month: str) -> pd.Series:
    return (months >= start_month) & (months <= end_month)


def full_months_from_rows(*, rows: pd.DataFrame, interval_seconds: int) -> list[str]:
    if rows.empty:
        return []
    return _full_month_range(
        timestamp_min=int(rows["timestamp_ms"].min()),
        timestamp_max=int(rows["timestamp_ms"].max()),
        interval_seconds=interval_seconds,
    )


def _full_month_range(*, timestamp_min: int, timestamp_max: int, interval_seconds: int) -> list[str]:
    first_dt = datetime.fromtimestamp(timestamp_min / 1000, tz=timezone.utc)
    last_dt = datetime.fromtimestamp(timestamp_max / 1000, tz=timezone.utc)

    first_month_start = first_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_dt != first_month_start:
        first_month_start = _add_months(first_month_start, 1)

    last_month_start = last_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    expected_last_bar = _add_months(last_month_start, 1).timestamp() - interval_seconds
    if timestamp_max < int(expected_last_bar * 1000):
        last_month_start = _add_months(last_month_start, -1)

    if last_month_start < first_month_start:
        return []

    months: list[str] = []
    current = first_month_start
    while current <= last_month_start:
        months.append(current.strftime("%Y-%m"))
        current = _add_months(current, 1)
    return months


def _add_months(value: datetime, months: int) -> datetime:
    year = value.year + ((value.month - 1 + months) // 12)
    month = ((value.month - 1 + months) % 12) + 1
    return value.replace(year=year, month=month, day=1)
