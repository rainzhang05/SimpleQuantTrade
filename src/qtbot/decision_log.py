"""CSV append logger for strategy decisions."""

from __future__ import annotations

import csv
from pathlib import Path

from qtbot.strategy.signals import Decision


DECISION_HEADERS = (
    "timestamp",
    "symbol",
    "close",
    "ema_fast",
    "ema_slow",
    "atr",
    "signal",
    "reason",
)


class DecisionCsvLogger:
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path

    def append_many(self, decisions: list[Decision]) -> None:
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self._csv_path.exists()
        with self._csv_path.open(mode="a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if not file_exists:
                writer.writerow(DECISION_HEADERS)
            for decision in decisions:
                writer.writerow(
                    (
                        decision.timestamp_utc,
                        decision.symbol,
                        _format_number(decision.close),
                        _format_number(decision.ema_fast),
                        _format_number(decision.ema_slow),
                        _format_number(decision.atr),
                        decision.signal,
                        decision.reason,
                    )
                )


def _format_number(value: float) -> str:
    return f"{value:.12g}"
