"""CSV append logger for executed trades."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from qtbot.ndax_client import OrderSide


TRADE_HEADERS = (
    "timestamp",
    "symbol",
    "side",
    "qty",
    "avg_price",
    "notional_cad",
    "fee_cad",
    "order_id",
)


@dataclass(frozen=True)
class TradeFillRecord:
    timestamp_utc: str
    symbol: str
    side: OrderSide
    qty: float
    avg_price: float
    fee_cad: float
    order_id: int

    @property
    def notional_cad(self) -> float:
        return self.qty * self.avg_price


class TradeCsvLogger:
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path

    def append(self, record: TradeFillRecord) -> None:
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self._csv_path.exists()
        with self._csv_path.open(mode="a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if not file_exists:
                writer.writerow(TRADE_HEADERS)
            writer.writerow(
                (
                    record.timestamp_utc,
                    record.symbol,
                    record.side,
                    _format_number(record.qty),
                    _format_number(record.avg_price),
                    _format_number(record.notional_cad),
                    _format_number(record.fee_cad),
                    str(record.order_id),
                )
            )


def _format_number(value: float) -> str:
    return f"{value:.12g}"
