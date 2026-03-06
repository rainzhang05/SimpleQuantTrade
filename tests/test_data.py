from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

import pyarrow.parquet as pq

from qtbot.data import MarketDataService, parse_timeframe_seconds
from qtbot.state import StateStore
from tests._helpers import make_runtime_config


def _ts_ms(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def _generate_15m_rows(*, start_ts_ms: int, periods: int, instrument_id: int) -> list[list[float]]:
    rows: list[list[float]] = []
    for idx in range(periods):
        ts = start_ts_ms + (idx * 900_000)
        price = 100.0 + (idx * 0.1)
        rows.append(
            [
                ts,
                price + 0.5,  # high
                price - 0.5,  # low
                price,  # open
                price + 0.2,  # close
                10.0 + idx,  # volume
                price + 0.1,  # bid
                price + 0.3,  # ask
                instrument_id,
            ]
        )
    return rows


class _FakeNdaxClient:
    def __init__(self) -> None:
        self._instruments = [
            {"Product1Symbol": "BTC", "Product2Symbol": "CAD", "Symbol": "BTCCAD", "InstrumentId": 1},
            {"Product1Symbol": "ETH", "Product2Symbol": "CAD", "Symbol": "ETHCAD", "InstrumentId": 2},
        ]
        self._rows_by_instrument = {
            1: _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=1),
            2: _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=2),
        }

    def get_instruments(self):
        return list(self._instruments)

    def get_ticker_history(self, *, instrument_id: int, interval_seconds: int, from_date: date, to_date: date):
        if interval_seconds != 900:
            return []
        start = datetime(from_date.year, from_date.month, from_date.day, tzinfo=timezone.utc)
        end_exclusive = datetime(to_date.year, to_date.month, to_date.day, tzinfo=timezone.utc) + timedelta(days=1)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end_exclusive.timestamp() * 1000)
        rows = self._rows_by_instrument.get(instrument_id, [])
        return [row for row in rows if start_ms <= int(row[0]) < end_ms]


class _FakeNdaxClientWithGap(_FakeNdaxClient):
    def __init__(self) -> None:
        super().__init__()
        rows = self._rows_by_instrument[2]
        # Remove one 15m candle from ETH to produce a single internal gap.
        self._rows_by_instrument[2] = [row for idx, row in enumerate(rows) if idx != 10]


class DataServiceTests(unittest.TestCase):
    def test_parse_timeframe_seconds(self) -> None:
        self.assertEqual(parse_timeframe_seconds("15m"), 900)
        self.assertEqual(parse_timeframe_seconds("900"), 900)
        with self.assertRaises(ValueError):
            parse_timeframe_seconds("1m")

    def test_backfill_is_resumable_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                state_store=store,
            )

            first = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 2),
                timeframe="15m",
            )
            self.assertEqual(first.symbols_with_errors, 0)

            second = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            self.assertEqual(second.symbols_with_errors, 0)

            btc_file = Path(td) / "data" / "raw" / "ndax" / "15m" / "BTCCAD.parquet"
            self.assertTrue(btc_file.exists())
            btc_rows = pq.read_table(btc_file).num_rows
            self.assertEqual(btc_rows, 96 * 3)

            # Re-running same range should not duplicate rows.
            third = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            self.assertEqual(third.symbols_with_errors, 0)
            btc_rows_after = pq.read_table(btc_file).num_rows
            self.assertEqual(btc_rows_after, 96 * 3)

    def test_backfill_expands_backward_range_after_recent_seed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                state_store=store,
            )

            # Seed only the latest day, then request an earlier start date.
            service.backfill(
                from_date=date(2026, 1, 3),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            expanded = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )

            btc_file = Path(td) / "data" / "raw" / "ndax" / "15m" / "BTCCAD.parquet"
            btc_rows = pq.read_table(btc_file).num_rows
            self.assertEqual(btc_rows, 96 * 3)

            # Once complete, rerun should skip chunk fetches for fully covered windows.
            rerun = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            btc_expanded = next(item for item in expanded.symbols if item.symbol == "BTCCAD")
            btc_rerun = next(item for item in rerun.symbols if item.symbol == "BTCCAD")
            self.assertGreaterEqual(btc_expanded.chunk_count, 1)
            self.assertEqual(btc_rerun.chunk_count, 0)

    def test_data_status_reports_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClientWithGap(),
                state_store=store,
            )
            service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            status = service.data_status(timeframe="15m")
            eth = next(item for item in status.symbols if item.symbol == "ETHCAD")
            self.assertGreater(eth.gap_count, 0)
            self.assertEqual(eth.status, "ok")

            coverage_rows = store.get_data_coverage(timeframe="15m")
            self.assertTrue(any(row["symbol"] == "ETHCAD" for row in coverage_rows))


if __name__ == "__main__":
    unittest.main()
