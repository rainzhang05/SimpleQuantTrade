from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
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
            {"Product1Symbol": "USDT", "Product2Symbol": "CAD", "Symbol": "USDTCAD", "InstrumentId": 3},
        ]
        self._rows_by_instrument = {
            1: _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=1),
            2: _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=2),
            3: _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=3),
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


class _FakeNdaxClientWithEmptyUni(_FakeNdaxClient):
    def __init__(self) -> None:
        super().__init__()
        self._instruments.append(
            {"Product1Symbol": "UNI", "Product2Symbol": "CAD", "Symbol": "UNICAD", "InstrumentId": 4}
        )
        self._rows_by_instrument[4] = []


class _FakeBinanceClient:
    def __init__(self) -> None:
        self._symbols = {"BTCUSDT", "ETHUSDT"}
        self._rows_by_symbol = {
            "BTCUSDT": self._generate_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, seed=70.0),
            "ETHUSDT": self._generate_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, seed=50.0),
        }

    def list_spot_symbols(self):
        return set(self._symbols)

    def get_klines(self, *, symbol: str, interval: str, start_time_ms: int, end_time_ms: int, limit: int = 1000):
        if interval != "15m":
            return []
        rows = self._rows_by_symbol.get(symbol.upper(), [])
        filtered = [row for row in rows if start_time_ms <= int(row[0]) <= end_time_ms]
        return filtered[:limit]

    @staticmethod
    def _generate_rows(*, start_ts_ms: int, periods: int, seed: float) -> list[list[float]]:
        rows: list[list[float]] = []
        for idx in range(periods + 1):
            ts = start_ts_ms - 900_000 + (idx * 900_000)
            price = seed + (idx * 0.07)
            rows.append(
                [
                    ts,
                    price,
                    price + 0.4,
                    price - 0.4,
                    price + 0.15,
                    20.0 + idx,
                    ts + 899_999,
                    0.0,
                    0,
                    0.0,
                    0.0,
                    0.0,
                ]
            )
        return rows


class _FakeBinanceClientWithGap(_FakeBinanceClient):
    def __init__(self) -> None:
        super().__init__()
        rows = self._rows_by_symbol["BTCUSDT"]
        self._rows_by_symbol["BTCUSDT"] = [row for idx, row in enumerate(rows) if idx not in {10, 11, 12}]


class _FakeBinanceClientWithUni(_FakeBinanceClient):
    def __init__(self) -> None:
        super().__init__()
        self._symbols.add("UNIUSDT")
        self._rows_by_symbol["UNIUSDT"] = self._generate_rows(
            start_ts_ms=_ts_ms(2026, 1, 1, 0, 0),
            periods=96 * 3,
            seed=35.0,
        )


def _generate_ndax_from_close_map(
    *,
    timestamps: list[int],
    close_fn,
    instrument_id: int,
) -> list[list[float]]:
    rows: list[list[float]] = []
    for idx, ts in enumerate(timestamps):
        close = float(close_fn(idx))
        rows.append([ts, close, close, close, close, 10.0 + idx, close, close, instrument_id])
    return rows


def _generate_binance_from_close_map(
    *,
    timestamps: list[int],
    close_fn,
) -> list[list[float]]:
    rows: list[list[float]] = []
    for idx, ts in enumerate(timestamps):
        close = float(close_fn(idx))
        raw_ts = ts - 900_000
        rows.append([raw_ts, close, close, close, close, 20.0 + idx, raw_ts + 899_999, 0.0, 0, 0.0, 0.0, 0.0])
    return rows


def _segment_timestamps(*, year: int, month: int, day_count: int) -> list[int]:
    timestamps: list[int] = []
    for day in range(1, day_count + 1):
        start = _ts_ms(year, month, day, 0, 0)
        for idx in range(96):
            timestamps.append(start + (idx * 900_000))
    return timestamps


class _FakeNdaxClientCarryForward:
    def __init__(self) -> None:
        self._instruments = [
            {"Product1Symbol": "BTC", "Product2Symbol": "CAD", "Symbol": "BTCCAD", "InstrumentId": 1},
            {"Product1Symbol": "UNI", "Product2Symbol": "CAD", "Symbol": "UNICAD", "InstrumentId": 2},
            {"Product1Symbol": "USDT", "Product2Symbol": "CAD", "Symbol": "USDTCAD", "InstrumentId": 3},
        ]
        december = _segment_timestamps(year=2025, month=12, day_count=3)
        january = _segment_timestamps(year=2026, month=1, day_count=3)
        february = _segment_timestamps(year=2026, month=2, day_count=3)
        self._rows_by_instrument = {
            1: _generate_ndax_from_close_map(
                timestamps=january,
                close_fn=lambda idx: (50.0 + (idx * 0.05)) * 2.0,
                instrument_id=1,
            ),
            2: [],
            3: _generate_ndax_from_close_map(
                timestamps=december + january + february,
                close_fn=lambda idx: 2.0,
                instrument_id=3,
            ),
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


class _FakeBinanceClientCarryForward:
    def __init__(self) -> None:
        december = _segment_timestamps(year=2025, month=12, day_count=3)
        january = _segment_timestamps(year=2026, month=1, day_count=3)
        february = _segment_timestamps(year=2026, month=2, day_count=3)
        self._symbols = {"BTCUSDT", "UNIUSDT"}
        self._rows_by_symbol = {
            "BTCUSDT": _generate_binance_from_close_map(
                timestamps=december + january + february,
                close_fn=lambda idx: 50.0 + (idx * 0.05),
            ),
            "UNIUSDT": _generate_binance_from_close_map(
                timestamps=december + january + february,
                close_fn=lambda idx: 25.0 + (idx * 0.03),
            ),
        }

    def list_spot_symbols(self):
        return set(self._symbols)

    def get_klines(self, *, symbol: str, interval: str, start_time_ms: int, end_time_ms: int, limit: int = 1000):
        if interval != "15m":
            return []
        rows = self._rows_by_symbol.get(symbol.upper(), [])
        filtered = [row for row in rows if start_time_ms <= int(row[0]) <= end_time_ms]
        return filtered[:limit]


class _FakeKrakenClient:
    def __init__(self) -> None:
        self.trade_calls = 0

    def get_trades(self, *, pair: str, since_ns: int | None = None):
        self.trade_calls += 1
        return [], since_ns


def _write_kraken_archive(path: Path, rows: list[tuple[int, float, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        for ts_ms, price, volume in rows:
            handle.write(f"{ts_ms / 1000:.3f},{price:.8f},{volume:.8f}\n")


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
                sources=["ndax"],
            )
            self.assertEqual(first.symbols_with_errors, 0)

            second = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax"],
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
                sources=["ndax"],
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
                sources=["ndax"],
            )
            expanded = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax"],
            )

            btc_file = Path(td) / "data" / "raw" / "ndax" / "15m" / "BTCCAD.parquet"
            btc_rows = pq.read_table(btc_file).num_rows
            self.assertEqual(btc_rows, 96 * 3)

            # Once complete, rerun should skip chunk fetches for fully covered windows.
            rerun = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax"],
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
                sources=["ndax"],
            )
            status = service.data_status(timeframe="15m", dataset="ndax")
            eth = next(item for item in status.symbols if item.symbol == "ETHCAD")
            self.assertGreater(eth.gap_count, 0)
            self.assertEqual(eth.status, "ok")

            coverage_rows = store.get_data_coverage(timeframe="15m")
            self.assertTrue(any(row["symbol"] == "ETHCAD" for row in coverage_rows))

    def test_backfill_repairs_binance_outage_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                binance_client=_FakeBinanceClientWithGap(),
                state_store=store,
            )

            summary = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["binance"],
            )

            btc = next(item for item in summary.symbols if item.symbol == "BTCUSDT")
            self.assertEqual(btc.gap_count, 0)
            self.assertEqual(btc.row_count, 96 * 3)

            btc_file = Path(td) / "data" / "raw" / "binance" / "15m" / "BTCUSDT.parquet"
            rows = pq.read_table(btc_file).to_pylist()
            repaired = [row for row in rows if row["source"] == "binance_gap_fill"]
            self.assertEqual(len(repaired), 3)
            self.assertTrue(all(float(row["volume"]) == 0.0 for row in repaired))

    def test_backfill_imports_kraken_archive_from_earliest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            rows: list[tuple[int, float, float]] = []
            for idx in range(96 * 2):
                ts = _ts_ms(2026, 1, 1, 0, 0) + (idx * 900_000) + 60_000
                rows.append((ts, 100.0 + idx, 1.0))
            _write_kraken_archive(root / "data" / "kraken" / "XBTCAD.csv", rows)

            store = StateStore(cfg.state_db)
            fake_kraken = _FakeKrakenClient()
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                kraken_client=fake_kraken,
                state_store=store,
            )

            summary = service.backfill(
                from_date=None,
                to_date=date(2026, 1, 2),
                timeframe="15m",
                sources=["kraken"],
            )

            btc = next(item for item in summary.symbols if item.symbol == "XBTCAD")
            self.assertEqual(btc.requested_from, "earliest")
            self.assertEqual(btc.row_count, (96 * 2) - 1)
            self.assertEqual(fake_kraken.trade_calls, 0)
            btc_file = root / "data" / "raw" / "kraken" / "15m" / "XBTCAD.parquet"
            self.assertTrue(btc_file.exists())
            status = service.data_status(timeframe="15m", dataset="kraken")
            btc_status = next(item for item in status.symbols if item.symbol == "XBTCAD")
            self.assertEqual(btc_status.gap_count, 0)

    def test_build_combined_prefers_kraken_when_quality_is_better(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                binance_client=_FakeBinanceClient(),
                kraken_client=_FakeKrakenClient(),
                state_store=store,
            )

            service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax", "binance"],
            )
            archive_rows: list[tuple[int, float, float]] = []
            for row in _generate_15m_rows(start_ts_ms=_ts_ms(2026, 1, 1, 0, 0), periods=96 * 3, instrument_id=0):
                ts_ms = int(row[0]) + 60_000
                archive_rows.append((ts_ms, float(row[4]), 1.0))
            _write_kraken_archive(root / "data" / "kraken" / "XBTCAD.csv", archive_rows)
            service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["kraken"],
            )

            combined = service.build_combined(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )

            btc = next(item for item in combined.symbols if item.symbol == "BTCCAD")
            self.assertEqual(btc.external_source, "kraken")
            self.assertEqual(btc.external_symbol, "XBTCAD")
            selection = json.loads((root / "data" / "raw" / "external" / "15m" / "selection.json").read_text(encoding="utf-8"))
            self.assertEqual(selection["selections"]["BTCCAD"]["source"], "kraken")
            self.assertTrue((root / "data" / "raw" / "external" / "15m" / "BTCCAD.parquet").exists())

    def test_build_combined_uses_shared_conversion_context_for_empty_ndax_history(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClientWithEmptyUni(),
                binance_client=_FakeBinanceClientWithUni(),
                state_store=store,
            )

            service.backfill(
                from_date=date(2025, 12, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax", "binance"],
            )
            combined = service.build_combined(
                from_date=date(2025, 12, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )

            uni = next(item for item in combined.symbols if item.symbol == "UNICAD")
            self.assertEqual(uni.combined_rows, 96 * 3)
            self.assertEqual(uni.gap_count, 0)

            uni_file = Path(td) / "data" / "combined" / "15m" / "UNICAD.parquet"
            rows = pq.read_table(uni_file).to_pylist()
            self.assertTrue(rows)
            self.assertTrue(all(row["source"] == "synthetic" for row in rows))

    def test_build_combined_uses_binance_fallback_when_primary_kraken_is_partial(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClientWithEmptyUni(),
                binance_client=_FakeBinanceClientWithUni(),
                kraken_client=_FakeKrakenClient(),
                state_store=store,
            )

            service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax", "binance"],
            )
            archive_rows: list[tuple[int, float, float]] = []
            for idx, ts_ms in enumerate(_segment_timestamps(year=2026, month=1, day_count=2)):
                archive_rows.append((ts_ms - 60_000, 25.0 + (idx * 0.03), 1.0))
            _write_kraken_archive(root / "data" / "kraken" / "UNIUSD.csv", archive_rows)
            service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 2),
                timeframe="15m",
                sources=["kraken"],
            )

            combined = service.build_combined(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )

            uni = next(item for item in combined.symbols if item.symbol == "UNICAD")
            self.assertEqual(uni.external_source, "kraken")
            self.assertEqual(uni.external_rows, 96 * 3)
            self.assertEqual(uni.combined_rows, 96 * 3)
            self.assertEqual(uni.gap_count, 0)

            selection = json.loads(
                (root / "data" / "raw" / "external" / "15m" / "selection.json").read_text(encoding="utf-8")
            )
            self.assertEqual(selection["selections"]["UNICAD"]["source"], "kraken")

    def test_dual_source_backfill_and_combined_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClient(),
                binance_client=_FakeBinanceClient(),
                state_store=store,
            )

            summary = service.backfill(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                sources=["ndax", "binance"],
            )
            self.assertEqual(summary.symbols_with_errors, 0)
            self.assertTrue(any(item.source == "ndax" for item in summary.symbols))
            self.assertTrue(any(item.source == "binance" for item in summary.symbols))

            status_all = service.data_status(timeframe="15m", dataset="all")
            payload = status_all.to_payload()
            self.assertEqual(payload["dataset"], "all")
            self.assertIn("combined", payload["datasets"])

            combined = service.build_combined(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
            )
            self.assertEqual(combined.symbols_with_errors, 0)
            self.assertTrue(any(item.combined_rows > 0 for item in combined.symbols))

            calibration = service.calibrate_weights(
                from_date=date(2026, 1, 1),
                to_date=date(2026, 1, 3),
                timeframe="15m",
                refresh="monthly",
            )
            self.assertGreater(calibration.rows_total, 0)
            self.assertTrue(Path(calibration.output_file).exists())

            weights = service.weight_status(timeframe="15m")
            self.assertGreaterEqual(weights.row_count, 1)

    def test_calibrate_weights_marks_direct_backward_forward_and_blocked_months(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = make_runtime_config(Path(td))
            store = StateStore(cfg.state_db)
            service = MarketDataService(
                config=cfg,
                ndax_client=_FakeNdaxClientCarryForward(),
                binance_client=_FakeBinanceClientCarryForward(),
                state_store=store,
            )

            service.backfill(
                from_date=date(2025, 12, 1),
                to_date=date(2026, 2, 3),
                timeframe="15m",
                sources=["ndax", "binance"],
            )
            service.build_combined(
                from_date=date(2025, 12, 1),
                to_date=date(2026, 2, 3),
                timeframe="15m",
            )
            service.calibrate_weights(
                from_date=date(2025, 12, 1),
                to_date=date(2026, 2, 3),
                timeframe="15m",
                refresh="monthly",
            )

            weights = {
                (row["symbol"], row["effective_month"]): row
                for row in store.get_synthetic_weights(timeframe="15m")
            }
            btc_dec = weights[("BTCCAD", "2025-12")]
            btc_jan = weights[("BTCCAD", "2026-01")]
            btc_feb = weights[("BTCCAD", "2026-02")]
            uni_dec = weights[("UNICAD", "2025-12")]

            self.assertEqual(int(btc_dec["supervised_eligible"]), 1)
            self.assertEqual(btc_dec["eligibility_mode"], "carry_backward")
            self.assertEqual(btc_dec["anchor_month"], "2026-01")

            self.assertEqual(int(btc_jan["supervised_eligible"]), 1)
            self.assertEqual(btc_jan["eligibility_mode"], "direct")
            self.assertEqual(btc_jan["anchor_month"], "2026-01")

            self.assertEqual(int(btc_feb["supervised_eligible"]), 1)
            self.assertEqual(btc_feb["eligibility_mode"], "carry_forward")
            self.assertEqual(btc_feb["anchor_month"], "2026-01")

            self.assertEqual(int(uni_dec["supervised_eligible"]), 0)
            self.assertEqual(uni_dec["eligibility_mode"], "blocked")
            self.assertIsNone(uni_dec["anchor_month"])


if __name__ == "__main__":
    unittest.main()
