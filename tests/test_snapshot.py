from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.snapshot import TrainingSnapshotService
from qtbot.state import StateStore
from tests._helpers import make_runtime_config


def _ts_ms(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def _write_market_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "timestamp_ms": [int(item["timestamp_ms"]) for item in rows],
            "open": [float(item["open"]) for item in rows],
            "high": [float(item["high"]) for item in rows],
            "low": [float(item["low"]) for item in rows],
            "close": [float(item["close"]) for item in rows],
            "volume": [float(item["volume"]) for item in rows],
            "inside_bid": [float(item.get("inside_bid", 0.0)) for item in rows],
            "inside_ask": [float(item.get("inside_ask", 0.0)) for item in rows],
            "instrument_id": [int(item.get("instrument_id", 0)) for item in rows],
            "symbol": [str(item["symbol"]) for item in rows],
            "interval_seconds": [900 for _ in rows],
            "source": [str(item["source"]) for item in rows],
        }
    )
    pq.write_table(table, path, compression="zstd")


class SnapshotTests(unittest.TestCase):
    def test_build_snapshot_supports_multi_bar_label_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "open": 102.0,
                        "high": 103.0,
                        "low": 101.0,
                        "close": 102.0,
                        "volume": 12.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 45),
                        "open": 103.0,
                        "high": 104.0,
                        "low": 102.0,
                        "close": 103.0,
                        "volume": 13.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )

            summary = service.build_snapshot(
                asof=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                timeframe="15m",
                label_horizon_bars=2,
            )

            self.assertEqual(summary.label_horizon_bars, 2)
            self.assertEqual(summary.row_count, 4)
            self.assertEqual(summary.trainable_row_count, 2)
            self.assertEqual(summary.unlabeled_row_count, 2)

            rows = pq.read_table(summary.rows_file).to_pylist()
            self.assertEqual(rows[0]["row_status"], "trainable")
            self.assertAlmostEqual(rows[0]["forward_return"], 0.02)
            self.assertEqual(rows[1]["row_status"], "trainable")
            self.assertAlmostEqual(rows[1]["forward_return"], (103.0 / 101.0) - 1.0)
            self.assertEqual(rows[2]["row_status"], "unlabeled_missing_horizon")
            self.assertEqual(rows[3]["row_status"], "unlabeled_missing_horizon")

    def test_build_snapshot_records_excluded_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )
            _write_market_rows(
                combined_dir / "ETHCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 50.0,
                        "high": 51.0,
                        "low": 49.0,
                        "close": 50.0,
                        "volume": 20.0,
                        "symbol": "ETHCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 51.0,
                        "high": 52.0,
                        "low": 50.0,
                        "close": 51.0,
                        "volume": 21.0,
                        "symbol": "ETHCAD",
                        "source": "ndax",
                    },
                ],
            )

            summary = service.build_snapshot(
                asof=datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc),
                timeframe="15m",
                exclude_symbols={"ETHCAD"},
            )

            self.assertEqual(summary.excluded_symbols, ["ETHCAD"])
            self.assertEqual(summary.skipped_symbols["ETHCAD"], "excluded_symbol")
            excluded = next(item for item in summary.symbols if item.symbol == "ETHCAD")
            self.assertEqual(excluded.status, "excluded")
            self.assertEqual(summary.symbols_included, 1)
            rows = pq.read_table(summary.rows_file).to_pylist()
            self.assertEqual({row["symbol"] for row in rows}, {"BTCCAD"})

    def test_build_snapshot_applies_weights_and_continuity_only_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "open": 102.0,
                        "high": 103.0,
                        "low": 101.0,
                        "close": 102.0,
                        "volume": 12.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 45),
                        "open": 103.0,
                        "high": 104.0,
                        "low": 102.0,
                        "close": 103.0,
                        "volume": 13.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )
            _write_market_rows(
                combined_dir / "ETHCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 50.0,
                        "high": 51.0,
                        "low": 49.0,
                        "close": 50.0,
                        "volume": 20.0,
                        "symbol": "ETHCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 49.0,
                        "high": 50.0,
                        "low": 48.0,
                        "close": 49.0,
                        "volume": 21.0,
                        "symbol": "ETHCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "open": 48.0,
                        "high": 49.0,
                        "low": 47.0,
                        "close": 48.0,
                        "volume": 22.0,
                        "symbol": "ETHCAD",
                        "source": "ndax",
                    },
                ],
            )

            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2026-01",
                weight_quality=0.40,
                weight_backtest=0.30,
                weight_final=0.35,
                overlap_rows=5000,
                quality_pass=True,
                method_version="bridge_weight_v1",
                supervised_eligible=True,
                eligibility_mode="direct",
                anchor_month="2026-01",
            )
            store.upsert_synthetic_weight(
                symbol="ETHCAD",
                timeframe="15m",
                effective_month="2026-01",
                weight_quality=0.25,
                weight_backtest=0.25,
                weight_final=0.25,
                overlap_rows=500,
                quality_pass=False,
                method_version="bridge_weight_v1",
                supervised_eligible=False,
                eligibility_mode="blocked",
                anchor_month=None,
            )

            summary = service.build_snapshot(
                asof=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                timeframe="15m",
            )

            self.assertEqual(summary.row_count, 7)
            self.assertEqual(summary.trainable_row_count, 3)
            self.assertEqual(summary.continuity_only_row_count, 2)
            self.assertEqual(summary.unlabeled_row_count, 2)
            self.assertTrue(summary.parity_check_passed)
            self.assertEqual(summary.source_mix["synthetic"], 4)
            self.assertEqual(summary.source_mix["ndax"], 3)
            self.assertEqual(summary.trainable_source_mix["synthetic"], 2)
            self.assertEqual(summary.trainable_source_mix["ndax"], 1)

            rows = pq.read_table(summary.rows_file).to_pylist()
            btc_rows = [row for row in rows if row["symbol"] == "BTCCAD"]
            eth_rows = [row for row in rows if row["symbol"] == "ETHCAD"]

            self.assertEqual(btc_rows[0]["row_status"], "trainable")
            self.assertTrue(btc_rows[0]["label_available"])
            self.assertAlmostEqual(btc_rows[0]["effective_monthly_weight"], 0.35)
            self.assertAlmostEqual(btc_rows[0]["supervised_row_weight"], 0.35)
            self.assertEqual(btc_rows[2]["source"], "ndax")
            self.assertAlmostEqual(btc_rows[2]["supervised_row_weight"], 1.0)
            self.assertEqual(btc_rows[3]["row_status"], "unlabeled_missing_next")
            self.assertFalse(btc_rows[3]["label_available"])

            self.assertEqual(eth_rows[0]["row_status"], "continuity_only")
            self.assertFalse(eth_rows[0]["label_available"])
            self.assertAlmostEqual(eth_rows[0]["effective_monthly_weight"], 0.25)
            self.assertAlmostEqual(eth_rows[0]["supervised_row_weight"], 0.0)
            self.assertIsNone(eth_rows[0]["forward_return"])
            self.assertEqual(eth_rows[2]["row_status"], "unlabeled_missing_next")

            weights_used = {(row.symbol, row.effective_month): row for row in summary.weights_used}
            self.assertEqual(weights_used[("BTCCAD", "2026-01")].trainable_rows, 2)
            self.assertEqual(weights_used[("ETHCAD", "2026-01")].continuity_only_rows, 2)

    def test_build_snapshot_excludes_gap_fill_rows_from_supervision(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 100.0,
                        "high": 100.0,
                        "low": 100.0,
                        "close": 100.0,
                        "volume": 0.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic_gap_fill",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 45),
                        "open": 102.0,
                        "high": 103.0,
                        "low": 101.0,
                        "close": 102.0,
                        "volume": 12.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )
            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2026-01",
                weight_quality=0.40,
                weight_backtest=0.30,
                weight_final=0.35,
                overlap_rows=5000,
                quality_pass=True,
                method_version="bridge_weight_v1",
                supervised_eligible=True,
                eligibility_mode="direct",
                anchor_month="2026-01",
            )

            summary = service.build_snapshot(
                asof=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc),
                timeframe="15m",
            )

            self.assertEqual(summary.trainable_row_count, 1)
            self.assertEqual(summary.continuity_only_row_count, 2)
            self.assertEqual(summary.unlabeled_row_count, 1)
            self.assertEqual(summary.source_mix["synthetic_gap_fill"], 1)

            rows = pq.read_table(summary.rows_file).to_pylist()
            self.assertEqual(rows[0]["row_status"], "continuity_only")
            self.assertFalse(rows[0]["label_available"])
            self.assertEqual(rows[1]["row_status"], "continuity_only")
            self.assertFalse(rows[1]["label_available"])
            self.assertEqual(rows[2]["row_status"], "trainable")
            self.assertEqual(rows[3]["row_status"], "unlabeled_missing_next")

    def test_build_snapshot_is_deterministic_and_reuses_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )
            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2026-01",
                weight_quality=0.40,
                weight_backtest=0.30,
                weight_final=0.35,
                overlap_rows=5000,
                quality_pass=True,
                method_version="bridge_weight_v1",
                supervised_eligible=True,
                eligibility_mode="direct",
                anchor_month="2026-01",
            )

            first = service.build_snapshot(
                asof=datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc),
                timeframe="15m",
            )
            second = service.build_snapshot(
                asof=datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc),
                timeframe="15m",
            )

            self.assertEqual(first.snapshot_id, second.snapshot_id)
            self.assertEqual(first.dataset_hash, second.dataset_hash)
            self.assertFalse(first.reused_existing)
            self.assertTrue(second.reused_existing)
            self.assertTrue(Path(first.manifest_file).exists())
            self.assertTrue(Path(first.rows_file).exists())

    def test_build_snapshot_requires_weights_for_synthetic_combined_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "ndax",
                    },
                ],
            )

            with self.assertRaises(ValueError):
                service.build_snapshot(
                    asof=datetime(2026, 1, 1, 0, 45, tzinfo=timezone.utc),
                    timeframe="15m",
                )

    def test_build_snapshot_uses_supervised_eligibility_not_quality_pass(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2026, 2, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2026, 2, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                ],
            )
            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2026-02",
                weight_quality=0.25,
                weight_backtest=0.25,
                weight_final=0.25,
                overlap_rows=0,
                quality_pass=False,
                method_version="bridge_weight_v1",
                supervised_eligible=True,
                eligibility_mode="carry_forward",
                anchor_month="2026-01",
            )

            summary = service.build_snapshot(
                asof=datetime(2026, 2, 1, 0, 45, tzinfo=timezone.utc),
                timeframe="15m",
            )
            rows = pq.read_table(summary.rows_file).to_pylist()
            self.assertEqual(rows[0]["row_status"], "trainable")
            self.assertAlmostEqual(rows[0]["supervised_row_weight"], 0.25)
            self.assertEqual(summary.trainable_row_count, 1)

    def test_build_snapshot_honors_carry_backward_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = TrainingSnapshotService(config=cfg, state_store=store)

            combined_dir = root / "data" / "combined" / "15m"
            _write_market_rows(
                combined_dir / "BTCCAD.parquet",
                [
                    {
                        "timestamp_ms": _ts_ms(2025, 12, 1, 0, 0),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                    {
                        "timestamp_ms": _ts_ms(2025, 12, 1, 0, 15),
                        "open": 101.0,
                        "high": 102.0,
                        "low": 100.0,
                        "close": 101.0,
                        "volume": 11.0,
                        "symbol": "BTCCAD",
                        "source": "synthetic",
                    },
                ],
            )
            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2025-12",
                weight_quality=0.25,
                weight_backtest=0.25,
                weight_final=0.25,
                overlap_rows=0,
                quality_pass=False,
                method_version="bridge_weight_v1",
                supervised_eligible=True,
                eligibility_mode="carry_backward",
                anchor_month="2026-01",
            )

            summary = service.build_snapshot(
                asof=datetime(2025, 12, 1, 0, 45, tzinfo=timezone.utc),
                timeframe="15m",
            )
            rows = pq.read_table(summary.rows_file).to_pylist()
            self.assertEqual(rows[0]["row_status"], "trainable")
            self.assertAlmostEqual(rows[0]["supervised_row_weight"], 0.25)
            self.assertEqual(summary.trainable_row_count, 1)


if __name__ == "__main__":
    unittest.main()
