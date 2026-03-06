from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.state import StateStore
from qtbot.training.trainer import TrainingService
from tests._helpers import make_runtime_config


_SNAPSHOT_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("timestamp_ms", pa.int64()),
        ("next_timestamp_ms", pa.int64()),
        ("interval_seconds", pa.int32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("inside_bid", pa.float64()),
        ("inside_ask", pa.float64()),
        ("source", pa.string()),
        ("effective_month", pa.string()),
        ("quality_pass", pa.bool_()),
        ("weight_method_version", pa.string()),
        ("effective_monthly_weight", pa.float64()),
        ("supervised_row_weight", pa.float64()),
        ("label_available", pa.bool_()),
        ("row_status", pa.string()),
        ("next_close", pa.float64()),
        ("forward_return", pa.float64()),
        ("y", pa.int8()),
    ]
)


def _ts_ms(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def _write_snapshot(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {name: [row.get(name) for row in rows] for name in _SNAPSHOT_SCHEMA.names}
    pq.write_table(pa.Table.from_pydict(data, schema=_SNAPSHOT_SCHEMA), path, compression="zstd")


def _write_market(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "timestamp_ms": [int(item["timestamp_ms"]) for item in rows],
            "open": [float(item["open"]) for item in rows],
            "high": [float(item["high"]) for item in rows],
            "low": [float(item["low"]) for item in rows],
            "close": [float(item["close"]) for item in rows],
            "volume": [float(item["volume"]) for item in rows],
            "inside_bid": [0.0 for _ in rows],
            "inside_ask": [0.0 for _ in rows],
            "instrument_id": [0 for _ in rows],
            "symbol": [str(item["symbol"]) for item in rows],
            "interval_seconds": [900 for _ in rows],
            "source": [str(item.get("source", "ndax")) for item in rows],
        }
    )
    pq.write_table(table, path, compression="zstd")


def _month_sequence(start_year: int, start_month: int, count: int) -> list[tuple[int, int]]:
    items: list[tuple[int, int]] = []
    year = start_year
    month = start_month
    for _ in range(count):
        items.append((year, month))
        month += 1
        if month > 12:
            year += 1
            month = 1
    return items


def _build_training_fixture(root: Path, snapshot_id: str, *, ndax_snapshot_start_month_index: int = 0, month_count: int = 14) -> None:
    snapshot_rows: list[dict[str, object]] = []
    market_rows_ndax: list[dict[str, object]] = []
    market_rows_binance: list[dict[str, object]] = []
    fx_rows: list[dict[str, object]] = []
    months = _month_sequence(2025, 1, month_count)
    previous_ts: int | None = None
    previous_close: float | None = None
    previous_month_index: int | None = None
    for month_index, (year, month) in enumerate(months):
        last_day = monthrange(year, month)[1]
        month_rows: list[tuple[int, float]] = []
        for idx in range(99):
            day = 1 + (idx // 96)
            slot = idx % 96
            ts = _ts_ms(year, month, day, slot // 4, (slot % 4) * 15)
            close = 100.0 + (month_index * 3.0) + (idx * 0.2)
            month_rows.append((ts, close))
        month_rows.append((_ts_ms(year, month, last_day, 23, 45), 120.0 + month_index))
        month_rows.sort(key=lambda item: item[0])
        for row_idx, (ts, close) in enumerate(month_rows):
            if previous_ts is not None and previous_close is not None:
                y = 1 if ((month_index + row_idx) % 2 == 0) else 0
                forward_return = 0.02 if y == 1 else -0.02
                snapshot_rows.append(
                    {
                        "symbol": "BTCCAD",
                        "timestamp_ms": previous_ts,
                        "next_timestamp_ms": ts,
                        "interval_seconds": 900,
                        "open": previous_close,
                        "high": previous_close,
                        "low": previous_close,
                        "close": previous_close,
                        "volume": 10.0 + row_idx,
                        "inside_bid": 0.0,
                        "inside_ask": 0.0,
                        "source": (
                            "ndax"
                            if (previous_month_index or 0) >= ndax_snapshot_start_month_index
                            else "synthetic"
                        ),
                        "effective_month": datetime.fromtimestamp(previous_ts / 1000, tz=timezone.utc).strftime("%Y-%m"),
                        "quality_pass": True,
                        "weight_method_version": (
                            "ndax_native_v1"
                            if (previous_month_index or 0) >= ndax_snapshot_start_month_index
                            else "combined_v2"
                        ),
                        "effective_monthly_weight": (
                            1.0 if (previous_month_index or 0) >= ndax_snapshot_start_month_index else 0.6
                        ),
                        "supervised_row_weight": (
                            1.0 if (previous_month_index or 0) >= ndax_snapshot_start_month_index else 0.6
                        ),
                        "label_available": True,
                        "row_status": "trainable",
                        "next_close": close,
                        "forward_return": forward_return,
                        "y": y,
                    }
                )
            market_rows_ndax.append({"timestamp_ms": ts, "open": close, "high": close, "low": close, "close": close, "volume": 10.0, "symbol": "BTCCAD", "source": "ndax"})
            market_rows_binance.append({"timestamp_ms": ts, "open": close / 2.0, "high": close / 2.0, "low": close / 2.0, "close": close / 2.0, "volume": 10.0, "symbol": "BTCUSDT", "source": "binance"})
            fx_rows.append({"timestamp_ms": ts, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "volume": 1.0, "symbol": "USDTCAD", "source": "ndax"})
            previous_ts = ts
            previous_close = close
            previous_month_index = month_index

    snapshot_rows.append(
        {
            "symbol": "BTCCAD",
            "timestamp_ms": previous_ts,
            "next_timestamp_ms": None,
            "interval_seconds": 900,
            "open": previous_close,
            "high": previous_close,
            "low": previous_close,
            "close": previous_close,
            "volume": 10.0,
            "inside_bid": 0.0,
            "inside_ask": 0.0,
            "source": "ndax",
            "effective_month": datetime.fromtimestamp(previous_ts / 1000, tz=timezone.utc).strftime("%Y-%m"),
            "quality_pass": True,
            "weight_method_version": "ndax_native_v1",
            "effective_monthly_weight": 1.0,
            "supervised_row_weight": 0.0,
            "label_available": False,
            "row_status": "unlabeled_missing_next",
            "next_close": None,
            "forward_return": None,
            "y": None,
        }
    )

    snapshot_dir = root / "data" / "snapshots" / snapshot_id
    _write_snapshot(snapshot_dir / "rows.parquet", snapshot_rows)
    with (snapshot_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "snapshot_id": snapshot_id,
                "dataset_hash": "datahash",
                "timeframe": "15m",
                "interval_seconds": 900,
                "trainable_source_mix": {"ndax": len(snapshot_rows) - 1},
            },
            handle,
        )

    _write_market(root / "data" / "raw" / "ndax" / "15m" / "BTCCAD.parquet", market_rows_ndax)
    _write_market(root / "data" / "raw" / "binance" / "15m" / "BTCUSDT.parquet", market_rows_binance)
    _write_market(root / "data" / "raw" / "external" / "15m" / "BTCCAD.parquet", market_rows_binance)
    _write_market(root / "data" / "raw" / "ndax" / "15m" / "USDTCAD.parquet", fx_rows)
    selection_path = root / "data" / "raw" / "external" / "15m" / "selection.json"
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-03-06T00:00:00+00:00",
                "interval_seconds": 900,
                "selections": {
                    "BTCCAD": {
                        "ticker": "BTC",
                        "source": "binance",
                        "symbol": "BTCUSDT",
                        "quote_currency": "USDT",
                    }
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


class TrainingServiceTests(unittest.TestCase):
    def test_training_service_writes_deterministic_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            snapshot_id = "train-snapshot"
            _build_training_fixture(root, snapshot_id)
            service = TrainingService(config=cfg, state_store=store)

            first = service.train(snapshot_id=snapshot_id, folds=1, universe="V1")
            second = service.train(snapshot_id=snapshot_id, folds=1, universe="V1")

            self.assertEqual(first.status, "trained")
            self.assertEqual(second.status, "trained")
            self.assertNotEqual(first.run_id, second.run_id)

            first_model = Path(first.artifact_dir) / "models" / "global" / "ndax_only" / "fold_01.txt"
            second_model = Path(second.artifact_dir) / "models" / "global" / "ndax_only" / "fold_01.txt"
            self.assertTrue(first_model.exists())
            self.assertTrue(second_model.exists())
            self.assertEqual(first_model.read_text(encoding="utf-8"), second_model.read_text(encoding="utf-8"))

            self.assertTrue((Path(first.artifact_dir) / "models" / "per_coin" / "BTCCAD" / "weighted_combined" / "fold_01.txt").exists())
            self.assertTrue((Path(first.artifact_dir) / "predictions" / "fold_01" / "weighted_combined.parquet").exists())

            run_record = store.get_training_run(run_id=first.run_id)
            assert run_record is not None
            self.assertEqual(run_record["status"], "trained")
            self.assertEqual(run_record["folds_built"], 1)

            fold_records = store.get_training_folds(run_id=first.run_id)
            self.assertEqual(len(fold_records), 1)
            self.assertEqual(fold_records[0]["status"], "trained")
            self.assertEqual(fold_records[0]["per_coin_skip_reasons"], {})

    def test_training_service_skips_ndax_only_folds_without_ndax_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(
                root,
                train_window_months=3,
                valid_window_months=1,
                train_step_months=1,
            )
            store = StateStore(cfg.state_db)
            snapshot_id = "partial-ndax-snapshot"
            _build_training_fixture(
                root,
                snapshot_id,
                ndax_snapshot_start_month_index=3,
                month_count=6,
            )
            service = TrainingService(config=cfg, state_store=store)

            summary = service.train(snapshot_id=snapshot_id, folds=3, universe="V1")

            self.assertEqual(summary.status, "trained")
            self.assertEqual(summary.folds_built, 2)
            run_record = store.get_training_run(run_id=summary.run_id)
            assert run_record is not None
            ndax_status = run_record["scenario_status"]["ndax_only"]
            self.assertEqual(ndax_status["status"], "partial")
            self.assertEqual(ndax_status["folds_completed"], 1)
            self.assertEqual(ndax_status["folds_skipped"], 1)
            self.assertEqual(
                ndax_status["skip_reasons"],
                [{"fold_index": 1, "reason": "train has no rows"}],
            )
            weighted_status = run_record["scenario_status"]["weighted_combined"]
            self.assertEqual(weighted_status["status"], "trained")
            self.assertTrue((Path(summary.artifact_dir) / "predictions" / "fold_01" / "weighted_combined.parquet").exists())
            self.assertFalse((Path(summary.artifact_dir) / "predictions" / "fold_01" / "ndax_only.parquet").exists())
            self.assertTrue((Path(summary.artifact_dir) / "predictions" / "fold_02" / "ndax_only.parquet").exists())


if __name__ == "__main__":
    unittest.main()
