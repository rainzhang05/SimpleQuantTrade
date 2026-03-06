from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.training.feature_builder import FeatureBuilder
from qtbot.training.feature_spec import FEATURE_COLUMNS, feature_spec_hash


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


def _write_snapshot_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {name: [row.get(name) for row in rows] for name in _SNAPSHOT_SCHEMA.names}
    pq.write_table(pa.Table.from_pydict(data, schema=_SNAPSHOT_SCHEMA), path, compression="zstd")


def _write_market_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "timestamp_ms": [int(item["timestamp_ms"]) for item in rows],
            "open": [float(item["open"]) for item in rows],
            "high": [float(item["high"]) for item in rows],
            "low": [float(item["low"]) for item in rows],
            "close": [float(item["close"]) for item in rows],
            "volume": [float(item.get("volume", 1.0)) for item in rows],
            "inside_bid": [0.0 for _ in rows],
            "inside_ask": [0.0 for _ in rows],
            "instrument_id": [0 for _ in rows],
            "symbol": [str(item["symbol"]) for item in rows],
            "interval_seconds": [900 for _ in rows],
            "source": [str(item.get("source", "ndax")) for item in rows],
        }
    )
    pq.write_table(table, path, compression="zstd")


class TrainingFeatureBuilderTests(unittest.TestCase):
    def test_feature_builder_is_deterministic_and_historical_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            snapshot_id = "snap123"
            snapshot_dir = root / "data" / "snapshots" / snapshot_id
            rows_path = snapshot_dir / "rows.parquet"
            _write_snapshot_rows(
                rows_path,
                [
                    {
                        "symbol": "BTCCAD",
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 0),
                        "next_timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "interval_seconds": 900,
                        "open": 100.0,
                        "high": 100.0,
                        "low": 100.0,
                        "close": 100.0,
                        "volume": 10.0,
                        "inside_bid": 0.0,
                        "inside_ask": 0.0,
                        "source": "ndax",
                        "effective_month": "2026-01",
                        "quality_pass": True,
                        "weight_method_version": "ndax_native_v1",
                        "effective_monthly_weight": 1.0,
                        "supervised_row_weight": 1.0,
                        "label_available": True,
                        "row_status": "trainable",
                        "next_close": 110.0,
                        "forward_return": 0.10,
                        "y": 1,
                    },
                    {
                        "symbol": "BTCCAD",
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 15),
                        "next_timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "interval_seconds": 900,
                        "open": 110.0,
                        "high": 110.0,
                        "low": 110.0,
                        "close": 110.0,
                        "volume": 11.0,
                        "inside_bid": 0.0,
                        "inside_ask": 0.0,
                        "source": "synthetic",
                        "effective_month": "2026-01",
                        "quality_pass": True,
                        "weight_method_version": "bridge_weight_v1",
                        "effective_monthly_weight": 0.5,
                        "supervised_row_weight": 0.5,
                        "label_available": True,
                        "row_status": "trainable",
                        "next_close": 90.0,
                        "forward_return": -0.1818,
                        "y": 0,
                    },
                    {
                        "symbol": "BTCCAD",
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 30),
                        "next_timestamp_ms": _ts_ms(2026, 1, 1, 0, 45),
                        "interval_seconds": 900,
                        "open": 90.0,
                        "high": 90.0,
                        "low": 90.0,
                        "close": 90.0,
                        "volume": 12.0,
                        "inside_bid": 0.0,
                        "inside_ask": 0.0,
                        "source": "synthetic",
                        "effective_month": "2026-01",
                        "quality_pass": True,
                        "weight_method_version": "bridge_weight_v1",
                        "effective_monthly_weight": 0.5,
                        "supervised_row_weight": 0.5,
                        "label_available": True,
                        "row_status": "trainable",
                        "next_close": 95.0,
                        "forward_return": 0.0555,
                        "y": 1,
                    },
                    {
                        "symbol": "BTCCAD",
                        "timestamp_ms": _ts_ms(2026, 1, 1, 0, 45),
                        "next_timestamp_ms": None,
                        "interval_seconds": 900,
                        "open": 95.0,
                        "high": 95.0,
                        "low": 95.0,
                        "close": 95.0,
                        "volume": 13.0,
                        "inside_bid": 0.0,
                        "inside_ask": 0.0,
                        "source": "ndax",
                        "effective_month": "2026-01",
                        "quality_pass": True,
                        "weight_method_version": "ndax_native_v1",
                        "effective_monthly_weight": 1.0,
                        "supervised_row_weight": 0.0,
                        "label_available": False,
                        "row_status": "unlabeled_missing_next",
                        "next_close": None,
                        "forward_return": None,
                        "y": None,
                    },
                ],
            )
            with (snapshot_dir / "manifest.json").open("w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "snapshot_id": snapshot_id,
                        "dataset_hash": "datahash",
                        "timeframe": "15m",
                        "interval_seconds": 900,
                        "trainable_source_mix": {"ndax": 1, "synthetic": 2},
                    },
                    handle,
                )

            external_rows = [
                {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 0), "open": 50.0, "high": 50.0, "low": 50.0, "close": 50.0, "symbol": "BTCUSDT", "source": "binance"},
                {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 15), "open": 55.0, "high": 55.0, "low": 55.0, "close": 55.0, "symbol": "BTCUSDT", "source": "binance"},
                {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 30), "open": 45.0, "high": 45.0, "low": 45.0, "close": 45.0, "symbol": "BTCUSDT", "source": "binance"},
                {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 45), "open": 47.5, "high": 47.5, "low": 47.5, "close": 47.5, "symbol": "BTCUSDT", "source": "binance"},
            ]
            _write_market_rows(
                root / "data" / "raw" / "binance" / "15m" / "BTCUSDT.parquet",
                external_rows,
            )
            _write_market_rows(
                root / "data" / "raw" / "external" / "15m" / "BTCCAD.parquet",
                external_rows,
            )
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
            _write_market_rows(
                root / "data" / "raw" / "ndax" / "15m" / "BTCCAD.parquet",
                [
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 0), "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "symbol": "BTCCAD", "source": "ndax"},
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 45), "open": 95.0, "high": 95.0, "low": 95.0, "close": 95.0, "symbol": "BTCCAD", "source": "ndax"},
                ],
            )
            _write_market_rows(
                root / "data" / "raw" / "ndax" / "15m" / "USDTCAD.parquet",
                [
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 0), "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "symbol": "USDTCAD", "source": "ndax"},
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 15), "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "symbol": "USDTCAD", "source": "ndax"},
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 30), "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "symbol": "USDTCAD", "source": "ndax"},
                    {"timestamp_ms": _ts_ms(2026, 1, 1, 0, 45), "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "symbol": "USDTCAD", "source": "ndax"},
                ],
            )

            builder = FeatureBuilder(repo_root=root)
            first = builder.build(snapshot_id=snapshot_id)
            second = builder.build(snapshot_id=snapshot_id)

            self.assertEqual(first.summary.feature_spec_hash, feature_spec_hash())
            self.assertEqual(first.summary.row_count, 3)
            self.assertEqual(first.summary.to_payload(), second.summary.to_payload())
            self.assertEqual(first.data[FEATURE_COLUMNS].to_dict("records"), second.data[FEATURE_COLUMNS].to_dict("records"))

            second_row = first.data.iloc[1]
            self.assertAlmostEqual(float(second_row["combined_ret_1"]), 0.10, places=6)
            self.assertEqual(float(second_row["ndax_ctx_available"]), 0.0)
            self.assertEqual(float(second_row["ndax_ret_1"]), 0.0)
            self.assertEqual(float(second_row["external_ctx_available"]), 1.0)


if __name__ == "__main__":
    unittest.main()
