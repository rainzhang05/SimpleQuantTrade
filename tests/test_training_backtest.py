from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_parquet_atomic
from qtbot.training.backtest import PortfolioBacktestService
from tests._helpers import make_runtime_config


def _write_snapshot_manifest(root: Path, *, snapshot_id: str, label_horizon_bars: int = 2) -> None:
    write_json_atomic(
        root / "data" / "snapshots" / snapshot_id / "manifest.json",
        {
            "snapshot_id": snapshot_id,
            "dataset_hash": "datahash",
            "timeframe": "15m",
            "interval_seconds": 900,
            "label_horizon_bars": label_horizon_bars,
        },
    )


def _prediction_frame(*, run_id: str, snapshot_id: str, scenario: str) -> pd.DataFrame:
    rows = [
        {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "fold_index": 1,
            "scenario": scenario,
            "model_scope": "global",
            "model_symbol": None,
            "symbol": "BTCCAD",
            "timestamp_ms": 1_000_000,
            "source": "ndax",
            "y": 1,
            "forward_return": 0.10,
            "supervised_row_weight": 1.0,
            "probability": 0.70,
        },
        {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "fold_index": 1,
            "scenario": scenario,
            "model_scope": "global",
            "model_symbol": None,
            "symbol": "ETHCAD",
            "timestamp_ms": 1_000_000,
            "source": "synthetic",
            "y": 0,
            "forward_return": -0.05,
            "supervised_row_weight": 0.8,
            "probability": 0.69,
        },
        {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "fold_index": 1,
            "scenario": scenario,
            "model_scope": "global",
            "model_symbol": None,
            "symbol": "BTCCAD",
            "timestamp_ms": 1_900_000,
            "source": "ndax",
            "y": 1,
            "forward_return": 0.03,
            "supervised_row_weight": 1.0,
            "probability": 0.68,
        },
    ]
    return pd.DataFrame(rows)


class PortfolioBacktestServiceTests(unittest.TestCase):
    def test_backtest_uses_promoted_scenario_and_respects_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(
                root,
                promotion_entry_threshold=0.60,
                backtest_initial_capital_cad=10_000.0,
                backtest_max_active_positions=1,
                backtest_position_fraction=0.50,
            )
            store = StateStore(cfg.state_db)
            run_id = "run123"
            snapshot_id = "snap123"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            prediction_dir = run_dir / "predictions" / "fold_01"
            prediction_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {"run_id": run_id, "snapshot_id": snapshot_id, "status": "evaluated"},
            )
            write_parquet_atomic(
                prediction_dir / "weighted_combined.parquet",
                _prediction_frame(run_id=run_id, snapshot_id=snapshot_id, scenario="weighted_combined"),
            )
            _write_snapshot_manifest(root, snapshot_id=snapshot_id, label_horizon_bars=2)
            store.upsert_training_run(
                run_id=run_id,
                snapshot_id=snapshot_id,
                dataset_hash="datahash",
                feature_spec_hash="featurehash",
                seed=42,
                timeframe="15m",
                train_window_months=12,
                valid_window_months=1,
                train_step_months=1,
                folds_requested=1,
                folds_built=1,
                status="evaluated",
                artifact_dir=str(run_dir),
                primary_scenario="ndax_only",
                scenario_status={"weighted_combined": {"status": "trained"}},
                metrics_summary={},
            )
            store.upsert_promotion(
                run_id=run_id,
                bundle_id="bundle123",
                decision="accepted",
                primary_scenario="weighted_combined",
                hard_failures=[],
                soft_warnings=[],
                omitted_symbols=[],
                bundle_dir=str(root / "models" / "bundles" / "bundle123"),
                signature_ok=True,
            )

            summary = PortfolioBacktestService(config=cfg, state_store=store).backtest(run_id=run_id)

            self.assertEqual(summary.scenario, "weighted_combined")
            self.assertEqual(summary.model_scope, "global")
            self.assertEqual(summary.trades_executed, 1)
            self.assertEqual(summary.skipped_capacity, 1)
            self.assertEqual(summary.skipped_symbol_open, 1)
            self.assertEqual(summary.label_horizon_bars, 2)
            self.assertAlmostEqual(summary.final_equity_cad, 10_479.0, places=3)
            self.assertAlmostEqual(summary.total_return_pct, 4.79, places=2)
            self.assertAlmostEqual(summary.avg_holding_bars, 2.0, places=6)
            self.assertAlmostEqual(summary.avg_holding_hours, 0.5, places=6)
            self.assertEqual(summary.source_mix["ndax"]["trades"], 1.0)
            self.assertTrue(Path(summary.summary_file).exists())
            self.assertTrue(Path(summary.trades_file).exists())
            with Path(summary.summary_file).open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["scenario"], "weighted_combined")

    def test_backtest_rejects_missing_predictions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            run_id = "run-empty"
            snapshot_id = "snap-empty"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            _write_snapshot_manifest(root, snapshot_id=snapshot_id, label_horizon_bars=1)
            store.upsert_training_run(
                run_id=run_id,
                snapshot_id=snapshot_id,
                dataset_hash="datahash",
                feature_spec_hash="featurehash",
                seed=42,
                timeframe="15m",
                train_window_months=12,
                valid_window_months=1,
                train_step_months=1,
                folds_requested=1,
                folds_built=1,
                status="evaluated",
                artifact_dir=str(run_dir),
                primary_scenario="weighted_combined",
                scenario_status={"weighted_combined": {"status": "trained"}},
                metrics_summary={},
            )

            with self.assertRaisesRegex(ValueError, "prediction artifacts missing"):
                PortfolioBacktestService(config=cfg, state_store=store).backtest(run_id=run_id)


if __name__ == "__main__":
    unittest.main()
