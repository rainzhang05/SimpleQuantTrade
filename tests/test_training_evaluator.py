from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_parquet_atomic
from qtbot.training.evaluator import EvaluationService
from tests._helpers import make_runtime_config


class EvaluationServiceTests(unittest.TestCase):
    def test_evaluator_is_cost_aware_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            run_id = "run123"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(run_dir / "manifest.json", {"run_id": run_id, "status": "trained"})

            weighted_predictions = pd.DataFrame(
                {
                    "run_id": [run_id, run_id, run_id],
                    "snapshot_id": ["snap123", "snap123", "snap123"],
                    "fold_index": [1, 1, 1],
                    "scenario": ["weighted_combined", "weighted_combined", "weighted_combined"],
                    "model_scope": ["global", "global", "global"],
                    "model_symbol": [None, None, None],
                    "symbol": ["BTCCAD", "BTCCAD", "BTCCAD"],
                    "timestamp_ms": [1, 2, 3],
                    "source": ["ndax", "synthetic", "synthetic"],
                    "y": [1, 0, 1],
                    "forward_return": [0.05, -0.02, 0.04],
                    "supervised_row_weight": [1.0, 0.5, 0.5],
                    "probability": [0.9, 0.7, 0.4],
                }
            )
            ndax_predictions = pd.DataFrame(
                {
                    "run_id": [run_id, run_id],
                    "snapshot_id": ["snap123", "snap123"],
                    "fold_index": [1, 1],
                    "scenario": ["ndax_only", "ndax_only"],
                    "model_scope": ["global", "global"],
                    "model_symbol": [None, None],
                    "symbol": ["BTCCAD", "BTCCAD"],
                    "timestamp_ms": [1, 2],
                    "source": ["ndax", "ndax"],
                    "y": [1, 0],
                    "forward_return": [0.03, -0.01],
                    "supervised_row_weight": [1.0, 1.0],
                    "probability": [0.8, 0.2],
                }
            )
            write_parquet_atomic(run_dir / "predictions" / "fold_01" / "weighted_combined.parquet", weighted_predictions)
            write_parquet_atomic(run_dir / "predictions" / "fold_01" / "ndax_only.parquet", ndax_predictions)

            store.upsert_training_run(
                run_id=run_id,
                snapshot_id="snap123",
                dataset_hash="datahash",
                feature_spec_hash="featurehash",
                seed=42,
                timeframe="15m",
                train_window_months=12,
                valid_window_months=1,
                train_step_months=1,
                folds_requested=1,
                folds_built=1,
                status="trained",
                artifact_dir=str(run_dir),
                scenario_status={"weighted_combined": {"status": "trained"}, "ndax_only": {"status": "trained"}},
                metrics_summary={},
            )

            service = EvaluationService(config=cfg, state_store=store)
            first = service.evaluate(run_id=run_id)
            second = service.evaluate(run_id=run_id)

            self.assertEqual(first.status, "evaluated")
            self.assertEqual(second.metrics_summary, first.metrics_summary)
            self.assertIn("weighted_combined", first.metrics_summary)
            self.assertIn("ndax_only", first.metrics_summary)

            metrics = store.get_fold_metrics(run_id=run_id)
            self.assertEqual(len(metrics), 5)
            weighted_all = next(
                row
                for row in metrics
                if row["scenario"] == "weighted_combined"
                and row["model_scope"] == "global"
                and row["split"] == "all"
            )
            self.assertEqual(weighted_all["trades"], 2)
            self.assertAlmostEqual(float(weighted_all["gross_return"]), 0.03)
            self.assertAlmostEqual(float(weighted_all["net_return"]), 0.022)

            run_record = store.get_training_run(run_id=run_id)
            assert run_record is not None
            self.assertEqual(run_record["status"], "evaluated")
            self.assertEqual(run_record["primary_scenario"], first.primary_scenario)
            self.assertTrue((run_dir / "metrics.json").exists())


if __name__ == "__main__":
    unittest.main()
