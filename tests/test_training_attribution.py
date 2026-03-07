from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_parquet_atomic
from qtbot.training.attribution import AttributionService
from tests._helpers import make_runtime_config


_SYMBOL_OFFSETS = {
    "GOODCAD": 1,
    "SPARSECAD": 2,
    "COSTCAD": 3,
    "SYNTHCAD": 4,
    "WEAKCAD": 5,
}


def _extend_predictions(
    rows: list[dict[str, object]],
    *,
    run_id: str,
    fold_index: int,
    symbol: str,
    pattern: str,
    scopes: tuple[str, ...] = ("per_coin", "global"),
) -> None:
    row_count = 20 if pattern == "sparse" else 1300
    base_ts = (fold_index * 1_000_000_000) + (_SYMBOL_OFFSETS[symbol] * 100_000)
    for model_scope in scopes:
        model_symbol = symbol if model_scope == "per_coin" else None
        for idx in range(row_count):
            timestamp_ms = base_ts + idx
            probability = 0.1
            y = 0
            forward_return = 0.0
            source = "ndax"

            if pattern == "good":
                if idx < 24:
                    probability = 0.9
                    y = 1
                    forward_return = 0.02
                elif idx < 30:
                    probability = 0.6
                    y = 0
                    forward_return = -0.005
            elif pattern == "cost_fragile":
                if idx < 30:
                    probability = 0.9
                    y = 1
                    forward_return = 0.003
            elif pattern == "synthetic_fragile":
                source = "synthetic" if idx < 1000 else "ndax"
                if idx < 24:
                    probability = 0.9
                    y = 1
                    forward_return = 0.003
                    source = "synthetic"
                elif 1000 <= idx < 1006:
                    probability = 0.9
                    y = 1
                    forward_return = 0.03
                    source = "ndax"
            elif pattern == "weak_signal":
                y = 1 if idx % 2 == 0 else 0
                if idx < 18:
                    probability = 0.6
                    forward_return = 0.02
                elif idx < 30:
                    probability = 0.6
                    forward_return = -0.005
                else:
                    probability = 0.4
            elif pattern == "sparse":
                if idx < 5:
                    probability = 0.9
                    y = 1
                    forward_return = 0.02
            else:
                raise ValueError(f"unsupported pattern: {pattern}")

            rows.append(
                {
                    "run_id": run_id,
                    "snapshot_id": "snap123",
                    "fold_index": fold_index,
                    "scenario": "weighted_combined",
                    "model_scope": model_scope,
                    "model_symbol": model_symbol,
                    "symbol": symbol,
                    "timestamp_ms": timestamp_ms,
                    "source": source,
                    "y": y,
                    "forward_return": forward_return,
                    "supervised_row_weight": 1.0,
                    "probability": probability,
                }
            )


class AttributionServiceTests(unittest.TestCase):
    def test_attribution_report_is_deterministic_and_classifies_bad_kinds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            run_id = "run-attribution"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {
                    "run_id": run_id,
                    "status": "evaluated",
                    "primary_scenario": "weighted_combined",
                },
            )
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
                folds_requested=4,
                folds_built=4,
                status="evaluated",
                artifact_dir=str(run_dir),
                primary_scenario="weighted_combined",
                scenario_status={"weighted_combined": {"status": "trained"}},
                metrics_summary={"weighted_combined": {"global": {"folds": 4, "trades": 120, "net_return": 1.0}}},
            )

            for fold_index in range(1, 5):
                fold_rows: list[dict[str, object]] = []
                _extend_predictions(fold_rows, run_id=run_id, fold_index=fold_index, symbol="GOODCAD", pattern="good")
                _extend_predictions(fold_rows, run_id=run_id, fold_index=fold_index, symbol="COSTCAD", pattern="cost_fragile")
                _extend_predictions(fold_rows, run_id=run_id, fold_index=fold_index, symbol="SYNTHCAD", pattern="synthetic_fragile")
                _extend_predictions(fold_rows, run_id=run_id, fold_index=fold_index, symbol="WEAKCAD", pattern="weak_signal")
                if fold_index < 4:
                    _extend_predictions(fold_rows, run_id=run_id, fold_index=fold_index, symbol="SPARSECAD", pattern="sparse")
                write_parquet_atomic(
                    run_dir / "predictions" / f"fold_{fold_index:02d}" / "weighted_combined.parquet",
                    pd.DataFrame(fold_rows),
                )

            service = AttributionService(config=cfg, state_store=store)
            first = service.generate(run_id=run_id)
            json_first = Path(first.attribution_json).read_text(encoding="utf-8")
            md_first = Path(first.attribution_markdown).read_text(encoding="utf-8")

            second = service.generate(run_id=run_id)
            self.assertEqual(first.to_payload(), second.to_payload())
            self.assertEqual(json_first, Path(second.attribution_json).read_text(encoding="utf-8"))
            self.assertEqual(md_first, Path(second.attribution_markdown).read_text(encoding="utf-8"))

            payload = json.loads(json_first)
            symbol_rows = {
                str(item["symbol"]): item
                for item in payload["scenarios"]["weighted_combined"]["per_coin"]["symbols"]
            }
            self.assertEqual(payload["primary_scenario_summary"]["eligible_symbols"], ["GOODCAD"])
            self.assertEqual(symbol_rows["GOODCAD"]["bad_kind"], None)
            self.assertTrue(symbol_rows["GOODCAD"]["promotion_eligible"])
            self.assertEqual(symbol_rows["SPARSECAD"]["bad_kind"], "sparse_history")
            self.assertEqual(symbol_rows["COSTCAD"]["bad_kind"], "cost_fragility")
            self.assertEqual(symbol_rows["SYNTHCAD"]["bad_kind"], "synthetic_fragility")
            self.assertEqual(symbol_rows["WEAKCAD"]["bad_kind"], "weak_signal")
            self.assertIn("folds_lt_4", symbol_rows["SPARSECAD"]["reasons"])
            self.assertIn("fees_erase_edge", symbol_rows["COSTCAD"]["reasons"])
            self.assertIn("synthetic_dominant_negative", symbol_rows["SYNTHCAD"]["reasons"])
            self.assertIn("roc_auc_lt_0.52", symbol_rows["WEAKCAD"]["reasons"])
            self.assertIn("## Primary Scenario Per-Coin Eligibility", md_first)
            self.assertIn("## Primary Scenario Global Worst Symbols", md_first)


if __name__ == "__main__":
    unittest.main()
