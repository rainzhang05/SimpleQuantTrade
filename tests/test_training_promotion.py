from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pandas as pd

from qtbot.control import Command, write_control
from qtbot.model_bundle import (
    BUNDLE_REQUIRED_FILES,
    bundle_dir,
    read_active_bundle_id,
    write_active_bundle_id_atomic,
    write_bundle_signature,
)
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_parquet_atomic
from qtbot.training.attribution import AttributionSummary
from qtbot.training.feature_builder import FeatureBuildResult, FeatureBuildSummary
from qtbot.training.feature_spec import FEATURE_COLUMNS, feature_spec_hash
from qtbot.training.promotion import PromotionService
from tests._helpers import make_runtime_config


def _extend_predictions(
    rows: list[dict[str, object]],
    *,
    run_id: str,
    fold_index: int,
    symbol: str,
    pattern: str,
) -> None:
    row_count = 20 if pattern == "sparse" else 1300
    base_ts = (fold_index * 1_000_000_000) + (100_000 if symbol == "GOODCAD" else 200_000)
    for idx in range(row_count):
        timestamp_ms = base_ts + idx
        probability = 0.1
        y = 0
        forward_return = 0.0
        if pattern == "good":
            if idx < 24:
                probability = 0.9
                y = 1
                forward_return = 0.02
            elif idx < 30:
                probability = 0.6
                y = 0
                forward_return = -0.005
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
                "model_scope": "per_coin",
                "model_symbol": symbol,
                "symbol": symbol,
                "timestamp_ms": timestamp_ms,
                "source": "ndax",
                "y": y,
                "forward_return": forward_return,
                "supervised_row_weight": 1.0,
                "probability": probability,
            }
        )


def _write_prediction_fixture(run_dir: Path, *, run_id: str) -> None:
    for fold_index in range(1, 5):
        rows: list[dict[str, object]] = []
        _extend_predictions(rows, run_id=run_id, fold_index=fold_index, symbol="GOODCAD", pattern="good")
        if fold_index < 4:
            _extend_predictions(rows, run_id=run_id, fold_index=fold_index, symbol="SPARSECAD", pattern="sparse")
        global_rows = []
        for row in rows:
            global_rows.append(
                {
                    **row,
                    "model_scope": "global",
                    "model_symbol": None,
                }
            )
        write_parquet_atomic(
            run_dir / "predictions" / f"fold_{fold_index:02d}" / "weighted_combined.parquet",
            pd.DataFrame(rows + global_rows),
        )


def _write_snapshot_manifest(root: Path, *, snapshot_id: str) -> None:
    snapshot_dir = root / "data" / "snapshots" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        snapshot_dir / "manifest.json",
        {
            "snapshot_id": snapshot_id,
            "dataset_hash": "datahash",
            "timeframe": "15m",
            "interval_seconds": 900,
            "parity_check_passed": True,
            "symbols": [
                {"symbol": "GOODCAD", "gap_count": 0, "coverage_pct": 1.0},
                {"symbol": "SPARSECAD", "gap_count": 0, "coverage_pct": 1.0},
            ],
        },
    )


def _feature_frame(*, rows_by_symbol: dict[str, int], include_synthetic: bool = False) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    timestamp = 1
    for symbol, row_count in rows_by_symbol.items():
        for idx in range(row_count):
            payload: dict[str, object] = {
                "symbol": symbol,
                "timestamp_ms": timestamp,
                "month": "2026-01",
                "source": "synthetic" if include_synthetic and idx % 3 == 0 else "ndax",
                "y": idx % 2,
                "forward_return": 0.02 if idx % 2 == 1 else -0.01,
                "supervised_row_weight": 1.0,
            }
            for feature_index, column in enumerate(FEATURE_COLUMNS):
                payload[column] = float(((idx + feature_index) % 11) / 10.0)
            rows.append(payload)
            timestamp += 1
    return pd.DataFrame(rows)


def _feature_result(root: Path, data: pd.DataFrame) -> FeatureBuildResult:
    return FeatureBuildResult(
        data=data,
        summary=FeatureBuildSummary(
            snapshot_id="snap123",
            dataset_hash="datahash",
            timeframe="15m",
            interval_seconds=900,
            label_horizon_bars=1,
            excluded_symbols=[],
            snapshot_dir=str(root / "data" / "snapshots" / "snap123"),
            row_count=len(data),
            feature_count=len(FEATURE_COLUMNS),
            feature_spec_hash=feature_spec_hash(),
            source_mix={"ndax": int((data["source"] == "ndax").sum())},
        ),
    )


def _write_minimal_bundle(root: Path, bundle_id: str) -> Path:
    path = bundle_dir(repo_root=root, bundle_id=bundle_id)
    (path / "per_coin").mkdir(parents=True, exist_ok=True)
    for name in BUNDLE_REQUIRED_FILES:
        if name == "manifest.json":
            write_json_atomic(
                path / name,
                {
                    "bundle_id": bundle_id,
                    "run_id": "run123",
                    "primary_scenario": "weighted_combined",
                    "omitted_symbols": [],
                },
            )
        elif name.endswith(".json"):
            write_json_atomic(path / name, {"bundle_id": bundle_id})
        else:
            (path / name).write_text("model\n", encoding="utf-8")
    write_bundle_signature(bundle_path=path)
    return path


class PromotionServiceTests(unittest.TestCase):
    def test_promote_accepts_refit_bundle_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(
                root,
                promotion_min_folds=4,
                promotion_min_trades=100,
                promotion_min_conversion_pass_rate=0.5,
            )
            store = StateStore(cfg.state_db)
            run_id = "run-promote"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {"run_id": run_id, "status": "evaluated", "primary_scenario": "weighted_combined"},
            )
            _write_prediction_fixture(run_dir, run_id=run_id)
            _write_snapshot_manifest(root, snapshot_id="snap123")
            store.insert_conversion_quality(
                symbol="GOODCAD",
                timeframe="15m",
                period_start="2026-01-01T00:00:00Z",
                period_end="2026-01-31T23:45:00Z",
                overlap_rows=1000,
                median_ape_close=0.001,
                median_abs_ret_err=0.001,
                ret_corr=0.9,
                direction_match=0.9,
                basis_median=0.0,
                basis_mad=0.0,
                quality_pass=True,
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
                metrics_summary={
                    "weighted_combined": {
                        "global": {
                            "folds": 4,
                            "trades": 120,
                            "net_return": 1.2,
                            "max_drawdown": 0.10,
                            "pr_auc": 0.70,
                        }
                    }
                },
            )

            feature_result = _feature_result(root, _feature_frame(rows_by_symbol={"GOODCAD": 1200, "ALTCAD": 1200}))
            with mock.patch("qtbot.training.promotion.FeatureBuilder.build", return_value=feature_result):
                service = PromotionService(config=cfg, state_store=store)
                first = service.promote(run_id=run_id)
                second = service.promote(run_id=run_id)

            self.assertEqual(first.decision, "accepted")
            self.assertEqual(second.decision, "accepted")
            self.assertEqual(first.bundle_id, second.bundle_id)
            self.assertTrue(first.signature_ok)
            self.assertEqual(first.omitted_symbols, ["SPARSECAD"])
            assert first.bundle_dir is not None
            bundle_path = Path(first.bundle_dir)
            self.assertTrue((bundle_path / "manifest.json").exists())
            self.assertTrue((bundle_path / "global_model.txt").exists())
            self.assertTrue((bundle_path / "feature_spec.json").exists())
            self.assertTrue((bundle_path / "thresholds.json").exists())
            self.assertTrue((bundle_path / "cost_model.json").exists())
            self.assertTrue((bundle_path / "signature.sha256").exists())
            self.assertTrue((bundle_path / "per_coin" / "GOODCAD.txt").exists())
            self.assertFalse((bundle_path / "per_coin" / "SPARSECAD.txt").exists())
            self.assertEqual(read_active_bundle_id(repo_root=root), first.bundle_id)

            promotion_record = store.get_promotion(run_id=run_id)
            assert promotion_record is not None
            self.assertEqual(promotion_record["decision"], "accepted")

            status = PromotionService(config=cfg, state_store=store).model_status()
            self.assertEqual(status.integrity_status, "ok")
            self.assertEqual(status.bundle_id, first.bundle_id)
            self.assertEqual(status.omitted_symbols_count, 1)

    def test_promote_selects_best_promotable_scenario_using_thresholded_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(
                root,
                promotion_min_folds=4,
                promotion_min_trades=100,
                promotion_min_conversion_pass_rate=0.6,
                promotion_entry_threshold=0.60,
            )
            store = StateStore(cfg.state_db)
            run_id = "run-scenario-select"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {"run_id": run_id, "status": "evaluated", "primary_scenario": "ndax_only"},
            )
            _write_snapshot_manifest(root, snapshot_id="snap123")

            weighted_rows: list[dict[str, object]] = []
            ndax_rows: list[dict[str, object]] = []
            for fold_index in range(1, 5):
                for idx in range(40):
                    weighted_rows.append(
                        {
                            "run_id": run_id,
                            "snapshot_id": "snap123",
                            "fold_index": fold_index,
                            "scenario": "weighted_combined",
                            "model_scope": "global",
                            "model_symbol": None,
                            "symbol": "GOODCAD",
                            "timestamp_ms": (fold_index * 1_000_000) + idx,
                            "source": "synthetic",
                            "y": 1 if idx % 2 == 0 else 0,
                            "forward_return": 0.018 if idx < 34 else -0.002,
                            "supervised_row_weight": 0.8,
                            "probability": 0.62 if idx < 34 else 0.40,
                        }
                    )
            for idx in range(120):
                ndax_rows.append(
                    {
                        "run_id": run_id,
                        "snapshot_id": "snap123",
                        "fold_index": 1,
                        "scenario": "ndax_only",
                        "model_scope": "global",
                        "model_symbol": None,
                        "symbol": "GOODCAD",
                        "timestamp_ms": 9_000_000 + idx,
                        "source": "ndax",
                        "y": 1 if idx % 2 == 0 else 0,
                        "forward_return": 0.02 if idx < 80 else -0.004,
                        "supervised_row_weight": 1.0,
                        "probability": 0.70 if idx < 80 else 0.30,
                    }
                )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_01" / "weighted_combined.parquet",
                pd.DataFrame([row for row in weighted_rows if row["fold_index"] == 1]),
            )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_02" / "weighted_combined.parquet",
                pd.DataFrame([row for row in weighted_rows if row["fold_index"] == 2]),
            )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_03" / "weighted_combined.parquet",
                pd.DataFrame([row for row in weighted_rows if row["fold_index"] == 3]),
            )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_04" / "weighted_combined.parquet",
                pd.DataFrame([row for row in weighted_rows if row["fold_index"] == 4]),
            )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_01" / "ndax_only.parquet",
                pd.DataFrame(ndax_rows),
            )

            for effective_month in ("2025-10", "2025-11", "2025-12", "2026-01"):
                store.upsert_synthetic_weight(
                    symbol="GOODCAD",
                    timeframe="15m",
                    effective_month=effective_month,
                    weight_quality=0.7,
                    weight_backtest=0.7,
                    weight_final=0.7,
                    overlap_rows=1200,
                    quality_pass=True,
                    method_version="v1",
                    supervised_eligible=True,
                    eligibility_mode="direct",
                    anchor_month=effective_month,
                )
                store.upsert_synthetic_weight(
                    symbol="SPARSECAD",
                    timeframe="15m",
                    effective_month=effective_month,
                    weight_quality=0.7,
                    weight_backtest=0.7,
                    weight_final=0.7,
                    overlap_rows=1200,
                    quality_pass=True,
                    method_version="v1",
                    supervised_eligible=True,
                    eligibility_mode="direct",
                    anchor_month=effective_month,
                )
            for month in ("2025-10", "2025-11", "2025-12", "2026-01"):
                store.insert_conversion_quality(
                    symbol="GOODCAD",
                    timeframe="15m",
                    period_start=f"{month}-01T00:00:00Z",
                    period_end=f"{month}-28T23:45:00Z",
                    overlap_rows=1200,
                    median_ape_close=0.5,
                    median_abs_ret_err=0.5,
                    ret_corr=0.0,
                    direction_match=0.5,
                    basis_median=0.0,
                    basis_mad=0.5,
                    quality_pass=False,
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
                primary_scenario="ndax_only",
                scenario_status={
                    "weighted_combined": {"status": "trained"},
                    "ndax_only": {"status": "trained"},
                },
                metrics_summary={
                    "weighted_combined": {
                        "global": {
                            "folds": 4,
                            "trades": 160,
                            "net_return": 1.7,
                            "max_drawdown": 0.08,
                            "pr_auc": 0.70,
                        }
                    },
                    "ndax_only": {
                        "global": {
                            "folds": 1,
                            "trades": 120,
                            "net_return": 2.2,
                            "max_drawdown": 0.07,
                            "pr_auc": 0.68,
                        }
                    },
                },
            )

            attribution_json = run_dir / "coin_attribution.json"
            attribution_md = run_dir / "coin_attribution.md"
            write_json_atomic(
                attribution_json,
                {
                    "run_id": run_id,
                    "generated_at_utc": "",
                    "primary_scenario": "ndax_only",
                    "slippage_stress_pct_per_side": cfg.promotion_slippage_stress_pct_per_side,
                    "scenarios": {
                        "weighted_combined": {
                            "per_coin": {
                                "eligible_symbol_count": 1,
                                "omitted_symbol_count": 1,
                                "eligible_symbols": ["GOODCAD"],
                                "omitted_symbols": ["SPARSECAD"],
                                "symbols": [
                                    {
                                        "symbol": "GOODCAD",
                                        "promotion_eligible": True,
                                        "bad_kind": None,
                                        "reasons": [],
                                        "trades": 160,
                                        "net_return": 1.7,
                                        "stressed_net_return": 1.38,
                                        "synthetic_share": 1.0,
                                    },
                                    {
                                        "symbol": "SPARSECAD",
                                        "promotion_eligible": False,
                                        "bad_kind": "sparse_history",
                                        "reasons": ["trades_lt_100"],
                                        "trades": 12,
                                        "net_return": 0.02,
                                        "stressed_net_return": -0.004,
                                        "synthetic_share": 1.0,
                                    },
                                ],
                            },
                            "global": {"worst_symbols": [], "symbols": []},
                        },
                        "ndax_only": {
                            "per_coin": {
                                "eligible_symbol_count": 0,
                                "omitted_symbol_count": 2,
                                "eligible_symbols": [],
                                "omitted_symbols": ["GOODCAD", "SPARSECAD"],
                                "symbols": [],
                            },
                            "global": {"worst_symbols": [], "symbols": []},
                        },
                    },
                    "primary_scenario_summary": {
                        "eligible_symbols": [],
                        "omitted_symbols": ["GOODCAD", "SPARSECAD"],
                        "worst_global_symbols": [],
                    },
                },
            )
            attribution_md.write_text("# Coin Attribution Report\n", encoding="utf-8")

            feature_result = _feature_result(root, _feature_frame(rows_by_symbol={"GOODCAD": 1400, "SPARSECAD": 1400}, include_synthetic=True))
            with (
                mock.patch("qtbot.training.promotion.FeatureBuilder.build", return_value=feature_result),
                mock.patch(
                    "qtbot.training.promotion.AttributionService.generate",
                    return_value=AttributionSummary(
                        run_id=run_id,
                        primary_scenario="ndax_only",
                        artifact_dir=str(run_dir),
                        eligible_symbol_count=1,
                        omitted_symbol_count=1,
                        attribution_json=str(attribution_json),
                        attribution_markdown=str(attribution_md),
                        status="attributed",
                    ),
                ),
            ):
                summary = PromotionService(config=cfg, state_store=store).promote(run_id=run_id)

            self.assertEqual(summary.decision, "accepted")
            self.assertEqual(summary.primary_scenario, "weighted_combined")
            assert summary.bundle_dir is not None
            manifest = json.loads((Path(summary.bundle_dir) / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["primary_scenario"], "weighted_combined")

    def test_promote_rejects_when_global_gate_fails_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root, promotion_min_folds=4, promotion_min_conversion_pass_rate=0.0)
            store = StateStore(cfg.state_db)
            run_id = "run-reject"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {"run_id": run_id, "status": "evaluated", "primary_scenario": "weighted_combined"},
            )
            write_parquet_atomic(
                run_dir / "predictions" / "fold_01" / "weighted_combined.parquet",
                pd.DataFrame(
                    [
                        {
                            "run_id": run_id,
                            "snapshot_id": "snap123",
                            "fold_index": 1,
                            "scenario": "weighted_combined",
                            "model_scope": "per_coin",
                            "model_symbol": "SPARSECAD",
                            "symbol": "SPARSECAD",
                            "timestamp_ms": 1,
                            "source": "ndax",
                            "y": 1,
                            "forward_return": 0.02,
                            "supervised_row_weight": 1.0,
                            "probability": 0.9,
                        }
                    ]
                ),
            )
            _write_snapshot_manifest(root, snapshot_id="snap123")
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
                metrics_summary={
                    "weighted_combined": {
                        "global": {
                            "folds": 4,
                            "trades": 150,
                            "net_return": 1.0,
                            "max_drawdown": 0.40,
                            "pr_auc": 0.60,
                        }
                    }
                },
            )

            service = PromotionService(config=cfg, state_store=store)
            first = service.promote(run_id=run_id)
            second = service.promote(run_id=run_id)

            self.assertEqual(first.decision, "rejected")
            self.assertEqual(second.decision, "rejected")
            self.assertIsNone(first.bundle_id)
            self.assertTrue(any(item["gate"] == "max_drawdown" for item in first.hard_failures))
            self.assertIsNone(read_active_bundle_id(repo_root=root))

            promotion_record = store.get_promotion(run_id=run_id)
            assert promotion_record is not None
            self.assertEqual(promotion_record["decision"], "rejected")

    def test_model_status_and_set_active_bundle_are_integrity_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            store = StateStore(cfg.state_db)
            service = PromotionService(config=cfg, state_store=store)

            missing = service.model_status()
            self.assertEqual(missing.integrity_status, "missing")

            invalid_bundle = bundle_dir(repo_root=root, bundle_id="invalid-bundle")
            (invalid_bundle / "per_coin").mkdir(parents=True, exist_ok=True)
            for name in BUNDLE_REQUIRED_FILES:
                if name.endswith(".json"):
                    write_json_atomic(invalid_bundle / name, {"bundle_id": "invalid-bundle"})
                else:
                    (invalid_bundle / name).write_text("model\n", encoding="utf-8")
            write_active_bundle_id_atomic(repo_root=root, bundle_id="invalid-bundle")
            invalid = service.model_status()
            self.assertEqual(invalid.integrity_status, "invalid")

            bundle_a = _write_minimal_bundle(root, "bundle-a")
            bundle_b = _write_minimal_bundle(root, "bundle-b")
            write_active_bundle_id_atomic(repo_root=root, bundle_id="bundle-a")
            write_control(cfg.control_file, Command.RUN, updated_by="test", reason="runner active")

            with (
                mock.patch("qtbot.training.promotion.read_runner_pid", return_value=123),
                mock.patch("qtbot.training.promotion.is_pid_alive", return_value=True),
            ):
                with self.assertRaisesRegex(ValueError, "paused or stopped"):
                    service.set_active_bundle(bundle_id="bundle-b")
            self.assertEqual(read_active_bundle_id(repo_root=root), "bundle-a")

            write_control(cfg.control_file, Command.PAUSE, updated_by="test", reason="safe switch")
            with (
                mock.patch("qtbot.training.promotion.read_runner_pid", return_value=123),
                mock.patch("qtbot.training.promotion.is_pid_alive", return_value=True),
            ):
                status = service.set_active_bundle(bundle_id="bundle-b")
            self.assertEqual(status.bundle_id, "bundle-b")
            self.assertEqual(read_active_bundle_id(repo_root=root), "bundle-b")

            (bundle_a / "global_model.txt").write_text("tampered\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "signature invalid"):
                service.set_active_bundle(bundle_id="bundle-a")
            self.assertEqual(read_active_bundle_id(repo_root=root), "bundle-b")

    def test_promote_refit_uses_primary_scenario_row_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root, promotion_min_folds=1, promotion_min_trades=1, promotion_min_conversion_pass_rate=0.0)
            store = StateStore(cfg.state_db)
            run_id = "run-ndax-filter"
            run_dir = cfg.runtime_dir / "research" / "training" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            write_json_atomic(
                run_dir / "manifest.json",
                {"run_id": run_id, "status": "evaluated", "primary_scenario": "ndax_only"},
            )
            _write_snapshot_manifest(root, snapshot_id="snap123")
            store.insert_conversion_quality(
                symbol="GOODCAD",
                timeframe="15m",
                period_start="2026-01-01T00:00:00Z",
                period_end="2026-01-31T23:45:00Z",
                overlap_rows=1000,
                median_ape_close=0.001,
                median_abs_ret_err=0.001,
                ret_corr=0.9,
                direction_match=0.9,
                basis_median=0.0,
                basis_mad=0.0,
                quality_pass=True,
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
                folds_requested=1,
                folds_built=1,
                status="evaluated",
                artifact_dir=str(run_dir),
                primary_scenario="ndax_only",
                scenario_status={"ndax_only": {"status": "trained"}},
                metrics_summary={
                    "ndax_only": {
                        "global": {
                            "folds": 1,
                            "trades": 10,
                            "net_return": 1.0,
                            "max_drawdown": 0.05,
                            "pr_auc": 0.60,
                        }
                    }
                },
            )

            attribution_json = run_dir / "coin_attribution.json"
            attribution_md = run_dir / "coin_attribution.md"
            write_json_atomic(
                attribution_json,
                {
                    "run_id": run_id,
                    "generated_at_utc": "",
                    "primary_scenario": "ndax_only",
                    "slippage_stress_pct_per_side": cfg.promotion_slippage_stress_pct_per_side,
                    "scenarios": {
                        "ndax_only": {
                            "per_coin": {
                                "eligible_symbol_count": 1,
                                "omitted_symbol_count": 0,
                                "eligible_symbols": ["GOODCAD"],
                                "omitted_symbols": [],
                                "symbols": [
                                    {
                                        "symbol": "GOODCAD",
                                        "promotion_eligible": True,
                                        "bad_kind": None,
                                        "reasons": [],
                                        "trades": 120,
                                        "net_return": 1.0,
                                        "stressed_net_return": 0.7,
                                        "synthetic_share": 0.0,
                                    }
                                ],
                            },
                            "global": {"worst_symbols": [], "symbols": []},
                        }
                    },
                    "primary_scenario_summary": {
                        "eligible_symbols": ["GOODCAD"],
                        "omitted_symbols": [],
                        "worst_global_symbols": [],
                    },
                },
            )
            attribution_md.write_text("# Coin Attribution Report\n", encoding="utf-8")

            feature_result = _feature_result(root, _feature_frame(rows_by_symbol={"GOODCAD": 1800}, include_synthetic=True))
            captured_calls: list[tuple[pd.DataFrame, str | None]] = []

            class _FakeBooster:
                def save_model(self, path: str) -> None:
                    Path(path).write_text("model\n", encoding="utf-8")

            class _FakeModel:
                def __init__(self) -> None:
                    self.booster_ = _FakeBooster()

            def _capture_fit(*, rows: pd.DataFrame, sample_weight_column: str | None, seed: int):
                captured_calls.append((rows.copy(), sample_weight_column))
                return _FakeModel()

            with (
                mock.patch("qtbot.training.promotion.FeatureBuilder.build", return_value=feature_result),
                mock.patch("qtbot.training.promotion.fit_model", side_effect=_capture_fit),
                mock.patch(
                    "qtbot.training.promotion.AttributionService.generate",
                    return_value=AttributionSummary(
                        run_id=run_id,
                        primary_scenario="ndax_only",
                        artifact_dir=str(run_dir),
                        eligible_symbol_count=1,
                        omitted_symbol_count=0,
                        attribution_json=str(attribution_json),
                        attribution_markdown=str(attribution_md),
                        status="attributed",
                    ),
                ),
            ):
                summary = PromotionService(config=cfg, state_store=store).promote(run_id=run_id)

            self.assertEqual(summary.decision, "accepted")
            self.assertGreaterEqual(len(captured_calls), 2)
            for frame, sample_weight_column in captured_calls:
                self.assertTrue((frame["source"] == "ndax").all())
                self.assertIsNone(sample_weight_column)


if __name__ == "__main__":
    unittest.main()
