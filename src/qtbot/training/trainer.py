"""Phase 6 deterministic walk-forward training service."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import lightgbm as lgb
import pandas as pd

from qtbot.config import RuntimeConfig
from qtbot.state import StateStore
from qtbot.training.artifacts import build_run_id, ensure_run_dir, write_json_atomic, write_parquet_atomic
from qtbot.training.feature_builder import FeatureBuilder
from qtbot.training.feature_spec import FEATURE_COLUMNS, feature_spec_payload
from qtbot.training.folds import FoldDefinition, build_walk_forward_folds, month_mask


_SCENARIOS = ("ndax_only", "weighted_combined")
_PER_COIN_MIN_TRAIN_ROWS = 1000
_PER_COIN_MIN_VALID_ROWS = 100


@dataclass(frozen=True)
class TrainingSummary:
    run_id: str
    snapshot_id: str
    dataset_hash: str
    feature_spec_hash: str
    artifact_dir: str
    folds_requested: int
    folds_built: int
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _ScenarioTrainingResult:
    predictions: pd.DataFrame
    per_coin_skip_reasons: dict[str, str]
    per_coin_model_count: int


class TrainingService:
    """Train deterministic global and per-coin LightGBM models over a sealed snapshot."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._repo_root = config.runtime_dir.parent

    def train(
        self,
        *,
        snapshot_id: str,
        folds: int,
        universe: str,
    ) -> TrainingSummary:
        if universe.strip().upper() != "V1":
            raise ValueError("train currently supports --universe V1 only")
        if folds <= 0:
            raise ValueError("--folds must be > 0")

        feature_builder = FeatureBuilder(repo_root=self._repo_root)
        feature_result = feature_builder.build(snapshot_id=snapshot_id)
        rows = feature_result.data
        if rows.empty:
            raise ValueError(f"snapshot contains no trainable rows: {snapshot_id}")

        fold_defs = build_walk_forward_folds(
            rows=rows,
            requested_folds=folds,
            train_window_months=self._config.train_window_months,
            valid_window_months=self._config.valid_window_months,
            step_months=self._config.train_step_months,
            interval_seconds=feature_result.summary.interval_seconds,
        )

        run_id = build_run_id(snapshot_id=snapshot_id)
        run_dir = ensure_run_dir(runtime_dir=self._config.runtime_dir, run_id=run_id)
        scenario_status: dict[str, dict[str, object]] = {
            scenario: {
                "status": "running",
                "folds_completed": 0,
                "folds_skipped": 0,
                "global_models": 0,
                "per_coin_models": 0,
                "skip_reasons": [],
            }
            for scenario in _SCENARIOS
        }
        summary = TrainingSummary(
            run_id=run_id,
            snapshot_id=snapshot_id,
            dataset_hash=feature_result.summary.dataset_hash,
            feature_spec_hash=feature_result.summary.feature_spec_hash,
            artifact_dir=str(run_dir),
            folds_requested=folds,
            folds_built=len(fold_defs),
            status="running",
        )
        self._state_store.upsert_training_run(
            run_id=run_id,
            snapshot_id=snapshot_id,
            dataset_hash=summary.dataset_hash,
            feature_spec_hash=summary.feature_spec_hash,
            seed=self._config.train_seed,
            timeframe=feature_result.summary.timeframe,
            train_window_months=self._config.train_window_months,
            valid_window_months=self._config.valid_window_months,
            train_step_months=self._config.train_step_months,
            folds_requested=folds,
            folds_built=len(fold_defs),
            status="running",
            artifact_dir=str(run_dir),
            scenario_status=scenario_status,
            metrics_summary={},
        )
        write_json_atomic(run_dir / "feature_spec.json", feature_spec_payload())
        write_json_atomic(
            run_dir / "folds.json",
            {"folds": [fold.to_payload() for fold in fold_defs]},
        )
        self._write_manifest(
            run_dir=run_dir,
            summary=summary,
            feature_summary=feature_result.summary.to_payload(),
            scenario_status=scenario_status,
            folds=fold_defs,
        )

        for fold in fold_defs:
            fold_train_mask = month_mask(
                months=rows["month"],
                start_month=fold.train_start_month,
                end_month=fold.train_end_month,
            )
            fold_valid_mask = month_mask(
                months=rows["month"],
                start_month=fold.valid_start_month,
                end_month=fold.valid_end_month,
            )
            fold_train = rows.loc[fold_train_mask].reset_index(drop=True)
            fold_valid = rows.loc[fold_valid_mask].reset_index(drop=True)
            if fold_train.empty or fold_valid.empty:
                raise ValueError(
                    f"fold {fold.fold_index} has empty train/valid split: "
                    f"train_rows={len(fold_train)} valid_rows={len(fold_valid)}"
                )

            fold_predictions_dir = run_dir / "predictions" / f"fold_{fold.fold_index:02d}"
            fold_source_mix = _fold_source_mix(train_rows=fold_train, valid_rows=fold_valid)
            per_coin_skip_reasons: dict[str, dict[str, str]] = {}
            for scenario in _SCENARIOS:
                scenario_skip_reason = _scenario_skip_reason(
                    train_rows=fold_train,
                    valid_rows=fold_valid,
                    scenario=scenario,
                )
                if scenario_skip_reason is not None:
                    if scenario == "weighted_combined":
                        raise ValueError(
                            f"fold={fold.fold_index} scenario={scenario} {scenario_skip_reason}"
                        )
                    scenario_status[scenario]["folds_skipped"] = int(scenario_status[scenario]["folds_skipped"]) + 1
                    cast_skip_reasons = scenario_status[scenario]["skip_reasons"]
                    if isinstance(cast_skip_reasons, list):
                        cast_skip_reasons.append(
                            {
                                "fold_index": fold.fold_index,
                                "reason": scenario_skip_reason,
                            }
                        )
                    continue
                scenario_result = self._train_scenario(
                    run_dir=run_dir,
                    fold=fold,
                    scenario=scenario,
                    train_rows=fold_train,
                    valid_rows=fold_valid,
                    run_id=run_id,
                    snapshot_id=snapshot_id,
                )
                scenario_status[scenario]["folds_completed"] = int(scenario_status[scenario]["folds_completed"]) + 1
                scenario_status[scenario]["global_models"] = int(scenario_status[scenario]["global_models"]) + 1
                scenario_status[scenario]["per_coin_models"] = int(scenario_status[scenario]["per_coin_models"]) + scenario_result.per_coin_model_count
                for symbol, reason in scenario_result.per_coin_skip_reasons.items():
                    per_coin_skip_reasons.setdefault(symbol, {})[scenario] = reason
                write_parquet_atomic(
                    fold_predictions_dir / f"{scenario}.parquet",
                    scenario_result.predictions,
                )

            self._state_store.upsert_training_fold(
                run_id=run_id,
                fold_index=fold.fold_index,
                train_start_month=fold.train_start_month,
                train_end_month=fold.train_end_month,
                valid_start_month=fold.valid_start_month,
                valid_end_month=fold.valid_end_month,
                train_rows=len(fold_train),
                valid_rows=len(fold_valid),
                source_mix=fold_source_mix,
                per_coin_skip_reasons=per_coin_skip_reasons,
                artifact_dir=str(fold_predictions_dir),
                status="trained",
            )

        final_summary = TrainingSummary(
            run_id=run_id,
            snapshot_id=snapshot_id,
            dataset_hash=summary.dataset_hash,
            feature_spec_hash=summary.feature_spec_hash,
            artifact_dir=str(run_dir),
            folds_requested=folds,
            folds_built=len(fold_defs),
            status="trained",
        )
        for scenario in _SCENARIOS:
            folds_completed = int(scenario_status[scenario]["folds_completed"])
            folds_skipped = int(scenario_status[scenario]["folds_skipped"])
            if folds_completed == len(fold_defs):
                scenario_status[scenario]["status"] = "trained"
            elif folds_completed > 0:
                scenario_status[scenario]["status"] = "partial"
            elif folds_skipped > 0:
                scenario_status[scenario]["status"] = "skipped"
            else:
                scenario_status[scenario]["status"] = "skipped"
        self._write_manifest(
            run_dir=run_dir,
            summary=final_summary,
            feature_summary=feature_result.summary.to_payload(),
            scenario_status=scenario_status,
            folds=fold_defs,
        )
        self._state_store.upsert_training_run(
            run_id=run_id,
            snapshot_id=snapshot_id,
            dataset_hash=final_summary.dataset_hash,
            feature_spec_hash=final_summary.feature_spec_hash,
            seed=self._config.train_seed,
            timeframe=feature_result.summary.timeframe,
            train_window_months=self._config.train_window_months,
            valid_window_months=self._config.valid_window_months,
            train_step_months=self._config.train_step_months,
            folds_requested=folds,
            folds_built=len(fold_defs),
            status="trained",
            artifact_dir=str(run_dir),
            scenario_status=scenario_status,
            metrics_summary={},
        )
        return final_summary

    def _train_scenario(
        self,
        *,
        run_dir: Path,
        fold: FoldDefinition,
        scenario: str,
        train_rows: pd.DataFrame,
        valid_rows: pd.DataFrame,
        run_id: str,
        snapshot_id: str,
    ) -> _ScenarioTrainingResult:
        scenario_train = _rows_for_scenario(rows=train_rows, scenario=scenario)
        scenario_valid = _rows_for_scenario(rows=valid_rows, scenario=scenario)
        _ensure_binary_labels(rows=scenario_train, context=f"fold={fold.fold_index} scenario={scenario} train")
        _ensure_binary_labels(rows=scenario_valid, context=f"fold={fold.fold_index} scenario={scenario} valid")

        global_model = _fit_model(
            rows=scenario_train,
            sample_weight_column=(None if scenario == "ndax_only" else "supervised_row_weight"),
            seed=self._config.train_seed,
        )
        global_model_path = run_dir / "models" / "global" / scenario / f"fold_{fold.fold_index:02d}.txt"
        global_model_path.parent.mkdir(parents=True, exist_ok=True)
        global_model.booster_.save_model(str(global_model_path))

        prediction_frames = [
            _prediction_frame(
                model=global_model,
                rows=scenario_valid,
                run_id=run_id,
                snapshot_id=snapshot_id,
                fold_index=fold.fold_index,
                scenario=scenario,
                model_scope="global",
                model_symbol=None,
            )
        ]

        per_coin_skip_reasons: dict[str, str] = {}
        per_coin_model_count = 0
        for symbol in sorted(scenario_valid["symbol"].unique()):
            train_symbol_rows = scenario_train.loc[scenario_train["symbol"] == symbol].reset_index(drop=True)
            valid_symbol_rows = scenario_valid.loc[scenario_valid["symbol"] == symbol].reset_index(drop=True)
            skip_reason = _per_coin_skip_reason(train_rows=train_symbol_rows, valid_rows=valid_symbol_rows)
            if skip_reason is not None:
                per_coin_skip_reasons[symbol] = skip_reason
                continue
            per_coin_model = _fit_model(
                rows=train_symbol_rows,
                sample_weight_column=(None if scenario == "ndax_only" else "supervised_row_weight"),
                seed=self._config.train_seed,
            )
            per_coin_path = run_dir / "models" / "per_coin" / symbol / scenario / f"fold_{fold.fold_index:02d}.txt"
            per_coin_path.parent.mkdir(parents=True, exist_ok=True)
            per_coin_model.booster_.save_model(str(per_coin_path))
            prediction_frames.append(
                _prediction_frame(
                    model=per_coin_model,
                    rows=valid_symbol_rows,
                    run_id=run_id,
                    snapshot_id=snapshot_id,
                    fold_index=fold.fold_index,
                    scenario=scenario,
                    model_scope="per_coin",
                    model_symbol=symbol,
                )
            )
            per_coin_model_count += 1

        predictions = pd.concat(prediction_frames, ignore_index=True)
        predictions.sort_values(["model_scope", "model_symbol", "symbol", "timestamp_ms"], inplace=True, kind="mergesort")
        predictions.reset_index(drop=True, inplace=True)
        return _ScenarioTrainingResult(
            predictions=predictions,
            per_coin_skip_reasons=per_coin_skip_reasons,
            per_coin_model_count=per_coin_model_count,
        )

    @staticmethod
    def _write_manifest(
        *,
        run_dir: Path,
        summary: TrainingSummary,
        feature_summary: dict[str, object],
        scenario_status: dict[str, dict[str, object]],
        folds: list[FoldDefinition],
    ) -> None:
        payload = {
            "run_id": summary.run_id,
            "snapshot_id": summary.snapshot_id,
            "dataset_hash": summary.dataset_hash,
            "feature_spec_hash": summary.feature_spec_hash,
            "artifact_dir": summary.artifact_dir,
            "folds_requested": summary.folds_requested,
            "folds_built": summary.folds_built,
            "status": summary.status,
            "feature_summary": feature_summary,
            "scenario_status": scenario_status,
            "folds": [fold.to_payload() for fold in folds],
        }
        write_json_atomic(run_dir / "manifest.json", payload)


def _rows_for_scenario(*, rows: pd.DataFrame, scenario: str) -> pd.DataFrame:
    if scenario == "ndax_only":
        return rows.loc[rows["source"] == "ndax"].reset_index(drop=True)
    if scenario == "weighted_combined":
        return rows.reset_index(drop=True)
    raise ValueError(f"unsupported training scenario: {scenario}")


def _scenario_skip_reason(*, train_rows: pd.DataFrame, valid_rows: pd.DataFrame, scenario: str) -> str | None:
    scenario_train = _rows_for_scenario(rows=train_rows, scenario=scenario)
    scenario_valid = _rows_for_scenario(rows=valid_rows, scenario=scenario)
    if scenario_train.empty:
        return "train has no rows"
    if scenario_valid.empty:
        return "valid has no rows"
    train_labels = {int(value) for value in scenario_train["y"].unique()}
    if train_labels != {0, 1}:
        return f"train must contain both classes, observed={sorted(train_labels)}"
    valid_labels = {int(value) for value in scenario_valid["y"].unique()}
    if valid_labels != {0, 1}:
        return f"valid must contain both classes, observed={sorted(valid_labels)}"
    return None


def _ensure_binary_labels(*, rows: pd.DataFrame, context: str) -> None:
    if rows.empty:
        raise ValueError(f"{context} has no rows")
    labels = {int(value) for value in rows["y"].unique()}
    if labels != {0, 1}:
        raise ValueError(f"{context} must contain both classes, observed={sorted(labels)}")


def _fit_model(*, rows: pd.DataFrame, sample_weight_column: str | None, seed: int) -> lgb.LGBMClassifier:
    model = lgb.LGBMClassifier(
        objective="binary",
        learning_rate=0.05,
        n_estimators=200,
        num_leaves=31,
        min_child_samples=50,
        subsample=1.0,
        colsample_bytree=1.0,
        reg_alpha=0.0,
        reg_lambda=0.0,
        random_state=seed,
        deterministic=True,
        force_col_wise=True,
        n_jobs=1,
        verbosity=-1,
    )
    sample_weight = None
    if sample_weight_column is not None:
        sample_weight = rows[sample_weight_column].astype("float64").to_numpy()
    model.fit(
        rows[FEATURE_COLUMNS].astype("float32"),
        rows["y"].astype("int32").to_numpy(),
        sample_weight=sample_weight,
    )
    return model


def _prediction_frame(
    *,
    model: lgb.LGBMClassifier,
    rows: pd.DataFrame,
    run_id: str,
    snapshot_id: str,
    fold_index: int,
    scenario: str,
    model_scope: str,
    model_symbol: str | None,
) -> pd.DataFrame:
    probability = model.predict_proba(rows[FEATURE_COLUMNS].astype("float32"))[:, 1]
    return pd.DataFrame(
        {
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "fold_index": int(fold_index),
            "scenario": scenario,
            "model_scope": model_scope,
            "model_symbol": model_symbol,
            "symbol": rows["symbol"].astype(str).to_numpy(),
            "timestamp_ms": rows["timestamp_ms"].astype("int64").to_numpy(),
            "source": rows["source"].astype(str).to_numpy(),
            "y": rows["y"].astype("int8").to_numpy(),
            "forward_return": rows["forward_return"].astype("float64").to_numpy(),
            "supervised_row_weight": rows["supervised_row_weight"].astype("float64").to_numpy(),
            "probability": probability.astype("float64"),
        }
    )


def _per_coin_skip_reason(*, train_rows: pd.DataFrame, valid_rows: pd.DataFrame) -> str | None:
    if len(train_rows) < _PER_COIN_MIN_TRAIN_ROWS:
        return f"train_rows_lt_{_PER_COIN_MIN_TRAIN_ROWS}"
    if len(valid_rows) < _PER_COIN_MIN_VALID_ROWS:
        return f"valid_rows_lt_{_PER_COIN_MIN_VALID_ROWS}"
    train_labels = {int(value) for value in train_rows["y"].unique()}
    if train_labels != {0, 1}:
        return "train_missing_class"
    valid_labels = {int(value) for value in valid_rows["y"].unique()}
    if valid_labels != {0, 1}:
        return "valid_missing_class"
    return None


def _fold_source_mix(*, train_rows: pd.DataFrame, valid_rows: pd.DataFrame) -> dict[str, int]:
    payload: dict[str, int] = {}
    for prefix, frame in (("train", train_rows), ("valid", valid_rows)):
        counts = frame["source"].value_counts().to_dict()
        for source, count in counts.items():
            payload[f"{prefix}_{source}"] = int(count)
    return payload
