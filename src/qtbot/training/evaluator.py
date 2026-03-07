"""Phase 6 deterministic validation evaluator."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from qtbot.config import RuntimeConfig
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic
from qtbot.training.metrics import iter_splits, metric_row, select_primary_scenario, summarize_metrics


@dataclass(frozen=True)
class EvaluationSummary:
    run_id: str
    primary_scenario: str
    metrics_summary: dict[str, object]
    artifact_dir: str
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


class EvaluationService:
    """Compute deterministic fold metrics from persisted validation predictions."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._state_store = state_store

    def evaluate(self, *, run_id: str) -> EvaluationSummary:
        run = self._state_store.get_training_run(run_id=run_id)
        if run is None:
            raise ValueError(f"training run not found: {run_id}")

        run_dir = Path(str(run["artifact_dir"]))
        predictions_root = run_dir / "predictions"
        if not predictions_root.exists():
            raise ValueError(f"prediction artifacts missing for run: {run_id}")

        prediction_files = sorted(predictions_root.rglob("*.parquet"))
        if not prediction_files:
            raise ValueError(f"prediction artifacts missing for run: {run_id}")

        self._state_store.delete_fold_metrics(run_id=run_id)
        metrics_rows: list[dict[str, object]] = []
        for file_path in prediction_files:
            frame = pd.read_parquet(file_path)
            if frame.empty:
                continue
            fold_index = int(frame["fold_index"].iloc[0])
            scenario = str(frame["scenario"].iloc[0])
            for model_scope, scope_frame in frame.groupby("model_scope", sort=True):
                for split, split_frame in iter_splits(scope_frame):
                    metric_payload = metric_row(
                        frame=split_frame,
                        fee_pct_per_side=self._config.fee_pct_per_side,
                    )
                    if metric_payload is None:
                        continue
                    record = {
                        "run_id": run_id,
                        "fold_index": fold_index,
                        "scenario": scenario,
                        "model_scope": str(model_scope),
                        "symbol": None,
                        "split": split,
                        **metric_payload,
                    }
                    metrics_rows.append(record)
                    self._state_store.insert_fold_metric(**record)

                for symbol, symbol_frame in scope_frame.groupby("symbol", sort=True):
                    for split, split_frame in iter_splits(symbol_frame):
                        metric_payload = metric_row(
                            frame=split_frame,
                            fee_pct_per_side=self._config.fee_pct_per_side,
                        )
                        if metric_payload is None:
                            continue
                        record = {
                            "run_id": run_id,
                            "fold_index": fold_index,
                            "scenario": scenario,
                            "model_scope": str(model_scope),
                            "symbol": str(symbol),
                            "split": split,
                            **metric_payload,
                        }
                        metrics_rows.append(record)
                        self._state_store.insert_fold_metric(**record)

        metrics_summary = summarize_metrics(metrics_rows)
        primary_scenario = select_primary_scenario(metrics_summary)
        payload = {
            "run_id": run_id,
            "primary_scenario": primary_scenario,
            "metrics_summary": metrics_summary,
            "metrics": metrics_rows,
        }
        write_json_atomic(run_dir / "metrics.json", payload)

        scenario_status = dict(run.get("scenario_status", {}))
        for value in scenario_status.values():
            if isinstance(value, dict):
                value["evaluation_status"] = "evaluated"
        self._state_store.upsert_training_run(
            run_id=run_id,
            snapshot_id=str(run["snapshot_id"]),
            dataset_hash=str(run["dataset_hash"]),
            feature_spec_hash=str(run["feature_spec_hash"]),
            seed=int(run["seed"]),
            timeframe=str(run["timeframe"]),
            train_window_months=int(run["train_window_months"]),
            valid_window_months=int(run["valid_window_months"]),
            train_step_months=int(run["train_step_months"]),
            folds_requested=int(run["folds_requested"]),
            folds_built=int(run["folds_built"]),
            status="evaluated",
            artifact_dir=str(run_dir),
            primary_scenario=primary_scenario,
            scenario_status=scenario_status,
            metrics_summary=metrics_summary,
        )
        manifest_path = run_dir / "manifest.json"
        manifest_payload = {}
        if manifest_path.exists():
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest_payload = json.load(handle)
        manifest_payload.update(
            {
                "status": "evaluated",
                "primary_scenario": primary_scenario,
                "metrics_summary": metrics_summary,
            }
        )
        write_json_atomic(manifest_path, manifest_payload)
        return EvaluationSummary(
            run_id=run_id,
            primary_scenario=primary_scenario,
            metrics_summary=metrics_summary,
            artifact_dir=str(run_dir),
            status="evaluated",
        )
