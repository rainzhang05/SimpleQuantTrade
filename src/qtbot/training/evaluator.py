"""Phase 6 deterministic validation evaluator."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

from qtbot.config import RuntimeConfig
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic


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
                for split, split_frame in _iter_splits(scope_frame):
                    metric_row = _metric_row(
                        frame=split_frame,
                        fee_pct_per_side=self._config.fee_pct_per_side,
                    )
                    if metric_row is None:
                        continue
                    record = {
                        "run_id": run_id,
                        "fold_index": fold_index,
                        "scenario": scenario,
                        "model_scope": str(model_scope),
                        "symbol": None,
                        "split": split,
                        **metric_row,
                    }
                    metrics_rows.append(record)
                    self._state_store.insert_fold_metric(**record)

                if str(model_scope) == "per_coin":
                    for symbol, symbol_frame in scope_frame.groupby("model_symbol", sort=True):
                        metric_row = _metric_row(
                            frame=symbol_frame,
                            fee_pct_per_side=self._config.fee_pct_per_side,
                        )
                        if metric_row is None:
                            continue
                        record = {
                            "run_id": run_id,
                            "fold_index": fold_index,
                            "scenario": scenario,
                            "model_scope": "per_coin",
                            "symbol": str(symbol),
                            "split": "all",
                            **metric_row,
                        }
                        metrics_rows.append(record)
                        self._state_store.insert_fold_metric(**record)

        metrics_summary = _summarize_metrics(metrics_rows)
        primary_scenario = _select_primary_scenario(metrics_summary)
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


def _iter_splits(frame: pd.DataFrame):
    yield "all", frame
    ndax = frame.loc[frame["source"] == "ndax"]
    if not ndax.empty:
        yield "ndax", ndax
    synthetic = frame.loc[frame["source"].isin(["synthetic", "synthetic_gap_fill"])]
    if not synthetic.empty:
        yield "synthetic", synthetic


def _metric_row(*, frame: pd.DataFrame, fee_pct_per_side: float) -> dict[str, object] | None:
    if frame.empty:
        return None
    y_true = frame["y"].astype("int32").to_numpy()
    probability = frame["probability"].astype("float64").clip(1e-6, 1.0 - 1e-6).to_numpy()
    forward_return = frame["forward_return"].astype("float64").to_numpy()

    trade_mask = probability >= 0.5
    traded_returns = forward_return[trade_mask]
    net_trade_returns = traded_returns - (2.0 * fee_pct_per_side)

    metric_row: dict[str, object] = {
        "row_count": int(len(frame)),
        "trades": int(trade_mask.sum()),
        "gross_return": float(traded_returns.sum()) if traded_returns.size else 0.0,
        "net_return": float(net_trade_returns.sum()) if net_trade_returns.size else 0.0,
        "win_rate": (float((net_trade_returns > 0.0).mean()) if net_trade_returns.size else None),
        "max_drawdown": (_max_drawdown(net_trade_returns) if net_trade_returns.size else None),
        "logloss": float(log_loss(y_true, probability, labels=[0, 1])),
        "roc_auc": None,
        "pr_auc": None,
        "brier": float(brier_score_loss(y_true, probability)),
    }
    if len(set(int(value) for value in y_true)) == 2:
        metric_row["roc_auc"] = float(roc_auc_score(y_true, probability))
        metric_row["pr_auc"] = float(average_precision_score(y_true, probability))
    return metric_row


def _max_drawdown(returns: np.ndarray) -> float:
    if returns.size <= 0:
        return 0.0
    equity = np.cumsum(returns)
    peaks = np.maximum.accumulate(equity)
    drawdowns = peaks - equity
    return float(drawdowns.max(initial=0.0))


def _summarize_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    summary: dict[str, object] = {}
    if not rows:
        return summary

    frame = pd.DataFrame(rows)
    global_rows = frame[(frame["model_scope"] == "global") & (frame["split"] == "all")]
    per_coin_rows = frame[(frame["model_scope"] == "per_coin") & (frame["split"] == "all") & (frame["symbol"].isna())]
    for scenario in sorted(frame["scenario"].dropna().unique()):
        scenario_summary: dict[str, object] = {}
        for label, subset in (("global", global_rows[global_rows["scenario"] == scenario]), ("per_coin", per_coin_rows[per_coin_rows["scenario"] == scenario])):
            if subset.empty:
                continue
            scenario_summary[label] = {
                "folds": int(subset["fold_index"].nunique()),
                "row_count": int(subset["row_count"].sum()),
                "trades": int(subset["trades"].sum()),
                "gross_return": float(subset["gross_return"].sum()),
                "net_return": float(subset["net_return"].sum()),
                "win_rate": _mean_nullable(subset["win_rate"]),
                "max_drawdown": _mean_nullable(subset["max_drawdown"]),
                "logloss": _mean_nullable(subset["logloss"]),
                "roc_auc": _mean_nullable(subset["roc_auc"]),
                "pr_auc": _mean_nullable(subset["pr_auc"]),
                "brier": _mean_nullable(subset["brier"]),
            }
        summary[str(scenario)] = scenario_summary
    return summary


def _select_primary_scenario(metrics_summary: dict[str, object]) -> str:
    best_scenario = ""
    best_key: tuple[float, float, str] | None = None
    for scenario, payload in metrics_summary.items():
        if not isinstance(payload, dict):
            continue
        global_payload = payload.get("global")
        if not isinstance(global_payload, dict):
            continue
        key = (
            float(global_payload.get("net_return") or 0.0),
            float(global_payload.get("pr_auc") or 0.0),
            str(scenario),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_scenario = str(scenario)
    if not best_scenario:
        raise ValueError("unable to select primary scenario from evaluation metrics")
    return best_scenario


def _mean_nullable(series: pd.Series) -> float | None:
    values = [float(value) for value in series.dropna().tolist()]
    if not values:
        return None
    return float(sum(values) / len(values))
