"""Shared deterministic model fitting helpers."""

from __future__ import annotations

import lightgbm as lgb
import pandas as pd

from qtbot.training.feature_spec import FEATURE_COLUMNS

SCENARIOS = ("ndax_only", "weighted_combined")
PER_COIN_MIN_TRAIN_ROWS = 1000
PER_COIN_MIN_VALID_ROWS = 100


def build_model_params(*, seed: int) -> dict[str, object]:
    return {
        "objective": "binary",
        "learning_rate": 0.05,
        "n_estimators": 200,
        "num_leaves": 31,
        "min_child_samples": 50,
        "subsample": 1.0,
        "colsample_bytree": 1.0,
        "reg_alpha": 0.0,
        "reg_lambda": 0.0,
        "random_state": seed,
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "verbosity": -1,
    }


def rows_for_scenario(*, rows: pd.DataFrame, scenario: str) -> pd.DataFrame:
    if scenario == "ndax_only":
        return rows.loc[rows["source"] == "ndax"].reset_index(drop=True)
    if scenario == "weighted_combined":
        return rows.reset_index(drop=True)
    raise ValueError(f"unsupported training scenario: {scenario}")


def scenario_skip_reason(*, train_rows: pd.DataFrame, valid_rows: pd.DataFrame, scenario: str) -> str | None:
    scenario_train = rows_for_scenario(rows=train_rows, scenario=scenario)
    scenario_valid = rows_for_scenario(rows=valid_rows, scenario=scenario)
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


def ensure_binary_labels(*, rows: pd.DataFrame, context: str) -> None:
    if rows.empty:
        raise ValueError(f"{context} has no rows")
    labels = {int(value) for value in rows["y"].unique()}
    if labels != {0, 1}:
        raise ValueError(f"{context} must contain both classes, observed={sorted(labels)}")


def fit_model(*, rows: pd.DataFrame, sample_weight_column: str | None, seed: int) -> lgb.LGBMClassifier:
    model = lgb.LGBMClassifier(**build_model_params(seed=seed))
    sample_weight = None
    if sample_weight_column is not None:
        sample_weight = rows[sample_weight_column].astype("float64").to_numpy()
    model.fit(
        rows[FEATURE_COLUMNS].astype("float32"),
        rows["y"].astype("int32").to_numpy(),
        sample_weight=sample_weight,
    )
    return model


def prediction_frame(
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


def per_coin_skip_reason(*, train_rows: pd.DataFrame, valid_rows: pd.DataFrame) -> str | None:
    if len(train_rows) < PER_COIN_MIN_TRAIN_ROWS:
        return f"train_rows_lt_{PER_COIN_MIN_TRAIN_ROWS}"
    if len(valid_rows) < PER_COIN_MIN_VALID_ROWS:
        return f"valid_rows_lt_{PER_COIN_MIN_VALID_ROWS}"
    train_labels = {int(value) for value in train_rows["y"].unique()}
    if train_labels != {0, 1}:
        return "train_missing_class"
    valid_labels = {int(value) for value in valid_rows["y"].unique()}
    if valid_labels != {0, 1}:
        return "valid_missing_class"
    return None


def final_per_coin_fit_skip_reason(*, rows: pd.DataFrame) -> str | None:
    if len(rows) < PER_COIN_MIN_TRAIN_ROWS:
        return f"train_rows_lt_{PER_COIN_MIN_TRAIN_ROWS}"
    labels = {int(value) for value in rows["y"].unique()}
    if labels != {0, 1}:
        return "train_missing_class"
    return None
