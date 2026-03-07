"""Shared metric helpers for evaluation, attribution, and promotion."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score


def iter_splits(frame: pd.DataFrame):
    yield "all", frame
    ndax = frame.loc[frame["source"] == "ndax"]
    if not ndax.empty:
        yield "ndax", ndax
    synthetic = frame.loc[frame["source"].isin(["synthetic", "synthetic_gap_fill"])]
    if not synthetic.empty:
        yield "synthetic", synthetic


def metric_row(
    *,
    frame: pd.DataFrame,
    fee_pct_per_side: float,
    trade_threshold: float = 0.5,
) -> dict[str, object] | None:
    if frame.empty:
        return None
    y_true = frame["y"].astype("int32").to_numpy()
    probability = frame["probability"].astype("float64").clip(1e-6, 1.0 - 1e-6).to_numpy()
    forward_return = frame["forward_return"].astype("float64").to_numpy()

    trade_mask = probability >= float(trade_threshold)
    traded_returns = forward_return[trade_mask]
    net_trade_returns = traded_returns - (2.0 * fee_pct_per_side)

    payload: dict[str, object] = {
        "row_count": int(len(frame)),
        "trades": int(trade_mask.sum()),
        "gross_return": float(traded_returns.sum()) if traded_returns.size else 0.0,
        "net_return": float(net_trade_returns.sum()) if net_trade_returns.size else 0.0,
        "win_rate": (float((net_trade_returns > 0.0).mean()) if net_trade_returns.size else None),
        "max_drawdown": (max_drawdown(net_trade_returns) if net_trade_returns.size else None),
        "logloss": float(log_loss(y_true, probability, labels=[0, 1])),
        "roc_auc": None,
        "pr_auc": None,
        "brier": float(brier_score_loss(y_true, probability)),
    }
    if len(set(int(value) for value in y_true)) == 2:
        payload["roc_auc"] = float(roc_auc_score(y_true, probability))
        payload["pr_auc"] = float(average_precision_score(y_true, probability))
    return payload


def summarize_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    summary: dict[str, object] = {}
    if not rows:
        return summary

    frame = pd.DataFrame(rows)
    aggregate_rows = frame[(frame["split"] == "all") & (frame["symbol"].isna())]
    for scenario in sorted(frame["scenario"].dropna().unique()):
        scenario_summary: dict[str, object] = {}
        for label in ("global", "per_coin"):
            subset = aggregate_rows[
                (aggregate_rows["scenario"] == scenario) & (aggregate_rows["model_scope"] == label)
            ]
            if subset.empty:
                continue
            scenario_summary[label] = {
                "folds": int(subset["fold_index"].nunique()),
                "row_count": int(subset["row_count"].sum()),
                "trades": int(subset["trades"].sum()),
                "gross_return": float(subset["gross_return"].sum()),
                "net_return": float(subset["net_return"].sum()),
                "win_rate": mean_nullable(subset["win_rate"]),
                "max_drawdown": mean_nullable(subset["max_drawdown"]),
                "logloss": mean_nullable(subset["logloss"]),
                "roc_auc": mean_nullable(subset["roc_auc"]),
                "pr_auc": mean_nullable(subset["pr_auc"]),
                "brier": mean_nullable(subset["brier"]),
            }
        summary[str(scenario)] = scenario_summary
    return summary


def select_primary_scenario(metrics_summary: dict[str, object]) -> str:
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


def stressed_net_return(*, net_return: float, trades: int, slippage_stress_pct_per_side: float) -> float:
    return float(net_return) - (2.0 * int(trades) * float(slippage_stress_pct_per_side))


def max_drawdown(returns: np.ndarray) -> float:
    if returns.size <= 0:
        return 0.0
    equity = np.cumsum(returns)
    peaks = np.maximum.accumulate(equity)
    drawdowns = peaks - equity
    return float(drawdowns.max(initial=0.0))


def mean_nullable(series: pd.Series) -> float | None:
    values = [float(value) for value in series.dropna().tolist()]
    if not values:
        return None
    return float(sum(values) / len(values))
