"""Deterministic coin attribution reporting for evaluated runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import pandas as pd

from qtbot.config import RuntimeConfig
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_text_atomic
from qtbot.training.metrics import iter_splits, metric_row, stressed_net_return


@dataclass(frozen=True)
class AttributionSummary:
    run_id: str
    primary_scenario: str
    artifact_dir: str
    eligible_symbol_count: int
    omitted_symbol_count: int
    attribution_json: str
    attribution_markdown: str
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


class AttributionService:
    """Generate deterministic per-symbol attribution from evaluated predictions."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._state_store = state_store

    def generate(self, *, run_id: str) -> AttributionSummary:
        run = self._state_store.get_training_run(run_id=run_id)
        if run is None:
            raise ValueError(f"training run not found: {run_id}")
        if str(run["status"]) not in {"evaluated", "trained"}:
            raise ValueError(f"training run must be trained or evaluated before attribution: {run_id}")

        run_dir = Path(str(run["artifact_dir"]))
        predictions_root = run_dir / "predictions"
        if not predictions_root.exists():
            raise ValueError(f"prediction artifacts missing for run: {run_id}")

        primary_scenario = str(run.get("primary_scenario") or "")
        if not primary_scenario:
            raise ValueError(f"training run missing primary scenario: {run_id}")

        symbol_metrics = self._collect_symbol_metrics(
            predictions_root=predictions_root,
            fee_pct_per_side=self._config.fee_pct_per_side,
            slippage_stress_pct_per_side=self._config.promotion_slippage_stress_pct_per_side,
        )
        scenario_payloads = self._build_scenario_payloads(symbol_metrics=symbol_metrics)
        primary_per_coin = scenario_payloads.get(primary_scenario, {}).get("per_coin", {})
        eligible_symbols = [
            str(item["symbol"])
            for item in primary_per_coin.get("symbols", [])
            if bool(item.get("promotion_eligible"))
        ]
        omitted_symbols = [
            str(item["symbol"])
            for item in primary_per_coin.get("symbols", [])
            if not bool(item.get("promotion_eligible"))
        ]
        worst_global_symbols = scenario_payloads.get(primary_scenario, {}).get("global", {}).get("worst_symbols", [])

        payload = {
            "run_id": run_id,
            "generated_at_utc": str(run.get("updated_at_utc") or ""),
            "primary_scenario": primary_scenario,
            "slippage_stress_pct_per_side": self._config.promotion_slippage_stress_pct_per_side,
            "scenarios": scenario_payloads,
            "primary_scenario_summary": {
                "eligible_symbols": eligible_symbols,
                "omitted_symbols": omitted_symbols,
                "worst_global_symbols": worst_global_symbols,
            },
        }
        json_path = run_dir / "coin_attribution.json"
        md_path = run_dir / "coin_attribution.md"
        write_json_atomic(json_path, payload)
        write_text_atomic(md_path, _markdown_report(payload))

        manifest_path = run_dir / "manifest.json"
        manifest_payload = {}
        if manifest_path.exists():
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest_payload = json.load(handle)
        manifest_payload["coin_attribution_files"] = {
            "json": "coin_attribution.json",
            "markdown": "coin_attribution.md",
        }
        manifest_payload["coin_attribution_summary"] = {
            "primary_scenario": primary_scenario,
            "eligible_symbol_count": len(eligible_symbols),
            "omitted_symbol_count": len(omitted_symbols),
            "worst_global_symbols": [item["symbol"] for item in worst_global_symbols],
        }
        write_json_atomic(manifest_path, manifest_payload)

        return AttributionSummary(
            run_id=run_id,
            primary_scenario=primary_scenario,
            artifact_dir=str(run_dir),
            eligible_symbol_count=len(eligible_symbols),
            omitted_symbol_count=len(omitted_symbols),
            attribution_json=str(json_path),
            attribution_markdown=str(md_path),
            status="attributed",
        )

    def _collect_symbol_metrics(
        self,
        *,
        predictions_root: Path,
        fee_pct_per_side: float,
        slippage_stress_pct_per_side: float,
    ) -> dict[str, dict[str, dict[str, dict[str, object]]]]:
        metrics: dict[str, dict[str, dict[str, list[dict[str, object]]]]] = {}
        for file_path in sorted(predictions_root.rglob("*.parquet")):
            frame = pd.read_parquet(file_path)
            if frame.empty:
                continue
            scenario = str(frame["scenario"].iloc[0])
            fold_index = int(frame["fold_index"].iloc[0])
            for model_scope, scope_frame in frame.groupby("model_scope", sort=True):
                scope_key = str(model_scope)
                for symbol, symbol_frame in scope_frame.groupby("symbol", sort=True):
                    split_payloads: dict[str, dict[str, object]] = {}
                    for split, split_frame in iter_splits(symbol_frame):
                        metric_payload = metric_row(
                            frame=split_frame,
                            fee_pct_per_side=fee_pct_per_side,
                        )
                        if metric_payload is None:
                            continue
                        metric_payload["fold_index"] = fold_index
                        split_payloads[split] = metric_payload
                    if not split_payloads:
                        continue
                    metrics.setdefault(scenario, {}).setdefault(scope_key, {}).setdefault(str(symbol), []).append(split_payloads)

        aggregated: dict[str, dict[str, dict[str, dict[str, object]]]] = {}
        for scenario, scenario_payload in metrics.items():
            aggregated[scenario] = {}
            for model_scope, symbol_payload in scenario_payload.items():
                aggregated[scenario][model_scope] = {}
                for symbol, fold_payloads in symbol_payload.items():
                    split_rows: dict[str, list[dict[str, object]]] = {}
                    for fold_payload in fold_payloads:
                        for split, metric_payload in fold_payload.items():
                            split_rows.setdefault(split, []).append(metric_payload)
                    split_summary: dict[str, dict[str, object]] = {}
                    for split, rows in split_rows.items():
                        split_summary[split] = _aggregate_metric_rows(
                            rows=rows,
                            slippage_stress_pct_per_side=slippage_stress_pct_per_side,
                        )
                    all_split = split_summary.get("all")
                    if all_split is None:
                        continue
                    synthetic_split = split_summary.get("synthetic")
                    ndax_split = split_summary.get("ndax")
                    synthetic_share = (
                        float(synthetic_split["row_count"]) / float(all_split["row_count"])
                        if synthetic_split is not None and float(all_split["row_count"]) > 0
                        else 0.0
                    )
                    bad_kind, reasons = _classify_bad_kind(
                        aggregate=all_split,
                        ndax_split=ndax_split,
                        synthetic_split=synthetic_split,
                        synthetic_share=synthetic_share,
                    )
                    promotion_eligible = (
                        bad_kind is None
                        and (
                            all_split.get("max_drawdown") is None
                            or float(all_split["max_drawdown"]) <= self._config.promotion_max_drawdown
                        )
                        and float(all_split["stressed_net_return"]) > 0.0
                        and float(all_split["net_return"]) > 0.0
                    )
                    if all_split.get("max_drawdown") is not None and float(all_split["max_drawdown"]) > self._config.promotion_max_drawdown:
                        reasons.append(
                            f"max_drawdown_gt_{self._config.promotion_max_drawdown:.2f}"
                        )
                    aggregated[scenario][model_scope][symbol] = {
                        "symbol": symbol,
                        "folds": int(all_split["folds"]),
                        "row_count": int(all_split["row_count"]),
                        "trades": int(all_split["trades"]),
                        "gross_return": float(all_split["gross_return"]),
                        "net_return": float(all_split["net_return"]),
                        "stressed_net_return": float(all_split["stressed_net_return"]),
                        "win_rate": all_split.get("win_rate"),
                        "max_drawdown": all_split.get("max_drawdown"),
                        "logloss": all_split.get("logloss"),
                        "roc_auc": all_split.get("roc_auc"),
                        "pr_auc": all_split.get("pr_auc"),
                        "brier": all_split.get("brier"),
                        "synthetic_share": synthetic_share,
                        "promotion_eligible": promotion_eligible if model_scope == "per_coin" else None,
                        "bad_kind": bad_kind if model_scope == "per_coin" else None,
                        "reasons": reasons,
                        "ndax_split": ndax_split,
                        "synthetic_split": synthetic_split,
                    }
        return aggregated

    def _build_scenario_payloads(
        self,
        *,
        symbol_metrics: dict[str, dict[str, dict[str, dict[str, object]]]],
    ) -> dict[str, dict[str, object]]:
        payload: dict[str, dict[str, object]] = {}
        for scenario, scenario_payload in sorted(symbol_metrics.items()):
            scope_payload: dict[str, object] = {}
            for model_scope, symbol_payload in sorted(scenario_payload.items()):
                symbols = sorted(symbol_payload.values(), key=lambda item: (str(item["symbol"])))
                if model_scope == "per_coin":
                    eligible = [item for item in symbols if bool(item.get("promotion_eligible"))]
                    omitted = [item for item in symbols if not bool(item.get("promotion_eligible"))]
                    scope_payload[model_scope] = {
                        "eligible_symbol_count": len(eligible),
                        "omitted_symbol_count": len(omitted),
                        "eligible_symbols": [item["symbol"] for item in eligible],
                        "omitted_symbols": [item["symbol"] for item in omitted],
                        "symbols": symbols,
                    }
                else:
                    worst = sorted(
                        symbols,
                        key=lambda item: (
                            float(item["stressed_net_return"]),
                            float(item["net_return"]),
                            str(item["symbol"]),
                        ),
                    )[:10]
                    scope_payload[model_scope] = {
                        "symbol_count": len(symbols),
                        "worst_symbols": worst,
                        "symbols": symbols,
                    }
            payload[scenario] = scope_payload
        return payload


def _aggregate_metric_rows(
    *,
    rows: list[dict[str, object]],
    slippage_stress_pct_per_side: float,
) -> dict[str, object]:
    frame = pd.DataFrame(rows)
    net_return = float(frame["net_return"].sum())
    trades = int(frame["trades"].sum())
    return {
        "folds": int(frame["fold_index"].nunique()),
        "row_count": int(frame["row_count"].sum()),
        "trades": trades,
        "gross_return": float(frame["gross_return"].sum()),
        "net_return": net_return,
        "stressed_net_return": stressed_net_return(
            net_return=net_return,
            trades=trades,
            slippage_stress_pct_per_side=slippage_stress_pct_per_side,
        ),
        "win_rate": _mean_nullable(frame["win_rate"]),
        "max_drawdown": _mean_nullable(frame["max_drawdown"]),
        "logloss": _mean_nullable(frame["logloss"]),
        "roc_auc": _mean_nullable(frame["roc_auc"]),
        "pr_auc": _mean_nullable(frame["pr_auc"]),
        "brier": _mean_nullable(frame["brier"]),
    }


def _classify_bad_kind(
    *,
    aggregate: dict[str, object],
    ndax_split: dict[str, object] | None,
    synthetic_split: dict[str, object] | None,
    synthetic_share: float,
) -> tuple[str | None, list[str]]:
    reasons: list[str] = []
    folds = int(aggregate["folds"])
    trades = int(aggregate["trades"])
    row_count = int(aggregate["row_count"])
    gross_return = float(aggregate["gross_return"])
    net_return = float(aggregate["net_return"])
    stressed = float(aggregate["stressed_net_return"])
    roc_auc = aggregate.get("roc_auc")

    if folds < 4 or trades < 100 or row_count < 5000:
        if folds < 4:
            reasons.append("folds_lt_4")
        if trades < 100:
            reasons.append("trades_lt_100")
        if row_count < 5000:
            reasons.append("row_count_lt_5000")
        return "sparse_history", reasons

    if (gross_return > 0.0 and net_return <= 0.0) or stressed <= 0.0:
        if gross_return > 0.0 and net_return <= 0.0:
            reasons.append("fees_erase_edge")
        if stressed <= 0.0:
            reasons.append("slippage_stress_non_positive")
        return "cost_fragility", reasons

    synthetic_net = float(synthetic_split["net_return"]) if synthetic_split is not None else None
    ndax_net = float(ndax_split["net_return"]) if ndax_split is not None else None
    if (
        (synthetic_share >= 0.70 and synthetic_net is not None and synthetic_net < 0.0)
        or (
            synthetic_net is not None
            and ndax_net is not None
            and synthetic_net <= (ndax_net - 1.0)
        )
    ):
        if synthetic_share >= 0.70 and synthetic_net is not None and synthetic_net < 0.0:
            reasons.append("synthetic_dominant_negative")
        if synthetic_net is not None and ndax_net is not None and synthetic_net <= (ndax_net - 1.0):
            reasons.append("synthetic_underperforms_ndax")
        return "synthetic_fragility", reasons

    if net_return < 0.0 or (roc_auc is not None and float(roc_auc) < 0.52):
        if net_return < 0.0:
            reasons.append("net_return_negative")
        if roc_auc is not None and float(roc_auc) < 0.52:
            reasons.append("roc_auc_lt_0.52")
        return "weak_signal", reasons

    return None, reasons


def _mean_nullable(series: pd.Series) -> float | None:
    values = [float(value) for value in series.dropna().tolist()]
    if not values:
        return None
    return float(sum(values) / len(values))


def _markdown_report(payload: dict[str, object]) -> str:
    primary_scenario = str(payload["primary_scenario"])
    scenarios = payload["scenarios"]
    primary = scenarios.get(primary_scenario, {}) if isinstance(scenarios, dict) else {}
    per_coin = primary.get("per_coin", {}) if isinstance(primary, dict) else {}
    global_scope = primary.get("global", {}) if isinstance(primary, dict) else {}

    lines = [
        f"# Coin Attribution Report",
        "",
        f"- Run: `{payload['run_id']}`",
        f"- Primary scenario: `{primary_scenario}`",
        f"- Slippage stress per side: `{payload['slippage_stress_pct_per_side']}`",
        "",
        "## Primary Scenario Per-Coin Eligibility",
        "",
        f"- Eligible symbols: `{per_coin.get('eligible_symbol_count', 0)}`",
        f"- Omitted symbols: `{per_coin.get('omitted_symbol_count', 0)}`",
        "",
        "| Symbol | Eligible | Bad Kind | Trades | Net Return | Stressed Net | Synthetic Share | Reasons |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for item in per_coin.get("symbols", []):
        lines.append(
            "| {symbol} | {eligible} | {bad_kind} | {trades} | {net_return:.4f} | {stressed_net_return:.4f} | {synthetic_share:.2f} | {reasons} |".format(
                symbol=item["symbol"],
                eligible="yes" if item.get("promotion_eligible") else "no",
                bad_kind=item.get("bad_kind") or "",
                trades=int(item["trades"]),
                net_return=float(item["net_return"]),
                stressed_net_return=float(item["stressed_net_return"]),
                synthetic_share=float(item["synthetic_share"]),
                reasons=", ".join(item.get("reasons", [])),
            )
        )

    lines.extend(
        [
            "",
            "## Primary Scenario Global Worst Symbols",
            "",
            "| Symbol | Trades | Net Return | Stressed Net | Synthetic Share |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in global_scope.get("worst_symbols", []):
        lines.append(
            "| {symbol} | {trades} | {net_return:.4f} | {stressed_net_return:.4f} | {synthetic_share:.2f} |".format(
                symbol=item["symbol"],
                trades=int(item["trades"]),
                net_return=float(item["net_return"]),
                stressed_net_return=float(item["stressed_net_return"]),
                synthetic_share=float(item["synthetic_share"]),
            )
        )
    lines.append("")
    return "\n".join(lines)
