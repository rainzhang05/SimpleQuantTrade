"""Phase 7 promotion gates and bundle publishing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any

from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control
from qtbot.model_bundle import (
    bundle_dir,
    bundle_root,
    read_active_bundle_id,
    validate_bundle_signature,
    write_active_bundle_id_atomic,
    write_bundle_signature,
)
from qtbot.runner import is_pid_alive, read_runner_pid
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic
from qtbot.training.attribution import AttributionService
from qtbot.training.feature_builder import FeatureBuilder
from qtbot.training.feature_spec import feature_spec_payload
from qtbot.training.modeling import (
    build_model_params,
    ensure_binary_labels,
    final_per_coin_fit_skip_reason,
    fit_model,
    rows_for_scenario,
)


@dataclass(frozen=True)
class PromotionSummary:
    run_id: str
    bundle_id: str | None
    decision: str
    primary_scenario: str
    hard_failures: list[object]
    soft_warnings: list[object]
    omitted_symbols: list[str]
    bundle_dir: str | None
    signature_ok: bool
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ModelStatusSummary:
    bundle_id: str | None
    bundle_dir: str | None
    active_pointer: str
    integrity_status: str
    signature_ok: bool
    omitted_symbols_count: int
    primary_scenario: str | None
    run_id: str | None
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


class PromotionService:
    """Promote evaluated training runs into signed deployable bundles."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._repo_root = config.runtime_dir.parent
        self._attribution = AttributionService(config=config, state_store=state_store)

    def promote(self, *, run_id: str) -> PromotionSummary:
        run = self._state_store.get_training_run(run_id=run_id)
        if run is None:
            raise ValueError(f"training run not found: {run_id}")
        if str(run["status"]) != "evaluated":
            raise ValueError(f"training run must be evaluated before promotion: {run_id}")

        prior = self._state_store.get_promotion(run_id=run_id)
        primary_scenario = str(run.get("primary_scenario") or "")
        if not primary_scenario:
            raise ValueError(f"training run missing primary scenario: {run_id}")

        bundle_id = _bundle_id(run_id=run_id, primary_scenario=primary_scenario)
        final_bundle_dir = bundle_dir(repo_root=self._repo_root, bundle_id=bundle_id)
        if prior is not None and str(prior["decision"]) == "accepted" and final_bundle_dir.exists():
            signature_ok, _, _ = validate_bundle_signature(bundle_path=final_bundle_dir)
            if signature_ok:
                write_active_bundle_id_atomic(repo_root=self._repo_root, bundle_id=bundle_id)
                return PromotionSummary(
                    run_id=run_id,
                    bundle_id=bundle_id,
                    decision="accepted",
                    primary_scenario=primary_scenario,
                    hard_failures=list(prior["hard_failures"]),
                    soft_warnings=list(prior["soft_warnings"]),
                    omitted_symbols=list(prior["omitted_symbols"]),
                    bundle_dir=str(final_bundle_dir),
                    signature_ok=True,
                    status="promoted",
                )
        if prior is not None and str(prior["decision"]) == "rejected":
            return PromotionSummary(
                run_id=run_id,
                bundle_id=None,
                decision="rejected",
                primary_scenario=primary_scenario,
                hard_failures=list(prior["hard_failures"]),
                soft_warnings=list(prior["soft_warnings"]),
                omitted_symbols=list(prior["omitted_symbols"]),
                bundle_dir=None,
                signature_ok=False,
                status="rejected",
            )

        attribution_summary = self._attribution.generate(run_id=run_id)
        attribution_payload = json.loads(Path(attribution_summary.attribution_json).read_text(encoding="utf-8"))
        primary_payload = attribution_payload["scenarios"].get(primary_scenario, {})
        per_coin_payload = primary_payload.get("per_coin", {})
        global_worst = primary_payload.get("global", {}).get("worst_symbols", [])
        soft_warnings: list[object] = []
        omitted_symbols = [str(item) for item in per_coin_payload.get("omitted_symbols", [])]
        if omitted_symbols:
            soft_warnings.append(
                {
                    "type": "per_coin_omitted",
                    "count": len(omitted_symbols),
                    "symbols": omitted_symbols,
                }
            )
        if global_worst:
            soft_warnings.append(
                {
                    "type": "global_worst_symbols",
                    "symbols": [item["symbol"] for item in global_worst[:5]],
                }
            )

        hard_failures = self._hard_failures(run=run, primary_scenario=primary_scenario)
        if hard_failures:
            self._state_store.upsert_promotion(
                run_id=run_id,
                bundle_id=None,
                decision="rejected",
                primary_scenario=primary_scenario,
                hard_failures=hard_failures,
                soft_warnings=soft_warnings,
                omitted_symbols=omitted_symbols,
                bundle_dir=None,
                signature_ok=False,
            )
            return PromotionSummary(
                run_id=run_id,
                bundle_id=None,
                decision="rejected",
                primary_scenario=primary_scenario,
                hard_failures=hard_failures,
                soft_warnings=soft_warnings,
                omitted_symbols=omitted_symbols,
                bundle_dir=None,
                signature_ok=False,
                status="rejected",
            )

        eligible_symbols = [str(item) for item in per_coin_payload.get("eligible_symbols", [])]
        written_bundle_dir, final_omitted_symbols = self._publish_bundle(
            run=run,
            bundle_id=bundle_id,
            primary_scenario=primary_scenario,
            eligible_symbols=eligible_symbols,
            omitted_symbols=omitted_symbols,
        )
        signature_ok, _, _ = validate_bundle_signature(bundle_path=written_bundle_dir)
        write_active_bundle_id_atomic(repo_root=self._repo_root, bundle_id=bundle_id)
        self._state_store.upsert_promotion(
            run_id=run_id,
            bundle_id=bundle_id,
            decision="accepted",
            primary_scenario=primary_scenario,
            hard_failures=[],
            soft_warnings=soft_warnings,
            omitted_symbols=final_omitted_symbols,
            bundle_dir=str(written_bundle_dir),
            signature_ok=signature_ok,
        )
        return PromotionSummary(
            run_id=run_id,
            bundle_id=bundle_id,
            decision="accepted",
            primary_scenario=primary_scenario,
            hard_failures=[],
            soft_warnings=soft_warnings,
            omitted_symbols=final_omitted_symbols,
            bundle_dir=str(written_bundle_dir),
            signature_ok=signature_ok,
            status="promoted",
        )

    def model_status(self) -> ModelStatusSummary:
        active_pointer = str(self._repo_root / "models" / "bundles" / "LATEST")
        active_bundle_id = read_active_bundle_id(repo_root=self._repo_root)
        if active_bundle_id is None:
            return ModelStatusSummary(
                bundle_id=None,
                bundle_dir=None,
                active_pointer=active_pointer,
                integrity_status="missing",
                signature_ok=False,
                omitted_symbols_count=0,
                primary_scenario=None,
                run_id=None,
                status="no_active_bundle",
            )

        path = bundle_dir(repo_root=self._repo_root, bundle_id=active_bundle_id)
        manifest_path = path / "manifest.json"
        if not path.exists() or not manifest_path.exists():
            return ModelStatusSummary(
                bundle_id=active_bundle_id,
                bundle_dir=str(path),
                active_pointer=active_pointer,
                integrity_status="invalid",
                signature_ok=False,
                omitted_symbols_count=0,
                primary_scenario=None,
                run_id=None,
                status="invalid",
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        signature_ok, _, _ = validate_bundle_signature(bundle_path=path)
        integrity_status = "ok" if signature_ok else "invalid"
        return ModelStatusSummary(
            bundle_id=active_bundle_id,
            bundle_dir=str(path),
            active_pointer=active_pointer,
            integrity_status=integrity_status,
            signature_ok=signature_ok,
            omitted_symbols_count=len(manifest.get("omitted_symbols", [])),
            primary_scenario=manifest.get("primary_scenario"),
            run_id=manifest.get("run_id"),
            status="active" if signature_ok else "invalid",
        )

    def set_active_bundle(self, *, bundle_id: str) -> ModelStatusSummary:
        control = read_control(self._config.control_file)
        pid = read_runner_pid(self._config.pid_file)
        runner_alive = is_pid_alive(pid) if pid is not None else False
        if runner_alive and control.command == Command.RUN:
            raise ValueError("active bundle switch requires the bot to be paused or stopped")

        path = bundle_dir(repo_root=self._repo_root, bundle_id=bundle_id)
        if not path.exists():
            raise ValueError(f"bundle not found: {bundle_id}")
        signature_ok, _, _ = validate_bundle_signature(bundle_path=path)
        if not signature_ok:
            raise ValueError(f"bundle signature invalid: {bundle_id}")

        write_active_bundle_id_atomic(repo_root=self._repo_root, bundle_id=bundle_id)
        return self.model_status()

    def _hard_failures(self, *, run: dict[str, object], primary_scenario: str) -> list[dict[str, object]]:
        metrics_summary = run.get("metrics_summary", {})
        scenario_metrics = metrics_summary.get(primary_scenario, {}) if isinstance(metrics_summary, dict) else {}
        global_metrics = scenario_metrics.get("global", {}) if isinstance(scenario_metrics, dict) else {}
        if not global_metrics:
            return [{"gate": "global_metrics_missing", "message": "primary scenario global metrics missing"}]

        failures: list[dict[str, object]] = []
        folds = int(global_metrics.get("folds") or 0)
        trades = int(global_metrics.get("trades") or 0)
        net_return = float(global_metrics.get("net_return") or 0.0)
        max_drawdown = global_metrics.get("max_drawdown")
        stressed = net_return - (2.0 * trades * self._config.promotion_slippage_stress_pct_per_side)
        if folds < self._config.promotion_min_folds:
            failures.append({"gate": "min_folds", "observed": folds, "required": self._config.promotion_min_folds})
        if trades < self._config.promotion_min_trades:
            failures.append({"gate": "min_trades", "observed": trades, "required": self._config.promotion_min_trades})
        if net_return <= 0.0:
            failures.append({"gate": "net_positive", "observed": net_return, "required": "> 0"})
        if max_drawdown is None or float(max_drawdown) > self._config.promotion_max_drawdown:
            failures.append(
                {
                    "gate": "max_drawdown",
                    "observed": max_drawdown,
                    "required": self._config.promotion_max_drawdown,
                }
            )
        if stressed <= 0.0:
            failures.append({"gate": "slippage_stress_positive", "observed": stressed, "required": "> 0"})

        snapshot_manifest = self._load_snapshot_manifest(snapshot_id=str(run["snapshot_id"]))
        if not bool(snapshot_manifest.get("parity_check_passed")):
            failures.append({"gate": "snapshot_parity", "observed": False, "required": True})
        for symbol_payload in snapshot_manifest.get("symbols", []):
            if int(symbol_payload.get("gap_count", 0)) > self._config.combined_max_gap_count:
                failures.append(
                    {
                        "gate": "combined_gap_count",
                        "symbol": symbol_payload.get("symbol"),
                        "observed": int(symbol_payload.get("gap_count", 0)),
                        "required": self._config.combined_max_gap_count,
                    }
                )
                break
            if float(symbol_payload.get("coverage_pct", 0.0)) < self._config.combined_min_coverage:
                failures.append(
                    {
                        "gate": "combined_coverage",
                        "symbol": symbol_payload.get("symbol"),
                        "observed": float(symbol_payload.get("coverage_pct", 0.0)),
                        "required": self._config.combined_min_coverage,
                    }
                )
                break

        conversion_quality = self._state_store.get_conversion_quality(timeframe=str(run["timeframe"]))
        if not conversion_quality:
            failures.append({"gate": "conversion_pass_rate", "observed": None, "required": self._config.promotion_min_conversion_pass_rate})
        else:
            pass_rate = sum(1 for row in conversion_quality if bool(row["quality_pass"])) / len(conversion_quality)
            if pass_rate < self._config.promotion_min_conversion_pass_rate:
                failures.append(
                    {
                        "gate": "conversion_pass_rate",
                        "observed": pass_rate,
                        "required": self._config.promotion_min_conversion_pass_rate,
                    }
                )
        return failures

    def _publish_bundle(
        self,
        *,
        run: dict[str, object],
        bundle_id: str,
        primary_scenario: str,
        eligible_symbols: list[str],
        omitted_symbols: list[str],
    ) -> tuple[Path, list[str]]:
        feature_builder = FeatureBuilder(repo_root=self._repo_root)
        feature_result = feature_builder.build(snapshot_id=str(run["snapshot_id"]))
        rows = rows_for_scenario(rows=feature_result.data, scenario=primary_scenario)
        ensure_binary_labels(rows=rows, context=f"bundle_refit scenario={primary_scenario}")

        bundles_root = bundle_root(repo_root=self._repo_root)
        bundles_root.mkdir(parents=True, exist_ok=True)
        final_bundle = bundle_dir(repo_root=self._repo_root, bundle_id=bundle_id)
        if final_bundle.exists():
            signature_ok, _, _ = validate_bundle_signature(bundle_path=final_bundle)
            if signature_ok:
                manifest = json.loads((final_bundle / "manifest.json").read_text(encoding="utf-8"))
                return final_bundle, [str(item) for item in manifest.get("omitted_symbols", [])]
            raise ValueError(f"existing bundle directory is invalid: {final_bundle}")

        temp_bundle = bundles_root / f".tmp.{bundle_id}.{os.getpid()}"
        if temp_bundle.exists():
            shutil.rmtree(temp_bundle)
        temp_bundle.mkdir(parents=True, exist_ok=False)

        sample_weight_column = None if primary_scenario == "ndax_only" else "supervised_row_weight"
        global_model = fit_model(
            rows=rows,
            sample_weight_column=sample_weight_column,
            seed=self._config.train_seed,
        )
        global_model.booster_.save_model(str(temp_bundle / "global_model.txt"))

        per_coin_dir = temp_bundle / "per_coin"
        per_coin_dir.mkdir(parents=True, exist_ok=True)
        final_omitted = sorted({item.strip().upper() for item in omitted_symbols})
        included_symbols: list[str] = []
        eligible_set = {item.strip().upper() for item in eligible_symbols}
        for symbol in sorted(eligible_set):
            symbol_rows = rows.loc[rows["symbol"] == symbol].reset_index(drop=True)
            skip_reason = final_per_coin_fit_skip_reason(rows=symbol_rows)
            if skip_reason is not None:
                final_omitted.append(symbol)
                continue
            model = fit_model(
                rows=symbol_rows,
                sample_weight_column=sample_weight_column,
                seed=self._config.train_seed,
            )
            symbol_path = per_coin_dir / f"{symbol}.txt"
            model.booster_.save_model(str(symbol_path))
            included_symbols.append(symbol)

        write_json_atomic(temp_bundle / "feature_spec.json", feature_spec_payload())
        write_json_atomic(
            temp_bundle / "thresholds.json",
            {
                "entry_threshold": self._config.promotion_entry_threshold,
                "exit_threshold": self._config.promotion_exit_threshold,
                "primary_scenario": primary_scenario,
            },
        )
        write_json_atomic(
            temp_bundle / "cost_model.json",
            {
                "fee_pct_per_side": self._config.fee_pct_per_side,
                "slippage_stress_pct_per_side": self._config.promotion_slippage_stress_pct_per_side,
                "label_threshold_return": 2.0 * self._config.fee_pct_per_side,
            },
        )
        manifest_payload = {
            "bundle_id": bundle_id,
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "code_version": _code_version(repo_root=self._repo_root),
            "config_hash": _config_hash(self._config),
            "dataset_hash": str(run["dataset_hash"]),
            "feature_spec_hash": str(run["feature_spec_hash"]),
            "training_window": {
                "snapshot_id": str(run["snapshot_id"]),
                "timeframe": str(run["timeframe"]),
            },
            "walk_forward": {
                "folds_requested": int(run["folds_requested"]),
                "folds_built": int(run["folds_built"]),
                "train_window_months": int(run["train_window_months"]),
                "valid_window_months": int(run["valid_window_months"]),
                "train_step_months": int(run["train_step_months"]),
            },
            "lgbm_params": build_model_params(seed=self._config.train_seed),
            "metrics_summary": {
                primary_scenario: run.get("metrics_summary", {}).get(primary_scenario, {}),
            },
            "run_id": str(run["run_id"]),
            "primary_scenario": primary_scenario,
            "included_per_coin_symbols": included_symbols,
            "omitted_symbols": sorted(set(final_omitted)),
        }
        write_json_atomic(temp_bundle / "manifest.json", manifest_payload)
        write_bundle_signature(bundle_path=temp_bundle)
        temp_bundle.rename(final_bundle)
        return final_bundle, sorted(set(final_omitted))

    def _load_snapshot_manifest(self, *, snapshot_id: str) -> dict[str, object]:
        path = self._repo_root / "data" / "snapshots" / snapshot_id / "manifest.json"
        if not path.exists():
            raise ValueError(f"snapshot manifest not found: {snapshot_id}")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


def _bundle_id(*, run_id: str, primary_scenario: str) -> str:
    return f"{run_id}_{primary_scenario}"


def _config_hash(config: RuntimeConfig) -> str:
    payload: dict[str, Any] = {
        "train_seed": config.train_seed,
        "train_window_months": config.train_window_months,
        "valid_window_months": config.valid_window_months,
        "train_step_months": config.train_step_months,
        "fee_pct_per_side": config.fee_pct_per_side,
        "promotion_min_folds": config.promotion_min_folds,
        "promotion_min_trades": config.promotion_min_trades,
        "promotion_max_drawdown": config.promotion_max_drawdown,
        "promotion_min_conversion_pass_rate": config.promotion_min_conversion_pass_rate,
        "promotion_slippage_stress_pct_per_side": config.promotion_slippage_stress_pct_per_side,
        "promotion_entry_threshold": config.promotion_entry_threshold,
        "promotion_exit_threshold": config.promotion_exit_threshold,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _code_version(*, repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            check=True,
            text=True,
        )
    except Exception:
        return "unknown"
    return proc.stdout.strip() or "unknown"
