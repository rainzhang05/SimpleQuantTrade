"""Portfolio-style research backtester over persisted validation predictions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from qtbot.config import RuntimeConfig
from qtbot.state import StateStore
from qtbot.training.artifacts import write_json_atomic, write_parquet_atomic


@dataclass(frozen=True)
class PortfolioBacktestSummary:
    run_id: str
    scenario: str
    model_scope: str
    entry_threshold: float
    initial_capital_cad: float
    final_equity_cad: float
    total_return_pct: float
    annualized_return_pct: float | None
    max_drawdown_pct: float
    trades_executed: int
    win_rate: float | None
    avg_net_pnl_cad: float | None
    avg_net_return_pct: float | None
    avg_holding_bars: float
    avg_holding_hours: float
    skipped_capacity: int
    skipped_symbol_open: int
    skipped_cash: int
    label_horizon_bars: int
    interval_seconds: int
    source_mix: dict[str, dict[str, float]]
    symbol_pnl: dict[str, float]
    monthly_returns_pct: dict[str, float]
    artifact_dir: str
    summary_file: str
    trades_file: str
    status: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _OpenPosition:
    symbol: str
    source: str
    entry_timestamp_ms: int
    exit_timestamp_ms: int
    probability: float
    forward_return: float
    notional_cad: float
    entry_cash_debit_cad: float


class PortfolioBacktestService:
    """Simulate cash-constrained horizon-hold portfolio trades from prediction artifacts."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._repo_root = config.runtime_dir.parent

    def backtest(
        self,
        *,
        run_id: str,
        scenario: str | None = None,
        model_scope: str = "global",
        entry_threshold: float | None = None,
        initial_capital_cad: float | None = None,
        max_active_positions: int | None = None,
        position_fraction: float | None = None,
        slippage_pct_per_side: float | None = None,
    ) -> PortfolioBacktestSummary:
        run = self._state_store.get_training_run(run_id=run_id)
        if run is None:
            raise ValueError(f"training run not found: {run_id}")
        if str(run["status"]) not in {"trained", "evaluated"}:
            raise ValueError(f"training run must be trained or evaluated before backtest: {run_id}")

        selected_scenario = self._resolve_scenario(run=run, requested_scenario=scenario)
        selected_model_scope = str(model_scope).strip().lower()
        if selected_model_scope not in {"global", "per_coin"}:
            raise ValueError("model_scope must be one of: global, per_coin")

        threshold = float(self._config.promotion_entry_threshold if entry_threshold is None else entry_threshold)
        if not (0.0 <= threshold <= 1.0):
            raise ValueError("entry_threshold must be in [0,1]")
        initial_capital = float(
            self._config.backtest_initial_capital_cad if initial_capital_cad is None else initial_capital_cad
        )
        if initial_capital <= 0.0:
            raise ValueError("initial_capital_cad must be > 0")
        max_positions = int(
            self._config.backtest_max_active_positions if max_active_positions is None else max_active_positions
        )
        if max_positions <= 0:
            raise ValueError("max_active_positions must be > 0")
        allocation_fraction = float(
            self._config.backtest_position_fraction if position_fraction is None else position_fraction
        )
        if not (0.0 < allocation_fraction <= 1.0):
            raise ValueError("position_fraction must be in (0,1]")
        slippage = float(
            self._config.backtest_slippage_pct_per_side if slippage_pct_per_side is None else slippage_pct_per_side
        )
        if not (0.0 <= slippage < 1.0):
            raise ValueError("slippage_pct_per_side must be in [0,1)")

        run_dir = Path(str(run["artifact_dir"]))
        predictions = self._load_predictions(run_dir=run_dir, scenario=selected_scenario, model_scope=selected_model_scope)
        snapshot_manifest = self._load_snapshot_manifest(snapshot_id=str(run["snapshot_id"]))
        interval_seconds = int(snapshot_manifest["interval_seconds"])
        label_horizon_bars = int(snapshot_manifest.get("label_horizon_bars", 1))
        if label_horizon_bars <= 0:
            raise ValueError("snapshot label_horizon_bars must be > 0")

        summary, trades = _simulate_portfolio(
            predictions=predictions,
            scenario=selected_scenario,
            model_scope=selected_model_scope,
            entry_threshold=threshold,
            initial_capital_cad=initial_capital,
            fee_pct_per_side=float(self._config.fee_pct_per_side),
            slippage_pct_per_side=slippage,
            max_active_positions=max_positions,
            position_fraction=allocation_fraction,
            label_horizon_bars=label_horizon_bars,
            interval_seconds=interval_seconds,
        )
        artifact_dir = run_dir / "backtests" / _backtest_id(
            scenario=selected_scenario,
            model_scope=selected_model_scope,
            entry_threshold=threshold,
            initial_capital_cad=initial_capital,
            max_active_positions=max_positions,
            position_fraction=allocation_fraction,
            slippage_pct_per_side=slippage,
        )
        summary_file = artifact_dir / "summary.json"
        trades_file = artifact_dir / "trades.parquet"
        write_json_atomic(summary_file, summary.to_payload())
        write_parquet_atomic(trades_file, trades)
        return PortfolioBacktestSummary(
            **{
                **summary.to_payload(),
                "artifact_dir": str(artifact_dir),
                "summary_file": str(summary_file),
                "trades_file": str(trades_file),
            }
        )

    def _resolve_scenario(self, *, run: dict[str, object], requested_scenario: str | None) -> str:
        if requested_scenario:
            return str(requested_scenario).strip().lower()
        promotion = self._state_store.get_promotion(run_id=str(run["run_id"]))
        if promotion is not None and str(promotion.get("decision")) == "accepted":
            return str(promotion["primary_scenario"])
        scenario = str(run.get("primary_scenario") or "")
        if not scenario:
            raise ValueError(f"training run missing primary scenario: {run['run_id']}")
        return scenario

    def _load_predictions(self, *, run_dir: Path, scenario: str, model_scope: str) -> pd.DataFrame:
        predictions_root = run_dir / "predictions"
        prediction_files = sorted(predictions_root.rglob(f"{scenario}.parquet"))
        if not prediction_files:
            raise ValueError(f"prediction artifacts missing for scenario={scenario}")
        frame = pd.concat([pd.read_parquet(item) for item in prediction_files], ignore_index=True)
        if frame.empty:
            raise ValueError(f"prediction artifacts empty for scenario={scenario}")
        filtered = frame.loc[frame["model_scope"] == model_scope].copy()
        if filtered.empty:
            raise ValueError(f"prediction artifacts missing for model_scope={model_scope} scenario={scenario}")
        filtered.sort_values(["timestamp_ms", "probability", "symbol"], ascending=[True, False, True], inplace=True, kind="mergesort")
        filtered.reset_index(drop=True, inplace=True)
        return filtered

    def _load_snapshot_manifest(self, *, snapshot_id: str) -> dict[str, object]:
        manifest_path = self._repo_root / "data" / "snapshots" / snapshot_id / "manifest.json"
        if not manifest_path.exists():
            raise ValueError(f"snapshot manifest missing: {snapshot_id}")
        with manifest_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


class _TradeSummaryBuilder:
    def __init__(self, *, initial_capital_cad: float, interval_seconds: int, label_horizon_bars: int) -> None:
        self.initial_capital_cad = initial_capital_cad
        self.interval_seconds = interval_seconds
        self.label_horizon_bars = label_horizon_bars
        self.equity_events: list[tuple[int, float]] = []
        self.trade_rows: list[dict[str, object]] = []
        self.skipped_capacity = 0
        self.skipped_symbol_open = 0
        self.skipped_cash = 0

    def record_equity(self, *, timestamp_ms: int, equity_cad: float) -> None:
        self.equity_events.append((int(timestamp_ms), float(equity_cad)))

    def build(
        self,
        *,
        run_id: str,
        scenario: str,
        model_scope: str,
        entry_threshold: float,
        cash_cad: float,
        source_mix: dict[str, dict[str, float]],
        symbol_pnl: dict[str, float],
    ) -> tuple[PortfolioBacktestSummary, pd.DataFrame]:
        final_equity = float(cash_cad)
        total_return_pct = ((final_equity / self.initial_capital_cad) - 1.0) * 100.0
        event_frame = pd.DataFrame(self.equity_events, columns=["timestamp_ms", "equity_cad"])
        event_frame.sort_values("timestamp_ms", inplace=True, kind="mergesort")
        event_frame = event_frame.drop_duplicates(subset=["timestamp_ms"], keep="last")
        event_frame.reset_index(drop=True, inplace=True)

        drawdown_pct = 0.0
        annualized_return_pct: float | None = None
        monthly_returns = {}
        if not event_frame.empty:
            equity = event_frame["equity_cad"].astype("float64")
            peaks = equity.cummax()
            drawdown_pct = float(((peaks - equity) / peaks.replace(0.0, np.nan)).fillna(0.0).max()) * 100.0
            first_ts = int(event_frame["timestamp_ms"].iloc[0])
            last_ts = int(event_frame["timestamp_ms"].iloc[-1])
            elapsed_days = max(0.0, (last_ts - first_ts) / 86_400_000)
            if elapsed_days >= 1.0 and final_equity > 0.0:
                annualized_return_pct = (((final_equity / self.initial_capital_cad) ** (365.0 / elapsed_days)) - 1.0) * 100.0
            monthly_returns = _monthly_returns(event_frame=event_frame, initial_capital_cad=self.initial_capital_cad)

        trades = pd.DataFrame(self.trade_rows)
        win_rate = None
        avg_net_pnl = None
        avg_net_return_pct = None
        avg_holding_bars = 0.0
        avg_holding_hours = 0.0
        if not trades.empty:
            win_rate = float((trades["net_pnl_cad"] > 0.0).mean())
            avg_net_pnl = float(trades["net_pnl_cad"].mean())
            avg_net_return_pct = float(trades["net_return_pct"].mean())
            avg_holding_bars = float(trades["holding_bars"].mean())
            avg_holding_hours = float(trades["holding_hours"].mean())

        summary = PortfolioBacktestSummary(
            run_id=run_id,
            scenario=scenario,
            model_scope=model_scope,
            entry_threshold=entry_threshold,
            initial_capital_cad=self.initial_capital_cad,
            final_equity_cad=final_equity,
            total_return_pct=total_return_pct,
            annualized_return_pct=annualized_return_pct,
            max_drawdown_pct=drawdown_pct,
            trades_executed=int(len(trades.index)),
            win_rate=win_rate,
            avg_net_pnl_cad=avg_net_pnl,
            avg_net_return_pct=avg_net_return_pct,
            avg_holding_bars=avg_holding_bars,
            avg_holding_hours=avg_holding_hours,
            skipped_capacity=self.skipped_capacity,
            skipped_symbol_open=self.skipped_symbol_open,
            skipped_cash=self.skipped_cash,
            label_horizon_bars=self.label_horizon_bars,
            interval_seconds=self.interval_seconds,
            source_mix=source_mix,
            symbol_pnl=symbol_pnl,
            monthly_returns_pct=monthly_returns,
            artifact_dir="",
            summary_file="",
            trades_file="",
            status="backtested",
        )
        return summary, trades


def _simulate_portfolio(
    *,
    predictions: pd.DataFrame,
    scenario: str,
    model_scope: str,
    entry_threshold: float,
    initial_capital_cad: float,
    fee_pct_per_side: float,
    slippage_pct_per_side: float,
    max_active_positions: int,
    position_fraction: float,
    label_horizon_bars: int,
    interval_seconds: int,
) -> tuple[PortfolioBacktestSummary, pd.DataFrame]:
    if predictions.empty:
        raise ValueError("predictions frame is empty")
    per_side_cost = float(fee_pct_per_side) + float(slippage_pct_per_side)
    horizon_ms = int(label_horizon_bars) * int(interval_seconds) * 1000
    summary_builder = _TradeSummaryBuilder(
        initial_capital_cad=float(initial_capital_cad),
        interval_seconds=int(interval_seconds),
        label_horizon_bars=int(label_horizon_bars),
    )

    cash = float(initial_capital_cad)
    open_positions: dict[str, _OpenPosition] = {}
    source_mix: dict[str, dict[str, float]] = {}
    symbol_pnl: dict[str, float] = {}

    first_timestamp = int(predictions["timestamp_ms"].iloc[0])
    summary_builder.record_equity(timestamp_ms=first_timestamp, equity_cad=cash)

    for timestamp_ms, group in predictions.groupby("timestamp_ms", sort=True):
        current_ts = int(timestamp_ms)
        for position in sorted(
            (item for item in open_positions.values() if item.exit_timestamp_ms <= current_ts),
            key=lambda item: (item.exit_timestamp_ms, item.symbol),
        ):
            cash += _close_position(
                summary_builder=summary_builder,
                position=position,
                per_side_cost=per_side_cost,
                symbol_pnl=symbol_pnl,
                source_mix=source_mix,
                interval_seconds=interval_seconds,
            )
            del open_positions[position.symbol]
        summary_builder.record_equity(
            timestamp_ms=current_ts,
            equity_cad=_portfolio_equity(cash_cad=cash, open_positions=open_positions),
        )

        candidates = group.loc[group["probability"].astype("float64") >= float(entry_threshold)].copy()
        if candidates.empty:
            continue
        candidates.sort_values(["probability", "symbol"], ascending=[False, True], inplace=True, kind="mergesort")
        for _, candidate in candidates.iterrows():
            symbol = str(candidate["symbol"])
            if symbol in open_positions:
                summary_builder.skipped_symbol_open += 1
                continue
            if len(open_positions) >= int(max_active_positions):
                summary_builder.skipped_capacity += 1
                continue
            equity_now = _portfolio_equity(cash_cad=cash, open_positions=open_positions)
            desired_notional = min(
                cash / (1.0 + per_side_cost),
                equity_now * float(position_fraction),
            )
            if desired_notional <= 0.0:
                summary_builder.skipped_cash += 1
                continue
            entry_debit = desired_notional * (1.0 + per_side_cost)
            if entry_debit > cash + 1e-9:
                summary_builder.skipped_cash += 1
                continue
            probability = float(candidate["probability"])
            forward_return = float(candidate["forward_return"])
            source = str(candidate["source"])
            cash -= entry_debit
            open_positions[symbol] = _OpenPosition(
                symbol=symbol,
                source=source,
                entry_timestamp_ms=current_ts,
                exit_timestamp_ms=current_ts + horizon_ms,
                probability=probability,
                forward_return=forward_return,
                notional_cad=desired_notional,
                entry_cash_debit_cad=entry_debit,
            )
        summary_builder.record_equity(
            timestamp_ms=current_ts,
            equity_cad=_portfolio_equity(cash_cad=cash, open_positions=open_positions),
        )

    for position in sorted(open_positions.values(), key=lambda item: (item.exit_timestamp_ms, item.symbol)):
        cash += _close_position(
            summary_builder=summary_builder,
            position=position,
            per_side_cost=per_side_cost,
            symbol_pnl=symbol_pnl,
            source_mix=source_mix,
            interval_seconds=interval_seconds,
        )
        summary_builder.record_equity(
            timestamp_ms=position.exit_timestamp_ms,
            equity_cad=cash,
        )

    return summary_builder.build(
        run_id=str(predictions["run_id"].iloc[0]),
        scenario=scenario,
        model_scope=model_scope,
        entry_threshold=entry_threshold,
        cash_cad=cash,
        source_mix=source_mix,
        symbol_pnl={key: float(value) for key, value in sorted(symbol_pnl.items())},
    )


def _close_position(
    *,
    summary_builder: _TradeSummaryBuilder,
    position: _OpenPosition,
    per_side_cost: float,
    symbol_pnl: dict[str, float],
    source_mix: dict[str, dict[str, float]],
    interval_seconds: int,
) -> float:
    exit_credit = position.notional_cad * (1.0 + position.forward_return) * (1.0 - per_side_cost)
    gross_pnl_cad = position.notional_cad * position.forward_return
    net_pnl_cad = exit_credit - position.entry_cash_debit_cad
    net_return_pct = ((exit_credit / position.entry_cash_debit_cad) - 1.0) * 100.0
    summary_builder.trade_rows.append(
        {
            "symbol": position.symbol,
            "source": position.source,
            "entry_timestamp_ms": position.entry_timestamp_ms,
            "exit_timestamp_ms": position.exit_timestamp_ms,
            "holding_bars": int((position.exit_timestamp_ms - position.entry_timestamp_ms) // (interval_seconds * 1000)),
            "holding_hours": ((position.exit_timestamp_ms - position.entry_timestamp_ms) / 3_600_000.0),
            "probability": position.probability,
            "forward_return": position.forward_return,
            "notional_cad": position.notional_cad,
            "entry_cash_debit_cad": position.entry_cash_debit_cad,
            "exit_cash_credit_cad": exit_credit,
            "gross_pnl_cad": gross_pnl_cad,
            "net_pnl_cad": net_pnl_cad,
            "net_return_pct": net_return_pct,
        }
    )
    symbol_pnl[position.symbol] = symbol_pnl.get(position.symbol, 0.0) + float(net_pnl_cad)
    bucket = source_mix.setdefault(position.source, {"trades": 0.0, "net_pnl_cad": 0.0, "gross_pnl_cad": 0.0})
    bucket["trades"] += 1.0
    bucket["gross_pnl_cad"] += float(gross_pnl_cad)
    bucket["net_pnl_cad"] += float(net_pnl_cad)
    return exit_credit


def _portfolio_equity(*, cash_cad: float, open_positions: dict[str, _OpenPosition]) -> float:
    return float(cash_cad) + sum(position.notional_cad for position in open_positions.values())


def _monthly_returns(*, event_frame: pd.DataFrame, initial_capital_cad: float) -> dict[str, float]:
    if event_frame.empty:
        return {}
    frame = event_frame.copy()
    frame["dt"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True)
    frame["month"] = frame["dt"].dt.strftime("%Y-%m")
    by_month = frame.groupby("month", sort=True).tail(1)
    result: dict[str, float] = {}
    previous_equity = float(initial_capital_cad)
    for _, row in by_month.iterrows():
        equity = float(row["equity_cad"])
        if previous_equity <= 0.0:
            result[str(row["month"])] = 0.0
        else:
            result[str(row["month"])] = ((equity / previous_equity) - 1.0) * 100.0
        previous_equity = equity
    return result


def _backtest_id(
    *,
    scenario: str,
    model_scope: str,
    entry_threshold: float,
    initial_capital_cad: float,
    max_active_positions: int,
    position_fraction: float,
    slippage_pct_per_side: float,
) -> str:
    threshold_tag = f"thr{int(round(entry_threshold * 1000)):03d}"
    capital_tag = f"cap{int(round(initial_capital_cad))}"
    positions_tag = f"pos{int(max_active_positions)}"
    allocation_tag = f"alloc{int(round(position_fraction * 1000)):03d}"
    slippage_tag = f"slip{int(round(slippage_pct_per_side * 1_000_000)):06d}"
    return f"{scenario}_{model_scope}_{threshold_tag}_{capital_tag}_{positions_tag}_{allocation_tag}_{slippage_tag}"
