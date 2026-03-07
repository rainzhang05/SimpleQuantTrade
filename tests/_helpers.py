"""Test helpers for runtime config and temporary state."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from qtbot.config import RuntimeConfig


def make_runtime_config(tmp_path: Path, **overrides) -> RuntimeConfig:
    runtime_dir = tmp_path / "runtime"
    cfg = RuntimeConfig(
        cadence_seconds=60,
        runtime_dir=runtime_dir,
        control_file=runtime_dir / "control.json",
        state_db=runtime_dir / "state.sqlite",
        log_file=runtime_dir / "logs" / "qtbot.log",
        pid_file=runtime_dir / "runner.pid",
        ndax_base_url="https://api.ndax.io/AP",
        ndax_oms_id=1,
        ndax_timeout_seconds=15.0,
        ndax_max_retries=2,
        signal_interval_seconds=60,
        ema_fast_period=60,
        ema_slow_period=360,
        atr_period=60,
        stop_k=2.5,
        max_hold_hours=48,
        cooldown_minutes=30,
        max_new_entries_per_cycle=3,
        enable_live_trading=False,
        taker_fee_rate=0.002,
        min_order_notional_cad=25.0,
        order_status_poll_seconds=0.01,
        order_status_max_attempts=3,
        preflight_min_warmup_coverage=0.8,
        daily_loss_cap_cad=250.0,
        max_slippage_pct=0.02,
        consecutive_error_limit=3,
        discord_webhook_url=None,
        discord_timeout_seconds=8.0,
        discord_max_retries=2,
        data_sources=("ndax", "kraken", "binance"),
        dataset_mode="combined",
        binance_base_url="https://api.binance.com",
        binance_quote="USDT",
        kraken_base_url="https://api.kraken.com",
        kraken_archive_dir=tmp_path / "data" / "kraken",
        external_source_priority=("kraken", "binance"),
        bridge_fx_symbol="USDTCAD",
        synth_weight_min=0.2,
        synth_weight_max=0.8,
        synth_weight_refresh="monthly",
        synth_weight_default=0.6,
        min_overlap_rows_for_weight=1000,
        conversion_max_median_ape=0.015,
        combined_max_gap_count=0,
        combined_min_coverage=0.999,
        label_horizon_bars=1,
        train_seed=42,
        train_window_months=12,
        valid_window_months=1,
        train_step_months=1,
        fee_pct_per_side=0.002,
        backtest_initial_capital_cad=10_000.0,
        backtest_max_active_positions=3,
        backtest_position_fraction=0.25,
        backtest_slippage_pct_per_side=0.0,
        promotion_min_folds=12,
        promotion_min_trades=200,
        promotion_max_drawdown=0.25,
        promotion_min_conversion_pass_rate=0.60,
        promotion_slippage_stress_pct_per_side=0.001,
        promotion_entry_threshold=0.60,
        promotion_exit_threshold=0.48,
    )
    if overrides:
        return replace(cfg, **overrides)
    return cfg
