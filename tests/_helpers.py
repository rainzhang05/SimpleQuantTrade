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
        taker_fee_rate=0.004,
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
    )
    if overrides:
        return replace(cfg, **overrides)
    return cfg
