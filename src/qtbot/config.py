"""Runtime configuration for qtbot."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from qtbot.env import load_dotenv


def _resolve_runtime_dir(raw_value: str) -> Path:
    runtime_dir = Path(raw_value).expanduser()
    if runtime_dir.is_absolute():
        return runtime_dir
    return (Path.cwd() / runtime_dir).resolve()


@dataclass(frozen=True)
class RuntimeConfig:
    cadence_seconds: int
    runtime_dir: Path
    control_file: Path
    state_db: Path
    log_file: Path
    pid_file: Path
    ndax_base_url: str
    ndax_oms_id: int
    ndax_timeout_seconds: float
    ndax_max_retries: int
    signal_interval_seconds: int
    ema_fast_period: int
    ema_slow_period: int
    atr_period: int
    stop_k: float
    max_hold_hours: int
    cooldown_minutes: int
    max_new_entries_per_cycle: int
    enable_live_trading: bool
    taker_fee_rate: float
    min_order_notional_cad: float
    order_status_poll_seconds: float
    order_status_max_attempts: int
    preflight_min_warmup_coverage: float
    daily_loss_cap_cad: float
    max_slippage_pct: float
    consecutive_error_limit: int


def load_runtime_config() -> RuntimeConfig:
    load_dotenv(Path.cwd() / ".env")

    cadence_raw = os.getenv("QTBOT_CADENCE_SECONDS", "60")
    cadence_seconds = int(cadence_raw)
    if cadence_seconds <= 0:
        raise ValueError("QTBOT_CADENCE_SECONDS must be > 0.")

    ndax_base_url = os.getenv("NDAX_BASE_URL", "https://api.ndax.io/AP").strip().rstrip("/")
    if not ndax_base_url:
        raise ValueError("NDAX_BASE_URL cannot be empty.")

    ndax_oms_id = int(os.getenv("NDAX_OMS_ID", "1"))
    if ndax_oms_id <= 0:
        raise ValueError("NDAX_OMS_ID must be > 0.")

    ndax_timeout_seconds = float(os.getenv("NDAX_TIMEOUT_SECONDS", "15"))
    if ndax_timeout_seconds <= 0:
        raise ValueError("NDAX_TIMEOUT_SECONDS must be > 0.")

    ndax_max_retries = int(os.getenv("NDAX_MAX_RETRIES", "3"))
    if ndax_max_retries < 0:
        raise ValueError("NDAX_MAX_RETRIES must be >= 0.")

    signal_interval_seconds = int(os.getenv("QTBOT_SIGNAL_INTERVAL_SECONDS", "60"))
    if signal_interval_seconds <= 0:
        raise ValueError("QTBOT_SIGNAL_INTERVAL_SECONDS must be > 0.")

    ema_fast_period = int(os.getenv("QTBOT_EMA_FAST", "60"))
    ema_slow_period = int(os.getenv("QTBOT_EMA_SLOW", "360"))
    atr_period = int(os.getenv("QTBOT_ATR_PERIOD", "60"))
    if min(ema_fast_period, ema_slow_period, atr_period) <= 0:
        raise ValueError("Indicator periods must be > 0.")

    stop_k = float(os.getenv("QTBOT_STOP_K", "2.5"))
    if stop_k <= 0:
        raise ValueError("QTBOT_STOP_K must be > 0.")

    max_hold_hours = int(os.getenv("QTBOT_MAX_HOLD_HOURS", "48"))
    cooldown_minutes = int(os.getenv("QTBOT_COOLDOWN_MINUTES", "30"))
    max_new_entries_per_cycle = int(os.getenv("QTBOT_MAX_NEW_ENTRIES_PER_CYCLE", "3"))
    if max_hold_hours <= 0 or cooldown_minutes < 0 or max_new_entries_per_cycle <= 0:
        raise ValueError("Hold/cooldown/entry limits are invalid.")

    enable_live_trading = _parse_bool(os.getenv("QTBOT_ENABLE_LIVE_TRADING", "false"))

    taker_fee_rate = float(os.getenv("QTBOT_TAKER_FEE_RATE", "0.004"))
    if taker_fee_rate < 0:
        raise ValueError("QTBOT_TAKER_FEE_RATE must be >= 0.")

    min_order_notional_cad = float(os.getenv("QTBOT_MIN_ORDER_NOTIONAL_CAD", "25"))
    if min_order_notional_cad <= 0:
        raise ValueError("QTBOT_MIN_ORDER_NOTIONAL_CAD must be > 0.")

    order_status_poll_seconds = float(os.getenv("QTBOT_ORDER_STATUS_POLL_SECONDS", "2"))
    if order_status_poll_seconds <= 0:
        raise ValueError("QTBOT_ORDER_STATUS_POLL_SECONDS must be > 0.")

    order_status_max_attempts = int(os.getenv("QTBOT_ORDER_STATUS_MAX_ATTEMPTS", "15"))
    if order_status_max_attempts <= 0:
        raise ValueError("QTBOT_ORDER_STATUS_MAX_ATTEMPTS must be > 0.")

    preflight_min_warmup_coverage = float(os.getenv("QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE", "0.8"))
    if not (0 < preflight_min_warmup_coverage <= 1):
        raise ValueError("QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE must be in (0, 1].")

    daily_loss_cap_cad = float(os.getenv("QTBOT_DAILY_LOSS_CAP_CAD", "250"))
    if daily_loss_cap_cad <= 0:
        raise ValueError("QTBOT_DAILY_LOSS_CAP_CAD must be > 0.")

    max_slippage_pct = float(os.getenv("QTBOT_MAX_SLIPPAGE_PCT", "0.02"))
    if not (0 < max_slippage_pct < 1):
        raise ValueError("QTBOT_MAX_SLIPPAGE_PCT must be in (0, 1).")

    consecutive_error_limit = int(os.getenv("QTBOT_CONSECUTIVE_ERROR_LIMIT", "3"))
    if consecutive_error_limit <= 0:
        raise ValueError("QTBOT_CONSECUTIVE_ERROR_LIMIT must be > 0.")

    runtime_dir = _resolve_runtime_dir(os.getenv("QTBOT_RUNTIME_DIR", "runtime"))
    return RuntimeConfig(
        cadence_seconds=cadence_seconds,
        runtime_dir=runtime_dir,
        control_file=runtime_dir / "control.json",
        state_db=runtime_dir / "state.sqlite",
        log_file=runtime_dir / "logs" / "qtbot.log",
        pid_file=runtime_dir / "runner.pid",
        ndax_base_url=ndax_base_url,
        ndax_oms_id=ndax_oms_id,
        ndax_timeout_seconds=ndax_timeout_seconds,
        ndax_max_retries=ndax_max_retries,
        signal_interval_seconds=signal_interval_seconds,
        ema_fast_period=ema_fast_period,
        ema_slow_period=ema_slow_period,
        atr_period=atr_period,
        stop_k=stop_k,
        max_hold_hours=max_hold_hours,
        cooldown_minutes=cooldown_minutes,
        max_new_entries_per_cycle=max_new_entries_per_cycle,
        enable_live_trading=enable_live_trading,
        taker_fee_rate=taker_fee_rate,
        min_order_notional_cad=min_order_notional_cad,
        order_status_poll_seconds=order_status_poll_seconds,
        order_status_max_attempts=order_status_max_attempts,
        preflight_min_warmup_coverage=preflight_min_warmup_coverage,
        daily_loss_cap_cad=daily_loss_cap_cad,
        max_slippage_pct=max_slippage_pct,
        consecutive_error_limit=consecutive_error_limit,
    )


def _parse_bool(raw_value: str) -> bool:
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")
