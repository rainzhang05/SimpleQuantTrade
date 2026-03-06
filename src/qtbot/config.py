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
    discord_webhook_url: str | None
    discord_timeout_seconds: float
    discord_max_retries: int
    data_sources: tuple[str, ...]
    dataset_mode: str
    binance_base_url: str
    binance_quote: str
    bridge_fx_symbol: str
    synth_weight_min: float
    synth_weight_max: float
    synth_weight_refresh: str
    synth_weight_default: float
    min_overlap_rows_for_weight: int
    conversion_max_median_ape: float
    combined_max_gap_count: int
    combined_min_coverage: float


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

    taker_fee_rate = float(os.getenv("QTBOT_TAKER_FEE_RATE", "0.002"))
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

    discord_webhook_url_raw = os.getenv("QTBOT_DISCORD_WEBHOOK_URL", "").strip()
    discord_webhook_url = discord_webhook_url_raw or None

    discord_timeout_seconds = float(os.getenv("QTBOT_DISCORD_TIMEOUT_SECONDS", "8"))
    if discord_timeout_seconds <= 0:
        raise ValueError("QTBOT_DISCORD_TIMEOUT_SECONDS must be > 0.")

    discord_max_retries = int(os.getenv("QTBOT_DISCORD_MAX_RETRIES", "2"))
    if discord_max_retries < 0:
        raise ValueError("QTBOT_DISCORD_MAX_RETRIES must be >= 0.")

    data_sources_raw = os.getenv("QTBOT_DATA_SOURCES", "ndax,binance")
    data_sources = tuple(
        item.strip().lower() for item in data_sources_raw.split(",") if item.strip()
    )
    valid_data_sources = {"ndax", "binance"}
    if not data_sources:
        raise ValueError("QTBOT_DATA_SOURCES must include at least one source.")
    if any(item not in valid_data_sources for item in data_sources):
        raise ValueError(
            "QTBOT_DATA_SOURCES supports only ndax and binance."
        )

    dataset_mode = os.getenv("QTBOT_DATASET_MODE", "combined").strip().lower()
    if dataset_mode not in {"ndax", "binance", "combined"}:
        raise ValueError("QTBOT_DATASET_MODE must be one of: ndax, binance, combined.")

    binance_base_url = os.getenv("QTBOT_BINANCE_BASE_URL", "https://api.binance.com").strip().rstrip("/")
    if not binance_base_url:
        raise ValueError("QTBOT_BINANCE_BASE_URL cannot be empty.")

    binance_quote = os.getenv("QTBOT_BINANCE_QUOTE", "USDT").strip().upper()
    if not binance_quote:
        raise ValueError("QTBOT_BINANCE_QUOTE cannot be empty.")

    bridge_fx_symbol = os.getenv("QTBOT_BRIDGE_FX_SYMBOL", "USDTCAD").strip().upper()
    if not bridge_fx_symbol:
        raise ValueError("QTBOT_BRIDGE_FX_SYMBOL cannot be empty.")

    synth_weight_min = float(os.getenv("QTBOT_SYNTH_WEIGHT_MIN", "0.20"))
    synth_weight_max = float(os.getenv("QTBOT_SYNTH_WEIGHT_MAX", "0.80"))
    synth_weight_default = float(os.getenv("QTBOT_SYNTH_WEIGHT_DEFAULT", "0.60"))
    if not (0.0 <= synth_weight_min <= 1.0):
        raise ValueError("QTBOT_SYNTH_WEIGHT_MIN must be in [0,1].")
    if not (0.0 <= synth_weight_max <= 1.0):
        raise ValueError("QTBOT_SYNTH_WEIGHT_MAX must be in [0,1].")
    if synth_weight_min > synth_weight_max:
        raise ValueError("QTBOT_SYNTH_WEIGHT_MIN must be <= QTBOT_SYNTH_WEIGHT_MAX.")
    if not (synth_weight_min <= synth_weight_default <= synth_weight_max):
        raise ValueError("QTBOT_SYNTH_WEIGHT_DEFAULT must be within min/max range.")

    synth_weight_refresh = os.getenv("QTBOT_SYNTH_WEIGHT_REFRESH", "monthly").strip().lower()
    if synth_weight_refresh not in {"monthly"}:
        raise ValueError("QTBOT_SYNTH_WEIGHT_REFRESH currently supports: monthly.")

    min_overlap_rows_for_weight = int(os.getenv("QTBOT_MIN_OVERLAP_ROWS_FOR_WEIGHT", "1000"))
    if min_overlap_rows_for_weight <= 0:
        raise ValueError("QTBOT_MIN_OVERLAP_ROWS_FOR_WEIGHT must be > 0.")

    conversion_max_median_ape = float(os.getenv("QTBOT_CONVERSION_MAX_MEDIAN_APE", "0.015"))
    if not (0 < conversion_max_median_ape < 1):
        raise ValueError("QTBOT_CONVERSION_MAX_MEDIAN_APE must be in (0,1).")

    combined_max_gap_count = int(os.getenv("QTBOT_COMBINED_MAX_GAP_COUNT", "0"))
    if combined_max_gap_count < 0:
        raise ValueError("QTBOT_COMBINED_MAX_GAP_COUNT must be >= 0.")

    combined_min_coverage = float(os.getenv("QTBOT_COMBINED_MIN_COVERAGE", "0.999"))
    if not (0 < combined_min_coverage <= 1):
        raise ValueError("QTBOT_COMBINED_MIN_COVERAGE must be in (0,1].")

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
        discord_webhook_url=discord_webhook_url,
        discord_timeout_seconds=discord_timeout_seconds,
        discord_max_retries=discord_max_retries,
        data_sources=data_sources,
        dataset_mode=dataset_mode,
        binance_base_url=binance_base_url,
        binance_quote=binance_quote,
        bridge_fx_symbol=bridge_fx_symbol,
        synth_weight_min=synth_weight_min,
        synth_weight_max=synth_weight_max,
        synth_weight_refresh=synth_weight_refresh,
        synth_weight_default=synth_weight_default,
        min_overlap_rows_for_weight=min_overlap_rows_for_weight,
        conversion_max_median_ape=conversion_max_median_ape,
        combined_max_gap_count=combined_max_gap_count,
        combined_min_coverage=combined_min_coverage,
    )


def _parse_bool(raw_value: str) -> bool:
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")
