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
    )
