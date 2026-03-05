"""Runtime configuration for qtbot."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


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


def load_runtime_config() -> RuntimeConfig:
    cadence_raw = os.getenv("QTBOT_CADENCE_SECONDS", "60")
    cadence_seconds = int(cadence_raw)
    if cadence_seconds <= 0:
        raise ValueError("QTBOT_CADENCE_SECONDS must be > 0.")

    runtime_dir = _resolve_runtime_dir(os.getenv("QTBOT_RUNTIME_DIR", "runtime"))
    return RuntimeConfig(
        cadence_seconds=cadence_seconds,
        runtime_dir=runtime_dir,
        control_file=runtime_dir / "control.json",
        state_db=runtime_dir / "state.sqlite",
        log_file=runtime_dir / "logs" / "qtbot.log",
        pid_file=runtime_dir / "runner.pid",
    )
