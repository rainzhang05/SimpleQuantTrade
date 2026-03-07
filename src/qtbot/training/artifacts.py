"""Artifact helpers for deterministic Phase 6 training runs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd


def build_run_id(*, snapshot_id: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    safe_snapshot = snapshot_id.replace("/", "_")
    return f"{stamp}_{safe_snapshot}"


def ensure_run_dir(*, runtime_dir: Path, run_id: str) -> Path:
    path = runtime_dir / "research" / "training" / run_id
    path.mkdir(parents=True, exist_ok=False)
    return path


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        frame.to_parquet(tmp_path, index=False, compression="zstd")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)
