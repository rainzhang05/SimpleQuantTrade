"""Control plane file operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
import tempfile


class Command(str, Enum):
    RUN = "RUN"
    PAUSE = "PAUSE"
    STOP = "STOP"


@dataclass(frozen=True)
class ControlState:
    command: Command
    updated_at_utc: str | None
    updated_by: str | None
    reason: str | None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_control(path: Path) -> ControlState:
    if not path.exists():
        return ControlState(
            command=Command.STOP,
            updated_at_utc=None,
            updated_by=None,
            reason="control file missing",
        )

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ControlState(
            command=Command.STOP,
            updated_at_utc=None,
            updated_by=None,
            reason="invalid control file",
        )

    command = _parse_command(payload.get("command"))
    return ControlState(
        command=command,
        updated_at_utc=payload.get("updated_at_utc"),
        updated_by=payload.get("updated_by"),
        reason=payload.get("reason"),
    )


def write_control(
    path: Path,
    command: Command,
    *,
    updated_by: str,
    reason: str,
) -> ControlState:
    payload = {
        "command": command.value,
        "updated_at_utc": utc_now_iso(),
        "updated_by": updated_by,
        "reason": reason,
    }
    _atomic_write_json(path, payload)
    return ControlState(
        command=command,
        updated_at_utc=payload["updated_at_utc"],
        updated_by=updated_by,
        reason=reason,
    )


def _parse_command(raw_command: object) -> Command:
    if not isinstance(raw_command, str):
        return Command.STOP
    try:
        return Command(raw_command)
    except ValueError:
        return Command.STOP


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
        ) as temp_file:
            json.dump(payload, temp_file, indent=2, sort_keys=True)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)

        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
