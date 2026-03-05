"""CLI entrypoint for qtbot lifecycle commands."""

from __future__ import annotations

import argparse
import json
import sys

from qtbot.config import RuntimeConfig, load_runtime_config
from qtbot.control import Command, read_control, write_control
from qtbot.runner import BotRunner, RunnerAlreadyRunningError, is_pid_alive, read_runner_pid
from qtbot.state import StateStore


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qtbot",
        description="SimpleQuantTrade lifecycle CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start", help="Start the bot loop.")
    start_parser.add_argument("--budget", type=positive_float, required=True, help="CAD budget")

    subparsers.add_parser("pause", help="Pause a running bot.")
    subparsers.add_parser("resume", help="Resume a paused bot.")
    subparsers.add_parser("stop", help="Stop a running bot.")
    subparsers.add_parser("status", help="Show current bot status.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_runtime_config()
    except Exception as exc:  # pragma: no cover - simple CLI guard
        print(f"Failed to load runtime config: {exc}", file=sys.stderr)
        return 2

    command = args.command
    if command == "start":
        return _handle_start(config=config, budget_cad=args.budget)
    if command == "pause":
        return _handle_control_write(config=config, command=Command.PAUSE, reason="pause command")
    if command == "resume":
        return _handle_control_write(config=config, command=Command.RUN, reason="resume command")
    if command == "stop":
        return _handle_control_write(config=config, command=Command.STOP, reason="stop command")
    if command == "status":
        return _handle_status(config=config)

    print(f"Unknown command: {command}", file=sys.stderr)
    return 2


def _handle_start(*, config: RuntimeConfig, budget_cad: float) -> int:
    try:
        result = BotRunner(config=config, budget_cad=budget_cad).run()
    except RunnerAlreadyRunningError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:  # pragma: no cover - defensive path
        return 130
    except Exception as exc:
        print(f"Runner failed: {exc}", file=sys.stderr)
        return 1

    print(f"qtbot stopped gracefully. loop_count={result.loop_count}")
    return 0


def _handle_control_write(*, config: RuntimeConfig, command: Command, reason: str) -> int:
    state = write_control(
        config.control_file,
        command,
        updated_by=f"cli:{command.value.lower()}",
        reason=reason,
    )
    print(
        f"control updated: command={state.command.value} "
        f"updated_at_utc={state.updated_at_utc}"
    )
    return 0


def _handle_status(*, config: RuntimeConfig) -> int:
    control = read_control(config.control_file)
    state_store = StateStore(config.state_db)
    snapshot = state_store.get_snapshot()

    pid = read_runner_pid(config.pid_file)
    alive = is_pid_alive(pid) if pid is not None else False

    payload: dict[str, object] = {
        "runtime_dir": str(config.runtime_dir),
        "control_command": control.command.value,
        "control_updated_at_utc": control.updated_at_utc,
        "control_updated_by": control.updated_by,
        "control_reason": control.reason,
        "runner_pid": pid,
        "runner_alive": alive,
        "state": snapshot,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
