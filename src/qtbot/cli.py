"""CLI entrypoint for qtbot lifecycle commands."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import sys

from qtbot.config import RuntimeConfig, load_runtime_config
from qtbot.control import Command, read_control, write_control
from qtbot.ndax_client import (
    NdaxAuthenticationError,
    NdaxClient,
    NdaxError,
    load_credentials_from_env,
)
from qtbot.runner import BotRunner, RunnerAlreadyRunningError, is_pid_alive, read_runner_pid
from qtbot.state import StateStore
from qtbot.universe import resolve_tradable_universe


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
    subparsers.add_parser("ndax-pairs", help="List tradable NDAX CAD pairs for the configured universe.")

    candles_parser = subparsers.add_parser("ndax-candles", help="Fetch NDAX 1m candle history for a symbol.")
    candles_parser.add_argument("--symbol", required=True, help="NDAX instrument symbol (example: SOLCAD).")
    candles_parser.add_argument("--interval", type=int, default=60, help="Candle interval in seconds.")
    candles_parser.add_argument(
        "--from-date",
        default=_default_from_date_utc(),
        help="Start date in YYYY-MM-DD (UTC).",
    )
    candles_parser.add_argument(
        "--to-date",
        default=_default_to_date_utc(),
        help="End date in YYYY-MM-DD (UTC).",
    )

    subparsers.add_parser("ndax-balances", help="Fetch authenticated NDAX balances.")

    check_parser = subparsers.add_parser("ndax-check", help="Run full M2 NDAX integration check.")
    check_parser.add_argument(
        "--symbol",
        default=None,
        help="Optional NDAX symbol override for candle check (example: SOLCAD).",
    )
    check_parser.add_argument("--interval", type=int, default=60, help="Candle interval in seconds.")
    check_parser.add_argument(
        "--from-date",
        default=_default_from_date_utc(),
        help="Start date in YYYY-MM-DD (UTC).",
    )
    check_parser.add_argument(
        "--to-date",
        default=_default_to_date_utc(),
        help="End date in YYYY-MM-DD (UTC).",
    )
    check_parser.add_argument(
        "--skip-balances",
        action="store_true",
        help="Skip private balance check (useful when credentials are unavailable locally).",
    )

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
    if command == "ndax-pairs":
        return _handle_ndax_pairs(config=config)
    if command == "ndax-candles":
        return _handle_ndax_candles(
            config=config,
            symbol=args.symbol,
            interval=args.interval,
            from_date=args.from_date,
            to_date=args.to_date,
        )
    if command == "ndax-balances":
        return _handle_ndax_balances(config=config)
    if command == "ndax-check":
        return _handle_ndax_check(
            config=config,
            symbol=args.symbol,
            interval=args.interval,
            from_date=args.from_date,
            to_date=args.to_date,
            skip_balances=args.skip_balances,
        )

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


def _handle_ndax_pairs(*, config: RuntimeConfig) -> int:
    try:
        client = _make_ndax_client(config)
        instruments = client.get_instruments()
        resolution = resolve_tradable_universe(instruments)
    except NdaxError as exc:
        print(f"NDAX pair discovery failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ndax_base_url": config.ndax_base_url,
        "oms_id": config.ndax_oms_id,
        "instrument_count": len(instruments),
        "tradable_count": len(resolution.tradable),
        "tradable": [
            {
                "ticker": entry.ticker,
                "symbol": entry.ndax_symbol,
                "instrument_id": entry.instrument_id,
            }
            for entry in resolution.tradable
        ],
        "skipped": resolution.skipped,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _handle_ndax_candles(
    *,
    config: RuntimeConfig,
    symbol: str,
    interval: int,
    from_date: str,
    to_date: str,
) -> int:
    if interval <= 0:
        print("--interval must be > 0", file=sys.stderr)
        return 2

    try:
        parsed_from = _parse_date(from_date)
        parsed_to = _parse_date(to_date)
        if parsed_from > parsed_to:
            raise ValueError("--from-date must be <= --to-date")

        client = _make_ndax_client(config)
        instruments = client.get_instruments()
        instrument = _find_instrument_by_symbol(instruments, symbol)
        if instrument is None:
            raise NdaxError(f"Instrument symbol not found on NDAX: {symbol}")
        instrument_id = int(instrument["InstrumentId"])

        candles = client.get_ticker_history(
            instrument_id=instrument_id,
            interval_seconds=interval,
            from_date=parsed_from,
            to_date=parsed_to,
        )
    except (NdaxError, ValueError) as exc:
        print(f"NDAX candle fetch failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "symbol": symbol.upper(),
        "instrument_id": instrument_id,
        "interval_seconds": interval,
        "from_date": parsed_from.isoformat(),
        "to_date": parsed_to.isoformat(),
        "candle_count": len(candles),
        "first_candle": candles[0] if candles else None,
        "last_candle": candles[-1] if candles else None,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _handle_ndax_balances(*, config: RuntimeConfig) -> int:
    try:
        client = _make_ndax_client(config)
        credentials = load_credentials_from_env()
        account_id, balances = client.fetch_balances(credentials=credentials)
    except NdaxAuthenticationError as exc:
        print(f"NDAX authentication failed: {exc}", file=sys.stderr)
        return 1
    except NdaxError as exc:
        print(f"NDAX balance fetch failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "account_id": account_id,
        "balance_count": len(balances),
        "balances": [
            {
                "asset": item.product_symbol,
                "amount": item.amount,
                "hold": item.hold,
                "available": item.available,
            }
            for item in balances
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _handle_ndax_check(
    *,
    config: RuntimeConfig,
    symbol: str | None,
    interval: int,
    from_date: str,
    to_date: str,
    skip_balances: bool,
) -> int:
    if interval <= 0:
        print("--interval must be > 0", file=sys.stderr)
        return 2

    try:
        parsed_from = _parse_date(from_date)
        parsed_to = _parse_date(to_date)
        if parsed_from > parsed_to:
            raise ValueError("--from-date must be <= --to-date")

        client = _make_ndax_client(config)
        instruments = client.get_instruments()
        resolution = resolve_tradable_universe(instruments)
        chosen_symbol = (symbol or _choose_default_symbol(resolution)).upper()

        instrument = _find_instrument_by_symbol(instruments, chosen_symbol)
        if instrument is None:
            raise NdaxError(f"Instrument symbol not found on NDAX: {chosen_symbol}")
        instrument_id = int(instrument["InstrumentId"])
        candles = client.get_ticker_history(
            instrument_id=instrument_id,
            interval_seconds=interval,
            from_date=parsed_from,
            to_date=parsed_to,
        )

        balance_section: dict[str, object] | None = None
        if not skip_balances:
            credentials = load_credentials_from_env()
            account_id, balances = client.fetch_balances(credentials=credentials)
            balance_section = {
                "account_id": account_id,
                "balance_count": len(balances),
                "cad_available": _extract_asset_available(balances, "CAD"),
            }
    except (NdaxAuthenticationError, NdaxError, ValueError) as exc:
        print(f"NDAX check failed: {exc}", file=sys.stderr)
        return 1

    payload = {
        "ndax_base_url": config.ndax_base_url,
        "oms_id": config.ndax_oms_id,
        "instrument_count": len(instruments),
        "tradable_count": len(resolution.tradable),
        "tradable_symbols": [entry.ndax_symbol for entry in resolution.tradable],
        "skipped": resolution.skipped,
        "candle_check": {
            "symbol": chosen_symbol,
            "instrument_id": instrument_id,
            "interval_seconds": interval,
            "from_date": parsed_from.isoformat(),
            "to_date": parsed_to.isoformat(),
            "candle_count": len(candles),
            "last_candle": candles[-1] if candles else None,
        },
        "balance_check": balance_section,
        "balance_check_skipped": skip_balances,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _make_ndax_client(config: RuntimeConfig) -> NdaxClient:
    return NdaxClient(
        base_url=config.ndax_base_url,
        oms_id=config.ndax_oms_id,
        timeout_seconds=config.ndax_timeout_seconds,
        max_retries=config.ndax_max_retries,
    )


def _parse_date(raw_value: str):
    return datetime.strptime(raw_value, "%Y-%m-%d").date()


def _find_instrument_by_symbol(instruments: list[dict[str, object]], symbol: str) -> dict[str, object] | None:
    symbol_upper = symbol.upper()
    for instrument in instruments:
        if str(instrument.get("Symbol", "")).upper() == symbol_upper:
            return instrument
    return None


def _choose_default_symbol(resolution) -> str:
    if resolution.tradable:
        return resolution.tradable[0].ndax_symbol
    raise NdaxError("No tradable NDAX CAD pairs available for the configured universe.")


def _extract_asset_available(balances, asset: str) -> float | None:
    for balance in balances:
        if balance.product_symbol == asset:
            return balance.available
    return None


def _default_from_date_utc() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def _default_to_date_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
