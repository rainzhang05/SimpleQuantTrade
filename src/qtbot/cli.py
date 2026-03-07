"""CLI entrypoint for qtbot lifecycle commands."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Callable

from qtbot.config import RuntimeConfig, load_runtime_config
from qtbot.control import Command, read_control, write_control
from qtbot.cutover import ProductionCutoverChecklist
from qtbot.binance_client import BinanceClient, BinanceError
from qtbot.kraken_client import KrakenClient, KrakenError
from qtbot.ndax_client import (
    NdaxAuthenticationError,
    NdaxClient,
    NdaxError,
    load_credentials_from_env,
)
from qtbot.runner import BotRunner, RunnerAlreadyRunningError, is_pid_alive, read_runner_pid
from qtbot.staging import StagingValidator
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
    check_parser.add_argument(
        "--require-balances",
        action="store_true",
        help="Fail ndax-check if private balance retrieval cannot be completed.",
    )

    data_backfill_parser = subparsers.add_parser(
        "data-backfill",
        help="Backfill NDAX/Binance candle data into local storage.",
    )
    data_backfill_parser.add_argument(
        "--from",
        dest="from_date",
        required=True,
        help="Start date in YYYY-MM-DD (UTC), or earliest for source-specific earliest available history.",
    )
    data_backfill_parser.add_argument(
        "--to",
        dest="to_date",
        required=True,
        help="End date in YYYY-MM-DD (UTC).",
    )
    data_backfill_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Candle timeframe alias (currently supports 15m).",
    )
    data_backfill_parser.add_argument(
        "--sources",
        default="ndax,kraken,binance",
        help="Comma-separated data sources: ndax,kraken,binance.",
    )
    data_backfill_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable live progress lines on stderr (JSON summary still printed on stdout).",
    )

    data_status_parser = subparsers.add_parser(
        "data-status",
        help="Show local candle coverage and gap status for selected dataset(s).",
    )
    data_status_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe alias to inspect (currently supports 15m).",
    )
    data_status_parser.add_argument(
        "--dataset",
        default="combined",
        help="Dataset to inspect: ndax|kraken|binance|combined|all.",
    )

    data_build_combined_parser = subparsers.add_parser(
        "data-build-combined",
        help="Build normalized combined NDAX+external CAD dataset (Kraken primary, Binance fallback).",
    )
    data_build_combined_parser.add_argument(
        "--from",
        dest="from_date",
        required=True,
        help="Start date in YYYY-MM-DD (UTC).",
    )
    data_build_combined_parser.add_argument(
        "--to",
        dest="to_date",
        required=True,
        help="End date in YYYY-MM-DD (UTC).",
    )
    data_build_combined_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe alias (currently supports 15m).",
    )

    data_calibrate_parser = subparsers.add_parser(
        "data-calibrate-weights",
        help="Calibrate synthetic data weighting from NDAX/external overlap.",
    )
    data_calibrate_parser.add_argument(
        "--from",
        dest="from_date",
        required=True,
        help="Start date in YYYY-MM-DD (UTC).",
    )
    data_calibrate_parser.add_argument(
        "--to",
        dest="to_date",
        required=True,
        help="End date in YYYY-MM-DD (UTC).",
    )
    data_calibrate_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe alias (currently supports 15m).",
    )
    data_calibrate_parser.add_argument(
        "--refresh",
        default="monthly",
        help="Weight refresh cadence (currently supports monthly).",
    )

    data_weight_status_parser = subparsers.add_parser(
        "data-weight-status",
        help="Show latest per-symbol synthetic weight status.",
    )
    data_weight_status_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe alias (currently supports 15m).",
    )

    build_snapshot_parser = subparsers.add_parser(
        "build-snapshot",
        help="Build a deterministic supervised training snapshot from local data.",
    )
    build_snapshot_parser.add_argument(
        "--asof",
        required=True,
        help="As-of time in ISO 8601. UTC is assumed when no offset is provided.",
    )
    build_snapshot_parser.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe alias (currently supports 15m).",
    )
    build_snapshot_parser.add_argument(
        "--label-horizon-bars",
        type=int,
        default=None,
        help="Optional label horizon in closed 15m bars; defaults to QTBOT_LABEL_HORIZON_BARS.",
    )
    build_snapshot_parser.add_argument(
        "--exclude-symbols",
        default="",
        help="Optional comma-separated tickers or CAD symbols to exclude from the snapshot experiment.",
    )

    train_parser = subparsers.add_parser(
        "train",
        help="Train deterministic walk-forward LightGBM models from a sealed snapshot.",
    )
    train_parser.add_argument(
        "--snapshot",
        required=True,
        help="Snapshot ID under data/snapshots/.",
    )
    train_parser.add_argument(
        "--folds",
        type=int,
        default=12,
        help="Number of latest eligible walk-forward folds to train.",
    )
    train_parser.add_argument(
        "--universe",
        default="V1",
        help="Universe name (currently supports V1 only).",
    )

    eval_parser = subparsers.add_parser(
        "eval",
        help="Evaluate persisted validation predictions for a training run.",
    )
    eval_parser.add_argument(
        "--run",
        required=True,
        help="Training run ID under runtime/research/training/.",
    )

    backtest_parser = subparsers.add_parser(
        "backtest",
        help="Run a portfolio-style backtest over persisted validation predictions.",
    )
    backtest_parser.add_argument(
        "--run",
        required=True,
        help="Training run ID under runtime/research/training/.",
    )
    backtest_parser.add_argument(
        "--scenario",
        default=None,
        help="Optional scenario override; defaults to promoted scenario when available, else run primary scenario.",
    )
    backtest_parser.add_argument(
        "--model-scope",
        default="global",
        help="Prediction scope to backtest: global|per_coin.",
    )
    backtest_parser.add_argument(
        "--entry-threshold",
        type=float,
        default=None,
        help="Optional probability threshold; defaults to QTBOT_PROMOTION_ENTRY_THRESHOLD.",
    )
    backtest_parser.add_argument(
        "--initial-capital",
        type=positive_float,
        default=None,
        help="Optional initial capital in CAD; defaults to QTBOT_BACKTEST_INITIAL_CAPITAL_CAD.",
    )
    backtest_parser.add_argument(
        "--max-active-positions",
        type=int,
        default=None,
        help="Optional max concurrent positions; defaults to QTBOT_BACKTEST_MAX_ACTIVE_POSITIONS.",
    )
    backtest_parser.add_argument(
        "--position-fraction",
        type=positive_float,
        default=None,
        help="Optional equity fraction per new position; defaults to QTBOT_BACKTEST_POSITION_FRACTION.",
    )
    backtest_parser.add_argument(
        "--slippage-pct-per-side",
        type=float,
        default=None,
        help="Optional slippage stress per side; defaults to QTBOT_BACKTEST_SLIPPAGE_PCT_PER_SIDE.",
    )

    attribution_parser = subparsers.add_parser(
        "attribution",
        help="Generate deterministic coin attribution report for a training run.",
    )
    attribution_parser.add_argument(
        "--run",
        required=True,
        help="Training run ID under runtime/research/training/.",
    )

    promote_parser = subparsers.add_parser(
        "promote",
        help="Apply deterministic promotion gates and publish a signed model bundle.",
    )
    promote_parser.add_argument(
        "--run",
        required=True,
        help="Training run ID under runtime/research/training/.",
    )

    subparsers.add_parser(
        "model-status",
        help="Show active promoted bundle status and integrity.",
    )

    set_active_bundle_parser = subparsers.add_parser(
        "set-active-bundle",
        help="Atomically switch the active promoted bundle while paused or stopped.",
    )
    set_active_bundle_parser.add_argument(
        "bundle_id",
        help="Bundle ID under models/bundles/.",
    )

    staging_parser = subparsers.add_parser(
        "staging-validate",
        help="Run M10 staging validation workflow and emit JSON report.",
    )
    staging_parser.add_argument(
        "--budget",
        type=positive_float,
        default=1000.0,
        help="Budget used for dry-run staging loop startup.",
    )
    staging_parser.add_argument(
        "--cadence-seconds",
        type=int,
        default=3,
        help="Temporary cadence used for staging loop validation.",
    )
    staging_parser.add_argument(
        "--min-loops",
        type=int,
        default=2,
        help="Minimum loop count required before lifecycle drill proceeds.",
    )
    staging_parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="Timeout for live staging drill phases.",
    )
    staging_parser.add_argument(
        "--offline-only",
        action="store_true",
        help="Skip live NDAX drills and run only offline/simulated staging checks.",
    )

    cutover_parser = subparsers.add_parser(
        "cutover-checklist",
        help="Run M11 production cutover checklist and emit JSON report.",
    )
    cutover_parser.add_argument(
        "--budget",
        type=positive_float,
        default=250.0,
        help="Constrained launch budget for preflight/cutover readiness checks.",
    )
    cutover_parser.add_argument(
        "--staging-max-age-hours",
        type=int,
        default=48,
        help="Maximum allowed age of staging validation report.",
    )
    cutover_parser.add_argument(
        "--offline-only",
        action="store_true",
        help="Skip live NDAX private/preflight gates and run offline cutover checks.",
    )
    cutover_parser.add_argument(
        "--require-discord",
        action="store_true",
        help="Require QTBOT_DISCORD_WEBHOOK_URL for cutover readiness pass.",
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
            require_balances=args.require_balances,
        )
    if command == "data-backfill":
        return _handle_data_backfill(
            config=config,
            from_date=args.from_date,
            to_date=args.to_date,
            timeframe=args.timeframe,
            sources=args.sources,
            quiet=args.quiet,
        )
    if command == "data-status":
        return _handle_data_status(
            config=config,
            timeframe=args.timeframe,
            dataset=args.dataset,
        )
    if command == "data-build-combined":
        return _handle_data_build_combined(
            config=config,
            from_date=args.from_date,
            to_date=args.to_date,
            timeframe=args.timeframe,
        )
    if command == "data-calibrate-weights":
        return _handle_data_calibrate_weights(
            config=config,
            from_date=args.from_date,
            to_date=args.to_date,
            timeframe=args.timeframe,
            refresh=args.refresh,
        )
    if command == "data-weight-status":
        return _handle_data_weight_status(
            config=config,
            timeframe=args.timeframe,
        )
    if command == "build-snapshot":
        return _handle_build_snapshot(
            config=config,
            asof=args.asof,
            timeframe=args.timeframe,
            label_horizon_bars=args.label_horizon_bars,
            exclude_symbols=args.exclude_symbols,
        )
    if command == "train":
        return _handle_train(
            config=config,
            snapshot_id=args.snapshot,
            folds=args.folds,
            universe=args.universe,
        )
    if command == "eval":
        return _handle_eval(
            config=config,
            run_id=args.run,
        )
    if command == "backtest":
        return _handle_backtest(
            config=config,
            run_id=args.run,
            scenario=args.scenario,
            model_scope=args.model_scope,
            entry_threshold=args.entry_threshold,
            initial_capital_cad=args.initial_capital,
            max_active_positions=args.max_active_positions,
            position_fraction=args.position_fraction,
            slippage_pct_per_side=args.slippage_pct_per_side,
        )
    if command == "attribution":
        return _handle_attribution(
            config=config,
            run_id=args.run,
        )
    if command == "promote":
        return _handle_promote(
            config=config,
            run_id=args.run,
        )
    if command == "model-status":
        return _handle_model_status(config=config)
    if command == "set-active-bundle":
        return _handle_set_active_bundle(
            config=config,
            bundle_id=args.bundle_id,
        )
    if command == "staging-validate":
        return _handle_staging_validate(
            config=config,
            budget_cad=args.budget,
            cadence_seconds=args.cadence_seconds,
            min_loops=args.min_loops,
            timeout_seconds=args.timeout_seconds,
            offline_only=args.offline_only,
        )
    if command == "cutover-checklist":
        return _handle_cutover_checklist(
            config=config,
            start_budget_cad=args.budget,
            staging_max_age_hours=args.staging_max_age_hours,
            offline_only=args.offline_only,
            require_discord=args.require_discord,
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
    require_balances: bool,
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
        balance_skipped_reason: str | None = None
        if not skip_balances:
            try:
                credentials = load_credentials_from_env()
                account_id, balances = client.fetch_balances(credentials=credentials)
                balance_section = {
                    "account_id": account_id,
                    "balance_count": len(balances),
                    "cad_available": _extract_asset_available(balances, "CAD"),
                }
            except NdaxAuthenticationError as exc:
                if require_balances:
                    raise
                balance_skipped_reason = str(exc)
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
        "balance_check_skipped": skip_balances or balance_section is None,
        "balance_check_skipped_reason": balance_skipped_reason if balance_section is None else None,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _handle_data_backfill(
    *,
    config: RuntimeConfig,
    from_date: str,
    to_date: str,
    timeframe: str,
    sources: str,
    quiet: bool = False,
) -> int:
    progress_callback, progress_log = _build_data_backfill_progress_writer(
        config=config,
        quiet=quiet,
    )
    try:
        parsed_from = _parse_backfill_start(from_date)
        parsed_to = _parse_date(to_date)
        if parsed_from is not None and parsed_from > parsed_to:
            raise ValueError("--from must be <= --to")

        service = _make_data_service(
            config,
            progress_callback=progress_callback,
        )
        summary = service.backfill(
            from_date=parsed_from,
            to_date=parsed_to,
            timeframe=timeframe,
            sources=_parse_sources_csv(sources),
        )
    except (NdaxError, BinanceError, KrakenError, ValueError) as exc:
        progress_callback(f"data_backfill_failed reason={exc}")
        print(f"Data backfill failed: {exc}", file=sys.stderr)
        return 1

    payload = summary.to_payload()
    payload["progress_log_file"] = str(progress_log)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if summary.symbols_with_errors == 0 else 1


def _handle_data_status(
    *,
    config: RuntimeConfig,
    timeframe: str,
    dataset: str,
) -> int:
    try:
        service = _make_data_service(config)
        summary = service.data_status(timeframe=timeframe, dataset=dataset)
    except (NdaxError, BinanceError, KrakenError, ValueError) as exc:
        print(f"Data status failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_data_build_combined(
    *,
    config: RuntimeConfig,
    from_date: str,
    to_date: str,
    timeframe: str,
) -> int:
    try:
        parsed_from = _parse_date(from_date)
        parsed_to = _parse_date(to_date)
        if parsed_from > parsed_to:
            raise ValueError("--from must be <= --to")
        service = _make_data_service(config)
        summary = service.build_combined(
            from_date=parsed_from,
            to_date=parsed_to,
            timeframe=timeframe,
        )
    except (NdaxError, BinanceError, KrakenError, ValueError) as exc:
        print(f"Combined dataset build failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0 if summary.symbols_with_errors == 0 else 1


def _handle_data_calibrate_weights(
    *,
    config: RuntimeConfig,
    from_date: str,
    to_date: str,
    timeframe: str,
    refresh: str,
) -> int:
    try:
        parsed_from = _parse_date(from_date)
        parsed_to = _parse_date(to_date)
        if parsed_from > parsed_to:
            raise ValueError("--from must be <= --to")
        service = _make_data_service(config)
        summary = service.calibrate_weights(
            from_date=parsed_from,
            to_date=parsed_to,
            timeframe=timeframe,
            refresh=refresh,
        )
    except (NdaxError, BinanceError, KrakenError, ValueError) as exc:
        print(f"Weight calibration failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_data_weight_status(
    *,
    config: RuntimeConfig,
    timeframe: str,
) -> int:
    try:
        service = _make_data_service(config)
        summary = service.weight_status(timeframe=timeframe)
    except (NdaxError, BinanceError, KrakenError, ValueError) as exc:
        print(f"Weight status failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_build_snapshot(
    *,
    config: RuntimeConfig,
    asof: str,
    timeframe: str,
    label_horizon_bars: int | None = None,
    exclude_symbols: str = "",
) -> int:
    try:
        parsed_asof = _parse_datetime_utc(asof)
        service = _make_snapshot_service(config)
        summary = service.build_snapshot(
            asof=parsed_asof,
            timeframe=timeframe,
            label_horizon_bars=label_horizon_bars,
            exclude_symbols=_parse_symbol_exclusions(exclude_symbols),
        )
    except ValueError as exc:
        print(f"Snapshot build failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_train(
    *,
    config: RuntimeConfig,
    snapshot_id: str,
    folds: int,
    universe: str,
) -> int:
    try:
        service = _make_training_service(config)
        summary = service.train(
            snapshot_id=snapshot_id,
            folds=folds,
            universe=universe,
        )
    except ValueError as exc:
        print(f"Training failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_eval(
    *,
    config: RuntimeConfig,
    run_id: str,
) -> int:
    try:
        service = _make_evaluation_service(config)
        summary = service.evaluate(run_id=run_id)
    except ValueError as exc:
        print(f"Evaluation failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_backtest(
    *,
    config: RuntimeConfig,
    run_id: str,
    scenario: str | None,
    model_scope: str,
    entry_threshold: float | None,
    initial_capital_cad: float | None,
    max_active_positions: int | None,
    position_fraction: float | None,
    slippage_pct_per_side: float | None,
) -> int:
    try:
        service = _make_backtest_service(config)
        summary = service.backtest(
            run_id=run_id,
            scenario=scenario,
            model_scope=model_scope,
            entry_threshold=entry_threshold,
            initial_capital_cad=initial_capital_cad,
            max_active_positions=max_active_positions,
            position_fraction=position_fraction,
            slippage_pct_per_side=slippage_pct_per_side,
        )
    except ValueError as exc:
        print(f"Backtest failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_attribution(
    *,
    config: RuntimeConfig,
    run_id: str,
) -> int:
    try:
        service = _make_attribution_service(config)
        summary = service.generate(run_id=run_id)
    except ValueError as exc:
        print(f"Attribution failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0


def _handle_promote(
    *,
    config: RuntimeConfig,
    run_id: str,
) -> int:
    try:
        service = _make_promotion_service(config)
        summary = service.promote(run_id=run_id)
    except ValueError as exc:
        print(f"Promotion failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0 if summary.decision == "accepted" else 1


def _handle_model_status(*, config: RuntimeConfig) -> int:
    service = _make_promotion_service(config)
    summary = service.model_status()
    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0 if summary.integrity_status in {"ok", "missing"} else 1


def _handle_set_active_bundle(
    *,
    config: RuntimeConfig,
    bundle_id: str,
) -> int:
    try:
        service = _make_promotion_service(config)
        summary = service.set_active_bundle(bundle_id=bundle_id)
    except ValueError as exc:
        print(f"Set active bundle failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary.to_payload(), indent=2, sort_keys=True))
    return 0 if summary.integrity_status == "ok" else 1


def _handle_staging_validate(
    *,
    config: RuntimeConfig,
    budget_cad: float,
    cadence_seconds: int,
    min_loops: int,
    timeout_seconds: int,
    offline_only: bool,
) -> int:
    if cadence_seconds <= 0:
        print("--cadence-seconds must be > 0", file=sys.stderr)
        return 2
    if min_loops <= 0:
        print("--min-loops must be > 0", file=sys.stderr)
        return 2
    if timeout_seconds <= 0:
        print("--timeout-seconds must be > 0", file=sys.stderr)
        return 2

    try:
        report = StagingValidator(config=config).run(
            budget_cad=budget_cad,
            cadence_seconds=cadence_seconds,
            min_loops=min_loops,
            timeout_seconds=timeout_seconds,
            offline_only=offline_only,
        )
    except Exception as exc:
        print(f"Staging validation failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report.to_payload(), indent=2, sort_keys=True))
    return 0 if report.passed else 1


def _handle_cutover_checklist(
    *,
    config: RuntimeConfig,
    start_budget_cad: float,
    staging_max_age_hours: int,
    offline_only: bool,
    require_discord: bool,
) -> int:
    if staging_max_age_hours <= 0:
        print("--staging-max-age-hours must be > 0", file=sys.stderr)
        return 2

    try:
        report = ProductionCutoverChecklist(config=config).run(
            start_budget_cad=start_budget_cad,
            staging_max_age_hours=staging_max_age_hours,
            offline_only=offline_only,
            require_discord=require_discord,
        )
    except Exception as exc:
        print(f"Cutover checklist failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report.to_payload(), indent=2, sort_keys=True))
    return 0 if report.passed else 1


def _make_ndax_client(config: RuntimeConfig) -> NdaxClient:
    return NdaxClient(
        base_url=config.ndax_base_url,
        oms_id=config.ndax_oms_id,
        timeout_seconds=config.ndax_timeout_seconds,
        max_retries=config.ndax_max_retries,
    )


def _make_binance_client(config: RuntimeConfig) -> BinanceClient:
    return BinanceClient(
        base_url=config.binance_base_url,
        timeout_seconds=config.ndax_timeout_seconds,
        max_retries=config.ndax_max_retries,
    )


def _make_kraken_client(config: RuntimeConfig) -> KrakenClient:
    return KrakenClient(
        base_url=config.kraken_base_url,
        timeout_seconds=config.ndax_timeout_seconds,
        max_retries=config.ndax_max_retries,
    )


def _make_data_service(
    config: RuntimeConfig,
    *,
    progress_callback: Callable[[str], None] | None = None,
):
    # Lazy import keeps non-data commands resilient if optional parquet dependency is absent.
    from qtbot.data import MarketDataService

    client = _make_ndax_client(config)
    binance_client = _make_binance_client(config)
    kraken_client = _make_kraken_client(config)
    state_store = StateStore(config.state_db)
    return MarketDataService(
        config=config,
        ndax_client=client,
        binance_client=binance_client,
        kraken_client=kraken_client,
        state_store=state_store,
        progress_callback=progress_callback,
    )


def _make_snapshot_service(config: RuntimeConfig):
    from qtbot.snapshot import TrainingSnapshotService

    return TrainingSnapshotService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _make_training_service(config: RuntimeConfig):
    from qtbot.training import TrainingService

    return TrainingService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _make_evaluation_service(config: RuntimeConfig):
    from qtbot.training import EvaluationService

    return EvaluationService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _make_backtest_service(config: RuntimeConfig):
    from qtbot.training import PortfolioBacktestService

    return PortfolioBacktestService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _make_attribution_service(config: RuntimeConfig):
    from qtbot.training import AttributionService

    return AttributionService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _make_promotion_service(config: RuntimeConfig):
    from qtbot.training import PromotionService

    return PromotionService(
        config=config,
        state_store=StateStore(config.state_db),
    )


def _build_data_backfill_progress_writer(
    *,
    config: RuntimeConfig,
    quiet: bool,
) -> tuple[Callable[[str], None], Path]:
    log_path = config.runtime_dir / "logs" / "data_backfill.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def _write(message: str) -> None:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        line = f"{timestamp} {message}"
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        if not quiet:
            print(line, file=sys.stderr, flush=True)

    return _write, log_path


def _parse_date(raw_value: str):
    return datetime.strptime(raw_value, "%Y-%m-%d").date()


def _parse_backfill_start(raw_value: str):
    value = raw_value.strip().lower()
    if value == "earliest":
        return None
    return _parse_date(raw_value)


def _parse_datetime_utc(raw_value: str) -> datetime:
    value = raw_value.strip()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("--asof must be a valid ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_sources_csv(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_symbol_exclusions(raw_value: str) -> set[str]:
    exclusions: set[str] = set()
    for item in raw_value.split(","):
        token = item.strip().upper()
        if not token:
            continue
        exclusions.add(token if token.endswith("CAD") else f"{token}CAD")
    return exclusions


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
