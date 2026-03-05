"""Startup reconciliation with NDAX as source of truth (M5)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

from qtbot.alerts import DiscordAlerter
from qtbot.config import RuntimeConfig
from qtbot.ndax_client import (
    NdaxBalance,
    NdaxClient,
    NdaxCredentials,
    NdaxError,
    load_credentials_from_env,
)
from qtbot.state import StateStore
from qtbot.universe import UniverseEntry, resolve_tradable_universe


@dataclass(frozen=True)
class ReconciliationSummary:
    account_id: int
    ndax_available_cad: float
    compared_symbols: int
    changed_symbols: int
    capped_bot_cash: bool
    message: str


class StartupReconciler:
    """Reconciles internal position state against live NDAX holdings."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        logger: logging.Logger,
        alerter: DiscordAlerter | None = None,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._state_store = state_store
        self._logger = logger
        self._alerter = alerter

    def reconcile(self) -> ReconciliationSummary:
        credentials = load_credentials_from_env()
        instruments = self._ndax_client.get_instruments()
        resolution = resolve_tradable_universe(instruments)
        account_id, balances = self._ndax_client.fetch_balances(credentials=credentials)

        balances_by_asset = {item.product_symbol.upper(): item for item in balances}
        ndax_available_cad = _available_cad(balances_by_asset.get("CAD"))
        internal_positions = self._state_store.get_positions()

        tradable_by_ticker = {item.ticker: item for item in resolution.tradable}
        compared_tickers = sorted(set(tradable_by_ticker) | set(internal_positions))
        changed_symbols = 0

        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for ticker in compared_tickers:
            internal_position = internal_positions.get(ticker)
            internal_qty = internal_position.qty if internal_position is not None else 0.0
            ndax_qty = _ndax_amount_for_ticker(balances_by_asset, ticker)
            if abs(internal_qty - ndax_qty) <= 1e-9:
                continue

            entry = tradable_by_ticker.get(ticker)
            reference_price = self._load_reference_price(credentials=credentials, entry=entry)
            changed = self._state_store.reconcile_position(
                symbol=ticker,
                ndax_qty=ndax_qty,
                reference_price=reference_price,
                reconciled_at_utc=now_iso,
                reason="startup_ndax_truth",
            )
            if changed:
                changed_symbols += 1
                self._logger.warning(
                    "Reconciled symbol=%s internal_qty=%.12g ndax_qty=%.12g reference_price=%s",
                    ticker,
                    internal_qty,
                    ndax_qty,
                    f"{reference_price:.12g}" if reference_price is not None else "None",
                )

        capped_bot_cash = self._state_store.cap_bot_cash(
            max_cash_cad=max(0.0, ndax_available_cad),
            reason="startup_reconciliation",
        )
        if capped_bot_cash:
            self._logger.warning(
                "Bot cash capped to available CAD after reconciliation: ndax_available_cad=%.12g",
                ndax_available_cad,
            )

        message = (
            "reconciliation_complete "
            f"account_id={account_id} compared={len(compared_tickers)} changed={changed_symbols} "
            f"capped_bot_cash={str(capped_bot_cash).lower()} ndax_available_cad={ndax_available_cad:.12g}"
        )
        self._state_store.add_event(
            event_type="RECONCILIATION_COMPLETED",
            detail=message,
        )
        if changed_symbols > 0 or capped_bot_cash:
            self._notify_reconciliation_anomaly(
                summary="startup reconciliation adjusted local state",
                detail=message,
            )
        return ReconciliationSummary(
            account_id=account_id,
            ndax_available_cad=ndax_available_cad,
            compared_symbols=len(compared_tickers),
            changed_symbols=changed_symbols,
            capped_bot_cash=capped_bot_cash,
            message=message,
        )

    def _load_reference_price(
        self,
        *,
        credentials: NdaxCredentials,
        entry: UniverseEntry | None,
    ) -> float | None:
        del credentials  # reserved for future private price sources
        if entry is None:
            return None
        try:
            candles = self._ndax_client.get_recent_ticker_history(
                instrument_id=entry.instrument_id,
                interval_seconds=self._config.signal_interval_seconds,
                lookback_hours=24,
            )
        except NdaxError:
            return None
        return _latest_close(candles)

    def _notify_reconciliation_anomaly(self, *, summary: str, detail: str) -> None:
        if self._alerter is None:
            return
        self._alerter.send(
            category="RECONCILIATION_ANOMALY",
            summary=summary,
            severity="WARNING",
            detail=detail,
        )


def _latest_close(candles: list[list[Any]]) -> float | None:
    if not candles:
        return None
    rows = sorted((row for row in candles if isinstance(row, list) and len(row) >= 5), key=lambda item: int(item[0]))
    if not rows:
        return None
    try:
        return float(rows[-1][4])
    except (TypeError, ValueError):
        return None


def _ndax_amount_for_ticker(balances_by_asset: dict[str, NdaxBalance], ticker: str) -> float:
    item = balances_by_asset.get(ticker.upper())
    if item is None:
        return 0.0
    return max(0.0, item.amount)


def _available_cad(balance: NdaxBalance | None) -> float:
    if balance is None:
        return 0.0
    return max(0.0, balance.available)
