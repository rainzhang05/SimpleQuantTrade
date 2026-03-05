"""Live execution engine for M4 market-order trading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import time

from qtbot.config import RuntimeConfig
from qtbot.control import Command, read_control
from qtbot.ndax_client import (
    NdaxClient,
    NdaxCredentials,
    NdaxError,
    NdaxOrderRejectedError,
    OrderSide,
    load_credentials_from_env,
)
from qtbot.state import StateStore
from qtbot.strategy.signals import Decision, PositionSnapshot, empty_position
from qtbot.trade_log import TradeCsvLogger, TradeFillRecord
from qtbot.universe import LOCKED_COINS, UniverseEntry


@dataclass(frozen=True)
class ExecutionSummary:
    enter_filled: int
    exit_filled: int
    skipped: int
    failed: int
    message: str


class LiveExecutionEngine:
    """Executes ENTER/EXIT decisions via NDAX market orders and updates ledger state."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        trade_logger: TradeCsvLogger,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._state_store = state_store
        self._trade_logger = trade_logger
        self._logger = logger
        self._credentials: NdaxCredentials | None = None
        self._client_order_seq = int(time.time() * 1000)

    def execute_decisions(
        self,
        *,
        now_utc: datetime,
        decisions: list[Decision],
        tradable: list[UniverseEntry],
    ) -> ExecutionSummary:
        if not self._config.enable_live_trading:
            return ExecutionSummary(
                enter_filled=0,
                exit_filled=0,
                skipped=0,
                failed=0,
                message="execution_disabled_dry_run",
            )

        credentials = self._load_credentials()
        account_id, balances = self._ndax_client.fetch_balances(credentials=credentials)
        available_by_asset = {
            item.product_symbol.upper(): max(0.0, item.available)
            for item in balances
        }
        ndax_available_cad = available_by_asset.get("CAD", 0.0)
        pair_to_entry = {entry.ndax_symbol: entry for entry in tradable}
        positions = self._state_store.get_positions()
        skipped = 0
        failed = 0
        enter_filled = 0
        exit_filled = 0

        exit_decisions = [item for item in decisions if item.signal == "EXIT"]
        exit_decisions.sort(key=lambda item: item.symbol)
        for decision in exit_decisions:
            if not self._should_continue():
                return ExecutionSummary(
                    enter_filled=enter_filled,
                    exit_filled=exit_filled,
                    skipped=skipped,
                    failed=failed,
                    message="execution_interrupted_by_control",
                )

            entry = pair_to_entry.get(decision.symbol)
            if entry is None:
                skipped += 1
                continue
            if entry.ticker in LOCKED_COINS:
                skipped += 1
                continue

            position = positions.get(entry.ticker)
            if position is None or position.qty <= 0:
                skipped += 1
                continue

            exchange_available_qty = available_by_asset.get(entry.ticker, 0.0)
            sell_qty = min(position.qty, exchange_available_qty)
            if sell_qty <= 0:
                skipped += 1
                continue

            try:
                fill = self._execute_market_order(
                    credentials=credentials,
                    account_id=account_id,
                    entry=entry,
                    side="SELL",
                    quantity=sell_qty,
                    position=position,
                )
                exit_filled += 1
                ndax_available_cad += (fill.qty * fill.avg_price) - fill.fee_cad
                available_by_asset[entry.ticker] = max(0.0, exchange_available_qty - fill.qty)
                positions[entry.ticker] = self._state_store.get_positions().get(entry.ticker, empty_position(entry.ticker))
            except NdaxError:
                failed += 1
                self._logger.error(
                    "EXIT execution failed for %s (%s).",
                    decision.symbol,
                    entry.ticker,
                    exc_info=True,
                )

        enter_decisions = [item for item in decisions if item.signal == "ENTER"]
        enter_decisions.sort(key=lambda item: (-(item.score or 0.0), item.symbol))
        for index, decision in enumerate(enter_decisions):
            if not self._should_continue():
                return ExecutionSummary(
                    enter_filled=enter_filled,
                    exit_filled=exit_filled,
                    skipped=skipped,
                    failed=failed,
                    message="execution_interrupted_by_control",
                )

            entry = pair_to_entry.get(decision.symbol)
            if entry is None:
                skipped += 1
                continue
            if entry.ticker in LOCKED_COINS:
                skipped += 1
                continue

            position = positions.get(entry.ticker)
            if position is not None and position.qty > 0:
                skipped += 1
                continue

            remaining = len(enter_decisions) - index
            bot_cash_cad = self._state_store.get_bot_cash_cad()
            spendable_cad = min(bot_cash_cad, ndax_available_cad)
            order_notional = self._calculate_entry_notional(
                spendable_cad=spendable_cad,
                remaining_candidates=remaining,
            )
            if order_notional < self._config.min_order_notional_cad:
                skipped += 1
                continue
            if decision.close <= 0:
                skipped += 1
                continue
            quantity = order_notional / decision.close
            if quantity <= 0:
                skipped += 1
                continue

            try:
                fill = self._execute_market_order(
                    credentials=credentials,
                    account_id=account_id,
                    entry=entry,
                    side="BUY",
                    quantity=quantity,
                    position=position or empty_position(entry.ticker),
                )
                enter_filled += 1
                ndax_available_cad -= (fill.qty * fill.avg_price) + fill.fee_cad
                available_by_asset[entry.ticker] = available_by_asset.get(entry.ticker, 0.0) + fill.qty
                positions[entry.ticker] = self._state_store.get_positions().get(entry.ticker, empty_position(entry.ticker))
            except NdaxError:
                failed += 1
                self._logger.error(
                    "ENTER execution failed for %s (%s).",
                    decision.symbol,
                    entry.ticker,
                    exc_info=True,
                )

        message = (
            f"execution_complete enter_filled={enter_filled} exit_filled={exit_filled} "
            f"skipped={skipped} failed={failed}"
        )
        return ExecutionSummary(
            enter_filled=enter_filled,
            exit_filled=exit_filled,
            skipped=skipped,
            failed=failed,
            message=message,
        )

    def _execute_market_order(
        self,
        *,
        credentials: NdaxCredentials,
        account_id: int,
        entry: UniverseEntry,
        side: OrderSide,
        quantity: float,
        position: PositionSnapshot,
    ) -> TradeFillRecord:
        client_order_id = self._next_client_order_id()
        acceptance = self._ndax_client.send_market_order(
            credentials=credentials,
            account_id=account_id,
            instrument_id=entry.instrument_id,
            side=side,
            quantity=quantity,
            client_order_id=client_order_id,
        )
        fill = self._ndax_client.wait_for_fill(
            credentials=credentials,
            account_id=account_id,
            order_id=acceptance.order_id,
            poll_seconds=self._config.order_status_poll_seconds,
            max_attempts=self._config.order_status_max_attempts,
        )
        fill_qty = max(0.0, fill.qty_executed)
        fill_price = max(0.0, fill.avg_price)
        if fill_qty <= 0 or fill_price <= 0:
            raise NdaxOrderRejectedError(
                f"NDAX order {acceptance.order_id} fill invalid qty={fill_qty} avg_price={fill_price}"
            )

        timestamp_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        if side == "BUY":
            fee_cad = fill_qty * fill_price * self._config.taker_fee_rate
            self._state_store.apply_buy_fill(
                symbol=entry.ticker,
                qty=fill_qty,
                avg_price=fill_price,
                fee_cad=fee_cad,
                filled_at_utc=timestamp_utc,
                order_id=acceptance.order_id,
                ndax_symbol=entry.ndax_symbol,
            )
        else:
            sell_qty = min(fill_qty, position.qty)
            if sell_qty <= 0:
                raise NdaxOrderRejectedError(
                    f"Sell fill qty is not positive after position clamp: {sell_qty}"
                )
            fee_cad = sell_qty * fill_price * self._config.taker_fee_rate
            self._state_store.apply_sell_fill(
                symbol=entry.ticker,
                qty=sell_qty,
                avg_price=fill_price,
                fee_cad=fee_cad,
                filled_at_utc=timestamp_utc,
                order_id=acceptance.order_id,
                ndax_symbol=entry.ndax_symbol,
            )
            fill_qty = sell_qty

        record = TradeFillRecord(
            timestamp_utc=timestamp_utc,
            symbol=entry.ndax_symbol,
            side=side,
            qty=fill_qty,
            avg_price=fill_price,
            fee_cad=fee_cad,
            order_id=acceptance.order_id,
        )
        self._trade_logger.append(record)
        self._logger.info(
            "Order filled side=%s symbol=%s qty=%.12g avg_price=%.12g fee_cad=%.12g order_id=%s",
            side,
            entry.ndax_symbol,
            fill_qty,
            fill_price,
            fee_cad,
            acceptance.order_id,
        )
        return record

    def _load_credentials(self) -> NdaxCredentials:
        if self._credentials is None:
            self._credentials = load_credentials_from_env()
        return self._credentials

    def _should_continue(self) -> bool:
        control = read_control(self._config.control_file).command
        return control == Command.RUN

    def _calculate_entry_notional(self, *, spendable_cad: float, remaining_candidates: int) -> float:
        if spendable_cad <= 0 or remaining_candidates <= 0:
            return 0.0
        denominator = float(remaining_candidates)
        if self._config.taker_fee_rate > 0:
            denominator *= 1.0 + self._config.taker_fee_rate
        return spendable_cad / denominator

    def _next_client_order_id(self) -> int:
        self._client_order_seq = max(self._client_order_seq + 1, int(time.time() * 1000))
        return self._client_order_seq
