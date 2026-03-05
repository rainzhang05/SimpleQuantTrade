"""Ledger accounting helpers for fee-aware spot trading."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BuyLedgerUpdate:
    new_qty: float
    new_avg_entry_price: float
    cash_delta_cad: float
    fee_delta_cad: float
    realized_pnl_delta_cad: float


@dataclass(frozen=True)
class SellLedgerUpdate:
    new_qty: float
    new_avg_entry_price: float
    cash_delta_cad: float
    fee_delta_cad: float
    realized_pnl_delta_cad: float


def compute_buy_update(
    *,
    current_qty: float,
    current_avg_entry_price: float,
    fill_qty: float,
    fill_price: float,
    fee_cad: float,
) -> BuyLedgerUpdate:
    fill_notional_cad = fill_qty * fill_price
    current_cost_basis_cad = current_qty * current_avg_entry_price
    new_qty = current_qty + fill_qty
    if new_qty <= 0:
        new_avg_entry_price = 0.0
    else:
        # Buy fees are capitalized into cost basis so realized PnL is fee-aware on exit.
        new_avg_entry_price = (current_cost_basis_cad + fill_notional_cad + fee_cad) / new_qty
    return BuyLedgerUpdate(
        new_qty=new_qty,
        new_avg_entry_price=new_avg_entry_price,
        cash_delta_cad=-(fill_notional_cad + fee_cad),
        fee_delta_cad=fee_cad,
        realized_pnl_delta_cad=0.0,
    )


def compute_sell_update(
    *,
    current_qty: float,
    current_avg_entry_price: float,
    fill_qty: float,
    fill_price: float,
    fee_cad: float,
) -> SellLedgerUpdate:
    if fill_qty <= 0:
        raise ValueError("fill_qty must be > 0 for sell update.")
    if current_qty <= 0:
        raise ValueError("current_qty must be > 0 for sell update.")
    if fill_qty > current_qty + 1e-12:
        raise ValueError("Cannot sell more than current_qty.")

    fill_notional_cad = fill_qty * fill_price
    proceeds_after_fee = fill_notional_cad - fee_cad
    realized_pnl_delta_cad = proceeds_after_fee - (current_avg_entry_price * fill_qty)

    new_qty = current_qty - fill_qty
    if new_qty <= 1e-12:
        return SellLedgerUpdate(
            new_qty=0.0,
            new_avg_entry_price=0.0,
            cash_delta_cad=proceeds_after_fee,
            fee_delta_cad=fee_cad,
            realized_pnl_delta_cad=realized_pnl_delta_cad,
        )

    return SellLedgerUpdate(
        new_qty=new_qty,
        new_avg_entry_price=current_avg_entry_price,
        cash_delta_cad=proceeds_after_fee,
        fee_delta_cad=fee_cad,
        realized_pnl_delta_cad=realized_pnl_delta_cad,
    )
