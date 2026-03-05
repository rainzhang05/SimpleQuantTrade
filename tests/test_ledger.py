from __future__ import annotations

import unittest

from qtbot.ledger import compute_buy_update, compute_sell_update


class LedgerTests(unittest.TestCase):
    def test_compute_buy_update_capitalizes_fee(self) -> None:
        update = compute_buy_update(
            current_qty=1.0,
            current_avg_entry_price=100.0,
            fill_qty=1.0,
            fill_price=120.0,
            fee_cad=1.2,
        )
        self.assertAlmostEqual(update.new_qty, 2.0)
        self.assertAlmostEqual(update.new_avg_entry_price, 110.6)
        self.assertAlmostEqual(update.cash_delta_cad, -121.2)
        self.assertAlmostEqual(update.fee_delta_cad, 1.2)
        self.assertAlmostEqual(update.realized_pnl_delta_cad, 0.0)

    def test_compute_sell_update_partial_position(self) -> None:
        update = compute_sell_update(
            current_qty=2.0,
            current_avg_entry_price=100.0,
            fill_qty=1.0,
            fill_price=110.0,
            fee_cad=0.44,
        )
        self.assertAlmostEqual(update.new_qty, 1.0)
        self.assertAlmostEqual(update.new_avg_entry_price, 100.0)
        self.assertAlmostEqual(update.cash_delta_cad, 109.56)
        self.assertAlmostEqual(update.fee_delta_cad, 0.44)
        self.assertAlmostEqual(update.realized_pnl_delta_cad, 9.56)

    def test_compute_sell_update_full_close_resets_cost_basis(self) -> None:
        update = compute_sell_update(
            current_qty=1.0,
            current_avg_entry_price=100.0,
            fill_qty=1.0,
            fill_price=90.0,
            fee_cad=0.36,
        )
        self.assertEqual(update.new_qty, 0.0)
        self.assertEqual(update.new_avg_entry_price, 0.0)
        self.assertAlmostEqual(update.realized_pnl_delta_cad, -10.36)

    def test_compute_sell_update_validates_quantity(self) -> None:
        with self.assertRaises(ValueError):
            compute_sell_update(
                current_qty=1.0,
                current_avg_entry_price=100.0,
                fill_qty=2.0,
                fill_price=110.0,
                fee_cad=0.0,
            )


if __name__ == "__main__":
    unittest.main()
