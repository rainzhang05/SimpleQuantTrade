from __future__ import annotations

import unittest

from qtbot.universe import resolve_tradable_universe


class UniverseTests(unittest.TestCase):
    def test_resolve_tradable_universe_filters_locked_and_missing_pairs(self) -> None:
        instruments = [
            {"Product1Symbol": "BTC", "Product2Symbol": "CAD", "Symbol": "BTCCAD", "InstrumentId": 1},
            {"Product1Symbol": "ETH", "Product2Symbol": "CAD", "Symbol": "ETHCAD", "InstrumentId": 2},
            {"Product1Symbol": "SOL", "Product2Symbol": "CAD", "Symbol": "SOLCAD", "InstrumentId": 3},
            {"Product1Symbol": "ADA", "Product2Symbol": "USD", "Symbol": "ADAUSD", "InstrumentId": 4},
            {"Product1Symbol": "DOGE", "Product2Symbol": "CAD", "Symbol": "DOGECAD", "InstrumentId": 5},
        ]

        resolution = resolve_tradable_universe(instruments)
        tradable_symbols = {item.ndax_symbol for item in resolution.tradable}

        self.assertIn("SOLCAD", tradable_symbols)
        self.assertIn("DOGECAD", tradable_symbols)
        self.assertNotIn("BTCCAD", tradable_symbols)
        self.assertNotIn("ETHCAD", tradable_symbols)
        self.assertEqual(resolution.skipped["BTC"], "locked")
        self.assertEqual(resolution.skipped["ETH"], "locked")
        self.assertEqual(resolution.skipped["ADA"], "no_ndax_cad_pair")


if __name__ == "__main__":
    unittest.main()
