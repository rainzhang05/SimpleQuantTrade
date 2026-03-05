from __future__ import annotations

import unittest

from qtbot.strategy.indicators import atr, ema, latest_snapshot


class IndicatorTests(unittest.TestCase):
    def test_ema_values(self) -> None:
        values = ema([1.0, 2.0, 3.0], period=2)
        self.assertEqual(len(values), 3)
        self.assertAlmostEqual(values[0], 1.0)
        self.assertAlmostEqual(values[1], 1.6666666667, places=6)
        self.assertAlmostEqual(values[2], 2.5555555556, places=6)

    def test_atr_values(self) -> None:
        highs = [10.0, 12.0, 14.0]
        lows = [8.0, 10.0, 12.0]
        closes = [9.0, 11.0, 13.0]
        values = atr(highs, lows, closes, period=2)
        self.assertEqual(len(values), 3)
        self.assertAlmostEqual(values[0], 2.0)
        self.assertAlmostEqual(values[1], 2.5)
        self.assertAlmostEqual(values[2], 2.75)

    def test_latest_snapshot_requires_warmup(self) -> None:
        snapshot = latest_snapshot(
            highs=[1.0, 2.0],
            lows=[0.5, 1.5],
            closes=[0.8, 1.8],
            ema_fast_period=3,
            ema_slow_period=3,
            atr_period=3,
        )
        self.assertIsNone(snapshot)

    def test_latest_snapshot_returns_indicator_values(self) -> None:
        snapshot = latest_snapshot(
            highs=[10, 11, 12, 13, 14],
            lows=[9, 10, 11, 12, 13],
            closes=[9.5, 10.5, 11.5, 12.5, 13.5],
            ema_fast_period=2,
            ema_slow_period=3,
            atr_period=2,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.close, 13.5)
        self.assertGreater(snapshot.ema_fast, 0)
        self.assertGreater(snapshot.ema_slow, 0)
        self.assertGreater(snapshot.atr, 0)


if __name__ == "__main__":
    unittest.main()
