from __future__ import annotations

from datetime import datetime, timezone
import unittest

import pandas as pd

from qtbot.training.folds import build_walk_forward_folds


def _ts_ms(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


class TrainingFoldTests(unittest.TestCase):
    def test_build_walk_forward_folds_is_stable(self) -> None:
        rows = pd.DataFrame(
            {
                "timestamp_ms": [
                    _ts_ms(2025, 1, 1, 0, 0),
                    _ts_ms(2026, 3, 31, 23, 45),
                ]
            }
        )
        first = build_walk_forward_folds(
            rows=rows,
            requested_folds=2,
            train_window_months=12,
            valid_window_months=1,
            step_months=1,
            interval_seconds=900,
        )
        second = build_walk_forward_folds(
            rows=rows,
            requested_folds=2,
            train_window_months=12,
            valid_window_months=1,
            step_months=1,
            interval_seconds=900,
        )

        self.assertEqual([item.to_payload() for item in first], [item.to_payload() for item in second])
        self.assertEqual(first[0].train_start_month, "2025-02")
        self.assertEqual(first[0].train_end_month, "2026-01")
        self.assertEqual(first[0].valid_start_month, "2026-02")
        self.assertEqual(first[1].train_start_month, "2025-03")
        self.assertEqual(first[1].valid_start_month, "2026-03")

    def test_build_walk_forward_folds_rejects_insufficient_history(self) -> None:
        rows = pd.DataFrame({"timestamp_ms": [_ts_ms(2026, 1, 1, 0, 0), _ts_ms(2026, 1, 31, 23, 45)]})
        with self.assertRaises(ValueError):
            build_walk_forward_folds(
                rows=rows,
                requested_folds=1,
                train_window_months=12,
                valid_window_months=1,
                step_months=1,
                interval_seconds=900,
            )


if __name__ == "__main__":
    unittest.main()
