from __future__ import annotations

import argparse
import unittest

from qtbot.cli import build_parser, positive_float


class CliTests(unittest.TestCase):
    def test_positive_float_validation(self) -> None:
        self.assertEqual(positive_float("1.5"), 1.5)
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_float("0")

    def test_parser_accepts_start_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["start", "--budget", "1000"])
        self.assertEqual(args.command, "start")
        self.assertEqual(args.budget, 1000.0)

    def test_parser_accepts_ndax_check_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["ndax-check", "--skip-balances", "--interval", "60"])
        self.assertEqual(args.command, "ndax-check")
        self.assertTrue(args.skip_balances)
        self.assertEqual(args.interval, 60)

    def test_parser_accepts_staging_validate_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "staging-validate",
                "--budget",
                "1200",
                "--cadence-seconds",
                "4",
                "--min-loops",
                "3",
                "--timeout-seconds",
                "90",
                "--offline-only",
            ]
        )
        self.assertEqual(args.command, "staging-validate")
        self.assertEqual(args.budget, 1200.0)
        self.assertEqual(args.cadence_seconds, 4)
        self.assertEqual(args.min_loops, 3)
        self.assertEqual(args.timeout_seconds, 90)
        self.assertTrue(args.offline_only)

    def test_parser_accepts_cutover_checklist_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "cutover-checklist",
                "--budget",
                "300",
                "--staging-max-age-hours",
                "72",
                "--offline-only",
                "--require-discord",
            ]
        )
        self.assertEqual(args.command, "cutover-checklist")
        self.assertEqual(args.budget, 300.0)
        self.assertEqual(args.staging_max_age_hours, 72)
        self.assertTrue(args.offline_only)
        self.assertTrue(args.require_discord)

    def test_parser_accepts_data_backfill_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "data-backfill",
                "--from",
                "2026-01-01",
                "--to",
                "2026-03-05",
                "--timeframe",
                "15m",
                "--sources",
                "ndax,kraken,binance",
            ]
        )
        self.assertEqual(args.command, "data-backfill")
        self.assertEqual(args.from_date, "2026-01-01")
        self.assertEqual(args.to_date, "2026-03-05")
        self.assertEqual(args.timeframe, "15m")
        self.assertEqual(args.sources, "ndax,kraken,binance")
        self.assertFalse(args.quiet)

    def test_parser_accepts_data_backfill_earliest_start(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "data-backfill",
                "--from",
                "earliest",
                "--to",
                "2026-03-05",
            ]
        )
        self.assertEqual(args.from_date, "earliest")

    def test_parser_accepts_data_backfill_quiet_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "data-backfill",
                "--from",
                "2026-01-01",
                "--to",
                "2026-03-05",
                "--quiet",
            ]
        )
        self.assertEqual(args.command, "data-backfill")
        self.assertTrue(args.quiet)

    def test_parser_accepts_data_status_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["data-status", "--timeframe", "15m", "--dataset", "combined"])
        self.assertEqual(args.command, "data-status")
        self.assertEqual(args.timeframe, "15m")
        self.assertEqual(args.dataset, "combined")

    def test_parser_accepts_data_build_combined_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "data-build-combined",
                "--from",
                "2026-01-01",
                "--to",
                "2026-03-05",
                "--timeframe",
                "15m",
            ]
        )
        self.assertEqual(args.command, "data-build-combined")
        self.assertEqual(args.from_date, "2026-01-01")
        self.assertEqual(args.to_date, "2026-03-05")
        self.assertEqual(args.timeframe, "15m")

    def test_parser_accepts_data_calibrate_weights_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "data-calibrate-weights",
                "--from",
                "2026-01-01",
                "--to",
                "2026-03-05",
                "--timeframe",
                "15m",
                "--refresh",
                "monthly",
            ]
        )
        self.assertEqual(args.command, "data-calibrate-weights")
        self.assertEqual(args.refresh, "monthly")

    def test_parser_accepts_data_weight_status_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["data-weight-status", "--timeframe", "15m"])
        self.assertEqual(args.command, "data-weight-status")
        self.assertEqual(args.timeframe, "15m")

    def test_parser_accepts_build_snapshot_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "build-snapshot",
                "--asof",
                "2026-03-05T12:00:00Z",
                "--timeframe",
                "15m",
            ]
        )
        self.assertEqual(args.command, "build-snapshot")
        self.assertEqual(args.asof, "2026-03-05T12:00:00Z")
        self.assertEqual(args.timeframe, "15m")

    def test_parser_accepts_train_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "train",
                "--snapshot",
                "20260305T120000Z_combined_15m_hash",
                "--folds",
                "6",
                "--universe",
                "V1",
            ]
        )
        self.assertEqual(args.command, "train")
        self.assertEqual(args.snapshot, "20260305T120000Z_combined_15m_hash")
        self.assertEqual(args.folds, 6)
        self.assertEqual(args.universe, "V1")

    def test_parser_accepts_eval_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "eval",
                "--run",
                "20260305T120000Z_run",
            ]
        )
        self.assertEqual(args.command, "eval")
        self.assertEqual(args.run, "20260305T120000Z_run")


if __name__ == "__main__":
    unittest.main()
