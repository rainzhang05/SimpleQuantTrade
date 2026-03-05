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


if __name__ == "__main__":
    unittest.main()
