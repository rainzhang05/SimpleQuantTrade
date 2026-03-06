from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.config import load_runtime_config


@contextmanager
def pushd(path: Path):
    old = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


class RuntimeConfigTests(unittest.TestCase):
    def test_loads_values_from_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "QTBOT_RUNTIME_DIR=test_runtime",
                        "QTBOT_CADENCE_SECONDS=10",
                        "NDAX_BASE_URL=https://api.ndax.io/AP",
                        "NDAX_OMS_ID=3",
                        "QTBOT_ENABLE_LIVE_TRADING=true",
                        "QTBOT_TAKER_FEE_RATE=0.005",
                        "QTBOT_MIN_ORDER_NOTIONAL_CAD=40",
                        "QTBOT_ORDER_STATUS_POLL_SECONDS=1.5",
                        "QTBOT_ORDER_STATUS_MAX_ATTEMPTS=9",
                        "QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE=0.9",
                        "QTBOT_DAILY_LOSS_CAP_CAD=300",
                        "QTBOT_MAX_SLIPPAGE_PCT=0.03",
                        "QTBOT_CONSECUTIVE_ERROR_LIMIT=4",
                        "QTBOT_DISCORD_WEBHOOK_URL=https://discord.example/webhook",
                        "QTBOT_DISCORD_TIMEOUT_SECONDS=9",
                        "QTBOT_DISCORD_MAX_RETRIES=5",
                        "QTBOT_DATA_SOURCES=ndax,binance",
                        "QTBOT_DATASET_MODE=combined",
                        "QTBOT_BINANCE_BASE_URL=https://api.binance.com",
                        "QTBOT_BINANCE_QUOTE=USDT",
                        "QTBOT_BRIDGE_FX_SYMBOL=USDTCAD",
                        "QTBOT_SYNTH_WEIGHT_MIN=0.25",
                        "QTBOT_SYNTH_WEIGHT_MAX=0.75",
                        "QTBOT_SYNTH_WEIGHT_DEFAULT=0.55",
                        "QTBOT_SYNTH_WEIGHT_REFRESH=monthly",
                        "QTBOT_MIN_OVERLAP_ROWS_FOR_WEIGHT=1200",
                        "QTBOT_CONVERSION_MAX_MEDIAN_APE=0.02",
                        "QTBOT_COMBINED_MAX_GAP_COUNT=1",
                        "QTBOT_COMBINED_MIN_COVERAGE=0.98",
                    ]
                ),
                encoding="utf-8",
            )
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                cfg = load_runtime_config()

            self.assertEqual(cfg.cadence_seconds, 10)
            self.assertEqual(cfg.ndax_oms_id, 3)
            self.assertTrue(cfg.enable_live_trading)
            self.assertEqual(cfg.taker_fee_rate, 0.005)
            self.assertEqual(cfg.min_order_notional_cad, 40.0)
            self.assertEqual(cfg.order_status_poll_seconds, 1.5)
            self.assertEqual(cfg.order_status_max_attempts, 9)
            self.assertEqual(cfg.preflight_min_warmup_coverage, 0.9)
            self.assertEqual(cfg.daily_loss_cap_cad, 300.0)
            self.assertEqual(cfg.max_slippage_pct, 0.03)
            self.assertEqual(cfg.consecutive_error_limit, 4)
            self.assertEqual(cfg.discord_webhook_url, "https://discord.example/webhook")
            self.assertEqual(cfg.discord_timeout_seconds, 9.0)
            self.assertEqual(cfg.discord_max_retries, 5)
            self.assertEqual(cfg.data_sources, ("ndax", "binance"))
            self.assertEqual(cfg.dataset_mode, "combined")
            self.assertEqual(cfg.binance_base_url, "https://api.binance.com")
            self.assertEqual(cfg.binance_quote, "USDT")
            self.assertEqual(cfg.bridge_fx_symbol, "USDTCAD")
            self.assertEqual(cfg.synth_weight_min, 0.25)
            self.assertEqual(cfg.synth_weight_max, 0.75)
            self.assertEqual(cfg.synth_weight_default, 0.55)
            self.assertEqual(cfg.synth_weight_refresh, "monthly")
            self.assertEqual(cfg.min_overlap_rows_for_weight, 1200)
            self.assertEqual(cfg.conversion_max_median_ape, 0.02)
            self.assertEqual(cfg.combined_max_gap_count, 1)
            self.assertEqual(cfg.combined_min_coverage, 0.98)
            self.assertEqual(cfg.runtime_dir, (root / "test_runtime").resolve())

    def test_invalid_bool_raises_value_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("QTBOT_ENABLE_LIVE_TRADING=not_bool\n", encoding="utf-8")
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()

    def test_invalid_numeric_values_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("QTBOT_CADENCE_SECONDS=0\n", encoding="utf-8")
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()

    def test_invalid_preflight_coverage_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text("QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE=0\n", encoding="utf-8")
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()

    def test_invalid_risk_values_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text(
                "QTBOT_DAILY_LOSS_CAP_CAD=0\nQTBOT_MAX_SLIPPAGE_PCT=1\nQTBOT_CONSECUTIVE_ERROR_LIMIT=0\n",
                encoding="utf-8",
            )
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()

    def test_invalid_discord_values_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text(
                "QTBOT_DISCORD_TIMEOUT_SECONDS=0\nQTBOT_DISCORD_MAX_RETRIES=-1\n",
                encoding="utf-8",
            )
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()

    def test_invalid_dual_source_values_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".env").write_text(
                "\n".join(
                    [
                        "QTBOT_DATA_SOURCES=ndax,foo",
                        "QTBOT_DATASET_MODE=invalid",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with pushd(root), mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    load_runtime_config()


if __name__ == "__main__":
    unittest.main()
