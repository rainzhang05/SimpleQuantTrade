from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest

from qtbot.state import StateStore


class StateStoreTests(unittest.TestCase):
    def test_initialize_and_status_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)
            snapshot = store.get_snapshot()
            assert snapshot is not None
            self.assertEqual(snapshot["initial_budget_cad"], 1000.0)
            self.assertEqual(snapshot["bot_cash_cad"], 1000.0)
            self.assertEqual(snapshot["realized_pnl_cad"], 0.0)
            self.assertEqual(snapshot["fees_paid_cad"], 0.0)

            store.set_status(run_status="RUNNING", last_command="RUN", event_detail="startup")
            loop_count = store.record_loop(
                last_command="RUN",
                loop_started_at_utc="2026-03-05T00:00:00+00:00",
                loop_completed_at_utc="2026-03-05T00:01:00+00:00",
                event_detail="cycle_ok",
            )
            self.assertEqual(loop_count, 1)
            snapshot = store.get_snapshot()
            assert snapshot is not None
            self.assertEqual(snapshot["loop_count"], 1)
            self.assertEqual(snapshot["last_event"], "cycle_ok")

    def test_initialize_rejects_mismatched_budget(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=500.0)
            with self.assertRaises(ValueError):
                store.initialize(initial_budget_cad=600.0)

    def test_apply_buy_and_sell_fills_update_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)

            store.apply_buy_fill(
                symbol="SOL",
                qty=2.0,
                avg_price=100.0,
                fee_cad=0.8,
                filled_at_utc="2026-03-05T00:00:00+00:00",
                order_id=1,
                ndax_symbol="SOLCAD",
            )
            after_buy = store.get_snapshot()
            assert after_buy is not None
            self.assertAlmostEqual(float(after_buy["bot_cash_cad"]), 799.2)
            self.assertAlmostEqual(float(after_buy["fees_paid_cad"]), 0.8)
            pos = store.get_positions()["SOL"]
            self.assertAlmostEqual(pos.qty, 2.0)
            self.assertAlmostEqual(pos.avg_entry_price, 100.4)

            store.apply_sell_fill(
                symbol="SOL",
                qty=1.0,
                avg_price=110.0,
                fee_cad=0.44,
                filled_at_utc="2026-03-05T01:00:00+00:00",
                order_id=2,
                ndax_symbol="SOLCAD",
            )
            mid = store.get_snapshot()
            assert mid is not None
            self.assertAlmostEqual(float(mid["bot_cash_cad"]), 908.76)
            self.assertAlmostEqual(float(mid["fees_paid_cad"]), 1.24)
            self.assertAlmostEqual(float(mid["realized_pnl_cad"]), 9.16)
            pos = store.get_positions()["SOL"]
            self.assertAlmostEqual(pos.qty, 1.0)
            self.assertAlmostEqual(pos.avg_entry_price, 100.4)

            store.apply_sell_fill(
                symbol="SOL",
                qty=1.0,
                avg_price=90.0,
                fee_cad=0.36,
                filled_at_utc="2026-03-05T02:00:00+00:00",
                order_id=3,
                ndax_symbol="SOLCAD",
            )
            after_all = store.get_snapshot()
            assert after_all is not None
            self.assertAlmostEqual(float(after_all["bot_cash_cad"]), 998.4)
            self.assertAlmostEqual(float(after_all["fees_paid_cad"]), 1.6)
            self.assertAlmostEqual(float(after_all["realized_pnl_cad"]), -1.6)
            pos = store.get_positions()["SOL"]
            self.assertEqual(pos.qty, 0.0)
            self.assertEqual(pos.last_exit_time, "2026-03-05T02:00:00+00:00")

    def test_apply_buy_fill_rejects_negative_cash(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=10.0)
            with self.assertRaises(ValueError):
                store.apply_buy_fill(
                    symbol="SOL",
                    qty=1.0,
                    avg_price=20.0,
                    fee_cad=0.1,
                    filled_at_utc="2026-03-05T00:00:00+00:00",
                    order_id=1,
                    ndax_symbol="SOLCAD",
                )

    def test_schema_migration_adds_new_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE bot_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    initial_budget_cad REAL NOT NULL,
                    bot_cash_cad REAL NOT NULL,
                    run_status TEXT NOT NULL,
                    last_command TEXT NOT NULL,
                    loop_count INTEGER NOT NULL DEFAULT 0,
                    last_loop_started_at_utc TEXT,
                    last_loop_completed_at_utc TEXT,
                    last_event TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO bot_state (
                    id, initial_budget_cad, bot_cash_cad, run_status, last_command,
                    loop_count, last_loop_started_at_utc, last_loop_completed_at_utc,
                    last_event, updated_at_utc
                ) VALUES (1, 1000.0, 1000.0, 'STOPPED', 'STOP', 0, NULL, NULL, 'legacy', '2026-03-05T00:00:00+00:00')
                """
            )
            conn.commit()
            conn.close()

            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)
            snapshot = store.get_snapshot()
            assert snapshot is not None
            self.assertIn("realized_pnl_cad", snapshot)
            self.assertIn("fees_paid_cad", snapshot)
            self.assertEqual(float(snapshot["realized_pnl_cad"]), 0.0)
            self.assertEqual(float(snapshot["fees_paid_cad"]), 0.0)

    def test_reconcile_position_and_cap_bot_cash(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)

            changed = store.reconcile_position(
                symbol="SOL",
                ndax_qty=2.0,
                reference_price=120.0,
                reconciled_at_utc="2026-03-05T00:00:00+00:00",
                reason="test",
            )
            self.assertTrue(changed)
            pos = store.get_positions()["SOL"]
            self.assertEqual(pos.qty, 2.0)
            self.assertEqual(pos.entry_time, "2026-03-05T00:00:00+00:00")
            self.assertEqual(pos.avg_entry_price, 120.0)

            unchanged = store.reconcile_position(
                symbol="SOL",
                ndax_qty=2.0,
                reference_price=130.0,
                reconciled_at_utc="2026-03-05T00:10:00+00:00",
                reason="test",
            )
            self.assertFalse(unchanged)

            closed = store.reconcile_position(
                symbol="SOL",
                ndax_qty=0.0,
                reference_price=None,
                reconciled_at_utc="2026-03-05T01:00:00+00:00",
                reason="test",
            )
            self.assertTrue(closed)
            pos = store.get_positions()["SOL"]
            self.assertEqual(pos.qty, 0.0)
            self.assertEqual(pos.last_exit_time, "2026-03-05T01:00:00+00:00")

            capped = store.cap_bot_cash(max_cash_cad=100.0, reason="test")
            self.assertTrue(capped)
            snapshot = store.get_snapshot()
            assert snapshot is not None
            self.assertEqual(float(snapshot["bot_cash_cad"]), 100.0)

            capped_again = store.cap_bot_cash(max_cash_cad=200.0, reason="test")
            self.assertFalse(capped_again)

    def test_risk_state_daily_pnl_and_error_counters(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)

            day1 = datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc)
            self.assertEqual(store.get_daily_realized_pnl(now_utc=day1), 0.0)
            self.assertEqual(store.get_consecutive_error_count(now_utc=day1), 0)

            count = store.increment_consecutive_errors(
                now_utc=day1,
                by_count=2,
                reason="test",
            )
            self.assertEqual(count, 2)
            self.assertEqual(store.get_consecutive_error_count(now_utc=day1), 2)
            self.assertTrue(store.reset_consecutive_errors(now_utc=day1, reason="clear"))
            self.assertEqual(store.get_consecutive_error_count(now_utc=day1), 0)

            store.apply_buy_fill(
                symbol="SOL",
                qty=1.0,
                avg_price=100.0,
                fee_cad=0.4,
                filled_at_utc="2026-03-05T00:00:00+00:00",
                order_id=1,
                ndax_symbol="SOLCAD",
            )
            store.apply_sell_fill(
                symbol="SOL",
                qty=1.0,
                avg_price=120.0,
                fee_cad=0.48,
                filled_at_utc="2026-03-05T01:00:00+00:00",
                order_id=2,
                ndax_symbol="SOLCAD",
            )
            self.assertGreater(store.get_daily_realized_pnl(now_utc=day1), 0.0)

            day2 = datetime(2026, 3, 6, 0, 0, tzinfo=timezone.utc)
            self.assertEqual(store.get_daily_realized_pnl(now_utc=day2), 0.0)

    def test_dual_source_data_tables_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.sqlite"
            store = StateStore(db_path)
            store.initialize(initial_budget_cad=1000.0)

            store.upsert_data_sync_checkpoint(
                source="ndax",
                symbol="BTCCAD",
                timeframe="15m",
                requested_from="2026-01-01",
                requested_to="2026-01-31",
                last_success_ts=1735689600000,
                status="ok",
            )
            checkpoints = store.get_data_sync_checkpoints(source="ndax", timeframe="15m")
            self.assertEqual(len(checkpoints), 1)
            self.assertEqual(checkpoints[0]["symbol"], "BTCCAD")

            store.upsert_data_coverage_v2(
                dataset="combined",
                symbol="BTCCAD",
                timeframe="15m",
                first_ts=1735689600000,
                last_ts=1735776000000,
                row_count=96,
                gap_count=0,
                duplicate_count=0,
                misaligned_count=0,
                coverage_pct=1.0,
                ndax_share=0.7,
                synth_share=0.3,
            )
            coverage_rows = store.get_data_coverage_v2(dataset="combined", timeframe="15m")
            self.assertEqual(len(coverage_rows), 1)
            self.assertEqual(coverage_rows[0]["symbol"], "BTCCAD")

            store.insert_conversion_quality(
                symbol="BTCCAD",
                timeframe="15m",
                period_start="2026-01-01",
                period_end="2026-01-31",
                overlap_rows=1200,
                median_ape_close=0.01,
                median_abs_ret_err=0.002,
                ret_corr=0.91,
                direction_match=0.73,
                basis_median=1.0,
                basis_mad=0.01,
                quality_pass=True,
            )
            quality_rows = store.get_conversion_quality(symbol="BTCCAD", timeframe="15m")
            self.assertEqual(len(quality_rows), 1)
            self.assertEqual(int(quality_rows[0]["quality_pass"]), 1)

            store.upsert_synthetic_weight(
                symbol="BTCCAD",
                timeframe="15m",
                effective_month="2026-01",
                weight_quality=0.62,
                weight_backtest=0.58,
                weight_final=0.60,
                overlap_rows=1200,
                quality_pass=True,
                method_version="v1",
                supervised_eligible=True,
                eligibility_mode="direct",
                anchor_month="2026-01",
            )
            weights = store.get_synthetic_weights(timeframe="15m")
            self.assertEqual(len(weights), 1)
            self.assertEqual(weights[0]["effective_month"], "2026-01")
            self.assertEqual(int(weights[0]["supervised_eligible"]), 1)
            self.assertEqual(weights[0]["eligibility_mode"], "direct")
            self.assertEqual(weights[0]["anchor_month"], "2026-01")

            store.insert_combined_build(
                symbol="BTCCAD",
                timeframe="15m",
                from_ts=1735689600000,
                to_ts=1735775100000,
                ndax_rows=95,
                binance_rows=96,
                combined_rows=96,
                gap_count=0,
                build_hash="abc123",
            )

            store.upsert_training_run(
                run_id="run123",
                snapshot_id="snap123",
                dataset_hash="datahash",
                feature_spec_hash="featurehash",
                seed=42,
                timeframe="15m",
                train_window_months=12,
                valid_window_months=1,
                train_step_months=1,
                folds_requested=2,
                folds_built=2,
                status="trained",
                artifact_dir="/tmp/run123",
                scenario_status={"weighted_combined": {"status": "trained"}},
                metrics_summary={"weighted_combined": {"global": {"net_return": 0.1}}},
            )
            training_run = store.get_training_run(run_id="run123")
            assert training_run is not None
            self.assertEqual(training_run["snapshot_id"], "snap123")
            self.assertEqual(training_run["scenario_status"]["weighted_combined"]["status"], "trained")

            store.upsert_training_fold(
                run_id="run123",
                fold_index=1,
                train_start_month="2025-01",
                train_end_month="2025-12",
                valid_start_month="2026-01",
                valid_end_month="2026-01",
                train_rows=1200,
                valid_rows=100,
                source_mix={"train_ndax": 800, "valid_synthetic": 20},
                per_coin_skip_reasons={"ETHCAD": {"weighted_combined": "train_rows_lt_1000"}},
                artifact_dir="/tmp/run123/fold_01",
                status="trained",
            )
            training_folds = store.get_training_folds(run_id="run123")
            self.assertEqual(len(training_folds), 1)
            self.assertEqual(training_folds[0]["source_mix"]["train_ndax"], 800)

            store.insert_fold_metric(
                run_id="run123",
                fold_index=1,
                scenario="weighted_combined",
                model_scope="global",
                split="all",
                row_count=100,
                trades=20,
                gross_return=0.4,
                net_return=0.3,
                win_rate=0.55,
                max_drawdown=0.1,
                logloss=0.62,
                roc_auc=0.71,
                pr_auc=0.69,
                brier=0.21,
            )
            fold_metrics = store.get_fold_metrics(run_id="run123")
            self.assertEqual(len(fold_metrics), 1)
            self.assertEqual(fold_metrics[0]["scenario"], "weighted_combined")


if __name__ == "__main__":
    unittest.main()
