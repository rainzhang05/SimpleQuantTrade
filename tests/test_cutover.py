from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timezone

from qtbot.cutover import (
    CutoverCheckResult,
    ProductionCutoverChecklist,
    _parse_utc_iso,
)
from tests._helpers import make_runtime_config


def _write_staging_report(
    runtime_dir: Path,
    *,
    completed_at_utc: str,
    offline_only: bool,
    passed: bool = True,
) -> None:
    if offline_only:
        steps = [
            {"name": "offline_control_plane_drill", "passed": True},
            {"name": "reconciliation_fault_simulation", "passed": True},
            {"name": "risk_fault_simulation", "passed": True},
        ]
    else:
        steps = [
            {"name": "public_ndax_health_check", "passed": True},
            {"name": "dry_run_lifecycle_drill", "passed": True},
            {"name": "cli_failure_scenario_invalid_symbol", "passed": True},
            {"name": "reconciliation_fault_simulation", "passed": True},
            {"name": "risk_fault_simulation", "passed": True},
        ]

    report = {
        "started_at_utc": "2026-03-05T00:00:00+00:00",
        "completed_at_utc": completed_at_utc,
        "passed": passed,
        "message": "staging_validation_passed",
        "steps": steps,
    }
    path = runtime_dir / "staging_validation" / "logs" / "staging_validation_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_runbook(root: Path, *, valid: bool = True) -> None:
    path = root / "PRODUCTION_RUNBOOK.md"
    if valid:
        content = (
            "# Production Runbook\n\n"
            "## Rollback Procedure\n"
            "- Stop bot\n\n"
            "## Incident Response\n"
            "- Capture logs\n"
        )
    else:
        content = "# Production Runbook\n\n## Notes\n- missing required sections\n"
    path.write_text(content, encoding="utf-8")


class ProductionCutoverChecklistTests(unittest.TestCase):
    def test_offline_cutover_checklist_passes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            _write_runbook(root, valid=True)
            _write_staging_report(
                cfg.runtime_dir,
                completed_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                offline_only=True,
            )
            checklist = ProductionCutoverChecklist(config=cfg, working_dir=root)
            report = checklist.run(
                start_budget_cad=250.0,
                staging_max_age_hours=48,
                offline_only=True,
                require_discord=False,
            )
            self.assertTrue(report.passed)
            self.assertTrue(report.report_file.exists())
            payload = json.loads(report.report_file.read_text(encoding="utf-8"))
            self.assertTrue(payload["passed"])

    def test_cutover_fails_when_staging_report_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            _write_runbook(root, valid=True)
            checklist = ProductionCutoverChecklist(config=cfg, working_dir=root)
            report = checklist.run(
                start_budget_cad=250.0,
                staging_max_age_hours=48,
                offline_only=True,
                require_discord=False,
            )
            self.assertFalse(report.passed)
            self.assertIn("staging_validation_report", report.message)

    def test_cutover_fails_when_report_too_old(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            _write_runbook(root, valid=True)
            _write_staging_report(
                cfg.runtime_dir,
                completed_at_utc="2000-01-01T00:00:00+00:00",
                offline_only=True,
            )
            checklist = ProductionCutoverChecklist(config=cfg, working_dir=root)
            report = checklist.run(
                start_budget_cad=250.0,
                staging_max_age_hours=24,
                offline_only=True,
                require_discord=False,
            )
            self.assertFalse(report.passed)
            self.assertIn("staging_validation_report", report.message)

    def test_cutover_fails_when_runbook_missing_required_sections(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root)
            _write_runbook(root, valid=False)
            _write_staging_report(
                cfg.runtime_dir,
                completed_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                offline_only=True,
            )
            checklist = ProductionCutoverChecklist(config=cfg, working_dir=root)
            report = checklist.run(
                start_budget_cad=250.0,
                staging_max_age_hours=48,
                offline_only=True,
                require_discord=False,
            )
            self.assertFalse(report.passed)
            self.assertIn("production_runbook_presence", report.message)

    def test_live_mode_discord_required_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = make_runtime_config(root, discord_webhook_url=None)
            _write_runbook(root, valid=True)
            _write_staging_report(
                cfg.runtime_dir,
                completed_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                offline_only=False,
            )
            checklist = ProductionCutoverChecklist(config=cfg, working_dir=root)
            with mock.patch.object(
                ProductionCutoverChecklist,
                "_check_credentials_presence",
                return_value=CutoverCheckResult("credentials_presence", True, "ok"),
            ), mock.patch.object(
                ProductionCutoverChecklist,
                "_check_ndax_private_connectivity",
                return_value=CutoverCheckResult("ndax_private_connectivity", True, "ok"),
            ), mock.patch.object(
                ProductionCutoverChecklist,
                "_check_preflight_gate",
                return_value=CutoverCheckResult("go_live_preflight_check", True, "ok"),
            ):
                report = checklist.run(
                    start_budget_cad=250.0,
                    staging_max_age_hours=48,
                    offline_only=False,
                    require_discord=True,
                )
            self.assertFalse(report.passed)
            self.assertIn("discord_alerting_configuration", report.message)

    def test_parse_utc_iso(self) -> None:
        self.assertIsNotNone(_parse_utc_iso("2026-03-05T00:00:00+00:00"))
        self.assertIsNotNone(_parse_utc_iso("2026-03-05T00:00:00"))
        self.assertIsNone(_parse_utc_iso(""))
        self.assertIsNone(_parse_utc_iso("not-a-date"))


if __name__ == "__main__":
    unittest.main()
