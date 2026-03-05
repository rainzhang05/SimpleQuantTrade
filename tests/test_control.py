from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qtbot.control import Command, read_control, write_control


class ControlTests(unittest.TestCase):
    def test_read_missing_control_defaults_to_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "control.json"
            state = read_control(control_path)
            self.assertEqual(state.command, Command.STOP)
            self.assertEqual(state.reason, "control file missing")

    def test_write_and_read_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "runtime" / "control.json"
            written = write_control(
                control_path,
                Command.PAUSE,
                updated_by="test",
                reason="pause",
            )
            self.assertEqual(written.command, Command.PAUSE)
            loaded = read_control(control_path)
            self.assertEqual(loaded.command, Command.PAUSE)
            self.assertEqual(loaded.updated_by, "test")
            self.assertEqual(loaded.reason, "pause")
            raw = json.loads(control_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["command"], "PAUSE")

    def test_invalid_control_file_is_treated_as_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "control.json"
            control_path.write_text("{not-json", encoding="utf-8")
            state = read_control(control_path)
            self.assertEqual(state.command, Command.STOP)
            self.assertEqual(state.reason, "invalid control file")

    def test_unknown_command_value_falls_back_to_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            control_path = Path(td) / "control.json"
            control_path.write_text(
                json.dumps(
                    {
                        "command": "NOT_A_COMMAND",
                        "updated_at_utc": "2026-03-05T00:00:00+00:00",
                        "updated_by": "test",
                        "reason": "invalid",
                    }
                ),
                encoding="utf-8",
            )
            state = read_control(control_path)
            self.assertEqual(state.command, Command.STOP)
            self.assertEqual(state.reason, "invalid")


if __name__ == "__main__":
    unittest.main()
