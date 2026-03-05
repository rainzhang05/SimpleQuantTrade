from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from qtbot.runner import is_pid_alive, read_runner_pid


class RunnerHelperTests(unittest.TestCase):
    def test_read_runner_pid_handles_missing_and_invalid_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "runner.pid"
            self.assertIsNone(read_runner_pid(pid_path))

            pid_path.write_text("not-a-number", encoding="utf-8")
            self.assertIsNone(read_runner_pid(pid_path))

            pid_path.write_text("123\n", encoding="utf-8")
            self.assertEqual(read_runner_pid(pid_path), 123)

    def test_is_pid_alive_for_current_process(self) -> None:
        self.assertTrue(is_pid_alive(os.getpid()))
        self.assertFalse(is_pid_alive(-1))


if __name__ == "__main__":
    unittest.main()
