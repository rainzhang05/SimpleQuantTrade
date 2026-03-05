from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from qtbot.logging_setup import configure_logging


class LoggingSetupTests(unittest.TestCase):
    def test_configure_logging_creates_file_and_reuses_handlers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "logs" / "qtbot.log"
            logger = configure_logging(log_path)
            logger.info("hello-test")
            logger2 = configure_logging(log_path)
            self.assertIs(logger, logger2)
            text = log_path.read_text(encoding="utf-8")
            self.assertIn("hello-test", text)


if __name__ == "__main__":
    unittest.main()
