from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qtbot.env import load_dotenv


class LoadDotenvTests(unittest.TestCase):
    def test_loads_values_and_preserves_existing_env(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dotenv_path = Path(td) / ".env"
            dotenv_path.write_text(
                "\n".join(
                    [
                        "# comment",
                        "ALPHA=1",
                        "QUOTED=\"two words\"",
                        "SINGLE='three words'",
                        "SPACED = trimmed",
                        "EXISTING=from_file",
                        "NO_EQUALS",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"EXISTING": "from_os"}, clear=False):
                load_dotenv(dotenv_path)
                self.assertEqual(os.getenv("ALPHA"), "1")
                self.assertEqual(os.getenv("QUOTED"), "two words")
                self.assertEqual(os.getenv("SINGLE"), "three words")
                self.assertEqual(os.getenv("SPACED"), "trimmed")
                self.assertEqual(os.getenv("EXISTING"), "from_os")

    def test_missing_file_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            missing_path = Path(td) / "missing.env"
            with mock.patch.dict(os.environ, {}, clear=False):
                load_dotenv(missing_path)
                self.assertIsNone(os.getenv("MISSING_KEY"))


if __name__ == "__main__":
    unittest.main()
