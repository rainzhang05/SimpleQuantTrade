from __future__ import annotations

import runpy
import unittest
from unittest import mock


class MainModuleTests(unittest.TestCase):
    def test_python_m_entrypoint_exits_with_cli_code(self) -> None:
        with mock.patch("qtbot.cli.main", return_value=7):
            with self.assertRaises(SystemExit) as ctx:
                runpy.run_module("qtbot.__main__", run_name="__main__")
        self.assertEqual(ctx.exception.code, 7)


if __name__ == "__main__":
    unittest.main()
