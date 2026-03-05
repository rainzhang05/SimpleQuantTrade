from __future__ import annotations

import io
from urllib.error import HTTPError, URLError
import unittest
from unittest import mock

from qtbot.alerts import DiscordAlerter


class _Response:
    def read(self) -> bytes:
        return b"ok"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class AlertsTests(unittest.TestCase):
    def _logger(self):
        return mock.Mock()

    def test_send_noop_when_webhook_missing(self) -> None:
        alerter = DiscordAlerter(
            webhook_url=None,
            timeout_seconds=5.0,
            max_retries=1,
            logger=self._logger(),
        )
        result = alerter.send(category="X", summary="y")
        self.assertFalse(result.delivered)
        self.assertEqual(result.reason, "webhook_not_configured")

    def test_send_success(self) -> None:
        alerter = DiscordAlerter(
            webhook_url="https://discord.example/webhook",
            timeout_seconds=5.0,
            max_retries=1,
            logger=self._logger(),
        )
        with mock.patch("qtbot.alerts.request.urlopen", return_value=_Response()) as m:
            result = alerter.send(
                category="LIFECYCLE",
                summary="stop transition",
                severity="WARN",
            )
        self.assertTrue(result.delivered)
        self.assertEqual(result.reason, "ok")
        m.assert_called_once()

    def test_send_retries_on_network_error(self) -> None:
        alerter = DiscordAlerter(
            webhook_url="https://discord.example/webhook",
            timeout_seconds=5.0,
            max_retries=1,
            logger=self._logger(),
        )
        with mock.patch(
            "qtbot.alerts.request.urlopen",
            side_effect=[URLError("down"), _Response()],
        ), mock.patch("qtbot.alerts.time.sleep"):
            result = alerter.send(
                category="RISK",
                summary="repeated failures",
            )
        self.assertTrue(result.delivered)

    def test_send_handles_http_error(self) -> None:
        alerter = DiscordAlerter(
            webhook_url="https://discord.example/webhook",
            timeout_seconds=5.0,
            max_retries=0,
            logger=self._logger(),
        )
        http_400 = HTTPError("u", 400, "bad", None, io.BytesIO(b"bad payload"))
        with mock.patch("qtbot.alerts.request.urlopen", side_effect=http_400):
            result = alerter.send(
                category="RISK",
                summary="slippage breach",
            )
        http_400.close()
        self.assertFalse(result.delivered)
        self.assertEqual(result.reason, "http_400")


if __name__ == "__main__":
    unittest.main()
