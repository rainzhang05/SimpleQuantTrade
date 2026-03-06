from __future__ import annotations

import io
import json
import unittest
from unittest import mock
from urllib import error

from qtbot.kraken_client import KrakenClient, KrakenError


class _Response:
    def __init__(self, payload: object, status: int = 200) -> None:
        self.status = status
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class KrakenClientTests(unittest.TestCase):
    def test_get_trades_parses_rows_and_last_token(self) -> None:
        client = KrakenClient(base_url="https://api.kraken.com", timeout_seconds=5.0, max_retries=1)
        payload = {
            "error": [],
            "result": {
                "XXBTZUSD": [["100.0", "0.5", 1735689600.0, "b", "l", "", 1]],
                "last": "1735689600000000000",
            },
        }
        with mock.patch("qtbot.kraken_client.request.urlopen", return_value=_Response(payload)):
            rows, last_token = client.get_trades(pair="XBTUSD", since_ns=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(last_token or 0), 1735689600000000000)

    def test_get_trades_raises_on_error_envelope(self) -> None:
        client = KrakenClient(base_url="https://api.kraken.com", timeout_seconds=5.0, max_retries=0)
        payload = {"error": ["EQuery:Unknown asset pair"], "result": {}}
        with mock.patch("qtbot.kraken_client.request.urlopen", return_value=_Response(payload)):
            with self.assertRaises(KrakenError):
                client.get_trades(pair="NOPE")

    def test_retries_rate_limit_error_envelope_then_succeeds(self) -> None:
        client = KrakenClient(base_url="https://api.kraken.com", timeout_seconds=5.0, max_retries=1)
        limited = _Response({"error": ["EGeneral:Too many requests"], "result": {}})
        good = _Response({"error": [], "result": {"XXBTZUSD": [], "last": "1"}})
        with (
            mock.patch("qtbot.kraken_client.request.urlopen", side_effect=[limited, good]),
            mock.patch("qtbot.kraken_client.time.sleep") as sleep,
        ):
            rows, last_token = client.get_trades(pair="XBTUSD")
        self.assertEqual(rows, [])
        self.assertEqual(last_token, 1)
        self.assertGreaterEqual(sleep.call_count, 1)

    def test_retries_http_error_then_succeeds(self) -> None:
        client = KrakenClient(base_url="https://api.kraken.com", timeout_seconds=5.0, max_retries=1)
        good = _Response({"error": [], "result": {"XXBTZUSD": [], "last": "1"}})
        http_error = error.HTTPError(
            url="https://api.kraken.com/0/public/Trades",
            code=503,
            msg="busy",
            hdrs=None,
            fp=io.BytesIO(b"busy"),
        )
        with (
            mock.patch("qtbot.kraken_client.request.urlopen", side_effect=[http_error, good]),
            mock.patch("qtbot.kraken_client.time.sleep") as sleep,
        ):
            rows, last_token = client.get_trades(pair="XBTUSD")
        self.assertEqual(rows, [])
        self.assertEqual(last_token, 1)
        self.assertGreaterEqual(sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
