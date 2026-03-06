from __future__ import annotations

import unittest
from unittest import mock
from urllib import error

from qtbot.binance_client import (
    BinanceClient,
    BinanceError,
    _read_http_error_body,
    _retry_delay_seconds,
)


class _FakeResponse:
    def __init__(self, *, status: int, body: str) -> None:
        self.status = status
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


def _http_error(status: int, body: str) -> error.HTTPError:
    del body
    return error.HTTPError(
        url="https://api.binance.com/api/v3/test",
        code=status,
        msg="http error",
        hdrs=None,
        fp=None,
    )


class _BrokenReadError:
    def read(self) -> bytes:
        raise RuntimeError("boom")


class BinanceClientTests(unittest.TestCase):
    def _make_client(self, *, retries: int = 1) -> BinanceClient:
        return BinanceClient(
            base_url="https://api.binance.com/",
            timeout_seconds=1.0,
            max_retries=retries,
        )

    def test_get_exchange_info_requires_object(self) -> None:
        client = self._make_client()
        with mock.patch.object(client, "_request", return_value=[]):
            with self.assertRaises(BinanceError):
                client.get_exchange_info()

    def test_list_spot_symbols_filters_rows(self) -> None:
        client = self._make_client()
        payload = {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "isSpotTradingAllowed": True},
                {"symbol": "ETHUSDT", "status": "BREAK", "isSpotTradingAllowed": True},
                {"symbol": "SOLUSDT", "status": "TRADING", "isSpotTradingAllowed": False},
                {"symbol": "ADACAD", "status": "TRADING", "isSpotTradingAllowed": "1"},
                "not-a-dict",
            ]
        }
        with mock.patch.object(client, "get_exchange_info", return_value=payload):
            symbols = client.list_spot_symbols()
        self.assertEqual(symbols, {"BTCUSDT", "ADACAD"})

        with mock.patch.object(client, "get_exchange_info", return_value={"symbols": "invalid"}):
            self.assertEqual(client.list_spot_symbols(), set())

    def test_get_klines_validates_inputs_and_filters_rows(self) -> None:
        client = self._make_client()
        with self.assertRaises(ValueError):
            client.get_klines(symbol="BTCUSDT", interval="15m", start_time_ms=-1, end_time_ms=0)
        self.assertEqual(
            client.get_klines(symbol="BTCUSDT", interval="15m", start_time_ms=10, end_time_ms=9),
            [],
        )
        with self.assertRaises(ValueError):
            client.get_klines(symbol="BTCUSDT", interval="15m", start_time_ms=0, end_time_ms=10, limit=0)
        with self.assertRaises(ValueError):
            client.get_klines(symbol="BTCUSDT", interval="15m", start_time_ms=0, end_time_ms=10, limit=1001)

        with mock.patch.object(client, "_request", return_value={"bad": "payload"}):
            with self.assertRaises(BinanceError):
                client.get_klines(symbol="BTCUSDT", interval="15m", start_time_ms=0, end_time_ms=10)

        with mock.patch.object(
            client,
            "_request",
            return_value=[
                [1, 2, 3, 4, 5, 6],
                [1, 2, 3, 4, 5],
                "bad-row",
            ],
        ):
            rows = client.get_klines(symbol="btcusdt", interval="15m", start_time_ms=0, end_time_ms=10)
        self.assertEqual(rows, [[1, 2, 3, 4, 5, 6]])

    def test_request_success_builds_query(self) -> None:
        client = self._make_client()

        def _urlopen(req, timeout):  # noqa: ANN001
            self.assertIn("symbol=BTCUSDT", req.full_url)
            self.assertIn("interval=15m", req.full_url)
            self.assertEqual(timeout, 1.0)
            return _FakeResponse(status=200, body="[]")

        with mock.patch("qtbot.binance_client.request.urlopen", side_effect=_urlopen):
            rows = client.get_klines(
                symbol="BTCUSDT",
                interval="15m",
                start_time_ms=0,
                end_time_ms=1000,
            )
        self.assertEqual(rows, [])

    def test_request_empty_body_returns_none(self) -> None:
        client = self._make_client()
        with mock.patch(
            "qtbot.binance_client.request.urlopen",
            return_value=_FakeResponse(status=200, body="   "),
        ):
            payload = client._request(endpoint="/api/v3/exchangeInfo", params={})
        self.assertIsNone(payload)

    def test_request_retries_retryable_http_status_then_succeeds(self) -> None:
        client = self._make_client(retries=2)
        side_effect = [
            _http_error(429, "rate limit"),
            _FakeResponse(status=200, body='{"symbols": []}'),
        ]
        with (
            mock.patch("qtbot.binance_client.request.urlopen", side_effect=side_effect),
            mock.patch("qtbot.binance_client.time.sleep") as sleep,
        ):
            payload = client.get_exchange_info()
        self.assertEqual(payload, {"symbols": []})
        sleep.assert_called_once()

    def test_request_raises_on_non_retryable_http_status(self) -> None:
        client = self._make_client(retries=2)
        with mock.patch(
            "qtbot.binance_client.request.urlopen",
            side_effect=_http_error(404, "missing"),
        ):
            with self.assertRaises(BinanceError):
                client.get_exchange_info()

    def test_request_retries_urlerror_and_then_raises(self) -> None:
        client = self._make_client(retries=1)
        side_effect = [
            error.URLError("down"),
            error.URLError("still-down"),
        ]
        with (
            mock.patch("qtbot.binance_client.request.urlopen", side_effect=side_effect),
            mock.patch("qtbot.binance_client.time.sleep") as sleep,
        ):
            with self.assertRaises(BinanceError):
                client.get_exchange_info()
        self.assertEqual(sleep.call_count, 1)

    def test_request_retries_json_decode_error(self) -> None:
        client = self._make_client(retries=1)
        side_effect = [
            _FakeResponse(status=200, body="{bad-json"),
            _FakeResponse(status=200, body='{"symbols": []}'),
        ]
        with (
            mock.patch("qtbot.binance_client.request.urlopen", side_effect=side_effect),
            mock.patch("qtbot.binance_client.time.sleep") as sleep,
        ):
            payload = client.get_exchange_info()
        self.assertEqual(payload, {"symbols": []})
        self.assertEqual(sleep.call_count, 1)

    def test_retry_delay_and_error_body_helpers(self) -> None:
        with mock.patch("qtbot.binance_client.random.uniform", return_value=0.0):
            self.assertEqual(_retry_delay_seconds(1), 0.25)
            self.assertEqual(_retry_delay_seconds(100), 3.0)

        self.assertEqual(_read_http_error_body(_http_error(500, "")), "<empty>")
        text = _read_http_error_body(_BrokenReadError())  # type: ignore[arg-type]
        self.assertIn("<unavailable at", text)


if __name__ == "__main__":
    unittest.main()
