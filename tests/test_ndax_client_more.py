from __future__ import annotations

import io
from urllib.error import HTTPError, URLError
import unittest
from unittest import mock

from qtbot.ndax_client import (
    NdaxAuthenticationError,
    NdaxClient,
    NdaxError,
    NdaxOrderRejectedError,
    NdaxCredentials,
)


class _Response:
    def __init__(self, text: str) -> None:
        self._text = text

    def read(self) -> bytes:
        return self._text.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class NdaxClientMoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = NdaxClient(
            base_url="https://api.ndax.io/AP",
            oms_id=1,
            timeout_seconds=2.0,
            max_retries=1,
        )
        self.creds = NdaxCredentials(api_key="k", api_secret="s", user_id="123")

    def test_get_instruments_and_ticker_history_validation(self) -> None:
        with mock.patch.object(self.client, "_api_get", return_value=[{"InstrumentId": 1}, "bad"]):
            instruments = self.client.get_instruments()
            self.assertEqual(instruments, [{"InstrumentId": 1}])

        with mock.patch.object(self.client, "_api_get", return_value=[[1, 2, 3, 4, 5, 6], [1, 2]]):
            candles = self.client.get_ticker_history(
                instrument_id=1,
                interval_seconds=60,
                from_date=__import__("datetime").date(2026, 3, 4),
                to_date=__import__("datetime").date(2026, 3, 5),
            )
            self.assertEqual(len(candles), 1)

    def test_account_and_balance_parsing(self) -> None:
        with mock.patch.object(self.client, "_api_get_private", return_value=["1", "bad", 2]):
            ids = self.client.get_user_accounts(credentials=self.creds)
            self.assertEqual(ids, [1, 2])

        with mock.patch.object(
            self.client,
            "_api_get_private",
            return_value=[{"ProductSymbol": "CAD", "Amount": "10", "Hold": "3"}],
        ):
            rows = self.client.get_account_positions(credentials=self.creds, account_id=1)
            self.assertEqual(len(rows), 1)

        with mock.patch.object(self.client, "authenticate"), mock.patch.object(
            self.client, "get_user_accounts", return_value=[9]
        ), mock.patch.object(
            self.client,
            "get_account_positions",
            return_value=[{"ProductSymbol": "CAD", "Amount": "100", "Hold": "5"}],
        ):
            account_id, balances = self.client.fetch_balances(credentials=self.creds)
            self.assertEqual(account_id, 9)
            self.assertEqual(balances[0].available, 95.0)

    def test_fetch_balances_requires_account(self) -> None:
        with mock.patch.object(self.client, "authenticate"), mock.patch.object(
            self.client, "get_user_accounts", return_value=[]
        ):
            with self.assertRaises(NdaxError):
                self.client.fetch_balances(credentials=self.creds)

    def test_get_order_status_accepts_dict_or_list(self) -> None:
        with mock.patch.object(self.client, "_api_get_private", return_value={"OrderId": 1}):
            rows = self.client.get_order_status(credentials=self.creds, account_id=1, order_id=1)
            self.assertEqual(rows, [{"OrderId": 1}])
        with mock.patch.object(self.client, "_api_get_private", return_value=[{"OrderId": 1}, "x"]):
            rows = self.client.get_order_status(credentials=self.creds, account_id=1, order_id=1)
            self.assertEqual(rows, [{"OrderId": 1}])
        with mock.patch.object(self.client, "_api_get_private", return_value="bad"):
            with self.assertRaises(NdaxError):
                self.client.get_order_status(credentials=self.creds, account_id=1, order_id=1)

    def test_wait_for_fill_argument_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.client.wait_for_fill(
                credentials=self.creds,
                account_id=1,
                order_id=1,
                poll_seconds=0,
                max_attempts=1,
            )
        with self.assertRaises(ValueError):
            self.client.wait_for_fill(
                credentials=self.creds,
                account_id=1,
                order_id=1,
                poll_seconds=1,
                max_attempts=0,
            )

    def test_wait_for_fill_timeout_raises(self) -> None:
        with mock.patch.object(
            self.client,
            "get_order_status",
            return_value=[{"OrderId": 1, "QuantityExecuted": 0, "AvgPrice": 0, "OrderState": "Working"}],
        ), mock.patch("qtbot.ndax_client.time.sleep"):
            with self.assertRaises(NdaxOrderRejectedError):
                self.client.wait_for_fill(
                    credentials=self.creds,
                    account_id=1,
                    order_id=1,
                    poll_seconds=0.01,
                    max_attempts=2,
                )

    def test_request_success_and_error_paths(self) -> None:
        with mock.patch("qtbot.ndax_client.request.urlopen", return_value=_Response("{\"ok\": true}")):
            payload = self.client._request("GET", "X", params={}, headers=None, json_body=None)
            self.assertEqual(payload["ok"], True)

        http_500 = HTTPError("u", 500, "err", None, io.BytesIO(b"oops"))
        with mock.patch(
            "qtbot.ndax_client.request.urlopen",
            side_effect=[http_500, _Response("{\"ok\": true}")],
        ), mock.patch.object(self.client, "_sleep_backoff"):
            payload = self.client._request("GET", "X", params={}, headers=None, json_body=None)
            self.assertEqual(payload["ok"], True)

        http_404 = HTTPError("u", 404, "err", None, io.BytesIO(b"missing"))
        with mock.patch("qtbot.ndax_client.request.urlopen", side_effect=http_404):
            with self.assertRaises(NdaxAuthenticationError):
                self.client._request("GET", "X", params={}, headers={"APIKey": "k"}, json_body=None)

        with mock.patch("qtbot.ndax_client.request.urlopen", return_value=_Response("not-json")):
            with self.assertRaises(NdaxError):
                self.client._request("GET", "X", params={}, headers=None, json_body=None)

        with mock.patch("qtbot.ndax_client.request.urlopen", side_effect=URLError("boom")), mock.patch.object(
            self.client, "_sleep_backoff"
        ):
            with self.assertRaises(NdaxError):
                self.client._request("GET", "X", params={}, headers=None, json_body=None)

    def test_raise_if_api_error_conditions(self) -> None:
        with self.assertRaises(NdaxAuthenticationError):
            self.client._raise_if_api_error(
                endpoint="X",
                payload={"Authenticated": False},
                private_call=True,
            )
        with self.assertRaises(NdaxError):
            self.client._raise_if_api_error(
                endpoint="X",
                payload={"result": False, "errormsg": "bad"},
                private_call=False,
            )


if __name__ == "__main__":
    unittest.main()
