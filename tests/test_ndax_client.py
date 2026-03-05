from __future__ import annotations

import os
import unittest
from unittest import mock

from qtbot.ndax_client import (
    NdaxAuthenticationError,
    NdaxClient,
    NdaxCredentials,
    NdaxOrderRejectedError,
    _is_terminal_state,
    load_credentials_from_env,
)


class NdaxClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = NdaxClient(
            base_url="https://api.ndax.io/AP",
            oms_id=1,
            timeout_seconds=5.0,
            max_retries=1,
        )
        self.creds = NdaxCredentials(api_key="k", api_secret="s", user_id="123")

    def test_authenticate_uses_private_headers_path(self) -> None:
        with mock.patch.object(self.client, "_api_get_private", return_value={"Authenticated": True}) as m:
            self.client.authenticate(credentials=self.creds)
        m.assert_called_once_with("AuthenticateUser", params={}, credentials=self.creds)

    def test_authenticate_rejects_false_response(self) -> None:
        with mock.patch.object(self.client, "_api_get_private", return_value={"Authenticated": False}):
            with self.assertRaises(NdaxAuthenticationError):
                self.client.authenticate(credentials=self.creds)

    def test_send_market_order_validates_and_extracts_order_id(self) -> None:
        with mock.patch.object(self.client, "_api_post_private", return_value={"OrderId": 42, "OrderState": "Working"}):
            accepted = self.client.send_market_order(
                credentials=self.creds,
                account_id=5,
                instrument_id=99,
                side="BUY",
                quantity=1.5,
                client_order_id=12345,
            )
        self.assertEqual(accepted.order_id, 42)
        self.assertEqual(accepted.side, "BUY")

    def test_send_market_order_rejects_missing_order_id(self) -> None:
        with mock.patch.object(self.client, "_api_post_private", return_value={"result": True}):
            with self.assertRaises(NdaxOrderRejectedError):
                self.client.send_market_order(
                    credentials=self.creds,
                    account_id=5,
                    instrument_id=99,
                    side="SELL",
                    quantity=1.0,
                    client_order_id=12346,
                )

    def test_wait_for_fill_returns_when_executed(self) -> None:
        with mock.patch.object(
            self.client,
            "get_order_status",
            side_effect=[
                [{"OrderId": 7, "QuantityExecuted": 0, "AvgPrice": 0, "OrderState": "Working"}],
                [{"OrderId": 7, "QuantityExecuted": 2, "AvgPrice": 100, "OrderState": "FullyExecuted"}],
            ],
        ), mock.patch("qtbot.ndax_client.time.sleep"):
            fill = self.client.wait_for_fill(
                credentials=self.creds,
                account_id=5,
                order_id=7,
                poll_seconds=0.01,
                max_attempts=2,
            )
        self.assertEqual(fill.order_id, 7)
        self.assertEqual(fill.qty_executed, 2.0)
        self.assertEqual(fill.avg_price, 100.0)

    def test_wait_for_fill_raises_on_terminal_without_fill(self) -> None:
        with mock.patch.object(
            self.client,
            "get_order_status",
            return_value=[{"OrderId": 9, "QuantityExecuted": 0, "AvgPrice": 0, "OrderState": "Rejected"}],
        ):
            with self.assertRaises(NdaxOrderRejectedError):
                self.client.wait_for_fill(
                    credentials=self.creds,
                    account_id=5,
                    order_id=9,
                    poll_seconds=0.01,
                    max_attempts=1,
                )

    def test_load_credentials_from_env_validates_required_fields(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"NDAX_API_KEY": "a", "NDAX_API_SECRET": "b", "NDAX_USER_ID": "123"},
            clear=True,
        ):
            creds = load_credentials_from_env()
            self.assertEqual(creds.user_id, "123")

        with mock.patch.dict(os.environ, {"NDAX_API_KEY": "a"}, clear=True):
            with self.assertRaises(NdaxAuthenticationError):
                load_credentials_from_env()

    def test_is_terminal_state_helper(self) -> None:
        self.assertTrue(_is_terminal_state("FullyExecuted"))
        self.assertTrue(_is_terminal_state("7"))
        self.assertFalse(_is_terminal_state("Working"))


if __name__ == "__main__":
    unittest.main()
