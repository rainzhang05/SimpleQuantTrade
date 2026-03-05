"""NDAX REST client wrapper for public and private calls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import json
import random
import time
from typing import Any, Literal
from urllib import error, parse, request


class NdaxError(RuntimeError):
    """Base NDAX integration error."""


class NdaxAuthenticationError(NdaxError):
    """Raised when NDAX credentials are missing or rejected."""


class NdaxOrderRejectedError(NdaxError):
    """Raised when NDAX rejects order placement or reports a terminal failure."""


@dataclass(frozen=True)
class NdaxCredentials:
    api_key: str
    api_secret: str
    user_id: str
    username: str | None = None


@dataclass(frozen=True)
class NdaxBalance:
    product_symbol: str
    amount: float
    hold: float

    @property
    def available(self) -> float:
        return self.amount - self.hold


OrderSide = Literal["BUY", "SELL"]

SIDE_TO_NDAX: dict[OrderSide, int] = {"BUY": 0, "SELL": 1}
MARKET_ORDER_TYPE = 1
TIME_IN_FORCE_GTC = 1


@dataclass(frozen=True)
class NdaxOrderAcceptance:
    order_id: int
    client_order_id: int
    instrument_id: int
    side: OrderSide
    raw_status: str | None


@dataclass(frozen=True)
class NdaxOrderFill:
    order_id: int
    qty_executed: float
    avg_price: float
    order_state: str | None
    raw: dict[str, Any]


class NdaxClient:
    """Small NDAX API wrapper with retry logic."""

    def __init__(
        self,
        *,
        base_url: str,
        oms_id: int,
        timeout_seconds: float,
        max_retries: int,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._oms_id = oms_id
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    @property
    def oms_id(self) -> int:
        return self._oms_id

    def get_instruments(self) -> list[dict[str, Any]]:
        response = self._api_get("GetInstruments", params={"OMSId": self._oms_id})
        if not isinstance(response, list):
            raise NdaxError("GetInstruments returned non-list response.")
        return [item for item in response if isinstance(item, dict)]

    def get_ticker_history(
        self,
        *,
        instrument_id: int,
        interval_seconds: int,
        from_date: date,
        to_date: date,
    ) -> list[list[float]]:
        params = {
            "OMSId": self._oms_id,
            "InstrumentId": instrument_id,
            "Interval": interval_seconds,
            "FromDate": from_date.isoformat(),
            "ToDate": to_date.isoformat(),
        }
        response = self._api_get("GetTickerHistory", params=params)
        if not isinstance(response, list):
            raise NdaxError("GetTickerHistory returned non-list response.")
        candles: list[list[float]] = []
        for row in response:
            if isinstance(row, list) and len(row) >= 6:
                candles.append(row)
        return candles

    def get_recent_ticker_history(
        self,
        *,
        instrument_id: int,
        interval_seconds: int = 60,
        lookback_hours: int = 24,
    ) -> list[list[float]]:
        to_date = datetime.now(timezone.utc).date()
        from_date = to_date - timedelta(days=max(1, lookback_hours // 24 + 1))
        return self.get_ticker_history(
            instrument_id=instrument_id,
            interval_seconds=interval_seconds,
            from_date=from_date,
            to_date=to_date,
        )

    def get_user_accounts(self, *, credentials: NdaxCredentials) -> list[int]:
        params: dict[str, Any] = {
            "OMSId": self._oms_id,
            "UserId": int(credentials.user_id),
        }
        if credentials.username is not None:
            params["UserName"] = credentials.username
        response = self._api_get_private(
            "GetUserAccounts",
            params=params,
            credentials=credentials,
        )
        if not isinstance(response, list):
            raise NdaxError("GetUserAccounts returned non-list response.")

        account_ids: list[int] = []
        for value in response:
            account_id = _safe_int(value)
            if account_id is not None:
                account_ids.append(account_id)
        return account_ids

    def get_account_positions(
        self,
        *,
        credentials: NdaxCredentials,
        account_id: int,
    ) -> list[dict[str, Any]]:
        params = {"OMSId": self._oms_id, "AccountId": account_id}
        response = self._api_get_private(
            "GetAccountPositions",
            params=params,
            credentials=credentials,
        )
        if not isinstance(response, list):
            raise NdaxError("GetAccountPositions returned non-list response.")
        return [item for item in response if isinstance(item, dict)]

    def fetch_balances(self, *, credentials: NdaxCredentials) -> tuple[int, list[NdaxBalance]]:
        self.authenticate(credentials=credentials)
        account_ids = self.get_user_accounts(credentials=credentials)
        if not account_ids:
            raise NdaxError("No NDAX accounts returned for the authenticated user.")
        account_id = account_ids[0]
        positions = self.get_account_positions(credentials=credentials, account_id=account_id)

        balances: list[NdaxBalance] = []
        for position in positions:
            symbol = str(position.get("ProductSymbol", "")).upper()
            amount = _safe_float(position.get("Amount"))
            hold = _safe_float(position.get("Hold"))
            if not symbol:
                continue
            balances.append(NdaxBalance(product_symbol=symbol, amount=amount, hold=hold))
        return account_id, balances

    def send_market_order(
        self,
        *,
        credentials: NdaxCredentials,
        account_id: int,
        instrument_id: int,
        side: OrderSide,
        quantity: float,
        client_order_id: int,
    ) -> NdaxOrderAcceptance:
        if quantity <= 0:
            raise ValueError("quantity must be > 0 for market orders.")
        payload = {
            "OMSId": self._oms_id,
            "AccountId": account_id,
            "InstrumentId": instrument_id,
            "ClientOrderId": client_order_id,
            "Side": SIDE_TO_NDAX[side],
            "Quantity": quantity,
            "OrderType": MARKET_ORDER_TYPE,
            "TimeInForce": TIME_IN_FORCE_GTC,
            "UseDisplayQuantity": False,
        }
        response = self._api_post_private(
            "SendOrder",
            json_body=payload,
            credentials=credentials,
        )
        if not isinstance(response, dict):
            raise NdaxOrderRejectedError("NDAX SendOrder returned non-object response.")

        order_id = _safe_int(response.get("OrderId"))
        if order_id is None:
            detail = response.get("errormsg") or response.get("detail") or response
            raise NdaxOrderRejectedError(f"NDAX SendOrder did not return OrderId: {detail}")

        return NdaxOrderAcceptance(
            order_id=order_id,
            client_order_id=client_order_id,
            instrument_id=instrument_id,
            side=side,
            raw_status=_optional_str(response.get("OrderState") or response.get("Status")),
        )

    def get_order_status(
        self,
        *,
        credentials: NdaxCredentials,
        account_id: int,
        order_id: int,
    ) -> list[dict[str, Any]]:
        params = {
            "OMSId": self._oms_id,
            "AccountId": account_id,
            "OrderId": order_id,
        }
        response = self._api_get_private(
            "GetOrderStatus",
            params=params,
            credentials=credentials,
        )
        if isinstance(response, dict):
            return [response]
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        raise NdaxError("GetOrderStatus returned unsupported response type.")

    def wait_for_fill(
        self,
        *,
        credentials: NdaxCredentials,
        account_id: int,
        order_id: int,
        poll_seconds: float,
        max_attempts: int,
    ) -> NdaxOrderFill:
        if max_attempts <= 0:
            raise ValueError("max_attempts must be > 0.")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be > 0.")

        last_row: dict[str, Any] | None = None
        for attempt in range(max_attempts):
            rows = self.get_order_status(
                credentials=credentials,
                account_id=account_id,
                order_id=order_id,
            )
            row = _pick_order_row(rows=rows, order_id=order_id)
            if row is not None:
                last_row = row
                filled_qty = _safe_float(row.get("QuantityExecuted"))
                if filled_qty <= 0:
                    filled_qty = _safe_float(row.get("Quantity"))
                avg_price = _safe_float(row.get("AvgPrice"))
                if avg_price <= 0:
                    avg_price = _safe_float(row.get("Price"))
                state = _optional_str(row.get("OrderState"))
                if filled_qty > 0 and (avg_price > 0 or _is_terminal_state(state)):
                    return NdaxOrderFill(
                        order_id=order_id,
                        qty_executed=filled_qty,
                        avg_price=avg_price,
                        order_state=state,
                        raw=row,
                    )
                if _is_terminal_state(state):
                    detail = row.get("ErrorMessage") or row.get("RejectReason") or state
                    raise NdaxOrderRejectedError(
                        f"NDAX order {order_id} reached terminal state without fill: {detail}"
                    )
            if attempt < max_attempts - 1:
                time.sleep(poll_seconds)

        if last_row is not None:
            filled_qty = _safe_float(last_row.get("QuantityExecuted"))
            if filled_qty <= 0:
                filled_qty = _safe_float(last_row.get("Quantity"))
            avg_price = _safe_float(last_row.get("AvgPrice")) or _safe_float(last_row.get("Price"))
            state = _optional_str(last_row.get("OrderState"))
            if filled_qty > 0 and avg_price > 0:
                return NdaxOrderFill(
                    order_id=order_id,
                    qty_executed=filled_qty,
                    avg_price=avg_price,
                    order_state=state,
                    raw=last_row,
                )
            raise NdaxOrderRejectedError(
                f"NDAX order {order_id} was not filled after {max_attempts} polls. state={state}"
            )
        raise NdaxOrderRejectedError(
            f"NDAX order {order_id} status could not be confirmed after {max_attempts} polls."
        )

    def authenticate(self, *, credentials: NdaxCredentials) -> None:
        params = self._build_private_auth_params(credentials)
        payload = self._api_get("AuthenticateUser", params=params)
        if not isinstance(payload, dict):
            raise NdaxAuthenticationError("NDAX AuthenticateUser returned non-object response.")
        if not payload.get("Authenticated"):
            raise NdaxAuthenticationError(
                "NDAX AuthenticateUser rejected the provided API credentials."
            )

    def _api_get(self, endpoint: str, *, params: dict[str, Any]) -> Any:
        return self._request("GET", endpoint, params=params, headers=None, json_body=None)

    def _api_get_private(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        credentials: NdaxCredentials,
    ) -> Any:
        headers = self._build_private_headers(credentials)
        return self._request("GET", endpoint, params=params, headers=headers, json_body=None)

    def _api_post_private(
        self,
        endpoint: str,
        *,
        json_body: dict[str, Any],
        credentials: NdaxCredentials,
    ) -> Any:
        headers = self._build_private_headers(credentials)
        return self._request("POST", endpoint, params=None, headers=headers, json_body=json_body)

    def _build_private_headers(self, credentials: NdaxCredentials) -> dict[str, str]:
        auth = self._build_private_auth_params(credentials)
        return {
            "Nonce": auth["Nonce"],
            "APIKey": credentials.api_key,
            "Signature": auth["Signature"],
            "UserId": credentials.user_id,
        }

    def _build_private_auth_params(self, credentials: NdaxCredentials) -> dict[str, str]:
        nonce = str(int(time.time() * 1000))
        auth_payload = f"{nonce}{credentials.user_id}{credentials.api_key}".encode("utf-8")
        signature = hmac.new(
            credentials.api_secret.encode("utf-8"),
            auth_payload,
            hashlib.sha256,
        ).hexdigest()
        return {
            "Nonce": nonce,
            "APIKey": credentials.api_key,
            "Signature": signature,
            "UserId": credentials.user_id,
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
        json_body: dict[str, Any] | None,
    ) -> Any:
        query = parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{self._base_url}/{endpoint}"
        if query:
            url = f"{url}?{query}"

        merged_headers = {"Accept": "application/json"}
        if headers:
            merged_headers.update(headers)

        body_bytes = None
        if json_body is not None:
            merged_headers["Content-Type"] = "application/json"
            body_bytes = json.dumps(json_body).encode("utf-8")

        attempt = 0
        while True:
            try:
                req = request.Request(
                    url=url,
                    data=body_bytes,
                    headers=merged_headers,
                    method=method,
                )
                with request.urlopen(req, timeout=self._timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8")
            except error.HTTPError as exc:
                body = _read_http_error_body(exc)
                if exc.code == 404 and headers is not None:
                    raise NdaxAuthenticationError(
                        f"NDAX authentication failed for endpoint {endpoint}. "
                        "Check NDAX_API_KEY / NDAX_API_SECRET / NDAX_USER_ID / NDAX_USERNAME."
                    ) from exc
                if exc.code >= 500 and attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    attempt += 1
                    continue
                raise NdaxError(f"NDAX HTTP {exc.code} for {endpoint}: {body}") from exc
            except (error.URLError, TimeoutError) as exc:
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    attempt += 1
                    continue
                raise NdaxError(f"NDAX network failure for {endpoint}: {exc}") from exc

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise NdaxError(f"NDAX returned invalid JSON for {endpoint}: {raw[:120]}") from exc

            self._raise_if_api_error(endpoint=endpoint, payload=payload, private_call=headers is not None)
            return payload

    def _raise_if_api_error(self, *, endpoint: str, payload: Any, private_call: bool) -> None:
        if isinstance(payload, dict):
            if payload.get("Authenticated") is False and private_call:
                raise NdaxAuthenticationError(
                    f"NDAX authentication rejected for endpoint {endpoint}."
                )
            result = payload.get("result")
            if result is False:
                msg = payload.get("errormsg") or payload.get("detail") or "unknown error"
                raise NdaxError(f"NDAX call {endpoint} failed: {msg}")

    def _sleep_backoff(self, attempt: int) -> None:
        base = 0.5 * (2**attempt)
        jitter = random.uniform(0.0, 0.2)
        time.sleep(base + jitter)


def load_credentials_from_env() -> NdaxCredentials:
    import os

    api_key = os.getenv("NDAX_API_KEY", "").strip()
    api_secret = os.getenv("NDAX_API_SECRET", "").strip()
    user_id = os.getenv("NDAX_USER_ID", "").strip()
    username = os.getenv("NDAX_USERNAME", "").strip() or None
    if not api_key or not api_secret or not user_id:
        raise NdaxAuthenticationError(
            "Missing NDAX credentials. Required: NDAX_API_KEY, NDAX_API_SECRET, NDAX_USER_ID. "
            "Optional: NDAX_USERNAME."
        )
    return NdaxCredentials(
        api_key=api_key,
        api_secret=api_secret,
        user_id=user_id,
        username=username,
    )


def _read_http_error_body(exc: error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8")
    except Exception:
        return "<no-body>"


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _pick_order_row(*, rows: list[dict[str, Any]], order_id: int) -> dict[str, Any] | None:
    if not rows:
        return None
    for row in rows:
        row_order_id = _safe_int(row.get("OrderId"))
        if row_order_id == order_id:
            return row
    return rows[0]


def _is_terminal_state(raw_state: str | None) -> bool:
    if raw_state is None:
        return False
    normalized = raw_state.strip().lower()
    terminal_tokens = {
        "rejected",
        "canceled",
        "cancelled",
        "expired",
        "fullyexecuted",
        "filled",
        "5",
        "6",
        "7",
        "8",
    }
    return normalized in terminal_tokens
