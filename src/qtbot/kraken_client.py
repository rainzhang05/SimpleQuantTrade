"""Kraken public REST client for deterministic trade retrieval."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import random
import time
from typing import Any
from urllib import error, parse, request


class KrakenError(RuntimeError):
    """Base Kraken integration error."""


class KrakenClient:
    """Small Kraken public API wrapper with retry logic."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        max_retries: int,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._last_request_started_at = 0.0

    def get_asset_pairs(self) -> dict[str, Any]:
        payload = self._request(endpoint="/0/public/AssetPairs", params={})
        if not isinstance(payload, dict):
            raise KrakenError("AssetPairs returned non-object response.")
        return payload

    def get_trades(self, *, pair: str, since_ns: int | None = None) -> tuple[list[list[Any]], int | None]:
        params: dict[str, Any] = {"pair": pair}
        if since_ns is not None:
            if since_ns < 0:
                raise ValueError("since_ns must be >= 0.")
            params["since"] = int(since_ns)

        payload = self._request(endpoint="/0/public/Trades", params=params)
        if not isinstance(payload, dict):
            raise KrakenError("Trades returned non-object response.")

        last_token = payload.get("last")
        last_value: int | None = None
        if last_token is not None:
            try:
                last_value = int(last_token)
            except (TypeError, ValueError) as exc:
                raise KrakenError("Trades returned invalid last token.") from exc

        trades: list[list[Any]] = []
        for key, value in payload.items():
            if key == "last":
                continue
            if isinstance(value, list):
                for row in value:
                    if isinstance(row, list) and len(row) >= 3:
                        trades.append(row)

        return trades, last_value

    def _request(self, *, endpoint: str, params: dict[str, Any]) -> Any:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self._base_url}{endpoint}"
        if query:
            url = f"{url}?{query}"

        headers = {"Accept": "application/json"}
        attempt = 0
        while True:
            try:
                self._throttle_public_market_data()
                req = request.Request(url=url, headers=headers, method="GET")
                with request.urlopen(req, timeout=self._timeout_seconds) as response:
                    status_code = int(getattr(response, "status", 200))
                    body = response.read().decode("utf-8")
                if status_code >= 400:
                    raise KrakenError(f"Kraken GET failed status={status_code} endpoint={endpoint}")
                if not body.strip():
                    return None
                decoded = json.loads(body)
                if not isinstance(decoded, dict):
                    raise KrakenError("Kraken response envelope was not an object.")
                errors = decoded.get("error")
                if isinstance(errors, list) and errors:
                    retryable = any(_is_retryable_error(item) for item in errors)
                    if retryable and attempt < self._max_retries:
                        attempt += 1
                        time.sleep(_retry_delay_seconds(attempt, rate_limited=True))
                        continue
                    raise KrakenError(
                        f"Kraken request failed endpoint={endpoint} errors={','.join(str(item) for item in errors)}"
                    )
                return decoded.get("result")
            except error.HTTPError as exc:
                status = getattr(exc, "code", None)
                detail = _read_http_error_body(exc)
                should_retry = status in {408, 409, 425, 429, 500, 502, 503, 504}
                if not should_retry or attempt >= self._max_retries:
                    raise KrakenError(
                        f"Kraken request failed endpoint={endpoint} status={status} detail={detail}"
                    ) from exc
            except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt >= self._max_retries:
                    raise KrakenError(f"Kraken request failed endpoint={endpoint}: {exc}") from exc
            attempt += 1
            time.sleep(_retry_delay_seconds(attempt))

    def _throttle_public_market_data(self) -> None:
        now = time.monotonic()
        min_interval_seconds = 1.05
        wait_seconds = (self._last_request_started_at + min_interval_seconds) - now
        if wait_seconds > 0:
            time.sleep(wait_seconds)
            now = time.monotonic()
        self._last_request_started_at = now


def _retry_delay_seconds(attempt: int, *, rate_limited: bool = False) -> float:
    if rate_limited:
        base = 1.5 * (2 ** max(0, attempt - 1))
        jitter = random.uniform(0, 0.5)
        return min(20.0, base + jitter)
    base = 0.25 * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0, 0.2)
    return min(3.0, base + jitter)


def _is_retryable_error(item: object) -> bool:
    text = str(item).lower()
    return any(
        needle in text
        for needle in (
            "too many requests",
            "rate limit",
            "temporarily unavailable",
            "temporary lockout",
            "throttled",
            "busy",
        )
    )


def _read_http_error_body(exc: error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8")
        if not body.strip():
            return "<empty>"
        return body
    except Exception:
        return f"<unavailable at {datetime.now(timezone.utc).isoformat()}>"
