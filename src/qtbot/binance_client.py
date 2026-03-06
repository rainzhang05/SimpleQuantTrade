"""Binance public REST client for deterministic spot kline retrieval."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import random
import time
from typing import Any
from urllib import error, parse, request


class BinanceError(RuntimeError):
    """Base Binance integration error."""


class BinanceClient:
    """Small Binance public API wrapper with retry logic."""

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

    def get_exchange_info(self) -> dict[str, Any]:
        payload = self._request(
            endpoint="/api/v3/exchangeInfo",
            params={},
        )
        if not isinstance(payload, dict):
            raise BinanceError("exchangeInfo returned non-object response.")
        return payload

    def list_spot_symbols(self) -> set[str]:
        payload = self.get_exchange_info()
        rows = payload.get("symbols")
        if not isinstance(rows, list):
            return set()
        symbols: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("status", "")).upper() != "TRADING":
                continue
            if str(row.get("isSpotTradingAllowed", "true")).lower() not in {"true", "1"}:
                continue
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                symbols.add(symbol)
        return symbols

    def get_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> list[list[Any]]:
        if start_time_ms < 0 or end_time_ms < 0:
            raise ValueError("start_time_ms and end_time_ms must be >= 0.")
        if end_time_ms < start_time_ms:
            return []
        if limit <= 0 or limit > 1000:
            raise ValueError("limit must be in [1, 1000].")

        payload = self._request(
            endpoint="/api/v3/klines",
            params={
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "limit": limit,
            },
        )
        if not isinstance(payload, list):
            raise BinanceError("klines returned non-list response.")
        rows: list[list[Any]] = []
        for row in payload:
            if isinstance(row, list) and len(row) >= 6:
                rows.append(row)
        return rows

    def _request(self, *, endpoint: str, params: dict[str, Any]) -> Any:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self._base_url}{endpoint}"
        if query:
            url = f"{url}?{query}"

        headers = {"Accept": "application/json"}
        attempt = 0
        while True:
            try:
                req = request.Request(
                    url=url,
                    headers=headers,
                    method="GET",
                )
                with request.urlopen(req, timeout=self._timeout_seconds) as response:
                    status_code = int(getattr(response, "status", 200))
                    body = response.read().decode("utf-8")
                if status_code >= 400:
                    raise BinanceError(
                        f"Binance GET failed status={status_code} endpoint={endpoint}"
                    )
                if not body.strip():
                    return None
                return json.loads(body)
            except error.HTTPError as exc:
                status = getattr(exc, "code", None)
                detail = _read_http_error_body(exc)
                should_retry = status in {408, 409, 425, 429, 500, 502, 503, 504}
                if not should_retry or attempt >= self._max_retries:
                    raise BinanceError(
                        f"Binance request failed endpoint={endpoint} status={status} detail={detail}"
                    ) from exc
            except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt >= self._max_retries:
                    raise BinanceError(f"Binance request failed endpoint={endpoint}: {exc}") from exc
            attempt += 1
            sleep_seconds = _retry_delay_seconds(attempt)
            time.sleep(sleep_seconds)


def _retry_delay_seconds(attempt: int) -> float:
    base = 0.25 * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0, 0.2)
    return min(3.0, base + jitter)


def _read_http_error_body(exc: error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8")
        if not body.strip():
            return "<empty>"
        return body
    except Exception:
        return f"<unavailable at {datetime.now(timezone.utc).isoformat()}>"
