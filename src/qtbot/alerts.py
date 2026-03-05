"""Discord alert delivery for M8 operational notifications."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import time
from urllib import error, request

DEFAULT_ALERT_USER_AGENT = "SimpleQuantTrade/0.1"


@dataclass(frozen=True)
class AlertResult:
    delivered: bool
    reason: str


class DiscordAlerter:
    """Non-blocking Discord webhook sender with retry/backoff."""

    def __init__(
        self,
        *,
        webhook_url: str | None,
        timeout_seconds: float,
        max_retries: int,
        logger: logging.Logger,
    ) -> None:
        self._webhook_url = webhook_url
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._logger = logger

    @property
    def enabled(self) -> bool:
        return bool(self._webhook_url)

    def send(
        self,
        *,
        category: str,
        summary: str,
        severity: str = "INFO",
        detail: str | None = None,
    ) -> AlertResult:
        if not self.enabled:
            return AlertResult(delivered=False, reason="webhook_not_configured")

        content = self._format_content(
            category=category,
            summary=summary,
            severity=severity,
            detail=detail,
        )
        payload = {"content": content}
        payload_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": DEFAULT_ALERT_USER_AGENT,
        }

        for attempt in range(self._max_retries + 1):
            try:
                req = request.Request(
                    url=self._webhook_url or "",
                    data=payload_bytes,
                    headers=headers,
                    method="POST",
                )
                with request.urlopen(req, timeout=self._timeout_seconds) as resp:
                    resp.read()
                return AlertResult(delivered=True, reason="ok")
            except error.HTTPError as exc:
                body = _read_http_error_body(exc)
                retry = exc.code >= 500 and attempt < self._max_retries
                if retry:
                    self._sleep_backoff(attempt)
                    continue
                self._logger.warning(
                    "Discord alert failed category=%s status=%s body=%s",
                    category,
                    exc.code,
                    body,
                )
                return AlertResult(delivered=False, reason=f"http_{exc.code}")
            except error.URLError as exc:
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                self._logger.warning(
                    "Discord alert network error category=%s detail=%s",
                    category,
                    exc,
                )
                return AlertResult(delivered=False, reason="network_error")
            except Exception as exc:  # pragma: no cover - defensive guard
                self._logger.warning(
                    "Discord alert unexpected error category=%s detail=%s",
                    category,
                    exc,
                )
                return AlertResult(delivered=False, reason="unexpected_error")

        return AlertResult(delivered=False, reason="retry_exhausted")

    def _sleep_backoff(self, attempt: int) -> None:
        base = 0.4 * (2**attempt)
        time.sleep(base)

    def _format_content(
        self,
        *,
        category: str,
        summary: str,
        severity: str,
        detail: str | None,
    ) -> str:
        lines = [f"[{severity}] [{category}] {summary}"]
        if detail:
            lines.append(detail)
        content = "\n".join(lines)
        if len(content) > 1900:
            return f"{content[:1897]}..."
        return content


def _read_http_error_body(exc: error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8")
    except Exception:
        return "<no-body>"
