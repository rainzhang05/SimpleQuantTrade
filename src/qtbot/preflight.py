"""Go-live preflight checks for M6 live-trading safety gate."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import logging
import math
import os
from pathlib import Path
import tempfile
from typing import Any

from qtbot.config import RuntimeConfig
from qtbot.control import Command
from qtbot.ndax_client import NdaxAuthenticationError, NdaxClient, NdaxError, load_credentials_from_env
from qtbot.state import StateStore
from qtbot.universe import UniverseEntry, resolve_tradable_universe


@dataclass(frozen=True)
class PreflightCheckResult:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class PreflightSummary:
    checks: list[PreflightCheckResult]
    message: str

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> list[PreflightCheckResult]:
        return [check for check in self.checks if not check.passed]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "message": self.message,
            "checks": [asdict(item) for item in self.checks],
        }


class GoLivePreflight:
    """Validates safety-critical dependencies before live order placement."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._state_store = state_store
        self._logger = logger

    def run(self) -> PreflightSummary:
        checks: list[PreflightCheckResult] = []
        checks.append(self._check_credentials_auth())

        instruments: list[dict[str, Any]] | None = None
        reachability = self._check_ndax_reachability()
        checks.append(reachability.check)
        if reachability.check.passed:
            instruments = reachability.instruments

        coverage = self._check_cad_market_coverage(instruments=instruments)
        checks.append(coverage.check)
        checks.append(
            self._check_candle_warmup_sufficiency(tradable=coverage.tradable, blocked=not coverage.check.passed)
        )
        checks.append(self._check_state_db_health())
        checks.append(self._check_control_file_integrity())

        if all(item.passed for item in checks):
            message = f"go_live_preflight_passed checks={len(checks)}"
        else:
            failed = "; ".join(
                f"{item.name}={item.detail}" for item in checks if not item.passed
            )
            message = f"go_live_preflight_failed {failed}"

        summary = PreflightSummary(checks=checks, message=message)
        self._record_summary_event(summary)
        return summary

    def _check_credentials_auth(self) -> PreflightCheckResult:
        try:
            credentials = load_credentials_from_env()
            self._ndax_client.authenticate(credentials=credentials)
        except (NdaxAuthenticationError, NdaxError, ValueError) as exc:
            return PreflightCheckResult(
                name="credentials_auth",
                passed=False,
                detail=f"failed: {exc}",
            )

        return PreflightCheckResult(
            name="credentials_auth",
            passed=True,
            detail="NDAX credentials authenticated successfully.",
        )

    def _check_ndax_reachability(self) -> "_ReachabilityResult":
        try:
            instruments = self._ndax_client.get_instruments()
        except NdaxError as exc:
            return _ReachabilityResult(
                check=PreflightCheckResult(
                    name="ndax_api_reachability",
                    passed=False,
                    detail=f"failed: {exc}",
                ),
                instruments=None,
            )

        if not instruments:
            return _ReachabilityResult(
                check=PreflightCheckResult(
                    name="ndax_api_reachability",
                    passed=False,
                    detail="failed: GetInstruments returned no rows.",
                ),
                instruments=None,
            )

        return _ReachabilityResult(
            check=PreflightCheckResult(
                name="ndax_api_reachability",
                passed=True,
                detail=f"reachable: instruments={len(instruments)}",
            ),
            instruments=instruments,
        )

    def _check_cad_market_coverage(
        self, *, instruments: list[dict[str, Any]] | None
    ) -> "_CoverageResult":
        if instruments is None:
            return _CoverageResult(
                check=PreflightCheckResult(
                    name="cad_market_coverage",
                    passed=False,
                    detail="blocked: ndax_api_reachability failed.",
                ),
                tradable=[],
            )

        resolution = resolve_tradable_universe(instruments)
        tradable_count = len(resolution.tradable)
        if tradable_count <= 0:
            return _CoverageResult(
                check=PreflightCheckResult(
                    name="cad_market_coverage",
                    passed=False,
                    detail="failed: no tradable CAD pairs for configured universe.",
                ),
                tradable=[],
            )

        return _CoverageResult(
            check=PreflightCheckResult(
                name="cad_market_coverage",
                passed=True,
                detail=(
                    f"tradable={tradable_count} skipped={len(resolution.skipped)} "
                    f"(locked/no_pair entries included)."
                ),
            ),
            tradable=resolution.tradable,
        )

    def _check_candle_warmup_sufficiency(
        self, *, tradable: list[UniverseEntry], blocked: bool
    ) -> PreflightCheckResult:
        if blocked:
            return PreflightCheckResult(
                name="candle_warmup_sufficiency",
                passed=False,
                detail="blocked: cad_market_coverage failed.",
            )

        required_candles = max(self._config.ema_slow_period, self._config.atr_period) + 1
        lookback_hours = _calculate_lookback_hours(
            ema_slow_period=self._config.ema_slow_period,
            atr_period=self._config.atr_period,
        )

        insufficient: list[str] = []
        for entry in tradable:
            try:
                candles = self._ndax_client.get_recent_ticker_history(
                    instrument_id=entry.instrument_id,
                    interval_seconds=self._config.signal_interval_seconds,
                    lookback_hours=lookback_hours,
                )
            except NdaxError as exc:
                insufficient.append(f"{entry.ndax_symbol}:ndax_error:{exc}")
                continue

            candle_count = _count_unique_candles(candles)
            if candle_count < required_candles:
                insufficient.append(
                    f"{entry.ndax_symbol}:candles={candle_count}<required={required_candles}"
                )

        total_symbols = len(tradable)
        warm_symbols = total_symbols - len(insufficient)
        coverage = (warm_symbols / total_symbols) if total_symbols > 0 else 0.0
        min_coverage = self._config.preflight_min_warmup_coverage
        if warm_symbols <= 0 or coverage + 1e-12 < min_coverage:
            detail = "; ".join(insufficient[:5])
            extra = len(insufficient) - 5
            if extra > 0:
                detail = f"{detail}; +{extra} more"
            return PreflightCheckResult(
                name="candle_warmup_sufficiency",
                passed=False,
                detail=(
                    f"failed: warm_symbols={warm_symbols}/{total_symbols} "
                    f"coverage={coverage:.3f}<required={min_coverage:.3f}; {detail}"
                ),
            )

        return PreflightCheckResult(
            name="candle_warmup_sufficiency",
            passed=True,
            detail=(
                f"warmup_ok warm_symbols={warm_symbols}/{total_symbols} coverage={coverage:.3f} "
                f"required_candles={required_candles} lookback_hours={lookback_hours}"
            ),
        )

    def _check_state_db_health(self) -> PreflightCheckResult:
        try:
            snapshot = self._state_store.get_snapshot()
            if snapshot is None:
                return PreflightCheckResult(
                    name="state_db_health",
                    passed=False,
                    detail="failed: bot state snapshot missing.",
                )
            self._state_store.add_event(
                event_type="PREFLIGHT_DB_HEALTHCHECK",
                detail="state db read/write check passed",
            )
        except Exception as exc:
            return PreflightCheckResult(
                name="state_db_health",
                passed=False,
                detail=f"failed: {exc}",
            )

        return PreflightCheckResult(
            name="state_db_health",
            passed=True,
            detail="state db read/write check passed.",
        )

    def _check_control_file_integrity(self) -> PreflightCheckResult:
        path = self._config.control_file
        try:
            _validate_control_file(path)
        except ValueError as exc:
            return PreflightCheckResult(
                name="control_file_integrity",
                passed=False,
                detail=f"failed: {exc}",
            )
        except OSError as exc:
            return PreflightCheckResult(
                name="control_file_integrity",
                passed=False,
                detail=f"failed: {exc}",
            )

        return PreflightCheckResult(
            name="control_file_integrity",
            passed=True,
            detail=f"control file path healthy: {path}",
        )

    def _record_summary_event(self, summary: PreflightSummary) -> None:
        for check in summary.checks:
            level = logging.INFO if check.passed else logging.ERROR
            self._logger.log(
                level,
                "Go-live preflight check name=%s passed=%s detail=%s",
                check.name,
                str(check.passed).lower(),
                check.detail,
            )

        try:
            event_type = "GO_LIVE_PREFLIGHT_PASSED" if summary.passed else "GO_LIVE_PREFLIGHT_FAILED"
            self._state_store.add_event(
                event_type=event_type,
                detail=summary.message,
            )
        except Exception as exc:  # pragma: no cover - defensive only
            self._logger.warning("Unable to persist go-live preflight summary event: %s", exc)


@dataclass(frozen=True)
class _ReachabilityResult:
    check: PreflightCheckResult
    instruments: list[dict[str, Any]] | None


@dataclass(frozen=True)
class _CoverageResult:
    check: PreflightCheckResult
    tradable: list[UniverseEntry]


def _calculate_lookback_hours(*, ema_slow_period: int, atr_period: int) -> int:
    needed_minutes = max(ema_slow_period, atr_period) + 120
    needed_hours = math.ceil(needed_minutes / 60)
    return max(12, needed_hours)


def _count_unique_candles(candles: list[list[Any]]) -> int:
    timestamps: set[int] = set()
    for row in candles:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            timestamps.add(int(row[0]))
        except (TypeError, ValueError):
            continue
    return len(timestamps)


def _validate_control_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _assert_directory_writable(path.parent)

    if not path.exists():
        return

    payload = json.loads(path.read_text(encoding="utf-8"))
    command = payload.get("command")
    valid_commands = {item.value for item in Command}
    if command not in valid_commands:
        raise ValueError(f"Invalid command in control file: {command!r}")


def _assert_directory_writable(path: Path) -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path,
            delete=False,
        ) as temp_file:
            temp_file.write("healthcheck\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
