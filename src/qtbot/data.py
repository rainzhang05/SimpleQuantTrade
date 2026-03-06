"""15m market data retrieval, persistence, and coverage reporting."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import logging
import math
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.config import RuntimeConfig
from qtbot.ndax_client import NdaxClient, NdaxError
from qtbot.state import StateStore
from qtbot.universe import UniverseEntry, resolve_tradable_universe


_CHUNK_DAYS = 30
_RESUME_OVERLAP_DAYS = 1
_PARQUET_COMPRESSION = "zstd"


def parse_timeframe_seconds(raw_value: str) -> int:
    value = raw_value.strip().lower()
    aliases = {
        "15m": 900,
        "900": 900,
        "900s": 900,
    }
    if value in aliases:
        return aliases[value]
    raise ValueError(f"Unsupported timeframe value: {raw_value!r}. Expected 15m.")


@dataclass(frozen=True)
class SymbolBackfillSummary:
    ticker: str
    symbol: str
    instrument_id: int
    status: str
    message: str
    resume_from: str
    requested_from: str
    requested_to: str
    chunk_count: int
    fetched_rows: int
    row_count: int
    rows_added: int
    first_ts: int | None
    last_ts: int | None
    gap_count: int


@dataclass(frozen=True)
class BackfillSummary:
    started_at_utc: str
    completed_at_utc: str
    timeframe: str
    interval_seconds: int
    requested_from: str
    requested_to: str
    mode: str
    symbols_total: int
    symbols_processed: int
    symbols_with_errors: int
    skipped_pairs: dict[str, str]
    symbols: list[SymbolBackfillSummary]

    def to_payload(self) -> dict[str, object]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "timeframe": self.timeframe,
            "interval_seconds": self.interval_seconds,
            "requested_from": self.requested_from,
            "requested_to": self.requested_to,
            "mode": self.mode,
            "symbols_total": self.symbols_total,
            "symbols_processed": self.symbols_processed,
            "symbols_with_errors": self.symbols_with_errors,
            "skipped_pairs": self.skipped_pairs,
            "symbols": [asdict(item) for item in self.symbols],
        }


@dataclass(frozen=True)
class SymbolCoverageSummary:
    symbol: str
    status: str
    row_count: int
    first_ts: int | None
    last_ts: int | None
    gap_count: int
    timeframe: str
    file_path: str
    note: str | None = None


@dataclass(frozen=True)
class DataStatusSummary:
    generated_at_utc: str
    timeframe: str
    interval_seconds: int
    mode: str
    symbols_total: int
    symbols_with_data: int
    symbols_without_data: int
    symbols_with_gaps: int
    skipped_pairs: dict[str, str]
    symbols: list[SymbolCoverageSummary]

    def to_payload(self) -> dict[str, object]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "timeframe": self.timeframe,
            "interval_seconds": self.interval_seconds,
            "mode": self.mode,
            "symbols_total": self.symbols_total,
            "symbols_with_data": self.symbols_with_data,
            "symbols_without_data": self.symbols_without_data,
            "symbols_with_gaps": self.symbols_with_gaps,
            "skipped_pairs": self.skipped_pairs,
            "symbols": [asdict(item) for item in self.symbols],
        }


class MarketDataService:
    """Backfill and status workflows for deterministic 15m market data storage."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        logger: logging.Logger | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._state_store = state_store
        self._logger = logger or logging.getLogger("qtbot.data")
        self._progress_callback = progress_callback

    def backfill(
        self,
        *,
        from_date: date,
        to_date: date,
        timeframe: str,
    ) -> BackfillSummary:
        if from_date > to_date:
            raise ValueError("from_date must be <= to_date.")

        interval_seconds = parse_timeframe_seconds(timeframe)
        started_at = _utc_now_iso()
        self._emit_progress(
            "data_backfill_started "
            f"requested_from={from_date.isoformat()} requested_to={to_date.isoformat()} "
            f"timeframe={timeframe}"
        )
        self._emit_progress("data_backfill_discovering_universe")
        instruments = self._ndax_client.get_instruments()
        resolution = resolve_tradable_universe(instruments)
        self._emit_progress(
            "data_backfill_universe_ready "
            f"symbols={len(resolution.tradable)} skipped={len(resolution.skipped)}"
        )

        symbol_summaries: list[SymbolBackfillSummary] = []
        errors = 0
        for entry in resolution.tradable:
            try:
                summary = self._backfill_symbol(
                    entry=entry,
                    from_date=from_date,
                    to_date=to_date,
                    interval_seconds=interval_seconds,
                    timeframe=timeframe,
                )
            except Exception as exc:
                errors += 1
                summary = SymbolBackfillSummary(
                    ticker=entry.ticker,
                    symbol=entry.ndax_symbol,
                    instrument_id=entry.instrument_id,
                    status="error",
                    message=f"backfill_failed: {exc}",
                    resume_from=from_date.isoformat(),
                    requested_from=from_date.isoformat(),
                    requested_to=to_date.isoformat(),
                    chunk_count=0,
                    fetched_rows=0,
                    row_count=0,
                    rows_added=0,
                    first_ts=None,
                    last_ts=None,
                    gap_count=0,
                )
                self._logger.exception("Backfill failed for symbol=%s", entry.ndax_symbol)
            symbol_summaries.append(summary)
            self._emit_progress(
                "symbol_backfill_complete "
                f"symbol={summary.symbol} status={summary.status} rows={summary.row_count} "
                f"rows_added={summary.rows_added} gaps={summary.gap_count}"
            )

        completed_at = _utc_now_iso()
        self._emit_progress(
            "data_backfill_completed "
            f"symbols_processed={len(symbol_summaries)} errors={errors}"
        )
        return BackfillSummary(
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            requested_from=from_date.isoformat(),
            requested_to=to_date.isoformat(),
            mode="ndax_public_candles",
            symbols_total=len(resolution.tradable),
            symbols_processed=len(symbol_summaries),
            symbols_with_errors=errors,
            skipped_pairs=resolution.skipped,
            symbols=symbol_summaries,
        )

    def data_status(self, *, timeframe: str) -> DataStatusSummary:
        interval_seconds = parse_timeframe_seconds(timeframe)
        generated_at = _utc_now_iso()

        symbols: list[SymbolCoverageSummary] = []
        skipped_pairs: dict[str, str] = {}
        mode = "ndax_universe"
        try:
            instruments = self._ndax_client.get_instruments()
            resolution = resolve_tradable_universe(instruments)
            skipped_pairs = resolution.skipped
            for entry in resolution.tradable:
                symbols.append(
                    self._coverage_for_symbol(
                        symbol=entry.ndax_symbol,
                        timeframe=timeframe,
                        interval_seconds=interval_seconds,
                    )
                )
            for ticker, reason in sorted(skipped_pairs.items()):
                symbols.append(
                    SymbolCoverageSummary(
                        symbol=ticker,
                        status="no_pair",
                        row_count=0,
                        first_ts=None,
                        last_ts=None,
                        gap_count=0,
                        timeframe=timeframe,
                        file_path="",
                        note=reason,
                    )
                )
        except NdaxError:
            mode = "offline_files_only"
            symbols = self._coverage_from_local_files(
                timeframe=timeframe,
                interval_seconds=interval_seconds,
            )
            skipped_pairs = {}

        symbols_with_data = sum(1 for item in symbols if item.row_count > 0)
        symbols_without_data = sum(1 for item in symbols if item.status in {"missing_file", "empty", "no_pair"})
        symbols_with_gaps = sum(1 for item in symbols if item.gap_count > 0)
        return DataStatusSummary(
            generated_at_utc=generated_at,
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            mode=mode,
            symbols_total=len(symbols),
            symbols_with_data=symbols_with_data,
            symbols_without_data=symbols_without_data,
            symbols_with_gaps=symbols_with_gaps,
            skipped_pairs=skipped_pairs,
            symbols=symbols,
        )

    def _backfill_symbol(
        self,
        *,
        entry: UniverseEntry,
        from_date: date,
        to_date: date,
        interval_seconds: int,
        timeframe: str,
    ) -> SymbolBackfillSummary:
        output_path = self._symbol_path(entry.ndax_symbol, interval_seconds=interval_seconds)
        requested_from = from_date.isoformat()
        requested_to = to_date.isoformat()

        existing = _read_parquet_records(output_path)
        old_count = len(existing)
        resume_from = _resolve_resume_from(
            existing=existing,
            requested_from=from_date,
            overlap_days=_RESUME_OVERLAP_DAYS,
        )

        chunk_count = 0
        fetched_rows = 0
        chunk_start = resume_from
        chunks_total = _chunk_count(from_date=resume_from, to_date=to_date)
        while chunk_start <= to_date:
            chunk_end = min(to_date, chunk_start + timedelta(days=_CHUNK_DAYS - 1))
            before_merge_count = len(existing)
            self._emit_progress(
                "symbol_chunk_fetch_start "
                f"symbol={entry.ndax_symbol} chunk={chunk_count + 1}/{chunks_total} "
                f"chunk_from={chunk_start.isoformat()} chunk_to={chunk_end.isoformat()}"
            )
            rows = self._ndax_client.get_ticker_history(
                instrument_id=entry.instrument_id,
                interval_seconds=interval_seconds,
                from_date=chunk_start,
                to_date=chunk_end,
            )
            chunk_count += 1
            fetched_rows += len(rows)

            parsed = _parse_candle_rows(
                rows=rows,
                symbol=entry.ndax_symbol,
                instrument_id=entry.instrument_id,
                interval_seconds=interval_seconds,
            )
            _merge_records(
                target=existing,
                incoming=parsed,
                from_date=from_date,
                to_date=to_date,
            )
            _write_parquet_records_atomic(output_path, existing)
            after_merge_count = len(existing)
            self._emit_progress(
                "symbol_chunk "
                f"symbol={entry.ndax_symbol} chunk={chunk_count}/{chunks_total} "
                f"chunk_from={chunk_start.isoformat()} chunk_to={chunk_end.isoformat()} "
                f"fetched_rows={len(rows)} merged_new={max(0, after_merge_count - before_merge_count)}"
            )
            chunk_start = chunk_end + timedelta(days=1)

        in_range = _records_in_date_range(existing, from_date=from_date, to_date=to_date)
        timestamps = sorted(in_range)
        first_ts = timestamps[0] if timestamps else None
        last_ts = timestamps[-1] if timestamps else None
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        row_count = len(timestamps)
        rows_added = max(0, len(existing) - old_count)

        self._state_store.upsert_data_coverage(
            symbol=entry.ndax_symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
        )

        status = "ok" if row_count > 0 else "empty"
        message = "backfill_complete" if row_count > 0 else "no_rows_returned_for_requested_range"
        return SymbolBackfillSummary(
            ticker=entry.ticker,
            symbol=entry.ndax_symbol,
            instrument_id=entry.instrument_id,
            status=status,
            message=message,
            resume_from=resume_from.isoformat(),
            requested_from=requested_from,
            requested_to=requested_to,
            chunk_count=chunk_count,
            fetched_rows=fetched_rows,
            row_count=row_count,
            rows_added=rows_added,
            first_ts=first_ts,
            last_ts=last_ts,
            gap_count=gap_count,
        )

    def _emit_progress(self, message: str) -> None:
        if self._progress_callback is not None:
            self._progress_callback(message)

    def _coverage_for_symbol(
        self,
        *,
        symbol: str,
        timeframe: str,
        interval_seconds: int,
    ) -> SymbolCoverageSummary:
        path = self._symbol_path(symbol, interval_seconds=interval_seconds)
        if not path.exists():
            self._state_store.upsert_data_coverage(
                symbol=symbol,
                timeframe=timeframe,
                first_ts=None,
                last_ts=None,
                row_count=0,
                gap_count=0,
            )
            return SymbolCoverageSummary(
                symbol=symbol,
                status="missing_file",
                row_count=0,
                first_ts=None,
                last_ts=None,
                gap_count=0,
                timeframe=timeframe,
                file_path=str(path),
            )

        records = _read_parquet_records(path)
        timestamps = sorted(records)
        if not timestamps:
            self._state_store.upsert_data_coverage(
                symbol=symbol,
                timeframe=timeframe,
                first_ts=None,
                last_ts=None,
                row_count=0,
                gap_count=0,
            )
            return SymbolCoverageSummary(
                symbol=symbol,
                status="empty",
                row_count=0,
                first_ts=None,
                last_ts=None,
                gap_count=0,
                timeframe=timeframe,
                file_path=str(path),
            )

        first_ts = timestamps[0]
        last_ts = timestamps[-1]
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        row_count = len(timestamps)
        self._state_store.upsert_data_coverage(
            symbol=symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
        )
        return SymbolCoverageSummary(
            symbol=symbol,
            status="ok",
            row_count=row_count,
            first_ts=first_ts,
            last_ts=last_ts,
            gap_count=gap_count,
            timeframe=timeframe,
            file_path=str(path),
        )

    def _coverage_from_local_files(
        self,
        *,
        timeframe: str,
        interval_seconds: int,
    ) -> list[SymbolCoverageSummary]:
        base = self._base_path(interval_seconds=interval_seconds)
        if not base.exists():
            return []
        symbols: list[SymbolCoverageSummary] = []
        for file_path in sorted(base.glob("*.parquet")):
            symbol = file_path.stem.upper()
            symbols.append(
                self._coverage_for_symbol(
                    symbol=symbol,
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                )
            )
        return symbols

    def _base_path(self, *, interval_seconds: int) -> Path:
        timeframe_dir = _timeframe_dir(interval_seconds)
        return self._config.runtime_dir.parent / "data" / "raw" / "ndax" / timeframe_dir

    def _symbol_path(self, symbol: str, *, interval_seconds: int) -> Path:
        base = self._base_path(interval_seconds=interval_seconds)
        return base / f"{symbol.upper()}.parquet"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timeframe_dir(interval_seconds: int) -> str:
    if interval_seconds == 900:
        return "15m"
    return f"{interval_seconds}s"


def _resolve_resume_from(*, existing: dict[int, dict[str, Any]], requested_from: date, overlap_days: int) -> date:
    if not existing:
        return requested_from
    last_ts = max(existing)
    last_date = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).date()
    overlap_from = last_date - timedelta(days=max(0, overlap_days))
    return max(requested_from, overlap_from)


def _parse_candle_rows(
    *,
    rows: list[list[Any]],
    symbol: str,
    instrument_id: int,
    interval_seconds: int,
) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    interval_ms = interval_seconds * 1000
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            raw_ts = int(row[0])
            high = float(row[1])
            low = float(row[2])
            open_price = float(row[3])
            close = float(row[4])
            volume = float(row[5])
            inside_bid = float(row[6]) if len(row) > 6 else 0.0
            inside_ask = float(row[7]) if len(row) > 7 else 0.0
        except (TypeError, ValueError):
            continue
        if raw_ts <= 0:
            continue
        if not all(math.isfinite(value) for value in (open_price, high, low, close, volume, inside_bid, inside_ask)):
            continue
        ts = raw_ts - (raw_ts % interval_ms)
        result[ts] = {
            "timestamp_ms": ts,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "inside_bid": inside_bid,
            "inside_ask": inside_ask,
            "instrument_id": int(instrument_id),
            "symbol": symbol.upper(),
            "interval_seconds": int(interval_seconds),
        }
    return result


def _merge_records(
    *,
    target: dict[int, dict[str, Any]],
    incoming: dict[int, dict[str, Any]],
    from_date: date,
    to_date: date,
) -> None:
    start_ms = int(datetime(from_date.year, from_date.month, from_date.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_exclusive_ms = int(
        datetime(to_date.year, to_date.month, to_date.day, tzinfo=timezone.utc).timestamp() * 1000
    ) + 86_400_000
    for ts, record in incoming.items():
        if ts < start_ms or ts >= end_exclusive_ms:
            continue
        target[ts] = record


def _records_in_date_range(
    records: dict[int, dict[str, Any]],
    *,
    from_date: date,
    to_date: date,
) -> dict[int, dict[str, Any]]:
    start_ms = int(datetime(from_date.year, from_date.month, from_date.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_exclusive_ms = int(
        datetime(to_date.year, to_date.month, to_date.day, tzinfo=timezone.utc).timestamp() * 1000
    ) + 86_400_000
    return {
        ts: record
        for ts, record in records.items()
        if start_ms <= ts < end_exclusive_ms
    }


def _count_gaps(*, timestamps: list[int], interval_seconds: int) -> int:
    if len(timestamps) <= 1:
        return 0
    step = interval_seconds * 1000
    gaps = 0
    for left, right in zip(timestamps, timestamps[1:]):
        diff = right - left
        if diff <= step:
            continue
        gaps += max(0, diff // step - 1)
    return gaps


def _chunk_count(*, from_date: date, to_date: date) -> int:
    if from_date > to_date:
        return 0
    total_days = (to_date - from_date).days + 1
    return (total_days + _CHUNK_DAYS - 1) // _CHUNK_DAYS


def _read_parquet_records(path: Path) -> dict[int, dict[str, Any]]:
    if not path.exists():
        return {}
    table = pq.read_table(path)
    data = table.to_pydict()
    timestamps = data.get("timestamp_ms", [])
    result: dict[int, dict[str, Any]] = {}
    for idx in range(len(timestamps)):
        ts = int(timestamps[idx])
        result[ts] = {
            "timestamp_ms": ts,
            "open": float(data["open"][idx]),
            "high": float(data["high"][idx]),
            "low": float(data["low"][idx]),
            "close": float(data["close"][idx]),
            "volume": float(data["volume"][idx]),
            "inside_bid": float(data["inside_bid"][idx]),
            "inside_ask": float(data["inside_ask"][idx]),
            "instrument_id": int(data["instrument_id"][idx]),
            "symbol": str(data["symbol"][idx]).upper(),
            "interval_seconds": int(data["interval_seconds"][idx]),
        }
    return result


def _write_parquet_records_atomic(path: Path, records: dict[int, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_ts = sorted(records)
    arrays: dict[str, list[Any]] = {
        "timestamp_ms": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "inside_bid": [],
        "inside_ask": [],
        "instrument_id": [],
        "symbol": [],
        "interval_seconds": [],
    }
    for ts in sorted_ts:
        row = records[ts]
        arrays["timestamp_ms"].append(int(row["timestamp_ms"]))
        arrays["open"].append(float(row["open"]))
        arrays["high"].append(float(row["high"]))
        arrays["low"].append(float(row["low"]))
        arrays["close"].append(float(row["close"]))
        arrays["volume"].append(float(row["volume"]))
        arrays["inside_bid"].append(float(row["inside_bid"]))
        arrays["inside_ask"].append(float(row["inside_ask"]))
        arrays["instrument_id"].append(int(row["instrument_id"]))
        arrays["symbol"].append(str(row["symbol"]).upper())
        arrays["interval_seconds"].append(int(row["interval_seconds"]))

    schema = pa.schema(
        [
            ("timestamp_ms", pa.int64()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("inside_bid", pa.float64()),
            ("inside_ask", pa.float64()),
            ("instrument_id", pa.int64()),
            ("symbol", pa.string()),
            ("interval_seconds", pa.int32()),
        ]
    )
    table = pa.Table.from_pydict(arrays, schema=schema)

    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        pq.write_table(table, tmp_path, compression=_PARQUET_COMPRESSION)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
