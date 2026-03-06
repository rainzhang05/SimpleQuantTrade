"""15m market data retrieval, dual-source fusion, and calibration workflows."""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import statistics
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.binance_client import BinanceClient, BinanceError
from qtbot.config import RuntimeConfig
from qtbot.ndax_client import NdaxClient, NdaxError
from qtbot.state import StateStore
from qtbot.universe import UNIVERSE_V1_COINS, UniverseEntry, resolve_tradable_universe


_CHUNK_DAYS = 30
_PARQUET_COMPRESSION = "zstd"
_BINANCE_PAGE_LIMIT = 1000
_WEIGHT_METHOD_VERSION = "bridge_weight_v1"
_BINANCE_GAP_FILL_SOURCE = "binance_gap_fill"
_SYNTHETIC_GAP_FILL_SOURCE = "synthetic_gap_fill"
_SUPERVISED_ELIGIBILITY_MIN_OVERLAP_ROWS = 250


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
    source: str
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
    sources: list[str]
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
            "sources": list(self.sources),
            "symbols_total": self.symbols_total,
            "symbols_processed": self.symbols_processed,
            "symbols_with_errors": self.symbols_with_errors,
            "skipped_pairs": self.skipped_pairs,
            "symbols": [asdict(item) for item in self.symbols],
        }


@dataclass(frozen=True)
class SymbolCoverageSummary:
    dataset: str
    symbol: str
    status: str
    row_count: int
    first_ts: int | None
    last_ts: int | None
    gap_count: int
    duplicate_count: int
    misaligned_count: int
    coverage_pct: float
    ndax_share: float
    synth_share: float
    timeframe: str
    file_path: str
    note: str | None = None


@dataclass(frozen=True)
class DataStatusSummary:
    generated_at_utc: str
    timeframe: str
    interval_seconds: int
    dataset: str
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
            "dataset": self.dataset,
            "mode": self.mode,
            "symbols_total": self.symbols_total,
            "symbols_with_data": self.symbols_with_data,
            "symbols_without_data": self.symbols_without_data,
            "symbols_with_gaps": self.symbols_with_gaps,
            "skipped_pairs": self.skipped_pairs,
            "symbols": [asdict(item) for item in self.symbols],
        }


@dataclass(frozen=True)
class DataStatusAllSummary:
    generated_at_utc: str
    timeframe: str
    interval_seconds: int
    datasets: dict[str, DataStatusSummary]

    def to_payload(self) -> dict[str, object]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "timeframe": self.timeframe,
            "interval_seconds": self.interval_seconds,
            "dataset": "all",
            "datasets": {
                key: summary.to_payload()
                for key, summary in self.datasets.items()
            },
        }


@dataclass(frozen=True)
class CombinedSymbolSummary:
    symbol: str
    ticker: str
    status: str
    message: str
    ndax_rows: int
    binance_rows: int
    combined_rows: int
    gap_count: int
    ndax_share: float
    synth_share: float
    build_hash: str
    file_path: str


@dataclass(frozen=True)
class CombinedBuildSummary:
    started_at_utc: str
    completed_at_utc: str
    timeframe: str
    interval_seconds: int
    requested_from: str
    requested_to: str
    symbols_total: int
    symbols_built: int
    symbols_with_errors: int
    symbols: list[CombinedSymbolSummary]

    def to_payload(self) -> dict[str, object]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "timeframe": self.timeframe,
            "interval_seconds": self.interval_seconds,
            "requested_from": self.requested_from,
            "requested_to": self.requested_to,
            "symbols_total": self.symbols_total,
            "symbols_built": self.symbols_built,
            "symbols_with_errors": self.symbols_with_errors,
            "symbols": [asdict(item) for item in self.symbols],
        }


@dataclass(frozen=True)
class CalibrationWeightRow:
    symbol: str
    effective_month: str
    overlap_rows: int
    median_ape_close: float
    median_abs_ret_err: float
    ret_corr: float
    direction_match: float
    basis_median: float
    basis_mad: float
    quality_score: float
    quality_pass: bool
    weight_quality: float
    weight_backtest: float
    weight_final: float
    supervised_eligible: bool
    eligibility_mode: str
    anchor_month: str | None
    report_note: str


@dataclass(frozen=True)
class WeightCalibrationSummary:
    started_at_utc: str
    completed_at_utc: str
    run_id: str
    timeframe: str
    requested_from: str
    requested_to: str
    refresh: str
    symbols_total: int
    rows_total: int
    output_file: str
    rows: list[CalibrationWeightRow]

    def to_payload(self) -> dict[str, object]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "run_id": self.run_id,
            "timeframe": self.timeframe,
            "requested_from": self.requested_from,
            "requested_to": self.requested_to,
            "refresh": self.refresh,
            "symbols_total": self.symbols_total,
            "rows_total": self.rows_total,
            "output_file": self.output_file,
            "rows": [asdict(item) for item in self.rows],
        }


@dataclass(frozen=True)
class WeightStatusSummary:
    generated_at_utc: str
    timeframe: str
    row_count: int
    symbols: list[dict[str, object]]

    def to_payload(self) -> dict[str, object]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "timeframe": self.timeframe,
            "row_count": self.row_count,
            "symbols": self.symbols,
        }


@dataclass(frozen=True)
class _ReadResult:
    records: dict[int, dict[str, Any]]
    duplicate_count: int
    misaligned_count: int
    raw_row_count: int
    source_counts: dict[str, int]


@dataclass(frozen=True)
class _ConversionContext:
    ratio_by_month: dict[str, float]
    basis_by_month: dict[str, float]
    global_ratio: float | None
    global_basis: float | None
    fx_series: list[tuple[int, float]]
    fx_timestamps: list[int]


@dataclass(frozen=True)
class _OverlapMetrics:
    overlap_rows: int
    median_ape_close: float
    median_abs_ret_err: float
    ret_corr: float
    direction_match: float
    basis_median: float
    basis_mad: float
    quality_score: float
    quality_pass: bool
    ndax_returns: list[float]
    synth_returns: list[float]


class MarketDataService:
    """Dual-source deterministic market data workflows."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        ndax_client: NdaxClient,
        state_store: StateStore,
        binance_client: BinanceClient | None = None,
        logger: logging.Logger | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self._config = config
        self._ndax_client = ndax_client
        self._binance_client = binance_client
        self._state_store = state_store
        self._logger = logger or logging.getLogger("qtbot.data")
        self._progress_callback = progress_callback

    def backfill(
        self,
        *,
        from_date: date,
        to_date: date,
        timeframe: str,
        sources: Iterable[str] | None = None,
    ) -> BackfillSummary:
        if from_date > to_date:
            raise ValueError("from_date must be <= to_date.")

        interval_seconds = parse_timeframe_seconds(timeframe)
        source_list = _normalize_sources(sources or self._config.data_sources)

        started_at = _utc_now_iso()
        self._emit_progress(
            "data_backfill_started "
            f"requested_from={from_date.isoformat()} requested_to={to_date.isoformat()} "
            f"timeframe={timeframe} sources={','.join(source_list)}"
        )

        ndax_instruments: list[dict[str, Any]] = []
        ndax_resolution = None
        skipped_pairs: dict[str, str] = {}
        if "ndax" in source_list:
            self._emit_progress("data_backfill_discovering_ndax_universe")
            ndax_instruments = self._ndax_client.get_instruments()
            ndax_resolution = resolve_tradable_universe(ndax_instruments)
            skipped_pairs.update(ndax_resolution.skipped)
            self._emit_progress(
                "data_backfill_ndax_universe_ready "
                f"tradable={len(ndax_resolution.tradable)} skipped={len(ndax_resolution.skipped)}"
            )

        binance_pairs: dict[str, str] = {}
        if "binance" in source_list:
            binance_pairs, skipped_binance = self._resolve_binance_pairs()
            skipped_pairs.update({f"binance:{k}": v for k, v in skipped_binance.items()})
            self._emit_progress(
                "data_backfill_binance_universe_ready "
                f"pairs={len(binance_pairs)} skipped={len(skipped_binance)}"
            )

        symbol_summaries: list[SymbolBackfillSummary] = []
        errors = 0

        if "ndax" in source_list and ndax_resolution is not None:
            ndax_entries = list(ndax_resolution.tradable)
            bridge_entry = _find_ndax_entry(
                ndax_instruments,
                self._config.bridge_fx_symbol,
            )
            if bridge_entry is not None and all(e.ndax_symbol != bridge_entry.ndax_symbol for e in ndax_entries):
                ndax_entries.append(bridge_entry)

            for entry in ndax_entries:
                try:
                    summary = self._backfill_ndax_symbol(
                        entry=entry,
                        from_date=from_date,
                        to_date=to_date,
                        interval_seconds=interval_seconds,
                        timeframe=timeframe,
                    )
                except Exception as exc:
                    errors += 1
                    self._logger.exception("NDAX backfill failed symbol=%s", entry.ndax_symbol)
                    summary = SymbolBackfillSummary(
                        source="ndax",
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
                symbol_summaries.append(summary)
                self._emit_progress(
                    "symbol_backfill_complete "
                    f"source=ndax symbol={summary.symbol} status={summary.status} "
                    f"rows={summary.row_count} rows_added={summary.rows_added} gaps={summary.gap_count}"
                )

        if "binance" in source_list:
            for ticker, pair_symbol in sorted(binance_pairs.items()):
                try:
                    summary = self._backfill_binance_symbol(
                        ticker=ticker,
                        pair_symbol=pair_symbol,
                        from_date=from_date,
                        to_date=to_date,
                        interval_seconds=interval_seconds,
                        timeframe=timeframe,
                    )
                except Exception as exc:
                    errors += 1
                    self._logger.exception("Binance backfill failed symbol=%s", pair_symbol)
                    summary = SymbolBackfillSummary(
                        source="binance",
                        ticker=ticker,
                        symbol=pair_symbol,
                        instrument_id=0,
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
                symbol_summaries.append(summary)
                self._emit_progress(
                    "symbol_backfill_complete "
                    f"source=binance symbol={summary.symbol} status={summary.status} "
                    f"rows={summary.row_count} rows_added={summary.rows_added} gaps={summary.gap_count}"
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
            mode="dual_source_backfill",
            sources=list(source_list),
            symbols_total=len(symbol_summaries),
            symbols_processed=len(symbol_summaries),
            symbols_with_errors=errors,
            skipped_pairs=skipped_pairs,
            symbols=symbol_summaries,
        )

    def data_status(self, *, timeframe: str, dataset: str = "combined") -> DataStatusSummary | DataStatusAllSummary:
        interval_seconds = parse_timeframe_seconds(timeframe)
        dataset_key = dataset.strip().lower()
        if dataset_key not in {"ndax", "binance", "combined", "all"}:
            raise ValueError("dataset must be one of: ndax, binance, combined, all")

        generated_at = _utc_now_iso()
        if dataset_key == "all":
            summaries: dict[str, DataStatusSummary] = {}
            for key in ("ndax", "binance", "combined"):
                summary = self._data_status_single(
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                    dataset=key,
                    generated_at=generated_at,
                )
                summaries[key] = summary
            return DataStatusAllSummary(
                generated_at_utc=generated_at,
                timeframe=timeframe,
                interval_seconds=interval_seconds,
                datasets=summaries,
            )

        return self._data_status_single(
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            dataset=dataset_key,
            generated_at=generated_at,
        )

    def build_combined(
        self,
        *,
        from_date: date,
        to_date: date,
        timeframe: str,
    ) -> CombinedBuildSummary:
        if from_date > to_date:
            raise ValueError("from_date must be <= to_date.")

        interval_seconds = parse_timeframe_seconds(timeframe)
        started_at = _utc_now_iso()
        self._emit_progress(
            "data_build_combined_started "
            f"from={from_date.isoformat()} to={to_date.isoformat()} timeframe={timeframe}"
        )

        targets = self._resolve_combined_targets()
        bridge_rows = self._load_ndax_bridge_rows(interval_seconds=interval_seconds)
        shared_context = self._build_shared_conversion_context(
            targets=targets,
            interval_seconds=interval_seconds,
            bridge_rows=bridge_rows,
        )

        symbol_summaries: list[CombinedSymbolSummary] = []
        errors = 0
        for target in targets:
            try:
                summary = self._build_combined_symbol(
                    ticker=target[0],
                    ndax_symbol=target[1],
                    binance_symbol=target[2],
                    from_date=from_date,
                    to_date=to_date,
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                    bridge_rows=bridge_rows,
                    shared_context=shared_context,
                )
            except Exception as exc:
                errors += 1
                self._logger.exception("Combined build failed symbol=%s", target[1])
                summary = CombinedSymbolSummary(
                    symbol=target[1],
                    ticker=target[0],
                    status="error",
                    message=f"build_failed: {exc}",
                    ndax_rows=0,
                    binance_rows=0,
                    combined_rows=0,
                    gap_count=0,
                    ndax_share=0.0,
                    synth_share=0.0,
                    build_hash="",
                    file_path=str(self._combined_symbol_path(target[1], interval_seconds=interval_seconds)),
                )
            symbol_summaries.append(summary)
            self._emit_progress(
                "data_build_combined_symbol "
                f"symbol={summary.symbol} status={summary.status} combined_rows={summary.combined_rows} "
                f"gaps={summary.gap_count}"
            )

        completed_at = _utc_now_iso()
        self._emit_progress(
            "data_build_combined_completed "
            f"symbols={len(symbol_summaries)} errors={errors}"
        )

        return CombinedBuildSummary(
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            requested_from=from_date.isoformat(),
            requested_to=to_date.isoformat(),
            symbols_total=len(symbol_summaries),
            symbols_built=len(symbol_summaries) - errors,
            symbols_with_errors=errors,
            symbols=symbol_summaries,
        )

    def calibrate_weights(
        self,
        *,
        from_date: date,
        to_date: date,
        timeframe: str,
        refresh: str,
    ) -> WeightCalibrationSummary:
        if from_date > to_date:
            raise ValueError("from_date must be <= to_date.")
        refresh_key = refresh.strip().lower()
        if refresh_key != "monthly":
            raise ValueError("refresh currently supports only: monthly")

        interval_seconds = parse_timeframe_seconds(timeframe)
        started_at = _utc_now_iso()
        run_id = _run_id()

        self._emit_progress(
            "data_calibrate_weights_started "
            f"from={from_date.isoformat()} to={to_date.isoformat()} timeframe={timeframe} refresh={refresh_key}"
        )

        targets = self._resolve_combined_targets()
        bridge_rows = self._load_ndax_bridge_rows(interval_seconds=interval_seconds)
        month_ranges = _month_ranges(from_date=from_date, to_date=to_date)

        interim_rows: list[tuple[CalibrationWeightRow, float]] = []

        for ticker, ndax_symbol, binance_symbol in targets:
            ndax_read = _read_market_records(
                self._ndax_symbol_path(ndax_symbol, interval_seconds=interval_seconds),
                interval_seconds=interval_seconds,
                fallback_symbol=ndax_symbol,
                fallback_source="ndax",
            )
            binance_read = _read_market_records(
                self._binance_symbol_path(binance_symbol, interval_seconds=interval_seconds),
                interval_seconds=interval_seconds,
                fallback_symbol=binance_symbol,
                fallback_source="binance",
            )
            if not ndax_read.records and not binance_read.records:
                continue

            context = _build_conversion_context(
                ndax_rows=ndax_read.records,
                binance_rows=binance_read.records,
                fx_rows=bridge_rows,
            )

            for month_key, start_ms, end_exclusive_ms, period_start_iso, period_end_iso in month_ranges:
                metrics = _compute_overlap_metrics(
                    ndax_rows=ndax_read.records,
                    binance_rows=binance_read.records,
                    context=context,
                    start_ms=start_ms,
                    end_exclusive_ms=end_exclusive_ms,
                    min_overlap_rows=self._config.min_overlap_rows_for_weight,
                    max_median_ape=self._config.conversion_max_median_ape,
                )

                weight_quality = _clamp(
                    self._config.synth_weight_min
                    + (self._config.synth_weight_max - self._config.synth_weight_min) * metrics.quality_score,
                    self._config.synth_weight_min,
                    self._config.synth_weight_max,
                )
                weight_backtest = _grid_search_weight(
                    ndax_returns=metrics.ndax_returns,
                    synth_returns=metrics.synth_returns,
                    weight_min=self._config.synth_weight_min,
                    weight_max=self._config.synth_weight_max,
                    fee_per_side=self._config.taker_fee_rate,
                )
                weight_raw = 0.60 * weight_quality + 0.40 * weight_backtest

                self._state_store.insert_conversion_quality(
                    symbol=ndax_symbol,
                    timeframe=timeframe,
                    period_start=period_start_iso,
                    period_end=period_end_iso,
                    overlap_rows=metrics.overlap_rows,
                    median_ape_close=metrics.median_ape_close,
                    median_abs_ret_err=metrics.median_abs_ret_err,
                    ret_corr=metrics.ret_corr,
                    direction_match=metrics.direction_match,
                    basis_median=metrics.basis_median,
                    basis_mad=metrics.basis_mad,
                    quality_pass=metrics.quality_pass,
                )

                interim_rows.append(
                    (
                        CalibrationWeightRow(
                            symbol=ndax_symbol,
                            effective_month=month_key,
                            overlap_rows=metrics.overlap_rows,
                            median_ape_close=metrics.median_ape_close,
                            median_abs_ret_err=metrics.median_abs_ret_err,
                            ret_corr=metrics.ret_corr,
                            direction_match=metrics.direction_match,
                            basis_median=metrics.basis_median,
                            basis_mad=metrics.basis_mad,
                            quality_score=metrics.quality_score,
                            quality_pass=metrics.quality_pass,
                            weight_quality=weight_quality,
                            weight_backtest=weight_backtest,
                            weight_final=0.0,
                            supervised_eligible=False,
                            eligibility_mode="blocked",
                            anchor_month=None,
                            report_note="pending_global_shrinkage",
                        ),
                        weight_raw,
                    )
                )

        candidates = [weight_raw for row, weight_raw in interim_rows if row.quality_pass]
        if candidates:
            weight_global = statistics.median(candidates)
        else:
            weight_global = self._config.synth_weight_default

        finalized: list[CalibrationWeightRow] = []
        for row, weight_raw in interim_rows:
            if (not row.quality_pass) or row.overlap_rows < self._config.min_overlap_rows_for_weight:
                weight_final = 0.25
                note = "fallback_quality_guardrail"
            else:
                k = row.overlap_rows / (row.overlap_rows + 5000.0)
                weight_final = _clamp(
                    k * weight_raw + (1.0 - k) * weight_global,
                    self._config.synth_weight_min,
                    self._config.synth_weight_max,
                )
                note = f"shrunk_by_overlap k={k:.4f}"

            finalized_row = CalibrationWeightRow(
                symbol=row.symbol,
                effective_month=row.effective_month,
                overlap_rows=row.overlap_rows,
                median_ape_close=row.median_ape_close,
                median_abs_ret_err=row.median_abs_ret_err,
                ret_corr=row.ret_corr,
                direction_match=row.direction_match,
                basis_median=row.basis_median,
                basis_mad=row.basis_mad,
                quality_score=row.quality_score,
                quality_pass=row.quality_pass,
                weight_quality=row.weight_quality,
                weight_backtest=row.weight_backtest,
                weight_final=weight_final,
                supervised_eligible=False,
                eligibility_mode="blocked",
                anchor_month=None,
                report_note=note,
            )
            finalized.append(finalized_row)

        eligibility_overlap_min = _supervised_eligibility_min_overlap(
            min_overlap_rows=self._config.min_overlap_rows_for_weight
        )
        finalized_by_symbol: dict[str, list[CalibrationWeightRow]] = {}
        for row in finalized:
            finalized_by_symbol.setdefault(row.symbol, []).append(row)

        eligible_rows: list[CalibrationWeightRow] = []
        for symbol in sorted(finalized_by_symbol):
            anchor_month: str | None = None
            for row in sorted(finalized_by_symbol[symbol], key=lambda item: item.effective_month):
                if _direct_supervised_eligible(
                    overlap_rows=row.overlap_rows,
                    median_ape_close=row.median_ape_close,
                    ret_corr=row.ret_corr,
                    max_median_ape=self._config.conversion_max_median_ape,
                    min_overlap_rows=eligibility_overlap_min,
                ):
                    eligible_row = CalibrationWeightRow(
                        symbol=row.symbol,
                        effective_month=row.effective_month,
                        overlap_rows=row.overlap_rows,
                        median_ape_close=row.median_ape_close,
                        median_abs_ret_err=row.median_abs_ret_err,
                        ret_corr=row.ret_corr,
                        direction_match=row.direction_match,
                        basis_median=row.basis_median,
                        basis_mad=row.basis_mad,
                        quality_score=row.quality_score,
                        quality_pass=row.quality_pass,
                        weight_quality=row.weight_quality,
                        weight_backtest=row.weight_backtest,
                        weight_final=row.weight_final,
                        supervised_eligible=True,
                        eligibility_mode="direct",
                        anchor_month=row.effective_month,
                        report_note=row.report_note,
                    )
                    anchor_month = row.effective_month
                elif row.overlap_rows == 0 and anchor_month is not None:
                    eligible_row = CalibrationWeightRow(
                        symbol=row.symbol,
                        effective_month=row.effective_month,
                        overlap_rows=row.overlap_rows,
                        median_ape_close=row.median_ape_close,
                        median_abs_ret_err=row.median_abs_ret_err,
                        ret_corr=row.ret_corr,
                        direction_match=row.direction_match,
                        basis_median=row.basis_median,
                        basis_mad=row.basis_mad,
                        quality_score=row.quality_score,
                        quality_pass=row.quality_pass,
                        weight_quality=row.weight_quality,
                        weight_backtest=row.weight_backtest,
                        weight_final=row.weight_final,
                        supervised_eligible=True,
                        eligibility_mode="carry_forward",
                        anchor_month=anchor_month,
                        report_note=row.report_note,
                    )
                else:
                    eligible_row = row

                eligible_rows.append(eligible_row)
                self._state_store.upsert_synthetic_weight(
                    symbol=eligible_row.symbol,
                    timeframe=timeframe,
                    effective_month=eligible_row.effective_month,
                    weight_quality=eligible_row.weight_quality,
                    weight_backtest=eligible_row.weight_backtest,
                    weight_final=eligible_row.weight_final,
                    overlap_rows=eligible_row.overlap_rows,
                    quality_pass=eligible_row.quality_pass,
                    method_version=_WEIGHT_METHOD_VERSION,
                    supervised_eligible=eligible_row.supervised_eligible,
                    eligibility_mode=eligible_row.eligibility_mode,
                    anchor_month=eligible_row.anchor_month,
                )

        completed_at = _utc_now_iso()
        output_path = self._write_weight_report(
            run_id=run_id,
            payload={
                "started_at_utc": started_at,
                "completed_at_utc": completed_at,
                "timeframe": timeframe,
                "requested_from": from_date.isoformat(),
                "requested_to": to_date.isoformat(),
                "refresh": refresh_key,
                "weight_global": weight_global,
                "method_version": _WEIGHT_METHOD_VERSION,
                "rows": [asdict(row) for row in eligible_rows],
            },
        )

        self._emit_progress(
            "data_calibrate_weights_completed "
            f"run_id={run_id} rows={len(eligible_rows)} symbols={len({row.symbol for row in eligible_rows})}"
        )
        return WeightCalibrationSummary(
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            run_id=run_id,
            timeframe=timeframe,
            requested_from=from_date.isoformat(),
            requested_to=to_date.isoformat(),
            refresh=refresh_key,
            symbols_total=len({row.symbol for row in eligible_rows}),
            rows_total=len(eligible_rows),
            output_file=str(output_path),
            rows=eligible_rows,
        )

    def weight_status(self, *, timeframe: str) -> WeightStatusSummary:
        parse_timeframe_seconds(timeframe)
        rows = self._state_store.get_synthetic_weights(timeframe=timeframe)

        latest_by_symbol: dict[str, dict[str, object]] = {}
        for row in rows:
            symbol = str(row["symbol"])
            month = str(row["effective_month"])
            current = latest_by_symbol.get(symbol)
            if current is None or month > str(current["effective_month"]):
                latest_by_symbol[symbol] = row

        symbols_payload: list[dict[str, object]] = []
        for symbol in sorted(latest_by_symbol):
            row = latest_by_symbol[symbol]
            symbols_payload.append(
                {
                    "symbol": symbol,
                    "effective_month": row["effective_month"],
                    "weight_final": row["weight_final"],
                    "weight_quality": row["weight_quality"],
                    "weight_backtest": row["weight_backtest"],
                    "overlap_rows": row["overlap_rows"],
                    "quality_pass": bool(row["quality_pass"]),
                    "supervised_eligible": bool(row.get("supervised_eligible")),
                    "eligibility_mode": row.get("eligibility_mode"),
                    "anchor_month": row.get("anchor_month"),
                    "method_version": row["method_version"],
                    "updated_at_utc": row["updated_at_utc"],
                }
            )

        return WeightStatusSummary(
            generated_at_utc=_utc_now_iso(),
            timeframe=timeframe,
            row_count=len(symbols_payload),
            symbols=symbols_payload,
        )

    def _backfill_ndax_symbol(
        self,
        *,
        entry: UniverseEntry,
        from_date: date,
        to_date: date,
        interval_seconds: int,
        timeframe: str,
    ) -> SymbolBackfillSummary:
        output_path = self._ndax_symbol_path(entry.ndax_symbol, interval_seconds=interval_seconds)
        requested_from = from_date.isoformat()
        requested_to = to_date.isoformat()

        read_result = _read_market_records(
            output_path,
            interval_seconds=interval_seconds,
            fallback_symbol=entry.ndax_symbol,
            fallback_source="ndax",
        )
        existing = dict(read_result.records)
        old_count = len(existing)

        missing_windows = _missing_date_windows(
            existing_timestamps=set(existing),
            requested_from=from_date,
            requested_to=to_date,
            interval_seconds=interval_seconds,
            max_window_days=_CHUNK_DAYS,
        )
        resume_from = missing_windows[0][0] if missing_windows else from_date

        chunk_count = 0
        fetched_rows = 0
        for idx, (window_start, window_end) in enumerate(missing_windows, start=1):
            fetch_start = window_start - timedelta(days=1)
            fetch_end = window_end + timedelta(days=1)
            self._emit_progress(
                "symbol_chunk_fetch_start "
                f"source=ndax symbol={entry.ndax_symbol} chunk={idx}/{len(missing_windows)} "
                f"chunk_from={window_start.isoformat()} chunk_to={window_end.isoformat()} "
                f"fetch_from={fetch_start.isoformat()} fetch_to={fetch_end.isoformat()}"
            )

            rows = self._ndax_client.get_ticker_history(
                instrument_id=entry.instrument_id,
                interval_seconds=interval_seconds,
                from_date=fetch_start,
                to_date=fetch_end,
            )
            chunk_count += 1
            fetched_rows += len(rows)

            parsed = _parse_ndax_rows(
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
            _write_market_records_atomic(output_path, existing)

            in_range_timestamps = sorted(
                _records_in_date_range(existing, from_date=from_date, to_date=to_date)
            )
            self._state_store.upsert_data_sync_checkpoint(
                source="ndax",
                symbol=entry.ndax_symbol,
                timeframe=timeframe,
                requested_from=requested_from,
                requested_to=requested_to,
                last_success_ts=in_range_timestamps[-1] if in_range_timestamps else None,
                status="ok",
            )

        in_range = _records_in_date_range(existing, from_date=from_date, to_date=to_date)
        timestamps = sorted(in_range)
        first_ts = timestamps[0] if timestamps else None
        last_ts = timestamps[-1] if timestamps else None
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        row_count = len(timestamps)
        rows_added = max(0, len(existing) - old_count)
        coverage_pct = _coverage_pct_from_range(
            row_count=row_count,
            requested_from=from_date,
            requested_to=to_date,
            interval_seconds=interval_seconds,
        )

        self._state_store.upsert_data_coverage(
            symbol=entry.ndax_symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
        )
        self._state_store.upsert_data_coverage_v2(
            dataset="ndax",
            symbol=entry.ndax_symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
            duplicate_count=read_result.duplicate_count,
            misaligned_count=read_result.misaligned_count,
            coverage_pct=coverage_pct,
            ndax_share=1.0 if row_count > 0 else 0.0,
            synth_share=0.0,
        )

        status = "ok" if row_count > 0 else "empty"
        message = "backfill_complete" if row_count > 0 else "no_rows_returned_for_requested_range"
        return SymbolBackfillSummary(
            source="ndax",
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

    def _backfill_binance_symbol(
        self,
        *,
        ticker: str,
        pair_symbol: str,
        from_date: date,
        to_date: date,
        interval_seconds: int,
        timeframe: str,
    ) -> SymbolBackfillSummary:
        client = self._ensure_binance_client()
        output_path = self._binance_symbol_path(pair_symbol, interval_seconds=interval_seconds)
        requested_from = from_date.isoformat()
        requested_to = to_date.isoformat()

        read_result = _read_market_records(
            output_path,
            interval_seconds=interval_seconds,
            fallback_symbol=pair_symbol,
            fallback_source="binance",
        )
        existing = dict(read_result.records)
        old_count = len(existing)

        missing_windows = _missing_date_windows(
            existing_timestamps=set(existing),
            requested_from=from_date,
            requested_to=to_date,
            interval_seconds=interval_seconds,
            max_window_days=_CHUNK_DAYS,
        )
        resume_from = missing_windows[0][0] if missing_windows else from_date

        chunk_count = 0
        fetched_rows = 0
        for idx, (window_start, window_end) in enumerate(missing_windows, start=1):
            pages, fetched = self._fetch_binance_window(
                client=client,
                symbol=pair_symbol,
                from_date=window_start,
                to_date=window_end,
                interval_seconds=interval_seconds,
                target=existing,
                merge_from=from_date,
                merge_to=to_date,
            )
            chunk_count += pages
            fetched_rows += fetched

            _write_market_records_atomic(output_path, existing)
            in_range_timestamps = sorted(
                _records_in_date_range(existing, from_date=from_date, to_date=to_date)
            )
            self._state_store.upsert_data_sync_checkpoint(
                source="binance",
                symbol=pair_symbol,
                timeframe=timeframe,
                requested_from=requested_from,
                requested_to=requested_to,
                last_success_ts=in_range_timestamps[-1] if in_range_timestamps else None,
                status="ok",
            )
            self._emit_progress(
                "symbol_chunk "
                f"source=binance symbol={pair_symbol} chunk={idx}/{len(missing_windows)} "
                f"chunk_from={window_start.isoformat()} chunk_to={window_end.isoformat()} pages={pages} "
                f"fetched_rows={fetched}"
            )

        repaired_rows = _repair_binance_outage_gaps(
            target=existing,
            from_date=from_date,
            to_date=to_date,
            interval_seconds=interval_seconds,
            symbol=pair_symbol,
        )
        if repaired_rows > 0:
            _write_market_records_atomic(output_path, existing)
            self._emit_progress(
                "symbol_gap_repair "
                f"source=binance symbol={pair_symbol} repaired_rows={repaired_rows}"
            )

        in_range = _records_in_date_range(existing, from_date=from_date, to_date=to_date)
        timestamps = sorted(in_range)
        first_ts = timestamps[0] if timestamps else None
        last_ts = timestamps[-1] if timestamps else None
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        row_count = len(timestamps)
        rows_added = max(0, len(existing) - old_count)
        coverage_pct = _coverage_pct_from_range(
            row_count=row_count,
            requested_from=from_date,
            requested_to=to_date,
            interval_seconds=interval_seconds,
        )

        self._state_store.upsert_data_coverage_v2(
            dataset="binance",
            symbol=pair_symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
            duplicate_count=read_result.duplicate_count,
            misaligned_count=read_result.misaligned_count,
            coverage_pct=coverage_pct,
            ndax_share=0.0,
            synth_share=1.0 if row_count > 0 else 0.0,
        )

        status = "ok" if row_count > 0 else "empty"
        message = "backfill_complete" if row_count > 0 else "no_rows_returned_for_requested_range"
        return SymbolBackfillSummary(
            source="binance",
            ticker=ticker,
            symbol=pair_symbol,
            instrument_id=0,
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

    def _fetch_binance_window(
        self,
        *,
        client: BinanceClient,
        symbol: str,
        from_date: date,
        to_date: date,
        interval_seconds: int,
        target: dict[int, dict[str, Any]],
        merge_from: date,
        merge_to: date,
    ) -> tuple[int, int]:
        start_ms = _date_start_ms(from_date)
        end_exclusive_ms = _date_end_exclusive_ms(to_date)
        interval_ms = interval_seconds * 1000

        cursor = start_ms
        pages = 0
        fetched_rows = 0
        max_iterations = ((_expected_count(from_date, to_date, interval_seconds) // _BINANCE_PAGE_LIMIT) + 4) * 3

        while cursor < end_exclusive_ms and pages < max_iterations:
            query_start = max(start_ms, cursor - interval_ms)
            self._emit_progress(
                "symbol_page_fetch_start "
                f"source=binance symbol={symbol} page={pages + 1} "
                f"query_start_ms={query_start} query_end_ms={end_exclusive_ms - 1}"
            )
            rows = client.get_klines(
                symbol=symbol,
                interval=_binance_interval(interval_seconds),
                start_time_ms=query_start,
                end_time_ms=end_exclusive_ms - 1,
                limit=_BINANCE_PAGE_LIMIT,
            )
            pages += 1
            fetched_rows += len(rows)
            if not rows:
                cursor += interval_ms * _BINANCE_PAGE_LIMIT
                continue

            parsed = _parse_binance_rows(
                rows=rows,
                symbol=symbol,
                interval_seconds=interval_seconds,
            )
            if not parsed:
                cursor += interval_ms
                continue

            _merge_records(
                target=target,
                incoming=parsed,
                from_date=merge_from,
                to_date=merge_to,
            )
            last_ts = max(parsed)
            next_cursor = last_ts + interval_ms
            if next_cursor <= cursor:
                cursor += interval_ms
            else:
                cursor = next_cursor

            if last_ts >= end_exclusive_ms - interval_ms:
                break

        return pages, fetched_rows

    def _data_status_single(
        self,
        *,
        timeframe: str,
        interval_seconds: int,
        dataset: str,
        generated_at: str,
    ) -> DataStatusSummary:
        symbols: list[SymbolCoverageSummary] = []
        skipped_pairs: dict[str, str] = {}

        if dataset == "ndax":
            mode = "ndax_universe"
            try:
                instruments = self._ndax_client.get_instruments()
                resolution = resolve_tradable_universe(instruments)
                skipped_pairs = dict(resolution.skipped)
                for entry in resolution.tradable:
                    symbols.append(
                        self._coverage_for_symbol(
                            dataset=dataset,
                            symbol=entry.ndax_symbol,
                            timeframe=timeframe,
                            interval_seconds=interval_seconds,
                        )
                    )
                for ticker, reason in sorted(skipped_pairs.items()):
                    symbols.append(
                        SymbolCoverageSummary(
                            dataset=dataset,
                            symbol=ticker,
                            status="no_pair",
                            row_count=0,
                            first_ts=None,
                            last_ts=None,
                            gap_count=0,
                            duplicate_count=0,
                            misaligned_count=0,
                            coverage_pct=0.0,
                            ndax_share=0.0,
                            synth_share=0.0,
                            timeframe=timeframe,
                            file_path="",
                            note=reason,
                        )
                    )
            except NdaxError:
                mode = "offline_files_only"
                symbols = self._coverage_from_local_files(
                    dataset=dataset,
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                )

        elif dataset == "binance":
            mode = "binance_universe"
            pairs, skipped = self._resolve_binance_pairs()
            skipped_pairs = {f"binance:{k}": v for k, v in skipped.items()}
            for _, pair_symbol in sorted(pairs.items()):
                symbols.append(
                    self._coverage_for_symbol(
                        dataset=dataset,
                        symbol=pair_symbol,
                        timeframe=timeframe,
                        interval_seconds=interval_seconds,
                    )
                )
            for ticker, reason in sorted(skipped.items()):
                symbols.append(
                    SymbolCoverageSummary(
                        dataset=dataset,
                        symbol=ticker,
                        status="no_pair",
                        row_count=0,
                        first_ts=None,
                        last_ts=None,
                        gap_count=0,
                        duplicate_count=0,
                        misaligned_count=0,
                        coverage_pct=0.0,
                        ndax_share=0.0,
                        synth_share=0.0,
                        timeframe=timeframe,
                        file_path="",
                        note=reason,
                    )
                )

        else:
            mode = "combined_dataset"
            targets = self._resolve_combined_targets()
            if targets:
                for _, ndax_symbol, _ in targets:
                    symbols.append(
                        self._coverage_for_symbol(
                            dataset=dataset,
                            symbol=ndax_symbol,
                            timeframe=timeframe,
                            interval_seconds=interval_seconds,
                        )
                    )
            else:
                symbols = self._coverage_from_local_files(
                    dataset=dataset,
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                )

        symbols_with_data = sum(1 for item in symbols if item.row_count > 0)
        symbols_without_data = sum(
            1
            for item in symbols
            if item.status in {"missing_file", "empty", "no_pair"}
        )
        symbols_with_gaps = sum(1 for item in symbols if item.gap_count > 0)
        summary = DataStatusSummary(
            generated_at_utc=generated_at,
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            dataset=dataset,
            mode=mode,
            symbols_total=len(symbols),
            symbols_with_data=symbols_with_data,
            symbols_without_data=symbols_without_data,
            symbols_with_gaps=symbols_with_gaps,
            skipped_pairs=skipped_pairs,
            symbols=symbols,
        )
        self._write_coverage_report(dataset=dataset, payload=summary.to_payload())
        return summary

    def _coverage_for_symbol(
        self,
        *,
        dataset: str,
        symbol: str,
        timeframe: str,
        interval_seconds: int,
    ) -> SymbolCoverageSummary:
        path = self._dataset_symbol_path(dataset=dataset, symbol=symbol, interval_seconds=interval_seconds)
        if not path.exists():
            if dataset == "ndax":
                self._state_store.upsert_data_coverage(
                    symbol=symbol,
                    timeframe=timeframe,
                    first_ts=None,
                    last_ts=None,
                    row_count=0,
                    gap_count=0,
                )
            self._state_store.upsert_data_coverage_v2(
                dataset=dataset,
                symbol=symbol,
                timeframe=timeframe,
                first_ts=None,
                last_ts=None,
                row_count=0,
                gap_count=0,
                duplicate_count=0,
                misaligned_count=0,
                coverage_pct=0.0,
                ndax_share=0.0,
                synth_share=0.0,
            )
            return SymbolCoverageSummary(
                dataset=dataset,
                symbol=symbol,
                status="missing_file",
                row_count=0,
                first_ts=None,
                last_ts=None,
                gap_count=0,
                duplicate_count=0,
                misaligned_count=0,
                coverage_pct=0.0,
                ndax_share=0.0,
                synth_share=0.0,
                timeframe=timeframe,
                file_path=str(path),
            )

        read_result = _read_market_records(
            path,
            interval_seconds=interval_seconds,
            fallback_symbol=symbol,
            fallback_source=("ndax" if dataset == "ndax" else "binance"),
        )
        timestamps = sorted(read_result.records)
        if not timestamps:
            if dataset == "ndax":
                self._state_store.upsert_data_coverage(
                    symbol=symbol,
                    timeframe=timeframe,
                    first_ts=None,
                    last_ts=None,
                    row_count=0,
                    gap_count=0,
                )
            self._state_store.upsert_data_coverage_v2(
                dataset=dataset,
                symbol=symbol,
                timeframe=timeframe,
                first_ts=None,
                last_ts=None,
                row_count=0,
                gap_count=0,
                duplicate_count=read_result.duplicate_count,
                misaligned_count=read_result.misaligned_count,
                coverage_pct=0.0,
                ndax_share=0.0,
                synth_share=0.0,
            )
            return SymbolCoverageSummary(
                dataset=dataset,
                symbol=symbol,
                status="empty",
                row_count=0,
                first_ts=None,
                last_ts=None,
                gap_count=0,
                duplicate_count=read_result.duplicate_count,
                misaligned_count=read_result.misaligned_count,
                coverage_pct=0.0,
                ndax_share=0.0,
                synth_share=0.0,
                timeframe=timeframe,
                file_path=str(path),
            )

        first_ts = timestamps[0]
        last_ts = timestamps[-1]
        row_count = len(timestamps)
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        coverage_pct = _coverage_pct_from_span(
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            interval_seconds=interval_seconds,
        )

        if dataset == "ndax":
            ndax_share = 1.0
            synth_share = 0.0
        elif dataset == "binance":
            ndax_share = 0.0
            synth_share = 1.0
        else:
            ndax_rows = 0
            synth_rows = 0
            for source, count in read_result.source_counts.items():
                if source in {"ndax", "live"}:
                    ndax_rows += count
                else:
                    synth_rows += count
            denom = max(1, ndax_rows + synth_rows)
            ndax_share = ndax_rows / denom
            synth_share = synth_rows / denom

        if dataset == "ndax":
            self._state_store.upsert_data_coverage(
                symbol=symbol,
                timeframe=timeframe,
                first_ts=first_ts,
                last_ts=last_ts,
                row_count=row_count,
                gap_count=gap_count,
            )

        self._state_store.upsert_data_coverage_v2(
            dataset=dataset,
            symbol=symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=row_count,
            gap_count=gap_count,
            duplicate_count=read_result.duplicate_count,
            misaligned_count=read_result.misaligned_count,
            coverage_pct=coverage_pct,
            ndax_share=ndax_share,
            synth_share=synth_share,
        )

        return SymbolCoverageSummary(
            dataset=dataset,
            symbol=symbol,
            status="ok",
            row_count=row_count,
            first_ts=first_ts,
            last_ts=last_ts,
            gap_count=gap_count,
            duplicate_count=read_result.duplicate_count,
            misaligned_count=read_result.misaligned_count,
            coverage_pct=coverage_pct,
            ndax_share=ndax_share,
            synth_share=synth_share,
            timeframe=timeframe,
            file_path=str(path),
        )

    def _coverage_from_local_files(
        self,
        *,
        dataset: str,
        timeframe: str,
        interval_seconds: int,
    ) -> list[SymbolCoverageSummary]:
        base = self._dataset_base_path(dataset=dataset, interval_seconds=interval_seconds)
        if not base.exists():
            return []
        symbols: list[SymbolCoverageSummary] = []
        for file_path in sorted(base.glob("*.parquet")):
            symbol = file_path.stem.upper()
            symbols.append(
                self._coverage_for_symbol(
                    dataset=dataset,
                    symbol=symbol,
                    timeframe=timeframe,
                    interval_seconds=interval_seconds,
                )
            )
        return symbols

    def _resolve_binance_pairs(self) -> tuple[dict[str, str], dict[str, str]]:
        quote = self._config.binance_quote.upper()
        pairs = {ticker: f"{ticker}{quote}" for ticker in UNIVERSE_V1_COINS}
        skipped: dict[str, str] = {}

        try:
            available = self._ensure_binance_client().list_spot_symbols()
        except BinanceError:
            available = set()

        if available:
            filtered: dict[str, str] = {}
            for ticker, symbol in pairs.items():
                if symbol in available:
                    filtered[ticker] = symbol
                else:
                    skipped[ticker] = f"no_binance_{quote.lower()}_pair"
            return filtered, skipped
        return pairs, skipped

    def _resolve_combined_targets(self) -> list[tuple[str, str, str]]:
        quote = self._config.binance_quote.upper()
        try:
            instruments = self._ndax_client.get_instruments()
            resolution = resolve_tradable_universe(instruments)
            return [
                (entry.ticker, entry.ndax_symbol, f"{entry.ticker}{quote}")
                for entry in resolution.tradable
            ]
        except NdaxError:
            base = self._ndax_base_path(interval_seconds=900)
            targets: list[tuple[str, str, str]] = []
            for file_path in sorted(base.glob("*.parquet")):
                ndax_symbol = file_path.stem.upper()
                ticker = ndax_symbol[:-3] if ndax_symbol.endswith("CAD") else ndax_symbol
                targets.append((ticker, ndax_symbol, f"{ticker}{quote}"))
            return targets

    def _load_ndax_bridge_rows(self, *, interval_seconds: int) -> dict[int, dict[str, Any]]:
        path = self._ndax_symbol_path(self._config.bridge_fx_symbol, interval_seconds=interval_seconds)
        if not path.exists():
            return {}
        return _read_market_records(
            path,
            interval_seconds=interval_seconds,
            fallback_symbol=self._config.bridge_fx_symbol,
            fallback_source="ndax",
        ).records

    def _build_shared_conversion_context(
        self,
        *,
        targets: list[tuple[str, str, str]],
        interval_seconds: int,
        bridge_rows: dict[int, dict[str, Any]],
    ) -> _ConversionContext:
        ratio_by_month_values: dict[str, list[float]] = {}
        basis_by_month_values: dict[str, list[float]] = {}
        fx_series = _fx_series_from_rows(bridge_rows)
        fx_map = {ts: price for ts, price in fx_series}

        for _, ndax_symbol, binance_symbol in targets:
            ndax_rows = _read_market_records(
                self._ndax_symbol_path(ndax_symbol, interval_seconds=interval_seconds),
                interval_seconds=interval_seconds,
                fallback_symbol=ndax_symbol,
                fallback_source="ndax",
            ).records
            binance_rows = _read_market_records(
                self._binance_symbol_path(binance_symbol, interval_seconds=interval_seconds),
                interval_seconds=interval_seconds,
                fallback_symbol=binance_symbol,
                fallback_source="binance",
            ).records
            _collect_conversion_observations(
                ndax_rows=ndax_rows,
                binance_rows=binance_rows,
                fx_map=fx_map,
                ratio_by_month_values=ratio_by_month_values,
                basis_by_month_values=basis_by_month_values,
            )

        return _finalize_conversion_context(
            ratio_by_month_values=ratio_by_month_values,
            basis_by_month_values=basis_by_month_values,
            fx_series=fx_series,
        )

    def _build_combined_symbol(
        self,
        *,
        ticker: str,
        ndax_symbol: str,
        binance_symbol: str,
        from_date: date,
        to_date: date,
        timeframe: str,
        interval_seconds: int,
        bridge_rows: dict[int, dict[str, Any]],
        shared_context: _ConversionContext,
    ) -> CombinedSymbolSummary:
        ndax_path = self._ndax_symbol_path(ndax_symbol, interval_seconds=interval_seconds)
        binance_path = self._binance_symbol_path(binance_symbol, interval_seconds=interval_seconds)
        output_path = self._combined_symbol_path(ndax_symbol, interval_seconds=interval_seconds)

        ndax_all = _read_market_records(
            ndax_path,
            interval_seconds=interval_seconds,
            fallback_symbol=ndax_symbol,
            fallback_source="ndax",
        ).records
        binance_all = _read_market_records(
            binance_path,
            interval_seconds=interval_seconds,
            fallback_symbol=binance_symbol,
            fallback_source="binance",
        ).records

        ndax_rows = _records_in_date_range(ndax_all, from_date=from_date, to_date=to_date)
        binance_rows = _records_in_date_range(binance_all, from_date=from_date, to_date=to_date)
        fx_rows = _records_in_date_range(bridge_rows, from_date=from_date, to_date=to_date)

        context = _build_conversion_context(
            ndax_rows=ndax_rows,
            binance_rows=binance_rows,
            fx_rows=fx_rows,
        )

        combined: dict[int, dict[str, Any]] = {}
        ndax_count = 0
        synth_count = 0
        missing_count = 0
        for ts in _iter_expected_timestamps(from_date=from_date, to_date=to_date, interval_seconds=interval_seconds):
            ndax_row = ndax_rows.get(ts)
            if ndax_row is not None:
                row = dict(ndax_row)
                row["source"] = "ndax"
                combined[ts] = row
                ndax_count += 1
                continue

            binance_row = binance_rows.get(ts)
            if binance_row is None:
                missing_count += 1
                continue

            factor = _conversion_factor(ts=ts, context=context, fallback_context=shared_context)
            if factor is None or factor <= 0:
                missing_count += 1
                continue

            synthetic_source = (
                _SYNTHETIC_GAP_FILL_SOURCE
                if _is_gap_fill_source(binance_row.get("source"))
                else "synthetic"
            )

            converted = {
                "timestamp_ms": ts,
                "open": float(binance_row["open"]) * factor,
                "high": float(binance_row["high"]) * factor,
                "low": float(binance_row["low"]) * factor,
                "close": float(binance_row["close"]) * factor,
                "volume": float(binance_row["volume"]),
                "inside_bid": 0.0,
                "inside_ask": 0.0,
                "instrument_id": 0,
                "symbol": ndax_symbol,
                "interval_seconds": int(interval_seconds),
                "source": synthetic_source,
            }
            combined[ts] = converted
            synth_count += 1

        _write_market_records_atomic(output_path, combined)

        timestamps = sorted(combined)
        first_ts = timestamps[0] if timestamps else None
        last_ts = timestamps[-1] if timestamps else None
        gap_count = _count_gaps(timestamps=timestamps, interval_seconds=interval_seconds)
        combined_rows = len(combined)
        denom = max(1, ndax_count + synth_count)
        ndax_share = ndax_count / denom
        synth_share = synth_count / denom
        build_hash = _build_rows_hash(combined)

        self._state_store.insert_combined_build(
            symbol=ndax_symbol,
            timeframe=timeframe,
            from_ts=_date_start_ms(from_date),
            to_ts=_date_end_exclusive_ms(to_date) - interval_seconds * 1000,
            ndax_rows=len(ndax_rows),
            binance_rows=len(binance_rows),
            combined_rows=combined_rows,
            gap_count=gap_count,
            build_hash=build_hash,
        )

        coverage_pct = (
            _coverage_pct_from_span(
                first_ts=first_ts,
                last_ts=last_ts,
                row_count=combined_rows,
                interval_seconds=interval_seconds,
            )
            if first_ts is not None and last_ts is not None and combined_rows > 0
            else 0.0
        )
        self._state_store.upsert_data_coverage_v2(
            dataset="combined",
            symbol=ndax_symbol,
            timeframe=timeframe,
            first_ts=first_ts,
            last_ts=last_ts,
            row_count=combined_rows,
            gap_count=gap_count,
            duplicate_count=0,
            misaligned_count=0,
            coverage_pct=coverage_pct,
            ndax_share=ndax_share,
            synth_share=synth_share,
        )

        if combined_rows == 0:
            status = "empty"
            message = "no_combined_rows"
        elif gap_count > self._config.combined_max_gap_count:
            status = "warning"
            message = f"gap_count_exceeds_threshold gap_count={gap_count}"
        else:
            status = "ok"
            message = "combined_build_complete"

        return CombinedSymbolSummary(
            symbol=ndax_symbol,
            ticker=ticker,
            status=status,
            message=message,
            ndax_rows=len(ndax_rows),
            binance_rows=len(binance_rows),
            combined_rows=combined_rows,
            gap_count=gap_count,
            ndax_share=ndax_share,
            synth_share=synth_share,
            build_hash=build_hash,
            file_path=str(output_path),
        )

    def _write_coverage_report(self, *, dataset: str, payload: dict[str, object]) -> None:
        report_path = self._config.runtime_dir / "logs" / f"data_coverage_{dataset}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _write_weight_report(self, *, run_id: str, payload: dict[str, object]) -> Path:
        output_dir = self._config.runtime_dir / "research" / "bridge_weighting" / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "metrics.json"
        output_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return output_file

    def _emit_progress(self, message: str) -> None:
        if self._progress_callback is not None:
            self._progress_callback(message)

    def _ensure_binance_client(self) -> BinanceClient:
        if self._binance_client is None:
            self._binance_client = BinanceClient(
                base_url=self._config.binance_base_url,
                timeout_seconds=self._config.ndax_timeout_seconds,
                max_retries=self._config.ndax_max_retries,
            )
        return self._binance_client

    def _dataset_base_path(self, *, dataset: str, interval_seconds: int) -> Path:
        timeframe_dir = _timeframe_dir(interval_seconds)
        data_root = self._config.runtime_dir.parent / "data"
        if dataset == "ndax":
            return data_root / "raw" / "ndax" / timeframe_dir
        if dataset == "binance":
            return data_root / "raw" / "binance" / timeframe_dir
        if dataset == "combined":
            return data_root / "combined" / timeframe_dir
        raise ValueError(f"Unsupported dataset: {dataset}")

    def _dataset_symbol_path(self, *, dataset: str, symbol: str, interval_seconds: int) -> Path:
        return self._dataset_base_path(dataset=dataset, interval_seconds=interval_seconds) / f"{symbol.upper()}.parquet"

    def _ndax_base_path(self, *, interval_seconds: int) -> Path:
        return self._dataset_base_path(dataset="ndax", interval_seconds=interval_seconds)

    def _binance_base_path(self, *, interval_seconds: int) -> Path:
        return self._dataset_base_path(dataset="binance", interval_seconds=interval_seconds)

    def _combined_base_path(self, *, interval_seconds: int) -> Path:
        return self._dataset_base_path(dataset="combined", interval_seconds=interval_seconds)

    def _ndax_symbol_path(self, symbol: str, *, interval_seconds: int) -> Path:
        return self._ndax_base_path(interval_seconds=interval_seconds) / f"{symbol.upper()}.parquet"

    def _binance_symbol_path(self, symbol: str, *, interval_seconds: int) -> Path:
        return self._binance_base_path(interval_seconds=interval_seconds) / f"{symbol.upper()}.parquet"

    def _combined_symbol_path(self, symbol: str, *, interval_seconds: int) -> Path:
        return self._combined_base_path(interval_seconds=interval_seconds) / f"{symbol.upper()}.parquet"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _timeframe_dir(interval_seconds: int) -> str:
    if interval_seconds == 900:
        return "15m"
    return f"{interval_seconds}s"


def _binance_interval(interval_seconds: int) -> str:
    if interval_seconds == 900:
        return "15m"
    raise ValueError(f"Unsupported Binance interval for seconds={interval_seconds}")


def _normalize_sources(sources: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for item in sources:
        value = str(item).strip().lower()
        if not value:
            continue
        if value not in {"ndax", "binance"}:
            raise ValueError(f"Unsupported data source: {item}")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("At least one data source must be specified.")
    return tuple(normalized)


def _find_ndax_entry(instruments: list[dict[str, Any]], symbol: str) -> UniverseEntry | None:
    symbol_upper = symbol.upper()
    for instrument in instruments:
        current = str(instrument.get("Symbol", "")).upper()
        if current != symbol_upper:
            continue
        base = str(instrument.get("Product1Symbol", "")).upper() or symbol_upper.replace("CAD", "")
        instrument_id = int(instrument.get("InstrumentId", 0) or 0)
        if instrument_id <= 0:
            continue
        return UniverseEntry(
            ticker=base,
            ndax_symbol=symbol_upper,
            instrument_id=instrument_id,
        )
    return None


def _date_start_ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)


def _date_end_exclusive_ms(value: date) -> int:
    return _date_start_ms(value) + 86_400_000


def _expected_count(from_date: date, to_date: date, interval_seconds: int) -> int:
    if from_date > to_date:
        return 0
    step = interval_seconds * 1000
    start_ms = _date_start_ms(from_date)
    end_ms = _date_end_exclusive_ms(to_date)
    return max(0, (end_ms - start_ms) // step)


def _iter_expected_timestamps(*, from_date: date, to_date: date, interval_seconds: int) -> Iterable[int]:
    start_ms = _date_start_ms(from_date)
    end_ms = _date_end_exclusive_ms(to_date)
    step = interval_seconds * 1000
    ts = start_ms
    while ts < end_ms:
        yield ts
        ts += step


def _missing_date_windows(
    *,
    existing_timestamps: set[int],
    requested_from: date,
    requested_to: date,
    interval_seconds: int,
    max_window_days: int,
) -> list[tuple[date, date]]:
    missing_dates: list[date] = []
    current = requested_from
    while current <= requested_to:
        if not _day_has_full_coverage(
            existing_timestamps=existing_timestamps,
            day=current,
            interval_seconds=interval_seconds,
        ):
            missing_dates.append(current)
        current += timedelta(days=1)

    if not missing_dates:
        return []

    windows: list[tuple[date, date]] = []
    window_start = missing_dates[0]
    previous = missing_dates[0]
    for item in missing_dates[1:]:
        contiguous = (item - previous).days == 1
        window_days = (item - window_start).days + 1
        if contiguous and window_days <= max_window_days:
            previous = item
            continue
        windows.append((window_start, previous))
        window_start = item
        previous = item
    windows.append((window_start, previous))
    return windows


def _day_has_full_coverage(
    *,
    existing_timestamps: set[int],
    day: date,
    interval_seconds: int,
) -> bool:
    step = interval_seconds * 1000
    start_ms = _date_start_ms(day)
    end_ms = _date_end_exclusive_ms(day)
    ts = start_ms
    while ts < end_ms:
        if ts not in existing_timestamps:
            return False
        ts += step
    return True


def _parse_ndax_rows(
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
            "source": "ndax",
        }
    return result


def _parse_binance_rows(
    *,
    rows: list[list[Any]],
    symbol: str,
    interval_seconds: int,
) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    interval_ms = interval_seconds * 1000
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            raw_ts = int(row[0])
            open_price = float(row[1])
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
            volume = float(row[5])
        except (TypeError, ValueError):
            continue
        if raw_ts <= 0:
            continue
        if not all(math.isfinite(value) for value in (open_price, high, low, close, volume)):
            continue
        ts = raw_ts - (raw_ts % interval_ms)
        result[ts] = {
            "timestamp_ms": ts,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "inside_bid": 0.0,
            "inside_ask": 0.0,
            "instrument_id": 0,
            "symbol": symbol.upper(),
            "interval_seconds": int(interval_seconds),
            "source": "binance",
        }
    return result


def _merge_records(
    *,
    target: dict[int, dict[str, Any]],
    incoming: dict[int, dict[str, Any]],
    from_date: date,
    to_date: date,
) -> None:
    start_ms = _date_start_ms(from_date)
    end_exclusive_ms = _date_end_exclusive_ms(to_date)
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
    start_ms = _date_start_ms(from_date)
    end_exclusive_ms = _date_end_exclusive_ms(to_date)
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


def _read_market_records(
    path: Path,
    *,
    interval_seconds: int,
    fallback_symbol: str,
    fallback_source: str,
) -> _ReadResult:
    if not path.exists():
        return _ReadResult(records={}, duplicate_count=0, misaligned_count=0, raw_row_count=0, source_counts={})

    table = pq.read_table(path)
    data = table.to_pydict()
    timestamps = data.get("timestamp_ms", [])
    records: dict[int, dict[str, Any]] = {}
    duplicates = 0
    misaligned = 0
    source_counts: dict[str, int] = {}

    seen: set[int] = set()
    interval_ms = interval_seconds * 1000
    row_count = len(timestamps)

    for idx in range(row_count):
        try:
            ts = int(timestamps[idx])
        except (TypeError, ValueError):
            continue
        if ts in seen:
            duplicates += 1
        seen.add(ts)

        if ts % interval_ms != 0:
            misaligned += 1

        source = str(_column_value(data, "source", idx, fallback_source)).strip().lower() or fallback_source
        source_counts[source] = source_counts.get(source, 0) + 1

        try:
            record = {
                "timestamp_ms": ts,
                "open": float(_column_value(data, "open", idx, 0.0)),
                "high": float(_column_value(data, "high", idx, 0.0)),
                "low": float(_column_value(data, "low", idx, 0.0)),
                "close": float(_column_value(data, "close", idx, 0.0)),
                "volume": float(_column_value(data, "volume", idx, 0.0)),
                "inside_bid": float(_column_value(data, "inside_bid", idx, 0.0)),
                "inside_ask": float(_column_value(data, "inside_ask", idx, 0.0)),
                "instrument_id": int(_column_value(data, "instrument_id", idx, 0) or 0),
                "symbol": str(_column_value(data, "symbol", idx, fallback_symbol)).upper(),
                "interval_seconds": int(_column_value(data, "interval_seconds", idx, interval_seconds) or interval_seconds),
                "source": source,
            }
        except (TypeError, ValueError):
            continue

        records[ts] = record

    return _ReadResult(
        records=records,
        duplicate_count=duplicates,
        misaligned_count=misaligned,
        raw_row_count=row_count,
        source_counts=source_counts,
    )


def _column_value(data: dict[str, list[Any]], key: str, idx: int, default: Any) -> Any:
    values = data.get(key)
    if values is None or idx >= len(values):
        return default
    value = values[idx]
    if value is None:
        return default
    return value


def _write_market_records_atomic(path: Path, records: dict[int, dict[str, Any]]) -> None:
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
        "source": [],
    }
    for ts in sorted_ts:
        row = records[ts]
        arrays["timestamp_ms"].append(int(row["timestamp_ms"]))
        arrays["open"].append(float(row["open"]))
        arrays["high"].append(float(row["high"]))
        arrays["low"].append(float(row["low"]))
        arrays["close"].append(float(row["close"]))
        arrays["volume"].append(float(row["volume"]))
        arrays["inside_bid"].append(float(row.get("inside_bid", 0.0)))
        arrays["inside_ask"].append(float(row.get("inside_ask", 0.0)))
        arrays["instrument_id"].append(int(row.get("instrument_id", 0) or 0))
        arrays["symbol"].append(str(row.get("symbol", "")).upper())
        arrays["interval_seconds"].append(int(row.get("interval_seconds", 900) or 900))
        arrays["source"].append(str(row.get("source", "unknown")))

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
            ("source", pa.string()),
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


def _build_conversion_context(
    *,
    ndax_rows: dict[int, dict[str, Any]],
    binance_rows: dict[int, dict[str, Any]],
    fx_rows: dict[int, dict[str, Any]],
) -> _ConversionContext:
    ratio_by_month_values: dict[str, list[float]] = {}
    basis_by_month_values: dict[str, list[float]] = {}
    fx_series = _fx_series_from_rows(fx_rows)
    fx_map = {ts: price for ts, price in fx_series}

    _collect_conversion_observations(
        ndax_rows=ndax_rows,
        binance_rows=binance_rows,
        fx_map=fx_map,
        ratio_by_month_values=ratio_by_month_values,
        basis_by_month_values=basis_by_month_values,
    )

    return _finalize_conversion_context(
        ratio_by_month_values=ratio_by_month_values,
        basis_by_month_values=basis_by_month_values,
        fx_series=fx_series,
    )


def _conversion_factor(
    *,
    ts: int,
    context: _ConversionContext,
    fallback_context: _ConversionContext | None = None,
) -> float | None:
    factor = _conversion_factor_from_context(ts=ts, context=context)
    if factor is not None and factor > 0:
        return factor
    if fallback_context is not None:
        factor = _conversion_factor_from_context(ts=ts, context=fallback_context)
        if factor is not None and factor > 0:
            return factor
    return None


def _conversion_factor_from_context(*, ts: int, context: _ConversionContext) -> float | None:
    month_key = _month_key(ts)
    ratio_month = context.ratio_by_month.get(month_key)
    basis_month = context.basis_by_month.get(month_key)

    fx = _nearest_series_value(
        series=context.fx_series,
        timestamps=context.fx_timestamps,
        ts=ts,
    )
    if fx is not None and fx > 0:
        basis = basis_month if basis_month is not None else context.global_basis
        if basis is not None and basis > 0:
            return fx * basis

    if ratio_month is not None and ratio_month > 0:
        return ratio_month
    if context.global_ratio is not None and context.global_ratio > 0:
        return context.global_ratio

    return None


def _compute_overlap_metrics(
    *,
    ndax_rows: dict[int, dict[str, Any]],
    binance_rows: dict[int, dict[str, Any]],
    context: _ConversionContext,
    start_ms: int,
    end_exclusive_ms: int,
    min_overlap_rows: int,
    max_median_ape: float,
) -> _OverlapMetrics:
    overlap_points: list[tuple[int, float, float]] = []
    basis_values: list[float] = []
    for ts in sorted(set(ndax_rows).intersection(binance_rows)):
        if ts < start_ms or ts >= end_exclusive_ms:
            continue
        if _is_gap_fill_source(binance_rows[ts].get("source")):
            continue
        ndax_close = float(ndax_rows[ts]["close"])
        raw_binance_close = float(binance_rows[ts]["close"])
        if ndax_close <= 0 or raw_binance_close <= 0:
            continue
        factor = _conversion_factor(ts=ts, context=context)
        if factor is None or factor <= 0:
            continue
        synth_close = raw_binance_close * factor
        overlap_points.append((ts, ndax_close, synth_close))

        fx = _nearest_series_value(
            series=context.fx_series,
            timestamps=context.fx_timestamps,
            ts=ts,
        )
        if fx is not None and fx > 0:
            ratio = ndax_close / raw_binance_close
            basis_values.append(ratio / fx)

    overlap_rows = len(overlap_points)
    if overlap_rows == 0:
        return _OverlapMetrics(
            overlap_rows=0,
            median_ape_close=1.0,
            median_abs_ret_err=1.0,
            ret_corr=0.0,
            direction_match=0.0,
            basis_median=1.0,
            basis_mad=1.0,
            quality_score=0.0,
            quality_pass=False,
            ndax_returns=[],
            synth_returns=[],
        )

    ape_values: list[float] = []
    ndax_returns: list[float] = []
    synth_returns: list[float] = []
    previous_ndax: float | None = None
    previous_synth: float | None = None
    direction_hits = 0

    for _, ndax_close, synth_close in overlap_points:
        ape_values.append(abs(ndax_close - synth_close) / max(1e-12, ndax_close))
        if previous_ndax is not None and previous_synth is not None and previous_ndax > 0 and previous_synth > 0:
            ndax_ret = ndax_close / previous_ndax - 1.0
            synth_ret = synth_close / previous_synth - 1.0
            ndax_returns.append(ndax_ret)
            synth_returns.append(synth_ret)
            if _sign(ndax_ret) == _sign(synth_ret):
                direction_hits += 1
        previous_ndax = ndax_close
        previous_synth = synth_close

    median_ape_close = _median_or_default(ape_values, default=1.0)
    abs_ret_err = [abs(a - b) for a, b in zip(ndax_returns, synth_returns)]
    median_abs_ret_err = _median_or_default(abs_ret_err, default=1.0)
    ret_corr = _pearson_corr(ndax_returns, synth_returns)
    direction_match = (
        direction_hits / len(ndax_returns)
        if ndax_returns
        else 0.0
    )

    basis_median = _median_or_default(basis_values, default=1.0)
    basis_mad = _median_or_default([abs(v - basis_median) for v in basis_values], default=1.0)

    score_price = 1.0 - _clamp(median_ape_close / max(1e-9, max_median_ape), 0.0, 1.0)
    score_ret_err = 1.0 - _clamp(median_abs_ret_err / 0.01, 0.0, 1.0)
    score_corr = _clamp((ret_corr + 1.0) / 2.0, 0.0, 1.0)
    score_direction = _clamp((direction_match - 0.5) / 0.5, 0.0, 1.0)
    score_basis = 1.0 - _clamp(basis_mad / 0.02, 0.0, 1.0)
    quality_score = (
        0.35 * score_price
        + 0.25 * score_ret_err
        + 0.20 * score_corr
        + 0.10 * score_direction
        + 0.10 * score_basis
    )

    quality_pass = (
        overlap_rows >= min_overlap_rows
        and median_ape_close <= max_median_ape
        and ret_corr >= 0.30
    )

    return _OverlapMetrics(
        overlap_rows=overlap_rows,
        median_ape_close=median_ape_close,
        median_abs_ret_err=median_abs_ret_err,
        ret_corr=ret_corr,
        direction_match=direction_match,
        basis_median=basis_median,
        basis_mad=basis_mad,
        quality_score=_clamp(quality_score, 0.0, 1.0),
        quality_pass=quality_pass,
        ndax_returns=ndax_returns,
        synth_returns=synth_returns,
    )


def _grid_search_weight(
    *,
    ndax_returns: list[float],
    synth_returns: list[float],
    weight_min: float,
    weight_max: float,
    fee_per_side: float,
) -> float:
    if not ndax_returns or not synth_returns:
        return (weight_min + weight_max) / 2.0

    fee_roundtrip = 2.0 * fee_per_side
    candidates: list[float] = []
    current = weight_min
    while current <= weight_max + 1e-9:
        candidates.append(round(current, 4))
        current += 0.05

    best_weight = candidates[0]
    best_score = float("-inf")
    for weight in candidates:
        trade_returns: list[float] = []
        for ndax_ret, synth_ret in zip(ndax_returns, synth_returns):
            signal = weight * synth_ret
            if abs(signal) <= fee_roundtrip:
                continue
            signed = 1.0 if signal > 0 else -1.0
            trade_returns.append(signed * ndax_ret - fee_roundtrip)

        if not trade_returns:
            score = float("-inf")
        else:
            median_ret = _median_or_default(trade_returns, default=-1.0)
            volatility = statistics.pstdev(trade_returns) if len(trade_returns) > 1 else abs(median_ret)
            score = median_ret - 0.5 * volatility

        if score > best_score:
            best_score = score
            best_weight = weight

    return _clamp(best_weight, weight_min, weight_max)


def _supervised_eligibility_min_overlap(*, min_overlap_rows: int) -> int:
    return max(_SUPERVISED_ELIGIBILITY_MIN_OVERLAP_ROWS, max(1, min_overlap_rows // 4))


def _direct_supervised_eligible(
    *,
    overlap_rows: int,
    median_ape_close: float,
    ret_corr: float,
    max_median_ape: float,
    min_overlap_rows: int,
) -> bool:
    return (
        overlap_rows >= min_overlap_rows
        and median_ape_close <= max_median_ape
        and ret_corr >= 0.30
    )


def _nearest_series_value(*, series: list[tuple[int, float]], timestamps: list[int], ts: int) -> float | None:
    if not series:
        return None
    idx = bisect_left(timestamps, ts)
    candidates: list[tuple[int, float]] = []
    if idx < len(series):
        candidates.append(series[idx])
    if idx > 0:
        candidates.append(series[idx - 1])
    if not candidates:
        return None

    best = min(candidates, key=lambda item: (abs(item[0] - ts), item[0]))
    return best[1]


def _fx_series_from_rows(fx_rows: dict[int, dict[str, Any]]) -> list[tuple[int, float]]:
    return sorted(
        (
            ts,
            float(row["close"]),
        )
        for ts, row in fx_rows.items()
        if float(row["close"]) > 0
    )


def _collect_conversion_observations(
    *,
    ndax_rows: dict[int, dict[str, Any]],
    binance_rows: dict[int, dict[str, Any]],
    fx_map: dict[int, float],
    ratio_by_month_values: dict[str, list[float]],
    basis_by_month_values: dict[str, list[float]],
) -> None:
    for ts in sorted(set(ndax_rows).intersection(binance_rows)):
        binance_row = binance_rows[ts]
        if _is_gap_fill_source(binance_row.get("source")):
            continue
        ndax_close = float(ndax_rows[ts]["close"])
        binance_close = float(binance_row["close"])
        if ndax_close <= 0 or binance_close <= 0:
            continue
        ratio = ndax_close / binance_close
        month_key = _month_key(ts)
        ratio_by_month_values.setdefault(month_key, []).append(ratio)

        fx = fx_map.get(ts)
        if fx is not None and fx > 0:
            basis = ratio / fx
            basis_by_month_values.setdefault(month_key, []).append(basis)


def _finalize_conversion_context(
    *,
    ratio_by_month_values: dict[str, list[float]],
    basis_by_month_values: dict[str, list[float]],
    fx_series: list[tuple[int, float]],
) -> _ConversionContext:
    ratio_by_month = {key: _robust_median(values) for key, values in ratio_by_month_values.items() if values}
    basis_by_month = {key: _robust_median(values) for key, values in basis_by_month_values.items() if values}

    all_ratio_values = [value for values in ratio_by_month_values.values() for value in values]
    all_basis_values = [value for values in basis_by_month_values.values() for value in values]

    global_ratio = _robust_median(all_ratio_values) if all_ratio_values else None
    global_basis = _robust_median(all_basis_values) if all_basis_values else None

    return _ConversionContext(
        ratio_by_month=ratio_by_month,
        basis_by_month=basis_by_month,
        global_ratio=global_ratio,
        global_basis=global_basis,
        fx_series=fx_series,
        fx_timestamps=[item[0] for item in fx_series],
    )


def _is_gap_fill_source(source: Any) -> bool:
    return str(source).strip().lower() in {_BINANCE_GAP_FILL_SOURCE, _SYNTHETIC_GAP_FILL_SOURCE}


def _repair_binance_outage_gaps(
    *,
    target: dict[int, dict[str, Any]],
    from_date: date,
    to_date: date,
    interval_seconds: int,
    symbol: str,
) -> int:
    timestamps = sorted(_records_in_date_range(target, from_date=from_date, to_date=to_date))
    if len(timestamps) <= 1:
        return 0

    step = interval_seconds * 1000
    repaired = 0
    for left_ts, right_ts in zip(timestamps, timestamps[1:]):
        if right_ts - left_ts <= step:
            continue
        close_price = float(target[left_ts]["close"])
        if not math.isfinite(close_price) or close_price <= 0:
            continue
        missing_ts = left_ts + step
        while missing_ts < right_ts:
            target[missing_ts] = {
                "timestamp_ms": missing_ts,
                "open": close_price,
                "high": close_price,
                "low": close_price,
                "close": close_price,
                "volume": 0.0,
                "inside_bid": 0.0,
                "inside_ask": 0.0,
                "instrument_id": 0,
                "symbol": symbol.upper(),
                "interval_seconds": int(interval_seconds),
                "source": _BINANCE_GAP_FILL_SOURCE,
            }
            repaired += 1
            missing_ts += step
    return repaired


def _coverage_pct_from_span(*, first_ts: int, last_ts: int, row_count: int, interval_seconds: int) -> float:
    if row_count <= 0:
        return 0.0
    step = interval_seconds * 1000
    expected = ((last_ts - first_ts) // step) + 1
    if expected <= 0:
        return 0.0
    return _clamp(row_count / expected, 0.0, 1.0)


def _coverage_pct_from_range(*, row_count: int, requested_from: date, requested_to: date, interval_seconds: int) -> float:
    expected = _expected_count(requested_from, requested_to, interval_seconds)
    if expected <= 0:
        return 0.0
    return _clamp(row_count / expected, 0.0, 1.0)


def _month_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def _month_ranges(*, from_date: date, to_date: date) -> list[tuple[str, int, int, str, str]]:
    result: list[tuple[str, int, int, str, str]] = []
    current = date(from_date.year, from_date.month, 1)
    while current <= to_date:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)

        period_start = max(current, from_date)
        period_end = min(next_month - timedelta(days=1), to_date)
        if period_start <= period_end:
            month_key = f"{period_start.year:04d}-{period_start.month:02d}"
            result.append(
                (
                    month_key,
                    _date_start_ms(period_start),
                    _date_end_exclusive_ms(period_end),
                    period_start.isoformat(),
                    period_end.isoformat(),
                )
            )
        current = next_month
    return result


def _robust_median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) < 8:
        return float(statistics.median(sorted_values))
    low = int(len(sorted_values) * 0.05)
    high = int(len(sorted_values) * 0.95)
    clipped = sorted_values[low:max(low + 1, high)]
    return float(statistics.median(clipped))


def _median_or_default(values: list[float], *, default: float) -> float:
    if not values:
        return default
    return float(statistics.median(values))


def _pearson_corr(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0

    mean_left = statistics.fmean(left)
    mean_right = statistics.fmean(right)
    num = 0.0
    den_left = 0.0
    den_right = 0.0
    for a, b in zip(left, right):
        da = a - mean_left
        db = b - mean_right
        num += da * db
        den_left += da * da
        den_right += db * db
    if den_left <= 0 or den_right <= 0:
        return 0.0
    return _clamp(num / math.sqrt(den_left * den_right), -1.0, 1.0)


def _build_rows_hash(records: dict[int, dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for ts in sorted(records):
        row = records[ts]
        digest.update(
            (
                f"{ts}|{row['open']:.12f}|{row['high']:.12f}|{row['low']:.12f}|"
                f"{row['close']:.12f}|{row['volume']:.12f}|{row.get('source', '')}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
