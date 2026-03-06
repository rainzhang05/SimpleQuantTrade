"""Deterministic weighted training snapshot builder."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from qtbot.config import RuntimeConfig
from qtbot.data import parse_timeframe_seconds
from qtbot.state import StateStore
from qtbot.universe import UNIVERSE_V1_COINS


_PARQUET_COMPRESSION = "zstd"
_NATIVE_WEIGHT_METHOD_VERSION = "ndax_native_v1"
_SNAPSHOT_ROW_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("timestamp_ms", pa.int64()),
        ("next_timestamp_ms", pa.int64()),
        ("interval_seconds", pa.int32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("inside_bid", pa.float64()),
        ("inside_ask", pa.float64()),
        ("source", pa.string()),
        ("effective_month", pa.string()),
        ("quality_pass", pa.bool_()),
        ("weight_method_version", pa.string()),
        ("effective_monthly_weight", pa.float64()),
        ("supervised_row_weight", pa.float64()),
        ("label_available", pa.bool_()),
        ("row_status", pa.string()),
        ("next_close", pa.float64()),
        ("forward_return", pa.float64()),
        ("y", pa.int8()),
    ]
)


@dataclass(frozen=True)
class SnapshotSymbolSummary:
    symbol: str
    status: str
    message: str
    closed_input_rows: int
    snapshot_rows: int
    trainable_rows: int
    continuity_only_rows: int
    unlabeled_rows: int
    source_counts: dict[str, int]
    duplicate_count: int
    misaligned_count: int
    gap_count: int
    coverage_pct: float
    first_ts: int | None
    last_ts: int | None
    parity_ok: bool
    file_path: str


@dataclass(frozen=True)
class SnapshotWeightUsage:
    symbol: str
    effective_month: str
    quality_pass: bool
    weight_final: float
    method_version: str
    rows_total: int
    trainable_rows: int
    continuity_only_rows: int


@dataclass(frozen=True)
class SnapshotBuildSummary:
    snapshot_id: str
    created_at_utc: str
    asof_utc: str
    closed_cutoff_utc: str
    dataset: str
    timeframe: str
    interval_seconds: int
    label_threshold_return: float
    dataset_hash: str
    snapshot_dir: str
    manifest_file: str
    rows_file: str
    reused_existing: bool
    symbols_total: int
    symbols_included: int
    symbols_skipped: int
    skipped_symbols: dict[str, str]
    row_count: int
    trainable_row_count: int
    continuity_only_row_count: int
    unlabeled_row_count: int
    source_mix: dict[str, int]
    trainable_source_mix: dict[str, int]
    supervised_weight_sum: float
    parity_check_passed: bool
    symbols: list[SnapshotSymbolSummary]
    weights_used: list[SnapshotWeightUsage]

    def to_payload(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "created_at_utc": self.created_at_utc,
            "asof_utc": self.asof_utc,
            "closed_cutoff_utc": self.closed_cutoff_utc,
            "dataset": self.dataset,
            "timeframe": self.timeframe,
            "interval_seconds": self.interval_seconds,
            "label_threshold_return": self.label_threshold_return,
            "dataset_hash": self.dataset_hash,
            "snapshot_dir": self.snapshot_dir,
            "manifest_file": self.manifest_file,
            "rows_file": self.rows_file,
            "reused_existing": self.reused_existing,
            "symbols_total": self.symbols_total,
            "symbols_included": self.symbols_included,
            "symbols_skipped": self.symbols_skipped,
            "skipped_symbols": self.skipped_symbols,
            "row_count": self.row_count,
            "trainable_row_count": self.trainable_row_count,
            "continuity_only_row_count": self.continuity_only_row_count,
            "unlabeled_row_count": self.unlabeled_row_count,
            "source_mix": dict(self.source_mix),
            "trainable_source_mix": dict(self.trainable_source_mix),
            "supervised_weight_sum": self.supervised_weight_sum,
            "parity_check_passed": self.parity_check_passed,
            "symbols": [asdict(item) for item in self.symbols],
            "weights_used": [asdict(item) for item in self.weights_used],
        }


@dataclass(frozen=True)
class _DatasetRows:
    rows: list[dict[str, Any]]
    source_counts: dict[str, int]
    duplicate_count: int
    misaligned_count: int
    gap_count: int
    coverage_pct: float
    first_ts: int | None
    last_ts: int | None


class TrainingSnapshotService:
    """Builds deterministic supervised snapshots from local candle data."""

    def __init__(
        self,
        *,
        config: RuntimeConfig,
        state_store: StateStore,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._state_store = state_store
        self._logger = logger or logging.getLogger("qtbot.snapshot")

    def build_snapshot(
        self,
        *,
        asof: datetime,
        timeframe: str,
    ) -> SnapshotBuildSummary:
        asof_utc = _as_utc(asof)
        interval_seconds = parse_timeframe_seconds(timeframe)
        dataset = self._config.dataset_mode.strip().lower()
        if dataset not in {"ndax", "combined"}:
            raise ValueError("build-snapshot supports dataset modes: ndax, combined")

        closed_cutoff_ms = _closed_cutoff_ms(asof_utc=asof_utc, interval_seconds=interval_seconds)
        if closed_cutoff_ms <= 0:
            raise ValueError("asof does not contain any closed bars for the requested timeframe")

        label_threshold = 2.0 * float(self._config.taker_fee_rate)
        snapshot_root = self._config.runtime_dir.parent / "data" / "snapshots"
        snapshot_root.mkdir(parents=True, exist_ok=True)

        temp_root = snapshot_root / f".snapshot_build.{os.getpid()}.{_safe_build_stamp()}"
        temp_root.mkdir(parents=True, exist_ok=False)
        rows_path = temp_root / "rows.parquet"

        weight_lookup = self._load_weight_lookup(timeframe=timeframe) if dataset == "combined" else {}
        hash_digest = hashlib.sha256()
        writer: pq.ParquetWriter | None = None
        symbol_summaries: list[SnapshotSymbolSummary] = []
        skipped_symbols: dict[str, str] = {}
        source_mix: dict[str, int] = {}
        trainable_source_mix: dict[str, int] = {}
        weights_used: dict[tuple[str, str], dict[str, Any]] = {}
        row_count = 0
        trainable_row_count = 0
        continuity_only_row_count = 0
        unlabeled_row_count = 0
        supervised_weight_sum = 0.0

        try:
            for ticker in UNIVERSE_V1_COINS:
                symbol = f"{ticker}CAD"
                symbol_path = self._dataset_symbol_path(
                    dataset=dataset,
                    symbol=symbol,
                    interval_seconds=interval_seconds,
                )
                if not symbol_path.exists():
                    skipped_symbols[symbol] = "missing_dataset_file"
                    symbol_summaries.append(
                        SnapshotSymbolSummary(
                            symbol=symbol,
                            status="skipped",
                            message="missing_dataset_file",
                            closed_input_rows=0,
                            snapshot_rows=0,
                            trainable_rows=0,
                            continuity_only_rows=0,
                            unlabeled_rows=0,
                            source_counts={},
                            duplicate_count=0,
                            misaligned_count=0,
                            gap_count=0,
                            coverage_pct=0.0,
                            first_ts=None,
                            last_ts=None,
                            parity_ok=True,
                            file_path=str(symbol_path),
                        )
                    )
                    continue

                dataset_rows = _read_dataset_rows(
                    path=symbol_path,
                    interval_seconds=interval_seconds,
                    closed_cutoff_ms=closed_cutoff_ms,
                )
                if not dataset_rows.rows:
                    skipped_symbols[symbol] = "no_closed_rows_before_asof"
                    symbol_summaries.append(
                        SnapshotSymbolSummary(
                            symbol=symbol,
                            status="skipped",
                            message="no_closed_rows_before_asof",
                            closed_input_rows=0,
                            snapshot_rows=0,
                            trainable_rows=0,
                            continuity_only_rows=0,
                            unlabeled_rows=0,
                            source_counts={},
                            duplicate_count=dataset_rows.duplicate_count,
                            misaligned_count=dataset_rows.misaligned_count,
                            gap_count=dataset_rows.gap_count,
                            coverage_pct=dataset_rows.coverage_pct,
                            first_ts=dataset_rows.first_ts,
                            last_ts=dataset_rows.last_ts,
                            parity_ok=True,
                            file_path=str(symbol_path),
                        )
                    )
                    continue

                if dataset_rows.duplicate_count > 0:
                    raise ValueError(f"{symbol} has duplicate timestamps in closed snapshot window")
                if dataset_rows.misaligned_count > 0:
                    raise ValueError(f"{symbol} has misaligned timestamps in closed snapshot window")
                if dataset == "combined":
                    if dataset_rows.gap_count > self._config.combined_max_gap_count:
                        raise ValueError(
                            f"{symbol} combined gap_count={dataset_rows.gap_count} exceeds "
                            f"QTBOT_COMBINED_MAX_GAP_COUNT={self._config.combined_max_gap_count}"
                        )
                    if dataset_rows.coverage_pct < self._config.combined_min_coverage:
                        raise ValueError(
                            f"{symbol} combined coverage_pct={dataset_rows.coverage_pct:.6f} is below "
                            f"QTBOT_COMBINED_MIN_COVERAGE={self._config.combined_min_coverage:.6f}"
                        )

                arrays = _empty_snapshot_arrays()
                symbol_trainable = 0
                symbol_continuity_only = 0
                symbol_unlabeled = 0
                rows = dataset_rows.rows
                for idx, row in enumerate(rows):
                    current_ts = int(row["timestamp_ms"])
                    next_row = rows[idx + 1] if idx + 1 < len(rows) else None
                    has_next = next_row is not None and int(next_row["timestamp_ms"]) == current_ts + (interval_seconds * 1000)
                    source = str(row["source"]).strip().lower()
                    synthetic_source = source in {"synthetic", "synthetic_gap_fill"}
                    next_source = (
                        str(next_row["source"]).strip().lower()
                        if next_row is not None
                        else ""
                    )
                    next_gap_fill = next_source == "synthetic_gap_fill"
                    if source not in {"ndax", "synthetic", "synthetic_gap_fill"}:
                        raise ValueError(f"{symbol} contains unsupported source value: {source}")

                    effective_month = _month_key(current_ts)
                    quality_pass = True
                    effective_monthly_weight = 1.0
                    weight_method_version = _NATIVE_WEIGHT_METHOD_VERSION
                    if dataset == "combined" and synthetic_source:
                        weight_row = weight_lookup.get((symbol, effective_month))
                        if weight_row is None:
                            raise ValueError(
                                f"Missing synthetic weight for symbol={symbol} effective_month={effective_month}"
                            )
                        quality_pass = bool(weight_row["quality_pass"])
                        effective_monthly_weight = float(weight_row["weight_final"])
                        weight_method_version = str(weight_row["method_version"])
                        usage_key = (symbol, effective_month)
                        usage = weights_used.setdefault(
                            usage_key,
                            {
                                "symbol": symbol,
                                "effective_month": effective_month,
                                "quality_pass": quality_pass,
                                "weight_final": effective_monthly_weight,
                                "method_version": weight_method_version,
                                "rows_total": 0,
                                "trainable_rows": 0,
                                "continuity_only_rows": 0,
                            },
                        )
                        usage["rows_total"] += 1

                    row_status = "trainable"
                    label_available = has_next
                    if not has_next:
                        row_status = "unlabeled_missing_next"
                        label_available = False
                    elif dataset == "combined" and (source == "synthetic_gap_fill" or next_gap_fill):
                        row_status = "continuity_only"
                        label_available = False
                    elif dataset == "combined" and synthetic_source and not quality_pass:
                        row_status = "continuity_only"
                        label_available = False

                    next_timestamp_ms = int(next_row["timestamp_ms"]) if next_row is not None else None
                    next_close = float(next_row["close"]) if label_available and next_row is not None else None
                    forward_return = (
                        (next_close / float(row["close"])) - 1.0
                        if label_available and next_close is not None and float(row["close"]) > 0
                        else None
                    )
                    label_value = (
                        1
                        if forward_return is not None and forward_return > label_threshold
                        else 0
                        if forward_return is not None
                        else None
                    )
                    supervised_row_weight = effective_monthly_weight if label_available else 0.0

                    if row_status == "trainable":
                        symbol_trainable += 1
                        trainable_row_count += 1
                        trainable_source_mix[source] = trainable_source_mix.get(source, 0) + 1
                        supervised_weight_sum += supervised_row_weight
                        if dataset == "combined" and synthetic_source:
                            weights_used[(symbol, effective_month)]["trainable_rows"] += 1
                    elif row_status == "continuity_only":
                        symbol_continuity_only += 1
                        continuity_only_row_count += 1
                        if dataset == "combined" and synthetic_source:
                            weights_used[(symbol, effective_month)]["continuity_only_rows"] += 1
                    else:
                        symbol_unlabeled += 1
                        unlabeled_row_count += 1

                    arrays["symbol"].append(symbol)
                    arrays["timestamp_ms"].append(current_ts)
                    arrays["next_timestamp_ms"].append(next_timestamp_ms)
                    arrays["interval_seconds"].append(interval_seconds)
                    arrays["open"].append(float(row["open"]))
                    arrays["high"].append(float(row["high"]))
                    arrays["low"].append(float(row["low"]))
                    arrays["close"].append(float(row["close"]))
                    arrays["volume"].append(float(row["volume"]))
                    arrays["inside_bid"].append(float(row.get("inside_bid", 0.0)))
                    arrays["inside_ask"].append(float(row.get("inside_ask", 0.0)))
                    arrays["source"].append(source)
                    arrays["effective_month"].append(effective_month)
                    arrays["quality_pass"].append(bool(quality_pass))
                    arrays["weight_method_version"].append(weight_method_version)
                    arrays["effective_monthly_weight"].append(float(effective_monthly_weight))
                    arrays["supervised_row_weight"].append(float(supervised_row_weight))
                    arrays["label_available"].append(bool(label_available))
                    arrays["row_status"].append(row_status)
                    arrays["next_close"].append(next_close)
                    arrays["forward_return"].append(forward_return)
                    arrays["y"].append(label_value)

                    hash_digest.update(
                        _snapshot_hash_line(
                            symbol=symbol,
                            row=row,
                            next_timestamp_ms=next_timestamp_ms,
                            source=source,
                            effective_month=effective_month,
                            quality_pass=quality_pass,
                            weight_method_version=weight_method_version,
                            effective_monthly_weight=effective_monthly_weight,
                            supervised_row_weight=supervised_row_weight,
                            label_available=label_available,
                            row_status=row_status,
                            next_close=next_close,
                            forward_return=forward_return,
                            label_value=label_value,
                        )
                    )
                    source_mix[source] = source_mix.get(source, 0) + 1
                    row_count += 1

                if arrays["symbol"]:
                    table = pa.Table.from_pydict(arrays, schema=_SNAPSHOT_ROW_SCHEMA)
                    if writer is None:
                        writer = pq.ParquetWriter(rows_path, _SNAPSHOT_ROW_SCHEMA, compression=_PARQUET_COMPRESSION)
                    writer.write_table(table)

                parity_ok = len(rows) == len(arrays["symbol"]) == (
                    symbol_trainable + symbol_continuity_only + symbol_unlabeled
                )
                if not parity_ok:
                    raise ValueError(f"row-count parity failed for {symbol}")

                symbol_summaries.append(
                    SnapshotSymbolSummary(
                        symbol=symbol,
                        status="ok",
                        message="snapshot_rows_written",
                        closed_input_rows=len(rows),
                        snapshot_rows=len(arrays["symbol"]),
                        trainable_rows=symbol_trainable,
                        continuity_only_rows=symbol_continuity_only,
                        unlabeled_rows=symbol_unlabeled,
                        source_counts=dict(dataset_rows.source_counts),
                        duplicate_count=dataset_rows.duplicate_count,
                        misaligned_count=dataset_rows.misaligned_count,
                        gap_count=dataset_rows.gap_count,
                        coverage_pct=dataset_rows.coverage_pct,
                        first_ts=dataset_rows.first_ts,
                        last_ts=dataset_rows.last_ts,
                        parity_ok=True,
                        file_path=str(symbol_path),
                    )
                )
        except Exception:
            if writer is not None:
                writer.close()
            shutil.rmtree(temp_root, ignore_errors=True)
            raise
        else:
            if writer is not None:
                writer.close()

        if row_count <= 0 or writer is None:
            shutil.rmtree(temp_root, ignore_errors=True)
            raise ValueError("No closed rows were available for snapshot generation")

        dataset_hash = hash_digest.hexdigest()
        snapshot_id = _snapshot_id(
            asof_utc=asof_utc,
            dataset=dataset,
            timeframe=timeframe,
            dataset_hash=dataset_hash,
        )
        final_dir = snapshot_root / snapshot_id
        final_manifest = final_dir / "manifest.json"
        final_rows = final_dir / "rows.parquet"
        created_at_utc = _utc_now_iso()

        weight_usage_rows = [
            SnapshotWeightUsage(**weights_used[key])
            for key in sorted(weights_used)
        ]
        parity_check_passed = all(item.parity_ok for item in symbol_summaries)
        manifest_payload = {
            "snapshot_id": snapshot_id,
            "created_at_utc": created_at_utc,
            "asof_utc": asof_utc.replace(microsecond=0).isoformat(),
            "closed_cutoff_utc": _iso_from_timestamp_ms(closed_cutoff_ms),
            "dataset": dataset,
            "timeframe": timeframe,
            "interval_seconds": interval_seconds,
            "label_threshold_return": label_threshold,
            "dataset_hash": dataset_hash,
            "snapshot_files": {
                "manifest": "manifest.json",
                "rows": "rows.parquet",
            },
            "symbols_total": len(symbol_summaries),
            "symbols_included": sum(1 for item in symbol_summaries if item.status == "ok"),
            "symbols_skipped": len(skipped_symbols),
            "skipped_symbols": dict(skipped_symbols),
            "row_count": row_count,
            "trainable_row_count": trainable_row_count,
            "continuity_only_row_count": continuity_only_row_count,
            "unlabeled_row_count": unlabeled_row_count,
            "source_mix": dict(source_mix),
            "trainable_source_mix": dict(trainable_source_mix),
            "supervised_weight_sum": supervised_weight_sum,
            "parity_check_passed": parity_check_passed,
            "symbols": [asdict(item) for item in symbol_summaries],
            "weights_used": [asdict(item) for item in weight_usage_rows],
        }

        with (temp_root / "manifest.json").open("w", encoding="utf-8") as handle:
            json.dump(manifest_payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

        reused_existing = False
        if final_dir.exists():
            if not final_manifest.exists() or not final_rows.exists():
                shutil.rmtree(temp_root, ignore_errors=True)
                raise ValueError(f"Existing snapshot directory is incomplete: {final_dir}")
            existing_manifest = _load_manifest(final_manifest)
            if (
                str(existing_manifest.get("dataset_hash")) != dataset_hash
                or str(existing_manifest.get("asof_utc")) != manifest_payload["asof_utc"]
                or int(existing_manifest.get("row_count", -1)) != row_count
            ):
                shutil.rmtree(temp_root, ignore_errors=True)
                raise ValueError(f"Existing snapshot directory has conflicting content: {final_dir}")
            reused_existing = True
            created_at_utc = str(existing_manifest["created_at_utc"])
            shutil.rmtree(temp_root, ignore_errors=True)
        else:
            temp_root.rename(final_dir)

        return SnapshotBuildSummary(
            snapshot_id=snapshot_id,
            created_at_utc=created_at_utc,
            asof_utc=manifest_payload["asof_utc"],
            closed_cutoff_utc=manifest_payload["closed_cutoff_utc"],
            dataset=dataset,
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            label_threshold_return=label_threshold,
            dataset_hash=dataset_hash,
            snapshot_dir=str(final_dir),
            manifest_file=str(final_manifest),
            rows_file=str(final_rows),
            reused_existing=reused_existing,
            symbols_total=len(symbol_summaries),
            symbols_included=manifest_payload["symbols_included"],
            symbols_skipped=len(skipped_symbols),
            skipped_symbols=dict(skipped_symbols),
            row_count=row_count,
            trainable_row_count=trainable_row_count,
            continuity_only_row_count=continuity_only_row_count,
            unlabeled_row_count=unlabeled_row_count,
            source_mix=dict(source_mix),
            trainable_source_mix=dict(trainable_source_mix),
            supervised_weight_sum=supervised_weight_sum,
            parity_check_passed=parity_check_passed,
            symbols=symbol_summaries,
            weights_used=weight_usage_rows,
        )

    def _load_weight_lookup(self, *, timeframe: str) -> dict[tuple[str, str], dict[str, object]]:
        lookup: dict[tuple[str, str], dict[str, object]] = {}
        for row in self._state_store.get_synthetic_weights(timeframe=timeframe):
            symbol = str(row["symbol"]).strip().upper()
            effective_month = str(row["effective_month"])
            lookup[(symbol, effective_month)] = dict(row)
        return lookup

    def _dataset_symbol_path(self, *, dataset: str, symbol: str, interval_seconds: int) -> Path:
        data_root = self._config.runtime_dir.parent / "data"
        timeframe_dir = _timeframe_dir(interval_seconds)
        if dataset == "ndax":
            return data_root / "raw" / "ndax" / timeframe_dir / f"{symbol.upper()}.parquet"
        if dataset == "combined":
            return data_root / "combined" / timeframe_dir / f"{symbol.upper()}.parquet"
        raise ValueError(f"Unsupported dataset for snapshot building: {dataset}")


def _read_dataset_rows(
    *,
    path: Path,
    interval_seconds: int,
    closed_cutoff_ms: int,
) -> _DatasetRows:
    table = pq.read_table(path)
    data = table.to_pydict()
    timestamps = data.get("timestamp_ms", [])
    interval_ms = interval_seconds * 1000
    records: dict[int, dict[str, Any]] = {}
    duplicates = 0
    misaligned = 0
    seen: set[int] = set()

    for idx in range(len(timestamps)):
        try:
            ts = int(timestamps[idx])
        except (TypeError, ValueError):
            continue
        if ts >= closed_cutoff_ms:
            continue
        if ts in seen:
            duplicates += 1
        seen.add(ts)
        if ts % interval_ms != 0:
            misaligned += 1
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
                "source": str(_column_value(data, "source", idx, "ndax")).strip().lower() or "ndax",
            }
        except (TypeError, ValueError):
            continue
        records[ts] = record

    ordered_rows = [records[ts] for ts in sorted(records)]
    source_counts: dict[str, int] = {}
    for row in ordered_rows:
        source = str(row["source"])
        source_counts[source] = source_counts.get(source, 0) + 1

    if not ordered_rows:
        return _DatasetRows(
            rows=[],
            source_counts={},
            duplicate_count=duplicates,
            misaligned_count=misaligned,
            gap_count=0,
            coverage_pct=0.0,
            first_ts=None,
            last_ts=None,
        )

    first_ts = int(ordered_rows[0]["timestamp_ms"])
    last_ts = int(ordered_rows[-1]["timestamp_ms"])
    gap_count = _count_gaps(
        timestamps=[int(item["timestamp_ms"]) for item in ordered_rows],
        interval_seconds=interval_seconds,
    )
    coverage_pct = _coverage_pct_from_span(
        first_ts=first_ts,
        last_ts=last_ts,
        row_count=len(ordered_rows),
        interval_seconds=interval_seconds,
    )
    return _DatasetRows(
        rows=ordered_rows,
        source_counts=source_counts,
        duplicate_count=duplicates,
        misaligned_count=misaligned,
        gap_count=gap_count,
        coverage_pct=coverage_pct,
        first_ts=first_ts,
        last_ts=last_ts,
    )


def _empty_snapshot_arrays() -> dict[str, list[Any]]:
    return {
        "symbol": [],
        "timestamp_ms": [],
        "next_timestamp_ms": [],
        "interval_seconds": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "inside_bid": [],
        "inside_ask": [],
        "source": [],
        "effective_month": [],
        "quality_pass": [],
        "weight_method_version": [],
        "effective_monthly_weight": [],
        "supervised_row_weight": [],
        "label_available": [],
        "row_status": [],
        "next_close": [],
        "forward_return": [],
        "y": [],
    }


def _snapshot_hash_line(
    *,
    symbol: str,
    row: dict[str, Any],
    next_timestamp_ms: int | None,
    source: str,
    effective_month: str,
    quality_pass: bool,
    weight_method_version: str,
    effective_monthly_weight: float,
    supervised_row_weight: float,
    label_available: bool,
    row_status: str,
    next_close: float | None,
    forward_return: float | None,
    label_value: int | None,
) -> bytes:
    parts = [
        symbol,
        str(int(row["timestamp_ms"])),
        str(next_timestamp_ms or ""),
        source,
        effective_month,
        "1" if quality_pass else "0",
        weight_method_version,
        f"{float(row['open']):.12f}",
        f"{float(row['high']):.12f}",
        f"{float(row['low']):.12f}",
        f"{float(row['close']):.12f}",
        f"{float(row['volume']):.12f}",
        f"{float(row.get('inside_bid', 0.0)):.12f}",
        f"{float(row.get('inside_ask', 0.0)):.12f}",
        f"{effective_monthly_weight:.12f}",
        f"{supervised_row_weight:.12f}",
        "1" if label_available else "0",
        row_status,
        "" if next_close is None else f"{next_close:.12f}",
        "" if forward_return is None else f"{forward_return:.12f}",
        "" if label_value is None else str(int(label_value)),
    ]
    return ("|".join(parts) + "\n").encode("utf-8")


def _closed_cutoff_ms(*, asof_utc: datetime, interval_seconds: int) -> int:
    interval_ms = interval_seconds * 1000
    asof_ms = int(asof_utc.timestamp() * 1000)
    return (asof_ms // interval_ms) * interval_ms


def _count_gaps(*, timestamps: list[int], interval_seconds: int) -> int:
    if len(timestamps) <= 1:
        return 0
    expected_step = interval_seconds * 1000
    gaps = 0
    for left, right in zip(timestamps, timestamps[1:]):
        diff = right - left
        if diff <= expected_step:
            continue
        gaps += max(0, diff // expected_step - 1)
    return gaps


def _coverage_pct_from_span(*, first_ts: int, last_ts: int, row_count: int, interval_seconds: int) -> float:
    if row_count <= 0:
        return 0.0
    expected = ((last_ts - first_ts) // (interval_seconds * 1000)) + 1
    if expected <= 0:
        return 0.0
    return max(0.0, min(1.0, row_count / expected))


def _month_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def _column_value(data: dict[str, list[Any]], key: str, idx: int, default: Any) -> Any:
    values = data.get(key)
    if values is None or idx >= len(values):
        return default
    value = values[idx]
    if value is None:
        return default
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _iso_from_timestamp_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(microsecond=0).isoformat()


def _snapshot_id(*, asof_utc: datetime, dataset: str, timeframe: str, dataset_hash: str) -> str:
    stamp = asof_utc.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{dataset}_{timeframe}_{dataset_hash}"


def _timeframe_dir(interval_seconds: int) -> str:
    if interval_seconds == 900:
        return "15m"
    return f"{interval_seconds}s"


def _safe_build_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _load_manifest(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
