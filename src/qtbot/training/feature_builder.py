"""Deterministic snapshot feature generation for Phase 6."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from qtbot.training.feature_spec import FEATURE_COLUMNS, feature_spec_hash
from qtbot.universe import UNIVERSE_V1_COINS


@dataclass(frozen=True)
class FeatureBuildSummary:
    snapshot_id: str
    dataset_hash: str
    timeframe: str
    interval_seconds: int
    snapshot_dir: str
    row_count: int
    feature_count: int
    feature_spec_hash: str
    source_mix: dict[str, int]

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FeatureBuildResult:
    data: pd.DataFrame
    summary: FeatureBuildSummary


class FeatureBuilder:
    """Build historical-only features from a sealed snapshot plus raw context."""

    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root
        self._data_root = repo_root / "data"
        self._fx_series_cache: pd.Series | None = None

    def build(self, *, snapshot_id: str) -> FeatureBuildResult:
        snapshot_dir = self._data_root / "snapshots" / snapshot_id
        manifest_path = snapshot_dir / "manifest.json"
        rows_path = snapshot_dir / "rows.parquet"
        if not manifest_path.exists() or not rows_path.exists():
            raise ValueError(f"snapshot not found or incomplete: {snapshot_id}")

        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        interval_seconds = int(manifest["interval_seconds"])
        timeframe = str(manifest["timeframe"])

        pieces: list[pd.DataFrame] = []
        for ticker in UNIVERSE_V1_COINS:
            symbol = f"{ticker}CAD"
            frame = self._build_symbol_rows(
                symbol=symbol,
                ticker=ticker,
                snapshot_rows_path=rows_path,
                interval_seconds=interval_seconds,
            )
            if not frame.empty:
                pieces.append(frame)

        if not pieces:
            raise ValueError(f"snapshot contains no trainable rows: {snapshot_id}")

        data = pd.concat(pieces, ignore_index=True)
        data.sort_values(["symbol", "timestamp_ms"], inplace=True, kind="mergesort")
        data.reset_index(drop=True, inplace=True)

        summary = FeatureBuildSummary(
            snapshot_id=snapshot_id,
            dataset_hash=str(manifest["dataset_hash"]),
            timeframe=timeframe,
            interval_seconds=interval_seconds,
            snapshot_dir=str(snapshot_dir),
            row_count=int(len(data)),
            feature_count=len(FEATURE_COLUMNS),
            feature_spec_hash=feature_spec_hash(),
            source_mix={key: int(value) for key, value in manifest.get("trainable_source_mix", {}).items()},
        )
        return FeatureBuildResult(data=data, summary=summary)

    def _build_symbol_rows(
        self,
        *,
        symbol: str,
        ticker: str,
        snapshot_rows_path: Path,
        interval_seconds: int,
    ) -> pd.DataFrame:
        table = pq.read_table(snapshot_rows_path, filters=[("symbol", "=", symbol)])
        if table.num_rows <= 0:
            return pd.DataFrame()
        frame = table.to_pandas()
        if frame.empty:
            return pd.DataFrame()

        frame.sort_values("timestamp_ms", inplace=True, kind="mergesort")
        frame.reset_index(drop=True, inplace=True)

        close = frame["close"].astype("float64")
        volume = frame["volume"].astype("float64")
        ret_1 = close.pct_change()
        combined_is_synthetic = frame["source"].isin(["synthetic", "synthetic_gap_fill"]).astype("float64")
        is_gap_fill = (frame["source"] == "synthetic_gap_fill").astype("float64")

        features = pd.DataFrame(index=frame.index)
        for window in (1, 4, 16, 48, 96):
            features[f"combined_ret_{window}"] = _return(close, window)
        for window in (4, 16, 48, 96):
            features[f"combined_trend_{window}"] = ret_1.rolling(window, min_periods=window).mean()
            features[f"combined_vol_{window}"] = ret_1.rolling(window, min_periods=window).std(ddof=0)
            features[f"volume_ratio_{window}"] = _ratio_to_rolling_mean(volume, window)
        features["ema_ratio_8_32"] = _ema_ratio(close, 8, 32)
        features["ema_ratio_16_64"] = _ema_ratio(close, 16, 64)
        features["ema_ratio_32_128"] = _ema_ratio(close, 32, 128)
        features["rsi_14"] = _rsi(close, 14)
        features["rsi_28"] = _rsi(close, 28)
        for window in (16, 48, 96):
            mean = close.rolling(window, min_periods=window).mean()
            std = close.rolling(window, min_periods=window).std(ddof=0)
            features[f"dist_mean_{window}"] = (close / mean) - 1.0
            features[f"zscore_{window}"] = (close - mean) / std.replace(0.0, np.nan)
        features["is_ndax"] = (frame["source"] == "ndax").astype("float64")
        features["is_synthetic"] = (frame["source"] == "synthetic").astype("float64")
        features["is_synthetic_gap_fill"] = is_gap_fill
        features["effective_monthly_weight"] = frame["effective_monthly_weight"].astype("float64")
        features["supervised_row_weight_feature"] = frame["supervised_row_weight"].astype("float64")
        for window in (16, 48, 96):
            features[f"synthetic_share_{window}"] = combined_is_synthetic.rolling(window, min_periods=1).mean()
            features[f"gap_fill_count_{window}"] = is_gap_fill.rolling(window, min_periods=1).sum()

        raw_binance = self._read_close_series(self._data_root / "raw" / "binance" / "15m" / f"{ticker}USDT.parquet")
        raw_ndax = self._read_close_series(self._data_root / "raw" / "ndax" / "15m" / f"{symbol}.parquet")
        fx_series = self._fx_series()

        timestamp_values = frame["timestamp_ms"].astype("int64").to_numpy()
        binance_close = pd.Series(raw_binance.reindex(timestamp_values).to_numpy(), index=frame.index, dtype="float64")
        ndax_close = pd.Series(raw_ndax.reindex(timestamp_values).to_numpy(), index=frame.index, dtype="float64")
        fx_close = pd.Series(fx_series.reindex(timestamp_values).to_numpy(), index=frame.index, dtype="float64")

        binance_ret_1 = _return(binance_close, 1)
        features["binance_ret_1"] = binance_ret_1
        features["binance_ret_4"] = _return(binance_close, 4)
        features["binance_ret_16"] = _return(binance_close, 16)
        features["binance_vol_1"] = binance_ret_1.abs()
        features["binance_vol_4"] = binance_ret_1.rolling(4, min_periods=4).std(ddof=0)
        features["binance_vol_16"] = binance_ret_1.rolling(16, min_periods=16).std(ddof=0)

        features["ndax_ret_1"] = _return(ndax_close, 1)
        features["ndax_ret_4"] = _return(ndax_close, 4)
        features["ndax_ret_16"] = _return(ndax_close, 16)
        ndax_present = ndax_close.notna().astype("float64")
        features["ndax_present_1"] = ndax_present
        features["ndax_present_4"] = ndax_present.rolling(4, min_periods=1).mean()
        features["ndax_present_16"] = ndax_present.rolling(16, min_periods=1).mean()

        basis = ndax_close / (binance_close * fx_close)
        basis = basis.where((ndax_close > 0) & (binance_close > 0) & (fx_close > 0))
        features["basis_level"] = basis
        basis_mean = basis.rolling(96, min_periods=96).mean()
        basis_std = basis.rolling(96, min_periods=96).std(ddof=0)
        features["basis_zscore_96"] = (basis - basis_mean) / basis_std.replace(0.0, np.nan)
        features["binance_ctx_available"] = binance_close.notna().astype("float64")
        features["ndax_ctx_available"] = ndax_close.notna().astype("float64")
        features["basis_available"] = basis.notna().astype("float64")

        for column in FEATURE_COLUMNS:
            features[column] = features[column].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype("float32")

        trainable_mask = frame["row_status"] == "trainable"
        if not bool(trainable_mask.any()):
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "timestamp_ms",
                    "month",
                    "source",
                    "y",
                    "forward_return",
                    "supervised_row_weight",
                    *FEATURE_COLUMNS,
                ]
            )
        filtered = frame.loc[trainable_mask].reset_index(drop=True)
        metadata = pd.DataFrame(
            {
                "symbol": filtered["symbol"].astype(str),
                "timestamp_ms": filtered["timestamp_ms"].astype("int64"),
                "month": pd.to_datetime(filtered["timestamp_ms"], unit="ms", utc=True).dt.strftime("%Y-%m"),
                "source": filtered["source"].astype(str),
                "y": pd.to_numeric(filtered["y"], errors="raise").astype("int8"),
                "forward_return": filtered["forward_return"].astype("float64"),
                "supervised_row_weight": filtered["supervised_row_weight"].astype("float64"),
            }
        )
        output = pd.concat(
            [
                metadata,
                features.loc[trainable_mask].reset_index(drop=True),
            ],
            axis=1,
        )
        return output

    def _fx_series(self) -> pd.Series:
        if self._fx_series_cache is None:
            self._fx_series_cache = self._read_close_series(self._data_root / "raw" / "ndax" / "15m" / "USDTCAD.parquet")
        return self._fx_series_cache

    @staticmethod
    def _read_close_series(path: Path) -> pd.Series:
        if not path.exists():
            return pd.Series(dtype="float64")
        table = pq.read_table(path, columns=["timestamp_ms", "close"])
        frame = table.to_pandas()
        if frame.empty:
            return pd.Series(dtype="float64")
        frame.sort_values("timestamp_ms", inplace=True, kind="mergesort")
        frame = frame.drop_duplicates(subset=["timestamp_ms"], keep="last")
        return pd.Series(frame["close"].astype("float64").to_numpy(), index=frame["timestamp_ms"].astype("int64"))


def _return(series: pd.Series, window: int) -> pd.Series:
    return (series / series.shift(window)) - 1.0


def _ratio_to_rolling_mean(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window, min_periods=window).mean()
    return series / mean.replace(0.0, np.nan)


def _ema_ratio(series: pd.Series, fast_span: int, slow_span: int) -> pd.Series:
    ema_fast = series.ewm(span=fast_span, adjust=False, min_periods=fast_span).mean()
    ema_slow = series.ewm(span=slow_span, adjust=False, min_periods=slow_span).mean()
    return (ema_fast / ema_slow) - 1.0


def _rsi(series: pd.Series, window: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(window, min_periods=window).mean()
    avg_loss = loss.rolling(window, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))
