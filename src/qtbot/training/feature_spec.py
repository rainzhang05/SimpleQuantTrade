"""Deterministic feature specification for Phase 6 training."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json

FEATURE_SPEC_VERSION = "training_feature_spec_v1"
FEATURE_COLUMNS = [
    "combined_ret_1",
    "combined_ret_4",
    "combined_ret_16",
    "combined_ret_48",
    "combined_ret_96",
    "combined_trend_4",
    "combined_trend_16",
    "combined_trend_48",
    "combined_trend_96",
    "combined_vol_4",
    "combined_vol_16",
    "combined_vol_48",
    "combined_vol_96",
    "volume_ratio_4",
    "volume_ratio_16",
    "volume_ratio_48",
    "volume_ratio_96",
    "ema_ratio_8_32",
    "ema_ratio_16_64",
    "ema_ratio_32_128",
    "rsi_14",
    "rsi_28",
    "dist_mean_16",
    "dist_mean_48",
    "dist_mean_96",
    "zscore_16",
    "zscore_48",
    "zscore_96",
    "is_ndax",
    "is_synthetic",
    "is_synthetic_gap_fill",
    "effective_monthly_weight",
    "supervised_row_weight_feature",
    "synthetic_share_16",
    "synthetic_share_48",
    "synthetic_share_96",
    "gap_fill_count_16",
    "gap_fill_count_48",
    "gap_fill_count_96",
    "binance_ret_1",
    "binance_ret_4",
    "binance_ret_16",
    "binance_vol_1",
    "binance_vol_4",
    "binance_vol_16",
    "ndax_ret_1",
    "ndax_ret_4",
    "ndax_ret_16",
    "ndax_present_1",
    "ndax_present_4",
    "ndax_present_16",
    "basis_level",
    "basis_zscore_96",
    "binance_ctx_available",
    "ndax_ctx_available",
    "basis_available",
]


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    family: str
    description: str


FEATURE_DEFINITIONS = [
    FeatureDefinition("combined_ret_1", "combined", "Combined close return over 1 bar."),
    FeatureDefinition("combined_ret_4", "combined", "Combined close return over 4 bars."),
    FeatureDefinition("combined_ret_16", "combined", "Combined close return over 16 bars."),
    FeatureDefinition("combined_ret_48", "combined", "Combined close return over 48 bars."),
    FeatureDefinition("combined_ret_96", "combined", "Combined close return over 96 bars."),
    FeatureDefinition("combined_trend_4", "combined", "Combined close relative to 4-bar mean."),
    FeatureDefinition("combined_trend_16", "combined", "Combined close relative to 16-bar mean."),
    FeatureDefinition("combined_trend_48", "combined", "Combined close relative to 48-bar mean."),
    FeatureDefinition("combined_trend_96", "combined", "Combined close relative to 96-bar mean."),
    FeatureDefinition("combined_vol_4", "combined", "Combined return volatility over 4 bars."),
    FeatureDefinition("combined_vol_16", "combined", "Combined return volatility over 16 bars."),
    FeatureDefinition("combined_vol_48", "combined", "Combined return volatility over 48 bars."),
    FeatureDefinition("combined_vol_96", "combined", "Combined return volatility over 96 bars."),
    FeatureDefinition("volume_ratio_4", "combined", "Volume relative to 4-bar mean."),
    FeatureDefinition("volume_ratio_16", "combined", "Volume relative to 16-bar mean."),
    FeatureDefinition("volume_ratio_48", "combined", "Volume relative to 48-bar mean."),
    FeatureDefinition("volume_ratio_96", "combined", "Volume relative to 96-bar mean."),
    FeatureDefinition("ema_ratio_8_32", "combined", "EMA ratio 8 over 32 bars."),
    FeatureDefinition("ema_ratio_16_64", "combined", "EMA ratio 16 over 64 bars."),
    FeatureDefinition("ema_ratio_32_128", "combined", "EMA ratio 32 over 128 bars."),
    FeatureDefinition("rsi_14", "combined", "RSI over 14 bars."),
    FeatureDefinition("rsi_28", "combined", "RSI over 28 bars."),
    FeatureDefinition("dist_mean_16", "combined", "Distance to 16-bar mean."),
    FeatureDefinition("dist_mean_48", "combined", "Distance to 48-bar mean."),
    FeatureDefinition("dist_mean_96", "combined", "Distance to 96-bar mean."),
    FeatureDefinition("zscore_16", "combined", "Close z-score over 16 bars."),
    FeatureDefinition("zscore_48", "combined", "Close z-score over 48 bars."),
    FeatureDefinition("zscore_96", "combined", "Close z-score over 96 bars."),
    FeatureDefinition("is_ndax", "source", "Current row source flag for NDAX."),
    FeatureDefinition("is_synthetic", "source", "Current row source flag for synthetic normalized Binance."),
    FeatureDefinition("is_synthetic_gap_fill", "source", "Current row source flag for synthetic gap fill."),
    FeatureDefinition("effective_monthly_weight", "source", "Effective monthly synthetic row weight."),
    FeatureDefinition("supervised_row_weight_feature", "source", "Effective supervised row weight used in training."),
    FeatureDefinition("synthetic_share_16", "source", "Synthetic share over the last 16 bars."),
    FeatureDefinition("synthetic_share_48", "source", "Synthetic share over the last 48 bars."),
    FeatureDefinition("synthetic_share_96", "source", "Synthetic share over the last 96 bars."),
    FeatureDefinition("gap_fill_count_16", "source", "Synthetic gap-fill count over the last 16 bars."),
    FeatureDefinition("gap_fill_count_48", "source", "Synthetic gap-fill count over the last 48 bars."),
    FeatureDefinition("gap_fill_count_96", "source", "Synthetic gap-fill count over the last 96 bars."),
    FeatureDefinition("binance_ret_1", "raw_binance", "Raw Binance close return over 1 bar."),
    FeatureDefinition("binance_ret_4", "raw_binance", "Raw Binance close return over 4 bars."),
    FeatureDefinition("binance_ret_16", "raw_binance", "Raw Binance close return over 16 bars."),
    FeatureDefinition("binance_vol_1", "raw_binance", "Raw Binance absolute 1-bar return."),
    FeatureDefinition("binance_vol_4", "raw_binance", "Raw Binance return volatility over 4 bars."),
    FeatureDefinition("binance_vol_16", "raw_binance", "Raw Binance return volatility over 16 bars."),
    FeatureDefinition("ndax_ret_1", "raw_ndax", "Raw NDAX close return over 1 bar."),
    FeatureDefinition("ndax_ret_4", "raw_ndax", "Raw NDAX close return over 4 bars."),
    FeatureDefinition("ndax_ret_16", "raw_ndax", "Raw NDAX close return over 16 bars."),
    FeatureDefinition("ndax_present_1", "raw_ndax", "NDAX availability flag at the current bar."),
    FeatureDefinition("ndax_present_4", "raw_ndax", "NDAX availability share over 4 bars."),
    FeatureDefinition("ndax_present_16", "raw_ndax", "NDAX availability share over 16 bars."),
    FeatureDefinition("basis_level", "basis", "Observed NDAX/Binance close basis ratio."),
    FeatureDefinition("basis_zscore_96", "basis", "Basis z-score over 96 bars."),
    FeatureDefinition("binance_ctx_available", "availability", "Raw Binance context available at current bar."),
    FeatureDefinition("ndax_ctx_available", "availability", "Raw NDAX context available at current bar."),
    FeatureDefinition("basis_available", "availability", "Observed basis available at current bar."),
]


def feature_spec_payload() -> dict[str, object]:
    return {
        "version": FEATURE_SPEC_VERSION,
        "feature_count": len(FEATURE_DEFINITIONS),
        "features": [asdict(item) for item in FEATURE_DEFINITIONS],
    }


def feature_spec_hash() -> str:
    payload = json.dumps(feature_spec_payload(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
