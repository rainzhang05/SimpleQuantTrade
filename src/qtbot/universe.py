"""Universe definitions and NDAX market filtering."""

from __future__ import annotations

from dataclasses import dataclass


TOP_20_MARKET_COINS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "XRP",
    "SOL",
    "ADA",
    "DOGE",
    "TRX",
    "AVAX",
    "SHIB",
    "LINK",
    "DOT",
    "BCH",
    "LTC",
    "XLM",
    "TON",
    "UNI",
    "NEAR",
    "ATOM",
    "ETC",
    "HBAR",
)

LOCKED_COINS: frozenset[str] = frozenset({"BTC", "ETH"})


@dataclass(frozen=True)
class UniverseEntry:
    ticker: str
    ndax_symbol: str
    instrument_id: int


@dataclass(frozen=True)
class UniverseResolution:
    tradable: list[UniverseEntry]
    skipped: dict[str, str]


def resolve_tradable_universe(instruments: list[dict[str, object]]) -> UniverseResolution:
    """Resolve top-20-minus-locked universe against live NDAX CAD instruments."""
    cad_by_ticker: dict[str, UniverseEntry] = {}
    for instrument in instruments:
        base_symbol = str(instrument.get("Product1Symbol", "")).upper()
        quote_symbol = str(instrument.get("Product2Symbol", "")).upper()
        symbol = str(instrument.get("Symbol", "")).upper()
        instrument_id = int(instrument.get("InstrumentId", 0) or 0)
        if not base_symbol or quote_symbol != "CAD" or not symbol or instrument_id <= 0:
            continue
        cad_by_ticker[base_symbol] = UniverseEntry(
            ticker=base_symbol,
            ndax_symbol=symbol,
            instrument_id=instrument_id,
        )

    tradable: list[UniverseEntry] = []
    skipped: dict[str, str] = {}
    for ticker in TOP_20_MARKET_COINS:
        if ticker in LOCKED_COINS:
            skipped[ticker] = "locked"
            continue
        entry = cad_by_ticker.get(ticker)
        if entry is None:
            skipped[ticker] = "no_ndax_cad_pair"
            continue
        tradable.append(entry)

    return UniverseResolution(tradable=tradable, skipped=skipped)
