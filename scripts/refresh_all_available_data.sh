#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

TO_DATE="${1:-$(date -u +%F)}"
ASOF_UTC="${ASOF_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
TIMEFRAME="${TIMEFRAME:-15m}"
COMBINED_FROM="${COMBINED_FROM:-2021-01-01}"

echo "[1/6] Backfilling all available raw data through ${TO_DATE} UTC"
python3 -m qtbot data-backfill --from earliest --to "${TO_DATE}" --timeframe "${TIMEFRAME}" --sources ndax,kraken,binance

echo "[2/6] Inspecting raw-source coverage"
python3 -m qtbot data-status --timeframe "${TIMEFRAME}" --dataset all

echo "[3/6] Rebuilding combined dataset"
python3 -m qtbot data-build-combined --from "${COMBINED_FROM}" --to "${TO_DATE}" --timeframe "${TIMEFRAME}"

echo "[4/6] Verifying combined coverage"
python3 -m qtbot data-status --timeframe "${TIMEFRAME}" --dataset combined

echo "[5/6] Recomputing monthly synthetic weights"
python3 -m qtbot data-calibrate-weights --from "${COMBINED_FROM}" --to "${TO_DATE}" --timeframe "${TIMEFRAME}" --refresh monthly

echo "[6/6] Building sealed snapshot at ${ASOF_UTC}"
python3 -m qtbot build-snapshot --asof "${ASOF_UTC}" --timeframe "${TIMEFRAME}"

LATEST_SNAPSHOT="$(find data/snapshots -mindepth 1 -maxdepth 1 -type d -exec basename {} \\; | sort | tail -n 1)"
echo "latest_snapshot=${LATEST_SNAPSHOT}"
