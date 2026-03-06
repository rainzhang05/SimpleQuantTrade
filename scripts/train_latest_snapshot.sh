#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

FOLDS="${1:-12}"
UNIVERSE="${2:-V1}"

LATEST_SNAPSHOT="$(find data/snapshots -mindepth 1 -maxdepth 1 -type d -exec basename {} \\; | sort | tail -n 1)"

if [[ -z "${LATEST_SNAPSHOT}" ]]; then
  echo "No local snapshots found under data/snapshots" >&2
  exit 1
fi

echo "training_snapshot=${LATEST_SNAPSHOT}"
python3 -m qtbot train --snapshot "${LATEST_SNAPSHOT}" --folds "${FOLDS}" --universe "${UNIVERSE}"
