#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release -p amitoki-l3-lab --locked
docker build --tag amitoki-l3-test:local --file lab/Dockerfile .
exec python3 comparison/run.py \
  --directory "${1:-artifacts/l3-compare/$(TZ=Asia/Tokyo date +%Y-%m-%d/%H%M%S)}" \
  --repetitions "${L3_REPETITIONS:-3}" --duration-ms "${L3_DURATION_MS:-3000}"
