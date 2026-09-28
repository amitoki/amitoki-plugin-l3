#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release -p amitoki-l3-lab --locked
docker build --tag amitoki-l3-test:local --file lab/Dockerfile .
directory="${1:-artifacts/l3-tcp/$(TZ=Asia/Tokyo date +%Y-%m-%d/%H%M%S)}"
arguments=(--directory "$directory" --repetitions "${L3_REPETITIONS:-3}" --duration-ms "${L3_DURATION_MS:-3000}")
if [[ "${L3_TCP_QUICK:-0}" == 1 ]]; then arguments+=(--quick); fi
exec python3 comparison/reliable.py "${arguments[@]}"
