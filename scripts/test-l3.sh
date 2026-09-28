#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

directory="${1:-artifacts/l3/$(TZ=Asia/Tokyo date +%Y-%m-%d/%H%M%S)}"
mkdir -p "$directory"
directory=$(realpath "$directory")
container_name="amitoki-l3-$(id -u)-$$"
cleanup() {
  docker rm --force "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cargo build --release -p amitoki-l3-lab --locked
docker build --tag amitoki-l3-test:local --file lab/Dockerfile .
# 実験用vethとqdiscだけを作る。ホストのNIC・経路には接続しない。
docker run --rm --name "$container_name" --network none --cap-add NET_ADMIN --cap-add NET_RAW \
  --sysctl net.ipv6.conf.all.disable_ipv6=1 --sysctl net.ipv6.conf.default.disable_ipv6=1 \
  --mount "type=bind,src=$PWD,dst=/work,readonly" \
  --mount "type=bind,src=$directory,dst=/results" \
  --env "L3_OWNER_UID=$(id -u)" --env "L3_OWNER_GID=$(id -g)" \
  amitoki-l3-test:local python3 /work/lab/run.py \
  --binary /work/target/release/amitoki-l3 --directory /results \
  --repetitions "${L3_REPETITIONS:-3}" --duration-ms "${L3_DURATION_MS:-3000}" --suite "${L3_SUITE:-all}"
