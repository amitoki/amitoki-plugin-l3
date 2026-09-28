#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
directory="${1:-artifacts/fabric/$(TZ=Asia/Tokyo date +%Y-%m-%d/%H%M%S)}"
mkdir -p "$directory"
directory=$(realpath "$directory")
container_name="amitoki-fabric-$(id -u)-$$"
cleanup() { docker rm --force "$container_name" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
cargo build --release --workspace --locked
docker build --tag amitoki-l3-test:local --file lab/Dockerfile .
docker run --rm --name "$container_name" --network none --cap-add NET_ADMIN --cap-add NET_RAW \
  --sysctl net.ipv6.conf.all.disable_ipv6=1 --sysctl net.ipv6.conf.default.disable_ipv6=1 \
  --mount "type=bind,src=$PWD,dst=/work,readonly" --mount "type=bind,src=$directory,dst=/results" \
  amitoki-l3-test:local python3 /work/lab/fabric.py --binary /work/target/release/amitoki-l3 --directory /results
