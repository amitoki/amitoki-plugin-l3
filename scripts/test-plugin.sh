#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
directory="${1:-artifacts/plugin/$(TZ=Asia/Tokyo date +%Y-%m-%d/%H%M%S)}"
mkdir -p "$directory"
directory=$(realpath "$directory")
cargo build --release -p amitoki-plugin-l3 --locked
cargo test --release -p amitoki-plugin-l3 --test relay_network --no-run --locked --message-format=json > "$directory/build.jsonl"
test_binary=$(python3 - "$directory/build.jsonl" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    item = json.loads(line)
    if item.get('target', {}).get('name') == 'relay_network' and item.get('executable'):
        print(item['executable'])
PY
)
docker build --tag amitoki-l3-test:local --file lab/Dockerfile .
container_name="amitoki-l3-plugin-$(id -u)-$$"
trap 'docker rm --force "$container_name" >/dev/null 2>&1 || true' EXIT INT TERM
docker run --rm --name "$container_name" --network none --cap-add NET_ADMIN --cap-add NET_RAW \
  --sysctl net.ipv6.conf.all.disable_ipv6=1 --sysctl net.ipv6.conf.default.disable_ipv6=1 \
  --env "L3_OWNER_UID=$(id -u)" --env "L3_OWNER_GID=$(id -g)" \
  --mount "type=bind,src=$PWD,dst=$PWD,readonly" \
  --mount "type=bind,src=$directory,dst=/results" \
  amitoki-l3-test:local python3 "$PWD/lab/plugin_network.py" --test-binary "$test_binary" --directory /results
