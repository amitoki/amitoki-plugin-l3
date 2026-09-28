"""専用コンテナ内だけで3ノードのL3リンクを作り、SDK経由の中継を検証する。"""
import argparse
import json
import os
from pathlib import Path
import subprocess


def run(*arguments):
    return subprocess.check_output(arguments, text=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-binary', required=True)
    parser.add_argument('--directory', type=Path, required=True)
    options = parser.parse_args()
    results = options.directory
    options.directory = Path("/run/amitoki-l3-test")
    options.directory.mkdir(parents=True, exist_ok=True)
    options.directory.chmod(0o700)
    links = {node: [] for node in range(1, 4)}
    routes = {node: [] for node in range(1, 4)}
    for left, right in [(1, 2), (1, 3), (2, 3)]:
        interfaces = [f'l3{left}{right}', f'l3{right}{left}']
        macs = [f'02:88:b5:00:{node:02x}:{peer:02x}' for node, peer in [(left, right), (right, left)]]
        run('ip', 'link', 'add', interfaces[0], 'type', 'veth', 'peer', 'name', interfaces[1])
        for index, (node, peer) in enumerate([(left, right), (right, left)]):
            interface = interfaces[index]
            run('ip', 'link', 'set', interface, 'address', macs[index])
            run('ip', 'link', 'set', interface, 'up')
            assert not json.loads(run('ip', '-j', 'address', 'show', 'dev', interface))[0]['addr_info']
            links[node].append(dict(interface=interface, peer_mac=macs[1-index]))
            routes[node].append(dict(destination=peer, path=1, interface=interface))
    for node in range(1, 4):
        network = dict(node=node, links=links[node], routes=routes[node], scheduler='priority',
                       bytes_per_second=100_000_000, clock=dict(authority=2),
                       fabric=dict(adaptive_paths=True, telemetry=True, congestion_control=True, clock_independent=True))
        (options.directory / f'{node}.json').write_text(json.dumps(dict(network=network, peers=[peer for peer in range(1, 4) if peer != node])))
    for path in options.directory.glob("*.json"):
        destination = results / path.name
        destination.write_text(path.read_text())
        os.chown(destination, int(os.environ["L3_OWNER_UID"]), int(os.environ["L3_OWNER_GID"]))
    environment = dict(os.environ, L3_TEST_DIRECTORY=str(options.directory))
    subprocess.run([options.test_binary, '--ignored', '--nocapture'], env=environment, check=True)


if __name__ == '__main__':
    main()
