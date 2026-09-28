"""外部非接続のコンテナ内に、共有L2のない2経路を作る。"""
import json
import subprocess

LINKS = [("a1", "r1a"), ("r1b", "b1"), ("a2", "r2a"), ("r2b", "b2")]
MACS = {name: f"02:88:b5:00:00:{index:02x}" for index, name in enumerate(
    [name for pair in LINKS for name in pair], start=1)}
PEERS = {left: right for pair in LINKS for left, right in [pair, pair[::-1]]}
# 1Mbpsに揃え、背景転送がある場合に実験用ルーターのキューを混雑させる。
ROUTER_BYTES_PER_SECOND = 125_000
ENDPOINT_BYTES_PER_SECOND = 100_000_000


def command(*arguments):
    return subprocess.run(arguments, check=True, capture_output=True, text=True).stdout


def create_links():
    for left, right in LINKS:
        command("ip", "link", "add", left, "type", "veth", "peer", "name", right)
        for name in (left, right):
            command("ip", "link", "set", name, "address", MACS[name])
            command("ip", "link", "set", name, "up")
            addresses = json.loads(command("ip", "-j", "address", "show", "dev", name))[0]["addr_info"]
            if addresses:
                raise RuntimeError(f"実験リンク{name}にIPアドレスがあります: {addresses}")


def remove_links():
    for left, _ in LINKS:
        command("ip", "link", "delete", left)


def configure(directory, scheduler):
    definitions = {
        "a": (1, ["a1", "a2"], [(2, 1, "a1"), (2, 2, "a2")]),
        "b": (2, ["b1", "b2"], [(1, 1, "b1"), (1, 2, "b2")]),
        "r1": (11, ["r1a", "r1b"], [(1, 1, "r1a"), (2, 1, "r1b")]),
        "r2": (12, ["r2a", "r2b"], [(1, 2, "r2a"), (2, 2, "r2b")]),
    }
    for name, (node, interfaces, routes) in definitions.items():
        config = {
            "node": node,
            "links": [{"interface": interface, "peer_mac": MACS[PEERS[interface]]} for interface in interfaces],
            "routes": [{"destination": destination, "path": path, "interface": interface}
                       for destination, path, interface in routes],
            "scheduler": scheduler if name.startswith("r") else "priority",
            "bytes_per_second": ROUTER_BYTES_PER_SECOND if name.startswith("r") else ENDPOINT_BYTES_PER_SECOND,
        }
        (directory / f"{name}.json").write_text(json.dumps(config, indent=2) + "\n")


def impair_primary(*, delay_ms=0, loss=False):
    if delay_ms:
        command("tc", "qdisc", "replace", "dev", "r1b", "root", "netem", "delay", f"{delay_ms}ms")
    elif loss:
        command("tc", "qdisc", "replace", "dev", "r1b", "root", "netem", "loss", "100%")


def configure_clocks(directory, scenario):
    # 同じカーネルの時計で暗黙に動いてしまう実装を検出する。
    offsets = {"a": 5_000_000, "b": -3_000_000, "r1": 1_500_000, "r2": -500_000}
    for name, offset in offsets.items():
        path = directory / f"{name}.json"
        config = json.loads(path.read_text())
        config["clock"] = {
            "authority": 2,
            "max_age_us": 200_000 if scenario.stop_clock_replies else 1_000_000,
            "simulation": {"offset_us": offset, "drift_ppm": scenario.clock_drift_ppm * (1 if name in ("a", "r1") else -1)},
        }
        if name == "b":
            config["routes"] += [
                {"destination": 11, "path": 1, "interface": "b1"},
                {"destination": 12, "path": 2, "interface": "b2"},
            ]
        path.write_text(json.dumps(config, indent=2) + "\n")


def block_clock_replies():
    for interface in ("b1", "b2"):
        command("tc", "qdisc", "add", "dev", interface, "clsact")
        # tc u32のoffsetはEthernet payloadの先頭。kind=6の同期応答だけを落とす。
        command("tc", "filter", "add", "dev", interface, "egress", "protocol", "0x88b5",
                "u32", "match", "u8", "6", "0xff", "at", "5", "action", "drop")


def restore_clock_replies():
    for interface in ("b1", "b2"):
        command("tc", "qdisc", "del", "dev", interface, "clsact")
