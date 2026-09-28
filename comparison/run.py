"""別network namespaceの2端点で、独自L3とLinux UDP/IPv4を同じ負荷で測る。"""
import argparse
import hashlib
import json
import os
import signal
from pathlib import Path
import subprocess
import time
import uuid

# 再現に必要なサイズ・到着率を固定し、1条件だけのピーク値を性能と呼ばない。
WORKLOADS = [
    dict(name="idle", short_rate=100, bulk_rate=0, short_bytes=128, bulk_bytes=1200),
    dict(name="short_10k", short_rate=10_000, bulk_rate=0, short_bytes=64, bulk_bytes=1200),
    dict(name="bulk_10k", short_rate=0, bulk_rate=10_000, short_bytes=128, bulk_bytes=1400),
    dict(name="mixed_1mbps", short_rate=100, bulk_rate=500, short_bytes=128, bulk_bytes=1200),
]
READY_TIMEOUT = 5
RECEIVER_MARGIN_MS = 2000
IMAGE = "amitoki-l3-test:local"


def command(*arguments, **options):
    return subprocess.run(arguments, check=True, capture_output=True, text=True, **options).stdout


def config(node, peer_mac, limited=False):
    return dict(node=node, links=[dict(interface="eth0", peer_mac=peer_mac)],
                routes=[dict(destination=3-node, path=1, interface="eth0")], scheduler="priority",
                bytes_per_second=125_000 if limited else 100_000_000,
                clock=dict(authority=2))


def arguments_for(workload):
    return [argument for key in ("short_rate", "bulk_rate", "short_bytes", "bulk_bytes")
            for argument in ("--" + key.replace("_", "-"), str(workload[key]))]


def wait_ready(container, path, process):
    deadline = time.monotonic() + READY_TIMEOUT
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("受信プロセスが準備前に終了しました")
        if subprocess.run(["docker", "exec", container, "test", "-f", path], capture_output=True).returncode == 0:
            return
        time.sleep(0.02)
    raise RuntimeError("受信プロセスの準備がタイムアウトしました")


def shape_udp(container, enabled):
    if not enabled:
        return
    command("docker", "exec", container, "tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:",
            "tbf", "rate", "1mbit", "burst", "1478", "limit", str(192 * 1478))
    command("docker", "exec", container, "tc", "qdisc", "add", "dev", "eth0", "parent", "1:1", "handle", "10:",
            "pfifo", "limit", "192")


def run_case(environment, workload, transport):
    directory = environment["directory"]
    a, b = environment["containers"]
    limited = workload["name"] == "mixed_1mbps"
    (directory / "a.json").write_text(json.dumps(config(1, environment["macs"][1], limited)))
    (directory / "b.json").write_text(json.dumps(config(2, environment["macs"][0])))
    prefix = "/results/" + str(directory.relative_to(environment["root"]))
    duration = environment["duration_ms"]
    executable = "/work/target/release/amitoki-l3" + ("-udp" if transport == "udp" else "")
    if transport == "udp":
        connections = [["--bind", f"{environment['ips'][index]}:47000", "--peer", f"{environment['ips'][1-index]}:47000"] for index in range(2)]
    else:
        connections = [["--config", prefix + f"/{name}.json"] for name in ("a", "b")]
    receive = ["docker", "exec", b, executable, "receiver", *connections[1], "--duration-ms", str(duration + RECEIVER_MARGIN_MS),
               "--output", prefix + "/b.report.json", "--ready", prefix + "/b.ready.json"]
    if transport == "l3":
        receive += ["--short-credits-per-second", "100000", "--bulk-credits-per-second", "100000"]
    bench = ["docker", "exec", a, executable, "bench", *connections[0], "--duration-ms", str(duration),
             *arguments_for(workload), "--output", prefix + "/a.report.json"]
    if transport == "l3":
        bench += ["--delivery", "deadline", "--peer", "2", "--replica-bytes-per-second", "0", "--retries", "0"]
    shape_udp(a, limited and transport == "udp")
    try:
        with (directory / "b.log").open("w") as log:
            receiver = subprocess.Popen(receive, stdout=log, stderr=subprocess.STDOUT)
            try:
                wait_ready(b, prefix + "/b.ready.json", receiver)
                completed = subprocess.run(bench, capture_output=True, text=True, timeout=duration / 1000 + 10)
                (directory / "a.log").write_text(completed.stdout + completed.stderr)
                completed.check_returncode()
                if receiver.wait(timeout=RECEIVER_MARGIN_MS / 1000 + 5):
                    raise RuntimeError("受信プロセスが失敗しました")
            finally:
                # 受信側は有限時間で終了する。docker execだけを切って孤児にしない。
                receiver.wait(timeout=duration / 1000 + RECEIVER_MARGIN_MS / 1000 + 5)
    finally:
        if limited and transport == "udp":
            (directory / "qdisc.txt").write_text(command("docker", "exec", a, "tc", "-s", "qdisc", "show", "dev", "eth0"))
            command("docker", "exec", a, "tc", "qdisc", "del", "dev", "eth0", "root")
    reports = {name: json.loads((directory / f"{name}.report.json").read_text()) for name in ("a", "b")}
    verify(reports, workload, transport)
    return dict(workload=workload, transport=transport, reports=reports)


def verify(reports, workload, transport):
    bench = reports["a"]["benchmark"]
    if bench["invalid_acks"] or any(report["network"]["malformed"] for report in reports.values()):
        raise RuntimeError("パケットまたはACKの整合性が崩れました")
    for index, name in enumerate(("short", "bulk")):
        traffic = bench[name]
        expected = workload[name + "_rate"] * bench["duration_us"] // 1_000_000
        if traffic["offered"] != expected or not 0 <= traffic["acknowledged"] <= reports["b"]["receiver"]["delivered"][index] <= traffic["sent"] <= expected:
            raise RuntimeError("配送数が不整合です")
        deadline = 20_000 if name == "short" else 200_000
        if traffic["rtt_us"]["max"] is not None and traffic["rtt_us"]["max"] >= deadline:
            raise RuntimeError("期限後のACKを成功に数えています")
    if transport == "l3" and (reports["a"]["clock_sync"]["samples"]["accepted"] == 0 or reports["a"]["clock_sync"]["reading"] is None):
        raise RuntimeError("時計同期ができていません")
    if sum(bench[name]["acknowledged"] for name in ("short", "bulk")) == 0:
        raise RuntimeError("ACKを一件も確認できませんでした")


def report(directory, outcomes):
    lines = ["# UDP/IPv4との比較", "", "Dockerの内部Ethernet bridgeで2つのnetwork namespaceを接続。両方式ともRust、同じホストとリンクを順番に使用。",
             "独自L3は時計同期・credit・優先制御あり。UDPはLinux UDP socketとFIFO。再送・複製は両方式ともなし。",
             "短文の期限20ms、bulkは200ms。RTTは送信予定からACKまで。p99は期限内ACKのみなのでACK率と併記。",
             "1Mbps条件ではL3の送信キューとUDPのLinux TBF/pfifoをそれぞれ制限。キュー実装も含む比較で、ヘッダだけの差ではない。",
             "有効Mbpsは期限内ACKされた本文ビット/計測時間（生成後の最大200msの応答待ちを含む）。物理Ethernetの限界速度や無損失最大速度ではない。", "",
             "| 条件 | 回 | 方式 | short ACK率 | short p99 µs | bulk ACK率 | 有効Mbps | ACK pps |", "|---|---:|---|---:|---:|---:|---:|---:|"]
    for outcome in outcomes:
        bench = outcome["reports"]["a"]["benchmark"]
        workload = outcome["workload"]
        total = sum(bench[name]["acknowledged"] for name in ("short", "bulk"))
        goodput = sum(bench[name]["acknowledged"] * workload[name + "_bytes"] * 8 for name in ("short", "bulk")) / bench["elapsed_us"]
        ratios = ["—" if bench[name]["on_time_ratio"] is None else f"{bench[name]['on_time_ratio']:.1%}" for name in ("short", "bulk")]
        lines.append(f"| {workload['name']} | {outcome['repetition']} | {outcome['transport']} | {ratios[0]} | {bench['short']['rtt_us']['p99']} | {ratios[1]} | {goodput:.3f} | {total * 1_000_000 / bench['elapsed_us']:.0f} |")
    (directory / "report.md").write_text("\n".join(lines) + "\n")


def install_stop_handler():
    def stop(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)


def main():
    install_stop_handler()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration-ms", type=int, default=3000)
    options = parser.parse_args()
    if not 1 <= options.repetitions <= 10 or not 1000 <= options.duration_ms <= 30_000:
        parser.error("repetitionsは1〜10、duration-msは1000〜30000です")
    directory = options.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    repository = Path(__file__).resolve().parents[1]
    name = "amitoki-l3-compare-" + uuid.uuid4().hex[:12]
    containers = [name + "-a", name + "-b"]
    command("docker", "network", "create", "--internal", name)
    outcomes = []
    try:
        for container in containers:
            command("docker", "run", "-d", "--name", container, "--network", name,
                    "--cap-add", "NET_RAW", "--cap-add", "NET_ADMIN",
                    "--mount", f"type=bind,src={repository},dst=/work,readonly",
                    "--mount", f"type=bind,src={directory},dst=/results", IMAGE, "sleep", "1800")
        metadata = [json.loads(command("docker", "inspect", container))[0]["NetworkSettings"]["Networks"][name] for container in containers]
        environment = dict(containers=containers, macs=[node["MacAddress"] for node in metadata],
                           ips=[node["IPAddress"] for node in metadata], root=directory, duration_ms=options.duration_ms)
        for repetition in range(1, options.repetitions + 1):
            for workload in WORKLOADS:
                # 実行順による温度・キャッシュの偏りを固定しない。
                for transport in (("udp", "l3") if repetition % 2 else ("l3", "udp")):
                    path = directory / f"{repetition:02d}-{workload['name']}-{transport}"
                    path.mkdir()
                    outcome = run_case(dict(environment, directory=path), workload, transport)
                    outcome["repetition"] = repetition
                    outcomes.append(outcome)
                    (directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
                    print(f"{repetition}: {workload['name']}: {transport}: ACK " + str([outcome["reports"]["a"]["benchmark"][kind]["acknowledged"] for kind in ("short", "bulk")]), flush=True)
        report(directory, outcomes)
        verification = dict(status="passed", runs=len(outcomes), duration_ms=options.duration_ms,
                            topology="two network namespaces; internal Docker Ethernet bridge", machine=os.uname().machine,
                            kernel=os.uname().release, containers=metadata,
                            binaries={binary:hashlib.sha256((repository / "target/release" / binary).read_bytes()).hexdigest() for binary in ("amitoki-l3", "amitoki-l3-udp")})
        (directory / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
    finally:
        for container in containers:
            subprocess.run(["docker", "exec", "--user", "0", container, "chown", "-R", f"{os.getuid()}:{os.getgid()}", "/results"], capture_output=True)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True)
        subprocess.run(["docker", "network", "rm", name], capture_output=True)


if __name__ == "__main__":
    main()
