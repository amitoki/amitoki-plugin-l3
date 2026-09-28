"""L3の順序なし/順序あり、TCPの1接続/2接続を同じ負荷で比較する。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shlex
import subprocess
import uuid

from run import IMAGE, arguments_for, command, config, install_stop_handler, wait_ready
from reliable_metrics import measure, report

MODES = ("l3_unordered", "l3_ordered", "tcp_single", "tcp_split")
WORKLOADS = [
    dict(name="idle", short_rate=100, bulk_rate=0, short_bytes=128, bulk_bytes=1200, rate_mbps=0, loss_percent=0),
    dict(name="short_10k", short_rate=10_000, bulk_rate=0, short_bytes=64, bulk_bytes=1200, rate_mbps=0, loss_percent=0),
    dict(name="bulk_10k", short_rate=0, bulk_rate=10_000, short_bytes=128, bulk_bytes=1200, rate_mbps=0, loss_percent=0),
    dict(name="mixed_10mbps", short_rate=100, bulk_rate=1500, short_bytes=128, bulk_bytes=1200, rate_mbps=10, loss_percent=0),
    dict(name="loss_1pct", short_rate=100, bulk_rate=500, short_bytes=128, bulk_bytes=1200, rate_mbps=10, loss_percent=1),
]
TIMEOUT_MS = 10_000
RECEIVER_DURATION_MS = 30_000
# 実行順だけを再現する。netemの損失位置はランダムで、方式間でも同一ではない。
SEED = 20260928


def shape(container, workload):
    if not workload["rate_mbps"]:
        return
    options = ["delay", "1ms", "rate", f"{workload['rate_mbps']}mbit", "limit", "512"]
    if workload["loss_percent"]:
        options += ["loss", "random", f"{workload['loss_percent']}%"]
    command("docker", "exec", container, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", *options)


def run_case(environment, workload, *, mode, directory):
    a, b = environment["containers"]
    prefix = "/results/" + str(directory.relative_to(environment["directory"]))
    l3 = mode.startswith("l3")
    executable = "/work/target/release/amitoki-l3" + ("" if l3 else "-tcp")
    for index, name in enumerate(("a", "b")):
        node_config = config(index + 1, environment["macs"][1-index])
        if environment["clock_mode"] == "local":
            node_config.pop("clock")
        (directory / f"{name}.json").write_text(json.dumps(node_config))
    receive = [executable, "receiver", "--duration-ms", str(RECEIVER_DURATION_MS), "--output", prefix + "/b.report.json",
               "--ready", prefix + "/b.ready.json", "--receipt-log", prefix + "/receipts.jsonl"]
    bench = [executable, "bench", "--duration-ms", str(environment["duration_ms"]), *arguments_for(workload),
             "--delivery-timeout-ms", str(TIMEOUT_MS), "--output", prefix + "/a.report.json"]
    if l3:
        receive += ["--config", prefix + "/b.json", "--short-credits-per-second", "100000", "--bulk-credits-per-second", "100000"]
        ordering = "ordered" if mode == "l3_ordered" else "unordered"
        bench += ["--config", prefix + "/a.json", "--peer", "2", "--short-ordering", ordering, "--bulk-ordering", ordering]
    else:
        connections = "2" if mode == "tcp_split" else "1"
        receive += ["--bind", f"{environment['ips'][1]}:47000", "--connections", connections]
        bench += ["--peer", f"{environment['ips'][1]}:47000", "--connections", connections]
    # exec前のPIDを保存し、この受信プロセスだけへ終了通知する。
    shell = "echo $$ > " + shlex.quote(prefix + "/receiver.pid") + "\nexec " + shlex.join(receive)
    shape(a, workload)
    try:
        with (directory / "b.log").open("w") as log:
            receiver = subprocess.Popen(["docker", "exec", b, "sh", "-c", shell], stdout=log, stderr=subprocess.STDOUT)
            try:
                wait_ready(b, prefix + "/b.ready.json", receiver)
                completed = subprocess.run(["docker", "exec", a, *bench], capture_output=True, text=True,
                                           timeout=(environment["duration_ms"] + TIMEOUT_MS) / 1000 + 5)
                (directory / "a.log").write_text(completed.stdout + completed.stderr)
                sender_report = json.loads((directory / "a.report.json").read_text())
                bench_report = sender_report["reliable_benchmark" if l3 else "benchmark"]
                # 配送timeoutは比較結果として残す。reportの無い異常や矛盾は中断する。
                if completed.returncode and bench_report["complete"]:
                    completed.check_returncode()
            finally:
                if receiver.poll() is None:
                    pid = int(command("docker", "exec", b, "cat", prefix + "/receiver.pid").strip())
                    if pid <= 1:
                        raise RuntimeError("受信プロセスのPIDが不正です")
                    subprocess.run(["docker", "exec", b, "kill", "-TERM", str(pid)], capture_output=True)
                if receiver.wait(timeout=5):
                    raise RuntimeError(f"受信側が失敗しました: {directory / 'b.log'}")
    finally:
        if workload["rate_mbps"]:
            (directory / "qdisc.json").write_text(command("docker", "exec", a, "tc", "-s", "-j", "qdisc", "show", "dev", "eth0"))
            command("docker", "exec", a, "tc", "qdisc", "del", "dev", "eth0", "root")
    return measure(directory, workload, mode=mode, duration_ms=environment["duration_ms"], allow_incomplete=True)


def main():
    install_stop_handler()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--duration-ms", type=int, default=3000)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--mode", choices=MODES)
    parser.add_argument("--workload", choices=[workload["name"] for workload in WORKLOADS])
    parser.add_argument("--clock-mode", choices=("authority", "local"), default="authority",
                        help="localは同一ホスト時計を直接使う原因切り分け専用。標準比較はauthority")
    options = parser.parse_args()
    if not 1000 <= options.duration_ms <= 10_000 or not 1 <= options.repetitions <= 10:
        parser.error("duration-msは1000〜10000、repetitionsは1〜10です")
    directory = options.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    repository = Path(__file__).resolve().parents[3]
    name = "amitoki-l3-tcp-" + uuid.uuid4().hex[:12]
    containers = [name + "-a", name + "-b"]
    cpus = sorted(os.sched_getaffinity(0))[:2]
    if len(cpus) != 2:
        raise RuntimeError("比較には少なくとも2 CPUが必要です")
    command("docker", "network", "create", "--internal", name)
    outcomes = []
    try:
        for index, container in enumerate(containers):
            command("docker", "run", "-d", "--name", container, "--network", name, "--cpuset-cpus", str(cpus[index]),
                    "--cap-add", "NET_RAW", "--cap-add", "NET_ADMIN", "--mount", f"type=bind,src={repository},dst=/work,readonly",
                    "--mount", f"type=bind,src={directory},dst=/results", IMAGE, "sleep", "1800")
            command("docker", "exec", container, "ethtool", "-K", "eth0", "tso", "off", "gso", "off", "gro", "off")
            offload = command("docker", "exec", container, "ethtool", "-k", "eth0")
            (directory / f"offload-{index}.txt").write_text(offload)
            for feature in ("tcp-segmentation-offload", "generic-segmentation-offload", "generic-receive-offload"):
                if f"{feature}: off" not in offload:
                    raise RuntimeError(f"{feature}を無効化できませんでした")
        metadata = [json.loads(command("docker", "inspect", container))[0]["NetworkSettings"]["Networks"][name] for container in containers]
        environment = dict(directory=directory, duration_ms=options.duration_ms, containers=containers, clock_mode=options.clock_mode,
                           macs=[node["MacAddress"] for node in metadata], ips=[node["IPAddress"] for node in metadata])
        workloads = [workload for workload in WORKLOADS if not options.quick or workload["name"] in ("idle", "mixed_10mbps", "loss_1pct")]
        if options.workload:
            workloads = [workload for workload in workloads if workload["name"] == options.workload]
        if not workloads:
            parser.error("quickとworkloadの組合せに実行対象がありません")
        for repetition in range(1, options.repetitions + 1):
            for workload in workloads:
                modes = [options.mode] if options.mode else list(MODES)
                random.Random(SEED + repetition + WORKLOADS.index(workload)).shuffle(modes)
                for mode in modes:
                    path = directory / f"{repetition:02d}-{workload['name']}-{mode}"
                    path.mkdir()
                    outcome = run_case(environment, workload, mode=mode, directory=path)
                    outcome["repetition"] = repetition
                    outcome["clock_mode"] = options.clock_mode
                    outcomes.append(outcome)
                    (directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
                    metrics = outcome["metrics"]
                    print(f"{repetition}: {workload['name']}: {mode}: 完了={outcome['complete']}、配送={metrics['delivered']}、確認={outcome['acknowledged']}", flush=True)
        report(directory, outcomes)
        incomplete = sum(not outcome["complete"] for outcome in outcomes)
        (directory / "verification.json").write_text(json.dumps(dict(status="incomplete" if incomplete else "passed", incomplete_runs=incomplete,
            runs=len(outcomes), repetitions=options.repetitions,
            duration_ms=options.duration_ms, kernel=os.uname().release, machine=os.uname().machine, cpus=cpus,
            topology="two network namespaces on an internal Ethernet bridge; common CLOCK_BOOTTIME verified",
            offload=False, order_seed=SEED, loss_seed=None,
            clock_mode=options.clock_mode,
            receipts="all messages checked for fingerprint, size, sequence, duplicates and applicable ordering",
            binaries={binary:hashlib.sha256((repository / "target/release" / binary).read_bytes()).hexdigest()
                      for binary in ("amitoki-l3", "amitoki-l3-tcp")}), indent=2) + "\n")
        if incomplete:
            raise SystemExit(f"{incomplete}/{len(outcomes)}試行が未完了です。report.mdを確認してください")
    finally:
        for container in containers:
            subprocess.run(["docker", "exec", container, "chown", "-R", f"{os.getuid()}:{os.getgid()}", "/results"], capture_output=True)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True)
        subprocess.run(["docker", "network", "rm", name], capture_output=True)


if __name__ == "__main__":
    main()
