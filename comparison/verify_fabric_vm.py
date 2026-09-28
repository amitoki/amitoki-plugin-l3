"""4台の独立したKVMゲストで2経路と片経路切断を検証する。"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lab"))
from fabric import benchmark_arguments, configure_fabric, verify
from virtual_machines import VirtualMachines

GUEST = "/home/ubuntu/l3"
NODES = {"a": 0, "b": 1, "r1": 2, "r2": 3}
EDGES = [(0, "a1", 2, "r1a"), (2, "r1b", 1, "b1"), (0, "a2", 3, "r2a"), (3, "r2b", 1, "b2")]
NODE_DURATION_MS = 23000
READY_TIMEOUT_SECONDS = 5
# 送信済みを観測してから切断する。起動の遅さを故障復旧時間へ含めない。
FAULT_MIN_SENT = 50


def execute(machines, directory, *, mode, fault):
    directory.mkdir()
    configure_fabric(directory, mode)
    macs = {interface["name"]: interface["mac"] for interfaces in machines.interfaces.values() for interface in interfaces}
    peers = {left_name: right_name for _, left_name, _, right_name in EDGES}
    peers.update({right_name: left_name for _, left_name, _, right_name in EDGES})
    for name, node in NODES.items():
        path = directory / f"{name}.json"
        config = json.loads(path.read_text())
        config["observation"] = GUEST + f"/{name}.live.json"
        for link in config["links"]:
            link["peer_mac"] = macs[peers[link["interface"]]]
        path.write_text(json.dumps(config, indent=2) + "\n")
        machines.copy(node, path, GUEST + "/config.json")
    processes, logs = [], []
    try:
        for name in ("b", "r1", "r2"):
            node = NODES[name]
            ready = GUEST + f"/{directory.name}.ready.json"
            arguments = ["sudo", GUEST + "/amitoki-l3", "receiver" if name == "b" else "router", "--config", GUEST + "/config.json",
                "--duration-ms", str(NODE_DURATION_MS), "--output", GUEST + f"/{name}.report.json", "--ready", ready]
            if name == "b":
                arguments += ["--short-credits-per-second", "100000", "--bulk-credits-per-second", "100000", "--delivery-log", GUEST + "/deliveries.jsonl"]
            log = (directory / f"{name}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(machines.ssh(node, *arguments), stdout=log, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + READY_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                if subprocess.run(machines.ssh(node, "test", "-f", ready), capture_output=True, timeout=6).returncode == 0:
                    break
                if processes[-1].poll() is not None:
                    raise RuntimeError(f"{name}の準備が失敗しました")
            else:
                raise RuntimeError(f"{name}の準備がタイムアウトしました")

        def interrupt_path():
            deadline = time.monotonic() + READY_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                completed = subprocess.run(machines.ssh(0, "sudo", "cat", GUEST + "/a.live.json"), capture_output=True, text=True, timeout=6)
                if completed.returncode == 0:
                    report = json.loads(completed.stdout)
                    if sum(channel["metrics"]["sent"] for channel in report["channels"]) >= FAULT_MIN_SENT:
                        machines.run(3, "sudo", "ip", "link", "set", "r2b", "down")
                        (directory / "fault.json").write_text(json.dumps({"interface":"r2b", "sender_observed_us":report["observed_us"]}))
                        return
            raise RuntimeError("切断前の送信を観測できませんでした")

        machines.run(0, "sudo", "rm", "-f", GUEST + "/a.live.json")
        with ThreadPoolExecutor(max_workers=1) as workers:
            interruption = workers.submit(interrupt_path) if fault else None
            completed = subprocess.run(machines.ssh(0, "sudo", GUEST + "/amitoki-l3", "bench", "--config", GUEST + "/config.json",
                *benchmark_arguments(mode), "--output", GUEST + "/a.report.json"), capture_output=True, text=True, timeout=25)
            (directory / "a.log").write_text(completed.stdout + completed.stderr)
            if interruption:
                interruption.result()
        # ここでSSHをkillするとゲストのプロセスが残る。有限の実行時間で正常終了させる。
        for process in processes:
            if process.wait(timeout=25):
                raise RuntimeError("ルーターまたは受信ノードが失敗しました")
        reports = {}
        for name, node in NODES.items():
            content = machines.run(node, "cat", GUEST + f"/{name}.report.json")
            (directory / f"{name}.report.json").write_text(content)
            reports[name] = json.loads(content)
        (directory / "deliveries.jsonl").write_text(machines.run(1, "cat", GUEST + "/deliveries.jsonl"))
        completed.check_returncode()
        if len({report["clock_domain"] for report in reports.values()}) != 4:
            raise RuntimeError("時計が独立していません")
        if any(report["clock_simulation"] != {"offset_us":0,"drift_ppm":0} for report in reports.values()):
            raise RuntimeError("時計を模擬しています")
        return {"mode":mode, "fault":fault, **verify(directory, reports, mode)}
    finally:
        for process in processes:
            try:
                process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=5)
        for log in logs:
            log.close()
        if fault:
            machines.run(3, "sudo", "ip", "link", "set", "r2b", "up")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--image", type=Path, default=Path.home() / ".cache/amitoki-vm/ubuntu-24.04-server-cloudimg-amd64.img")
    options = parser.parse_args()
    directory = options.directory.resolve()
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    directory.chmod(0o700)
    machines = VirtualMachines(directory, options.image.resolve(), edges=EDGES)
    repository = Path(__file__).resolve().parents[1]
    executable = repository / "target/release/amitoki-l3"
    try:
        for node in (0, 2, 3, 1):
            machines.start(node, wait=False)
        for node in (0, 2, 3, 1):
            machines.wait_ready(node)
            machines.copy(node, executable, GUEST + "/amitoki-l3")
            machines.run(node, "chmod", "+x", GUEST + "/amitoki-l3")
        boot_ids = [machines.run(node, "cat", "/proc/sys/kernel/random/boot_id").strip() for node in NODES.values()]
        if len(set(boot_ids)) != 4:
            raise RuntimeError("独立した4カーネルではありません")
        links = []
        for node in NODES.values():
            for interface in machines.interfaces[node]:
                report = json.loads(machines.run(node, "ip", "-j", "address", "show", "dev", interface["name"]))[0]
                if report["addr_info"]:
                    raise RuntimeError("試験リンクにIPアドレスがあります")
                links.append(report)
        measurements = []
        for mode, fault in [("baseline",False),("full",False),("trim-pressure",False),("full",True)]:
            measurement = execute(machines, directory / (mode + ("-failure" if fault else "")), mode=mode, fault=fault)
            measurements.append(measurement)
            (directory / "measurements.json").write_text(json.dumps(measurements, indent=2) + "\n")
            print(json.dumps({key:value for key,value in measurement.items() if key != "paths"}), flush=True)
        (directory / "verification.json").write_text(json.dumps({"status":"passed", "boot_ids":boot_ids, "links":links,
            "runs":len(measurements), "image_sha256":machines.image_sha256, "binary_sha256":hashlib.sha256(executable.read_bytes()).hexdigest(),
            "topology":"four KVM guests, two routers, four isolated point-to-point QEMU socket links"},indent=2) + "\n")
    finally:
        machines.close()


if __name__ == "__main__":
    main()
