"""時計を模擬せず、2つのKVMゲスト間で独自L3とUDPを確認する。"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from run import arguments_for, config, verify, report, install_stop_handler
from virtual_machines import MACS, VirtualMachines

GUEST = "/home/ubuntu/l3"
DURATION_MS = 3000
RECEIVER_DURATION_MS = 6000


def execute(machines, directory, transport):
    workload = dict(name="vm_idle", short_rate=100, bulk_rate=0, short_bytes=128, bulk_bytes=1200)
    executable = GUEST + "/amitoki-l3" + ("-udp" if transport == "udp" else "")
    common = ["--bind", "192.0.2.2:47000", "--peer", "192.0.2.1:47000"] if transport == "udp" else ["--config", GUEST + "/config.json"]
    ready = GUEST + f"/{transport}.ready.json"
    receiver = machines.ssh(1, "sudo", executable, "receiver", *common, "--duration-ms", str(RECEIVER_DURATION_MS),
                            "--output", GUEST + "/b.report.json", "--ready", ready)
    with (directory / "b.log").open("w") as log:
        process = subprocess.Popen(receiver, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                check = subprocess.run(machines.ssh(1, "test", "-f", ready), capture_output=True, timeout=6)
                if check.returncode == 0:
                    break
                if process.poll() is not None:
                    raise RuntimeError("VMの受信ノードが準備前に終了しました")
            else:
                raise RuntimeError("VMの受信準備がタイムアウトしました")
            common = ["--bind", "192.0.2.1:47000", "--peer", "192.0.2.2:47000"] if transport == "udp" else ["--config", GUEST + "/config.json", "--peer", "2"]
            invocation = machines.ssh(0, "sudo", executable, "bench", *common, "--duration-ms", str(DURATION_MS),
                                      *arguments_for(workload), "--output", GUEST + "/a.report.json")
            completed = subprocess.run(invocation, capture_output=True, text=True, timeout=15)
            (directory / "a.log").write_text(completed.stdout + completed.stderr)
            completed.check_returncode()
            if process.wait(timeout=10):
                raise RuntimeError("VMの受信ノードが失敗しました")
        finally:
            process.wait(timeout=10)
    reports = {}
    for node, name in enumerate(("a", "b")):
        contents = machines.run(node, "cat", GUEST + f"/{name}.report.json")
        (directory / f"{name}.report.json").write_text(contents)
        reports[name] = json.loads(contents)
    verify(reports, workload, transport)
    if transport == "l3":
        if reports["a"]["clock_domain"] == reports["b"]["clock_domain"]:
            raise RuntimeError("別々の時計で検証できていません")
        for entry in reports.values():
            if entry["clock_simulation"] != dict(offset_us=0, drift_ppm=0):
                raise RuntimeError("VM試験では時計を模擬しません")
    return dict(workload=workload, transport=transport, repetition=1, reports=reports)


def main():
    install_stop_handler()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--image", type=Path, default=Path.home() / ".cache/amitoki-vm/ubuntu-24.04-server-cloudimg-amd64.img")
    options = parser.parse_args()
    directory = options.directory.resolve()
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    directory.chmod(0o700)
    machines = VirtualMachines(directory, options.image.resolve())
    repository = Path(__file__).resolve().parents[3]
    try:
        for node in (0, 1):
            machines.start(node)
            for executable in ("amitoki-l3", "amitoki-l3-udp"):
                machines.copy(node, repository / "target/release" / executable, GUEST + "/" + executable)
                machines.run(node, "chmod", "+x", GUEST + "/" + executable)
            configuration = config(node + 1, MACS[1-node])
            configuration["links"][0]["interface"] = "l3test0"
            configuration["routes"][0]["interface"] = "l3test0"
            path = directory / f"{node}.json"
            path.write_text(json.dumps(configuration))
            machines.copy(node, path, GUEST + "/config.json")
        boot_ids = [machines.run(node, "cat", "/proc/sys/kernel/random/boot_id").strip() for node in (0, 1)]
        if len(set(boot_ids)) != 2:
            raise RuntimeError("別カーネルのVMになっていません")
        links = [json.loads(machines.run(node, "ip", "-j", "address", "show", "dev", "l3test0"))[0] for node in (0, 1)]
        if any(link["addr_info"] for link in links):
            raise RuntimeError("L3試験リンクにIPアドレスがあります")
        kernels = [machines.run(node, "uname", "-r").strip() for node in (0, 1)]
        outcomes = []
        for transport in ("l3", "udp"):
            if transport == "udp":
                for node in (0, 1):
                    machines.run(node, "sudo", "ip", "address", "add", f"192.0.2.{node+1}/30", "dev", "l3test0")
            path = directory / transport
            path.mkdir()
            outcomes.append(execute(machines, path, transport))
            print(f"VM {transport}: 期限内ACK {outcomes[-1]['reports']['a']['benchmark']['short']['on_time_ratio']:.1%}", flush=True)
        (directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
        report(directory, outcomes)
        report_path = directory / "report.md"
        report_path.write_text(report_path.read_text().replace("Dockerの内部Ethernet bridgeで2つのnetwork namespaceを接続。", "KVMの2ゲストをQEMU socket backendの仮想Ethernetで接続。時計の模擬なし。"))
        (directory / "verification.json").write_text(json.dumps(dict(status="passed", boot_ids=boot_ids,
            image_sha256=machines.image_sha256, clock_simulation=False, duration_ms=DURATION_MS,
            kernels=kernels, links_before_udp=links,
            binaries={binary:hashlib.sha256((repository / "target/release" / binary).read_bytes()).hexdigest() for binary in ("amitoki-l3", "amitoki-l3-udp")},
            topology="two KVM guests; virtio-net; QEMU socket backend; no IP on L3 test link"), indent=2) + "\n")
    finally:
        machines.close()


if __name__ == "__main__":
    main()
