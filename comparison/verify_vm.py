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
    workload = dict(name="vm_idle", short_rate=100, bulk_rate=100 if transport == "reliable" else 0, short_bytes=128, bulk_bytes=1200)
    executable = GUEST + "/amitoki-l3" + ("-udp" if transport == "udp" else "")
    common = ["--bind", "192.0.2.2:47000", "--peer", "192.0.2.1:47000"] if transport == "udp" else ["--config", GUEST + "/config.json"]
    ready = GUEST + f"/{transport}.ready.json"
    receiver = machines.ssh(1, "sudo", executable, "receiver", *common, "--duration-ms", str(RECEIVER_DURATION_MS),
                            "--output", GUEST + "/b.report.json", "--ready", ready,
                            *(["--delivery-log", GUEST + "/deliveries.jsonl"] if transport == "reliable" else []))
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
            common = ["--bind", "192.0.2.1:47000", "--peer", "192.0.2.2:47000"] if transport == "udp" else ["--delivery", "reliable" if transport == "reliable" else "deadline", "--config", GUEST + "/config.json", "--peer", "2"]
            if transport == "reliable":
                common += ["--bulk-ordering", "ordered"]
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
    if transport == "reliable":
        verify_reliable(machines, directory, reports=reports, workload=workload)
    else:
        verify(reports, workload, transport)
    if transport != "udp":
        if reports["a"]["clock_domain"] == reports["b"]["clock_domain"]:
            raise RuntimeError("別々の時計で検証できていません")
        for entry in reports.values():
            if entry["clock_simulation"] != dict(offset_us=0, drift_ppm=0):
                raise RuntimeError("VM試験では時計を模擬しません")
    return dict(workload=workload, transport=transport, repetition=1, reports=reports)


def verify_reliable(machines, directory, *, reports, workload):
    contents = machines.run(1, "cat", GUEST + "/deliveries.jsonl")
    (directory / "deliveries.jsonl").write_text(contents)
    deliveries = [json.loads(line) for line in contents.splitlines()]
    bench = reports["a"]["reliable_benchmark"]
    if not bench["complete"] or any(entry["network"]["malformed"] for entry in reports.values()):
        raise RuntimeError("VM間の信頼性配送が完了していません")
    for index, channel in enumerate(bench["channels"]):
        expected = DURATION_MS * 100 // 1000
        received = [item for item in deliveries if item["channel"] == index + 1]
        sequences = [item["sequence"] for item in received]
        if channel["metrics"]["acknowledged"] != expected or sorted(sequences) != list(range(1, expected + 1)):
            raise RuntimeError("VM間の配送に欠落か重複があります")
        if channel["ordering"] == "ordered" and sequences != sorted(sequences):
            raise RuntimeError("VM間の順序保証に違反しています")
        size = workload["short_bytes" if index == 0 else "bulk_bytes"]
        if any(item["payload"] != [(item["sequence"] + offset) & 255 for offset in range(size)] for item in received):
            raise RuntimeError("VM間の本文が一致しません")


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
        for transport in ("l3", "reliable", "udp"):
            if transport == "udp":
                for node in (0, 1):
                    machines.run(node, "sudo", "ip", "address", "add", f"192.0.2.{node+1}/30", "dev", "l3test0")
            path = directory / transport
            path.mkdir()
            outcomes.append(execute(machines, path, transport))
            if transport == "reliable":
                print("VM reliable: 順序なし300件・順序あり300件の本文と配送を確認", flush=True)
            else:
                print(f"VM {transport}: 期限内ACK {outcomes[-1]['reports']['a']['benchmark']['short']['on_time_ratio']:.1%}", flush=True)
        (directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
        report(directory, [outcome for outcome in outcomes if outcome["transport"] != "reliable"])
        report_path = directory / "report.md"
        report_path.write_text(report_path.read_text().replace("Dockerの内部Ethernet bridgeで2つのnetwork namespaceを接続。", "KVMの2ゲストをQEMU socket backendの仮想Ethernetで接続。時計の模擬なし。"))
        with report_path.open("a") as output:
            output.write("\n信頼性配送も同じIPなしのリンクで確認。順序なし300件・順序あり300件の本文が一致し、欠落・重複なし。TCPとの性能比較ではない。\n")
        (directory / "verification.json").write_text(json.dumps(dict(status="passed", boot_ids=boot_ids,
            image_sha256=machines.image_sha256, clock_simulation=False, duration_ms=DURATION_MS,
            kernels=kernels, links_before_udp=links,
            binaries={binary:hashlib.sha256((repository / "target/release" / binary).read_bytes()).hexdigest() for binary in ("amitoki-l3", "amitoki-l3-udp")},
            topology="two KVM guests; virtio-net; QEMU socket backend; no IP on L3 test link"), indent=2) + "\n")
    finally:
        machines.close()


if __name__ == "__main__":
    main()
