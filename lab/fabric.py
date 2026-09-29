"""動的経路・混雑通知・trimmingを同一負荷のdiamond構成で比較する。"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import time

from artifacts import restore_ownership
from processes import wait_ready, stop_processes
from topology import configure, configure_clocks, create_links, remove_links, command

DURATION_MS = 3000
# 片側1Mbps、他方4Mbps。合計帯域に近い短文+bulkを投入する。
RATES = [300, 300]
SIZES = [64, 1200]
MODES = {
    "baseline": {},
    "adaptive": {"adaptive_paths": True},
    "feedback": {"adaptive_paths": True, "telemetry": True, "congestion_control": True},
    "trimming": {"adaptive_paths": True, "telemetry": True, "trimming": True},
    "trim-pressure": {"telemetry": True, "trimming": True},
    "full": {"adaptive_paths": True, "telemetry": True, "congestion_control": True, "trimming": True},
}


def configure_fabric(directory, mode):
    configure(directory, "priority")
    scenario = argparse.Namespace(stop_clock_replies=False, clock_drift_ppm=0)
    configure_clocks(directory, scenario)
    for name in ("a", "b", "r1", "r2"):
        path = directory / f"{name}.json"
        configuration = json.loads(path.read_text())
        configuration["fabric"] = {**MODES[mode], "clock_independent": True}
        configuration["clock"].pop("simulation")
        configuration["observation"] = str(directory / f"{name}.live.json")
        if name == "r2":
            configuration["bytes_per_second"] = 500_000
        path.write_text(json.dumps(configuration, indent=2) + "\n")


def workload(mode):
    return (100, [10000, 0], "1") if mode == "trim-pressure" else (DURATION_MS, RATES, "1,2")


def benchmark_arguments(mode=None):
    duration, rates, paths = workload(mode)
    return ["--peer", "2", "--duration-ms", str(duration), "--paths", paths,
            "--short-rate", str(rates[0]), "--short-bytes", str(SIZES[0]),
            "--bulk-rate", str(rates[1]), "--bulk-bytes", str(SIZES[1]),
            "--delivery-timeout-ms", "15000", "--replica-bytes-per-second", "1000000"]


def verify(directory, reports, mode=None):
    duration, rates, _ = workload(mode)
    bench = reports["a"]["reliable_benchmark"]
    if not bench["complete"]:
        raise RuntimeError("配送が完了していません")
    deliveries = [json.loads(line) for line in (directory / "deliveries.jsonl").read_text().splitlines()]
    for index, channel in enumerate(bench["channels"]):
        expected = rates[index] * duration // 1000
        received = [item for item in deliveries if item["channel"] == index + 1]
        if channel["metrics"]["acknowledged"] != expected or sorted(item["sequence"] for item in received) != list(range(1, expected+1)):
            raise RuntimeError("欠落または重複配送があります")
        if any(item["payload"] != [(item["sequence"] + offset) & 255 for offset in range(SIZES[index])] for item in received):
            raise RuntimeError("本文が一致しません")
        if channel["fabric"]["in_flight_bytes"] != 0 or channel["metrics"]["invalid_responses"]:
            raise RuntimeError("未確認状態または不正なACKがあります")
    if any(report["network"]["malformed"] for report in reports.values()):
        raise RuntimeError("不正なパケットがあります")
    return {"delivered": len(deliveries), "elapsed_us": bench["elapsed_us"],
            "retransmissions": sum(channel["metrics"]["retransmissions"] for channel in bench["channels"]),
            "short_ack_max_us": bench["channels"][0]["metrics"]["acknowledgement_max_us"],
            "short_ack_p99_upper_us": bench["channels"][0]["metrics"].get("acknowledgement_p99_upper_us"),
            "trimmed": sum(report["network"]["trimmed"] for report in reports.values()),
            "paths": [channel["fabric"]["paths"] for channel in bench["channels"]]}


def execute(binary, directory, *, mode, fault, restore=False):
    directory.mkdir()
    configure_fabric(directory, mode)
    create_links()
    processes, logs = [], []
    try:
        for name in ("b", "r1", "r2"):
            invocation = [binary, "receiver" if name == "b" else "router", "--config", str(directory / f"{name}.json"),
                          "--duration-ms", "25000", "--output", str(directory / f"{name}.report.json"),
                          "--ready", str(directory / f"{name}.ready.json")]
            if name == "b":
                invocation += ["--short-credits-per-second", "100000", "--bulk-credits-per-second", "100000",
                               "--delivery-log", str(directory / "deliveries.jsonl")]
            log = (directory / f"{name}.log").open("w")
            logs.append(log)
            processes.append((name, subprocess.Popen(invocation, stdout=log, stderr=subprocess.STDOUT)))
        wait_ready(processes, directory)
        def interrupt_path():
            deadline = time.monotonic() + 5
            observed = None
            while time.monotonic() < deadline:
                path = directory / "a.live.json"
                if path.exists():
                    observed = json.loads(path.read_text())
                    if sum(channel["metrics"]["sent"] for channel in observed["channels"]) >= 50:
                        break
                time.sleep(0.01)
            else:
                raise RuntimeError("切断前のDATA送信を観測できませんでした")
            # 送信を確認してから高速な経路2を切断する。
            command("ip", "link", "set", "r2b", "down")
            (directory / "fault.json").write_text(json.dumps({"interface": "r2b", "action": "down", "sender_observed_us": observed["observed_us"]}))
            if restore:
                # 再送とprobeの複数周期を跨いでからリンクを戻す。
                time.sleep(0.5)
                command("ip", "link", "set", "r2b", "up")
                (directory / "restored.json").write_text(json.dumps({"interface":"r2b", "action":"up"}))

        with ThreadPoolExecutor(max_workers=1) as workers:
            interruption = workers.submit(interrupt_path) if fault else None
            completed = subprocess.run([binary, "bench", "--config", str(directory / "a.json"),
                *benchmark_arguments(mode), "--output", str(directory / "a.report.json")], capture_output=True, text=True, timeout=23)
            if interruption:
                interruption.result()
        (directory / "a.log").write_text(completed.stdout + completed.stderr)
        stop_processes(processes)
        processes.clear()
        reports = {name: json.loads((directory / f"{name}.report.json").read_text()) for name in ("a", "b", "r1", "r2")}
        completed.check_returncode()
        measurement = {"mode": mode, "fault": fault, "restored": restore, **verify(directory, reports, mode)}
        if mode not in ("baseline", "trim-pressure") and not fault and any(sum(channel["fabric"]["paths"][index]["sent"] for channel in reports["a"]["reliable_benchmark"]["channels"]) == 0 for index in (0,1)):
            raise RuntimeError("複数経路を使用していません")
        if mode == "trim-pressure" and measurement["trimmed"] == 0:
            raise RuntimeError("trimmingが発生していません")
        if restore:
            fault_at = json.loads((directory / "fault.json").read_text())["sender_observed_us"]
            recovered = any(channel["fabric"]["paths"][1]["last_data_us"] > fault_at + 500_000
                            for channel in reports["a"]["reliable_benchmark"]["channels"])
            if not recovered:
                raise RuntimeError("復旧した経路でのACKを確認できませんでした")
        return measurement
    finally:
        stop_processes(processes)
        for log in logs:
            log.close()
        remove_links()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    options = parser.parse_args()
    try:
        run_suite(options)
    finally:
        restore_ownership(options.directory)


def run_suite(options):
    measurements = []
    for mode, fault, restore in [(mode, False, False) for mode in MODES] + [("full", True, False), ("full", True, True)]:
        measurement = execute(options.binary, options.directory / (mode + ("-recovery" if restore else "-failure" if fault else "")), mode=mode, fault=fault, restore=restore)
        measurements.append(measurement)
        (options.directory / "measurements.json").write_text(json.dumps(measurements, indent=2) + "\n")
        print(json.dumps({key: value for key, value in measurement.items() if key != "paths"}), flush=True)
    (options.directory / "verification.json").write_text(json.dumps({"status":"passed", "runs":len(measurements),
        "binary_sha256": hashlib.sha256(Path(options.binary).read_bytes()).hexdigest()}, indent=2) + "\n")


if __name__ == "__main__":
    main()
