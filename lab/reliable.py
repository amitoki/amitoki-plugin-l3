"""信頼性配送の欠落・順序・受信待ちをIPなしの実ネットワークで検証する。"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import select
import socket
import subprocess
import time

from capture import Capture, ETHER_TYPE
from processes import wait_ready, stop_processes
from topology import block_clock_replies, command, configure, configure_clocks, create_links, impair_primary, remove_links, restore_clock_replies

# 最初のDATAを観測してから故障を解除する。プロセス起動時間には依存しない。
FAULT_DURATION_SECONDS = 0.25
OBSERVATION_TIMEOUT_SECONDS = 5
BENCH_DURATION_MS = 1000
MESSAGE_RATE = 100


@dataclass(frozen=True)
class Scenario:
    name: str
    ordering: str = "unordered"
    fault: str = "none"
    permanent: bool = False
    paths: str = "1"
    rate: int = 1000
    window: int = 64
    pending: int = 256
    timeout_ms: int = 10_000
    clock_sync: bool = False
    clock_drift_ppm: int = 0
    stop_clock_replies: bool = False


SCENARIOS = [
    Scenario("unordered_gap", fault="first"),
    Scenario("ordered_gap", ordering="ordered", fault="first"),
    Scenario("mixed_channels", ordering="ordered", fault="first"),
    Scenario("lost_ack", fault="ack"),
    Scenario("backpressure", rate=20, window=4, pending=4),
    Scenario("alternate_path", fault="path", paths="1,2"),
    Scenario("channel_timeout", ordering="ordered", fault="first", permanent=True, timeout_ms=800),
    Scenario("receiver_restart", fault="restart"),
    Scenario("different_clocks", clock_sync=True, clock_drift_ppm=200),
    Scenario("clock_recovery", fault="clock", clock_sync=True, stop_clock_replies=True),
]


def install_fault(scenario):
    if scenario.fault == "path":
        impair_primary(loss=True)
        return
    if scenario.fault not in ("first", "ack", "restart"):
        return
    interface = "b1" if scenario.fault in ("ack", "restart") else "r1b"
    command("tc", "qdisc", "add", "dev", interface, "clsact")
    kind = "10" if scenario.fault in ("ack", "restart") else "9"
    matches = ["match", "u8", kind, "0xff", "at", "5"]
    if scenario.fault == "first":
        matches += ["match", "u32", "0", "0xffffffff", "at", "24",
                    "match", "u32", "1", "0xffffffff", "at", "28",
                    "match", "u32", "1", "0xffffffff", "at", "96"]
    command("tc", "filter", "add", "dev", interface, "egress", "protocol", "0x88b5", "u32", *matches, "action", "drop")


def after_first_data(observer, action):
    deadline = time.monotonic() + OBSERVATION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        ready, _, _ = select.select([observer], [], [], max(0, deadline - time.monotonic()))
        if not ready:
            break
        frame = observer.recv(65535)
        if len(frame) > 19 and frame[19] == 9:
            time.sleep(FAULT_DURATION_SECONDS)
            action()
            return
    raise RuntimeError("故障解除前のDATAを観測できませんでした")


def execute(binary, directory, scenario):
    directory.mkdir()
    configure(directory, "priority")
    if scenario.clock_sync:
        configure_clocks(directory, scenario)
    create_links()
    processes, logs = [], []
    observer = None
    try:
        install_fault(scenario)
        receiver_invocation = None
        for name in ("b", "r1", "r2"):
            invocation = [binary, "receiver" if name == "b" else "router", "--config", str(directory / f"{name}.json"),
                          "--duration-ms", "25000", "--output", str(directory / f"{name}.report.json"),
                          "--ready", str(directory / f"{name}.ready.json")]
            if name == "b":
                invocation += ["--receive-window", str(scenario.window), "--short-credits-per-second", str(scenario.rate),
                               "--delivery-log", str(directory / "deliveries.jsonl")]
                receiver_invocation = invocation
            log = (directory / f"{name}.log").open("w")
            logs.append(log)
            processes.append((name, subprocess.Popen(invocation, stdout=log, stderr=subprocess.STDOUT)))
        wait_ready(processes, directory, different_clocks=scenario.clock_sync)

        def restore():
            if scenario.fault == "clock":
                block_clock_replies()
                # 200msの同期寿命を超えてから復旧し、保持した本文の再送を確認する。
                time.sleep(0.5)
                restore_clock_replies()
                return
            if scenario.fault == "restart":
                stop_processes([processes[0]])
                (directory / "deliveries.jsonl").rename(directory / "before-restart.jsonl")
                (directory / "b.report.json").rename(directory / "before-restart.report.json")
                (directory / "b.ready.json").unlink()
                processes[0] = ("b", subprocess.Popen(receiver_invocation, stdout=logs[0], stderr=subprocess.STDOUT))
                wait_ready([processes[0]], directory)
            interface = "b1" if scenario.fault in ("ack", "restart") else "r1b"
            command("tc", "qdisc", "del", "dev", interface, "clsact")

        temporary = scenario.fault in ("first", "ack", "restart", "clock") and not scenario.permanent
        if temporary:
            observer = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETHER_TYPE))
            observer.bind(("r1a", 0))
        invocation = [binary, "bench", "--config", str(directory / "a.json"), "--peer", "2", "--duration-ms", str(BENCH_DURATION_MS),
                      "--short-rate", str(MESSAGE_RATE), "--short-bytes", "64", "--bulk-bytes", "64",
                      "--bulk-rate", str(MESSAGE_RATE if scenario.name in ("mixed_channels", "channel_timeout") else 0),
                      "--short-ordering", scenario.ordering, "--bulk-ordering", "ordered",
                      "--paths", scenario.paths, "--delivery-timeout-ms", str(scenario.timeout_ms),
                      "--pending-limit", str(scenario.pending), "--output", str(directory / "a.report.json")]
        with Capture(directory / "sample.pcap"), ThreadPoolExecutor(max_workers=1) as workers:
            recovery = workers.submit(after_first_data, observer, restore) if temporary else None
            completed = subprocess.run(invocation, capture_output=True, text=True, timeout=20)
            if recovery:
                recovery.result()
        (directory / "a.log").write_text(completed.stdout + completed.stderr)
        stop_processes(processes)
        processes.clear()
        reports = {name: json.loads((directory / f"{name}.report.json").read_text()) for name in ("a", "b", "r1", "r2")}
        deliveries = [json.loads(line) for line in (directory / "deliveries.jsonl").read_text().splitlines()]
        verify(scenario, completed=completed, reports=reports, deliveries=deliveries, directory=directory)
        return {"name": scenario.name, "exit_code": completed.returncode, "reports": reports, "delivered": len(deliveries)}
    finally:
        if observer:
            observer.close()
        for _, process in processes:
            if process.poll() is None:
                process.kill()
            process.wait()
        for log in logs:
            log.close()
        remove_links()


def verify(scenario, *, completed, reports, deliveries, directory):
    bench = reports["a"]["reliable_benchmark"]
    receiver = reports["b"]["reliable_receiver"]
    expected_failure = scenario.name in ("channel_timeout", "receiver_restart")
    if (completed.returncode != 0) != expected_failure or bench["complete"] == expected_failure:
        raise RuntimeError(f"配送の成功/失敗が期待と違います: {completed.stderr}")
    for report in reports.values():
        if report["network"]["malformed"] or report["network"]["wrong_clock"]:
            raise RuntimeError("不正なパケットまたは時計世代があります")
    if any(channel["metrics"]["invalid_responses"] for channel in bench["channels"]):
        raise RuntimeError("ACKの照合に失敗しました")
    seen = set()
    sequences = {1: [], 2: []}
    for delivery in deliveries:
        key = (delivery["channel"], delivery["sequence"])
        if key in seen or delivery["payload"] != [(delivery["sequence"] + offset) & 255 for offset in range(64)]:
            raise RuntimeError("重複配送または本文の不一致があります")
        seen.add(key)
        sequences[delivery["channel"]].append(delivery["sequence"])
    for index, channel in enumerate(bench["channels"]):
        expected = channel["offered"]
        metrics = channel["metrics"]
        if metrics["acknowledged"] + metrics["unconfirmed"] + channel["pending"] != metrics["submitted"] or metrics["submitted"] + channel["unsubmitted"] != expected:
            raise RuntimeError("予定数・受付数・未確認数の集計が一致しません")
        if not expected_failure or (scenario.name == "channel_timeout" and index == 1):
            if channel["metrics"]["acknowledged"] != expected or sorted(sequences[index + 1]) != list(range(1, expected + 1)):
                raise RuntimeError("受付またはアプリへの配送に欠落があります")
        if channel["ordering"] == "ordered" and sequences[index + 1] != sorted(sequences[index + 1]):
            raise RuntimeError("順序保証に違反しています")
        if channel["metrics"]["peak_pending"] > scenario.pending or receiver["peak_buffered"] > scenario.window:
            raise RuntimeError("送受信キューの上限を超えました")
    if scenario.name == "unordered_gap" and sequences[1][0] == 1:
        raise RuntimeError("欠落中の後続メッセージが先に届きませんでした")
    if scenario.name == "mixed_channels":
        if next(index for index, item in enumerate(deliveries) if item["channel"] == 2) >= next(index for index, item in enumerate(deliveries) if item["channel"] == 1):
            raise RuntimeError("独立したchannelが順序待ちに巻き込まれました")
    if scenario.name == "lost_ack" and receiver["duplicates"] == 0:
        raise RuntimeError("ACK欠落後の再送を確認できませんでした")
    if scenario.name == "backpressure" and bench["channels"][0]["backpressure_events"] == 0:
        raise RuntimeError("送信キューの待ちを確認できませんでした")
    if expected_failure:
        channel = bench["channels"][0]
        expected_state = "peer_reset" if scenario.name == "receiver_restart" else "timed_out"
        if channel["state"] != expected_state or not channel["metrics"]["unconfirmed"]:
            raise RuntimeError("未確認分が明示的な失敗になっていません")
    if scenario.name == "receiver_restart":
        before = (directory / "before-restart.jsonl").read_text().splitlines()
        if not before or deliveries:
            raise RuntimeError("再起動前の受付、または再起動後の再配送防止を確認できませんでした")
    if scenario.clock_sync:
        if len({entry["clock_domain"] for entry in reports.values()}) != 4:
            raise RuntimeError("別々の時計で検証できていません")
        if any(not reports[name]["clock_sync"]["samples"]["accepted"] for name in ("a", "r1", "r2")):
            raise RuntimeError("時計交換が成功していません")
    if scenario.fault == "clock" and not any(entry["network"]["unsynchronized"] for entry in reports.values()):
        raise RuntimeError("時計の失効を確認できませんでした")


def run_suite(binary, directory):
    directory.mkdir()
    outcomes = []
    for scenario in SCENARIOS:
        outcome = execute(binary, directory / scenario.name, scenario)
        outcomes.append(outcome)
        (directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
        print(f"信頼性配送 {scenario.name}: 検証成功、配送{outcome['delivered']}件", flush=True)
    verification = {"status": "passed", "runs": len(outcomes), "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest()}
    (directory / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
    return len(outcomes)
