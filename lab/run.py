"""同じ実装と負荷で機能を切り替え、独立したプロセス間の実通信を比較する。"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from capture import Capture
from processes import wait_ready, stop_processes
from scenarios import SCENARIOS
from topology import block_clock_replies, command, configure, configure_clocks, create_links, impair_primary, remove_links, restore_clock_replies


def execute_scenario(arguments, scenario, directory):
    directory.mkdir()
    configure(directory, scenario.scheduler)
    if scenario.clock_sync:
        configure_clocks(directory, scenario)
    create_links()
    processes = []
    logs = []
    try:
        impair_primary(delay_ms=scenario.delay_ms, loss=scenario.loss)
        names = ["b", "r2"] + (["r1"] if scenario.router else [])
        for name in names:
            role = "receiver" if name == "b" else "router"
            invocation = [arguments.binary, role, "--config", str(directory / f"{name}.json"),
                          "--duration-ms", str(arguments.duration_ms + 10_000),
                          "--output", str(directory / f"{name}.report.json"),
                          "--ready", str(directory / f"{name}.ready.json")]
            if name == "b":
                invocation += ["--short-credits-per-second", str(scenario.receiver_rate)]
            log = (directory / f"{name}.log").open("w")
            logs.append(log)
            processes.append((name, subprocess.Popen(invocation, stdout=log, stderr=subprocess.STDOUT)))
        wait_ready(processes, directory, different_clocks=scenario.clock_sync)
        invocation = [arguments.binary, "bench", "--delivery", "deadline", "--config", str(directory / "a.json"), "--peer", "2",
                      "--duration-ms", str(arguments.duration_ms), "--bulk-rate", str(scenario.bulk_rate),
                      "--paths", scenario.paths, "--replica-bytes-per-second", str(scenario.replica_budget),
                      "--short-deadline-us", str(scenario.short_deadline_us), "--retries", str(scenario.retries),
                      "--output", str(directory / "a.report.json")]
        def interrupt_clock():
            time.sleep(0.4)
            block_clock_replies()
            if scenario.recover_clock_replies:
                time.sleep(0.4)
                restore_clock_replies()

        with Capture(directory / "sample.pcap") as capture, ThreadPoolExecutor(max_workers=1) as workers:
            interruption = workers.submit(interrupt_clock) if scenario.stop_clock_replies else None
            completed = subprocess.run(invocation, capture_output=True, text=True, timeout=arguments.duration_ms / 1000 + 10)
            if interruption:
                interruption.result()
        (directory / "a.log").write_text(completed.stdout + completed.stderr)
        stop_processes(processes)
        processes = []
        if not scenario.router or scenario.expect_unreachable:
            if completed.returncode == 0 or "credit" not in completed.stderr:
                raise RuntimeError("到達不能の条件で通信できてしまいました")
            if scenario.clock_sync:
                router = json.loads((directory / "r1.report.json").read_text())
                if router["clock_sync"]["reading"] is not None:
                    raise RuntimeError("誤差が上限を超えた時計が有効になっています")
            return {"scenario":asdict(scenario), "expected_unreachable":True, "captured":capture.count}
        if completed.returncode:
            raise RuntimeError(f"送信側の終了値が{completed.returncode}: {completed.stderr}")
        reports = {name: json.loads((directory / f"{name}.report.json").read_text()) for name in ["a", "b", "r1", "r2"]}
        verify(scenario, reports)
        return {"scenario":asdict(scenario), "captured":capture.count, "reports":reports}
    finally:
        for _, process in processes:
            if process.poll() is None:
                process.kill()
            process.wait()
        for log in logs:
            log.close()
        remove_links()


def verify(scenario, reports):
    sender = reports["a"]["benchmark"]
    receiver = reports["b"]["receiver"]
    # 初期32枠と、受信プロセスの稼働時間中に補充できる枠だけを認める。
    allowed = 32 + scenario.receiver_rate * reports["b"]["elapsed_us"] // 1_000_000
    if receiver["granted_slots"][0] > allowed:
        raise RuntimeError("受信側が送信枠を過剰に発行しています")
    if sender["invalid_acks"] or receiver["rejected"]:
        raise RuntimeError("ACKまたはcreditの整合性が崩れました")
    if any(report["network"]["wrong_clock"] or report["network"]["malformed"] or
           (report["network"]["send_errors"] and not (scenario.stop_clock_replies and name == "b"))
           for name, report in reports.items()):
        raise RuntimeError("時計・パケット・socketのエラーがあります")
    for index, name in enumerate(("short", "bulk")):
        traffic = sender[name]
        if not 0 <= traffic["acknowledged"] <= receiver["delivered"][index] <= traffic["sent"] <= traffic["offered"]:
            raise RuntimeError(f"{name}の配送数が不整合です")
    if sender["short"]["rtt_us"]["max"] is not None and sender["short"]["rtt_us"]["max"] >= scenario.short_deadline_us:
        raise RuntimeError("期限後のACKを成功に数えています")
    budget = scenario.replica_budget * (sender["duration_us"] + 200_000) / 1_000_000 + 2 * 1478
    if sender["redundant_bytes"] > budget:
        raise RuntimeError("複製・再送の帯域上限を超えました")
    if scenario.replica_budget == 0 and sender["replicas"] != 0:
        raise RuntimeError("上限ゼロで複製されています")
    if scenario.delay_ms * 1000 > scenario.short_deadline_us and (scenario.paths == "1" or scenario.replica_budget == 0):
        if sender["short"]["acknowledged"] != 0:
            raise RuntimeError("期限より長い経路で成功しています")
    if scenario.name in ("idle_priority", "delayed_dual", "primary_loss_dual", "dual_duplicates", "retry_delayed"):
        if sender["short"]["acknowledged"] == 0:
            raise RuntimeError("利用可能な経路で短文が一件も届きませんでした")
    if scenario.name in ("dual_duplicates", "retry_delayed") and receiver["duplicates"] == 0:
        raise RuntimeError("重複の実通信を確認できませんでした")
    if scenario.clock_sync:
        if len({report["clock_domain"] for report in reports.values()}) != 4:
            raise RuntimeError("時計をずらした4ノードで検証できていません")
        if sender["short"]["acknowledged"] == 0:
            raise RuntimeError("同期後にパケットが届きませんでした")
        for name in ("a", "r1", "r2"):
            synchronization = reports[name]["clock_sync"]
            if synchronization["samples"]["accepted"] == 0:
                raise RuntimeError(f"{name}の時計交換が成功していません")
            if scenario.stop_clock_replies and not scenario.recover_clock_replies:
                if synchronization["reading"] is not None:
                    raise RuntimeError("同期の失効後も期限判断を続けています")
            elif synchronization["reading"] is None:
                raise RuntimeError(f"{name}が時計同期を維持できませんでした")
        if scenario.stop_clock_replies and sender["short"]["unsynchronized"] == 0:
            raise RuntimeError("同期の失効時に送信を停止していません")


def make_report(directory, outcomes):
    lines = ["# 独自L3の比較試験", "", "同一カーネル・外部非接続のコンテナ内で4プロセスと4組のvethを使用。IPアドレスなし。",
             "ルーターは各出力1Mbps。短文128Bを100件/秒、背景負荷は1200Bを500件/秒。",
             "期限内ACK率は送信予定の全メッセージを分母とし、送信枠不足・期限切れ・未応答も含む。RTTは送信予定時刻から計測。",
             "p50/p99は期限内にACKされたものだけ。帯域はEthernetヘッダ込み、FCS/preamble/IFGを含まない。", "",
             "| 条件 | 回 | 期限内ACK率 | RTT p50 / p99 (µs) | 複製件数 | receiver重複 |", "|---|---:|---:|---:|---:|---:|"]
    for outcome in outcomes:
        name = outcome["scenario"]["name"]
        if outcome.get("expected_unreachable"):
            lines.append(f"| {name} | {outcome['repetition']} | 到達不能を確認 | — | — | — |")
            continue
        bench = outcome["reports"]["a"]["benchmark"]
        traffic = bench["short"]
        lines.append(f"| {name} | {outcome['repetition']} | {traffic['on_time_ratio']:.1%} | {traffic['rtt_us']['p50']} / {traffic['rtt_us']['p99']} | {bench['replicas']} | {outcome['reports']['b']['receiver']['duplicates']} |")
    lines += ["", "clock_*条件は4ノードの時計を別々にずらして同期する。物理NIC・AF_XDPは未検証。UDP比較と別VM試験は別スクリプトを使用。", ""]
    (directory / "report.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration-ms", type=int, default=3000)
    parser.add_argument("--suite", choices=("all", "deadline", "reliable"), default="all")
    arguments = parser.parse_args()
    if not 1 <= arguments.repetitions <= 10 or not 1000 <= arguments.duration_ms <= 30_000:
        parser.error("repetitionsは1〜10、duration-msは1000〜30000です")
    arguments.directory.mkdir(parents=True, exist_ok=True)
    if (arguments.directory / "verification.json").exists():
        parser.error("既存の試験結果を上書きしないよう、新しいdirectoryを指定してください")
    outcomes = []
    try:
        for repetition in range(1, arguments.repetitions + 1):
            for scenario in SCENARIOS if arguments.suite != "reliable" else []:
                directory = arguments.directory / f"{repetition:02d}-{scenario.name}"
                outcome = execute_scenario(arguments, scenario, directory)
                outcome["repetition"] = repetition
                outcomes.append(outcome)
                (arguments.directory / "measurements.json").write_text(json.dumps(outcomes, indent=2) + "\n")
                if outcome.get("expected_unreachable"):
                    print(f"{repetition}: {scenario.name}: 到達不能を確認", flush=True)
                else:
                    traffic = outcome["reports"]["a"]["benchmark"]["short"]
                    print(f"{repetition}: {scenario.name}: 期限内ACK {traffic['on_time_ratio']:.1%}, p99={traffic['rtt_us']['p99']}µs", flush=True)
        if outcomes:
            make_report(arguments.directory, outcomes)
        from reliable import run_suite
        reliable_runs = run_suite(arguments.binary, arguments.directory / "reliable") if arguments.suite != "deadline" else 0
        with Path(arguments.binary).open("rb") as executable:
            binary_sha256 = hashlib.file_digest(executable, "sha256").hexdigest()
        verification = {"status":"passed", "runs":len(outcomes), "reliable_runs":reliable_runs, "repetitions":arguments.repetitions,
                        "duration_ms":arguments.duration_ms, "kernel":platform.release(), "machine":platform.machine(),
                        "ether_type":"0x88b5", "external_network":False, "link_ip_addresses":False,
                        "clock":"CLOCK_BOOTTIME; shared clock and four-timestamp synchronization with simulated offset/drift",
                        "binary_version":command(arguments.binary, "--version").strip(),
                        "binary_sha256":binary_sha256}
        (arguments.directory / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
    finally:
        owner = (int(os.environ.get("L3_OWNER_UID", "0")), int(os.environ.get("L3_OWNER_GID", "0")))
        for path in [*arguments.directory.rglob("*"), arguments.directory]:
            os.chown(path, *owner, follow_symlinks=False)


if __name__ == "__main__":
    main()
