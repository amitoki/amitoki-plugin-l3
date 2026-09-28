"""同じCLOCK_BOOTTIME上で予定時刻からアプリ配送までを照合・集計する。"""
import functools
import json
import math
import statistics


@functools.lru_cache(maxsize=1024)
def expected_fingerprint(sequence_modulo, size):
    value = 0xCBF29CE484222325
    for offset in range(size):
        value = ((value ^ ((sequence_modulo + offset) & 255)) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def percentile(samples, percent):
    return sorted(samples)[max(0, math.ceil(len(samples) * percent / 100) - 1)] if samples else None


def measure(directory, workload, *, mode, duration_ms, allow_incomplete=False):
    sender = json.loads((directory / "a.report.json").read_text())
    receiver = json.loads((directory / "b.report.json").read_text())
    if sender["clock_domain"] != receiver["clock_domain"]:
        raise RuntimeError("片道時間の計測には同じboot ID/time namespaceが必要です")
    bench = sender["reliable_benchmark" if mode.startswith("l3") else "benchmark"]
    if not bench["complete"] and not allow_incomplete:
        raise RuntimeError("全メッセージの受付が完了していません")
    seen = [set(), set()]
    latencies = [[], []]
    last_sequence = [0, 0]
    inversions = [0, 0]
    last_received = bench["start_us"]
    for line in (directory / "receipts.jsonl").read_text().splitlines():
        receipt = json.loads(line)
        index = receipt["channel"] - 1
        if index not in (0, 1):
            raise RuntimeError("未知のchannelを受信しました")
        name = ("short", "bulk")[index]
        sequence = receipt["sequence"]
        rate, size = workload[name + "_rate"], workload[name + "_bytes"]
        if not rate or sequence in seen[index] or not 1 <= sequence <= duration_ms * rate // 1000:
            raise RuntimeError("重複・予定外の配送があります")
        if receipt["bytes"] != size or receipt["fingerprint"] != expected_fingerprint(sequence % 256, size):
            raise RuntimeError("受信した本文が送信予定と一致しません")
        due = bench["start_us"] + (sequence - 1) * 1_000_000 // rate
        latency = receipt["received_us"] - due
        if latency < 0:
            raise RuntimeError("生成予定より先に届いています")
        latencies[index].append(latency)
        seen[index].add(sequence)
        inversions[index] += sequence < last_sequence[index]
        last_sequence[index] = sequence
        last_received = max(last_received, receipt["received_us"])
    counts = [duration_ms * workload[name + "_rate"] // 1000 for name in ("short", "bulk")]
    delivered = list(map(len, seen))
    application_complete = delivered == counts
    if (not application_complete and (bench["complete"] or not allow_incomplete)) or (mode != "l3_unordered" and any(inversions)):
        raise RuntimeError("アプリ配送に欠落または順序違反があります")
    if mode.startswith("l3"):
        if any(channel["metrics"]["invalid_responses"] for channel in bench["channels"]):
            raise RuntimeError("L3の応答が不正です")
        for report in (sender, receiver):
            if report["clock_simulation"] != {"offset_us": 0, "drift_ppm": 0} or report["network"]["malformed"]:
                raise RuntimeError("模擬時計または不正なパケットがあります")
    elapsed = last_received - bench["start_us"]
    delivered_bytes = sum(delivered[index] * workload[name + "_bytes"] for index, name in enumerate(("short", "bulk")))
    metrics = {"elapsed_to_delivery_us": elapsed, "goodput_mbps": delivered_bytes * 8 / elapsed if elapsed else None,
               "delivered": delivered, "delivered_pps": sum(delivered) * 1_000_000 / elapsed if elapsed else None,
               "cpu_us_per_message": sum(report["process_usage"][key] for report in (sender, receiver)
                                         for key in ("user_us", "system_us")) / sum(delivered) if sum(delivered) else None,
               "receiver_order_inversions": inversions}
    for index, name in enumerate(("short", "bulk")):
        samples = latencies[index]
        metrics[name] = {"p50_us": percentile(samples, 50), "p99_us": percentile(samples, 99),
                         "max_us": max(samples) if samples else None,
                         "within_20ms": sum(latency <= 20_000 for latency in samples) / len(samples) if samples else None}
    acknowledged = [channel["metrics"]["acknowledged"] for channel in bench["channels"]] if mode.startswith("l3") else bench["acknowledged"]
    if len(acknowledged) != 2 or any(not 0 <= acknowledged[index] <= counts[index] for index in (0, 1)) or (bench["complete"] and acknowledged != counts):
        raise RuntimeError("受付確認数と完了判定が一致しません")
    return {"workload": workload, "mode": mode, "complete": bench["complete"], "application_complete": application_complete,
            "offered": counts, "acknowledged": acknowledged, "metrics": metrics, "reports": {"a": sender, "b": receiver}}


def report(directory, outcomes):
    lines = ["# 信頼性配送とTCPの比較", "", "同じホストの別network namespaceを内部Docker Ethernet bridgeで接続。本文・予定レート・記録処理を合わせた比較。",
             "L3時計モード: " + ", ".join(sorted({outcome.get("clock_mode", "authority") for outcome in outcomes})),
             "CLOCK_BOOTTIMEのboot ID/time namespace一致を確認し、予定時刻から受信アプリへ渡すまでの片道時間を計測する。ACK時間は主指標へ混ぜない。",
             "接続・L3同期の準備、生成待ち、受信の順序待ちを含む。本文は全件照合。TCPはNODELAY有効、既定のLinux輻輳制御、1接続またはクラス別2接続。",
             "offloadは両端で無効。混雑・損失は同じLinux netemを全方式へ適用し、L3の送信側上限は100MB/s。",
             "goodputは全本文ビット/最後のアプリ配送までの時間。固定負荷の結果で、物理NICの最大速度ではない。CPUは送受信プロセスに計上されたuser+system時間。", "",
             "未完了試行も記録し、速度・遅延の中央値には全件の配送と受付確認が完了した試行だけを使う。完了回数と併せて読む。", "",
             "| 条件 | 方式 | 完了/試行 | short p99 ms（中央値） | bulk p99 ms（中央値） | 有効Mbps（中央値） | CPU µs/件（中央値） |", "|---|---|---:|---:|---:|---:|---:|"]
    grouped = {}
    for outcome in outcomes:
        grouped.setdefault((outcome["workload"]["name"], outcome["mode"]), []).append(outcome)
    summary = []
    for (workload, mode), trials in grouped.items():
        samples = [trial["metrics"] for trial in trials if trial["complete"]]
        row = {"workload": workload, "mode": mode, "repetitions": len(trials), "completed": len(samples),
               "offered": [sum(trial["offered"][index] for trial in trials) for index in (0, 1)],
               "delivered": [sum(trial["metrics"]["delivered"][index] for trial in trials) for index in (0, 1)],
               "acknowledged": [sum(trial["acknowledged"][index] for trial in trials) for index in (0, 1)]}
        for name in ("short", "bulk"):
            values = [sample[name]["p99_us"] for sample in samples if sample[name]["p99_us"] is not None]
            row[name + "_p99_us"] = {"median": statistics.median(values), "min": min(values), "max": max(values)} if values else None
        for name in ("goodput_mbps", "cpu_us_per_message"):
            values = [sample[name] for sample in samples]
            row[name] = {"median": statistics.median(values), "min": min(values), "max": max(values)} if values else None
        summary.append(row)
        latency = lambda name: f"{row[name + '_p99_us']['median']/1000:.3f}" if row[name + "_p99_us"] else "—"
        value = lambda name: f"{row[name]['median']:.3f}" if row[name] else "—"
        lines.append(f"| {workload} | {mode} | {len(samples)}/{len(trials)} | {latency('short')} | {latency('bulk')} | {value('goodput_mbps')} | {value('cpu_us_per_message')} |")
    incomplete = [outcome for outcome in outcomes if not outcome["complete"]]
    if incomplete:
        lines += ["", "## 未完了試行", "", "順序は[short, bulk]。受信アプリへ届いていても、送信側の受付確認が終わらなければ未完了とする。", ""]
        for outcome in incomplete:
            lines.append(f"- {outcome['repetition']}回目 {outcome['workload']['name']} / {outcome['mode']}: 予定{outcome['offered']}、配送{outcome['metrics']['delivered']}、確認{outcome['acknowledged']}")
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "report.md").write_text("\n".join(lines) + "\n")
