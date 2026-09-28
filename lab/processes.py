"""実験プロセスの起動確認と通常終了。"""
import json
import signal
import subprocess
import time

# 起動はreadyファイルで確認する。この値は準備失敗の打切り上限。
READY_TIMEOUT_SECONDS = 5
PROCESS_TIMEOUT_SECONDS = 5


def wait_ready(processes, directory, *, different_clocks=False):
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if any(process.poll() is not None for _, process in processes):
            raise RuntimeError(f"ノードが起動前に終了しました: {directory}")
        paths = [directory / f"{name}.ready.json" for name, _ in processes]
        try:
            ready = [json.loads(path.read_text()) for path in paths]
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.01)
            continue
        if not different_clocks and len({entry["clock_domain"] for entry in ready}) != 1:
            raise RuntimeError("時計ドメインが一致しません")
        return
    raise RuntimeError(f"ノードの準備待ちがタイムアウトしました: {directory}")


def stop_processes(processes):
    for _, process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
    for name, process in processes:
        try:
            code = process.wait(timeout=PROCESS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError(f"{name}を通常終了できませんでした") from None
        if code != 0:
            raise RuntimeError(f"{name}がexit={code}で終了しました")
