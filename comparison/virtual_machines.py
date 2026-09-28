"""この試験で起動したQEMUプロセスだけを所有し、別カーネルの時計を用意する。"""
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import time

BOOT_TIMEOUT = 180
SSH_TIMEOUT = 5
# パッケージ導入やDBを行わない2台の小さなUbuntuゲスト。
MEMORY_MIB = 1024
CPU_COUNT = 2
MACS = ["02:88:b5:01:00:01", "02:88:b5:01:00:02"]


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def verify_image(image):
    checksums = image.parent / "SHA256SUMS"
    expected = next(line.split()[0] for line in checksums.read_text().splitlines()
                    if line.split()[-1].lstrip("*") == image.name)
    with image.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise RuntimeError("VMイメージのSHA256が一致しません")
    return actual


class VirtualMachines:
    def __init__(self, directory, image, *, edges=None):
        if not os.access("/dev/kvm", os.R_OK | os.W_OK):
            raise RuntimeError("/dev/kvmの読み書き権限が必要です")
        self.directory = directory
        self.image = image
        self.image_sha256 = verify_image(image)
        edges = edges or [(0, "l3test0", 1, "l3test0")]
        node_count = max(max(left, right) for left, _, right, _ in edges) + 1
        self.ports = [unused_port() for _ in range(node_count)]
        link_ports = [unused_port() for _ in edges]
        self.interfaces = {node: [] for node in range(node_count)}
        for index, (left, left_name, right, right_name) in enumerate(edges):
            for side, node, name in [(0, left, left_name), (1, right, right_name)]:
                self.interfaces[node].append(dict(name=name, mac=f"02:88:b5:01:00:{index*2+side+1:02x}",
                    netdev=f"lab{index}", role="listen" if side == 0 else "connect", port=link_ports[index]))
        if len(set([*self.ports, *link_ports])) != node_count + len(edges):
            raise RuntimeError("空きportが重複しました。新しい試験ディレクトリで再実行してください")
        self.processes = []
        self.process_by_node = {}
        self.logs = []
        self.key = directory / "id_ed25519"
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.key)], check=True)

    def ssh(self, node, *arguments):
        return ["ssh", "-i", str(self.key), "-p", str(self.ports[node]),
                "-o", "BatchMode=yes", "-o", f"ConnectTimeout={SSH_TIMEOUT}",
                "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={self.directory / 'known_hosts'}",
                "ubuntu@127.0.0.1", shlex.join(arguments)]

    def run(self, node, *arguments, **options):
        return subprocess.run(self.ssh(node, *arguments), check=True, capture_output=True, text=True,
                              timeout=30, **options).stdout

    def copy(self, node, source, destination):
        # sshのstdinを使い、SCPのリモートパス解釈とシェル展開を避ける。
        with Path(source).open("rb") as stream:
            subprocess.run(self.ssh(node, "tee", destination), stdin=stream, stdout=subprocess.DEVNULL,
                           stderr=subprocess.PIPE, check=True, timeout=30)

    def start(self, node, *, wait=True):
        directory = self.directory / str(node)
        directory.mkdir()
        cloud = {"hostname": f"amitoki-l3-{node}", "manage_etc_hosts": True, "ssh_pwauth": False,
                 "ssh_authorized_keys": [self.key.with_suffix(".pub").read_text().strip()]}
        (directory / "user-data").write_text("#cloud-config\n" + json.dumps(cloud))
        (directory / "meta-data").write_text(f"instance-id: amitoki-l3-{node}\nlocal-hostname: amitoki-l3-{node}\n")
        subprocess.run([os.environ.get("AMITOKI_GENISOIMAGE", "genisoimage"), "-quiet", "-output", str(directory / "seed.iso"),
                        "-volid", "cidata", "-joliet", "-rock", str(directory / "user-data"), str(directory / "meta-data")], check=True)
        subprocess.run(["qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", str(self.image),
                        str(directory / "disk.qcow2"), "6G"], check=True)
        network_arguments = []
        for interface in self.interfaces[node]:
            network_arguments += ["-netdev", f"socket,id={interface['netdev']},{interface['role']}=127.0.0.1:{interface['port']}",
                "-device", f"virtio-net-pci,netdev={interface['netdev']},mac={interface['mac']}"]
        log = (directory / "qemu.log").open("w")
        self.logs.append(log)
        process = subprocess.Popen([
            "qemu-system-x86_64", "-name", f"amitoki-l3-test-{node}", "-enable-kvm", "-cpu", "host",
            "-smp", str(CPU_COUNT), "-m", str(MEMORY_MIB), "-display", "none",
            "-drive", f"file={directory / 'disk.qcow2'},format=qcow2,if=virtio",
            "-drive", f"file={directory / 'seed.iso'},format=raw,media=cdrom,readonly=on",
            "-netdev", f"user,id=control,restrict=on,hostfwd=tcp:127.0.0.1:{self.ports[node]}-:22",
            "-device", "virtio-net-pci,netdev=control", *network_arguments,
            "-serial", f"file:{directory / 'serial.log'}",
        ], stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(process)
        self.process_by_node[node] = process
        self.wait_listeners(node)
        if wait:
            self.wait_ready(node)

    def wait_listeners(self, node):
        expected = {interface["port"] for interface in self.interfaces[node] if interface["role"] == "listen"}
        deadline = time.monotonic() + SSH_TIMEOUT
        while time.monotonic() < deadline:
            # 接続probeはQEMUの実験リンクへ混入するため、listen状態を/procで確認する。
            ports = {int(line.split()[1].split(":")[1],16) for line in Path("/proc/net/tcp").read_text().splitlines()[1:] if line.split()[3] == "0A"}
            if expected <= ports:
                return
            if self.process_by_node[node].poll() is not None:
                break
            time.sleep(0.01)
        raise RuntimeError(f"VM {node}のリンク待受が失敗しました")

    def wait_ready(self, node):
        directory = self.directory / str(node)
        process = self.process_by_node[node]
        deadline = time.monotonic() + BOOT_TIMEOUT
        progress_at = 0
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"VM {node}が終了しました: {directory / 'qemu.log'}")
            try:
                ready = subprocess.run(self.ssh(node, "test", "-f", "/var/lib/cloud/instance/boot-finished"),
                                       capture_output=True, timeout=SSH_TIMEOUT + 1)
                if ready.returncode == 0:
                    self.configure(node)
                    return
            except subprocess.TimeoutExpired:
                pass
            if time.monotonic() > progress_at:
                print(f"VM {node}: 起動待ち", flush=True)
                progress_at = time.monotonic() + 20
            time.sleep(1)
        raise RuntimeError(f"VM {node}の起動待ちがタイムアウトしました")

    def configure(self, node):
        interfaces = json.loads(self.run(node, "ip", "-j", "link"))
        for definition in self.interfaces[node]:
            name = next(interface["ifname"] for interface in interfaces if interface.get("address") == definition["mac"])
            target = definition["name"]
            self.run(node, "sudo", "ip", "link", "set", name, "down")
            self.run(node, "sudo", "ip", "link", "set", name, "name", target)
            self.run(node, "sudo", "sysctl", "-w", f"net.ipv6.conf.{target}.disable_ipv6=1")
            self.run(node, "sudo", "ip", "address", "flush", "dev", target)
            self.run(node, "sudo", "ip", "link", "set", target, "up")
        self.run(node, "mkdir", "-p", "/home/ubuntu/l3")

    def close(self):
        # PIDをファイルから読み直さず、この実行が所有するPopenだけを終了する。
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in self.logs:
            log.close()
