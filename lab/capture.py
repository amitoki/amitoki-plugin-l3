"""先頭のEthernetフレームをclassic PCAPへ保存し、独自L3の実通信を残す。"""
import select
import socket
import struct
import threading
import time

ETHER_TYPE = 0x88B5
# 生ログの容量を一定にする。送受信性能の全トレースではない。
CAPTURE_LIMIT = 128


class Capture:
    def __init__(self, path):
        self.path = path
        self.stop = threading.Event()
        self.failure = None
        self.count = 0
        self.sockets = []
        try:
            for interface in ("a1", "a2", "b1", "b2"):
                channel = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETHER_TYPE))
                self.sockets.append(channel)
                channel.bind((interface, 0))
        except Exception:
            for channel in self.sockets:
                channel.close()
            raise
        self.thread = threading.Thread(target=self.record, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def record(self):
        try:
            with self.path.open("wb") as output:
                output.write(struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1))
                while not self.stop.is_set() and self.count < CAPTURE_LIMIT:
                    ready, _, _ = select.select(self.sockets, [], [], 0.05)
                    for channel in ready:
                        frame = channel.recv(65535)
                        # 端点の受信だけを取得する。各経路にはルーターが1台ある。
                        valid_hops = frame[21] in (15, 16) if frame[19] in (5, 6) else frame[21] == 15
                        if frame[12:14] != b"\x88\xb5" or frame[14:19] != b"AMTK\x02" or not valid_hops:
                            raise RuntimeError("EtherType、独自ヘッダ、hop減算が一致しません")
                        timestamp = time.time_ns()
                        output.write(struct.pack("<IIII", timestamp // 1_000_000_000, (timestamp // 1000) % 1_000_000, len(frame), len(frame)))
                        output.write(frame)
                        self.count += 1
                        if self.count == CAPTURE_LIMIT:
                            break
        except Exception as error:
            self.failure = error
        finally:
            for channel in self.sockets:
                channel.close()

    def __exit__(self, *exception):
        self.stop.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("PCAP取得が停止しませんでした")
        if self.failure:
            raise self.failure
