#!/usr/bin/env python3
"""ビルド済みプラグインからCLIインストール用配布物を作成する。"""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("binary", type=Path)
parser.add_argument("destination", type=Path)
options = parser.parse_args()
manifest = json.loads(subprocess.check_output([str(options.binary.resolve()), "--describe"], text=True))
architecture = {"aarch64": "aarch64", "x86_64": "x86_64"}[platform.machine()]
target = f"{architecture}-unknown-linux-gnu"
binary = f"amitoki-plugin-{manifest['name']}"
options.destination.mkdir(parents=True, exist_ok=True)
shutil.copy2(options.binary, options.destination / binary)
(options.destination / binary).chmod(0o755)
with options.binary.open("rb") as stream:
    digest = hashlib.file_digest(stream, "sha256").hexdigest()
package = {"manifest": manifest, "target": target, "binary": binary, "sha256": digest}
contents = json.dumps(package, ensure_ascii=False, indent=2) + "\n"
(options.destination / "plugin.json").write_text(contents)
(options.destination / f"plugin-{target}.json").write_text(contents)
shutil.copy2(options.binary, options.destination / f"{binary}-{target}")
print(options.destination)
