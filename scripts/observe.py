"""観測JSONとビルド済みWeb画面をlocalhostだけで配信する。"""
import argparse
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
from urllib.parse import unquote, urlsplit

# このAPIは観測JSONだけを読み、delivery logや設定ファイルを公開しない。
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_REPORTS = 64


def snapshots(directory):
    live = sorted(directory.glob("*.live.json"))
    paths = live or sorted(directory.glob("*.report.json"))
    reports = []
    for path in paths[:MAX_REPORTS]:
        if path.is_symlink() or not path.is_file():
            continue
        try:
            with path.open("rb") as source:
                content = source.read(MAX_REPORT_BYTES + 1)
            if len(content) > MAX_REPORT_BYTES:
                continue
            report = json.loads(content)
            if not isinstance(report, dict) or "network" not in report:
                continue
            reports.append(dict(name=path.name, modified=path.stat().st_mtime, report=report))
        except (OSError, ValueError):
            continue
    return reports


class Handler(BaseHTTPRequestHandler):
    def __init__(self, *arguments, reports, assets, **options):
        self.reports, self.assets = reports, assets
        super().__init__(*arguments, **options)

    def do_GET(self):
        # DNS rebindingと第三者サイトからの読み取りを同時に拒否する。
        expected = f"127.0.0.1:{self.server.server_port}"
        if self.headers.get("Host") != expected or self.headers.get("Origin") not in (None, "http://" + expected):
            self.send_error(403)
            return
        path = urlsplit(self.path).path
        if path == "/api/reports":
            self.respond(json.dumps(snapshots(self.reports)).encode(), "application/json")
            return
        relative = "index.html" if path == "/" else unquote(path).lstrip("/")
        candidate = (self.assets / relative).resolve()
        if not candidate.is_relative_to(self.assets) or not candidate.is_file():
            self.send_error(404)
            return
        self.respond(candidate.read_bytes(), mimetypes.guess_type(candidate.name)[0] or "application/octet-stream")

    def respond(self, content, content_type):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'")
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--assets", type=Path, default=Path(__file__).resolve().parents[1] / "web/dist")
    parser.add_argument("--port", type=int, default=8720)
    options = parser.parse_args()
    directory, assets = options.reports.resolve(), options.assets.resolve()
    if not directory.is_dir() or not (assets / "index.html").is_file():
        parser.error("観測ディレクトリとビルド済みweb/distが必要です")
    server = ThreadingHTTPServer(("127.0.0.1", options.port), partial(Handler, reports=directory, assets=assets))
    print(f"http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
