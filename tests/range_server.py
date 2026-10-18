"""Local HEAD/Range GET fixture, not an implementation of RustFS."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import patch


@contextmanager
def range_server(data: bytes):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()

        def do_GET(self):
            value = self.headers.get("Range")
            if value:
                start, end = map(int, value.removeprefix("bytes=").split("-"))
                end = min(end, len(data) - 1)
                body = data[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            else:
                body = data
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with patch.dict("os.environ", {
        "RUSTFS_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
        "RUSTFS_ACCESS_KEY": "local-test", "RUSTFS_SECRET_KEY": "local-test",
        "RUSTFS_BUCKET": "fixture",
    }):
        try:
            yield
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
