"""A counting reverse proxy in front of an S3-compatible server.

Every request is forwarded unchanged; counts by HTTP method (and bytes in each direction) are
kept per "tag" that the harness sets with ``POST /__tag/<name>`` and read with
``GET /__stats``. Run: ``python s3proxy.py <listen-port> <upstream-host:port>``.
"""

import http.client
import json
import sys
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = None
LOCK = threading.Lock()
STATS = defaultdict(lambda: defaultdict(int))
TAG = ["none"]
LOCAL = threading.local()


def conn():
    c = getattr(LOCAL, "conn", None)
    if c is None:
        host, port = UPSTREAM.split(":")
        c = LOCAL.conn = http.client.HTTPConnection(host, int(port), timeout=120)
    return c


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _control(self):
        if self.path.startswith("/__tag/"):
            with LOCK:
                TAG[0] = self.path[len("/__tag/") :]
            body = b"ok"
        elif self.path == "/__stats":
            with LOCK:
                body = json.dumps({t: dict(v) for t, v in STATS.items()}).encode()
        else:
            return False
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return True

    def _forward(self):
        if self.path.startswith("/__") and self._control():
            return
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        headers = {k: v for k, v in self.headers.items() if k.lower() != "connection"}
        for attempt in range(2):
            try:
                c = conn()
                c.request(self.command, self.path, body=body, headers=headers)
                r = c.getresponse()
                data = r.read()
                break
            except (http.client.HTTPException, OSError):
                LOCAL.conn = None
                if attempt:
                    raise
        with LOCK:
            s = STATS[TAG[0]]
            s[self.command] += 1
            s["bytes_up"] += n
            s["bytes_down"] += len(data)
        self.send_response(r.status, r.reason)
        for k, v in r.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        # A HEAD response carries the object's length, not the (empty) body's.
        length = r.getheader("Content-Length") if self.command == "HEAD" else None
        self.send_header("Content-Length", length or str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _forward


if __name__ == "__main__":
    UPSTREAM = sys.argv[2]
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Proxy).serve_forever()
