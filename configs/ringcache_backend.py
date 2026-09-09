#!/usr/bin/env python3
"""Manual-test backend for the ringcache HTTP filter.

Serves deterministic bodies and response headers per path so the ringcache
RFC 9111 guards, capacity limits, and cache-key variants can be exercised
against a running Envoy. Logs every request it receives to stdout.
"""

import sys

from http import server


class Handler(server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def send_plain(self, code, body, extra=()):
        """Send a minimal response with only Content-Type and Content-Length."""
        self.send_response_only(code)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        path = self.path
        if path == '/a':
            self.send_plain(200, b'A' * 600)
        elif path == '/b':
            self.send_plain(200, b'B' * 600)
        elif path == '/medium':
            self.send_plain(200, b'M' * 900)
        elif path == '/giant':
            self.send_plain(200, b'G' * 100000)
        elif path == '/private':
            self.send_plain(200, b'secret', (('Cache-Control', 'private'),))
        elif path == '/setcookie':
            self.send_plain(200, b'cookie', (('Set-Cookie', 'sid=1'),))
        elif path == '/vary':
            self.send_plain(200, b'variant', (('Vary', 'Accept-Encoding'),))
        elif path == '/tenant':
            self.send_plain(200, self.headers.get('x-tenant', 'none').encode())
        else:
            self.send_plain(200, b'hello')

    def log_message(self, fmt, *args):
        print(f'BACKEND {self.command} {self.path}', flush=True)


class ReusableThreadingHTTPServer(server.ThreadingHTTPServer):
    allow_reuse_address = True


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    with ReusableThreadingHTTPServer(('127.0.0.1', port), Handler) as httpd:
        print(f'ringcache test backend listening on 127.0.0.1:{port}', flush=True)
        httpd.serve_forever()


if __name__ == '__main__':
    main()
