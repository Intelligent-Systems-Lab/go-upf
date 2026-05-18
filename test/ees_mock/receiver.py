#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, HTTPServer
import json, sys, time

HOST = "127.0.0.1"
PORT = 9000

class CallbackHandler(BaseHTTPRequestHandler):
    # only allow POST /callback
    def do_POST(self):
        if self.path != "/callback":
            self.send_response(404); self.end_headers()
            self.wfile.write(b"Not Found")
            return

        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        try:
            data = json.loads(raw.decode("utf-8"))
            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            print(f"\n[{timestamp}] === EES Notify Received ===")
            print(pretty)
        except Exception as e:
            print(f"\n[{timestamp}] invalid JSON: {e}\nraw={raw!r}")

        # Always reply 204 (no content) to keep sender simple
        self.send_response(204)
        self.end_headers()

    def log_message(self, fmt, *args):
        # silence default access log; keep output clean
        return

if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), CallbackHandler)
    print(f"Receiver listening on http://{HOST}:{PORT}/callback")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down receiver...")
        server.server_close()
        sys.exit(0)
