#!/usr/bin/env python3
"""
Lightweight HTTP server for the Weekly Disagreement Scanner dashboard.
Uses Python stdlib only — no Flask or other frameworks.
"""

import os
import sys
import json
import argparse
import mimetypes
from http.server import HTTPServer, SimpleHTTPRequestHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "disagreement_data.json")
DASHBOARD_FILE = os.path.join(SCRIPT_DIR, "disagreement_dashboard.html")


class DisagreementHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=SCRIPT_DIR, **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._serve_file(DASHBOARD_FILE, "text/html")
        elif self.path == "/api/data":
            self._serve_file(DATA_FILE, "application/json", cors=True)
        else:
            # Serve static files from script directory
            super().do_GET()

    def _serve_file(self, filepath, content_type, cors=False):
        if not os.path.exists(filepath):
            self.send_error(404, f"File not found: {os.path.basename(filepath)}")
            return
        with open(filepath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        print(f"[server] {args[0]}")


def main():
    parser = argparse.ArgumentParser(description="Disagreement Dashboard Server")
    parser.add_argument("--port", type=int, default=5050, help="Port (default: 5050)")
    parser.add_argument("--scan", action="store_true", help="Run scanner before serving")
    parser.add_argument("--core", action="store_true", help="Scan core tickers only (with --scan)")
    args = parser.parse_args()

    if args.scan:
        from weekly_disagreement_scanner import run_scan
        run_scan(core_only=args.core)

    if not os.path.exists(DATA_FILE):
        print(f"WARNING: {DATA_FILE} not found. Run the scanner first or use --scan.")

    server = HTTPServer(("", args.port), DisagreementHandler)
    print(f"Serving dashboard at http://localhost:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
