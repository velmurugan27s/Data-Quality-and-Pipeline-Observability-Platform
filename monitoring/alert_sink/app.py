"""Minimal Alertmanager webhook receiver: logs alerts to stdout (docker compose logs -f alert-sink)."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        for a in body.get("alerts", []):
            lbl, ann = a.get("labels", {}), a.get("annotations", {})
            print(f"[{a.get('status', '?').upper():8}] {lbl.get('severity', '-'):8} {lbl.get('alertname')} "
                  f"table={lbl.get('table', '-')} :: {ann.get('summary', '')}", flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):  # silence default access log
        pass


if __name__ == "__main__":
    print("alert-sink listening on :5001", flush=True)
    HTTPServer(("0.0.0.0", 5001), Handler).serve_forever()
