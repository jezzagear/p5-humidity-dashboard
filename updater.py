"""
Humidity Dashboard — Background Updater
========================================
Run this once in Terminal, then use the "Update Data" button in dashboard.html.

    python updater.py

Listens on http://localhost:5051. When the dashboard button is clicked it
runs refresh_dashboard.py, then the page auto-reloads with fresh data.
Keep this running in the background while you use the dashboard.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import subprocess, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 5051


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # CORS headers so the HTML file can call this from any origin
        if self.path == "/refresh":
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"running")
            self.wfile.flush()

            print("[updater] Running refresh_dashboard.py …", flush=True)
            result = subprocess.run(
                [sys.executable, "refresh_dashboard.py"],
                cwd=HERE,
                capture_output=False,
            )
            if result.returncode == 0:
                print("[updater] Done ✅", flush=True)
            else:
                print(f"[updater] Script exited with code {result.returncode}", flush=True)

        elif self.path == "/ping":
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # silence default request logs


if __name__ == "__main__":
    print(f"\n── Humidity Dashboard Updater ──")
    print(f"   Listening on http://localhost:{PORT}")
    print(f"   Open dashboard.html and click 'Update Data'")
    print(f"   Stop: Ctrl+C\n")
    try:
        HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\n[updater] Stopped.")
