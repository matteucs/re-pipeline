from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self._respond(200, {
            "status": "queued",
            "message": "Pipeline runs are managed by the Railway scheduler"
        })

    def do_GET(self):
        self._respond(200, {
            "status": "ok",
            "message": "POST to this endpoint to trigger a run"
        })

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
