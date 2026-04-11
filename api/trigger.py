from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Vercel can't run long jobs — respond with instructions
        # to call your Railway scheduler's webhook endpoint instead
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "queued",
            "message": "Trigger sent to scheduler",
            "note": "Pipeline runs are managed by Railway scheduler"
        }).encode())
