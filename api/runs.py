from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import json, os
import psycopg2
import psycopg2.extras

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            limit = int(params.get("limit", ["10"])[0])
            self._respond(200, self._fetch(limit))
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _fetch(self, limit):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT %s", (limit,)
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
