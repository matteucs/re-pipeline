from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import json, os
import psycopg2
import psycopg2.extras

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self._respond(200, self._fetch())
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def do_POST(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            alert_id = int(params.get("id", [0])[0])
            self._ack(alert_id)
            self._respond(200, {"acknowledged": True})
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _fetch(self):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(
                "SELECT * FROM alerts WHERE acknowledged = FALSE ORDER BY triggered_at DESC"
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def _ack(self, alert_id):
        conn = psycopg2.connect(DB)
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE alerts SET acknowledged = TRUE WHERE id = %s", (alert_id,)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
