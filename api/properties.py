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
            limit     = int(params.get("limit",      ["25"])[0])
            min_score = float(params.get("min_score", ["50"])[0])
            market    = params.get("market", [None])[0]
            rows = self._fetch(limit, min_score, market)
            self._respond(200, rows)
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _fetch(self, limit, min_score, market):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            if market:
                cur.execute("""
                    SELECT * FROM properties
                    WHERE deal_score >= %s AND market = %s
                    AND run_id = (
                        SELECT MAX(id) FROM runs WHERE status = 'success'
                    )
                    ORDER BY deal_score DESC LIMIT %s
                """, (min_score, market, limit))
            else:
                cur.execute("""
                    SELECT * FROM properties
                    WHERE deal_score >= %s
                    AND run_id = (
                        SELECT MAX(id) FROM runs WHERE status = 'success'
                    )
                    ORDER BY deal_score DESC LIMIT %s
                """, (min_score, limit))
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
