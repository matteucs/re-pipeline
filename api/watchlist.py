from http.server import BaseHTTPRequestHandler
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
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            self._respond(201, self._insert(body))
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _fetch(self):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SELECT * FROM watchlist WHERE active = TRUE")
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def _insert(self, body):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                INSERT INTO watchlist
                (label, market, property_type, min_deal_score, min_traffic, max_price_m)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                body.get("label"), body.get("market"),
                body.get("property_type"),
                body.get("min_deal_score", 70),
                body.get("min_traffic", 0),
                body.get("max_price_m"),
            ))
            conn.commit()
            row = cur.fetchone()
            return {"id": row["id"], "label": body.get("label")}
        finally:
            cur.close()
            conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
