from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import asyncio, json, os
import asyncpg

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            rows = asyncio.run(self._fetch())
            self._respond(200, rows)
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def do_POST(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            alert_id = int(params.get("id", [0])[0])
            asyncio.run(self._ack(alert_id))
            self._respond(200, {"acknowledged": True})
        except Exception as e:
            self._respond(500, {"error": str(e)})

    async def _fetch(self):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            rows = await conn.fetch(
                "SELECT * FROM alerts WHERE acknowledged = FALSE ORDER BY triggered_at DESC"
            )
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    async def _ack(self, alert_id):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            await conn.execute(
                "UPDATE alerts SET acknowledged = TRUE WHERE id = $1", alert_id
            )
        finally:
            await conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
