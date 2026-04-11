from http.server import BaseHTTPRequestHandler
import asyncio, json, os
import asyncpg

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        rows = asyncio.run(self._fetch())
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(rows).encode())

    def do_POST(self):
        # POST /api/alerts?id=123&action=ack
        from urllib.parse import urlparse, parse_qs
        params = parse_qs(urlparse(self.path).query)
        alert_id = int(params.get("id", [0])[0])
        asyncio.run(self._ack(alert_id))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"acknowledged": True}).encode())

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
