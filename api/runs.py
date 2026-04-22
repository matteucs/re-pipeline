from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import asyncio, json, os
import asyncpg

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            limit = int(params.get("limit", ["10"])[0])
            rows = asyncio.run(self._fetch(limit))
            self._respond(200, rows)
        except Exception as e:
            self._respond(500, {"error": str(e)})

    async def _fetch(self, limit):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            rows = await conn.fetch(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT $1", limit
            )
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
