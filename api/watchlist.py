from http.server import BaseHTTPRequestHandler
import asyncio, json, os
import asyncpg

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        rows = asyncio.run(self._fetch())
        self._respond(rows)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        result = asyncio.run(self._insert(body))
        self._respond(result, 201)

    def _respond(self, data, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    async def _fetch(self):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            rows = await conn.fetch("SELECT * FROM watchlist WHERE active = TRUE")
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    async def _insert(self, body):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            row = await conn.fetchrow(
                """INSERT INTO watchlist
                   (label, market, property_type, min_deal_score, min_traffic, max_price_m)
                   VALUES ($1,$2,$3,$4,$5,$6) RETURNING id""",
                body.get("label"), body.get("market"), body.get("property_type"),
                body.get("min_deal_score", 70), body.get("min_traffic", 0),
                body.get("max_price_m"),
            )
            return {"id": row["id"], "label": body.get("label")}
        finally:
            await conn.close()
