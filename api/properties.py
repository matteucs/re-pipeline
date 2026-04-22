from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import asyncio, json, os
import asyncpg

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            limit     = int(params.get("limit",      ["25"])[0])
            min_score = float(params.get("min_score", ["50"])[0])
            market    = params.get("market", [None])[0]
            rows = asyncio.run(self._fetch(limit, min_score, market))
            self._respond(200, rows)
        except Exception as e:
            self._respond(500, {"error": str(e)})

    async def _fetch(self, limit, min_score, market):
        conn = await asyncpg.connect(DB, statement_cache_size=0)
        try:
            q = """
                SELECT * FROM properties
                WHERE deal_score >= $1
                AND run_id = (
                    SELECT MAX(id) FROM runs WHERE status = 'success'
                )
            """
            args = [min_score]
            if market:
                q += " AND market = $2"
                args.append(market)
            q += f" ORDER BY deal_score DESC LIMIT ${len(args) + 1}"
            args.append(limit)
            rows = await conn.fetch(q, *args)
            return [dict(r) for r in rows]
        finally:
            await conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
