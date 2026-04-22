from http.server import BaseHTTPRequestHandler
import json, os
import psycopg2
import psycopg2.extras

DB = os.environ.get("DATABASE_URL", "")

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data = self._fetch()
            self._respond(200, data)
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _fetch(self):
        conn = psycopg2.connect(DB)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("""
                SELECT
                    COUNT(*)                                            AS total_properties,
                    ROUND(AVG(deal_score)::numeric, 1)                 AS avg_score,
                    ROUND(MAX(deal_score)::numeric, 1)                 AS top_score,
                    COUNT(DISTINCT market)                             AS markets_covered,
                    SUM(CASE WHEN deal_score >= 80 THEN 1 ELSE 0 END)  AS hot_deals
                FROM properties
                WHERE run_id = (
                    SELECT MAX(id) FROM runs WHERE status = 'success'
                )
            """)
            row = cur.fetchone()
            stats = dict(row) if row else {}

            cur.execute("""
                SELECT (deal_score/10)::int * 10 AS bucket, COUNT(*) AS count
                FROM properties
                WHERE run_id = (
                    SELECT MAX(id) FROM runs WHERE status = 'success'
                )
                GROUP BY bucket ORDER BY bucket
            """)
            stats["score_distribution"] = [dict(r) for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*) as count FROM alerts WHERE acknowledged = FALSE"
            )
            stats["unacknowledged_alerts"] = cur.fetchone()["count"]
            return {k: (int(v) if hasattr(v, '__int__')
                       and not isinstance(v, float) else v)
                    for k, v in stats.items()}
        finally:
            cur.close()
            conn.close()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
