import os
import asyncpg

async def get_connection():
    db_url = os.environ.get("DATABASE_URL", "")
    conn = await asyncpg.connect(db_url, statement_cache_size=0)
    return conn
