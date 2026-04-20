import os
import asyncpg

_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.environ["DATABASE_URL"],
            min_size=1,
            max_size=5,
            statement_cache_size=0,   # required for Supabase pooler
        )
    return _pool
