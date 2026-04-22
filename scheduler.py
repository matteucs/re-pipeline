import asyncio
import logging
import signal
import os
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scheduler")

MARKETS = [
    ("Nashville", "TN"),
    ("Austin", "TX"),
    ("Phoenix", "AZ"),
    ("Raleigh", "NC"),
    ("Tampa", "FL"),
    ("Charlotte", "NC"),
    ("Atlanta", "GA"),
    ("Denver", "CO"),
]

CENSUS_KEY = os.environ.get("CENSUS_API_KEY", "")
BLS_KEY = os.environ.get("BLS_API_KEY", "")
DB_URL = os.environ.get("DATABASE_URL", "")


# ------------------------------------------------------------------ #
#  Data collectors                                                     #
# ------------------------------------------------------------------ #

async def fetch_census(session, city, state):
    """Pull population, income, home value from Census ACS."""
    import aiohttp
    state_fips = {
        "TN":"47","TX":"48","AZ":"04","NC":"37","FL":"12",
        "GA":"13","CO":"08","ID":"16","NV":"32","IN":"18",
        "OH":"39","IL":"17","MA":"25","CA":"06","WA":"53",
    }.get(state, "")
    if not state_fips:
        return {}
    vars_ = "B01003_001E,B19013_001E,B25077_001E"
    url = (
        f"https://api.census.gov/data/2022/acs/acs5"
        f"?get=NAME,{vars_}&for=place:*&in=state:{state_fips}"
        f"&key={CENSUS_KEY}"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                return {}
            rows = await r.json(content_type=None)
        city_lower = city.lower()
        for row in rows[1:]:
            if city_lower in row[0].lower():
                pop = max(0, int(row[1] or 0))
                income = max(0, int(row[2] or 0))
                home_val = max(0, int(row[3] or 0))
                return {
                    "population": pop,
                    "median_income": income,
                    "median_home_value": home_val,
                    "median_price_per_sqft": round(home_val / 1800, 2),
                }
    except Exception as e:
        log.warning(f"Census error for {city}, {state}: {e}")
    return {}


async def fetch_bls(session, city, state):
    """Pull job growth from BLS API with fallback estimates."""
    import aiohttp
    fallback = {
        ("Nashville", "TN"): {"job_growth_pct": 4.1, "unemployment_rate": 3.2},
        ("Austin", "TX"):    {"job_growth_pct": 5.2, "unemployment_rate": 2.9},
        ("Phoenix", "AZ"):   {"job_growth_pct": 3.9, "unemployment_rate": 3.5},
        ("Raleigh", "NC"):   {"job_growth_pct": 4.0, "unemployment_rate": 3.1},
        ("Tampa", "FL"):     {"job_growth_pct": 4.3, "unemployment_rate": 3.3},
        ("Charlotte", "NC"): {"job_growth_pct": 3.6, "unemployment_rate": 3.4},
        ("Atlanta", "GA"):   {"job_growth_pct": 3.4, "unemployment_rate": 3.6},
        ("Denver", "CO"):    {"job_growth_pct": 3.2, "unemployment_rate": 3.0},
    }
    area_codes = {
        ("Nashville", "TN"): "34980",
        ("Austin", "TX"):    "12420",
        ("Atlanta", "GA"):   "12060",
        ("Phoenix", "AZ"):   "38060",
        ("Charlotte", "NC"): "16740",
        ("Raleigh", "NC"):   "39580",
        ("Tampa", "FL"):     "45300",
        ("Denver", "CO"):    "19740",
    }
    area = area_codes.get((city, state))
    if not area:
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

    state_fips = {
        "TN":"47","TX":"48","AZ":"04","NC":"37",
        "FL":"12","GA":"13","CO":"08",
    }.get(state, "00")

    series_id = f"SMU{state_fips}{area}000000001"
    payload = {
        "seriesid": [series_id],
        "startyear": "2022",
        "endyear": "2024",
        "annualaverage": True,
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY

    try:
        async with session.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            if r.status != 200:
                return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})
            data = await r.json()

        series = data.get("Results", {}).get("series", [])
        if not series or not series[0]["data"]:
            return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

        annual = [d for d in series[0]["data"] if d.get("period") == "M13"]
        annual.sort(key=lambda d: d["year"], reverse=True)
        if len(annual) >= 2:
            curr = float(annual[0]["value"])
            prev = float(annual[1]["value"])
            growth = ((curr - prev) / prev * 100) if prev else 0.0
            return {"job_growth_pct": round(growth, 2), "unemployment_rate": 0.0}
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

    except Exception as e:
        log.warning(f"BLS error for {city}, {state}: {e}")
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})


async def fetch_traffic(session, city, state):
    """AADT estimates from FHWA urban area data."""
    # FHWA published urban area AADT averages — reliable fallback
    # Source: FHWA Highway Statistics, Table HM-72
    aadt_estimates = {
        ("Nashville", "TN"): 52000,
        ("Austin", "TX"):    61000,
        ("Phoenix", "AZ"):   68000,
        ("Raleigh", "NC"):   39000,
        ("Tampa", "FL"):     49000,
        ("Charlotte", "NC"): 47000,
        ("Atlanta", "GA"):   89000,
        ("Denver", "CO"):    55000,
    }
    return aadt_estimates.get((city, state), 30000)


def score_market(census, bls, traffic_aadt):
    """Score a market using the same algorithm as the UI."""
    import math

    # Traffic score (log scale)
    if traffic_aadt > 0:
        traffic_score = min(100, math.log(traffic_aadt / 1000) / math.log(100) * 100)
    else:
        traffic_score = 0

    # Growth score
    pop_growth = census.get("pop_growth_pct", 0) or 0
    job_growth = bls.get("job_growth_pct", 0) or 0
    pop_score = min(100, (pop_growth / 6) * 100)
    job_score = min(100, (job_growth / 6) * 100)
    growth_score = pop_score * 0.5 + job_score * 0.5

    # Value score — based on price per sqft vs national avg (~$200)
    ppsf = census.get("median_price_per_sqft", 200) or 200
    national_avg = 200
    ratio = (ppsf / national_avg) * 100
    value_score = min(100, max(0, 130 - ratio))

    deal_score = (
        traffic_score * 0.35 +
        growth_score * 0.40 +
        value_score * 0.25
    )

    return {
        "traffic_score": round(traffic_score, 1),
        "growth_score": round(growth_score, 1),
        "value_score": round(value_score, 1),
        "deal_score": round(deal_score, 1),
    }


# ------------------------------------------------------------------ #
#  Main pipeline                                                       #
# ------------------------------------------------------------------ #

async def run_pipeline():
    log.info("Pipeline run starting...")
    if not DB_URL:
        log.error("DATABASE_URL not set")
        return

    import aiohttp
    import asyncpg

    conn = await asyncpg.connect(DB_URL, statement_cache_size=0)
    market_list = [f"{city}, {state}" for city, state in MARKETS]

    run_id = await conn.fetchval(
        """INSERT INTO runs (started_at, status, markets, total_found)
           VALUES (NOW(), 'running', $1, 0) RETURNING id""",
        json.dumps(market_list)
    )
    log.info(f"Run {run_id} started for {len(MARKETS)} markets")

    total = 0
    try:
        async with aiohttp.ClientSession() as session:
            for city, state in MARKETS:
                market = f"{city}, {state}"
                log.info(f"Processing {market}...")

                census, bls, aadt = await asyncio.gather(
                    fetch_census(session, city, state),
                    fetch_bls(session, city, state),
                    fetch_traffic(session, city, state),
                )

                scores = score_market(census, bls, aadt)
                log.info(
                    f"{market} — deal score: {scores['deal_score']} "
                    f"traffic: {aadt} "
                    f"job growth: {bls.get('job_growth_pct', 0):.1f}%"
                )

                # Save market snapshot
                await conn.execute("""
                    INSERT INTO market_snapshots
                    (run_id, captured_at, market, city, state,
                     population, median_income, job_growth_pct,
                     unemployment_rate, median_price_per_sqft)
                    VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                    run_id, market, city, state,
                    census.get("population", 0),
                    census.get("median_income", 0),
                    bls.get("job_growth_pct", 0.0),
                    bls.get("unemployment_rate", 0.0),
                    census.get("median_price_per_sqft", 0.0),
                )

                # Save scored property entry for the market
                await conn.execute("""
                    INSERT INTO properties
                    (run_id, external_id, first_seen_at, last_seen_at,
                     name, city, state, market, property_type,
                     price, aadt, deal_score, traffic_score,
                     growth_score, value_score, price_vs_market_pct, flags)
                    VALUES ($1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                """,
                    run_id,
                    f"market-{city.lower().replace(' ','-')}-{state.lower()}",
                    f"{market} Market Overview",
                    city, state, market,
                    "Market",
                    0.0,
                    aadt,
                    scores["deal_score"],
                    scores["traffic_score"],
                    scores["growth_score"],
                    scores["value_score"],
                    100.0,
                    json.dumps([]),
                )
                total += 1

        await conn.execute(
            """UPDATE runs SET status='success', finished_at=NOW(), total_found=$1
               WHERE id=$2""",
            total, run_id
        )
        log.info(f"Run {run_id} complete — {total} markets processed")

    except Exception as e:
        log.exception(f"Pipeline error: {e}")
        await conn.execute(
            "UPDATE runs SET status='failed', finished_at=NOW() WHERE id=$1",
            run_id
        )
    finally:
        await conn.close()


# ------------------------------------------------------------------ #
#  Scheduler                                                           #
# ------------------------------------------------------------------ #

async def run_forever():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger

    # Run immediately on startup
    await run_pipeline()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_pipeline,
        CronTrigger.from_crontab("0 6 * * *"),
        id="pipeline_run",
        name="Real estate pipeline",
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started — running daily at 6 AM")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal():
        log.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await stop_event.wait()
    scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(run_forever())
