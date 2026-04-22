import asyncio
import logging
import signal
import os
import json
import re
import uuid
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scheduler")

MARKETS = [
    # Florida
    ("Miami",             "FL", "florida"),
    ("Tampa",             "FL", "florida"),
    ("Orlando",           "FL", "florida"),
    ("Jacksonville",      "FL", "florida"),
    ("Fort Lauderdale",   "FL", "florida"),
    ("Sarasota",          "FL", "florida"),
    # Texas
    ("Austin",            "TX", "texas"),
    ("Dallas",            "TX", "texas"),
    ("Houston",           "TX", "texas"),
    ("San Antonio",       "TX", "texas"),
    ("Fort Worth",        "TX", "texas"),
    # Georgia
    ("Atlanta",           "GA", "georgia"),
    ("Savannah",          "GA", "georgia"),
    # North Carolina
    ("Raleigh",           "NC", "north-carolina"),
    ("Charlotte",         "NC", "north-carolina"),
    ("Durham",            "NC", "north-carolina"),
    # South Carolina
    ("Charleston",        "SC", "south-carolina"),
    ("Greenville",        "SC", "south-carolina"),
    # Tennessee
    ("Nashville",         "TN", "tennessee"),
    ("Knoxville",         "TN", "tennessee"),
    # Arizona
    ("Phoenix",           "AZ", "arizona"),
    ("Tucson",            "AZ", "arizona"),
    # Colorado
    ("Denver",            "CO", "colorado"),
    ("Colorado Springs",  "CO", "colorado"),
    # Nevada
    ("Las Vegas",         "NV", "nevada"),
    ("Reno",              "NV", "nevada"),
    # Oklahoma
    ("Oklahoma City",     "OK", "oklahoma"),
    ("Tulsa",             "OK", "oklahoma"),
    # Indiana
    ("Indianapolis",      "IN", "indiana"),
    ("Fort Wayne",        "IN", "indiana"),
    # Virginia
    ("Richmond",          "VA", "virginia"),
    ("Virginia Beach",    "VA", "virginia"),
    # Pennsylvania
    ("Philadelphia",      "PA", "pennsylvania"),
    ("Pittsburgh",        "PA", "pennsylvania"),
    # Missouri
    ("Kansas City",       "MO", "missouri"),
    ("St. Louis",         "MO", "missouri"),
    # New York
    ("New York",          "NY", "new-york"),
    ("Buffalo",           "NY", "new-york"),
    # Iowa
    ("Des Moines",        "IA", "iowa"),
    # Kansas
    ("Wichita",           "KS", "kansas"),
    # Maine
    ("Portland",          "ME", "maine"),
    ("Lewiston",          "ME", "maine"),
    ("Bangor",            "ME", "maine"),
    ("Augusta",           "ME", "maine"),
    # New Hampshire
    ("Manchester",        "NH", "new-hampshire"),
    ("Nashua",            "NH", "new-hampshire"),
    ("Concord",           "NH", "new-hampshire"),
    ("Dover",             "NH", "new-hampshire"),
    # Connecticut
    ("Bridgeport",        "CT", "connecticut"),
    ("Hartford",          "CT", "connecticut"),
    ("New Haven",         "CT", "connecticut"),
    ("Stamford",          "CT", "connecticut"),
    # West Virginia
    ("Charleston",        "WV", "west-virginia"),
    ("Huntington",        "WV", "west-virginia"),
    ("Morgantown",        "WV", "west-virginia"),
    ("Parkersburg",       "WV", "west-virginia"),
    # Arkansas
    ("Little Rock",       "AR", "arkansas"),
    ("Fayetteville",      "AR", "arkansas"),
    ("Fort Smith",        "AR", "arkansas"),
    ("Jonesboro",         "AR", "arkansas"),
    # Kentucky
    ("Louisville",        "KY", "kentucky"),
    ("Lexington",         "KY", "kentucky"),
    ("Bowling Green",     "KY", "kentucky"),
    ("Owensboro",         "KY", "kentucky"),
]

CENSUS_KEY  = os.environ.get("CENSUS_API_KEY", "")
BLS_KEY     = os.environ.get("BLS_API_KEY", "")
DB_URL      = os.environ.get("DATABASE_URL", "")
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")


# ------------------------------------------------------------------ #
#  Market data collectors                                              #
# ------------------------------------------------------------------ #

async def fetch_census(session, city, state):
    import aiohttp
    state_fips = {
        "ME":"23","NH":"33","CT":"09","WV":"54","AR":"05","KY":"21",
        "TN":"47","TX":"48","AZ":"04","NC":"37","FL":"12","GA":"13",
        "CO":"08","ID":"16","NV":"32","IN":"18","OH":"39","IL":"17",
        "MA":"25","CA":"06","WA":"53","SC":"45","VA":"51","PA":"42",
        "MO":"29","NY":"36","IA":"19","KS":"20","OK":"40","MN":"27",
        "OR":"41","UT":"49","MD":"24","LA":"22","WI":"55","NM":"35",
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
                pop      = max(0, int(row[1] or 0))
                income   = max(0, int(row[2] or 0))
                home_val = max(0, int(row[3] or 0))
                return {
                    "population":            pop,
                    "median_income":         income,
                    "median_home_value":     home_val,
                    "median_price_per_sqft": round(home_val / 1800, 2),
                }
    except Exception as e:
        log.warning(f"Census error for {city}, {state}: {e}")
    return {}


async def fetch_bls(session, city, state):
    import aiohttp
    fallback = {
        ("Miami",             "FL"): {"job_growth_pct": 3.7, "unemployment_rate": 3.9},
        ("Tampa",             "FL"): {"job_growth_pct": 4.3, "unemployment_rate": 3.3},
        ("Orlando",           "FL"): {"job_growth_pct": 4.6, "unemployment_rate": 3.4},
        ("Jacksonville",      "FL"): {"job_growth_pct": 3.5, "unemployment_rate": 3.2},
        ("Fort Lauderdale",   "FL"): {"job_growth_pct": 3.4, "unemployment_rate": 3.8},
        ("Sarasota",          "FL"): {"job_growth_pct": 4.1, "unemployment_rate": 3.0},
        ("Austin",            "TX"): {"job_growth_pct": 5.2, "unemployment_rate": 2.9},
        ("Dallas",            "TX"): {"job_growth_pct": 3.8, "unemployment_rate": 3.6},
        ("Houston",           "TX"): {"job_growth_pct": 3.1, "unemployment_rate": 4.1},
        ("San Antonio",       "TX"): {"job_growth_pct": 3.2, "unemployment_rate": 3.8},
        ("Fort Worth",        "TX"): {"job_growth_pct": 3.9, "unemployment_rate": 3.5},
        ("Atlanta",           "GA"): {"job_growth_pct": 3.4, "unemployment_rate": 3.6},
        ("Savannah",          "GA"): {"job_growth_pct": 3.8, "unemployment_rate": 3.1},
        ("Raleigh",           "NC"): {"job_growth_pct": 4.0, "unemployment_rate": 3.1},
        ("Charlotte",         "NC"): {"job_growth_pct": 3.6, "unemployment_rate": 3.4},
        ("Durham",            "NC"): {"job_growth_pct": 3.9, "unemployment_rate": 3.0},
        ("Charleston",        "SC"): {"job_growth_pct": 4.2, "unemployment_rate": 2.9},
        ("Greenville",        "SC"): {"job_growth_pct": 3.5, "unemployment_rate": 3.2},
        ("Nashville",         "TN"): {"job_growth_pct": 4.1, "unemployment_rate": 3.2},
        ("Knoxville",         "TN"): {"job_growth_pct": 3.2, "unemployment_rate": 3.4},
        ("Phoenix",           "AZ"): {"job_growth_pct": 3.9, "unemployment_rate": 3.5},
        ("Tucson",            "AZ"): {"job_growth_pct": 2.3, "unemployment_rate": 4.0},
        ("Denver",            "CO"): {"job_growth_pct": 3.2, "unemployment_rate": 3.0},
        ("Colorado Springs",  "CO"): {"job_growth_pct": 2.8, "unemployment_rate": 3.4},
        ("Las Vegas",         "NV"): {"job_growth_pct": 3.3, "unemployment_rate": 4.6},
        ("Reno",              "NV"): {"job_growth_pct": 3.6, "unemployment_rate": 3.8},
        ("Oklahoma City",     "OK"): {"job_growth_pct": 2.4, "unemployment_rate": 3.3},
        ("Tulsa",             "OK"): {"job_growth_pct": 2.1, "unemployment_rate": 3.5},
        ("Indianapolis",      "IN"): {"job_growth_pct": 2.6, "unemployment_rate": 3.4},
        ("Fort Wayne",        "IN"): {"job_growth_pct": 2.3, "unemployment_rate": 3.1},
        ("Richmond",          "VA"): {"job_growth_pct": 2.8, "unemployment_rate": 3.2},
        ("Virginia Beach",    "VA"): {"job_growth_pct": 2.0, "unemployment_rate": 3.2},
        ("Philadelphia",      "PA"): {"job_growth_pct": 1.6, "unemployment_rate": 4.2},
        ("Pittsburgh",        "PA"): {"job_growth_pct": 1.4, "unemployment_rate": 3.9},
        ("Kansas City",       "MO"): {"job_growth_pct": 2.5, "unemployment_rate": 3.6},
        ("St. Louis",         "MO"): {"job_growth_pct": 1.5, "unemployment_rate": 3.8},
        ("New York",          "NY"): {"job_growth_pct": 1.8, "unemployment_rate": 4.8},
        ("Buffalo",           "NY"): {"job_growth_pct": 1.2, "unemployment_rate": 4.1},
        ("Des Moines",        "IA"): {"job_growth_pct": 2.7, "unemployment_rate": 2.9},
        ("Wichita",           "KS"): {"job_growth_pct": 1.9, "unemployment_rate": 3.4},
        ("Portland",          "ME"): {"job_growth_pct": 2.1, "unemployment_rate": 2.8},
        ("Lewiston",          "ME"): {"job_growth_pct": 1.4, "unemployment_rate": 3.2},
        ("Bangor",            "ME"): {"job_growth_pct": 1.2, "unemployment_rate": 3.1},
        ("Augusta",           "ME"): {"job_growth_pct": 1.0, "unemployment_rate": 3.0},
        ("Manchester",        "NH"): {"job_growth_pct": 2.4, "unemployment_rate": 2.6},
        ("Nashua",            "NH"): {"job_growth_pct": 2.2, "unemployment_rate": 2.5},
        ("Concord",           "NH"): {"job_growth_pct": 1.8, "unemployment_rate": 2.7},
        ("Dover",             "NH"): {"job_growth_pct": 2.0, "unemployment_rate": 2.6},
        ("Bridgeport",        "CT"): {"job_growth_pct": 1.6, "unemployment_rate": 4.8},
        ("Hartford",          "CT"): {"job_growth_pct": 1.4, "unemployment_rate": 4.2},
        ("New Haven",         "CT"): {"job_growth_pct": 1.5, "unemployment_rate": 4.5},
        ("Stamford",          "CT"): {"job_growth_pct": 2.0, "unemployment_rate": 3.8},
        ("Charleston",        "WV"): {"job_growth_pct": 0.8, "unemployment_rate": 4.6},
        ("Huntington",        "WV"): {"job_growth_pct": 0.6, "unemployment_rate": 4.9},
        ("Morgantown",        "WV"): {"job_growth_pct": 1.4, "unemployment_rate": 3.8},
        ("Parkersburg",       "WV"): {"job_growth_pct": 0.7, "unemployment_rate": 4.7},
        ("Little Rock",       "AR"): {"job_growth_pct": 2.2, "unemployment_rate": 3.4},
        ("Fayetteville",      "AR"): {"job_growth_pct": 3.8, "unemployment_rate": 2.9},
        ("Fort Smith",        "AR"): {"job_growth_pct": 1.5, "unemployment_rate": 3.8},
        ("Jonesboro",         "AR"): {"job_growth_pct": 2.0, "unemployment_rate": 3.5},
        ("Louisville",        "KY"): {"job_growth_pct": 2.1, "unemployment_rate": 3.8},
        ("Lexington",         "KY"): {"job_growth_pct": 2.4, "unemployment_rate": 3.5},
        ("Bowling Green",     "KY"): {"job_growth_pct": 2.8, "unemployment_rate": 3.2},
        ("Owensboro",         "KY"): {"job_growth_pct": 1.6, "unemployment_rate": 3.9},
    }

    area_codes = {
        ("Miami",             "FL"): "33100",
        ("Tampa",             "FL"): "45300",
        ("Orlando",           "FL"): "36740",
        ("Jacksonville",      "FL"): "27260",
        ("Fort Lauderdale",   "FL"): "22744",
        ("Sarasota",          "FL"): "42260",
        ("Austin",            "TX"): "12420",
        ("Dallas",            "TX"): "19100",
        ("Houston",           "TX"): "26420",
        ("San Antonio",       "TX"): "41700",
        ("Fort Worth",        "TX"): "19100",
        ("Atlanta",           "GA"): "12060",
        ("Savannah",          "GA"): "42340",
        ("Raleigh",           "NC"): "39580",
        ("Charlotte",         "NC"): "16740",
        ("Durham",            "NC"): "20500",
        ("Charleston",        "SC"): "16700",
        ("Greenville",        "SC"): "24860",
        ("Nashville",         "TN"): "34980",
        ("Knoxville",         "TN"): "28940",
        ("Phoenix",           "AZ"): "38060",
        ("Tucson",            "AZ"): "46060",
        ("Denver",            "CO"): "19740",
        ("Colorado Springs",  "CO"): "17820",
        ("Las Vegas",         "NV"): "29820",
        ("Reno",              "NV"): "39900",
        ("Oklahoma City",     "OK"): "36420",
        ("Tulsa",             "OK"): "46140",
        ("Indianapolis",      "IN"): "26900",
        ("Fort Wayne",        "IN"): "23060",
        ("Richmond",          "VA"): "40060",
        ("Virginia Beach",    "VA"): "47260",
        ("Philadelphia",      "PA"): "37980",
        ("Pittsburgh",        "PA"): "38300",
        ("Kansas City",       "MO"): "28140",
        ("St. Louis",         "MO"): "41180",
        ("New York",          "NY"): "35620",
        ("Buffalo",           "NY"): "15380",
        ("Des Moines",        "IA"): "19780",
        ("Wichita",           "KS"): "48620",
        ("Portland",          "ME"): "38860",
        ("Lewiston",          "ME"): "30340",
        ("Bangor",            "ME"): "12620",
        ("Augusta",           "ME"): "11700",
        ("Manchester",        "NH"): "31700",
        ("Nashua",            "NH"): "31700",
        ("Concord",           "NH"): "18180",
        ("Dover",             "NH"): "14460",
        ("Bridgeport",        "CT"): "14860",
        ("Hartford",          "CT"): "25540",
        ("New Haven",         "CT"): "35300",
        ("Stamford",          "CT"): "14860",
        ("Charleston",        "WV"): "16620",
        ("Huntington",        "WV"): "26580",
        ("Morgantown",        "WV"): "34060",
        ("Parkersburg",       "WV"): "37620",
        ("Little Rock",       "AR"): "30780",
        ("Fayetteville",      "AR"): "22220",
        ("Fort Smith",        "AR"): "22900",
        ("Jonesboro",         "AR"): "27860",
        ("Louisville",        "KY"): "31140",
        ("Lexington",         "KY"): "30460",
        ("Bowling Green",     "KY"): "14540",
        ("Owensboro",         "KY"): "36980",
    }

    area = area_codes.get((city, state))
    if not area:
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

    state_fips = {
        "FL":"12","TX":"48","GA":"13","NC":"37","SC":"45","TN":"47",
        "AZ":"04","CO":"08","NV":"32","OK":"40","IN":"18","VA":"51",
        "PA":"42","MO":"29","NY":"36","IA":"19","KS":"20","ME":"23",
        "NH":"33","CT":"09","WV":"54","AR":"05","KY":"21",
    }.get(state, "00")

    series_id = f"SMU{state_fips}{area}000000001"
    payload = {
        "seriesid":    [series_id],
        "startyear":   "2022",
        "endyear":     "2024",
        "annualaverage": True,
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY

    try:
        async with session.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=20),
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
            curr   = float(annual[0]["value"])
            prev   = float(annual[1]["value"])
            growth = ((curr - prev) / prev * 100) if prev else 0.0
            return {"job_growth_pct": round(growth, 2), "unemployment_rate": 0.0}
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

    except Exception as e:
        log.warning(f"BLS error for {city}, {state}: {e}")
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})


async def fetch_traffic(session, city, state):
    aadt_estimates = {
        ("Miami",             "FL"): 74000,
        ("Tampa",             "FL"): 49000,
        ("Orlando",           "FL"): 58000,
        ("Jacksonville",      "FL"): 44000,
        ("Fort Lauderdale",   "FL"): 61000,
        ("Sarasota",          "FL"): 32000,
        ("Austin",            "TX"): 61000,
        ("Dallas",            "TX"): 72000,
        ("Houston",           "TX"): 78000,
        ("San Antonio",       "TX"): 58000,
        ("Fort Worth",        "TX"): 55000,
        ("Atlanta",           "GA"): 89000,
        ("Savannah",          "GA"): 29000,
        ("Raleigh",           "NC"): 39000,
        ("Charlotte",         "NC"): 47000,
        ("Durham",            "NC"): 35000,
        ("Charleston",        "SC"): 31000,
        ("Greenville",        "SC"): 33000,
        ("Nashville",         "TN"): 52000,
        ("Knoxville",         "TN"): 34000,
        ("Phoenix",           "AZ"): 68000,
        ("Tucson",            "AZ"): 38000,
        ("Denver",            "CO"): 55000,
        ("Colorado Springs",  "CO"): 41000,
        ("Las Vegas",         "NV"): 63000,
        ("Reno",              "NV"): 36000,
        ("Oklahoma City",     "OK"): 46000,
        ("Tulsa",             "OK"): 41000,
        ("Indianapolis",      "IN"): 51000,
        ("Fort Wayne",        "IN"): 28000,
        ("Richmond",          "VA"): 44000,
        ("Virginia Beach",    "VA"): 43000,
        ("Philadelphia",      "PA"): 67000,
        ("Pittsburgh",        "PA"): 49000,
        ("Kansas City",       "MO"): 54000,
        ("St. Louis",         "MO"): 58000,
        ("New York",          "NY"): 98000,
        ("Buffalo",           "NY"): 39000,
        ("Des Moines",        "IA"): 37000,
        ("Wichita",           "KS"): 31000,
        ("Portland",          "ME"): 24000,
        ("Lewiston",          "ME"): 18000,
        ("Bangor",            "ME"): 16000,
        ("Augusta",           "ME"): 14000,
        ("Manchester",        "NH"): 22000,
        ("Nashua",            "NH"): 21000,
        ("Concord",           "NH"): 19000,
        ("Dover",             "NH"): 17000,
        ("Bridgeport",        "CT"): 38000,
        ("Hartford",          "CT"): 44000,
        ("New Haven",         "CT"): 41000,
        ("Stamford",          "CT"): 36000,
        ("Charleston",        "WV"): 22000,
        ("Huntington",        "WV"): 19000,
        ("Morgantown",        "WV"): 17000,
        ("Parkersburg",       "WV"): 15000,
        ("Little Rock",       "AR"): 31000,
        ("Fayetteville",      "AR"): 28000,
        ("Fort Smith",        "AR"): 22000,
        ("Jonesboro",         "AR"): 18000,
        ("Louisville",        "KY"): 45000,
        ("Lexington",         "KY"): 38000,
        ("Bowling Green",     "KY"): 24000,
        ("Owensboro",         "KY"): 19000,
    }
    return aadt_estimates.get((city, state), 20000)


def score_market(census, bls, traffic_aadt):
    import math
    traffic_score = (
        min(100, math.log(traffic_aadt / 1000) / math.log(100) * 100)
        if traffic_aadt > 0 else 0
    )
    pop_growth   = census.get("pop_growth_pct", 0) or 0
    job_growth   = bls.get("job_growth_pct", 0) or 0
    growth_score = (
        min(100, (pop_growth / 6) * 100) * 0.5 +
        min(100, (job_growth / 6) * 100) * 0.5
    )
    ppsf        = census.get("median_price_per_sqft", 200) or 200
    value_score = min(100, max(0, 130 - (ppsf / 200) * 100))
    deal_score  = traffic_score * 0.35 + growth_score * 0.40 + value_score * 0.25
    return {
        "traffic_score": round(traffic_score, 1),
        "growth_score":  round(growth_score, 1),
        "value_score":   round(value_score, 1),
        "deal_score":    round(deal_score, 1),
    }


# ------------------------------------------------------------------ #
#  Property listing collectors                                         #
# ------------------------------------------------------------------ #

def _normalize_type(raw):
    raw = (raw or "").lower()
    if any(x in raw for x in ["retail", "store", "shop"]):
        return "Retail"
    if any(x in raw for x in ["industrial", "warehouse", "flex", "manufacturing"]):
        return "Industrial"
    if any(x in raw for x in ["land", "lot", "acreage"]):
        return "Land"
    if any(x in raw for x in ["mixed", "multi"]):
        return "Mixed-use"
    if any(x in raw for x in ["office", "commercial"]):
        return "Commercial"
    return "Commercial"


async def fetch_realty_listings(session, city, state):
    """
    Pull commercial listings from Realty in US via RapidAPI.
    Free tier: 500 requests/month.
    """
    import aiohttp
    if not RAPIDAPI_KEY:
        log.warning("RAPIDAPI_KEY not set — skipping listings")
        return []

    url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
        "Content-Type":    "application/json",
    }
    payload = {
        "limit":      20,
        "offset":     0,
        "city":       city,
        "state_code": state,
        "status":     ["for_sale"],
        "sort": {
            "direction": "desc",
            "field":     "list_date",
        },
    }

    properties = []
    try:
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 429:
                log.warning("RapidAPI rate limit hit — skipping")
                return []
            if r.status != 200:
                log.warning(f"Realty API {r.status} for {city}, {state}")
                return []
            data = await r.json()

        results = (
            data.get("data", {}).get("home_search", {}).get("results") or
            data.get("results") or
            []
        )

        for home in results:
            desc      = home.get("description", {})
            location  = home.get("location", {})
            address   = location.get("address", {})
            listing   = home.get("list_price") or home.get("price") or 0
            prop_type = desc.get("type") or home.get("prop_type") or "Commercial"
            sqft      = desc.get("sqft") or desc.get("lot_sqft")
            name      = (
                address.get("line") or
                f"{prop_type.title()} — {city}, {state}"
            )
            full_addr = (
                f"{address.get('line', '')}, "
                f"{address.get('city', city)}, "
                f"{address.get('state_code', state)} "
                f"{address.get('postal_code', '')}"
            ).strip(", ")
            prop_id     = home.get("property_id") or home.get("listing_id") or str(uuid.uuid4())[:8]
            listing_url = f"https://www.realtor.com/realestateandhomes-detail/{prop_id}"

            if not listing or float(listing) < 50_000:
                continue

            properties.append({
                "name":          name,
                "price":         float(listing),
                "city":          city,
                "state":         state,
                "property_type": _normalize_type(prop_type),
                "source":        "realtor",
                "listing_url":   listing_url,
                "address":       full_addr,
                "sqft":          sqft,
                "aadt":          None,
            })

        log.info(f"Realty API: {len(properties)} listings for {city}, {state}")
    except Exception as e:
        log.warning(f"Realty API error for {city}, {state}: {e}")
    return properties


async def fetch_realty_land(session, city, state):
    """
    Second RapidAPI call filtered specifically for land and commercial types.
    """
    import aiohttp
    if not RAPIDAPI_KEY:
        return []

    url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
        "Content-Type":    "application/json",
    }
    payload = {
        "limit":      20,
        "offset":     0,
        "city":       city,
        "state_code": state,
        "status":     ["for_sale"],
        "prop_type":  ["land", "commercial"],
        "sort": {
            "direction": "desc",
            "field":     "list_date",
        },
    }

    properties = []
    try:
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status == 429:
                log.warning("RapidAPI rate limit hit")
                return []
            if r.status != 200:
                return []
            data = await r.json()

        results = (
            data.get("data", {}).get("home_search", {}).get("results") or
            data.get("results") or
            []
        )

        for home in results:
            desc      = home.get("description", {})
            location  = home.get("location", {})
            address   = location.get("address", {})
            listing   = home.get("list_price") or home.get("price") or 0
            prop_type = desc.get("type") or "Land"
            sqft      = desc.get("sqft") or desc.get("lot_sqft")
            name      = address.get("line") or f"Land Parcel — {city}, {state}"
            full_addr = (
                f"{address.get('line', '')}, "
                f"{address.get('city', city)}, "
                f"{address.get('state_code', state)}"
            ).strip(", ")
            prop_id     = home.get("property_id") or str(uuid.uuid4())[:8]
            listing_url = f"https://www.realtor.com/realestateandhomes-detail/{prop_id}"

            if not listing or float(listing) < 50_000:
                continue

            properties.append({
                "name":          name,
                "price":         float(listing),
                "city":          city,
                "state":         state,
                "property_type": _normalize_type(prop_type),
                "source":        "realtor-land",
                "listing_url":   listing_url,
                "address":       full_addr,
                "sqft":          sqft,
                "aadt":          None,
            })

        log.info(f"Realty Land API: {len(properties)} listings for {city}, {state}")
    except Exception as e:
        log.warning(f"Realty Land API error for {city}, {state}: {e}")
    return properties


def score_property(p, market_scores, census):
    import math
    aadt          = p.get("aadt") or market_scores.get("aadt", 20000)
    traffic_score = (
        min(100, math.log(aadt / 1000) / math.log(100) * 100)
        if aadt > 0 else 0
    )
    growth_score  = market_scores.get("growth_score", 30)
    price         = p.get("price", 0) or 0
    sqft          = p.get("sqft") or 5000
    ppsf          = (price / sqft) if sqft and price else 0
    market_ppsf   = census.get("median_price_per_sqft", 200) or 200
    if ppsf > 0 and market_ppsf > 0:
        ratio       = (ppsf / market_ppsf) * 100
        value_score = min(100, max(0, 130 - ratio))
    else:
        value_score = market_scores.get("value_score", 30)
    deal_score      = traffic_score * 0.35 + growth_score * 0.40 + value_score * 0.25
    price_vs_market = round((ppsf / market_ppsf * 100), 1) if ppsf and market_ppsf else 100.0
    return {
        "traffic_score":       round(traffic_score, 1),
        "growth_score":        round(growth_score, 1),
        "value_score":         round(value_score, 1),
        "deal_score":          round(deal_score, 1),
        "price_vs_market_pct": price_vs_market,
        "aadt":                aadt,
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

    conn        = await asyncpg.connect(DB_URL, statement_cache_size=0)
    market_list = [f"{city}, {state}" for city, state, _ in MARKETS]

    run_id = await conn.fetchval(
        """INSERT INTO runs (started_at, status, markets, total_found)
           VALUES (NOW(), 'running', $1, 0) RETURNING id""",
        json.dumps(market_list),
    )
    log.info(f"Run {run_id} started for {len(MARKETS)} markets")

    total = 0
    try:
        async with aiohttp.ClientSession() as session:
            for city, state, state_slug in MARKETS:
                market = f"{city}, {state}"
                log.info(f"Processing {market}...")

                # Collect market-level data
                census, bls, aadt = await asyncio.gather(
                    fetch_census(session, city, state),
                    fetch_bls(session, city, state),
                    fetch_traffic(session, city, state),
                )
                market_scores         = score_market(census, bls, aadt)
                market_scores["aadt"] = aadt

                log.info(
                    f"{market} — score: {market_scores['deal_score']} "
                    f"traffic: {aadt} "
                    f"jobs: {bls.get('job_growth_pct', 0):.1f}%"
                )

                # Save market snapshot
                await conn.execute(
                    """INSERT INTO market_snapshots
                       (run_id, captured_at, market, city, state,
                        population, median_income, job_growth_pct,
                        unemployment_rate, median_price_per_sqft)
                       VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8, $9)""",
                    run_id, market, city, state,
                    census.get("population", 0),
                    census.get("median_income", 0),
                    bls.get("job_growth_pct", 0.0),
                    bls.get("unemployment_rate", 0.0),
                    census.get("median_price_per_sqft", 0.0),
                )

                # Save market overview entry
                await conn.execute(
                    """INSERT INTO properties
                       (run_id, external_id, first_seen_at, last_seen_at,
                        name, city, state, market, property_type,
                        price, aadt, deal_score, traffic_score,
                        growth_score, value_score, price_vs_market_pct, flags)
                       VALUES ($1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,$9,
                               $10,$11,$12,$13,$14,$15)""",
                    run_id,
                    f"market-{city.lower().replace(' ', '-').replace('.', '')}-{state.lower()}",
                    f"{market} Market Overview",
                    city, state, market, "Market",
                    0.0, aadt,
                    market_scores["deal_score"],
                    market_scores["traffic_score"],
                    market_scores["growth_score"],
                    market_scores["value_score"],
                    100.0,
                    json.dumps([]),
                )
                total += 1

                # Fetch real property listings
                realty_props, land_props = await asyncio.gather(
                    fetch_realty_listings(session, city, state),
                    fetch_realty_land(session, city, state),
                )

                # Deduplicate by address + price
                seen      = set()
                all_props = []
                for p in realty_props + land_props:
                    key = p.get("address", "") + str(p.get("price", ""))
                    if key not in seen:
                        seen.add(key)
                        all_props.append(p)

                log.info(f"{market} — {len(all_props)} property listings found")

                for p in all_props:
                    if not p.get("price") or p["price"] < 50_000:
                        continue

                    scores = score_property(p, market_scores, census)
                    ext_id = (
                        f"{p['source']}-"
                        f"{city.lower().replace(' ', '-').replace('.', '')}-"
                        f"{abs(hash(p.get('listing_url', '') + p.get('name', ''))) % 999999}"
                    )

                    await conn.execute(
                        """INSERT INTO properties
                           (run_id, external_id, first_seen_at, last_seen_at,
                            name, address, city, state, market, property_type,
                            price, sqft, aadt, source, listing_url,
                            deal_score, traffic_score, growth_score,
                            value_score, price_vs_market_pct, flags)
                           VALUES ($1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,
                                   $9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)""",
                        run_id, ext_id,
                        p.get("name", "Unknown"),
                        p.get("address", f"{city}, {state}"),
                        city, state, market,
                        p.get("property_type", "Commercial"),
                        p.get("price", 0),
                        p.get("sqft"),
                        scores["aadt"],
                        p.get("source", "unknown"),
                        p.get("listing_url", ""),
                        scores["deal_score"],
                        scores["traffic_score"],
                        scores["growth_score"],
                        scores["value_score"],
                        scores["price_vs_market_pct"],
                        json.dumps([]),
                    )
                    total += 1

        await conn.execute(
            "UPDATE runs SET status='success', finished_at=NOW(), total_found=$1 WHERE id=$2",
            total, run_id,
        )
        log.info(f"Run {run_id} complete — {total} total entries saved")

    except Exception as e:
        log.exception(f"Pipeline error: {e}")
        await conn.execute(
            "UPDATE runs SET status='failed', finished_at=NOW() WHERE id=$1",
            run_id,
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
        CronTrigger.from_crontab("0 6 * * 1"),  # every Monday at 6 AM
        id="pipeline_run",
        name="Real estate pipeline",
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started — running every Monday at 6 AM")

    stop_event = asyncio.Event()
    loop       = asyncio.get_running_loop()

    def _handle_signal():
        log.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await stop_event.wait()
    scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(run_forever())
