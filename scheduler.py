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

CENSUS_KEY = os.environ.get("CENSUS_API_KEY", "")
BLS_KEY    = os.environ.get("BLS_API_KEY", "")
DB_URL     = os.environ.get("DATABASE_URL", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ------------------------------------------------------------------ #
#  Market data collectors                                              #
# ------------------------------------------------------------------ #

async def fetch_census(session, city, state):
    import aiohttp
    state_fips = {
        "ME":"23","NH":"33","CT":"09","WV":"54","AR":"05","KY":"21",
        "TN":"47","TX":"48","AZ":"04","NC":"37","FL":"12",
        "GA":"13","CO":"08","ID":"16","NV":"32","IN":"18",
        "OH":"39","IL":"17","MA":"25","CA":"06","WA":"53",
        "SC":"45","VA":"51","PA":"42","MO":"29","NY":"36",
        "IA":"19","KS":"20","OK":"40","MN":"27","OR":"41",
        "UT":"49","MD":"24","LA":"22","WI":"55","NM":"35",
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
                    "population": pop,
                    "median_income": income,
                    "median_home_value": home_val,
                    "median_price_per_sqft": round(home_val / 1800, 2),
                }
    except Exception as e:
        log.warning(f"Census error for {city}, {state}: {e}")
    return {}


async def fetch_bls(session, city, state):
    import aiohttp
    fallback = {
        # Florida
        ("Miami",             "FL"): {"job_growth_pct": 3.7, "unemployment_rate": 3.9},
        ("Tampa",             "FL"): {"job_growth_pct": 4.3, "unemployment_rate": 3.3},
        ("Orlando",           "FL"): {"job_growth_pct": 4.6, "unemployment_rate": 3.4},
        ("Jacksonville",      "FL"): {"job_growth_pct": 3.5, "unemployment_rate": 3.2},
        ("Fort Lauderdale",   "FL"): {"job_growth_pct": 3.4, "unemployment_rate": 3.8},
        ("Sarasota",          "FL"): {"job_growth_pct": 4.1, "unemployment_rate": 3.0},
        # Texas
        ("Austin",            "TX"): {"job_growth_pct": 5.2, "unemployment_rate": 2.9},
        ("Dallas",            "TX"): {"job_growth_pct": 3.8, "unemployment_rate": 3.6},
        ("Houston",           "TX"): {"job_growth_pct": 3.1, "unemployment_rate": 4.1},
        ("San Antonio",       "TX"): {"job_growth_pct": 3.2, "unemployment_rate": 3.8},
        ("Fort Worth",        "TX"): {"job_growth_pct": 3.9, "unemployment_rate": 3.5},
        # Georgia
        ("Atlanta",           "GA"): {"job_growth_pct": 3.4, "unemployment_rate": 3.6},
        ("Savannah",          "GA"): {"job_growth_pct": 3.8, "unemployment_rate": 3.1},
        # North Carolina
        ("Raleigh",           "NC"): {"job_growth_pct": 4.0, "unemployment_rate": 3.1},
        ("Charlotte",         "NC"): {"job_growth_pct": 3.6, "unemployment_rate": 3.4},
        ("Durham",            "NC"): {"job_growth_pct": 3.9, "unemployment_rate": 3.0},
        # South Carolina
        ("Charleston",        "SC"): {"job_growth_pct": 4.2, "unemployment_rate": 2.9},
        ("Greenville",        "SC"): {"job_growth_pct": 3.5, "unemployment_rate": 3.2},
        # Tennessee
        ("Nashville",         "TN"): {"job_growth_pct": 4.1, "unemployment_rate": 3.2},
        ("Knoxville",         "TN"): {"job_growth_pct": 3.2, "unemployment_rate": 3.4},
        # Arizona
        ("Phoenix",           "AZ"): {"job_growth_pct": 3.9, "unemployment_rate": 3.5},
        ("Tucson",            "AZ"): {"job_growth_pct": 2.3, "unemployment_rate": 4.0},
        # Colorado
        ("Denver",            "CO"): {"job_growth_pct": 3.2, "unemployment_rate": 3.0},
        ("Colorado Springs",  "CO"): {"job_growth_pct": 2.8, "unemployment_rate": 3.4},
        # Nevada
        ("Las Vegas",         "NV"): {"job_growth_pct": 3.3, "unemployment_rate": 4.6},
        ("Reno",              "NV"): {"job_growth_pct": 3.6, "unemployment_rate": 3.8},
        # Oklahoma
        ("Oklahoma City",     "OK"): {"job_growth_pct": 2.4, "unemployment_rate": 3.3},
        ("Tulsa",             "OK"): {"job_growth_pct": 2.1, "unemployment_rate": 3.5},
        # Indiana
        ("Indianapolis",      "IN"): {"job_growth_pct": 2.6, "unemployment_rate": 3.4},
        ("Fort Wayne",        "IN"): {"job_growth_pct": 2.3, "unemployment_rate": 3.1},
        # Virginia
        ("Richmond",          "VA"): {"job_growth_pct": 2.8, "unemployment_rate": 3.2},
        ("Virginia Beach",    "VA"): {"job_growth_pct": 2.0, "unemployment_rate": 3.2},
        # Pennsylvania
        ("Philadelphia",      "PA"): {"job_growth_pct": 1.6, "unemployment_rate": 4.2},
        ("Pittsburgh",        "PA"): {"job_growth_pct": 1.4, "unemployment_rate": 3.9},
        # Missouri
        ("Kansas City",       "MO"): {"job_growth_pct": 2.5, "unemployment_rate": 3.6},
        ("St. Louis",         "MO"): {"job_growth_pct": 1.5, "unemployment_rate": 3.8},
        # New York
        ("New York",          "NY"): {"job_growth_pct": 1.8, "unemployment_rate": 4.8},
        ("Buffalo",           "NY"): {"job_growth_pct": 1.2, "unemployment_rate": 4.1},
        # Iowa
        ("Des Moines",        "IA"): {"job_growth_pct": 2.7, "unemployment_rate": 2.9},
        # Kansas
        ("Wichita",           "KS"): {"job_growth_pct": 1.9, "unemployment_rate": 3.4},
        # Maine
        ("Portland",          "ME"): {"job_growth_pct": 2.1, "unemployment_rate": 2.8},
        ("Lewiston",          "ME"): {"job_growth_pct": 1.4, "unemployment_rate": 3.2},
        ("Bangor",            "ME"): {"job_growth_pct": 1.2, "unemployment_rate": 3.1},
        ("Augusta",           "ME"): {"job_growth_pct": 1.0, "unemployment_rate": 3.0},
        # New Hampshire
        ("Manchester",        "NH"): {"job_growth_pct": 2.4, "unemployment_rate": 2.6},
        ("Nashua",            "NH"): {"job_growth_pct": 2.2, "unemployment_rate": 2.5},
        ("Concord",           "NH"): {"job_growth_pct": 1.8, "unemployment_rate": 2.7},
        ("Dover",             "NH"): {"job_growth_pct": 2.0, "unemployment_rate": 2.6},
        # Connecticut
        ("Bridgeport",        "CT"): {"job_growth_pct": 1.6, "unemployment_rate": 4.8},
        ("Hartford",          "CT"): {"job_growth_pct": 1.4, "unemployment_rate": 4.2},
        ("New Haven",         "CT"): {"job_growth_pct": 1.5, "unemployment_rate": 4.5},
        ("Stamford",          "CT"): {"job_growth_pct": 2.0, "unemployment_rate": 3.8},
        # West Virginia
        ("Charleston",        "WV"): {"job_growth_pct": 0.8, "unemployment_rate": 4.6},
        ("Huntington",        "WV"): {"job_growth_pct": 0.6, "unemployment_rate": 4.9},
        ("Morgantown",        "WV"): {"job_growth_pct": 1.4, "unemployment_rate": 3.8},
        ("Parkersburg",       "WV"): {"job_growth_pct": 0.7, "unemployment_rate": 4.7},
        # Arkansas
        ("Little Rock",       "AR"): {"job_growth_pct": 2.2, "unemployment_rate": 3.4},
        ("Fayetteville",      "AR"): {"job_growth_pct": 3.8, "unemployment_rate": 2.9},
        ("Fort Smith",        "AR"): {"job_growth_pct": 1.5, "unemployment_rate": 3.8},
        ("Jonesboro",         "AR"): {"job_growth_pct": 2.0, "unemployment_rate": 3.5},
        # Kentucky
        ("Louisville",        "KY"): {"job_growth_pct": 2.1, "unemployment_rate": 3.8},
        ("Lexington",         "KY"): {"job_growth_pct": 2.4, "unemployment_rate": 3.5},
        ("Bowling Green",     "KY"): {"job_growth_pct": 2.8, "unemployment_rate": 3.2},
        ("Owensboro",         "KY"): {"job_growth_pct": 1.6, "unemployment_rate": 3.9},
    }

    area_codes = {
        # Florida
        ("Miami",             "FL"): "33100",
        ("Tampa",             "FL"): "45300",
        ("Orlando",           "FL"): "36740",
        ("Jacksonville",      "FL"): "27260",
        ("Fort Lauderdale",   "FL"): "22744",
        ("Sarasota",          "FL"): "42260",
        # Texas
        ("Austin",            "TX"): "12420",
        ("Dallas",            "TX"): "19100",
        ("Houston",           "TX"): "26420",
        ("San Antonio",       "TX"): "41700",
        ("Fort Worth",        "TX"): "19100",
        # Georgia
        ("Atlanta",           "GA"): "12060",
        ("Savannah",          "GA"): "42340",
        # North Carolina
        ("Raleigh",           "NC"): "39580",
        ("Charlotte",         "NC"): "16740",
        ("Durham",            "NC"): "20500",
        # South Carolina
        ("Charleston",        "SC"): "16700",
        ("Greenville",        "SC"): "24860",
        # Tennessee
        ("Nashville",         "TN"): "34980",
        ("Knoxville",         "TN"): "28940",
        # Arizona
        ("Phoenix",           "AZ"): "38060",
        ("Tucson",            "AZ"): "46060",
        # Colorado
        ("Denver",            "CO"): "19740",
        ("Colorado Springs",  "CO"): "17820",
        # Nevada
        ("Las Vegas",         "NV"): "29820",
        ("Reno",              "NV"): "39900",
        # Oklahoma
        ("Oklahoma City",     "OK"): "36420",
        ("Tulsa",             "OK"): "46140",
        # Indiana
        ("Indianapolis",      "IN"): "26900",
        ("Fort Wayne",        "IN"): "23060",
        # Virginia
        ("Richmond",          "VA"): "40060",
        ("Virginia Beach",    "VA"): "47260",
        # Pennsylvania
        ("Philadelphia",      "PA"): "37980",
        ("Pittsburgh",        "PA"): "38300",
        # Missouri
        ("Kansas City",       "MO"): "28140",
        ("St. Louis",         "MO"): "41180",
        # New York
        ("New York",          "NY"): "35620",
        ("Buffalo",           "NY"): "15380",
        # Iowa
        ("Des Moines",        "IA"): "19780",
        # Kansas
        ("Wichita",           "KS"): "48620",
        # Maine
        ("Portland",          "ME"): "38860",
        ("Lewiston",          "ME"): "30340",
        ("Bangor",            "ME"): "12620",
        ("Augusta",           "ME"): "11700",
        # New Hampshire
        ("Manchester",        "NH"): "31700",
        ("Nashua",            "NH"): "31700",
        ("Concord",           "NH"): "18180",
        ("Dover",             "NH"): "14460",
        # Connecticut
        ("Bridgeport",        "CT"): "14860",
        ("Hartford",          "CT"): "25540",
        ("New Haven",         "CT"): "35300",
        ("Stamford",          "CT"): "14860",
        # West Virginia
        ("Charleston",        "WV"): "16620",
        ("Huntington",        "WV"): "26580",
        ("Morgantown",        "WV"): "34060",
        ("Parkersburg",       "WV"): "37620",
        # Arkansas
        ("Little Rock",       "AR"): "30780",
        ("Fayetteville",      "AR"): "22220",
        ("Fort Smith",        "AR"): "22900",
        ("Jonesboro",         "AR"): "27860",
        # Kentucky
        ("Louisville",        "KY"): "31140",
        ("Lexington",         "KY"): "30460",
        ("Bowling Green",     "KY"): "14540",
        ("Owensboro",         "KY"): "36980",
    }

    area = area_codes.get((city, state))
    if not area:
        return fallback.get((city, state), {"job_growth_pct": 0.0, "unemployment_rate": 0.0})

    state_fips = {
        "FL":"12","TX":"48","GA":"13","NC":"37","SC":"45",
        "TN":"47","AZ":"04","CO":"08","NV":"32","OK":"40",
        "IN":"18","VA":"51","PA":"42","MO":"29","NY":"36",
        "IA":"19","KS":"20","ME":"23","NH":"33","CT":"09",
        "WV":"54","AR":"05","KY":"21",
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
        # Florida
        ("Miami",             "FL"): 74000,
        ("Tampa",             "FL"): 49000,
        ("Orlando",           "FL"): 58000,
        ("Jacksonville",      "FL"): 44000,
        ("Fort Lauderdale",   "FL"): 61000,
        ("Sarasota",          "FL"): 32000,
        # Texas
        ("Austin",            "TX"): 61000,
        ("Dallas",            "TX"): 72000,
        ("Houston",           "TX"): 78000,
        ("San Antonio",       "TX"): 58000,
        ("Fort Worth",        "TX"): 55000,
        # Georgia
        ("Atlanta",           "GA"): 89000,
        ("Savannah",          "GA"): 29000,
        # North Carolina
        ("Raleigh",           "NC"): 39000,
        ("Charlotte",         "NC"): 47000,
        ("Durham",            "NC"): 35000,
        # South Carolina
        ("Charleston",        "SC"): 31000,
        ("Greenville",        "SC"): 33000,
        # Tennessee
        ("Nashville",         "TN"): 52000,
        ("Knoxville",         "TN"): 34000,
        # Arizona
        ("Phoenix",           "AZ"): 68000,
        ("Tucson",            "AZ"): 38000,
        # Colorado
        ("Denver",            "CO"): 55000,
        ("Colorado Springs",  "CO"): 41000,
        # Nevada
        ("Las Vegas",         "NV"): 63000,
        ("Reno",              "NV"): 36000,
        # Oklahoma
        ("Oklahoma City",     "OK"): 46000,
        ("Tulsa",             "OK"): 41000,
        # Indiana
        ("Indianapolis",      "IN"): 51000,
        ("Fort Wayne",        "IN"): 28000,
        # Virginia
        ("Richmond",          "VA"): 44000,
        ("Virginia Beach",    "VA"): 43000,
        # Pennsylvania
        ("Philadelphia",      "PA"): 67000,
        ("Pittsburgh",        "PA"): 49000,
        # Missouri
        ("Kansas City",       "MO"): 54000,
        ("St. Louis",         "MO"): 58000,
        # New York
        ("New York",          "NY"): 98000,
        ("Buffalo",           "NY"): 39000,
        # Iowa
        ("Des Moines",        "IA"): 37000,
        # Kansas
        ("Wichita",           "KS"): 31000,
        # Maine
        ("Portland",          "ME"): 24000,
        ("Lewiston",          "ME"): 18000,
        ("Bangor",            "ME"): 16000,
        ("Augusta",           "ME"): 14000,
        # New Hampshire
        ("Manchester",        "NH"): 22000,
        ("Nashua",            "NH"): 21000,
        ("Concord",           "NH"): 19000,
        ("Dover",             "NH"): 17000,
        # Connecticut
        ("Bridgeport",        "CT"): 38000,
        ("Hartford",          "CT"): 44000,
        ("New Haven",         "CT"): 41000,
        ("Stamford",          "CT"): 36000,
        # West Virginia
        ("Charleston",        "WV"): 22000,
        ("Huntington",        "WV"): 19000,
        ("Morgantown",        "WV"): 17000,
        ("Parkersburg",       "WV"): 15000,
        # Arkansas
        ("Little Rock",       "AR"): 31000,
        ("Fayetteville",      "AR"): 28000,
        ("Fort Smith",        "AR"): 22000,
        ("Jonesboro",         "AR"): 18000,
        # Kentucky
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

async def fetch_landandfarm(session, city, state, state_slug):
    import aiohttp
    url = f"https://www.landandfarm.com/search/{state_slug}/commercial-land-for-sale/"
    properties = []
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
        ) as r:
            if r.status != 200:
                log.warning(f"LandAndFarm {r.status} for {city}, {state}")
                return []
            html = await r.text()

        price_pattern = re.compile(r'\$[\d,]+')

        json_ld = re.findall(
            r'<script type="application/ld\+json">(.*?)</script>',
            html, re.DOTALL
        )
        for block in json_ld:
            try:
                data  = json.loads(block)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("RealEstateListing", "Product", "Offer"):
                        name  = item.get("name", "")
                        price = item.get("price") or item.get("offers", {}).get("price", 0)
                        addr  = item.get("address", {})
                        city_n = (
                            addr.get("addressLocality", city)
                            if isinstance(addr, dict) else city
                        )
                        if name and city_n.lower() == city.lower():
                            properties.append({
                                "name":          name,
                                "price":         float(str(price).replace(",", "").replace("$", "")) if price else 0,
                                "city":          city,
                                "state":         state,
                                "property_type": "Land",
                                "source":        "landandfarm",
                                "listing_url":   item.get("url", url),
                                "address":       (
                                    str(addr) if isinstance(addr, str)
                                    else f"{addr.get('streetAddress','')}, {city}, {state}"
                                ),
                                "sqft": None, "aadt": None,
                            })
            except Exception:
                pass

        if not properties:
            cards = re.findall(
                r'href="(/land/[^"]+)".*?<[^>]+class="[^"]*price[^"]*"[^>]*>(.*?)<',
                html, re.DOTALL
            )
            for path, price_raw in cards[:20]:
                price_match = price_pattern.search(price_raw)
                if not price_match:
                    continue
                try:
                    price = float(price_match.group().replace("$", "").replace(",", ""))
                except ValueError:
                    continue
                if price < 50_000:
                    continue
                properties.append({
                    "name":          f"Commercial Land — {city}, {state}",
                    "price":         price,
                    "city":          city,
                    "state":         state,
                    "property_type": "Land",
                    "source":        "landandfarm",
                    "listing_url":   f"https://www.landandfarm.com{path}",
                    "address":       f"{city}, {state}",
                    "sqft": None, "aadt": None,
                })

        log.info(f"LandAndFarm: {len(properties)} listings for {city}, {state}")
    except Exception as e:
        log.warning(f"LandAndFarm error for {city}, {state}: {e}")
    return properties


async def fetch_landwatch(session, city, state, state_slug):
    import aiohttp
    url = f"https://www.landwatch.com/{state_slug}/commercial-land-for-sale"
    properties = []
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
        ) as r:
            if r.status != 200:
                log.warning(f"LandWatch {r.status} for {city}, {state}")
                return []
            html = await r.text()

        state_match = re.search(
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});\s*</script>',
            html, re.DOTALL
        )
        if state_match:
            try:
                data = json.loads(state_match.group(1))
                listings_data = (
                    data.get("listings", {}).get("listings") or
                    data.get("search", {}).get("results") or []
                )
                for listing in listings_data[:20]:
                    price    = listing.get("price") or listing.get("listPrice") or 0
                    name     = listing.get("title") or listing.get("propertyName") or f"Commercial Land — {city}, {state}"
                    lcity    = listing.get("city") or listing.get("address", {}).get("city", "")
                    if lcity and lcity.lower() != city.lower():
                        continue
                    lurl = listing.get("url") or listing.get("detailUrl") or url
                    if not lurl.startswith("http"):
                        lurl = f"https://www.landwatch.com{lurl}"
                    properties.append({
                        "name":          name,
                        "price":         float(price) if price else 0,
                        "city":          city,
                        "state":         state,
                        "property_type": "Land",
                        "source":        "landwatch",
                        "listing_url":   lurl,
                        "address":       f"{lcity or city}, {state}",
                        "sqft":          listing.get("buildingSize") or listing.get("sqft"),
                        "aadt":          None,
                    })
            except json.JSONDecodeError:
                pass

        if not properties:
            json_ld = re.findall(
                r'<script type="application/ld\+json">(.*?)</script>',
                html, re.DOTALL
            )
            for block in json_ld:
                try:
                    data = json.loads(block)
                    if isinstance(data, dict) and data.get("@type") == "ItemList":
                        for item in data.get("itemListElement", [])[:20]:
                            thing      = item.get("item", {})
                            name       = thing.get("name", f"Commercial Land — {city}, {state}")
                            price_info = thing.get("offers", {})
                            price      = price_info.get("price", 0) if isinstance(price_info, dict) else 0
                            properties.append({
                                "name":          name,
                                "price":         float(price) if price else 0,
                                "city":          city,
                                "state":         state,
                                "property_type": "Land",
                                "source":        "landwatch",
                                "listing_url":   thing.get("url", url),
                                "address":       f"{city}, {state}",
                                "sqft": None, "aadt": None,
                            })
                except Exception:
                    pass

        log.info(f"LandWatch: {len(properties)} listings for {city}, {state}")
    except Exception as e:
        log.warning(f"LandWatch error for {city}, {state}: {e}")
    return properties


async def fetch_myelisting(session, city, state):
    import aiohttp
    url = (
        f"https://myelisting.com/commercial-real-estate-for-sale"
        f"/{state.lower()}/{city.lower().replace(' ', '-')}"
    )
    properties = []
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
        ) as r:
            if r.status not in (200, 301, 302):
                log.warning(f"MyEListing {r.status} for {city}, {state}")
                return []
            html = await r.text()

        json_matches = re.findall(r'({[^{}]*"price"[^{}]*"address"[^{}]*})', html)
        for match in json_matches[:20]:
            try:
                data    = json.loads(match)
                price   = data.get("price", 0)
                address = data.get("address", f"{city}, {state}")
                name    = data.get("title") or data.get("name") or f"Commercial — {address}"
                prop_id = data.get("id") or str(uuid.uuid4())[:8]
                properties.append({
                    "name":          name,
                    "price":         float(str(price).replace(",", "").replace("$", "")) if price else 0,
                    "city":          city,
                    "state":         state,
                    "property_type": _myelisting_type(data.get("type", "")),
                    "source":        "myelisting",
                    "listing_url":   f"https://myelisting.com/listing/{prop_id}",
                    "address":       address,
                    "sqft":          data.get("sqft") or data.get("size"),
                    "aadt":          None,
                })
            except Exception:
                pass

        if not properties:
            json_ld = re.findall(
                r'<script type="application/ld\+json">(.*?)</script>',
                html, re.DOTALL
            )
            for block in json_ld:
                try:
                    data  = json.loads(block)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if item.get("@type") in ("RealEstateListing", "Product"):
                            name  = item.get("name", f"Commercial — {city}, {state}")
                            price = item.get("price") or 0
                            addr  = item.get("address", {})
                            properties.append({
                                "name":          name,
                                "price":         float(str(price).replace(",", "")) if price else 0,
                                "city":          city,
                                "state":         state,
                                "property_type": "Commercial",
                                "source":        "myelisting",
                                "listing_url":   item.get("url", url),
                                "address":       (
                                    f"{addr.get('streetAddress','')}, {city}, {state}"
                                    if isinstance(addr, dict) else f"{city}, {state}"
                                ),
                                "sqft": None, "aadt": None,
                            })
                except Exception:
                    pass

        log.info(f"MyEListing: {len(properties)} listings for {city}, {state}")
    except Exception as e:
        log.warning(f"MyEListing error for {city}, {state}: {e}")
    return properties


def _myelisting_type(raw):
    mapping = {
        "office":     "Commercial",
        "retail":     "Retail",
        "industrial": "Industrial",
        "warehouse":  "Industrial",
        "land":       "Land",
        "flex":       "Industrial",
        "mixed":      "Mixed-use",
    }
    for key, val in mapping.items():
        if key in raw.lower():
            return val
    return "Commercial"


def score_property(p, market_scores, census):
    import math
    aadt         = p.get("aadt") or market_scores.get("aadt", 20000)
    traffic_score = min(100, math.log(aadt / 1000) / math.log(100) * 100) if aadt > 0 else 0
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
    deal_score       = traffic_score * 0.35 + growth_score * 0.40 + value_score * 0.25
    price_vs_market  = round((ppsf / market_ppsf * 100), 1) if ppsf and market_ppsf else 100.0
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
        json.dumps(market_list)
    )
    log.info(f"Run {run_id} started for {len(MARKETS)} markets")

    total = 0
    try:
        async with aiohttp.ClientSession() as session:
            for city, state, state_slug in MARKETS:
                market = f"{city}, {state}"
                log.info(f"Processing {market}...")

                census, bls, aadt = await asyncio.gather(
                    fetch_census(session, city, state),
                    fetch_bls(session, city, state),
                    fetch_traffic(session, city, state),
                )
                market_scores        = score_market(census, bls, aadt)
                market_scores["aadt"] = aadt

                log.info(
                    f"{market} — score: {market_scores['deal_score']} "
                    f"traffic: {aadt} "
                    f"jobs: {bls.get('job_growth_pct', 0):.1f}%"
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

                # Save market overview property entry
                await conn.execute("""
                    INSERT INTO properties
                    (run_id, external_id, first_seen_at, last_seen_at,
                     name, city, state, market, property_type,
                     price, aadt, deal_score, traffic_score,
                     growth_score, value_score, price_vs_market_pct, flags)
                    VALUES ($1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                """,
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

                # Fetch real listings from all sources
                laf_props, lw_props, mye_props = await asyncio.gather(
                    fetch_landandfarm(session, city, state, state_slug),
                    fetch_landwatch(session, city, state, state_slug),
                    fetch_myelisting(session, city, state),
                )

                all_props = laf_props + lw_props + mye_props
                log.info(f"{market} — {len(all_props)} property listings found")

                for p in all_props:
                    if not p.get("price") or p["price"] < 50_000:
                        continue

                    scores = score_property(p, market_scores, census)
                    ext_id = (
                        f"{p['source']}-{city.lower().replace(' ', '-')}-"
                        f"{abs(hash(p.get('listing_url', '') + p.get('name', ''))) % 999999}"
                    )

                    await conn.execute("""
                        INSERT INTO properties
                        (run_id, external_id, first_seen_at, last_seen_at,
                         name, address, city, state, market, property_type,
                         price, sqft, aadt, source, listing_url,
                         deal_score, traffic_score, growth_score,
                         value_score, price_vs_market_pct, flags)
                        VALUES ($1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,
                                $9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                    """,
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
            total, run_id
        )
        log.info(f"Run {run_id} complete — {total} total entries saved")

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
