import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SEOUL_TZ = ZoneInfo(os.getenv("SEOUL_TIMEZONE", "Asia/Seoul"))
RKSI_LAT = float(os.getenv("RKSI_LAT", "37.469"))
RKSI_LON = float(os.getenv("RKSI_LON", "126.451"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "30"))
MARKET_SLUG_PREFIX = os.getenv(
    "MARKET_SLUG_PREFIX", "highest-temperature-in-seoul-on"
)

CLOB_HOST = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = "https://gamma-api.polymarket.com"
DATA_HOST = "https://data-api.polymarket.com"

POLY_USER_ADDRESS = os.getenv("POLYMARKET_USER_ADDRESS", "").strip()
POLY_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
POLY_API_KEY = os.getenv("POLYMARKET_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
POLY_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
POLY_SIGNATURE_TYPE = os.getenv("POLYMARKET_SIGNATURE_TYPE", "").strip()
POLY_FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_http_client: Optional[httpx.AsyncClient] = None
_cache: Dict[str, Dict[str, Any]] = {}


@app.on_event("startup")
async def on_startup() -> None:
    global _http_client
    _http_client = httpx.AsyncClient(timeout=15)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if _http_client:
        await _http_client.aclose()


def _now_kst() -> datetime:
    return datetime.now(SEOUL_TZ)


def _build_slug(local_date: datetime.date) -> str:
    month = local_date.strftime("%B").lower()
    day = local_date.day
    return f"{MARKET_SLUG_PREFIX}-{month}-{day}"


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(ts)


async def _cached(key: str, fetcher, ttl: int = CACHE_TTL):
    now = time.time()
    entry = _cache.get(key)
    if entry and now - entry["ts"] < ttl:
        return entry["data"]
    data = await fetcher()
    _cache[key] = {"ts": now, "data": data}
    return data


async def _fetch_forecast() -> Dict[str, Any]:
    assert _http_client
    params = {
        "latitude": RKSI_LAT,
        "longitude": RKSI_LON,
        "hourly": "temperature_2m",
        "timezone": "Asia/Seoul",
    }
    resp = await _http_client.get("https://api.open-meteo.com/v1/forecast", params=params)
    resp.raise_for_status()
    return resp.json()


async def _fetch_metar() -> List[Dict[str, Any]]:
    assert _http_client
    params = {"ids": "RKSI", "format": "json", "hours": "24"}
    resp = await _http_client.get("https://aviationweather.gov/api/data/metar", params=params)
    resp.raise_for_status()
    return resp.json()


async def _fetch_event(slug: str) -> Optional[Dict[str, Any]]:
    assert _http_client
    url = f"{GAMMA_HOST}/events/slug/{slug}"
    resp = await _http_client.get(url)
    if resp.status_code == 404:
        resp = await _http_client.get(f"{GAMMA_HOST}/events", params={"slug": slug})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, dict) and "events" in data and data["events"]:
            return data["events"][0]
        if isinstance(data, list) and data:
            return data[0]
        return None
    resp.raise_for_status()
    return resp.json()


async def _fetch_price(token_id: str) -> Optional[float]:
    assert _http_client
    params = {"token_id": token_id, "side": "buy"}
    resp = await _http_client.get(f"{CLOB_HOST}/price", params=params)
    if resp.status_code != 200:
        return None
    data = resp.json()
    try:
        return float(data.get("price"))
    except (TypeError, ValueError):
        return None


def _build_hourly_axis(local_date: datetime.date) -> List[datetime]:
    start = datetime.combine(local_date, datetime.min.time()).replace(tzinfo=SEOUL_TZ)
    return [start + timedelta(hours=i) for i in range(24)]


def _forecast_for_date(forecast: Dict[str, Any], local_date: datetime.date) -> List[Optional[float]]:
    times = forecast.get("hourly", {}).get("time", [])
    temps = forecast.get("hourly", {}).get("temperature_2m", [])
    hourly_map: Dict[str, float] = {}
    for t, temp in zip(times, temps):
        try:
            dt = datetime.fromisoformat(t)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SEOUL_TZ)
        if dt.date() != local_date:
            continue
        key = dt.strftime("%Y-%m-%dT%H:00")
        hourly_map[key] = temp

    hourly_axis = _build_hourly_axis(local_date)
    out: List[Optional[float]] = []
    for dt in hourly_axis:
        key = dt.strftime("%Y-%m-%dT%H:00")
        out.append(hourly_map.get(key))
    return out


def _actuals_for_date(metar: List[Dict[str, Any]], local_date: datetime.date) -> Dict[str, Any]:
    readings: List[tuple[datetime, float]] = []
    for obs in metar:
        report_time = obs.get("reportTime") or obs.get("receiptTime")
        temp = obs.get("temp")
        if report_time is None or temp is None:
            continue
        try:
            dt_utc = _parse_iso(report_time)
        except ValueError:
            continue
        dt_local = dt_utc.astimezone(SEOUL_TZ)
        readings.append((dt_local, float(temp)))

    readings.sort(key=lambda r: r[0])
    hourly_axis = _build_hourly_axis(local_date)

    hourly: List[Optional[float]] = []
    latest: Optional[float] = None
    idx = 0
    for hour_dt in hourly_axis:
        while idx < len(readings) and readings[idx][0] <= hour_dt:
            latest = readings[idx][1]
            idx += 1
        hourly.append(latest)

    day_temps = [temp for dt, temp in readings if dt.date() == local_date]
    day_high = max(day_temps) if day_temps else None

    return {"hourly": hourly, "day_high": day_high}


def _format_axis_times(local_date: datetime.date) -> List[str]:
    return [
        dt.astimezone(SEOUL_TZ).isoformat()
        for dt in _build_hourly_axis(local_date)
    ]


def _get_clob_balance_sync() -> Optional[Dict[str, Any]]:
    if not (POLY_PRIVATE_KEY and POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE):
        return None
    signature_type = int(POLY_SIGNATURE_TYPE) if POLY_SIGNATURE_TYPE else None
    funder = POLY_FUNDER_ADDRESS or None
    client = ClobClient(
        CLOB_HOST,
        chain_id=POLY_CHAIN_ID,
        key=POLY_PRIVATE_KEY,
        signature_type=signature_type,
        funder=funder,
    )
    creds = ApiCreds(
        api_key=POLY_API_KEY,
        api_secret=POLY_API_SECRET,
        api_passphrase=POLY_API_PASSPHRASE,
    )
    client.set_api_creds(creds)
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    return client.get_balance_allowance(params)


async def _get_clob_balance() -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(_get_clob_balance_sync)


async def _get_positions() -> List[Dict[str, Any]]:
    if not POLY_USER_ADDRESS:
        return []
    assert _http_client
    resp = await _http_client.get(
        f"{DATA_HOST}/positions", params={"user": POLY_USER_ADDRESS}
    )
    if resp.status_code != 200:
        return []
    data = resp.json()
    if isinstance(data, dict) and "data" in data:
        return data.get("data", [])
    if isinstance(data, list):
        return data
    return []


@app.get("/api/dashboard")
async def dashboard() -> Dict[str, Any]:
    now_kst = _now_kst()
    local_date = now_kst.date()
    slug = _build_slug(local_date)

    forecast = await _cached("forecast", _fetch_forecast)
    metar = await _cached("metar", _fetch_metar)

    event = await _cached(f"event:{slug}", lambda: _fetch_event(slug))

    outcomes: List[Dict[str, Any]] = []
    if event and isinstance(event, dict):
        markets = event.get("markets", [])
        for market in markets:
            token_ids = market.get("clobTokenIds")
            if isinstance(token_ids, str):
                try:
                    token_ids = json.loads(token_ids)
                except json.JSONDecodeError:
                    token_ids = []
            token_yes = token_ids[0] if token_ids else None
            price = None
            if token_yes:
                price = await _cached(
                    f"price:{token_yes}", lambda: _fetch_price(token_yes)
                )
            outcomes.append(
                {
                    "title": market.get("groupItemTitle") or market.get("question"),
                    "tokenId": token_yes,
                    "price": price,
                }
            )

    weather_actuals = _actuals_for_date(metar, local_date)

    balance = await _cached("balance", _get_clob_balance)
    positions = await _cached("positions", _get_positions)

    return {
        "meta": {
            "lastRefresh": now_kst.isoformat(),
            "kstDate": local_date.isoformat(),
            "slug": slug,
            "eventFound": event is not None,
        },
        "weather": {
            "hourly": {
                "times": _format_axis_times(local_date),
                "forecast": _forecast_for_date(forecast, local_date),
                "actual": weather_actuals["hourly"],
            },
            "dayHigh": weather_actuals["day_high"],
        },
        "market": {
            "eventTitle": event.get("title") if event else None,
            "outcomes": outcomes,
        },
        "portfolio": {
            "balance": balance,
            "positions": positions,
        },
    }
