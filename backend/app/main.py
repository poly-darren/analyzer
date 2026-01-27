import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
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

CHECK_WX_API_KEY = os.getenv("CHECK_WX_API_KEY", "").strip()
CHECK_WX_HOST = os.getenv("CHECK_WX_HOST", "https://api.checkwx.com")
CHECK_WX_TTL = int(os.getenv("CHECK_WX_TTL_SECONDS", str(CACHE_TTL)))

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
    parsed = datetime.fromisoformat(ts)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def _cached(key: str, fetcher, ttl: int = CACHE_TTL):
    now = time.time()
    entry = _cache.get(key)
    if entry and now - entry["ts"] < ttl:
        return entry["data"]
    data = await fetcher()
    _cache[key] = {"ts": now, "data": data}
    return data


async def _fetch_metar_aviation_weather() -> List[Dict[str, Any]]:
    assert _http_client
    params = {"ids": "RKSI", "format": "json", "hours": "24"}
    resp = await _http_client.get("https://aviationweather.gov/api/data/metar", params=params)
    resp.raise_for_status()
    return resp.json()


async def _fetch_metar_checkwx() -> List[Dict[str, Any]]:
    if not CHECK_WX_API_KEY:
        return []
    assert _http_client
    headers = {"X-API-Key": CHECK_WX_API_KEY}
    resp = await _http_client.get(f"{CHECK_WX_HOST}/metar/RKSI/decoded", headers=headers)
    if resp.status_code != 200:
        return []
    payload = resp.json()
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        observed = entry.get("observed")
        temperature = entry.get("temperature")
        temp_c = None
        if isinstance(temperature, dict):
            temp_c = temperature.get("celsius")
        temp_c = _coerce_float(temp_c)
        if observed is None or temp_c is None:
            continue
        normalized.append({"reportTime": observed, "temp": temp_c})
    return normalized


async def _fetch_metar() -> Dict[str, List[Dict[str, Any]]]:
    try:
        awc = await _cached("metar:awc", _fetch_metar_aviation_weather)
    except Exception:
        awc = []
    try:
        checkwx = await _cached(
            "metar:checkwx", _fetch_metar_checkwx, ttl=CHECK_WX_TTL
        )
    except Exception:
        checkwx = []
    return {"awc": awc or [], "checkwx": checkwx or []}


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


async def _fetch_best_ask(token_id: str) -> Optional[float]:
    assert _http_client
    resp = await _http_client.get(f"{CLOB_HOST}/book", params={"token_id": token_id})
    if resp.status_code != 200:
        return None
    data = resp.json()
    try:
        asks = data.get("asks")
        if isinstance(asks, list) and asks:
            prices = [
                _coerce_float(ask.get("price"))
                for ask in asks
                if isinstance(ask, dict)
            ]
            prices = [p for p in prices if isinstance(p, (int, float))]
            if prices:
                return min(prices)
        return None
    except (TypeError, ValueError):
        return None


def _build_time_axis(local_date: datetime.date, minutes_step: int = 30) -> List[datetime]:
    start = datetime.combine(local_date, datetime.min.time()).replace(tzinfo=SEOUL_TZ)
    total_minutes = 24 * 60
    steps = total_minutes // minutes_step
    return [start + timedelta(minutes=i * minutes_step) for i in range(steps)]


def _coerce_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _select_tokens(market: Dict[str, Any]) -> Dict[str, Optional[str]]:
    token_ids = _coerce_list(market.get("clobTokenIds"))
    outcomes = _coerce_list(market.get("outcomes"))
    tokens: Dict[str, Optional[str]] = {"yes": None, "no": None}
    if outcomes and token_ids:
        for idx, outcome in enumerate(outcomes):
            if idx >= len(token_ids) or not isinstance(outcome, str):
                continue
            label = outcome.strip().lower()
            if label == "yes":
                tokens["yes"] = token_ids[idx]
            elif label == "no":
                tokens["no"] = token_ids[idx]
    if tokens["yes"] is None and token_ids:
        tokens["yes"] = token_ids[0]
    if tokens["no"] is None and len(token_ids) > 1:
        tokens["no"] = token_ids[1]
    return tokens


def _actuals_for_date(
    metar: List[Dict[str, Any]], local_date: datetime.date, now_kst: datetime
) -> Dict[str, Any]:
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
        if dt_local.date() != local_date:
            continue
        readings.append((dt_local, float(temp)))

    readings.sort(key=lambda r: r[0])
    hourly_axis = _build_time_axis(local_date, minutes_step=30)

    hourly: List[Optional[float]] = []
    latest: Optional[float] = None
    idx = 0
    for hour_dt in hourly_axis:
        if hour_dt > now_kst:
            hourly.append(None)
            continue
        while idx < len(readings) and readings[idx][0] <= hour_dt:
            latest = readings[idx][1]
            idx += 1
        hourly.append(latest)

    day_temps = [temp for dt, temp in readings if dt <= now_kst]
    day_high = max(day_temps) if day_temps else None

    return {"hourly": hourly, "day_high": day_high}


def _latest_hour_index(values: List[Optional[float]]) -> Optional[int]:
    for idx in range(len(values) - 1, -1, -1):
        if isinstance(values[idx], (int, float)):
            return idx
    return None


def _max_optional(values: List[Optional[float]]) -> Optional[float]:
    present = [val for val in values if isinstance(val, (int, float))]
    return max(present) if present else None


def _format_axis_times(local_date: datetime.date) -> List[str]:
    return [
        dt.astimezone(SEOUL_TZ).isoformat()
        for dt in _build_time_axis(local_date, minutes_step=30)
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

    metar = await _cached("metar", _fetch_metar)
    if isinstance(metar, dict):
        metar_awc = metar.get("awc", []) or []
        metar_checkwx = metar.get("checkwx", []) or []
    else:
        metar_awc = []
        metar_checkwx = []

    event = await _cached(f"event:{slug}", lambda: _fetch_event(slug))

    outcomes: List[Dict[str, Any]] = []
    if event and isinstance(event, dict):
        markets = event.get("markets", [])
        for market in markets:
            tokens = _select_tokens(market)
            token_yes = tokens.get("yes")
            token_no = tokens.get("no")
            yes_price = None
            no_price = None
            if token_yes:
                yes_price = await _cached(
                    f"price:{token_yes}", lambda: _fetch_best_ask(token_yes)
                )
            if token_no:
                no_price = await _cached(
                    f"price:{token_no}", lambda: _fetch_best_ask(token_no)
                )
            if yes_price is None or yes_price <= 0:
                yes_price = _coerce_float(market.get("bestAsk")) or _coerce_float(
                    market.get("lastTradePrice")
                ) or yes_price
            if no_price is not None and no_price <= 0:
                no_price = None
            outcomes.append(
                {
                    "title": market.get("groupItemTitle") or market.get("question"),
                    "tokenId": token_yes,
                    "tokenYes": token_yes,
                    "tokenNo": token_no,
                    "price": yes_price,
                    "yesPrice": yes_price,
                    "noPrice": no_price,
                    "volume24hr": _coerce_float(market.get("volume24hr"))
                    or _coerce_float(market.get("volume24hrClob")),
                }
            )

    awc_actuals = _actuals_for_date(metar_awc, local_date, now_kst)
    checkwx_actuals = _actuals_for_date(metar_checkwx, local_date, now_kst)
    axis_times = _format_axis_times(local_date)

    awc_latest_idx = _latest_hour_index(awc_actuals["hourly"])
    checkwx_latest_idx = _latest_hour_index(checkwx_actuals["hourly"])
    awc_latest = (
        awc_actuals["hourly"][awc_latest_idx]
        if awc_latest_idx is not None
        else None
    )
    checkwx_latest = (
        checkwx_actuals["hourly"][checkwx_latest_idx]
        if checkwx_latest_idx is not None
        else None
    )
    awc_latest_time = (
        axis_times[awc_latest_idx] if awc_latest_idx is not None else None
    )
    checkwx_latest_time = (
        axis_times[checkwx_latest_idx] if checkwx_latest_idx is not None else None
    )

    delta = None
    match = None
    if isinstance(awc_latest, (int, float)) and isinstance(checkwx_latest, (int, float)):
        delta = round(checkwx_latest - awc_latest, 2)
        match = abs(delta) < 0.05

    day_high = _max_optional([awc_actuals["day_high"], checkwx_actuals["day_high"]])

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
                "times": axis_times,
                "awc": awc_actuals["hourly"],
                "checkwx": checkwx_actuals["hourly"],
            },
            "dayHigh": day_high,
            "sources": {
                "awc": {
                    "latest": awc_latest,
                    "latestTime": awc_latest_time,
                    "dayHigh": awc_actuals["day_high"],
                },
                "checkwx": {
                    "latest": checkwx_latest,
                    "latestTime": checkwx_latest_time,
                    "dayHigh": checkwx_actuals["day_high"],
                },
                "match": match,
                "delta": delta,
            },
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
