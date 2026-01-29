import asyncio
import json
import os
import re
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

from app.supabase_writer import SupabaseConfig, SupabaseWriter

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SEOUL_TZ = ZoneInfo(os.getenv("SEOUL_TIMEZONE", "Asia/Seoul"))
RKSI_LAT = float(os.getenv("RKSI_LAT", "37.469"))
RKSI_LON = float(os.getenv("RKSI_LON", "126.451"))

CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "900"))
MARKET_TTL = int(os.getenv("MARKET_TTL_SECONDS", "30"))
AWC_TTL = int(os.getenv("AWC_TTL_SECONDS", "60"))
EVENT_TTL = int(os.getenv("EVENT_TTL_SECONDS", "900"))
PORTFOLIO_TTL = int(os.getenv("PORTFOLIO_TTL_SECONDS", "900"))
FORECAST_TTL = int(os.getenv("FORECAST_TTL_SECONDS", "3600"))
INGESTION_ENABLED = os.getenv("INGESTION_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

OPEN_METEO_HOST = os.getenv("OPEN_METEO_HOST", "https://api.open-meteo.com")
OPEN_METEO_KMA_MODEL = os.getenv("OPEN_METEO_KMA_MODEL", "kma_seamless").strip()
_open_meteo_models_raw = os.getenv("OPEN_METEO_KMA_MODELS", "").strip()
_default_open_meteo_models = ["kma_seamless", "kma_gdps", "kma_ldps"]
OPEN_METEO_KMA_MODELS = (
    [
        model.strip()
        for model in _open_meteo_models_raw.split(",")
        if model.strip()
    ]
    if _open_meteo_models_raw
    else list(_default_open_meteo_models)
)
if not OPEN_METEO_KMA_MODEL:
    OPEN_METEO_KMA_MODEL = OPEN_METEO_KMA_MODELS[0]
if OPEN_METEO_KMA_MODEL in OPEN_METEO_KMA_MODELS:
    OPEN_METEO_KMA_MODELS = [OPEN_METEO_KMA_MODEL] + [
        model for model in OPEN_METEO_KMA_MODELS if model != OPEN_METEO_KMA_MODEL
    ]
else:
    OPEN_METEO_KMA_MODELS = [OPEN_METEO_KMA_MODEL] + OPEN_METEO_KMA_MODELS
OPEN_METEO_FORECAST_DAYS = int(os.getenv("OPEN_METEO_FORECAST_DAYS", "3"))
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
CHECK_WX_TTL = int(os.getenv("CHECK_WX_TTL_SECONDS", "900"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_WRITE_ENABLED = os.getenv("SUPABASE_WRITE_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_http_client: Optional[httpx.AsyncClient] = None
_supabase: Optional[SupabaseWriter] = None
_last_persisted: Dict[str, float] = {}

_ingestion_tasks: List["asyncio.Task[None]"] = []
_state_lock = asyncio.Lock()
_health: Dict[str, Dict[str, Optional[str]]] = {
    "awc": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
    "checkwx": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
    "event": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
    "market": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
    "forecast": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
    "portfolio": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
}

_latest_slug: Optional[str] = None
_latest_event: Optional[Dict[str, Any]] = None
_latest_event_fetched_ts: Optional[float] = None
_latest_market_outcomes: List[Dict[str, Any]] = []
_latest_market_state: List[Dict[str, Any]] = []

_latest_metar_awc: List[Dict[str, Any]] = []
_latest_metar_checkwx: List[Dict[str, Any]] = []

_latest_forecast: Optional[Dict[str, Any]] = None
_latest_forecast_fetched_ts: Optional[float] = None

_latest_balance: Optional[Dict[str, Any]] = None
_latest_positions: List[Dict[str, Any]] = []


@app.on_event("startup")
async def on_startup() -> None:
    global _http_client, _supabase, _ingestion_tasks
    _http_client = httpx.AsyncClient(timeout=15)
    _supabase = SupabaseWriter(
        SupabaseConfig(
            url=SUPABASE_URL,
            service_role_key=SUPABASE_SERVICE_ROLE_KEY,
            write_enabled=SUPABASE_WRITE_ENABLED,
        ),
        _http_client,
    )
    if INGESTION_ENABLED:
        _ingestion_tasks = [
            asyncio.create_task(_ingest_market_loop()),
            asyncio.create_task(_ingest_awc_loop()),
            asyncio.create_task(_ingest_checkwx_loop()),
            asyncio.create_task(_ingest_forecast_loop()),
            asyncio.create_task(_ingest_portfolio_loop()),
        ]


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _ingestion_tasks
    for task in _ingestion_tasks:
        task.cancel()
    if _ingestion_tasks:
        await asyncio.gather(*_ingestion_tasks, return_exceptions=True)
    _ingestion_tasks = []
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


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_celsius_bounds(title: Any) -> tuple[Optional[int], Optional[int]]:
    if not isinstance(title, str):
        return None, None
    match = re.search(r"(-?\d+)\s*°\s*C", title)
    if not match:
        return None, None
    value = _coerce_int(match.group(1))
    if value is None:
        return None, None
    lowered = title.lower()
    if "or below" in lowered:
        return None, value
    if "or higher" in lowered:
        return value, None
    return value, value


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
        normalized.append({"reportTime": observed, "temp": temp_c, "raw": entry})
    return normalized


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


async def _fetch_book(token_id: str) -> Optional[Dict[str, Any]]:
    assert _http_client
    resp = await _http_client.get(f"{CLOB_HOST}/book", params={"token_id": token_id})
    if resp.status_code != 200:
        return None
    data = resp.json()
    return data if isinstance(data, dict) else None


def _top_of_book(book: Any) -> Dict[str, Optional[float]]:
    if not isinstance(book, dict):
        return {
            "best_bid": None,
            "best_ask": None,
            "bid_size": None,
            "ask_size": None,
        }
    bids = book.get("bids")
    asks = book.get("asks")

    best_bid = None
    bid_size = None
    if isinstance(bids, list) and bids:
        prices = []
        for bid in bids:
            if not isinstance(bid, dict):
                continue
            price = _coerce_float(bid.get("price"))
            size = _coerce_float(bid.get("size"))
            if price is None:
                continue
            prices.append((price, size))
        if prices:
            best_bid, bid_size = max(prices, key=lambda x: x[0])

    best_ask = None
    ask_size = None
    if isinstance(asks, list) and asks:
        prices = []
        for ask in asks:
            if not isinstance(ask, dict):
                continue
            price = _coerce_float(ask.get("price"))
            size = _coerce_float(ask.get("size"))
            if price is None:
                continue
            prices.append((price, size))
        if prices:
            best_ask, ask_size = min(prices, key=lambda x: x[0])

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
    }


async def _fetch_open_meteo_hourly() -> Dict[str, Any]:
    assert _http_client
    params = {
        "latitude": RKSI_LAT,
        "longitude": RKSI_LON,
        "hourly": "temperature_2m",
        "models": ",".join(OPEN_METEO_KMA_MODELS),
        "timezone": SEOUL_TZ.key,
        "forecast_days": OPEN_METEO_FORECAST_DAYS,
    }
    resp = await _http_client.get(f"{OPEN_METEO_HOST}/v1/forecast", params=params)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        return {}
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), dict) else {}
    times = hourly.get("time") if isinstance(hourly.get("time"), list) else []
    temp_by_model: Dict[str, Any] = {}
    if len(OPEN_METEO_KMA_MODELS) == 1:
        temps = hourly.get("temperature_2m") if isinstance(hourly.get("temperature_2m"), list) else []
        temp_by_model[OPEN_METEO_KMA_MODELS[0]] = temps
    else:
        for model in OPEN_METEO_KMA_MODELS:
            key = f"temperature_2m_{model}"
            temps = hourly.get(key) if isinstance(hourly.get(key), list) else []
            temp_by_model[model] = temps
    return {
        "source": "open-meteo",
        "defaultModel": OPEN_METEO_KMA_MODELS[0],
        "models": OPEN_METEO_KMA_MODELS,
        "timezone": payload.get("timezone") or SEOUL_TZ.key,
        "generatedAt": payload.get("generationtime_ms"),
        "hourly": {"times": times, "temp_c_by_model": temp_by_model},
    }


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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _mark_health_success(source: str) -> None:
    async with _state_lock:
        entry = _health.setdefault(
            source, {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None}
        )
        entry["lastSuccessAt"] = _utc_now_iso()
        entry["lastError"] = None
        entry["lastErrorAt"] = None


async def _mark_health_error(source: str, err: Exception) -> None:
    message = str(err) or repr(err)
    async with _state_lock:
        entry = _health.setdefault(
            source, {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None}
        )
        entry["lastError"] = message[:500]
        entry["lastErrorAt"] = _utc_now_iso()


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


def _latest_common_hour_index(
    left: List[Optional[float]], right: List[Optional[float]]
) -> Optional[int]:
    limit = min(len(left), len(right))
    for idx in range(limit - 1, -1, -1):
        if isinstance(left[idx], (int, float)) and isinstance(right[idx], (int, float)):
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


def _latest_metar_observation(metar: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    latest: Optional[Dict[str, Any]] = None
    latest_dt: Optional[datetime] = None
    for obs in metar:
        report_time = obs.get("reportTime") or obs.get("receiptTime")
        if not isinstance(report_time, str):
            continue
        try:
            dt = _parse_iso(report_time)
        except ValueError:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
            latest = obs
    return latest


async def _persist_to_supabase(
    *,
    captured_at: datetime,
    date_kst: datetime.date,
    slug: str,
    event: Optional[Dict[str, Any]],
    market_state: List[Dict[str, Any]],
    metar_awc: List[Dict[str, Any]],
    metar_checkwx: List[Dict[str, Any]],
    forecast: Optional[Dict[str, Any]],
    forecast_cache_ts: Optional[float],
) -> None:
    if not _supabase or not _supabase.enabled():
        return

    now_utc = captured_at.astimezone(timezone.utc)
    try:
        awc_latest = _latest_metar_observation(metar_awc)
        if awc_latest:
            report_time = awc_latest.get("reportTime") or awc_latest.get("receiptTime")
            if isinstance(report_time, str):
                try:
                    observed_at = _parse_iso(report_time).isoformat()
                except ValueError:
                    observed_at = None
                temp_c = _coerce_float(awc_latest.get("temp"))
                if observed_at and temp_c is not None:
                    await _supabase.upsert(
                        "weather_metar_obs",
                        {
                            "station": "RKSI",
                            "source": "awc",
                            "observed_at": observed_at,
                            "raw_text": awc_latest.get("rawOb"),
                            "temp_c": temp_c,
                            "dewpoint_c": _coerce_float(awc_latest.get("dewp")),
                            "wind_dir_deg": _coerce_int(awc_latest.get("wdir")),
                            "wind_speed_kt": _coerce_int(awc_latest.get("wspd")),
                            "wind_gust_kt": _coerce_int(awc_latest.get("gust")),
                            "pressure_hpa": _coerce_float(awc_latest.get("altim")),
                            "visibility": awc_latest.get("visib"),
                            "flight_category": awc_latest.get("fltCat"),
                            "raw": awc_latest,
                        },
                        on_conflict="station,source,observed_at",
                    )

        checkwx_latest = _latest_metar_observation(metar_checkwx)
        if checkwx_latest:
            report_time = checkwx_latest.get("reportTime") or checkwx_latest.get("receiptTime")
            if isinstance(report_time, str):
                try:
                    observed_at = _parse_iso(report_time).isoformat()
                except ValueError:
                    observed_at = None
                temp_c = _coerce_float(checkwx_latest.get("temp"))
                if observed_at and temp_c is not None:
                    await _supabase.upsert(
                        "weather_metar_obs",
                        {
                            "station": "RKSI",
                            "source": "checkwx",
                            "observed_at": observed_at,
                            "temp_c": temp_c,
                            "raw": checkwx_latest.get("raw"),
                        },
                        on_conflict="station,source,observed_at",
                    )

        if (
            forecast
            and isinstance(forecast_cache_ts, (int, float))
            and forecast_cache_ts > _last_persisted.get("forecast", 0)
        ):
            hourly = forecast.get("hourly") if isinstance(forecast.get("hourly"), dict) else {}
            times = hourly.get("times") if isinstance(hourly.get("times"), list) else []
            temp_by_model = hourly.get("temp_c_by_model") if isinstance(hourly.get("temp_c_by_model"), dict) else None

            runs_to_insert: List[Dict[str, Any]] = []
            series_by_model: Dict[str, List[Any]] = {}

            if isinstance(temp_by_model, dict) and temp_by_model:
                for model, temps in temp_by_model.items():
                    if not isinstance(model, str):
                        continue
                    if not isinstance(temps, list):
                        continue
                    series_by_model[model] = temps
            else:
                model = (
                    forecast.get("model")
                    if isinstance(forecast.get("model"), str)
                    else forecast.get("defaultModel")
                    if isinstance(forecast.get("defaultModel"), str)
                    else OPEN_METEO_KMA_MODEL
                )
                temps = hourly.get("temp_c") if isinstance(hourly.get("temp_c"), list) else []
                series_by_model[model] = temps

            for model in series_by_model.keys():
                runs_to_insert.append(
                    {
                        "model": model,
                        "station": "RKSI",
                        "run_at": now_utc.isoformat(),
                        "source": forecast.get("source") or "open-meteo",
                        "raw": forecast,
                    }
                )

            run_rows = (
                await _supabase.insert("forecast_runs", runs_to_insert, select="id,model")
                if runs_to_insert
                else []
            )
            run_id_by_model: Dict[str, str] = {}
            for row in run_rows:
                model = row.get("model")
                run_id = row.get("id")
                if isinstance(model, str) and isinstance(run_id, str):
                    run_id_by_model[model] = run_id

            if times:
                hourly_rows: List[Dict[str, Any]] = []
                for model, temps in series_by_model.items():
                    run_id = run_id_by_model.get(model)
                    if not run_id:
                        continue
                    for valid_at, temp in zip(times, temps):
                        if not isinstance(valid_at, str):
                            continue
                        parsed = None
                        try:
                            parsed = datetime.fromisoformat(valid_at)
                        except ValueError:
                            parsed = None
                        if parsed is None:
                            continue
                        if parsed.tzinfo is None:
                            parsed = parsed.replace(tzinfo=SEOUL_TZ)
                        parsed_utc = parsed.astimezone(timezone.utc)
                        temp_val = _coerce_float(temp)
                        if temp_val is None:
                            continue
                        hourly_rows.append(
                            {
                                "run_id": run_id,
                                "valid_at": parsed_utc.isoformat(),
                                "temp_c": temp_val,
                            }
                        )
                if hourly_rows:
                    await _supabase.insert("forecast_hourly", hourly_rows)

            _last_persisted["forecast"] = float(forecast_cache_ts)

        if not isinstance(event, dict):
            return

        gamma_event_id = event.get("id")
        if gamma_event_id is None:
            return

        event_rows = await _supabase.upsert(
            "events",
            {
                "date_kst": date_kst.isoformat(),
                "slug": slug,
                "gamma_event_id": str(gamma_event_id),
                "last_seen_at": now_utc.isoformat(),
                "raw": event,
            },
            on_conflict="slug",
            select="id",
        )
        event_id = event_rows[0].get("id") if event_rows else None
        if not event_id:
            fetched = await _supabase.select(
                "events",
                select="id",
                filters={"slug": f"eq.{slug}"},
                limit=1,
            )
            event_id = fetched[0].get("id") if fetched else None
        if not event_id:
            return

        market_rows: List[Dict[str, Any]] = []
        for state in market_state:
            gamma_market = state.get("gamma_market")
            tokens = state.get("tokens") or {}
            if not isinstance(gamma_market, dict):
                continue
            gamma_market_id = gamma_market.get("id")
            if gamma_market_id is None:
                continue
            yes_token_id = tokens.get("yes")
            no_token_id = tokens.get("no")
            if not yes_token_id or not no_token_id:
                continue
            group_title = gamma_market.get("groupItemTitle")
            lower_c, upper_c = _parse_celsius_bounds(group_title)
            market_rows.append(
                {
                    "event_id": event_id,
                    "gamma_market_id": str(gamma_market_id),
                    "condition_id": gamma_market.get("conditionId"),
                    "market_slug": gamma_market.get("slug"),
                    "question": gamma_market.get("question"),
                    "group_item_title": group_title,
                    "group_item_threshold": _coerce_int(gamma_market.get("groupItemThreshold")),
                    "lower_bound_celsius": lower_c,
                    "upper_bound_celsius": upper_c,
                    "yes_token_id": str(yes_token_id),
                    "no_token_id": str(no_token_id),
                    "raw": gamma_market,
                }
            )

        if market_rows:
            await _supabase.upsert(
                "event_markets",
                market_rows,
                on_conflict="event_id,gamma_market_id",
                select="id,gamma_market_id",
            )

        market_id_rows = await _supabase.select(
            "event_markets",
            select="id,gamma_market_id",
            filters={"event_id": f"eq.{event_id}"},
        )
        market_id_by_gamma: Dict[str, str] = {}
        for row in market_id_rows:
            gamma_market_id = row.get("gamma_market_id")
            market_id = row.get("id")
            if isinstance(gamma_market_id, str) and isinstance(market_id, str):
                market_id_by_gamma[gamma_market_id] = market_id

        snapshot_rows: List[Dict[str, Any]] = []
        for state in market_state:
            gamma_market = state.get("gamma_market")
            if not isinstance(gamma_market, dict):
                continue
            gamma_market_id = gamma_market.get("id")
            if gamma_market_id is None:
                continue
            market_id = market_id_by_gamma.get(str(gamma_market_id))
            if not market_id:
                continue

            accepting_orders_ts = gamma_market.get("acceptingOrdersTimestamp")
            if not isinstance(accepting_orders_ts, str):
                accepting_orders_ts = None
            else:
                try:
                    accepting_orders_ts = _parse_iso(accepting_orders_ts).isoformat()
                except ValueError:
                    accepting_orders_ts = None

            yes_top = state.get("yes_top") or {}
            no_top = state.get("no_top") or {}
            snapshot_rows.append(
                {
                    "captured_at": now_utc.isoformat(),
                    "event_id": event_id,
                    "market_id": market_id,
                    "accepting_orders": gamma_market.get("acceptingOrders"),
                    "accepting_orders_timestamp": accepting_orders_ts,
                    "yes_best_bid": yes_top.get("best_bid"),
                    "yes_best_ask": yes_top.get("best_ask"),
                    "no_best_bid": no_top.get("best_bid"),
                    "no_best_ask": no_top.get("best_ask"),
                    "yes_bid_size": yes_top.get("bid_size"),
                    "yes_ask_size": yes_top.get("ask_size"),
                    "no_bid_size": no_top.get("bid_size"),
                    "no_ask_size": no_top.get("ask_size"),
                    "volume24h": state.get("volume24h"),
                    "source": "clob_orderbook",
                    "raw": {
                        "gamma": gamma_market,
                        "clob_yes": state.get("yes_book"),
                        "clob_no": state.get("no_book"),
                    },
                }
            )

        if snapshot_rows:
            await _supabase.insert("market_snapshots", snapshot_rows)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def _sleep_remaining(started_ts: float, interval_seconds: int) -> None:
    elapsed = time.time() - started_ts
    await asyncio.sleep(max(0.0, interval_seconds - elapsed))


async def _build_market_outcomes(
    event: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    markets = event.get("markets", [])
    if not isinstance(markets, list):
        return [], []

    token_ids: List[str] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        tokens = _select_tokens(market)
        token_yes = tokens.get("yes")
        token_no = tokens.get("no")
        if token_yes:
            token_ids.append(token_yes)
        if token_no:
            token_ids.append(token_no)

    book_tasks: Dict[str, "asyncio.Task[Any]"] = {}
    for token_id in sorted(set(token_ids)):
        book_tasks[token_id] = asyncio.create_task(_fetch_book(token_id))

    book_results = (
        await asyncio.gather(*book_tasks.values(), return_exceptions=True)
        if book_tasks
        else []
    )
    books: Dict[str, Any] = {}
    for token_id, result in zip(book_tasks.keys(), book_results):
        books[token_id] = None if isinstance(result, Exception) else result

    outcomes: List[Dict[str, Any]] = []
    market_state: List[Dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        tokens = _select_tokens(market)
        token_yes = tokens.get("yes")
        token_no = tokens.get("no")

        yes_book = books.get(token_yes) if token_yes else None
        no_book = books.get(token_no) if token_no else None

        yes_top = _top_of_book(yes_book) if token_yes else _top_of_book(None)
        no_top = _top_of_book(no_book) if token_no else _top_of_book(None)

        yes_best_ask = yes_top["best_ask"]
        no_best_ask = no_top["best_ask"]

        outcome_prices = _coerce_list(market.get("outcomePrices"))
        yes_fallback = None
        no_fallback = None
        if len(outcome_prices) >= 2:
            yes_fallback = _coerce_float(outcome_prices[0])
            no_fallback = _coerce_float(outcome_prices[1])

        yes_price = yes_best_ask
        if yes_price is None or yes_price <= 0:
            yes_price = (
                _coerce_float(market.get("bestAsk"))
                or _coerce_float(market.get("lastTradePrice"))
                or yes_fallback
            )

        no_price = no_best_ask
        if no_price is None or no_price <= 0:
            no_price = no_fallback

        outcomes.append(
            {
                "title": market.get("groupItemTitle") or market.get("question"),
                "tokenId": token_yes,
                "tokenYes": token_yes,
                "tokenNo": token_no,
                "yesBestBid": yes_top["best_bid"],
                "yesBestAsk": yes_top["best_ask"],
                "noBestBid": no_top["best_bid"],
                "noBestAsk": no_top["best_ask"],
                "yesBidSize": yes_top["bid_size"],
                "yesAskSize": yes_top["ask_size"],
                "noBidSize": no_top["bid_size"],
                "noAskSize": no_top["ask_size"],
                "price": yes_price,
                "yesPrice": yes_price,
                "noPrice": no_price,
                "acceptingOrders": market.get("acceptingOrders"),
                "volume24hr": _coerce_float(market.get("volume24hr"))
                or _coerce_float(market.get("volume24hrClob")),
            }
        )
        market_state.append(
            {
                "gamma_market": market,
                "tokens": tokens,
                "yes_book": yes_book,
                "no_book": no_book,
                "yes_top": yes_top,
                "no_top": no_top,
                "volume24h": _coerce_float(market.get("volume24hr"))
                or _coerce_float(market.get("volume24hrClob")),
            }
        )

    return outcomes, market_state


async def _ingest_market_loop() -> None:
    global _latest_event, _latest_event_fetched_ts, _latest_market_outcomes, _latest_market_state, _latest_slug
    while True:
        started = time.time()
        now_kst = _now_kst()
        local_date = now_kst.date()
        slug = _build_slug(local_date)

        async with _state_lock:
            if _latest_slug != slug:
                _latest_slug = slug
                _latest_event = None
                _latest_event_fetched_ts = None
                _latest_market_outcomes = []
                _latest_market_state = []
            event = _latest_event
            event_fetched_ts = _latest_event_fetched_ts

        needs_event_refresh = (
            event is None
            or event_fetched_ts is None
            or (time.time() - event_fetched_ts) >= EVENT_TTL
        )
        if needs_event_refresh:
            try:
                fetched = await _fetch_event(slug)
                async with _state_lock:
                    _latest_event = fetched if isinstance(fetched, dict) else None
                    _latest_event_fetched_ts = time.time()
                await _mark_health_success("event")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await _mark_health_error("event", exc)
                await _sleep_remaining(started, MARKET_TTL)
                continue

        async with _state_lock:
            event = _latest_event

        if not isinstance(event, dict):
            async with _state_lock:
                _latest_market_outcomes = []
                _latest_market_state = []
            await _mark_health_success("market")
            await _sleep_remaining(started, MARKET_TTL)
            continue

        captured_at = datetime.now(timezone.utc)
        try:
            outcomes, market_state = await _build_market_outcomes(event)
            async with _state_lock:
                _latest_market_outcomes = outcomes
                _latest_market_state = market_state
            await _mark_health_success("market")

            await _persist_to_supabase(
                captured_at=captured_at,
                date_kst=local_date,
                slug=slug,
                event=event,
                market_state=market_state,
                metar_awc=[],
                metar_checkwx=[],
                forecast=None,
                forecast_cache_ts=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("market", exc)

        await _sleep_remaining(started, MARKET_TTL)


async def _ingest_awc_loop() -> None:
    global _latest_metar_awc
    while True:
        started = time.time()
        try:
            metar_awc = await _fetch_metar_aviation_weather()
            if isinstance(metar_awc, list):
                async with _state_lock:
                    _latest_metar_awc = metar_awc
            await _mark_health_success("awc")

            now_kst = _now_kst()
            local_date = now_kst.date()
            slug = _build_slug(local_date)
            await _persist_to_supabase(
                captured_at=datetime.now(timezone.utc),
                date_kst=local_date,
                slug=slug,
                event=None,
                market_state=[],
                metar_awc=metar_awc if isinstance(metar_awc, list) else [],
                metar_checkwx=[],
                forecast=None,
                forecast_cache_ts=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("awc", exc)

        await _sleep_remaining(started, AWC_TTL)


async def _ingest_checkwx_loop() -> None:
    global _latest_metar_checkwx
    while True:
        started = time.time()
        try:
            metar_checkwx = await _fetch_metar_checkwx()
            if isinstance(metar_checkwx, list):
                now_utc = datetime.now(timezone.utc)
                cutoff = now_utc - timedelta(hours=48)
                merged: Dict[str, Dict[str, Any]] = {}
                async with _state_lock:
                    for obs in _latest_metar_checkwx:
                        report_time = obs.get("reportTime") or obs.get("receiptTime")
                        if isinstance(report_time, str):
                            merged[report_time] = obs
                    for obs in metar_checkwx:
                        report_time = obs.get("reportTime") or obs.get("receiptTime")
                        if isinstance(report_time, str):
                            merged[report_time] = obs

                    pruned: List[tuple[datetime, Dict[str, Any]]] = []
                    for report_time, obs in merged.items():
                        try:
                            dt = _parse_iso(report_time)
                        except ValueError:
                            continue
                        if dt < cutoff:
                            continue
                        pruned.append((dt, obs))
                    pruned.sort(key=lambda row: row[0])
                    _latest_metar_checkwx = [obs for _, obs in pruned]
            await _mark_health_success("checkwx")

            now_kst = _now_kst()
            local_date = now_kst.date()
            slug = _build_slug(local_date)
            await _persist_to_supabase(
                captured_at=datetime.now(timezone.utc),
                date_kst=local_date,
                slug=slug,
                event=None,
                market_state=[],
                metar_awc=[],
                metar_checkwx=metar_checkwx if isinstance(metar_checkwx, list) else [],
                forecast=None,
                forecast_cache_ts=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("checkwx", exc)

        await _sleep_remaining(started, CHECK_WX_TTL)


async def _ingest_forecast_loop() -> None:
    global _latest_forecast, _latest_forecast_fetched_ts
    while True:
        started = time.time()
        try:
            fetched = await _fetch_open_meteo_hourly()
            fetched_ts = time.time()
            if isinstance(fetched, dict):
                async with _state_lock:
                    _latest_forecast = fetched
                    _latest_forecast_fetched_ts = fetched_ts
            await _mark_health_success("forecast")

            now_kst = _now_kst()
            local_date = now_kst.date()
            slug = _build_slug(local_date)
            await _persist_to_supabase(
                captured_at=datetime.now(timezone.utc),
                date_kst=local_date,
                slug=slug,
                event=None,
                market_state=[],
                metar_awc=[],
                metar_checkwx=[],
                forecast=fetched,
                forecast_cache_ts=fetched_ts,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("forecast", exc)

        await _sleep_remaining(started, FORECAST_TTL)


async def _ingest_portfolio_loop() -> None:
    global _latest_balance, _latest_positions
    while True:
        started = time.time()
        try:
            balance = await _get_clob_balance()
            positions = await _get_positions()
            async with _state_lock:
                _latest_balance = balance if isinstance(balance, dict) else None
                _latest_positions = positions if isinstance(positions, list) else []
            await _mark_health_success("portfolio")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("portfolio", exc)

        await _sleep_remaining(started, PORTFOLIO_TTL)


@app.get("/api/dashboard")
async def dashboard() -> Dict[str, Any]:
    now_kst = _now_kst()
    local_date = now_kst.date()
    slug = _build_slug(local_date)
    async with _state_lock:
        state_slug = _latest_slug
        event = _latest_event if state_slug == slug else None
        outcomes = _latest_market_outcomes if state_slug == slug else []
        metar_awc = _latest_metar_awc
        metar_checkwx = _latest_metar_checkwx
        forecast = _latest_forecast
        balance = _latest_balance
        positions = _latest_positions
        health = {key: dict(val) for key, val in _health.items()}

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
    compare_idx = _latest_common_hour_index(awc_actuals["hourly"], checkwx_actuals["hourly"])
    if compare_idx is not None:
        awc_compare = awc_actuals["hourly"][compare_idx]
        checkwx_compare = checkwx_actuals["hourly"][compare_idx]
        if isinstance(awc_compare, (int, float)) and isinstance(checkwx_compare, (int, float)):
            delta = round(checkwx_compare - awc_compare, 2)
            match = abs(delta) < 0.05

    day_high = _max_optional([awc_actuals["day_high"], checkwx_actuals["day_high"]])

    return {
        "meta": {
            "lastRefresh": now_kst.isoformat(),
            "kstDate": local_date.isoformat(),
            "slug": slug,
            "eventFound": event is not None,
            "health": health,
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
        "forecast": forecast,
        "portfolio": {
            "balance": balance,
            "positions": positions,
        },
    }
