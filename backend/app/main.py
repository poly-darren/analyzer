import asyncio
import html
import json
import math
import os
import re
import time
from bisect import bisect_left, bisect_right
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
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
WU_TTL = int(os.getenv("WU_TTL_SECONDS", "300"))
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

WU_HISTORY_HOST = os.getenv("WU_HISTORY_HOST", "https://www.wunderground.com").rstrip("/")

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
    "wunderground": {"lastSuccessAt": None, "lastError": None, "lastErrorAt": None},
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

_latest_wu_history: Optional[Dict[str, Any]] = None
_wu_observed_max_by_date: Dict[str, float] = {}
_wu_observed_max_whole_by_date: Dict[str, int] = {}
_wu_observed_max_loaded: set[str] = set()

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
            asyncio.create_task(_ingest_wunderground_loop()),
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


def _parse_date_kst(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date_kst; use YYYY-MM-DD") from exc


def _parse_time_hhmm(value: str) -> tuple[dt_time, bool]:
    if value == "24:00":
        return dt_time(0, 0), True
    try:
        return dt_time.fromisoformat(value), False
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid time; use HH:MM") from exc


def _kst_datetime(day: date, hhmm: str) -> datetime:
    parsed, is_next_day = _parse_time_hhmm(hhmm)
    base = datetime.combine(day, parsed, tzinfo=SEOUL_TZ)
    return base + timedelta(days=1) if is_next_day else base


def _format_kst_hhmm(dt_utc: datetime) -> str:
    return dt_utc.astimezone(SEOUL_TZ).strftime("%H:%M")


def _require_supabase() -> SupabaseWriter:
    if not _supabase or not _supabase.enabled():
        raise HTTPException(status_code=503, detail="Supabase not configured")
    return _supabase


def _market_matches_bucket(
    lower: Optional[int], upper: Optional[int], target: int
) -> bool:
    if lower is None and upper is None:
        return False
    if lower is None:
        return target <= upper  # type: ignore[operator]
    if upper is None:
        return target >= lower
    return lower <= target <= upper


def _sorted_markets(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(row: Dict[str, Any]) -> tuple[int, int]:
        threshold = row.get("group_item_threshold")
        if isinstance(threshold, int):
            return (0, threshold)
        return (1, 0)

    return sorted(markets, key=sort_key)


def _resample_to_anchors(
    snapshots: List[Dict[str, Any]],
    anchors_utc: List[datetime],
    *,
    mode: str = "closest",
) -> List[Dict[str, Any]]:
    if not snapshots:
        return []
    parsed: List[tuple[datetime, Dict[str, Any]]] = []
    for row in snapshots:
        captured_at = row.get("captured_at")
        if not isinstance(captured_at, str):
            continue
        try:
            parsed_dt = _parse_iso(captured_at)
        except ValueError:
            continue
        parsed.append((parsed_dt, row))

    parsed.sort(key=lambda item: item[0])
    times = [item[0] for item in parsed]

    out: List[Dict[str, Any]] = []
    for anchor in anchors_utc:
        chosen: Optional[Dict[str, Any]] = None
        if mode == "carry":
            idx = bisect_right(times, anchor) - 1
            if idx >= 0:
                chosen = parsed[idx][1]
        else:
            idx = bisect_left(times, anchor)
            candidates: List[tuple[datetime, Dict[str, Any]]] = []
            if 0 <= idx < len(parsed):
                candidates.append(parsed[idx])
            if idx - 1 >= 0:
                candidates.append(parsed[idx - 1])
            if candidates:
                chosen = min(
                    candidates, key=lambda item: abs((item[0] - anchor).total_seconds())
                )[1]
        if chosen:
            out.append(chosen)
        else:
            out.append({})
    return out


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


def _build_wu_history_url(local_date: datetime.date) -> str:
    return (
        f"{WU_HISTORY_HOST}/history/daily/kr/incheon/RKSI/date/"
        f"{local_date.year}-{local_date.month}-{local_date.day}"
    )


async def _fetch_wu_history_html(url: str) -> str:
    assert _http_client
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "referer": f"{WU_HISTORY_HOST}/",
    }
    resp = await _http_client.get(url, headers=headers, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _fahrenheit_to_celsius(value_f: float) -> float:
    return (value_f - 32.0) * (5.0 / 9.0)


def _round_half_away_from_zero(value: float) -> int:
    if value >= 0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


async def _fetch_event_for_date(
    sb: SupabaseWriter, *, date_kst: str
) -> Optional[Dict[str, Any]]:
    rows = await sb.select(
        "events",
        select="id,slug,date_kst",
        filters={"date_kst": f"eq.{date_kst}"},
        limit=1,
    )
    return rows[0] if rows else None


async def _fetch_markets_for_event(
    sb: SupabaseWriter, *, event_id: str
) -> List[Dict[str, Any]]:
    return await sb.select(
        "event_markets",
        select=(
            "id,group_item_title,lower_bound_celsius,upper_bound_celsius,"
            "group_item_threshold,yes_token_id,no_token_id"
        ),
        filters={"event_id": f"eq.{event_id}"},
        order="group_item_threshold.asc",
    )


async def _compute_day_high_c(
    sb: SupabaseWriter, *, date_kst: date
) -> Optional[int]:
    day_str = date_kst.isoformat()
    rows = await sb.select(
        "weather_day_high_changes",
        select="high_celsius,observed_at",
        filters={"date_kst": f"eq.{day_str}"},
        order="observed_at.desc",
        limit=1,
    )
    if rows:
        high_val = _coerce_int(rows[0].get("high_celsius"))
        if high_val is not None:
            return high_val

    start_utc = _kst_datetime(date_kst, "00:00").astimezone(timezone.utc)
    end_utc = _kst_datetime(date_kst, "24:00").astimezone(timezone.utc)
    obs_rows = await sb.select(
        "weather_metar_obs",
        select="observed_at,temp_c,source",
        filters={"observed_at": f"gte.{start_utc.isoformat()}"},
        order="observed_at.asc",
    )
    max_temp: Optional[float] = None
    for row in obs_rows:
        observed_at = row.get("observed_at")
        if not isinstance(observed_at, str):
            continue
        try:
            observed_dt = _parse_iso(observed_at)
        except ValueError:
            continue
        if observed_dt >= end_utc:
            break
        temp_c = _coerce_float(row.get("temp_c"))
        if temp_c is None:
            continue
        if max_temp is None or temp_c > max_temp:
            max_temp = temp_c
    if max_temp is None:
        return None
    return _round_half_away_from_zero(max_temp)


async def _fetch_new_high_events(
    sb: SupabaseWriter, *, date_kst: date
) -> List[Dict[str, Any]]:
    day_str = date_kst.isoformat()
    rows = await sb.select(
        "weather_day_high_changes",
        select="observed_at,previous_high_celsius,high_celsius,source",
        filters={"date_kst": f"eq.{day_str}"},
        order="observed_at.asc",
    )
    events: List[Dict[str, Any]] = []
    if rows:
        for row in rows:
            observed_at = row.get("observed_at")
            if not isinstance(observed_at, str):
                continue
            try:
                obs_dt = _parse_iso(observed_at)
            except ValueError:
                continue
            events.append(
                {
                    "observed_at": obs_dt,
                    "observed_kst": _format_kst_hhmm(obs_dt),
                    "previous_high_celsius": _coerce_int(row.get("previous_high_celsius")),
                    "high_celsius": _coerce_int(row.get("high_celsius")),
                    "source": row.get("source"),
                }
            )
        return events

    start_utc = _kst_datetime(date_kst, "00:00").astimezone(timezone.utc)
    end_utc = _kst_datetime(date_kst, "24:00").astimezone(timezone.utc)
    obs_rows = await sb.select(
        "weather_metar_obs",
        select="observed_at,temp_c,source",
        filters={"observed_at": f"gte.{start_utc.isoformat()}"},
        order="observed_at.asc",
    )
    current_high: Optional[int] = None
    for row in obs_rows:
        observed_at = row.get("observed_at")
        if not isinstance(observed_at, str):
            continue
        try:
            obs_dt = _parse_iso(observed_at)
        except ValueError:
            continue
        if obs_dt >= end_utc:
            break
        temp_c = _coerce_float(row.get("temp_c"))
        if temp_c is None:
            continue
        whole_c = _round_half_away_from_zero(temp_c)
        if current_high is None or whole_c > current_high:
            events.append(
                {
                    "observed_at": obs_dt,
                    "observed_kst": _format_kst_hhmm(obs_dt),
                    "previous_high_celsius": current_high,
                    "high_celsius": whole_c,
                    "source": row.get("source"),
                }
            )
            current_high = whole_c
    return events


def _default_market_id_for_high(
    markets: List[Dict[str, Any]], *, high_c: Optional[int]
) -> Optional[str]:
    if high_c is not None:
        for row in markets:
            lower = _coerce_int(row.get("lower_bound_celsius"))
            upper = _coerce_int(row.get("upper_bound_celsius"))
            if _market_matches_bucket(lower, upper, high_c):
                market_id = row.get("id")
                if isinstance(market_id, str):
                    return market_id
    sorted_markets = _sorted_markets(markets)
    if not sorted_markets:
        return None
    mid = sorted_markets[len(sorted_markets) // 2]
    return mid.get("id") if isinstance(mid.get("id"), str) else None


async def _fetch_snapshots_for_market(
    sb: SupabaseWriter,
    *,
    market_id: str,
    start_utc: datetime,
    end_utc: datetime,
) -> List[Dict[str, Any]]:
    rows = await sb.select(
        "market_snapshots",
        select=(
            "captured_at,yes_best_bid,yes_best_ask,no_best_bid,no_best_ask,"
            "yes_bid_size,yes_ask_size,no_bid_size,no_ask_size,accepting_orders"
        ),
        filters={
            "market_id": f"eq.{market_id}",
            "captured_at": f"gte.{start_utc.isoformat()}",
        },
        order="captured_at.asc",
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        captured_at = row.get("captured_at")
        if not isinstance(captured_at, str):
            continue
        try:
            captured_dt = _parse_iso(captured_at)
        except ValueError:
            continue
        if captured_dt > end_utc:
            break
        out.append(row)
    return out


async def _fetch_temp_obs_for_day(
    sb: SupabaseWriter, *, date_kst: date, source: str
) -> List[tuple[datetime, float]]:
    day_start_utc = _kst_datetime(date_kst, "00:00").astimezone(timezone.utc)
    day_end_utc = _kst_datetime(date_kst, "24:00").astimezone(timezone.utc)
    rows = await sb.select(
        "weather_metar_obs",
        select="observed_at,temp_c,source,station",
        filters={
            "station": "eq.RKSI",
            "source": f"eq.{source}",
            "observed_at": f"gte.{day_start_utc.isoformat()}",
        },
        order="observed_at.asc",
    )
    out: List[tuple[datetime, float]] = []
    for row in rows:
        observed_at = row.get("observed_at")
        if not isinstance(observed_at, str):
            continue
        try:
            obs_dt = _parse_iso(observed_at)
        except ValueError:
            continue
        if obs_dt >= day_end_utc:
            break
        temp_c = _coerce_float(row.get("temp_c"))
        if temp_c is None:
            continue
        out.append((obs_dt, temp_c))
    return out


def _temps_for_anchors(
    observations: List[tuple[datetime, float]], anchors_utc: List[datetime]
) -> List[Optional[float]]:
    temps: List[Optional[float]] = []
    idx = 0
    last_temp: Optional[float] = None
    for anchor in anchors_utc:
        while idx < len(observations) and observations[idx][0] <= anchor:
            last_temp = observations[idx][1]
            idx += 1
        temps.append(last_temp)
    return temps


async def _build_temp_series(
    sb: SupabaseWriter, *, date_kst: date, anchors_utc: List[datetime]
) -> tuple[List[Optional[float]], Optional[str]]:
    for source in ("awc",):
        obs = await _fetch_temp_obs_for_day(sb, date_kst=date_kst, source=source)
        if obs:
            return _temps_for_anchors(obs, anchors_utc), source
    return [None for _ in anchors_utc], None


def _html_to_text(payload: str) -> str:
    payload = re.sub(r"(?is)<script.*?</script>", " ", payload)
    payload = re.sub(r"(?is)<style.*?</style>", " ", payload)
    payload = re.sub(r"(?s)<[^>]+>", " ", payload)
    payload = html.unescape(payload)
    payload = re.sub(r"\s+", " ", payload)
    return payload.strip()


def _extract_wu_temp(text: str, *, section: str, label: str) -> Optional[tuple[float, str]]:
    pattern = rf"{section}\s+{label}\s+(-?\d+(?:\.\d+)?)\s*°?\s*([FC])"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    value = _coerce_float(match.group(1))
    if value is None:
        return None
    unit = match.group(2).upper()
    if unit not in ("F", "C"):
        return None
    return value, unit


def _parse_wu_latest_observation(
    text: str, *, local_date: datetime.date
) -> Optional[Dict[str, Any]]:
    idx = text.lower().find("observations")
    scoped = text[idx:] if idx >= 0 else text
    matches = list(
        re.finditer(
            r"(\d{1,2}:\d{2})\s*(AM|PM)?\s+(-?\d+(?:\.\d+)?)\s*°?\s*([FC])",
            scoped,
            flags=re.IGNORECASE,
        )
    )
    if not matches:
        return None

    best_minutes: Optional[int] = None
    best_value: Optional[float] = None
    best_unit: Optional[str] = None
    for match in matches:
        time_part = match.group(1)
        ampm = match.group(2).upper() if match.group(2) else None
        value = _coerce_float(match.group(3))
        unit = match.group(4).upper()
        if value is None or unit not in ("F", "C"):
            continue
        try:
            hour_str, minute_str = time_part.split(":")
            hour = int(hour_str)
            minute = int(minute_str)
        except ValueError:
            continue
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            continue
        minutes = hour * 60 + minute
        if best_minutes is None or minutes > best_minutes:
            best_minutes = minutes
            best_value = value
            best_unit = unit

    if best_minutes is None or best_value is None or best_unit is None:
        return None

    observed_at = datetime.combine(local_date, datetime.min.time()).replace(tzinfo=SEOUL_TZ) + timedelta(
        minutes=best_minutes
    )
    temp_c = best_value if best_unit == "C" else _fahrenheit_to_celsius(best_value)
    return {
        "observedAt": observed_at.isoformat(),
        "tempC": round(temp_c, 2),
        "unit": best_unit,
        "temp": best_value,
    }


def _parse_wu_history(html_payload: str, *, local_date: datetime.date, url: str) -> Optional[Dict[str, Any]]:
    text = _html_to_text(html_payload)
    high = _extract_wu_temp(text, section="Temperature", label="High")
    low = _extract_wu_temp(text, section="Temperature", label="Low")
    if high is None or low is None:
        high = high or _extract_wu_temp(text, section="Daily Summary", label="High")
        low = low or _extract_wu_temp(text, section="Daily Summary", label="Low")

    day_high_c = None
    if high is not None:
        value, unit = high
        day_high_c = value if unit == "C" else _fahrenheit_to_celsius(value)

    day_low_c = None
    if low is not None:
        value, unit = low
        day_low_c = value if unit == "C" else _fahrenheit_to_celsius(value)

    latest = _parse_wu_latest_observation(text, local_date=local_date)

    payload: Dict[str, Any] = {
        "source": "wunderground_history",
        "url": url,
        "dateKst": local_date.isoformat(),
        "dayHighC": round(day_high_c, 2) if isinstance(day_high_c, (int, float)) else None,
        "dayLowC": round(day_low_c, 2) if isinstance(day_low_c, (int, float)) else None,
    }
    if isinstance(day_high_c, (int, float)):
        payload["dayHighCelsiusWhole"] = _round_half_away_from_zero(day_high_c)
    if isinstance(day_low_c, (int, float)):
        payload["dayLowCelsiusWhole"] = _round_half_away_from_zero(day_low_c)
    if latest:
        payload["current"] = latest
    return payload


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


def _latest_observed_at_for_date(
    metar: List[Dict[str, Any]], local_date: datetime.date, now_kst: datetime
) -> Optional[str]:
    latest_dt: Optional[datetime] = None
    for obs in metar:
        report_time = obs.get("reportTime") or obs.get("receiptTime")
        temp = obs.get("temp")
        if not isinstance(report_time, str) or temp is None:
            continue
        try:
            dt_utc = _parse_iso(report_time)
        except ValueError:
            continue
        dt_local = dt_utc.astimezone(SEOUL_TZ).replace(second=0, microsecond=0)
        if dt_local.date() != local_date or dt_local > now_kst:
            continue
        if latest_dt is None or dt_local > latest_dt:
            latest_dt = dt_local
    return latest_dt.isoformat() if latest_dt else None


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


async def _persist_wunderground_to_supabase(
    *,
    captured_at: datetime,
    local_date: datetime.date,
    wu_history: Optional[Dict[str, Any]],
) -> None:
    if not _supabase or not _supabase.enabled() or not isinstance(wu_history, dict):
        return

    current = wu_history.get("current")
    if isinstance(current, dict):
        observed_at = current.get("observedAt")
        temp_c = _coerce_float(current.get("tempC"))
        if isinstance(observed_at, str) and temp_c is not None:
            await _supabase.upsert(
                "weather_wu_obs",
                {
                    "station": "RKSI",
                    "observed_at": observed_at,
                    "temp_c": temp_c,
                    "source_url": wu_history.get("url"),
                    "raw": wu_history,
                },
                on_conflict="station,observed_at",
            )

    high_whole = _coerce_int(wu_history.get("dayHighCelsiusWhole"))
    if high_whole is not None:
        await _supabase.upsert(
            "weather_day_high_changes",
            {
                "station": "RKSI",
                "source": "wunderground",
                "date_kst": local_date.isoformat(),
                "observed_at": captured_at.astimezone(timezone.utc).isoformat(),
                "previous_high_celsius": None,
                "high_celsius": high_whole,
            },
            on_conflict="station,source,date_kst,high_celsius",
        )


async def _ensure_wu_observed_max_loaded(local_date: datetime.date) -> None:
    key = local_date.isoformat()
    if key in _wu_observed_max_loaded:
        return
    _wu_observed_max_loaded.add(key)
    if not _supabase or not _supabase.enabled():
        return
    rows = await _supabase.select(
        "weather_day_high_changes",
        select="high_celsius",
        filters={"source": "eq.wunderground_observed", "date_kst": f"eq.{key}"},
        order="high_celsius.desc",
        limit=1,
    )
    if rows:
        high = _coerce_int(rows[0].get("high_celsius"))
        if high is not None:
            _wu_observed_max_by_date[key] = float(high)
            _wu_observed_max_whole_by_date[key] = high


async def _update_wu_observed_running_max(
    *,
    local_date: datetime.date,
    wu_history: Optional[Dict[str, Any]],
) -> None:
    if not isinstance(wu_history, dict):
        return
    current = wu_history.get("current")
    if not isinstance(current, dict):
        return
    temp_c = _coerce_float(current.get("tempC"))
    observed_at = current.get("observedAt")
    if temp_c is None or not isinstance(observed_at, str):
        return

    await _ensure_wu_observed_max_loaded(local_date)

    key = local_date.isoformat()
    prev_max = _wu_observed_max_by_date.get(key)
    if prev_max is None or temp_c > prev_max + 1e-6:
        prev_whole = _wu_observed_max_whole_by_date.get(key)
        new_whole = _round_half_away_from_zero(temp_c)
        _wu_observed_max_by_date[key] = temp_c
        _wu_observed_max_whole_by_date[key] = new_whole
        if _supabase and _supabase.enabled():
            await _supabase.upsert(
                "weather_day_high_changes",
                {
                    "station": "RKSI",
                    "source": "wunderground_observed",
                    "date_kst": key,
                    "observed_at": observed_at,
                    "previous_high_celsius": prev_whole,
                    "high_celsius": new_whole,
                },
                on_conflict="station,source,date_kst,high_celsius",
            )


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
                forecast=None,
                forecast_cache_ts=None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("awc", exc)

        await _sleep_remaining(started, AWC_TTL)


async def _ingest_wunderground_loop() -> None:
    global _latest_wu_history
    while True:
        started = time.time()
        try:
            now_kst = _now_kst()
            local_date = now_kst.date()
            url = _build_wu_history_url(local_date)
            captured_at = datetime.now(timezone.utc)
            html_payload = await _fetch_wu_history_html(url)
            parsed = _parse_wu_history(html_payload, local_date=local_date, url=url)
            async with _state_lock:
                _latest_wu_history = parsed
            await _mark_health_success("wunderground")
            await _update_wu_observed_running_max(
                local_date=local_date, wu_history=parsed
            )
            await _persist_wunderground_to_supabase(
                captured_at=captured_at, local_date=local_date, wu_history=parsed
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _mark_health_error("wunderground", exc)

        await _sleep_remaining(started, WU_TTL)


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
        wu_history = _latest_wu_history
        forecast = _latest_forecast
        balance = _latest_balance
        positions = _latest_positions
        health = {key: dict(val) for key, val in _health.items()}

    await _ensure_wu_observed_max_loaded(local_date)
    awc_actuals = _actuals_for_date(metar_awc, local_date, now_kst)
    axis_times = _format_axis_times(local_date)

    awc_latest_idx = _latest_hour_index(awc_actuals["hourly"])
    awc_latest = (
        awc_actuals["hourly"][awc_latest_idx]
        if awc_latest_idx is not None
        else None
    )
    awc_latest_time = (
        axis_times[awc_latest_idx] if awc_latest_idx is not None else None
    )

    awc_latest_observed_at = _latest_observed_at_for_date(metar_awc, local_date, now_kst)

    wu_current_temp = None
    if isinstance(wu_history, dict):
        current = wu_history.get("current")
        if isinstance(current, dict):
            wu_current_temp = _coerce_float(current.get("tempC"))

    wu_key = local_date.isoformat()
    wu_observed_max = _wu_observed_max_by_date.get(wu_key)
    wu_observed_max_whole = _wu_observed_max_whole_by_date.get(wu_key)

    wu_day_high = _coerce_float(wu_history.get("dayHighC")) if isinstance(wu_history, dict) else None
    wu_day_high_whole = (
        _coerce_int(wu_history.get("dayHighCelsiusWhole")) if isinstance(wu_history, dict) else None
    )
    day_high = (
        wu_day_high
        if wu_day_high is not None
        else wu_observed_max
        if wu_observed_max is not None
        else _max_optional([awc_actuals["day_high"]])
    )
    day_high_whole = (
        wu_day_high_whole
        if wu_day_high_whole is not None
        else wu_observed_max_whole
        if wu_observed_max_whole is not None
        else _round_half_away_from_zero(day_high)
        if isinstance(day_high, (int, float))
        else None
    )

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
            },
            "dayHigh": day_high,
            "dayHighCelsiusWhole": day_high_whole,
            "wunderground": (
                {
                    **wu_history,
                    "observedMaxC": round(wu_observed_max, 2)
                    if isinstance(wu_observed_max, (int, float))
                    else None,
                    "observedMaxCelsiusWhole": wu_observed_max_whole,
                }
                if isinstance(wu_history, dict)
                else {
                    "source": "wunderground_history",
                    "observedMaxC": round(wu_observed_max, 2)
                    if isinstance(wu_observed_max, (int, float))
                    else None,
                    "observedMaxCelsiusWhole": wu_observed_max_whole,
                }
            ),
            "sources": {
                "awc": {
                    "latest": awc_latest,
                    "latestTime": awc_latest_time,
                    "latestObservedAt": awc_latest_observed_at,
                    "dayHigh": awc_actuals["day_high"],
                    "deltaVsWunderground": (
                        round(awc_actuals["day_high"] - (wu_day_high or wu_observed_max), 2)
                        if isinstance(awc_actuals["day_high"], (int, float))
                        and isinstance((wu_day_high or wu_observed_max), (int, float))
                        else None
                    ),
                    "latestDeltaVsWunderground": (
                        round(awc_latest - wu_current_temp, 2)
                        if isinstance(awc_latest, (int, float))
                        and isinstance(wu_current_temp, (int, float))
                        else None
                    ),
                },
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


@app.get("/api/trends/dates")
async def trend_dates() -> Dict[str, Any]:
    sb = _require_supabase()
    rows = await sb.select("events", select="date_kst", order="date_kst.desc")
    seen: set[str] = set()
    dates: List[str] = []
    for row in rows:
        value = row.get("date_kst")
        if isinstance(value, str) and value not in seen:
            seen.add(value)
            dates.append(value)
    return {"dates": dates}


@app.get("/api/trends/markets")
async def trend_markets(
    date_kst: str = Query(..., description="KST date (YYYY-MM-DD)")
) -> Dict[str, Any]:
    sb = _require_supabase()
    day = _parse_date_kst(date_kst)
    event = await _fetch_event_for_date(sb, date_kst=date_kst)
    if not event:
        raise HTTPException(status_code=404, detail="No event for date")
    event_id = event.get("id")
    if not isinstance(event_id, str):
        raise HTTPException(status_code=500, detail="Event id missing")
    markets = await _fetch_markets_for_event(sb, event_id=event_id)
    day_high_c = await _compute_day_high_c(sb, date_kst=day)
    default_market_id = _default_market_id_for_high(markets, high_c=day_high_c)
    return {
        "date_kst": date_kst,
        "slug": event.get("slug"),
        "event_id": event_id,
        "day_high_c": day_high_c,
        "default_market_id": default_market_id,
        "markets": markets,
    }


@app.get("/api/trends")
async def trend_series(
    date_kst: str = Query(..., description="KST date (YYYY-MM-DD)"),
    market_id: Optional[str] = Query(None),
    start_kst: str = Query("00:00"),
    end_kst: str = Query("24:00"),
    interval_minutes: int = Query(15, ge=1, le=240),
    mode: str = Query("closest", pattern="^(closest|carry)$"),
) -> Dict[str, Any]:
    sb = _require_supabase()
    day = _parse_date_kst(date_kst)
    event = await _fetch_event_for_date(sb, date_kst=date_kst)
    if not event:
        raise HTTPException(status_code=404, detail="No event for date")
    event_id = event.get("id")
    if not isinstance(event_id, str):
        raise HTTPException(status_code=500, detail="Event id missing")

    markets = await _fetch_markets_for_event(sb, event_id=event_id)
    if not market_id:
        day_high_c = await _compute_day_high_c(sb, date_kst=day)
        market_id = _default_market_id_for_high(markets, high_c=day_high_c)
    if not market_id:
        raise HTTPException(status_code=404, detail="Market not found")

    start_dt_kst = _kst_datetime(day, start_kst)
    end_dt_kst = _kst_datetime(day, end_kst)
    if end_dt_kst <= start_dt_kst:
        raise HTTPException(
            status_code=400, detail="end_kst must be after start_kst"
        )
    start_utc = start_dt_kst.astimezone(timezone.utc)
    end_utc = end_dt_kst.astimezone(timezone.utc)

    snapshots = await _fetch_snapshots_for_market(
        sb, market_id=market_id, start_utc=start_utc, end_utc=end_utc
    )
    anchors_kst: List[datetime] = []
    anchors_utc: List[datetime] = []
    current = start_dt_kst
    while current <= end_dt_kst:
        anchors_kst.append(current)
        anchors_utc.append(current.astimezone(timezone.utc))
        current += timedelta(minutes=interval_minutes)

    temps, temp_source = await _build_temp_series(
        sb, date_kst=day, anchors_utc=anchors_utc
    )

    resampled = _resample_to_anchors(snapshots, anchors_utc, mode=mode)
    series: List[Dict[str, Any]] = []
    missing = 0
    for anchor_kst_dt, anchor_utc_dt, row, temp_c in zip(
        anchors_kst, anchors_utc, resampled, temps
    ):
        captured_at = row.get("captured_at") if row else None
        if not captured_at:
            missing += 1
        series.append(
            {
                "anchor_kst": anchor_kst_dt.strftime("%H:%M"),
                "anchor_utc": anchor_utc_dt.isoformat(),
                "captured_at": captured_at,
                "temp_c": temp_c,
                "yes_best_bid": row.get("yes_best_bid"),
                "yes_best_ask": row.get("yes_best_ask"),
                "no_best_bid": row.get("no_best_bid"),
                "no_best_ask": row.get("no_best_ask"),
                "yes_bid_size": row.get("yes_bid_size"),
                "yes_ask_size": row.get("yes_ask_size"),
                "no_bid_size": row.get("no_bid_size"),
                "no_ask_size": row.get("no_ask_size"),
                "accepting_orders": row.get("accepting_orders"),
            }
        )

    market_label = None
    for row in markets:
        if row.get("id") == market_id:
            market_label = row.get("group_item_title")
            break

    coverage = {
        "snapshots": len(snapshots),
        "first_snapshot": snapshots[0].get("captured_at") if snapshots else None,
        "last_snapshot": snapshots[-1].get("captured_at") if snapshots else None,
        "resampled_points": len(series),
        "missing_points": missing,
    }

    return {
        "meta": {
            "date_kst": date_kst,
            "slug": event.get("slug"),
            "event_id": event_id,
            "market_id": market_id,
            "market_label": market_label,
            "temp_source": temp_source,
            "timezone": "Asia/Seoul",
            "interval_minutes": interval_minutes,
            "start_kst": start_kst,
            "end_kst": end_kst,
            "mode": mode,
        },
        "coverage": coverage,
        "series": series,
    }


@app.get("/api/new-highs")
async def new_highs(
    date_kst: str = Query(..., description="KST date (YYYY-MM-DD)")
) -> Dict[str, Any]:
    sb = _require_supabase()
    day = _parse_date_kst(date_kst)
    events = await _fetch_new_high_events(sb, date_kst=day)
    payload = [
        {
            "observed_at": item["observed_at"].isoformat(),
            "observed_kst": item["observed_kst"],
            "previous_high_celsius": item["previous_high_celsius"],
            "high_celsius": item["high_celsius"],
            "source": item.get("source"),
        }
        for item in events
    ]
    return {"date_kst": date_kst, "events": payload}


@app.get("/api/event-study")
async def event_study(
    date_kst: str = Query(..., description="KST date (YYYY-MM-DD)"),
    high_c: Optional[int] = Query(None),
    pre_minutes: int = Query(60, ge=1, le=720),
    post_minutes: int = Query(120, ge=1, le=720),
    interval_minutes: int = Query(5, ge=1, le=120),
    markets: str = Query("prev,new"),
    mode: str = Query("closest", pattern="^(closest|carry)$"),
) -> Dict[str, Any]:
    sb = _require_supabase()
    day = _parse_date_kst(date_kst)
    event = await _fetch_event_for_date(sb, date_kst=date_kst)
    if not event:
        raise HTTPException(status_code=404, detail="No event for date")
    event_id = event.get("id")
    if not isinstance(event_id, str):
        raise HTTPException(status_code=500, detail="Event id missing")
    markets_all = await _fetch_markets_for_event(sb, event_id=event_id)

    new_high_events = await _fetch_new_high_events(sb, date_kst=day)
    if not new_high_events:
        return {"meta": {"date_kst": date_kst}, "event": None, "series": []}

    selected_event = None
    if high_c is not None:
        for item in new_high_events:
            if item.get("high_celsius") == high_c:
                selected_event = item
                break
    if selected_event is None:
        selected_event = new_high_events[-1]
    selected_high = selected_event.get("high_celsius")
    observed_at = selected_event.get("observed_at")
    if not isinstance(selected_high, int) or not isinstance(observed_at, datetime):
        raise HTTPException(status_code=500, detail="Invalid new-high event")

    tokens = [token.strip() for token in markets.split(",") if token.strip()]
    market_ids: List[str] = []
    for token in tokens:
        if token == "prev":
            prev_id = _default_market_id_for_high(markets_all, high_c=selected_high - 1)
            if prev_id:
                market_ids.append(prev_id)
        elif token == "new":
            new_id = _default_market_id_for_high(markets_all, high_c=selected_high)
            if new_id:
                market_ids.append(new_id)
        else:
            for row in markets_all:
                if row.get("id") == token:
                    market_ids.append(token)
                    break

    # de-duplicate while preserving order
    seen_ids: set[str] = set()
    market_ids = [mid for mid in market_ids if not (mid in seen_ids or seen_ids.add(mid))]

    start_utc = observed_at - timedelta(minutes=pre_minutes)
    end_utc = observed_at + timedelta(minutes=post_minutes)
    anchors_utc: List[datetime] = []
    offsets: List[int] = []
    current = -pre_minutes
    while current <= post_minutes:
        offsets.append(current)
        anchors_utc.append(observed_at + timedelta(minutes=current))
        current += interval_minutes

    series_by_market: List[Dict[str, Any]] = []
    for market_id in market_ids:
        snapshots = await _fetch_snapshots_for_market(
            sb, market_id=market_id, start_utc=start_utc, end_utc=end_utc
        )
        resampled = _resample_to_anchors(snapshots, anchors_utc, mode=mode)
        points: List[Dict[str, Any]] = []
        for offset, anchor_utc, row in zip(offsets, anchors_utc, resampled):
            captured_at = row.get("captured_at") if row else None
            points.append(
                {
                    "offset_minutes": offset,
                    "anchor_utc": anchor_utc.isoformat(),
                    "anchor_kst": _format_kst_hhmm(anchor_utc),
                    "captured_at": captured_at,
                    "yes_best_bid": row.get("yes_best_bid"),
                    "yes_best_ask": row.get("yes_best_ask"),
                    "no_best_bid": row.get("no_best_bid"),
                    "no_best_ask": row.get("no_best_ask"),
                    "yes_bid_size": row.get("yes_bid_size"),
                    "yes_ask_size": row.get("yes_ask_size"),
                    "no_bid_size": row.get("no_bid_size"),
                    "no_ask_size": row.get("no_ask_size"),
                    "accepting_orders": row.get("accepting_orders"),
                }
            )
        label = None
        for row in markets_all:
            if row.get("id") == market_id:
                label = row.get("group_item_title")
                break
        series_by_market.append(
            {
                "market_id": market_id,
                "market_label": label,
                "points": points,
            }
        )

    return {
        "meta": {
            "date_kst": date_kst,
            "event_id": event_id,
            "slug": event.get("slug"),
            "high_celsius": selected_high,
            "observed_at": observed_at.isoformat(),
            "observed_kst": _format_kst_hhmm(observed_at),
            "pre_minutes": pre_minutes,
            "post_minutes": post_minutes,
            "interval_minutes": interval_minutes,
            "mode": mode,
        },
        "event": {
            "previous_high_celsius": selected_event.get("previous_high_celsius"),
            "high_celsius": selected_high,
            "observed_at": observed_at.isoformat(),
            "observed_kst": _format_kst_hhmm(observed_at),
        },
        "series": series_by_market,
    }
