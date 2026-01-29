# Tasks (PolyWeather)

This file tracks implementation work derived from `specs/v2.md`.

Related docs:
- `specs/supabase.md` (Supabase project + schema setup)

## P0 — Persistence + Ingestion (v2 scope)
- [x] Add Supabase writer (PostgREST via `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY`, or direct Postgres via `SUPABASE_DB_URL`) gated by `SUPABASE_WRITE_ENABLED`.
- [x] Add schema migration SQL (tables + indexes) and apply in Supabase.
- [ ] Implement telemetry writes (idempotent):
  - [x] Upsert `events` (slug/date_kst/first_seen_at/last_seen_at/raw Gamma payload).
  - [x] Upsert `event_markets` (gamma ids, `groupItemTitle`/`groupItemThreshold`, token ids, parsed bounds, raw).
  - [x] Insert `market_snapshots` every 30s per market (top-of-book bid/ask + sizes + `accepting_orders` + `source` + raw payloads).
  - [x] Insert `weather_metar_obs` only when a *new* observation timestamp appears (unique `(station, source, observed_at)`); set `ingested_at` for latency measurement.
  - [ ] Insert `weather_day_high_changes` when the day-high integer °C changes (per source + combined).
- [ ] Persist Open‑Meteo KMA runs:
  - [x] Insert `forecast_runs` for each fetch (model + run_at + raw).
  - [x] Insert `forecast_hourly` (valid_at + temp_c) and link to `forecast_runs`.
- [ ] Add AviationWeather TAF ingestion:
  - [ ] Fetch `https://aviationweather.gov/api/data/taf?ids=RKSI&format=json`.
  - [ ] Store raw TAF (and optionally parsed TX/TN) in DB (either a dedicated table or as a `forecast_runs` “taf” model).

## P1 — Decouple UI from polling
- [x] Add background pollers (per cadence) that update an in-memory “latest state”.
- [x] Make `/api/dashboard` serve the latest aggregated state without forcing upstream fetches on every request.
- [x] Add per-upstream health in payload (last success time + last error).

## P2 — UI upgrades (still no trading actions)
- [ ] Plot Open‑Meteo hourly forecast on the chart (separate series) and show predicted daily max + time.
- [ ] Show market microstructure: YES/NO best bid/ask + top sizes + `acceptingOrders`.
- [ ] Add “data freshness” indicators (age of latest METAR, age of latest market snapshot).

## P3 — Analytics (decision support)
- [ ] Supabase views/queries for:
  - [ ] Model error by lead time (daily max and hourly).
  - [ ] Bin hit-rate / calibration (probability → realized frequency).
  - [ ] Polymarket reaction latency to new highs (liquidity + price thresholds).
- [ ] Retention/downsampling job for `market_snapshots` (e.g., 5m/15m aggregates after 30–90 days).

## P4 — Automation (future, outside v2 non-goals)
- [ ] Event discovery for tomorrow’s slug; record first-seen time + initial pricing snapshot.
- [ ] Strategy engine: convert forecast distribution → target bin orders (risk-limited).
- [ ] Execution layer: place/cancel orders with idempotency + full audit trail in DB.
- [ ] Backtest harness powered by stored snapshots/actuals.
