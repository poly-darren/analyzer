# Visualization Spec: Market Trend Explorer

Goal: a read-only page to explore **historical market microstructure** for Seoul daily-high events using the existing Supabase tables (`events`, `event_markets`, `market_snapshots`, `weather_metar_obs`).

## 1) Filters (what + why)

### 1.1 Date (required)
- **UI:** dropdown (or combo box) containing **only `events.date_kst` values that exist in DB**.
- **Default:** latest `date_kst` available.
- **Why:** avoids empty states; keeps analysis grounded in captured telemetry.

Honest note: I’d keep this “only existing dates” constraint, but also add a small “coverage” hint (e.g. `785 snapshots`, last snapshot time) so you can tell if a day is partial.

### 1.2 Time range (optional; defaults to full day)
- **UI:** start/end time pickers (KST), plus shortcuts:
  - `Full day` → `00:00–24:00`
  - `12–16` → `12:00–16:00` (your primary shortcut)
- **Default:** `00:00–24:00` in **Asia/Seoul**.
- **Why:** lets you zoom into the “decision window” while keeping full-day context available.

Honest note: keep the time range in **KST** everywhere (labels/tooltips) and show the UTC conversion in a small helper line to avoid confusion.

### 1.3 Interval / resample frequency
- **UI:** dropdown: `1m`, `5m`, `15m`, `30m`, `60m` (extensible).
- **Default:** `15m`.
- **Semantics:** “closest snapshot to each anchor” (or “last observation carried forward”); choose one and label it.
- **Why:** controls noise vs. readability and performance.

Honest note: interval selection is essential, but I’d *constrain it based on availability* (e.g., if a day is sparse, default to `30m` and warn).

### 1.4 Market selection
- **UI:** dropdown of markets for that date’s event (from `event_markets`, ordered by `group_item_threshold`), showing:
  - bucket label (e.g. `-2°C`)
  - token ids (optional, in tooltip)
- **Default:** “market for the highest temperature of the day”.

Honest note (important): “highest temp of the day” is not knowable *intraday* unless you define it as **day-high-so-far**. I recommend:
- For **past dates**: default to **final observed day high** from your stored weather (max integer °C over the KST day).
- For **today**: default to **day-high-so-far** (same logic, computed up to now).
- Always allow manual override to any bucket (because as a trader you’ll often analyze *below-high* buckets losing liquidity, not just the winning one).

**Proposed default market algorithm**
1. Compute `day_high_c`:
   - `max(high_celsius)` from `weather_day_high_changes` for that `date_kst` (across sources), else
   - `floor(max(temp_c))` from `weather_metar_obs` within that KST day, else
   - fallback to the event’s “center” bucket (median `group_item_threshold`).
2. Pick the market whose bounds contain `day_high_c`:
   - exact bucket: `lower_bound_celsius = upper_bound_celsius = day_high_c`
   - else edge buckets (“or below/above”) if applicable.

## 2) Page layout (wireframe)

Single page route (e.g. `/trends`), minimal + data-science friendly.

```text
┌───────────────────────────────────────────────────────────────────────────┐
│ Seoul Daily High (RKSI) • Market Trend Explorer                            │
│ Selected date: 2026-01-29 (KST) • Event slug: highest-temperature-in-seoul… │
└───────────────────────────────────────────────────────────────────────────┘

┌─ Filters ──────────────────────────────────────────────────────────────────┐
│ Date [ 2026-01-29 ▼ ]  Market [ Day-high bucket (auto) ▼ ]                 │
│ Time range [00:00] to [24:00]  Shortcuts: [Full day] [12–16]               │
│ Interval [15m ▼]  Price view: [YES bid/ask] (toggle: YES | NO | BOTH)      │
│ Coverage: 785 snapshots • last: 17:41 KST • resampled points: 97           │
└───────────────────────────────────────────────────────────────────────────┘

┌─ Daily summary ────────────────────────────────────────────────────────────┐
│ Start 53.0¢  End 0.1¢  Net -52.9¢  High 95.0¢  Low 0.1¢  Avg spread 4.2¢   │
│ Liquidity (median top size): YES ask 12.0  YES bid 40.0  (NO mirrored)     │
└───────────────────────────────────────────────────────────────────────────┘

┌─ Chart ───────────────────────────────────────────────────────────────────┐
│  Price (¢)                                                                 │
│   100 ┤                    ┌─────── YES ask                                │
│    80 ┤          ┌─────────┘                                               │
│    60 ┤ ┌────────┘  (optional shaded bid/ask spread)                       │
│    40 ┤┌┘                                                                   │
│    20 ┤                                                                    │
│     0 ┼───────────────────────────────────────────────────────────────────│
│       00:00        06:00        12:00        18:00        24:00 (KST)      │
│       (time axis tick colors reflect actual temperature)                   │
└───────────────────────────────────────────────────────────────────────────┘

┌─ Table ───────────────────────────────────────────────────────────────────┐
│ KST time | YES bid | YES ask | NO bid | NO ask | sizes… | acceptingOrders  │
│ 12:00    | 49.0    | 53.0    | 47.0   | 51.0   | …      | true             │
│ 12:15    | 48.0    | 54.0    | 46.0   | 52.0   | …      | true             │
│ …                                                                          │
│ [Export CSV] [Copy query]                                                  │
└───────────────────────────────────────────────────────────────────────────┘
```

## 3) “Trend for every day” (multi-day view)

To make “every day” analysis fast, add a secondary panel (or separate tab) that does **daily aggregation**:
- Rows = `date_kst`
- Columns = start/end/net, high/low, volatility proxy, % missing points, “time of max move”
- Click a row to load that day into the single-day explorer.

Optional, high-signal visualization:
- **Heatmap**: x = time-of-day (KST, 15m), y = `date_kst`, color = chosen price (e.g., YES ask). Great for pattern spotting.

## 4) Data contract (backend API suggestion)

Keep the frontend simple: backend returns a ready-to-plot time series.

Example:
- `GET /api/trends?date_kst=2026-01-29&market_id=...&start_kst=00:00&end_kst=24:00&interval_minutes=15&mode=closest`

Response shape:
```json
{
  "meta": {
    "date_kst": "2026-01-29",
    "slug": "highest-temperature-in-seoul-on-january-29",
    "market_id": "…",
    "market_label": "-2°C",
    "timezone": "Asia/Seoul",
    "interval_minutes": 15,
    "start_kst": "00:00",
    "end_kst": "24:00",
    "mode": "closest"
  },
  "series": [
    {
      "anchor_kst": "12:00",
      "captured_at": "2026-01-29T03:00:23.278770Z",
      "yes_best_bid": 0.49,
      "yes_best_ask": 0.53,
      "no_best_bid": 0.47,
      "no_best_ask": 0.51,
      "yes_bid_size": 53.21,
      "yes_ask_size": 19.0,
      "no_bid_size": 19.0,
      "no_ask_size": 53.21,
      "accepting_orders": true
    }
  ],
  "daily": {
    "start_yes_ask": 0.53,
    "end_yes_ask": 0.001,
    "min_yes_ask": 0.001,
    "max_yes_ask": 0.95
  }
}
```

## 5) My honest take on your filter set

I agree with all four filters; they’re exactly what you want for a “trader + analyst” workflow.

If I could add just one thing: make the **Market default** explicitly “day-high bucket (auto)” and define whether it means *final* (historical) vs *so-far* (today), because that choice materially changes what the chart “means”.
