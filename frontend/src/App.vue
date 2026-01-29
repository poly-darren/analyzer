<template>
  <div class="app">
    <header class="header">
      <div class="title">
        <p class="eyebrow">Seoul Daily High (RKSI)</p>
        <h1>
          {{
            isTrends
              ? "Market Trend Explorer"
              : dashboard?.market?.eventTitle || "Highest Temperature in Seoul"
          }}
        </h1>
        <p class="subtitle">
          {{
            isTrends
              ? "Historical price action and new-high reaction analysis."
              : `Slug: ${dashboard?.meta?.slug || "—"}`
          }}
        </p>
      </div>
      <div class="header-actions">
        <button
          class="ghost-button"
          :class="{ active: !isTrends }"
          type="button"
          @click="navigate('/')"
        >
          Dashboard
        </button>
        <button
          class="ghost-button"
          :class="{ active: isTrends }"
          type="button"
          @click="navigate('/trends')"
        >
          Trends
        </button>
      </div>
      <div v-if="!isTrends" class="metrics">
        <div class="metric">
          <span
            >Today (KST)
            <span
              class="info-tip"
              data-tip="The KST date used to build today’s market slug."
              >i</span
            ></span
          >
          <strong>{{ dashboard?.meta?.kstDate || "—" }}</strong>
        </div>
        <div class="metric">
          <span
            >Day High So Far
            <span
              class="info-tip"
              data-tip="Settlement-canonical day-high from Wunderground RKSI daily history (the same source Polymarket uses to resolve)."
              >i</span
            ></span
          >
          <strong>{{ formatTemp(dashboard?.weather?.dayHigh) }}</strong>
        </div>
        <div class="metric">
          <span
            >Bucket Now (rounded)
            <span
              class="info-tip"
              data-tip="Rounded whole °C of day-high so far — which outcome would win if the day ended now."
              >i</span
            ></span
          >
          <strong>{{ formatBucket(bucketNow) }}</strong>
        </div>
        <div class="metric">
          <span
            >Time Left (KST)
            <span
              class="info-tip"
              data-tip="Minutes until the KST day rolls over (00:00)."
              >i</span
            ></span
          >
          <strong>{{ formatDuration(minutesLeftInDay) }}</strong>
        </div>
        <div class="metric">
          <span
            >Last Refresh (KST)
            <span
              class="info-tip"
              data-tip="Timestamp of the latest dashboard payload from the backend."
              >i</span
            ></span
          >
          <strong>{{ formatTime(dashboard?.meta?.lastRefresh) }}</strong>
        </div>
      </div>
    </header>

    <main v-if="!isTrends" class="grid">
      <section class="panel weather-panel">
        <div class="panel-header">
          <div>
            <h2>Weather (30‑min)</h2>
            <p>Primary: Wunderground daily history (settlement) · Secondary: METAR sources</p>
          </div>
          <div class="legend">
            <span><span class="dot wu"></span> WU</span>
            <span><span class="dot actual"></span> AWC</span>
          </div>
        </div>
        <div class="submetrics">
          <div class="submetric submetric--primary">
            <span class="label"
              ><span class="dot wu"></span> Wunderground (settlement)
              <span
                class="info-tip"
                data-tip="Scraped from Wunderground RKSI daily history page. Use this for bucket/settlement; current is the latest observation shown on the history page."
                >i</span
              ></span
            >
            <strong>{{ formatTemp(dashboard?.weather?.wunderground?.current?.tempC) }}</strong>
            <span class="meta">
              {{ formatTime(dashboard?.weather?.wunderground?.current?.observedAt) }}
              · {{ formatAge(wuStaleMinutes) }}
              <span v-if="dashboard?.weather?.wunderground?.observedMaxC != null">
                · Obs max {{ formatTemp(dashboard?.weather?.wunderground?.observedMaxC) }}
              </span>
              <span v-if="dashboard?.weather?.wunderground?.dayLowC != null">
                · Low {{ formatTemp(dashboard?.weather?.wunderground?.dayLowC) }}
              </span>
            </span>
          </div>
          <div class="submetric">
            <span class="label"
              ><span class="dot actual"></span> AWC latest
              <span
                class="info-tip"
                data-tip="Latest METAR temperature from Aviation Weather Center; age is minutes since the observation time."
                >i</span
              ></span
            >
            <strong>{{ formatTemp(dashboard?.weather?.sources?.awc?.latest) }}</strong>
            <span class="meta">
              {{ formatTime(dashboard?.weather?.sources?.awc?.latestObservedAt) }}
              · {{ formatAge(awcStaleMinutes) }}
              <span v-if="dashboard?.weather?.sources?.awc?.latestDeltaVsWunderground != null">
                · Δ vs WU {{ formatDelta(dashboard?.weather?.sources?.awc?.latestDeltaVsWunderground) }}
              </span>
            </span>
          </div>
        </div>
        <div class="chart-container">
          <Line v-if="chartData" :data="chartData" :options="chartOptions" />
          <div v-else class="loading-chart">Loading chart data...</div>
        </div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2>Market Prices</h2>
            <p>Polymarket CLOB (Yes/No prices).</p>
          </div>
          <div class="panel-actions">
            <button
              class="ghost-button ghost-button--sm"
              :class="{ active: showLockedOutcomes }"
              type="button"
              @click="showLockedOutcomes = !showLockedOutcomes"
            >
              {{ showLockedOutcomes ? "Hide locked" : "Show locked" }}
            </button>
            <span class="badge" :class="dashboard?.meta?.eventFound ? 'ok' : 'warn'">
              {{ dashboard?.meta?.eventFound ? "Event Found" : "Event Missing" }}
            </span>
          </div>
        </div>
        <div class="summary-grid">
          <div class="summary-card">
            <span
              >Market Implied μ
              <span
                class="info-tip"
                data-tip="Heuristic mean temperature from normalized YES mid prices across buckets (uses bucket midpoints; ignores vig and missing liquidity). Σmid is the sum of mids used for normalization (ideally ~1 with complete data)."
                >i</span
              ></span
            >
            <strong>{{ formatTemp(marketImplied.mean) }}</strong>
            <span class="summary-sub">
              Top: {{ marketImplied.topTitle || "—" }}
              <span v-if="marketImplied.topProb !== null">({{ formatProb(marketImplied.topProb) }})</span>
              · Σmid {{ formatPrice(marketImplied.sum) }}
            </span>
          </div>
          <div class="summary-card">
            <span
              >Forecast Max (today)
              <span
                class="info-tip"
                data-tip="Max hourly temperature forecast for today’s KST date; range is min–max across forecast models."
                >i</span
              ></span
            >
            <strong>{{ formatTemp(forecastSummary.defaultMax) }}</strong>
            <span class="summary-sub">
              {{ forecastSummary.defaultModel || "—" }}
              <span v-if="forecastSummary.defaultTime"> @ {{ forecastSummary.defaultTime }}</span>
              <span v-if="forecastSummary.rangeText"> · Range {{ forecastSummary.rangeText }}</span>
            </span>
          </div>
          <div class="summary-card summary-card--wide">
            <span
              >Now Bucket
              <span
                class="info-tip"
                data-tip="The outcome matching the rounded day-high so far; Δ shows how much warming would flip to the next bucket (bucket+0.5°C)."
                >i</span
              ></span
            >
            <strong>{{ currentBucketTitle }}</strong>
            <span class="summary-sub">Δ to next: {{ formatTempDelta(toNextBucketDelta) }}</span>
          </div>
        </div>
        <div class="table market-table">
          <div class="row header">
            <span class="market-label">Outcome</span>
            <span class="right"
              >Yes (bid/ask)
              <span
                class="info-tip"
                data-tip="Best bid/ask for YES. Bid is the price you can sell YES now; ask is the price you can buy YES now. Spread = ask−bid. Size = top-of-book bid/ask size."
                >i</span
              ></span
            >
            <span class="right"
              >No (bid/ask)
              <span
                class="info-tip"
                data-tip="Best bid/ask for NO. Bid is the price you can sell NO now; ask is the price you can buy NO now. Spread = ask−bid. Size = top-of-book bid/ask size."
                >i</span
              ></span
            >
          </div>
          <div
            v-for="outcome in visibleMarketOutcomes"
            :key="outcomeKey(outcome)"
            class="row market-row"
            :class="{
              locked: isNonTradable(outcome),
              'match-high': isHighestMatch(outcome)
            }"
          >
            <div class="outcome">
              <span class="name">{{ outcome.title || "—" }}</span>
            </div>
            <div class="right price-cell">
              <span class="price">
                {{ formatBidAsk(outcome.yesBestBid, bestYesAsk(outcome)) }}
              </span>
              <span
                v-if="formatBookMeta(outcome.yesBestBid, bestYesAsk(outcome), outcome.yesBidSize, outcome.yesAskSize)"
                class="subprice"
                >{{ formatBookMeta(outcome.yesBestBid, bestYesAsk(outcome), outcome.yesBidSize, outcome.yesAskSize) }}</span
              >
            </div>
            <div class="right price-cell">
              <span class="price">
                {{ formatBidAsk(outcome.noBestBid, bestNoAsk(outcome)) }}
              </span>
              <span
                v-if="formatBookMeta(outcome.noBestBid, bestNoAsk(outcome), outcome.noBidSize, outcome.noAskSize)"
                class="subprice"
                >{{ formatBookMeta(outcome.noBestBid, bestNoAsk(outcome), outcome.noBidSize, outcome.noAskSize) }}</span
              >
            </div>
          </div>
          <div v-if="visibleMarketOutcomes.length === 0" class="row empty">
            <span v-if="showLockedOutcomes">No outcomes available</span>
            <span v-else>No tradable outcomes (toggle “Show locked”)</span>
          </div>
        </div>
      </section>
    </main>

    <main v-else class="grid">
      <section class="panel span-full trend-panel">
        <div class="panel-header">
          <div>
            <h2>Trend Explorer</h2>
            <p>Historical price action + new-high reaction windows (KST).</p>
          </div>
          <div class="panel-actions">
            <span
              class="badge"
              :class="trendError ? 'warn' : 'ok'"
              >{{ trendError ? "Needs Supabase" : "Ready" }}</span
            >
          </div>
        </div>

        <div class="trend-filters">
          <div class="trend-field">
            <label>Date</label>
            <select v-model="trendDate">
              <option v-for="date in trendDates" :key="date" :value="date">
                {{ date }}
              </option>
            </select>
          </div>
          <div class="trend-field">
            <label>Market</label>
            <select v-model="trendMarketId">
              <option value="">{{ autoMarketLabel }}</option>
              <option v-for="market in trendMarkets" :key="market.id" :value="market.id">
                {{ market.group_item_title || "—" }}
              </option>
            </select>
          </div>
          <div class="trend-field">
            <label>Start (KST)</label>
            <input v-model="trendStart" type="text" placeholder="00:00" />
          </div>
          <div class="trend-field">
            <label>End (KST)</label>
            <input v-model="trendEnd" type="text" placeholder="24:00" />
          </div>
          <div class="trend-field trend-shortcuts">
            <label>Shortcuts</label>
            <div class="trend-shortcut-buttons">
              <button class="ghost-button" type="button" @click="setFullDay">
                Full day
              </button>
              <button class="ghost-button" type="button" @click="setWindow1216">
                12–16
              </button>
            </div>
          </div>
          <div class="trend-field">
            <label>Interval</label>
            <select v-model.number="trendInterval">
              <option :value="1">1m</option>
              <option :value="5">5m</option>
              <option :value="15">15m</option>
              <option :value="30">30m</option>
              <option :value="60">60m</option>
            </select>
          </div>
          <div class="trend-field">
            <label>Series</label>
            <div class="trend-checks">
              <label>
                <input type="checkbox" v-model="showYesAsk" />
                YES ask
              </label>
              <label>
                <input type="checkbox" v-model="showYesBid" />
                YES bid
              </label>
              <label>
                <input type="checkbox" v-model="showNoAsk" />
                NO ask
              </label>
              <label>
                <input type="checkbox" v-model="showNoBid" />
                NO bid
              </label>
            </div>
          </div>
          <div class="trend-field">
            <label>Mode</label>
            <select v-model="trendMode">
              <option value="closest">Closest</option>
              <option value="carry">Carry</option>
            </select>
          </div>
        </div>

        <div class="trend-meta">
          <div class="trend-chip">
            Market: {{ selectedTrendMarketLabel || "Auto" }}
          </div>
          <div class="trend-chip">
            Interval: {{ trendInterval }}m
          </div>
          <div class="trend-chip" v-if="trendTempSource">
            Temp colors: {{ trendTempSource }}
          </div>
          <div class="trend-chip" v-if="trendTempRange">
            Temp range: {{ trendTempRange.min.toFixed(1) }}–{{ trendTempRange.max.toFixed(1) }}°C
          </div>
          <div class="trend-chip" v-if="trendCoverage">
            Snapshots: {{ trendCoverage.snapshots }}
          </div>
          <div class="trend-chip" v-if="trendCoverage?.last_snapshot">
            Last: {{ formatTime(trendCoverage.last_snapshot) }}
          </div>
          <div class="trend-chip" v-if="trendCoverage">
            Points: {{ trendCoverage.resampled_points }} (missing {{ trendCoverage.missing_points }})
          </div>
        </div>
        <span v-if="trendError" class="error">{{ trendError }}</span>

        <div v-if="trendSummary" class="summary-grid">
          <div class="summary-card">
            <span>Start (YES ask)</span>
            <strong>{{ formatCents(trendSummary.start) }}</strong>
          </div>
          <div class="summary-card">
            <span>End (YES ask)</span>
            <strong>{{ formatCents(trendSummary.end) }}</strong>
          </div>
          <div class="summary-card">
            <span>Net</span>
            <strong>{{ formatCents(trendSummary.net) }}</strong>
          </div>
          <div class="summary-card">
            <span>Low</span>
            <strong>{{ formatCents(trendSummary.min) }}</strong>
          </div>
          <div class="summary-card">
            <span>High</span>
            <strong>{{ formatCents(trendSummary.max) }}</strong>
          </div>
        </div>

        <div class="trend-chart-actions">
          <span class="subprice">Scroll to zoom · drag to zoom · shift+drag to pan</span>
          <button class="ghost-button ghost-button--sm" type="button" @click="resetTrendZoom">
            Reset zoom
          </button>
        </div>
        <div class="chart-container">
          <Line
            v-if="trendChartData"
            ref="trendChartRef"
            :data="trendChartData"
            :options="trendChartOptions"
          />
          <div v-else class="loading-chart">
            {{ trendLoading ? "Loading trend data..." : "No trend data" }}
          </div>
        </div>

        <div class="table trend-table">
          <div class="row header trend-row">
            <span>Time (KST)</span>
            <span class="right">YES bid/ask</span>
            <span class="right">NO bid/ask</span>
          </div>
          <div v-for="(row, idx) in trendSeries" :key="`${row.anchor_kst}-${idx}`" class="row trend-row">
            <span>{{ row.anchor_kst }}</span>
            <span class="right">{{ formatBidAsk(row.yes_best_bid, row.yes_best_ask) }}</span>
            <span class="right">{{ formatBidAsk(row.no_best_bid, row.no_best_ask) }}</span>
          </div>
          <div v-if="trendSeries.length === 0" class="row empty">
            <span>{{ trendLoading ? "Loading..." : "No data" }}</span>
          </div>
        </div>
      </section>
    </main>

    <footer class="footer">
      <span>Polling every 30s · Data cached server-side</span>
      <div class="footer-right">
        <button
          class="ghost-button"
          :class="{ active: soundEnabled }"
          type="button"
          @click="toggleSound"
        >
          {{ soundEnabled ? "Sound on" : "Enable sound" }}
        </button>
        <button
          v-if="enableNotifyTest"
          class="ghost-button"
          type="button"
          @click="triggerTestNotification"
        >
          Test notification
        </button>
        <span v-if="healthLine" class="health-line">{{ healthLine }}</span>
        <span v-if="highNotice" class="notice">{{ highNotice }}</span>
        <span v-if="error" class="error">{{ error }}</span>
      </div>
    </footer>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import type { ChartOptions, ChartData } from 'chart.js';
import { Line } from 'vue-chartjs';
import zoomPlugin from "chartjs-plugin-zoom";

const tempRibbonPlugin = {
  id: "tempRibbon",
  beforeDatasetsDraw(chart: ChartJS, _args: unknown, opts: any) {
    if (!opts?.enabled) return;
    const temps = Array.isArray(opts.temps) ? opts.temps : [];
    if (!temps.length) return;
    const min = typeof opts.min === "number" ? opts.min : null;
    const max = typeof opts.max === "number" ? opts.max : null;
    if (min === null || max === null) return;
    const height = typeof opts.height === "number" ? opts.height : 8;
    const opacity = typeof opts.opacity === "number" ? opts.opacity : 0.35;
    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales?.x) return;
    const xScale = scales.x;
    const y0 = chartArea.bottom - height;
    const y1 = chartArea.bottom;

    const colorFor = (temp: number | null | undefined) => {
      if (temp === null || temp === undefined || !Number.isFinite(temp)) {
        return "rgba(15, 27, 36, 0.08)";
      }
      if (min === max) return "rgba(15, 27, 36, 0.08)";
      const t = (temp - min) / (max - min);
      const hue = 220 - t * 200; // blue -> orange
      return `hsla(${hue}, 70%, 50%, ${opacity})`;
    };

    ctx.save();
    for (let i = 0; i < temps.length; i += 1) {
      const temp = temps[i];
      const color = colorFor(temp);
      const xCenter = xScale.getPixelForValue(i);
      const xPrev = i > 0 ? xScale.getPixelForValue(i - 1) : chartArea.left;
      const xNext = i + 1 < temps.length ? xScale.getPixelForValue(i + 1) : chartArea.right;
      let xStart = i === 0 ? chartArea.left : (xPrev + xCenter) / 2;
      let xEnd = i + 1 < temps.length ? (xCenter + xNext) / 2 : chartArea.right;
      if (xEnd < xStart) {
        const swap = xStart;
        xStart = xEnd;
        xEnd = swap;
      }
      ctx.fillStyle = color;
      ctx.fillRect(xStart, y0, xEnd - xStart, y1 - y0);
    }
    ctx.restore();
  }
};

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  zoomPlugin,
  tempRibbonPlugin
);

type Dashboard = {
  meta: {
    lastRefresh: string;
    kstDate: string;
    slug: string;
    eventFound: boolean;
    health?: Record<
      string,
      {
        lastSuccessAt: string | null;
        lastError: string | null;
        lastErrorAt: string | null;
      }
    >;
  };
  weather: {
    hourly: {
      times: string[];
      awc: Array<number | null>;
    };
    dayHigh: number | null;
    dayHighCelsiusWhole?: number | null;
    wunderground?: {
      source: string;
      url: string;
      dateKst: string;
      dayHighC: number | null;
      dayLowC: number | null;
      dayHighCelsiusWhole?: number | null;
      dayLowCelsiusWhole?: number | null;
      observedMaxC?: number | null;
      observedMaxCelsiusWhole?: number | null;
      current?: {
        observedAt: string;
        tempC: number;
        temp: number;
        unit: string;
      } | null;
    } | null;
    sources?: {
      awc: {
        latest: number | null;
        latestTime: string | null;
        latestObservedAt?: string | null;
        dayHigh: number | null;
        deltaVsWunderground?: number | null;
        latestDeltaVsWunderground?: number | null;
      };
    };
  };
  market: {
    eventTitle: string | null;
    outcomes: Array<{
      title: string | null;
      tokenId: string | null;
      tokenYes?: string | null;
      tokenNo?: string | null;
      price: number | null;
      yesPrice?: number | null;
      noPrice?: number | null;
      yesBestBid?: number | null;
      yesBestAsk?: number | null;
      noBestBid?: number | null;
      noBestAsk?: number | null;
      yesBidSize?: number | null;
      yesAskSize?: number | null;
      noBidSize?: number | null;
      noAskSize?: number | null;
      acceptingOrders?: boolean | null;
      volume24hr?: number | null;
    }>;
  };
  forecast?: {
    source: string;
    defaultModel: string;
    models: string[];
    timezone: string;
    hourly: {
      times: string[];
      temp_c_by_model: Record<string, Array<number | null>>;
    };
  } | null;
  portfolio?: {
    balance: unknown;
    positions: unknown;
  };
};

type TrendMarket = {
  id: string;
  group_item_title: string | null;
  lower_bound_celsius: number | null;
  upper_bound_celsius: number | null;
  group_item_threshold: number | null;
  yes_token_id?: string | null;
  no_token_id?: string | null;
};

type TrendPoint = {
  anchor_kst: string;
  anchor_utc: string;
  captured_at: string | null;
  temp_c?: number | null;
  yes_best_bid: number | null;
  yes_best_ask: number | null;
  no_best_bid: number | null;
  no_best_ask: number | null;
  yes_bid_size: number | null;
  yes_ask_size: number | null;
  no_bid_size: number | null;
  no_ask_size: number | null;
  accepting_orders: boolean | null;
};

type TrendCoverage = {
  snapshots: number;
  first_snapshot: string | null;
  last_snapshot: string | null;
  resampled_points: number;
  missing_points: number;
};

type TrendResponse = {
  meta: {
    date_kst: string;
    slug: string | null;
    event_id: string;
    market_id: string;
    market_label: string | null;
    temp_source?: string | null;
    timezone: string;
    interval_minutes: number;
    start_kst: string;
    end_kst: string;
    mode: string;
  };
  coverage: TrendCoverage;
  series: TrendPoint[];
};

const dashboard = ref<Dashboard | null>(null);
const error = ref<string | null>(null);
const highNotice = ref<string | null>(null);
const enableNotifyTest = ref(false);
const soundEnabled = ref(false);
const showLockedOutcomes = ref(false);

const trendChartRef = ref<InstanceType<typeof Line> | null>(null);
const trendDates = ref<string[]>([]);
const trendDate = ref<string | null>(null);
const trendMarkets = ref<TrendMarket[]>([]);
const trendDefaultMarketId = ref<string | null>(null);
const trendMarketId = ref<string>(""); // empty = auto
const trendStart = ref("00:00");
const trendEnd = ref("24:00");
const trendInterval = ref(15);
const trendMode = ref<"closest" | "carry">("closest");
const showYesAsk = ref(true);
const showYesBid = ref(false);
const showNoAsk = ref(false);
const showNoBid = ref(false);
const trendSeries = ref<TrendPoint[]>([]);
const trendCoverage = ref<TrendCoverage | null>(null);
const trendTempSource = ref<string | null>(null);
const trendLoading = ref(false);
const trendError = ref<string | null>(null);
let timer: number | undefined;
let noticeTimer: number | undefined;
let notifyStateLoaded = false;
let lastNotifiedDate: string | null = null;
let lastNotifiedHigh: number | null = null;
const NOTIFY_STORAGE_KEY = "dayHighNotification";
let audioContext: AudioContext | null = null;
let soundUnlockHandler: (() => void) | null = null;
const SOUND_STORAGE_KEY = "dayHighSoundEnabled";

const routePath = ref(
  typeof window !== "undefined" ? window.location.pathname : "/"
);
const isTrends = computed(() => routePath.value.startsWith("/trends"));
const handlePopState = () => {
  if (typeof window === "undefined") return;
  routePath.value = window.location.pathname;
};

const marketOutcomes = computed(() => dashboard.value?.market?.outcomes ?? []);
const visibleMarketOutcomes = computed(() =>
  showLockedOutcomes.value
    ? marketOutcomes.value
    : marketOutcomes.value.filter((outcome) => !isNonTradable(outcome))
);

const kstNow = computed(() => {
  const ts = dashboard.value?.meta?.lastRefresh;
  if (!ts) return null;
  const dt = new Date(ts);
  return Number.isNaN(dt.getTime()) ? null : dt;
});

const minutesLeftInDay = computed<number | null>(() => {
  const now = kstNow.value;
  const kstDate = dashboard.value?.meta?.kstDate;
  if (!now || !kstDate) return null;
  const start = new Date(`${kstDate}T00:00:00+09:00`);
  if (Number.isNaN(start.getTime())) return null;
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  const mins = (end.getTime() - now.getTime()) / 60000;
  return Math.max(0, mins);
});

const ageMinutesFromNow = (ts: string | null | undefined): number | null => {
  if (!ts) return null;
  const now = kstNow.value;
  if (!now) return null;
  const then = new Date(ts);
  if (Number.isNaN(then.getTime())) return null;
  return Math.max(0, (now.getTime() - then.getTime()) / 60000);
};

const awcStaleMinutes = computed(
  () => ageMinutesFromNow(dashboard.value?.weather?.sources?.awc?.latestObservedAt ?? null)
);
const wuStaleMinutes = computed(
  () => ageMinutesFromNow(dashboard.value?.weather?.wunderground?.current?.observedAt ?? null)
);

const bucketNow = computed<number | null>(() => {
  const whole = dashboard.value?.weather?.dayHighCelsiusWhole;
  if (typeof whole === "number") return whole;
  const dayHigh = dashboard.value?.weather?.dayHigh;
  if (typeof dayHigh !== "number") return null;
  return Math.round(dayHigh);
});

const toNextBucketDelta = computed<number | null>(() => {
  const dayHigh = dashboard.value?.weather?.dayHigh;
  const bucket = bucketNow.value;
  if (typeof dayHigh !== "number" || typeof bucket !== "number") return null;
  const nextThreshold = bucket + 0.5;
  const delta = nextThreshold - dayHigh;
  return delta > 0 ? delta : 0;
});

const trendDefaultMarketLabel = computed(() => {
  if (!trendDefaultMarketId.value) return null;
  const found = trendMarkets.value.find((m) => m.id === trendDefaultMarketId.value);
  return found?.group_item_title ?? null;
});

const trendTemps = computed(() => trendSeries.value.map((point) => point.temp_c ?? null));
const trendTempRange = computed(() => {
  const temps = trendSeries.value
    .map((point) => point.temp_c)
    .filter((val): val is number => typeof val === "number" && Number.isFinite(val));
  if (!temps.length) return null;
  return { min: Math.min(...temps), max: Math.max(...temps) };
});

const selectedTrendMarketLabel = computed(() => {
  if (!trendMarketId.value) return trendDefaultMarketLabel.value;
  const found = trendMarkets.value.find((m) => m.id === trendMarketId.value);
  return found?.group_item_title ?? null;
});

const autoMarketLabel = computed(() => {
  return trendDefaultMarketLabel.value
    ? `Auto (${trendDefaultMarketLabel.value})`
    : "Auto";
});

const trendSummary = computed(() => {
  if (!trendSeries.value.length) return null;
  const values = trendSeries.value
    .map((point) => point.yes_best_ask)
    .filter((val): val is number => typeof val === "number" && Number.isFinite(val));
  if (!values.length) return null;
  const first = values[0];
  const last = values[values.length - 1];
  const min = Math.min(...values);
  const max = Math.max(...values);
  return {
    start: first,
    end: last,
    net: last - first,
    min,
    max
  };
});


type ParsedOutcome = {
  lower: number | null;
  upper: number | null;
  repr: number | null;
};

const parseOutcomeTitle = (
  title: string | null | undefined
): ParsedOutcome | null => {
  if (!title) return null;
  const text = title.toLowerCase();

  const range =
    text.match(/(-?\d+)\s*°?\s*c\s*(?:-|–|—|to)\s*(-?\d+)\s*°?\s*c/) ||
    text.match(/between\s+(-?\d+)\s*°?\s*c?\s+and\s+(-?\d+)\s*°?\s*c?/);
  if (range) {
    const a = Number.parseInt(range[1], 10);
    const b = Number.parseInt(range[2], 10);
    if (!Number.isNaN(a) && !Number.isNaN(b)) {
      const lower = Math.min(a, b);
      const upper = Math.max(a, b);
      return { lower, upper, repr: (lower + upper) / 2 };
    }
  }

  const single = text.match(/(-?\d+)\s*°?\s*c/);
  if (!single) return null;
  const n = Number.parseInt(single[1], 10);
  if (Number.isNaN(n)) return null;

  if (text.includes("or below") || text.includes("or lower")) {
    return { lower: null, upper: n, repr: n };
  }
  if (text.includes("or higher") || text.includes("or above")) {
    return { lower: n, upper: null, repr: n };
  }
  return { lower: n, upper: n, repr: n };
};

const outcomeMatchesValue = (
  title: string | null | undefined,
  value: number
): boolean => {
  const parsed = parseOutcomeTitle(title);
  if (!parsed) return false;
  if (typeof parsed.lower === "number" && typeof parsed.upper === "number") {
    return value >= parsed.lower && value <= parsed.upper;
  }
  if (typeof parsed.lower === "number") return value >= parsed.lower;
  if (typeof parsed.upper === "number") return value <= parsed.upper;
  return false;
};

const findOutcomeForValue = (
  outcomes: Array<{ title: string | null; tokenId: string | null }>,
  value: number
) => {
  let exact: { title: string | null; tokenId: string | null } | null = null;
  let bestRange: { title: string | null; tokenId: string | null } | null = null;
  let bestRangeWidth: number | null = null;
  let boundary: { title: string | null; tokenId: string | null } | null = null;

  for (const outcome of outcomes) {
    const parsed = parseOutcomeTitle(outcome.title);
    if (!parsed) continue;

    if (
      typeof parsed.lower === "number" &&
      typeof parsed.upper === "number" &&
      parsed.lower === value &&
      parsed.upper === value
    ) {
      exact = outcome;
      continue;
    }

    if (
      typeof parsed.lower === "number" &&
      typeof parsed.upper === "number" &&
      value >= parsed.lower &&
      value <= parsed.upper
    ) {
      const width = parsed.upper - parsed.lower;
      if (bestRangeWidth === null || width < bestRangeWidth) {
        bestRange = outcome;
        bestRangeWidth = width;
      }
      continue;
    }

    if (
      (typeof parsed.lower === "number" && parsed.upper === null && value >= parsed.lower) ||
      (typeof parsed.upper === "number" && parsed.lower === null && value <= parsed.upper)
    ) {
      boundary ??= outcome;
    }
  }

  return exact ?? bestRange ?? boundary;
};

const currentBucketTitle = computed(() => {
  const target = bucketNow.value;
  if (typeof target !== "number") return "—";
  const found = findOutcomeForValue(marketOutcomes.value, target);
  return found?.title || `${target}°C`;
});

const parseForecastTime = (ts: string, timezone: string | undefined): Date | null => {
  if (!ts) return null;
  const hasZone = /[zZ]$/.test(ts) || /[+-]\d{2}:\d{2}$/.test(ts);
  const dt = hasZone
    ? new Date(ts)
    : timezone === "Asia/Seoul"
    ? new Date(`${ts}+09:00`)
    : new Date(ts);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const formatForecastClock = (ts: string | null): string | null => {
  if (!ts) return null;
  const match = ts.match(/T(\d{2}):(\d{2})/);
  if (match) return `${match[1]}:${match[2]}`;
  return ts;
};

const forecastSummary = computed(() => {
  const forecast = dashboard.value?.forecast ?? null;
  const kstDate = dashboard.value?.meta?.kstDate;
  const now = kstNow.value;
  if (!forecast || !kstDate) {
    return { defaultModel: null, defaultMax: null, defaultTime: null, rangeText: null };
  }
  const models = Array.isArray(forecast.models) ? forecast.models : [];
  const times = forecast.hourly?.times ?? [];
  const tz = forecast.timezone;

  const perModel = models
    .map((model) => {
      const temps = forecast.hourly?.temp_c_by_model?.[model] ?? [];
      let max: number | null = null;
      let maxTime: string | null = null;
      for (let i = 0; i < Math.min(times.length, temps.length); i++) {
        const t = times[i];
        if (typeof t !== "string" || !t.startsWith(kstDate)) continue;
        const val = temps[i];
        if (typeof val !== "number") continue;
        if (max === null || val > max) {
          max = val;
          maxTime = t;
        }
      }

      let remainingMax: number | null = null;
      let remainingTime: string | null = null;
      if (now) {
        for (let i = 0; i < Math.min(times.length, temps.length); i++) {
          const t = times[i];
          if (typeof t !== "string" || !t.startsWith(kstDate)) continue;
          const dt = parseForecastTime(t, tz);
          if (!dt || dt.getTime() < now.getTime()) continue;
          const val = temps[i];
          if (typeof val !== "number") continue;
          if (remainingMax === null || val > remainingMax) {
            remainingMax = val;
            remainingTime = t;
          }
        }
      }

      return { model, max, maxTime, remainingMax, remainingTime };
    })
    .filter((row) => row.model);

  const maxValues = perModel
    .map((row) => row.max)
    .filter((val): val is number => typeof val === "number");
  const ensembleMin = maxValues.length ? Math.min(...maxValues) : null;
  const ensembleMax = maxValues.length ? Math.max(...maxValues) : null;

  const defaultModel =
    typeof forecast.defaultModel === "string" ? forecast.defaultModel : models[0] ?? null;
  const defaultRow = perModel.find((row) => row.model === defaultModel) ?? null;

  const rangeText =
    ensembleMin !== null && ensembleMax !== null
      ? `${ensembleMin.toFixed(1)}–${ensembleMax.toFixed(1)}°C`
      : null;

  return {
    defaultModel,
    defaultMax: defaultRow?.max ?? null,
    defaultTime: formatForecastClock(defaultRow?.maxTime ?? null),
    rangeText,
  };
});

const marketImplied = computed(() => {
  const items = marketOutcomes.value
    .map((outcome) => {
      const parsed = parseOutcomeTitle(outcome.title);
      const repr = parsed?.repr;
      const bid = outcome.yesBestBid;
      const ask = bestYesAsk(outcome);
      let mid: number | null = null;
      if (
        typeof bid === "number" &&
        typeof ask === "number" &&
        bid > 0 &&
        ask > 0
      ) {
        mid = (bid + ask) / 2;
      } else if (typeof ask === "number" && ask > 0) {
        mid = ask;
      } else if (typeof bid === "number" && bid > 0) {
        mid = bid;
      }
      if (typeof repr !== "number" || typeof mid !== "number") return null;
      return { title: outcome.title, repr, mid };
    })
    .filter((row): row is { title: string | null; repr: number; mid: number } => !!row);

  const sum = items.reduce((acc, row) => acc + row.mid, 0);
  if (!items.length || sum <= 0) {
    return { mean: null as number | null, topTitle: null as string | null, topProb: null as number | null, sum: null as number | null };
  }

  let mean = 0;
  let top: { title: string | null; prob: number } | null = null;
  for (const row of items) {
    const p = row.mid / sum;
    mean += p * row.repr;
    if (!top || p > top.prob) top = { title: row.title, prob: p };
  }

  return { mean, topTitle: top?.title ?? null, topProb: top?.prob ?? null, sum };
});

const healthLine = computed(() => {
  const health = dashboard.value?.meta?.health;
  if (!health) return null;
  const now = kstNow.value;
  if (!now) return null;
  const order = ["market", "event", "wunderground", "awc", "forecast", "portfolio"];
  const keys = Object.keys(health);
  keys.sort((a, b) => {
    const ia = order.indexOf(a);
    const ib = order.indexOf(b);
    if (ia === -1 && ib === -1) return a.localeCompare(b);
    if (ia === -1) return 1;
    if (ib === -1) return -1;
    return ia - ib;
  });

  const parts: string[] = [];
  for (const key of keys) {
    const entry = health[key];
    if (!entry) continue;
    if (entry.lastError) {
      parts.push(`${key}: ERR`);
      continue;
    }
    const mins = ageMinutesFromNow(entry.lastSuccessAt);
    parts.push(`${key}: ${formatAge(mins)}`);
  }
  return parts.length ? `Health · ${parts.join(" · ")}` : null;
});

// --- Chart Data & Options ---

const chartData = computed<ChartData<'line'> | null>(() => {
  if (!dashboard.value) return null;
  
  const times = dashboard.value.weather.hourly.times || [];
  const awc = dashboard.value.weather.hourly.awc || [];

  return {
    labels: times.map(t => formatHourLabel(t)),
    datasets: [
      {
        label: 'AWC',
        borderColor: '#1f7a8c', // var(--accent)
        backgroundColor: '#1f7a8c',
        data: awc,
        tension: 0.1,
        pointRadius: 3,
        pointHoverRadius: 6
      }
    ]
  };
});

const chartOptions = computed<ChartOptions<'line'>>(() => {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false // We use our custom legend
      },
      tooltip: {
        backgroundColor: '#0f1b24',
        titleFont: {
          family: "'IBM Plex Mono', monospace",
          size: 13
        },
        bodyFont: {
          family: "'IBM Plex Mono', monospace",
          size: 13
        },
        padding: 12,
        cornerRadius: 8,
        displayColors: false,
        callbacks: {
          label: (context) => {
             let label = context.dataset.label || '';
             if (label) {
                 label += ': ';
             }
             if (context.parsed.y !== null) {
                 label += context.parsed.y.toFixed(1) + '°C';
             }
             return label;
          }
        }
      }
    },
    scales: {
      x: {
        grid: {
          color: 'rgba(15, 27, 36, 0.05)'
        },
        ticks: {
          color: '#5c6a73', // var(--muted)
          font: {
             family: "'IBM Plex Mono', monospace",
             size: 14 // Increased size
          },
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 6
        }
      },
      y: {
        grid: {
          color: 'rgba(15, 27, 36, 0.08)'
        },
        ticks: {
          color: '#5c6a73',
          font: {
            family: "'IBM Plex Mono', monospace",
             size: 14 // Increased size
          },
          callback: (value) => {
            return typeof value === 'number' ? value.toFixed(1) + '°C' : value;
          }
        }
      }
    }
  };
});

const navigate = (path: string) => {
  if (typeof window === "undefined") return;
  if (window.location.pathname === path) return;
  window.history.pushState({}, "", path);
  routePath.value = window.location.pathname;
  window.scrollTo({ top: 0, behavior: "smooth" });
};

const trendChartData = computed<ChartData<"line"> | null>(() => {
  if (!trendSeries.value.length) return null;
  const labels = trendSeries.value.map((point) => point.anchor_kst);
  const datasets = [];
  if (showYesAsk.value) {
    datasets.push({
      label: "YES ask",
      data: trendSeries.value.map((point) => point.yes_best_ask),
      borderColor: "rgba(31, 122, 140, 1)",
      backgroundColor: "rgba(31, 122, 140, 0.15)",
      yAxisID: "price",
      pointRadius: 0,
      tension: 0.25
    });
  }
  if (showYesBid.value) {
    datasets.push({
      label: "YES bid",
      data: trendSeries.value.map((point) => point.yes_best_bid),
      borderColor: "rgba(31, 122, 140, 0.7)",
      backgroundColor: "rgba(31, 122, 140, 0.1)",
      borderDash: [6, 4],
      yAxisID: "price",
      pointRadius: 0,
      tension: 0.25
    });
  }
  if (showNoAsk.value) {
    datasets.push({
      label: "NO ask",
      data: trendSeries.value.map((point) => point.no_best_ask),
      borderColor: "rgba(208, 140, 42, 1)",
      backgroundColor: "rgba(208, 140, 42, 0.15)",
      yAxisID: "price",
      pointRadius: 0,
      tension: 0.25
    });
  }
  if (showNoBid.value) {
    datasets.push({
      label: "NO bid",
      data: trendSeries.value.map((point) => point.no_best_bid),
      borderColor: "rgba(208, 140, 42, 0.7)",
      backgroundColor: "rgba(208, 140, 42, 0.1)",
      borderDash: [6, 4],
      yAxisID: "price",
      pointRadius: 0,
      tension: 0.25
    });
  }
  return {
    labels,
    datasets
  };
});

const trendChartOptions = computed<ChartOptions<"line">>(() => {
  const options: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false
    },
    plugins: {
      legend: {
        position: "top" as const,
        labels: {
          font: {
            family: "'IBM Plex Mono', monospace",
            size: 11
          }
        }
      },
      tooltip: {
        callbacks: {
          label: (context) => {
            const val = context.parsed.y;
            if (val === null || val === undefined) return "—";
            return `${context.dataset.label}: ${(val * 100).toFixed(1)}¢`;
          },
          afterBody: (items) => {
            const idx = items?.[0]?.dataIndex ?? null;
            if (idx === null) return "";
            const temp = trendSeries.value[idx]?.temp_c;
            if (typeof temp === "number" && Number.isFinite(temp)) {
              return `Temp: ${temp.toFixed(1)}°C`;
            }
            return "";
          }
        }
      },
      zoom: {
        pan: {
          enabled: true,
          modifierKey: "shift",
          mode: "x"
        },
        zoom: {
          wheel: { enabled: true },
          pinch: { enabled: true },
          drag: { enabled: true },
          mode: "x"
        }
      }
    },
    scales: {
      x: {
        grid: { color: "rgba(15, 27, 36, 0.05)" },
        ticks: {
          color: "#5c6a73",
          font: { family: "'IBM Plex Mono', monospace", size: 11 },
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 10
        }
      },
      price: {
        grid: { color: "rgba(15, 27, 36, 0.08)" },
        position: "left",
        ticks: {
          color: "#5c6a73",
          font: { family: "'IBM Plex Mono', monospace", size: 11 },
          callback: (value) => {
            return typeof value === "number" ? `${(value * 100).toFixed(0)}¢` : value;
          }
        }
      }
    }
  };
  const ribbonOptions = trendTempRange.value
    ? {
        enabled: true,
        temps: trendTemps.value,
        min: trendTempRange.value.min,
        max: trendTempRange.value.max,
        height: 8,
        opacity: 0.25
      }
    : { enabled: false };
  (options.plugins as any).tempRibbon = ribbonOptions;
  return options;
});


const load = async () => {
  try {
    const res = await fetch("/api/dashboard");
    if (!res.ok) throw new Error(`API ${res.status}`);
    const data = await res.json();
    maybeNotifyNewHigh(data);
    dashboard.value = data;
    error.value = null;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Failed to load";
  }
};

const fetchTrendDates = async () => {
  try {
    const res = await fetch("/api/trends/dates");
    if (!res.ok) throw new Error(`API ${res.status}`);
    const data = await res.json();
    trendDates.value = Array.isArray(data.dates) ? data.dates : [];
    if (!trendDate.value && trendDates.value.length) {
      trendDate.value = trendDates.value[0];
    }
    trendError.value = null;
  } catch (err) {
    trendError.value = err instanceof Error ? err.message : "Failed to load dates";
  }
};

const fetchTrendMarkets = async (date_kst: string) => {
  try {
    const res = await fetch(`/api/trends/markets?date_kst=${encodeURIComponent(date_kst)}`);
    if (!res.ok) throw new Error(`API ${res.status}`);
    const data = await res.json();
    trendMarkets.value = Array.isArray(data.markets) ? data.markets : [];
    trendDefaultMarketId.value = data.default_market_id ?? null;
    if (!trendMarketId.value) {
      trendMarketId.value = "";
    } else {
      const exists = trendMarkets.value.some((m) => m.id === trendMarketId.value);
      if (!exists) trendMarketId.value = "";
    }
    trendError.value = null;
  } catch (err) {
    trendError.value = err instanceof Error ? err.message : "Failed to load markets";
  }
};

const fetchTrendSeries = async () => {
  if (!trendDate.value) return;
  trendLoading.value = true;
  trendError.value = null;
  try {
    const params = new URLSearchParams();
    params.set("date_kst", trendDate.value);
    if (trendMarketId.value) params.set("market_id", trendMarketId.value);
    params.set("start_kst", trendStart.value);
    params.set("end_kst", trendEnd.value);
    params.set("interval_minutes", trendInterval.value.toString());
    params.set("mode", trendMode.value);
    const res = await fetch(`/api/trends?${params.toString()}`);
    if (!res.ok) throw new Error(`API ${res.status}`);
    const data: TrendResponse = await res.json();
    trendSeries.value = Array.isArray(data.series) ? data.series : [];
    trendCoverage.value = data.coverage ?? null;
    trendTempSource.value = data.meta?.temp_source ?? null;
    trendError.value = null;
    await nextTick();
    resetTrendZoom();
  } catch (err) {
    trendError.value = err instanceof Error ? err.message : "Failed to load trends";
    trendSeries.value = [];
    trendCoverage.value = null;
    trendTempSource.value = null;
  } finally {
    trendLoading.value = false;
  }
};

const resetTrendZoom = () => {
  const chart = trendChartRef.value?.chart;
  if (chart && typeof chart.resetZoom === "function") {
    chart.resetZoom();
  }
};


const setFullDay = () => {
  trendStart.value = "00:00";
  trendEnd.value = "24:00";
};

const setWindow1216 = () => {
  trendStart.value = "12:00";
  trendEnd.value = "16:00";
};

onMounted(() => {
  load();
  timer = window.setInterval(load, 30000);
  fetchTrendDates();
  window.addEventListener("popstate", handlePopState);
  loadSoundPreference();
  if (typeof window !== "undefined") {
    const params = new URLSearchParams(window.location.search);
    if (params.get("notify") === "button") {
      enableNotifyTest.value = true;
    }
  }
});

watch(
  () => trendDate.value,
  async (value) => {
    if (!value) return;
    await fetchTrendMarkets(value);
    await fetchTrendSeries();
  }
);

watch(
  () => [trendMarketId.value, trendStart.value, trendEnd.value, trendInterval.value, trendMode.value],
  async () => {
    if (!trendDate.value) return;
    await fetchTrendSeries();
  }
);

watch(
  () => [showYesAsk.value, showYesBid.value, showNoAsk.value, showNoBid.value],
  () => {
    if (showYesAsk.value || showYesBid.value || showNoAsk.value || showNoBid.value) return;
    showYesAsk.value = true;
  }
);


onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
  if (noticeTimer) window.clearInterval(noticeTimer);
  window.removeEventListener("popstate", handlePopState);
  if (soundUnlockHandler) {
    window.removeEventListener("click", soundUnlockHandler);
    window.removeEventListener("keydown", soundUnlockHandler);
    soundUnlockHandler = null;
  }
});

const formatTemp = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return `${val.toFixed(1)}°C`;
};

const formatTempDelta = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return `${val.toFixed(2)}°C`;
};

const formatDelta = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  const sign = val > 0 ? "+" : "";
  return `${sign}${val.toFixed(2)}°C`;
};

const formatBucket = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return `${val}°C`;
};

const formatPrice = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return val.toFixed(3);
};

const formatCents = (val: number | null | undefined) => {
  if (val === null || val === undefined || !Number.isFinite(val)) return "—";
  return `${(val * 100).toFixed(1)}¢`;
};

const formatProb = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return `${(val * 100).toFixed(1)}%`;
};

const formatAge = (minutes: number | null | undefined) => {
  if (minutes === null || minutes === undefined) return "—";
  if (minutes < 1) return "<1m";
  if (minutes < 60) return `${Math.round(minutes)}m`;
  const h = Math.floor(minutes / 60);
  const m = Math.round(minutes % 60);
  return m ? `${h}h ${m}m` : `${h}h`;
};

const formatDuration = (minutes: number | null | undefined) => {
  if (minutes === null || minutes === undefined) return "—";
  if (minutes < 60) return `${Math.ceil(minutes)}m`;
  const h = Math.floor(minutes / 60);
  const m = Math.ceil(minutes % 60);
  return m ? `${h}h ${m}m` : `${h}h`;
};

const showHighNotice = (message: string) => {
  highNotice.value = message;
  if (noticeTimer) window.clearTimeout(noticeTimer);
  noticeTimer = window.setTimeout(() => {
    highNotice.value = null;
  }, 12000);
};

const setupSoundUnlock = () => {
  if (typeof window === "undefined" || soundUnlockHandler) return;
  soundUnlockHandler = async () => {
    await ensureAudioContext();
    if (soundUnlockHandler) {
      window.removeEventListener("click", soundUnlockHandler);
      window.removeEventListener("keydown", soundUnlockHandler);
      soundUnlockHandler = null;
    }
  };
  window.addEventListener("click", soundUnlockHandler, { once: true });
  window.addEventListener("keydown", soundUnlockHandler, { once: true });
};

const ensureAudioContext = async (): Promise<AudioContext | null> => {
  if (typeof window === "undefined") return null;
  const AudioCtor = window.AudioContext || (window as typeof window & {
    webkitAudioContext?: typeof AudioContext;
  }).webkitAudioContext;
  if (!AudioCtor) return null;
  if (!audioContext) {
    audioContext = new AudioCtor();
  }
  if (audioContext.state === "suspended") {
    await audioContext.resume();
  }
  return audioContext;
};

const playBeep = () => {
  if (!audioContext || audioContext.state !== "running") return;
  const osc = audioContext.createOscillator();
  const gain = audioContext.createGain();
  osc.type = "sine";
  osc.frequency.value = 880;
  const now = audioContext.currentTime;
  gain.gain.setValueAtTime(0.0001, now);
  gain.gain.exponentialRampToValueAtTime(0.08, now + 0.02);
  gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.12);
  osc.connect(gain);
  gain.connect(audioContext.destination);
  osc.start(now);
  osc.stop(now + 0.13);
};

const loadSoundPreference = () => {
  if (typeof window === "undefined") return;
  try {
    const raw = window.localStorage.getItem(SOUND_STORAGE_KEY);
    if (raw === "true") {
      soundEnabled.value = true;
      setupSoundUnlock();
    }
  } catch {
    // ignore storage errors
  }
};

const persistSoundPreference = () => {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(
      SOUND_STORAGE_KEY,
      soundEnabled.value ? "true" : "false"
    );
  } catch {
    // ignore storage errors
  }
};

const toggleSound = async () => {
  if (soundEnabled.value) {
    soundEnabled.value = false;
    persistSoundPreference();
    if (audioContext && audioContext.state === "running") {
      await audioContext.suspend();
    }
    return;
  }
  const ctx = await ensureAudioContext();
  if (!ctx || ctx.state !== "running") {
    showHighNotice("Sound blocked — click again");
    return;
  }
  soundEnabled.value = true;
  persistSoundPreference();
  playBeep();
};

const loadNotifyState = () => {
  if (notifyStateLoaded || typeof window === "undefined") return;
  notifyStateLoaded = true;
  try {
    const raw = window.localStorage.getItem(NOTIFY_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      const date = typeof parsed.date === "string" ? parsed.date : null;
      const value = typeof parsed.value === "number" ? parsed.value : null;
      lastNotifiedDate = date;
      lastNotifiedHigh = value;
    }
  } catch {
    // ignore storage errors
  }
};

const persistNotifyState = () => {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(
      NOTIFY_STORAGE_KEY,
      JSON.stringify({ date: lastNotifiedDate, value: lastNotifiedHigh })
    );
  } catch {
    // ignore storage errors
  }
};

const notifyBrowser = (title: string, message: string) => {
  if (typeof window === "undefined") return;
  if (!("Notification" in window)) return;
  if (Notification.permission === "granted") {
    new Notification(title, { body: message, tag: "day-high" });
    return;
  }
  if (Notification.permission === "default") {
    Notification.requestPermission().then((permission) => {
      if (permission === "granted") {
        new Notification(title, { body: message, tag: "day-high" });
      }
    });
  }
};

const triggerTestNotification = () => {
  const message = "Test notification: new day high";
  showHighNotice(message);
  notifyBrowser("New day high: -2.0°C", message);
  if (soundEnabled.value) {
    playBeep();
  }
};

const maybeNotifyNewHigh = (next: Dashboard) => {
  loadNotifyState();
  const dayHigh = next?.weather?.dayHigh;
  const dayHighWhole = next?.weather?.dayHighCelsiusWhole;
  const kstDate = next?.meta?.kstDate;
  const signal =
    typeof dayHighWhole === "number" ? dayHighWhole : typeof dayHigh === "number" ? dayHigh : null;
  if (typeof signal !== "number" || !kstDate) return;

  if (lastNotifiedDate !== kstDate) {
    lastNotifiedDate = kstDate;
    lastNotifiedHigh = signal;
    persistNotifyState();
    return;
  }

  if (lastNotifiedHigh === null) {
    lastNotifiedHigh = signal;
    persistNotifyState();
    return;
  }

  if (signal > lastNotifiedHigh + 1e-6) {
    const tempText = typeof dayHighWhole === "number" ? formatBucket(signal) : formatTemp(signal);
    const message =
      typeof dayHighWhole === "number"
        ? `New winning bucket: ${tempText} (WU)`
        : `New day high: ${tempText} (KST)`;
    const title =
      typeof dayHighWhole === "number" ? `New bucket: ${tempText}` : `New day high: ${tempText}`;
    showHighNotice(message);
    notifyBrowser(title, message);
    if (soundEnabled.value) {
      playBeep();
    }
    lastNotifiedHigh = signal;
    persistNotifyState();
  }
};

const formatTime = (ts: string | null | undefined) => {
  if (!ts) return "—";
  try {
    const dt = new Date(ts);
    return dt.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "Asia/Seoul",
    });
  } catch {
    return "—";
  }
};

const formatHourLabel = (ts: string) => {
  try {
    const dt = new Date(ts);
    return dt.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "Asia/Seoul",
    });
  } catch {
    return "--:--";
  }
};

const outcomeKey = (outcome: {
  tokenId: string | null;
  title: string | null;
}) => outcome.tokenId || outcome.title || "—";

const highestOutcomeKey = computed(() => {
  const target = bucketNow.value;
  if (typeof target !== "number") return null;
  const found = findOutcomeForValue(marketOutcomes.value, target);
  return found ? outcomeKey(found) : null;
});

const isHighestMatch = (outcome: { tokenId: string | null; title: string | null }) =>
  outcomeKey(outcome) === highestOutcomeKey.value;

const bestYesAsk = (outcome: {
  yesBestAsk?: number | null;
  yesPrice?: number | null;
  price: number | null;
}) => {
  const ask =
    typeof outcome.yesBestAsk === "number"
      ? outcome.yesBestAsk
      : typeof outcome.yesPrice === "number"
      ? outcome.yesPrice
      : typeof outcome.price === "number"
      ? outcome.price
      : null;
  return typeof ask === "number" && ask > 0 ? ask : null;
};

const bestNoAsk = (outcome: { noBestAsk?: number | null; noPrice?: number | null }) => {
  const ask = typeof outcome.noBestAsk === "number" ? outcome.noBestAsk : outcome.noPrice;
  return typeof ask === "number" && ask > 0 ? ask : null;
};

const formatBidAsk = (
  bid: number | null | undefined,
  ask: number | null | undefined
) => {
  const b = typeof bid === "number" ? bid : null;
  const a = typeof ask === "number" ? ask : null;
  if (b === null && a === null) return "—";
  if (b === null) return `— / ${formatPrice(a)}`;
  if (a === null) return `${formatPrice(b)} / —`;
  return `${formatPrice(b)} / ${formatPrice(a)}`;
};

const formatSize = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  if (!Number.isFinite(val)) return "—";
  if (val >= 1000) return `${Math.round(val)}`;
  if (val >= 10) return `${val.toFixed(0)}`;
  return `${val.toFixed(2)}`;
};

const formatBookMeta = (
  bid: number | null | undefined,
  ask: number | null | undefined,
  bidSize: number | null | undefined,
  askSize: number | null | undefined
) => {
  const b = typeof bid === "number" ? bid : null;
  const a = typeof ask === "number" ? ask : null;
  const hasAnySize =
    typeof bidSize === "number" ||
    typeof askSize === "number";
  const hasSpread = b !== null && a !== null && Number.isFinite(a - b) && (a - b) >= 0;

  const parts: string[] = [];
  if (hasSpread) {
    parts.push(`spread ${formatPrice(a! - b!)}`);
  }
  if (hasAnySize) {
    parts.push(`size ${formatSize(bidSize)}/${formatSize(askSize)}`);
  }
  return parts.length ? parts.join(" · ") : null;
};

const isNonTradable = (outcome: {
  price: number | null;
  yesPrice?: number | null;
  noPrice?: number | null;
  yesBestBid?: number | null;
  yesBestAsk?: number | null;
  noBestBid?: number | null;
  noBestAsk?: number | null;
  acceptingOrders?: boolean | null;
}) => {
  if (outcome.acceptingOrders === false) return true;

  const yesBid = typeof outcome.yesBestBid === "number" ? outcome.yesBestBid : null;
  const yesAsk = bestYesAsk(outcome);
  const noBid = typeof outcome.noBestBid === "number" ? outcome.noBestBid : null;
  const noAsk = bestNoAsk(outcome);

  const prices = [yesBid, yesAsk, noBid, noAsk].filter(
    (val): val is number => typeof val === "number" && Number.isFinite(val)
  );
  if (!prices.length) return true;

  const actionable = prices.some((val) => val > 0.001 && val < 0.999);
  return !actionable;
};
</script>
