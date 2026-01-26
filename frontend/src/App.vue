<template>
  <div class="app">
    <header class="header">
      <div class="title">
        <p class="eyebrow">Seoul Daily High (RKSI)</p>
        <h1>{{ dashboard?.market?.eventTitle || "Highest Temperature in Seoul" }}</h1>
        <p class="subtitle">Slug: {{ dashboard?.meta?.slug || "—" }}</p>
      </div>
      <div class="metrics">
        <div class="metric">
          <span>Today (KST)</span>
          <strong>{{ dashboard?.meta?.kstDate || "—" }}</strong>
        </div>
        <div class="metric">
          <span>Day High So Far</span>
          <strong>{{ formatTemp(dashboard?.weather?.dayHigh) }}</strong>
        </div>
        <div class="metric">
          <span>Last Refresh</span>
          <strong>{{ formatTime(dashboard?.meta?.lastRefresh) }}</strong>
        </div>
      </div>
    </header>

    <main class="grid">
      <section class="panel">
        <div class="panel-header">
          <div>
            <h2>Weather (Hourly)</h2>
            <p>Forecast vs Actual — Asia/Seoul</p>
          </div>
          <div class="legend">
            <span class="dot actual"></span> Actual
            <span class="dot forecast"></span> Forecast
          </div>
        </div>
        <div class="chart">
          <svg viewBox="0 0 1000 260" role="img" aria-label="Temperature chart">
            <defs>
              <linearGradient id="forecastGlow" x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stop-color="#f2b04c" stop-opacity="0.4" />
                <stop offset="100%" stop-color="#f2b04c" stop-opacity="0" />
              </linearGradient>
            </defs>
            <rect x="0" y="0" width="1000" height="260" class="chart-bg" />
            <g class="grid-lines">
              <line v-for="n in 4" :key="n" :x1="40" :x2="960" :y1="40 + n * 40" :y2="40 + n * 40" />
            </g>
            <path v-if="chart.forecastPath" :d="chart.forecastPath" class="line forecast" />
            <path v-if="chart.actualPath" :d="chart.actualPath" class="line actual" />
          </svg>
          <div class="axis">
            <span v-for="label in axisLabels" :key="label">{{ label }}</span>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2>Market Prices</h2>
            <p>Polymarket CLOB (Yes price)</p>
          </div>
          <span class="badge" :class="dashboard?.meta?.eventFound ? 'ok' : 'warn'">
            {{ dashboard?.meta?.eventFound ? "Event Found" : "Event Missing" }}
          </span>
        </div>
        <div class="table market-table">
          <div class="row header">
            <span>Outcome</span>
            <span class="right">Price</span>
          </div>
          <div v-for="outcome in marketOutcomes" :key="outcome.tokenId || outcome.title" class="row">
            <span>{{ outcome.title || "—" }}</span>
            <span class="right">{{ formatPrice(outcome.price) }}</span>
          </div>
          <div v-if="marketOutcomes.length === 0" class="row empty">
            <span>No outcomes available</span>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2>Portfolio</h2>
            <p>CLOB collateral balance and positions</p>
          </div>
          <div class="balance">
            <span>Balance</span>
            <strong>{{ formatBalance(dashboard?.portfolio?.balance) }}</strong>
          </div>
        </div>
        <div class="table positions-table">
          <div class="row header">
            <span>Market</span>
            <span class="right">Size</span>
            <span class="right">Avg</span>
          </div>
          <div v-for="pos in positions" :key="pos.key" class="row">
            <span class="truncate">{{ pos.market }}</span>
            <span class="right">{{ pos.size }}</span>
            <span class="right">{{ pos.avg }}</span>
          </div>
          <div v-if="positions.length === 0" class="row empty">
            <span>No positions</span>
          </div>
        </div>
      </section>
    </main>

    <footer class="footer">
      <span>Polling every 30s · Data cached server-side</span>
      <span v-if="error" class="error">{{ error }}</span>
    </footer>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";

type Dashboard = {
  meta: {
    lastRefresh: string;
    kstDate: string;
    slug: string;
    eventFound: boolean;
  };
  weather: {
    hourly: {
      times: string[];
      forecast: Array<number | null>;
      actual: Array<number | null>;
    };
    dayHigh: number | null;
  };
  market: {
    eventTitle: string | null;
    outcomes: Array<{ title: string | null; tokenId: string | null; price: number | null }>;
  };
  portfolio: {
    balance: Record<string, unknown> | null;
    positions: Array<Record<string, unknown>>;
  };
};

const dashboard = ref<Dashboard | null>(null);
const error = ref<string | null>(null);
let timer: number | undefined;

const marketOutcomes = computed(() => dashboard.value?.market?.outcomes ?? []);

const positions = computed(() => {
  const raw = dashboard.value?.portfolio?.positions ?? [];
  return raw.slice(0, 6).map((pos, idx) => {
    const market =
      (pos as any)?.market?.title ||
      (pos as any)?.market?.question ||
      (pos as any)?.market?.slug ||
      (pos as any)?.market ||
      (pos as any)?.asset ||
      "—";
    const size = (pos as any)?.size ?? (pos as any)?.position_size ?? "—";
    const avg = (pos as any)?.average_price ?? (pos as any)?.avg_price ?? "—";
    return { key: `${market}-${idx}`, market, size, avg };
  });
});

const axisLabels = computed(() => ["00:00", "06:00", "12:00", "18:00", "24:00"]);

const chart = computed(() => {
  const forecast = dashboard.value?.weather?.hourly?.forecast ?? [];
  const actual = dashboard.value?.weather?.hourly?.actual ?? [];
  const temps = [...forecast, ...actual].filter((v) => typeof v === "number") as number[];
  if (temps.length === 0) {
    return { forecastPath: "", actualPath: "" };
  }
  const min = Math.min(...temps) - 1;
  const max = Math.max(...temps) + 1;
  return {
    forecastPath: buildPath(forecast, min, max),
    actualPath: buildPath(actual, min, max),
  };
});

const buildPath = (values: Array<number | null>, min: number, max: number) => {
  const width = 1000;
  const height = 260;
  const padX = 40;
  const padY = 20;
  const step = values.length > 1 ? (width - padX * 2) / (values.length - 1) : 0;

  const scaleY = (val: number) => {
    if (max === min) return height / 2;
    const pct = (max - val) / (max - min);
    return padY + pct * (height - padY * 2);
  };

  let path = "";
  let started = false;
  values.forEach((val, index) => {
    if (val === null || typeof val !== "number") {
      started = false;
      return;
    }
    const x = padX + index * step;
    const y = scaleY(val);
    path += `${started ? "L" : "M"} ${x.toFixed(2)} ${y.toFixed(2)} `;
    started = true;
  });
  return path;
};

const load = async () => {
  try {
    const res = await fetch("/api/dashboard");
    if (!res.ok) throw new Error(`API ${res.status}`);
    dashboard.value = await res.json();
    error.value = null;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Failed to load";
  }
};

onMounted(() => {
  load();
  timer = window.setInterval(load, 30000);
});

onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
});

const formatTemp = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return `${val.toFixed(1)}°C`;
};

const formatPrice = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return val.toFixed(3);
};

const formatBalance = (balance: Record<string, unknown> | null | undefined) => {
  if (!balance) return "—";
  const candidateKeys = ["balance", "available", "collateral", "amount", "collateralBalance"];
  for (const key of candidateKeys) {
    const value = (balance as any)[key];
    if (typeof value === "number") return value.toFixed(2);
    if (typeof value === "string") return value;
  }
  if (Array.isArray(balance) && balance.length > 0) {
    const first = balance[0] as any;
    if (typeof first?.balance === "string") return first.balance;
  }
  return "—";
};

const formatTime = (ts: string | undefined) => {
  if (!ts) return "—";
  try {
    const dt = new Date(ts);
    return dt.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
  } catch {
    return "—";
  }
};
</script>
