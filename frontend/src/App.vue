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
          <span>Last Refresh (KST)</span>
          <strong>{{ formatTime(dashboard?.meta?.lastRefresh) }}</strong>
        </div>
      </div>
    </header>

    <main class="grid">
      <section class="panel weather-panel">
        <div class="panel-header">
          <div>
            <h2>Weather (30‑min)</h2>
            <p>Actual — METAR + CheckWX</p>
          </div>
          <div class="legend">
            <span><span class="dot actual"></span> AWC</span>
            <span><span class="dot checkwx"></span> CheckWX</span>
            <span
              v-if="sourceMatch !== null"
              class="badge"
              :class="sourceMatch ? 'ok' : 'warn'"
            >
              {{ sourceMatch ? "Sources match" : `Mismatch ${formatDelta(sourceDelta)}` }}
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
          <span class="badge" :class="dashboard?.meta?.eventFound ? 'ok' : 'warn'">
            {{ dashboard?.meta?.eventFound ? "Event Found" : "Event Missing" }}
          </span>
        </div>
        <div class="table market-table">
          <div class="row header">
            <span class="market-label">Outcome</span>
            <span class="right">Yes</span>
            <span class="right">No</span>
          </div>
          <div
            v-for="outcome in marketOutcomes"
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
            <span class="right price">{{ formatPrice(outcome.yesPrice ?? outcome.price) }}</span>
            <span class="right price">{{ formatPrice(outcome.noPrice) }}</span>
          </div>
          <div v-if="marketOutcomes.length === 0" class="row empty">
            <span>No outcomes available</span>
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
        <span v-if="highNotice" class="notice">{{ highNotice }}</span>
        <span v-if="error" class="error">{{ error }}</span>
      </div>
    </footer>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
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

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

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
      awc: Array<number | null>;
      checkwx: Array<number | null>;
    };
    dayHigh: number | null;
    sources?: {
      awc: {
        latest: number | null;
        latestTime: string | null;
        dayHigh: number | null;
      };
      checkwx: {
        latest: number | null;
        latestTime: string | null;
        dayHigh: number | null;
      };
      match: boolean | null;
      delta: number | null;
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
      volume24hr?: number | null;
    }>;
  };
};

const dashboard = ref<Dashboard | null>(null);
const error = ref<string | null>(null);
const highNotice = ref<string | null>(null);
const enableNotifyTest = ref(false);
const soundEnabled = ref(false);
let timer: number | undefined;
let noticeTimer: number | undefined;
let notifyStateLoaded = false;
let lastNotifiedDate: string | null = null;
let lastNotifiedHigh: number | null = null;
const NOTIFY_STORAGE_KEY = "dayHighNotification";
let audioContext: AudioContext | null = null;
let soundUnlockHandler: (() => void) | null = null;
const SOUND_STORAGE_KEY = "dayHighSoundEnabled";

const marketOutcomes = computed(() => dashboard.value?.market?.outcomes ?? []);

// --- Chart Data & Options ---

const chartData = computed<ChartData<'line'> | null>(() => {
  if (!dashboard.value) return null;
  
  const times = dashboard.value.weather.hourly.times || [];
  const awc = dashboard.value.weather.hourly.awc || [];
  const checkwx = dashboard.value.weather.hourly.checkwx || [];

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
      },
      {
        label: 'CheckWX',
        borderColor: '#d08c2a', // var(--accent-2)
        backgroundColor: '#d08c2a',
        data: checkwx,
        borderDash: [6, 4],
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

onMounted(() => {
  load();
  timer = window.setInterval(load, 30000);
  loadSoundPreference();
  if (typeof window !== "undefined") {
    const params = new URLSearchParams(window.location.search);
    if (params.get("notify") === "button") {
      enableNotifyTest.value = true;
    }
  }
});

onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
  if (noticeTimer) window.clearInterval(noticeTimer);
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

const formatDelta = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  const sign = val > 0 ? "+" : "";
  return `${sign}${val.toFixed(2)}°C`;
};

const formatPrice = (val: number | null | undefined) => {
  if (val === null || val === undefined) return "—";
  return val.toFixed(3);
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
  const kstDate = next?.meta?.kstDate;
  if (typeof dayHigh !== "number" || !kstDate) return;

  if (lastNotifiedDate !== kstDate) {
    lastNotifiedDate = kstDate;
    lastNotifiedHigh = dayHigh;
    persistNotifyState();
    return;
  }

  if (lastNotifiedHigh === null) {
    lastNotifiedHigh = dayHigh;
    persistNotifyState();
    return;
  }

  if (dayHigh > lastNotifiedHigh + 1e-6) {
    const tempText = formatTemp(dayHigh);
    const message = `New day high: ${tempText} (KST)`;
    const title = `New day high: ${tempText}`;
    showHighNotice(message);
    notifyBrowser(title, message);
    if (soundEnabled.value) {
      playBeep();
    }
    lastNotifiedHigh = dayHigh;
    persistNotifyState();
  }
};

const formatTime = (ts: string | undefined) => {
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

const matchesOutcome = (
  title: string | null | undefined,
  value: number
): boolean => {
  if (!title) return false;
  const text = title.toLowerCase();
  const match = text.match(/(-?\d+)\s*°?\s*c/);
  if (!match) return false;
  const numeric = Number.parseInt(match[1], 10);
  if (Number.isNaN(numeric)) return false;
  if (text.includes("or below") || text.includes("or lower")) {
    return value <= numeric;
  }
  if (text.includes("or higher") || text.includes("or above")) {
    return value >= numeric;
  }
  return value === numeric;
};

const highestOutcomeKey = computed(() => {
  const dayHigh = dashboard.value?.weather?.dayHigh;
  if (typeof dayHigh !== "number") return null;
  const target = Math.round(dayHigh);
  for (const outcome of marketOutcomes.value) {
    if (matchesOutcome(outcome.title, target)) {
      return outcomeKey(outcome);
    }
  }
  return null;
});

const sourceMatch = computed(() => {
  const match = dashboard.value?.weather?.sources?.match;
  return typeof match === "boolean" ? match : null;
});

const sourceDelta = computed(
  () => dashboard.value?.weather?.sources?.delta ?? null
);

const isHighestMatch = (outcome: { tokenId: string | null; title: string | null }) =>
  outcomeKey(outcome) === highestOutcomeKey.value;

const isNonTradable = (outcome: { price: number | null; yesPrice?: number | null }) => {
  const price =
    typeof outcome.yesPrice === "number"
      ? outcome.yesPrice
      : typeof outcome.price === "number"
      ? outcome.price
      : null;
  if (price === null) return false;
  return price <= 0.001;
};
</script>
