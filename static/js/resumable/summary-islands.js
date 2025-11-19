const HOLDINGS_LIMIT = 5;
const TRANSACTIONS_LIMIT = 5;
const PERIOD_LIMITS = {
  "1D": 1,
  "1W": 7,
  "1M": 30,
  "3M": 90,
  "6M": 180,
  "1Y": 365,
  All: null,
};

const SUMMARY_PREFETCH_GLOBALS = (() => {
  if (typeof window === "undefined") {
    return {
      key: "page:summary-data",
      ttl: 3 * 60 * 1000,
      keys: {}
    };
  }
  const keys = window.PAGE_PREFETCH_KEYS || {};
  const baseTtl = Number.isFinite(window.PAGE_PREFETCH_DEFAULT_TTL)
    ? window.PAGE_PREFETCH_DEFAULT_TTL * 3
    : 3 * 60 * 1000;
  return {
    key: keys.summary || "page:summary-data",
    ttl: baseTtl,
    keys
  };
})();

const SUMMARY_PREFETCH_KEY = SUMMARY_PREFETCH_GLOBALS.key;
const SUMMARY_PREFETCH_TTL = SUMMARY_PREFETCH_GLOBALS.ttl;
const CRITICAL_PREFETCH_KEYS = [
  SUMMARY_PREFETCH_KEY,
  SUMMARY_PREFETCH_GLOBALS.keys.accountInfo,
  SUMMARY_PREFETCH_GLOBALS.keys.dashboard,
  SUMMARY_PREFETCH_GLOBALS.keys.notifications
].filter((key, index, list) => key && list.indexOf(key) === index);

let summaryPrefetchRegistered = false;
let criticalPrefetchPrimed = false;

function readSummaryPrefetchCache() {
  if (typeof window === "undefined" || typeof window.getPagePrefetchData !== "function") {
    return undefined;
  }
  return window.getPagePrefetchData(SUMMARY_PREFETCH_KEY);
}

function storeSummaryPrefetch(payload) {
  if (typeof window === "undefined" || typeof window.setPagePrefetchData !== "function") {
    return;
  }
  window.setPagePrefetchData(SUMMARY_PREFETCH_KEY, payload, { ttl: SUMMARY_PREFETCH_TTL });
}

async function fetchSummaryPayload() {
  const response = await fetch("/api/summary", { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error("Failed to load summary data");
  }
  return response.json();
}

async function requestSummaryData(forceNetwork = false) {
  if (!forceNetwork) {
    const cached = readSummaryPrefetchCache();
    if (cached !== undefined && cached !== null) {
      return cached;
    }
  }

  if (typeof window !== "undefined" && typeof window.runPagePrefetch === "function") {
    try {
      const prefetched = await window.runPagePrefetch(SUMMARY_PREFETCH_KEY, { force: forceNetwork });
      if (prefetched !== undefined && prefetched !== null) {
        return prefetched;
      }
    } catch (error) {
      console.warn("Summary prefetch runner failed", error);
    }
  }

  const payload = await fetchSummaryPayload();
  storeSummaryPrefetch(payload);
  return payload;
}

function ensureSummaryPrefetcher() {
  if (summaryPrefetchRegistered) return;
  if (typeof window === "undefined" || typeof window.registerPagePrefetcher !== "function") {
    return;
  }
  summaryPrefetchRegistered = true;
  window.registerPagePrefetcher(SUMMARY_PREFETCH_KEY, async ({ setCached }) => {
    const data = await fetchSummaryPayload();
    setCached(data, { ttl: SUMMARY_PREFETCH_TTL });
    return data;
  });
}

function primeCriticalPrefetchers() {
  if (criticalPrefetchPrimed) return;
  if (typeof window === "undefined" || typeof window.runPagePrefetch !== "function") {
    return;
  }
  criticalPrefetchPrimed = true;
  CRITICAL_PREFETCH_KEYS.forEach((key, index) => {
    setTimeout(() => {
      window.runPagePrefetch(key).catch(() => {});
    }, index * 120);
  });
}

ensureSummaryPrefetcher();

function parseJsonScript(id) {
  const el = document.getElementById(id);
  if (!el) return {};
  try {
    return JSON.parse(el.textContent || "{}") || {};
  } catch (error) {
    console.warn("Failed to parse summary props", error);
    return {};
  }
}

function normalizeBrokerName(value) {
  if (!value) return "";
  return String(value).toLowerCase().trim();
}

function extractBrokers(element, cache) {
  if (!element) return [];
  if (cache.has(element)) return cache.get(element);
  const rawValue = element.dataset?.brokers;
  let brokerList = [];
  if (rawValue) {
    try {
      const parsed = JSON.parse(rawValue);
      if (Array.isArray(parsed)) {
        brokerList = parsed.map((item) => String(item));
      }
    } catch (error) {
      brokerList = String(rawValue)
        .split(/\s*,\s*/)
        .map((item) => item.trim())
        .filter(Boolean);
    }
  }
  cache.set(element, brokerList);
  return brokerList;
}

function setupHoldingsFilter() {
  const brokerCache = new WeakMap();
  const brokerFilter = document.getElementById("top-holdings-broker-filter");
  const holdingsTableContainer = document.querySelector(".top-holdings-table-container");
  const holdingsTableRows = holdingsTableContainer
    ? Array.from(holdingsTableContainer.querySelectorAll("tbody tr"))
    : [];
  const holdingsCardList = document.querySelector(".top-holdings-card-list");
  const holdingsCardItems = holdingsCardList
    ? Array.from(holdingsCardList.querySelectorAll(".top-holdings-card"))
    : [];
  const holdingsEmptyState = document.getElementById("top-holdings-empty-state");
  const holdingsToggle = document.getElementById("top-holdings-toggle");

  let holdingsShowAll = false;

  function matchesBroker(element, normalizedSelection, showAllSelection) {
    if (!element) return false;
    if (showAllSelection) return true;
    const brokers = extractBrokers(element, brokerCache)
      .map((name) => normalizeBrokerName(name))
      .filter(Boolean);
    if (!brokers.length) return false;
    return brokers.includes(normalizedSelection);
  }

  function applyBrokerFilter(selectedBroker) {
    const normalizedSelection = normalizeBrokerName(
      typeof selectedBroker === "string" ? selectedBroker : "",
    );
    const showAllSelection = !normalizedSelection || normalizedSelection === "all";

    let visibleRows = 0;
    let visibleCards = 0;
    let tableMatchCount = 0;
    let cardMatchCount = 0;

    holdingsTableRows.forEach((row) => {
      const isMatch = matchesBroker(row, normalizedSelection, showAllSelection);
      if (isMatch) {
        const shouldShow = holdingsShowAll || tableMatchCount < HOLDINGS_LIMIT;
        row.style.display = shouldShow ? "" : "none";
        tableMatchCount += 1;
        if (shouldShow) visibleRows += 1;
      } else {
        row.style.display = "none";
      }
    });

    holdingsCardItems.forEach((card) => {
      const isMatch = matchesBroker(card, normalizedSelection, showAllSelection);
      if (isMatch) {
        const shouldShow = holdingsShowAll || cardMatchCount < HOLDINGS_LIMIT;
        card.style.display = shouldShow ? "" : "none";
        cardMatchCount += 1;
        if (shouldShow) visibleCards += 1;
      } else {
        card.style.display = "none";
      }
    });

    if (holdingsTableContainer) {
      holdingsTableContainer.hidden = holdingsTableRows.length > 0 && visibleRows === 0;
    }
    if (holdingsCardList) {
      holdingsCardList.hidden = holdingsCardItems.length > 0 && visibleCards === 0;
    }
    if (holdingsEmptyState) {
      const hasVisible = visibleRows + visibleCards > 0;
      holdingsEmptyState.hidden = hasVisible;
    }

    const totalMatches = Math.max(tableMatchCount, cardMatchCount);
    if (totalMatches <= HOLDINGS_LIMIT && holdingsShowAll) {
      holdingsShowAll = false;
    }

    if (holdingsToggle) {
      holdingsToggle.hidden = totalMatches <= HOLDINGS_LIMIT;
      if (!holdingsToggle.hidden) {
        holdingsToggle.textContent = holdingsShowAll ? "View Less" : "View All";
      }
    }
  }

  function toggleHoldingsView() {
    const totalHoldings = Math.max(holdingsTableRows.length, holdingsCardItems.length);
    if (totalHoldings <= HOLDINGS_LIMIT) return;
    holdingsShowAll = !holdingsShowAll;
    applyBrokerFilter(brokerFilter ? brokerFilter.value : "");
  }

  if (brokerFilter) {
    applyBrokerFilter(brokerFilter.value);
    brokerFilter.addEventListener("change", () => {
      applyBrokerFilter(brokerFilter.value);
    });
  } else {
    applyBrokerFilter("");
  }

  if (holdingsToggle) {
    holdingsToggle.addEventListener("click", (event) => {
      event.preventDefault();
      toggleHoldingsView();
    });
  }
}

function setupTransactionsToggle() {
  const transactionsToggle = document.getElementById("recent-transactions-toggle");
  const transactionItems = Array.from(document.querySelectorAll(".summary-transaction"));
  let transactionsShowAll = false;

  function updateTransactionsVisibility() {
    if (!transactionItems.length) {
      if (transactionsToggle) transactionsToggle.hidden = true;
      return;
    }
    const shouldCollapse = transactionItems.length > TRANSACTIONS_LIMIT;
    if (!shouldCollapse && transactionsShowAll) {
      transactionsShowAll = false;
    }
    transactionItems.forEach((item, index) => {
      const shouldShow = transactionsShowAll || index < TRANSACTIONS_LIMIT;
      item.style.display = shouldShow ? "" : "none";
    });
    if (transactionsToggle) {
      transactionsToggle.hidden = !shouldCollapse;
      if (shouldCollapse) {
        transactionsToggle.textContent = transactionsShowAll ? "View Less" : "View All";
      }
    }
  }

  updateTransactionsVisibility();
  if (transactionsToggle) {
    transactionsToggle.addEventListener("click", (event) => {
      event.preventDefault();
      if (transactionItems.length <= TRANSACTIONS_LIMIT) return;
      transactionsShowAll = !transactionsShowAll;
      updateTransactionsVisibility();
    });
  }
}

function setupPerformanceChart(props) {
  const canvas = document.getElementById("portfolio-performance-chart");
  const emptyState = document.getElementById("portfolio-performance-empty");
  const loadingState = document.getElementById("portfolio-performance-loading");
  const chips = Array.from(document.querySelectorAll(".summary-chip[data-period]"));
  const chartGlobal = window.Chart;

  if (!canvas) return;

  const hasChart = typeof chartGlobal === "function";
  if (!hasChart) {
    canvas.setAttribute("hidden", "");
    if (emptyState) emptyState.hidden = false;
    return;
  }

  const warmedCache = props?.warmed_cache || {};
  const prefetchedSummary = readSummaryPrefetchCache();
  const initialSeries = props?.performance_summary?.series || [];
  let cachedSeries = [];
  let isLoadingSeries = false;
  let chartInstance = null;

  const hasTimeScale = !!(
    chartGlobal?.registry?.getScale?.("time") && chartGlobal?._adapters?.date
  );
  const xScaleType = hasTimeScale ? "time" : "linear";

  function normalizeSeries(series) {
    if (!Array.isArray(series)) return [];
    return series
      .map((point) => {
        if (!point) return null;
        const date = new Date(point.timestamp);
        if (Number.isNaN(date.getTime())) return null;
        const value = Number(point.value);
        if (!Number.isFinite(value)) return null;
        return { timestamp: date.toISOString(), value };
      })
      .filter(Boolean)
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }

  function mapToDataset(series) {
    return series.map((point) => {
      const date = new Date(point.timestamp);
      const xValue = hasTimeScale ? date : date.getTime();
      return { x: xValue, y: Number(point.value) };
    });
  }

  function ensureChart() {
    if (chartInstance) return chartInstance;
    const context = canvas.getContext("2d");
    chartInstance = new chartGlobal(context, {
      type: "line",
      data: {
        datasets: [
          {
            label: "Portfolio Performance",
            data: [],
            borderColor: "rgba(79, 70, 229, 0.9)",
            backgroundColor: "rgba(79, 70, 229, 0.12)",
            borderWidth: 2,
            tension: 0.3,
            fill: true,
            pointRadius: 0,
            pointHoverRadius: 3,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        parsing: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: {
            type: xScaleType,
            time: hasTimeScale ? { tooltipFormat: "MMM d, yyyy HH:mm" } : undefined,
            grid: { color: "rgba(107, 114, 128, 0.15)" },
            ticks: {
              color: "#6b7280",
              maxTicksLimit: 8,
              callback(value) {
                if (hasTimeScale) return this.getLabelForValue(value);
                const date = new Date(Number(value));
                if (Number.isNaN(date.getTime())) return "";
                return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
              },
            },
          },
          y: {
            grid: { color: "rgba(107, 114, 128, 0.15)" },
            ticks: {
              color: "#6b7280",
              callback(value) {
                return `₹${Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
              },
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            mode: "index",
            intersect: false,
            callbacks: {
              label(context) {
                const value = context.parsed.y;
                return `₹${Number(value).toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}`;
              },
            },
          },
        },
      },
    });
    return chartInstance;
  }

  function setLoadingState(isLoading) {
    isLoadingSeries = Boolean(isLoading);
    if (loadingState) loadingState.hidden = !isLoadingSeries;
  }

  function toggleState(hasData) {
    if (hasData) {
      canvas.removeAttribute("hidden");
      if (emptyState) emptyState.hidden = true;
      setLoadingState(false);
      return;
    }
    canvas.setAttribute("hidden", "");
    if (isLoadingSeries) {
      if (emptyState) emptyState.hidden = true;
      setLoadingState(true);
      return;
    }
    if (emptyState) emptyState.hidden = false;
    setLoadingState(false);
  }

  function updateChart(series) {
    const chart = ensureChart();
    const hasData = Array.isArray(series) && series.length > 0;
    toggleState(hasData);
    if (!hasData) {
      chart.data.datasets[0].data = [];
      chart.update("none");
      return;
    }
    chart.data.datasets[0].data = mapToDataset(series);
    chart.update("none");
  }

  function filterSeries(periodKey) {
    if (!cachedSeries.length) return [];
    const limitDays = PERIOD_LIMITS[periodKey] ?? null;
    if (!limitDays) return cachedSeries.slice();
    const now = Date.now();
    const threshold = now - limitDays * 24 * 60 * 60 * 1000;
    return cachedSeries.filter((point) => {
      const time = Date.parse(point.timestamp);
      return Number.isFinite(time) && time >= threshold;
    });
  }

  function setActiveChip(period) {
    chips.forEach((chip) => {
      chip.classList.toggle("is-active", chip.dataset.period === period);
    });
  }

  const periodOrder = chips
    .map((chip) => chip.dataset.period)
    .filter((period, index, arr) => period && arr.indexOf(period) === index);

  let activePeriod =
    chips.find((chip) => chip.classList.contains("is-active"))?.dataset.period || "1D";
  if (!activePeriod || !(activePeriod in PERIOD_LIMITS)) {
    activePeriod = periodOrder[0] || "All";
  }

  function findFallbackSeries(excludePeriod) {
    if (!cachedSeries.length) return { period: excludePeriod, series: [] };
    const candidateOrder = periodOrder.length ? periodOrder : Object.keys(PERIOD_LIMITS);
    for (const candidate of candidateOrder) {
      if (!candidate || candidate === excludePeriod) continue;
      const candidateSeries = filterSeries(candidate);
      if (candidateSeries.length) return { period: candidate, series: candidateSeries };
    }
    return { period: excludePeriod, series: cachedSeries.slice() };
  }

  function applyPeriod(period) {
    if (!period || !(period in PERIOD_LIMITS)) period = activePeriod;
    let resolvedPeriod = period;
    let series = filterSeries(period);
    if (!series.length && cachedSeries.length) {
      const fallback = findFallbackSeries(period);
      resolvedPeriod = fallback.period;
      series = fallback.series;
    }
    activePeriod = resolvedPeriod;
    setActiveChip(resolvedPeriod);
    updateChart(series);
  }

  function refreshSeries(forceNetwork = false) {
    setLoadingState(!cachedSeries.length);
    requestSummaryData(forceNetwork)
      .then((data) => {
        const incoming = data?.performance_summary?.series;
        if (!Array.isArray(incoming)) {
          setLoadingState(false);
          if (!cachedSeries.length) toggleState(false);
          return;
        }
        cachedSeries = normalizeSeries(incoming);
        setLoadingState(false);
        applyPeriod(activePeriod);
      })
      .catch(() => {
        setLoadingState(false);
        if (!cachedSeries.length) toggleState(false);
      });
  }

  const warmSeries = normalizeSeries(warmedCache.summary_series || warmedCache.summarySeries || []);
  const prefetchedSeries = normalizeSeries(prefetchedSummary?.performance_summary?.series || []);
  cachedSeries = normalizeSeries(initialSeries);
  if (!cachedSeries.length && prefetchedSeries.length) {
    cachedSeries = prefetchedSeries;
  }
  if (!cachedSeries.length && warmSeries.length) {
    cachedSeries = warmSeries;
  }
  if (cachedSeries.length) {
    applyPeriod(activePeriod);
  } else {
    setLoadingState(true);
    toggleState(false);
    refreshSeries(true);
  }

  const REFRESH_EVENT_KEY = "__summaryRefreshHandler";
  if (window[REFRESH_EVENT_KEY]) {
    document.removeEventListener("summary:refresh", window[REFRESH_EVENT_KEY]);
  }
  const refreshHandler = () => {
    if (typeof window.runPagePrefetch === "function") {
      window.runPagePrefetch(SUMMARY_PREFETCH_KEY, { force: true }).catch(() => {});
    }
    refreshSeries(true);
  };
  window[REFRESH_EVENT_KEY] = refreshHandler;
  document.addEventListener("summary:refresh", refreshHandler);

  chips.forEach((chip) => {
    if (chip.dataset.bound === "true") return;
    chip.dataset.bound = "true";
    const handleActivate = () => applyPeriod(chip.dataset.period);
    chip.addEventListener("click", handleActivate);
    chip.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        handleActivate();
      }
    });
  });
}

export function initSummaryIslands() {
  const props = parseJsonScript("summary-props");
  setupHoldingsFilter();
  setupTransactionsToggle();
  setupPerformanceChart(props);
  primeCriticalPrefetchers();
}
