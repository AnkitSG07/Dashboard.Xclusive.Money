(function initializePagePrefetchers() {
  if (typeof window === 'undefined') {
    return;
  }

  const PREFETCH_KEY = {
    copyTrading: 'copy-trading',
    alerts: 'create-alerts',
    groups: 'groups',
    addAccount: 'Add-Account',
    dematStrategies: 'demat-strategies'
  };

  const baseTtl = Number.isFinite(window.PAGE_PREFETCH_DEFAULT_TTL)
    ? Math.max(window.PAGE_PREFETCH_DEFAULT_TTL, 60 * 1000)
    : 2 * 60 * 1000;

  const TTL = {
    copyTrading: Math.max(baseTtl / 2, 45 * 1000),
    alerts: Math.max(baseTtl * 2, 4 * 60 * 1000),
    groups: Math.max(baseTtl * 1.5, 3 * 60 * 1000),
    addAccount: Math.max(baseTtl, 90 * 1000),
    dematStrategies: Math.max(baseTtl * 2, 4 * 60 * 1000)
  };

  function computeAccountsSignature(list) {
    try {
      return JSON.stringify(Array.isArray(list) ? list : []);
    } catch (error) {
      console.warn('Failed to serialize accounts payload for cache', error);
      return String(Date.now());
    }
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(response => {
      if (!response.ok) {
        const error = new Error('Request failed');
        error.status = response.status;
        throw error;
      }
      return response.json();
    });
  }

  function registerCopyTradingPrefetcher() {
    if (typeof window.registerPagePrefetcher !== 'function') {
      return;
    }
    window.registerPagePrefetcher(PREFETCH_KEY.copyTrading, async ({ setCached }) => {
      const data = await fetchJson('/api/accounts');
      setCached(data, { ttl: TTL.copyTrading });
      return data;
    });
  }

  function registerAlertsPrefetcher() {
    if (typeof window.registerPagePrefetcher !== 'function') {
      return;
    }
    window.registerPagePrefetcher(PREFETCH_KEY.alerts, async ({ setCached }) => {
      const [accountsResult, symbolsResult] = await Promise.allSettled([
        fetchJson('/api/accounts?cache_only=1'),
        fetchJson('/api/symbols')
      ]);

      const payload = {
        accountPayload: accountsResult.status === 'fulfilled' ? accountsResult.value : null,
        symbols: symbolsResult.status === 'fulfilled' ? symbolsResult.value : [],
        fetchedAt: Date.now()
      };
      setCached(payload, { ttl: TTL.alerts });
      return payload;
    });
  }

  function registerGroupsPrefetcher() {
    if (typeof window.registerPagePrefetcher !== 'function') {
      return;
    }
    window.registerPagePrefetcher(PREFETCH_KEY.groups, async ({ setCached }) => {
      const [groupsResult, accountsResult, symbolsResult] = await Promise.allSettled([
        fetchJson('/api/groups'),
        fetchJson('/api/accounts'),
        fetchJson('/api/symbols')
      ]);

      const payload = {
        groups: groupsResult.status === 'fulfilled' ? groupsResult.value : [],
        accounts: accountsResult.status === 'fulfilled'
          ? (Array.isArray(accountsResult.value?.accounts)
            ? accountsResult.value.accounts
            : accountsResult.value)
          : [],
        symbols: symbolsResult.status === 'fulfilled' ? symbolsResult.value : [],
        fetchedAt: Date.now()
      };
      setCached(payload, { ttl: TTL.groups });
      return payload;
    });
  }

  function registerAddAccountPrefetcher() {
    if (typeof window.registerPagePrefetcher !== 'function') {
      return;
    }
    window.registerPagePrefetcher(PREFETCH_KEY.addAccount, async ({ setCached }) => {
      const response = await fetchJson('/api/accounts?cache_only=1');
      const accounts = Array.isArray(response?.accounts) ? response.accounts : [];
      const payload = {
        accounts,
        signature: computeAccountsSignature(accounts),
        fetchedAt: Date.now()
      };
      setCached(payload, { ttl: TTL.addAccount });
      return payload;
    });
  }

  function registerDematStrategiesPrefetcher() {
    if (typeof window.registerPagePrefetcher !== 'function') {
      return;
    }
    window.registerPagePrefetcher(PREFETCH_KEY.dematStrategies, async ({ setCached }) => {
      const [accountsResult, strategiesResult] = await Promise.allSettled([
        fetchJson('/api/accounts?cache_only=1'),
        fetchJson('/api/strategies')
      ]);

      const payload = {
        accountsPayload: accountsResult.status === 'fulfilled' ? accountsResult.value : null,
        strategies: strategiesResult.status === 'fulfilled' ? strategiesResult.value : [],
        fetchedAt: Date.now()
      };
      setCached(payload, { ttl: TTL.dematStrategies });
      return payload;
    });
  }

  function init() {
    registerCopyTradingPrefetcher();
    registerAlertsPrefetcher();
    registerGroupsPrefetcher();
    registerAddAccountPrefetcher();
    registerDematStrategiesPrefetcher();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
