(() => {
  'use strict';

  const EXT = 'reachy-embodiment-panel';
  if (window.__hermesReachyEmbodimentPanelLoaded) return;
  window.__hermesReachyEmbodimentPanelLoaded = true;

  const IDS = {
    root: 'hwxReachyPanel',
    button: 'hwxReachyPanelButton',
    label: 'hwxReachyPanelLabel',
    summary: 'hwxReachyPanelSummary',
    drawer: 'hwxReachyPanelDrawer',
    body: 'hwxReachyPanelBody',
    close: 'hwxReachyPanelClose'
  };
  const DEFAULTS = {
    enabled: true,
    visibility_mode: 'attention',
    daemon_url: 'http://reachy-mini.local:8000',
    dashboard_url: 'http://reachy-mini.local:7860',
    poll_seconds: 20
  };
  const READ_ENDPOINTS = [
    ['daemon', '/api/daemon/status'],
    ['lock', '/api/daemon/robot-app-lock-status'],
    ['motors', '/api/motors/status'],
    ['media', '/api/media/status'],
    ['moves', '/api/move/running']
  ];

  let state = { results: {}, error: null, opened: false, timer: null, lastFetch: 0 };

  function settings() {
    let api = null;
    try { api = window.HermesExtensionSettings && window.HermesExtensionSettings.settingsForExtension ? window.HermesExtensionSettings.settingsForExtension(EXT) : null; } catch (_) { api = null; }
    function get(key) {
      try {
        if (api && typeof api.get === 'function') {
          const value = api.get(key);
          if (value !== undefined && value !== null && value !== '') return value;
        }
      } catch (_) {}
      return DEFAULTS[key];
    }
    return {
      enabled: get('enabled') !== false,
      visibility_mode: String(get('visibility_mode') || DEFAULTS.visibility_mode),
      daemon_url: cleanBaseUrl(get('daemon_url') || DEFAULTS.daemon_url),
      dashboard_url: cleanBaseUrl(get('dashboard_url') || DEFAULTS.dashboard_url),
      poll_seconds: clampInt(get('poll_seconds'), 10, 180, DEFAULTS.poll_seconds)
    };
  }

  function clampInt(value, min, max, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, Math.round(n)));
  }

  function cleanBaseUrl(raw) {
    try {
      const url = new URL(String(raw || '').trim());
      if (!['http:', 'https:'].includes(url.protocol)) return '';
      url.pathname = url.pathname.replace(/\/+$/, '');
      url.search = '';
      url.hash = '';
      return url.toString().replace(/\/$/, '');
    } catch (_) { return ''; }
  }

  function endpointUrl(base, path) { return base + path; }

  async function fetchJson(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 4000);
    try {
      const response = await fetch(url, { cache: 'no-store', credentials: 'omit', signal: controller.signal, headers: { Accept: 'application/json' } });
      if (!response.ok) throw new Error('HTTP ' + response.status);
      return await response.json();
    } finally {
      clearTimeout(timer);
    }
  }

  async function refresh() {
    const s = settings();
    if (!s.enabled || !s.daemon_url) {
      render();
      schedule();
      return;
    }
    const entries = await Promise.all(READ_ENDPOINTS.map(async ([key, path]) => {
      try { return [key, { ok: true, data: await fetchJson(endpointUrl(s.daemon_url, path)) }]; }
      catch (error) { return [key, { ok: false, error: String(error && error.message ? error.message : error) }]; }
    }));
    state.results = Object.fromEntries(entries);
    state.error = null;
    state.lastFetch = Date.now();
    render();
    schedule();
  }

  function schedule() {
    clearTimeout(state.timer);
    const s = settings();
    const delay = (document.visibilityState === 'hidden' ? Math.max(45, s.poll_seconds * 2) : s.poll_seconds) * 1000;
    state.timer = setTimeout(refresh, delay);
  }

  function ensureDom() {
    let root = document.getElementById(IDS.root);
    if (root) return root;
    root = document.createElement('section');
    root.id = IDS.root;
    root.className = 'hwx-rp';
    root.setAttribute('aria-live', 'polite');

    const button = document.createElement('button');
    button.id = IDS.button;
    button.type = 'button';
    button.className = 'hwx-rp-trigger';
    button.title = 'Open Reachy embodiment panel';
    button.innerHTML = '<span class="hwx-rp-dot" aria-hidden="true"></span><span id="' + IDS.label + '">Reachy</span><span id="' + IDS.summary + '" class="hwx-rp-summary">checking</span>';
    button.addEventListener('click', () => toggleDrawer());

    const drawer = document.createElement('aside');
    drawer.id = IDS.drawer;
    drawer.className = 'hwx-rp-drawer';
    drawer.hidden = true;
    drawer.innerHTML = '<header class="hwx-rp-head"><div><strong>Reachy Embodiment</strong><span>Read-only robot/body status</span></div><button id="' + IDS.close + '" type="button" aria-label="Close">×</button></header><div id="' + IDS.body + '" class="hwx-rp-body"></div>';

    root.append(button, drawer);
    document.body.appendChild(root);
    const close = document.getElementById(IDS.close);
    if (close) close.addEventListener('click', closeDrawer);
    return root;
  }

  function classify() {
    const r = state.results || {};
    const keys = Object.keys(r);
    if (!keys.length) return { level: 'unknown', label: 'Reachy unknown', reason: 'No status read yet' };
    const failures = keys.filter((key) => !r[key].ok);
    if (failures.length === keys.length) return { level: 'offline', label: 'Reachy offline', reason: 'Daemon not reachable from browser' };
    if (failures.length) return { level: 'warn', label: 'Reachy partial', reason: failures.join(', ') + ' unavailable' };
    const lock = r.lock && r.lock.data;
    const motors = r.motors && r.motors.data;
    const media = r.media && r.media.data;
    const moves = r.moves && r.moves.data;
    const moving = Array.isArray(moves) ? moves.length > 0 : !!(moves && (moves.running || moves.moves));
    if (moving) return { level: 'active', label: 'Reachy moving', reason: 'Movement is currently reported as running' };
    const motorText = JSON.stringify(motors || {}).toLowerCase();
    if (motorText.includes('error') || motorText.includes('fault')) return { level: 'warn', label: 'Reachy attention', reason: 'Motor status mentions error/fault' };
    const locked = lock && (lock.locked === true || lock.is_locked === true || lock.app_locked === true);
    const muted = media && (media.muted === true || media.is_muted === true);
    const reason = [locked ? 'app lock active' : 'app lock clear', muted ? 'media muted' : 'media ready'].join(' · ');
    return { level: 'ok', label: 'Reachy ready', reason };
  }

  function shouldShow(diag, s) {
    if (!s.enabled) return false;
    if (state.opened) return true;
    if (s.visibility_mode === 'always') return true;
    if (s.visibility_mode === 'hidden') return false;
    return ['offline', 'warn', 'active'].includes(diag.level);
  }

  function render() {
    const s = settings();
    const root = ensureDom();
    const diag = classify();
    root.dataset.status = diag.level;
    root.hidden = !shouldShow(diag, s);
    const label = document.getElementById(IDS.label);
    const summary = document.getElementById(IDS.summary);
    if (label) label.textContent = diag.label;
    if (summary) summary.textContent = diag.reason;
    renderDrawer(diag, s);
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function compactJson(value) {
    try { return JSON.stringify(value, null, 2).slice(0, 900); }
    catch (_) { return String(value); }
  }

  function renderDrawer(diag, s) {
    const body = document.getElementById(IDS.body);
    if (!body) return;
    const rows = READ_ENDPOINTS.map(([key]) => {
      const item = state.results[key];
      const cls = item && item.ok ? 'ok' : 'bad';
      const text = item ? (item.ok ? compactJson(item.data) : item.error) : 'not read yet';
      return '<details class="hwx-rp-item hwx-rp-item--' + cls + '"><summary><span>' + escapeHtml(key) + '</span><b>' + escapeHtml(item && item.ok ? 'ok' : 'unavailable') + '</b></summary><pre>' + escapeHtml(text) + '</pre></details>';
    }).join('');
    const dash = s.dashboard_url ? '<a href="' + escapeHtml(s.dashboard_url) + '" target="_blank" rel="noopener noreferrer">Open dashboard</a>' : '';
    body.innerHTML = '<section><h4>Status</h4><p class="hwx-rp-muted">' + escapeHtml(diag.reason) + '</p><div class="hwx-rp-actions">' + dash + '<button type="button" id="hwxReachyPanelRefresh">Refresh</button></div></section><section><h4>Read-only probes</h4>' + rows + '</section><p class="hwx-rp-foot">No POST requests. No movement, mic, speaker, or camera capture.</p>';
    const refreshBtn = document.getElementById('hwxReachyPanelRefresh');
    if (refreshBtn) refreshBtn.addEventListener('click', refresh);
  }

  function openDrawer() { state.opened = true; ensureDom(); const drawer = document.getElementById(IDS.drawer); if (drawer) drawer.hidden = false; render(); }
  function closeDrawer() { state.opened = false; const drawer = document.getElementById(IDS.drawer); if (drawer) drawer.hidden = true; render(); }
  function toggleDrawer() { state.opened ? closeDrawer() : openDrawer(); }

  window.HermesReachyEmbodimentPanel = { open: openDrawer, close: closeDrawer, refresh, status: () => ({ results: state.results, diagnostic: classify() }) };
  document.addEventListener('keydown', (ev) => {
    if (ev.ctrlKey && ev.shiftKey && String(ev.key || '').toLowerCase() === 'r') {
      ev.preventDefault();
      toggleDrawer();
    }
  });
  document.addEventListener('visibilitychange', schedule);
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', refresh, { once: true });
  else refresh();
})();
