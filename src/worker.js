const CONFIG_KEY = "settings";
const STATE_KEY = "monitor-state";
const START_LOCK_KEY_PREFIX = "start-lock:";
const DEFAULT_VPS_CRON = "*/5 * * * *";
const DEFAULT_START_COOLDOWN_MINUTES = 15;
const MAX_EVENT_LOGS = 100;

const DEFAULT_SETTINGS = {
  checkIntervalNote: "Worker wakes up every minute; each VPS controls its own cron schedule.",
  defaultFailureThreshold: 2,
  panels: []
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return htmlResponse(APP_HTML);
    }

    if (url.pathname === "/api/health") {
      return jsonResponse({ ok: true });
    }

    if (!url.pathname.startsWith("/api/")) {
      return jsonResponse({ error: "Not found" }, 404);
    }

    const authError = requireAdmin(request, env);
    if (authError) return authError;

    try {
      if (url.pathname === "/api/settings") {
        if (request.method === "GET") {
          return jsonResponse(await getSettings(env));
        }

        if (request.method === "PUT") {
          const body = await request.json();
          const settings = normalizeSettings(body);
          await putJson(env, CONFIG_KEY, settings);
          return jsonResponse(settings);
        }
      }

      if (url.pathname === "/api/state" && request.method === "GET") {
        return jsonResponse(await getState(env));
      }

      if (url.pathname === "/api/logs/clear" && request.method === "POST") {
        const body = await request.json().catch(() => ({}));
        const state = await clearLogs(env, body.scope || "recent");
        return jsonResponse(state);
      }

      if (url.pathname === "/api/panels/test" && request.method === "POST") {
        const panel = normalizePanel(await request.json());
        const list = await listVirtualizorVps(panel);
        return jsonResponse({ ok: true, vps: list });
      }

      if (url.pathname === "/api/vps/refresh" && request.method === "POST") {
        const settings = await getSettings(env);
        const refreshed = await refreshConfiguredVps(settings);
        return jsonResponse(refreshed);
      }

      if (url.pathname === "/api/check-now" && request.method === "POST") {
        const result = await runMonitor(env, { force: true });
        return jsonResponse(result);
      }

      if (url.pathname === "/api/vps/start" && request.method === "POST") {
        const body = await request.json();
        const settings = await getSettings(env);
        const { panel, vps } = findConfiguredVps(settings, body.panelId, body.vpsId);
        const result = await startVirtualizorVpsWithCooldown(env, panel, vps, new Date());
        return jsonResponse({ ok: true, panelId: panel.id, vpsId: vps.id, result });
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (error) {
      return jsonResponse({ error: error.message || String(error) }, 500);
    }
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(runMonitor(env));
  }
};

async function runMonitor(env, options = {}) {
  const settings = await getSettings(env);
  const state = await getState(env);
  const now = new Date();
  const checks = [];
  let settingsChanged = false;

  for (const panel of settings.panels.filter((item) => item.enabled)) {
    for (const vps of panel.vps.filter((item) => item.enabled)) {
      const key = `${panel.id}:${vps.id}`;
      const previous = state.vps[key] || {};
      const result = {
        panelId: panel.id,
        panelName: panel.name,
        vpsId: vps.id,
        name: vps.name,
        cron: vps.cron,
        checkedAt: now.toISOString(),
        online: false,
        failureCount: previous.failureCount || 0,
        started: false,
        startSuppressed: false,
        lastStartAttemptAt: previous.lastStartAttemptAt || null,
        startCooldownUntil: previous.startCooldownUntil || null,
        error: null
      };

      try {
        if (!options.force && !isCronDue(vps.cron, now)) {
          continue;
        }

        const info = await getVirtualizorVpsInfo(panel, vps.id);
        result.online = isVpsOnline(info);
        result.status = readVpsStatus(info);
        result.ip = readVpsIp(info);
        result.hostname = readVpsHostname(info) || vps.hostname;

        if (result.online) {
          result.failureCount = 0;
          result.lastStartAttemptAt = null;
          result.startCooldownUntil = null;
          if (previous.lastStartAttemptAt || previous.startCooldownUntil) {
            await clearStartLock(env, key);
          }
        } else {
          result.failureCount += 1;
          const threshold = Number(vps.failureThreshold || settings.defaultFailureThreshold || 2);
          if (vps.autoStart && result.failureCount >= threshold) {
            const startAttempt = await startVirtualizorVpsWithCooldown(env, panel, vps, now);
            result.startSuppressed = startAttempt.startSuppressed;
            result.lastStartAttemptAt = startAttempt.lastStartAttemptAt;
            result.startCooldownUntil = startAttempt.startCooldownUntil;
            if (startAttempt.started) {
              result.startResult = startAttempt.startResult;
              result.started = true;
              result.failureCount = 0;
            }
          }
        }
      } catch (error) {
        if (error.startAttempt) {
          result.lastStartAttemptAt = error.startAttempt.lastStartAttemptAt;
          result.startCooldownUntil = error.startAttempt.startCooldownUntil;
        }
        result.error = error.message || String(error);
        result.failureCount += 1;
      }

      state.vps[key] = result;
      appendMonitorEvent(state, result);
      syncConfiguredVpsStatus(vps, result);
      settingsChanged = true;
      checks.push(result);
    }
  }

  state.lastRunAt = now.toISOString();
  await putJson(env, STATE_KEY, state);
  if (settingsChanged) {
    await putJson(env, CONFIG_KEY, settings);
  }

  return { ok: true, checked: checks.length, checks };
}

async function refreshConfiguredVps(settings) {
  const panels = [];

  for (const panel of settings.panels) {
    const remoteVps = await listVirtualizorVps(panel);
    const existing = new Map(panel.vps.map((item) => [String(item.id), item]));
    const merged = remoteVps.map((item) => ({
      id: String(item.id),
      name: existing.get(String(item.id))?.name || item.name || item.hostname || `VPS ${item.id}`,
      hostname: item.hostname || "",
      ip: item.ip || "",
      status: item.status,
      enabled: existing.get(String(item.id))?.enabled ?? true,
      autoStart: existing.get(String(item.id))?.autoStart ?? true,
      cron: existing.get(String(item.id))?.cron || DEFAULT_VPS_CRON,
      failureThreshold: existing.get(String(item.id))?.failureThreshold || settings.defaultFailureThreshold || 2,
      startCooldownMinutes: existing.get(String(item.id))?.startCooldownMinutes || DEFAULT_START_COOLDOWN_MINUTES,
      lastCheckedAt: existing.get(String(item.id))?.lastCheckedAt || null
    }));

    panels.push({ ...panel, vps: merged });
  }

  return { ...settings, panels };
}

async function listVirtualizorVps(panel) {
  const data = await virtualizorRequest(panel, { act: "listvs" });
  const entries = extractVirtualizorVpsEntries(data);

  return entries.map(([, item]) => ({
    id: String(item.vpsid),
    name: item.vps_name || item.hostname || `VPS ${item.vpsid}`,
    hostname: item.hostname || "",
    ip: readIpFromVirtualizorList(item),
    status: Number(item.status)
  }));
}

function extractVirtualizorVpsEntries(data) {
  const entries = [];
  const seen = new Set();

  if (data?.vs && typeof data.vs === "object") {
    for (const [key, value] of Object.entries(data.vs)) {
      if (isVirtualizorVpsObject(value, key)) {
        const item = { ...value, vpsid: value.vpsid || key };
        entries.push([String(item.vpsid), item]);
      }
    }
  }

  function visit(value, key = "") {
    if (!value || typeof value !== "object") return;
    if (seen.has(value)) return;
    seen.add(value);

    if (isVirtualizorVpsObject(value, key)) {
      const item = { ...value, vpsid: value.vpsid || key };
      entries.push([String(item.vpsid), item]);
      return;
    }

    for (const [childKey, childValue] of Object.entries(value)) {
      if (!childValue || typeof childValue !== "object") continue;
      visit(childValue, childKey);
    }
  }

  visit(data);

  const deduped = new Map();
  for (const [key, item] of entries) {
    if (!deduped.has(key)) {
      deduped.set(key, item);
    }
  }

  return [...deduped.entries()];
}

function isVirtualizorVpsObject(value, key = "") {
  if (!value || typeof value !== "object") return false;

  const hasVpsId = Boolean(value.vpsid) || /^\d+$/.test(key);
  const hasVpsShape = Boolean(
    value.hostname ||
    value.vps_name ||
    value.uuid ||
    value.virt ||
    value.os_name ||
    value.ips
  );

  return hasVpsId && hasVpsShape;
}

async function getVirtualizorVpsInfo(panel, vpsId) {
  return virtualizorRequest(panel, { act: "vpsmanage", svs: vpsId });
}

async function startVirtualizorVps(panel, vpsId) {
  return virtualizorRequest(panel, { act: "start", svs: vpsId, do: "1" });
}

async function startVirtualizorVpsWithCooldown(env, panel, vps, now) {
  const key = `${panel.id}:${vps.id}`;
  const lockKey = `${START_LOCK_KEY_PREFIX}${key}`;
  const cooldownMinutes = positiveInteger(vps.startCooldownMinutes, DEFAULT_START_COOLDOWN_MINUTES);
  const activeLock = await getStartLock(env, lockKey, now);

  if (activeLock) {
    return {
      started: false,
      startSuppressed: true,
      lastStartAttemptAt: activeLock.lastStartAttemptAt,
      startCooldownUntil: activeLock.startCooldownUntil
    };
  }

  const lastStartAttemptAt = now.toISOString();
  const startCooldownUntil = new Date(now.getTime() + cooldownMinutes * 60 * 1000).toISOString();
  const startAttempt = { lastStartAttemptAt, startCooldownUntil };

  await putStartLock(env, lockKey, startAttempt, cooldownMinutes);

  try {
    const startResult = await startVirtualizorVps(panel, vps.id);
    return {
      started: true,
      startSuppressed: false,
      lastStartAttemptAt,
      startCooldownUntil,
      startResult
    };
  } catch (error) {
    error.startAttempt = startAttempt;
    throw error;
  }
}

async function getStartLock(env, lockKey, now) {
  ensureKv(env);
  const lock = await env.CONFIG.get(lockKey, "json");
  if (!lock?.startCooldownUntil) return null;

  const cooldownUntil = new Date(lock.startCooldownUntil);
  if (!Number.isFinite(cooldownUntil.getTime()) || cooldownUntil <= now) {
    return null;
  }

  return {
    lastStartAttemptAt: lock.lastStartAttemptAt || null,
    startCooldownUntil: lock.startCooldownUntil
  };
}

async function putStartLock(env, lockKey, value, cooldownMinutes) {
  ensureKv(env);
  await env.CONFIG.put(lockKey, JSON.stringify(value), {
    expirationTtl: Math.max(60, cooldownMinutes * 60)
  });
}

async function clearStartLock(env, key) {
  ensureKv(env);
  await env.CONFIG.delete(`${START_LOCK_KEY_PREFIX}${key}`);
}

async function virtualizorRequest(panel, params) {
  const baseUrl = normalizeBaseUrl(panel.baseUrl);
  const url = new URL("/index.php", baseUrl);

  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  url.searchParams.set("api", "json");
  url.searchParams.set("apikey", panel.apiKey);
  url.searchParams.set("apipass", panel.apiPass);

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" }
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Virtualizor ${response.status}: ${text.slice(0, 200)}`);
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (error) {
    throw new Error(`Virtualizor returned non-JSON response: ${text.slice(0, 200)}`);
  }

  if (data.error && typeof data.error === "string") {
    throw new Error(data.error);
  }

  return data;
}

async function getSettings(env) {
  const settings = await getJson(env, CONFIG_KEY, DEFAULT_SETTINGS);
  return normalizeSettings(settings);
}

async function getState(env) {
  const state = await getJson(env, STATE_KEY, { lastRunAt: null, vps: {}, events: [], importantEvents: [] });
  return {
    lastRunAt: state.lastRunAt || null,
    vps: state.vps || {},
    events: Array.isArray(state.events) ? state.events : [],
    importantEvents: Array.isArray(state.importantEvents) ? state.importantEvents : []
  };
}

async function clearLogs(env, scope) {
  const state = await getState(env);
  if (scope === "recent" || scope === "all") {
    state.events = [];
  }
  if (scope === "important" || scope === "all") {
    state.importantEvents = [];
  }
  if (!["recent", "important", "all"].includes(scope)) {
    throw new Error("Invalid log clear scope");
  }
  await putJson(env, STATE_KEY, state);
  return state;
}

async function getJson(env, key, fallback) {
  ensureKv(env);
  const value = await env.CONFIG.get(key, "json");
  return value || structuredClone(fallback);
}

async function putJson(env, key, value) {
  ensureKv(env);
  await env.CONFIG.put(key, JSON.stringify(value, null, 2));
}

function normalizeSettings(input) {
  return {
    checkIntervalNote: DEFAULT_SETTINGS.checkIntervalNote,
    defaultFailureThreshold: positiveInteger(input.defaultFailureThreshold, 2),
    panels: Array.isArray(input.panels) ? input.panels.map(normalizePanel) : []
  };
}

function normalizePanel(input) {
  const id = String(input.id || crypto.randomUUID());
  return {
    id,
    name: String(input.name || "Virtualizor Panel").trim(),
    baseUrl: normalizeBaseUrl(input.baseUrl || ""),
    apiKey: String(input.apiKey || "").trim(),
    apiPass: String(input.apiPass || "").trim(),
    enabled: Boolean(input.enabled ?? true),
    vps: Array.isArray(input.vps) ? input.vps.map(normalizeVps) : []
  };
}

function normalizeVps(input) {
  return {
    id: String(input.id || "").trim(),
    name: String(input.name || input.hostname || input.id || "VPS").trim(),
    hostname: String(input.hostname || "").trim(),
    ip: String(input.ip || "").trim(),
    status: input.status === undefined ? null : Number(input.status),
    enabled: Boolean(input.enabled ?? true),
    autoStart: Boolean(input.autoStart ?? true),
    cron: String(input.cron || DEFAULT_VPS_CRON).trim(),
    failureThreshold: positiveInteger(input.failureThreshold, 2),
    startCooldownMinutes: positiveInteger(input.startCooldownMinutes, DEFAULT_START_COOLDOWN_MINUTES),
    lastCheckedAt: input.lastCheckedAt || null,
    lastOnline: input.lastOnline === undefined ? null : Boolean(input.lastOnline),
    failureCount: Number(input.failureCount || 0),
    lastStartAttemptAt: input.lastStartAttemptAt || null,
    startCooldownUntil: input.startCooldownUntil || null
  };
}

function normalizeBaseUrl(value) {
  const trimmed = String(value || "").trim().replace(/\/+$/, "");
  if (!trimmed) return "";
  const url = new URL(trimmed);
  return `${url.protocol}//${url.host}`;
}

function positiveInteger(value, fallback) {
  const number = Number(value);
  return Number.isInteger(number) && number > 0 ? number : fallback;
}

function findConfiguredVps(settings, panelId, vpsId) {
  const panel = settings.panels.find((item) => item.id === panelId);
  if (!panel) throw new Error("Panel not found");

  const vps = panel.vps.find((item) => item.id === String(vpsId));
  if (!vps) throw new Error("VPS not found");

  return { panel, vps };
}

function syncConfiguredVpsStatus(vps, result) {
  if (!result.error) {
    vps.status = Number.isFinite(result.status) ? result.status : null;
    vps.ip = result.ip || vps.ip || "";
    vps.hostname = result.hostname || vps.hostname || "";
    vps.lastOnline = result.online;
  }

  vps.lastCheckedAt = result.checkedAt;
  vps.failureCount = result.failureCount || 0;
  vps.lastStartAttemptAt = result.lastStartAttemptAt || null;
  vps.startCooldownUntil = result.startCooldownUntil || null;
}

function appendMonitorEvent(state, result) {
  const type = result.started ? "start" : result.startSuppressed ? "start-suppressed" : result.error ? "error" : result.online ? "online" : "offline";
  const level = result.error ? "error" : result.started || result.startSuppressed || !result.online ? "warn" : "info";
  const important = result.started || Boolean(result.error);
  const event = {
    id: crypto.randomUUID(),
    type,
    level,
    important,
    at: result.checkedAt,
    panelId: result.panelId,
    panelName: result.panelName,
    vpsId: result.vpsId,
    name: result.name,
    online: result.online,
    status: Number.isFinite(result.status) ? result.status : null,
    failureCount: result.failureCount || 0,
    started: Boolean(result.started),
    startSuppressed: Boolean(result.startSuppressed),
    lastStartAttemptAt: result.lastStartAttemptAt || null,
    startCooldownUntil: result.startCooldownUntil || null,
    error: result.error || null,
    startResult: result.startResult || null
  };

  state.events = [event, ...(Array.isArray(state.events) ? state.events : [])].slice(0, MAX_EVENT_LOGS);
  if (important) {
    state.importantEvents = [event, ...(Array.isArray(state.importantEvents) ? state.importantEvents : [])];
  }
}

function isCronDue(expression, date) {
  const parts = String(expression || DEFAULT_VPS_CRON).trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: ${expression}`);
  }

  const values = [
    date.getUTCMinutes(),
    date.getUTCHours(),
    date.getUTCDate(),
    date.getUTCMonth() + 1,
    date.getUTCDay()
  ];
  const ranges = [
    [0, 59],
    [0, 23],
    [1, 31],
    [1, 12],
    [0, 7]
  ];

  return parts.every((part, index) => matchCronField(part, values[index], ranges[index][0], ranges[index][1], index === 4));
}

function matchCronField(field, value, min, max, isDayOfWeek = false) {
  return field.split(",").some((segment) => matchCronSegment(segment.trim(), value, min, max, isDayOfWeek));
}

function matchCronSegment(segment, value, min, max, isDayOfWeek) {
  if (!segment) return false;

  const [base, stepText] = segment.split("/");
  const step = stepText === undefined ? 1 : Number(stepText);
  if (!Number.isInteger(step) || step < 1) return false;

  let start = min;
  let end = max;

  if (base !== "*") {
    if (base.includes("-")) {
      const [rangeStart, rangeEnd] = base.split("-").map(Number);
      start = normalizeCronValue(rangeStart, isDayOfWeek);
      end = normalizeCronValue(rangeEnd, isDayOfWeek);
    } else {
      start = normalizeCronValue(Number(base), isDayOfWeek);
      end = start;
    }
  }

  const normalizedValue = normalizeCronValue(value, isDayOfWeek);
  if (!Number.isInteger(start) || !Number.isInteger(end)) return false;
  if (start < min || end > max || start > end) return false;
  if (normalizedValue < start || normalizedValue > end) return false;

  return (normalizedValue - start) % step === 0;
}

function normalizeCronValue(value, isDayOfWeek) {
  if (isDayOfWeek && value === 7) return 0;
  return value;
}

function isVpsOnline(info) {
  return readVpsStatus(info) === 1;
}

function readVpsStatus(info) {
  const status = info?.info?.status ?? info?.status ?? info?.info?.vps?.status;
  return Number(status);
}

function readVpsIp(info) {
  const ip = info?.info?.ip;
  if (Array.isArray(ip)) return ip[0] || "";
  return "";
}

function readVpsHostname(info) {
  return info?.info?.hostname || info?.info?.vps?.hostname || "";
}

function readIpFromVirtualizorList(item) {
  if (!item.ips || typeof item.ips !== "object") return "";
  return Object.values(item.ips)[0] || "";
}

function ensureKv(env) {
  if (!env.CONFIG) {
    throw new Error("Missing KV binding: CONFIG");
  }
}

function requireAdmin(request, env) {
  if (!env.ADMIN_TOKEN) {
    return jsonResponse({ error: "Missing secret: ADMIN_TOKEN" }, 500);
  }

  const authorization = request.headers.get("Authorization") || "";
  const bearer = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
  const token = request.headers.get("X-Admin-Token") || bearer;

  if (token !== env.ADMIN_TOKEN) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  return null;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

function htmlResponse(html) {
  return new Response(html, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

const APP_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DediRock Keep Live</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --text: #172033;
      --muted: #647089;
      --line: #dfe5ef;
      --primary: #2454d6;
      --primary-soft: #e8efff;
      --danger: #c93030;
      --ok: #16803c;
      --warn: #9a6500;
    }
    * { box-sizing: border-box; }
    html {
      max-width: 100%;
      overflow-x: hidden;
    }
    body {
      margin: 0;
      min-height: 100vh;
      max-width: 100%;
      overflow-x: hidden;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, input {
      font: inherit;
    }
    button {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      min-height: 36px;
      padding: 0 12px;
      border-radius: 6px;
      cursor: pointer;
    }
    button.primary {
      border-color: var(--primary);
      background: var(--primary);
      color: #fff;
    }
    button.danger {
      border-color: #f0c7c7;
      color: var(--danger);
    }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    input {
      width: 100%;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px 10px;
      background: #fff;
      color: var(--text);
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    .shell {
      display: grid;
      grid-template-columns: 270px minmax(0, 1fr);
      min-height: 100vh;
      max-width: 100%;
      overflow-x: hidden;
    }
    aside {
      min-width: 0;
      border-right: 1px solid var(--line);
      background: #fff;
      padding: 20px;
    }
    main {
      min-width: 0;
      overflow-x: hidden;
      padding: 24px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 22px;
      letter-spacing: 0;
    }
    h2 {
      margin: 0;
      font-size: 16px;
      letter-spacing: 0;
    }
    .muted {
      color: var(--muted);
    }
    .stack {
      display: grid;
      gap: 16px;
      min-width: 0;
    }
    .row {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    .between {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-width: 0;
      flex-wrap: wrap;
    }
    .card {
      min-width: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      min-width: 0;
    }
    .panel-list {
      display: grid;
      gap: 10px;
      margin-top: 16px;
    }
    .panel-tab {
      width: 100%;
      text-align: left;
      height: auto;
      padding: 10px;
      border-radius: 6px;
      background: #fff;
    }
    .panel-tab.active {
      border-color: var(--primary);
      background: var(--primary-soft);
    }
    .table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
      vertical-align: middle;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      background: #fafbfe;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #eef2f7;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .badge.ok {
      color: var(--ok);
      background: #e8f6ee;
    }
    .badge.down {
      color: var(--danger);
      background: #fdecec;
    }
    .badge.warn {
      color: var(--warn);
      background: #fff4dd;
    }
    .switch {
      width: auto;
    }
    .notice {
      min-height: 20px;
      color: var(--muted);
    }
    .notice.error {
      color: var(--danger);
    }
    .empty {
      padding: 28px;
      text-align: center;
      color: var(--muted);
      border: 1px dashed var(--line);
      border-radius: 8px;
      background: #fff;
    }
    .status-summary {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
      min-height: 64px;
      padding: 14px 16px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
    }
    .status-summary > div {
      min-width: 0;
    }
    .status-summary strong {
      font-size: 20px;
      overflow-wrap: anywhere;
    }
    .status-icon {
      position: relative;
      flex: 0 0 auto;
      width: 24px;
      height: 24px;
      border-radius: 999px;
      background: var(--muted);
    }
    .status-icon::after {
      content: "";
      position: absolute;
      left: 8px;
      top: 5px;
      width: 6px;
      height: 11px;
      border: solid #fff;
      border-width: 0 2px 2px 0;
      transform: rotate(45deg);
    }
    .status-summary.ok .status-icon {
      background: var(--ok);
    }
    .status-summary.warn .status-icon {
      background: var(--warn);
    }
    .status-summary.warn .status-icon::after {
      left: 11px;
      top: 5px;
      width: 2px;
      height: 10px;
      border: 0;
      border-radius: 2px;
      background: #fff;
      transform: none;
      box-shadow: 0 13px 0 #fff;
    }
    .status-summary.down .status-icon {
      background: var(--danger);
    }
    .status-summary.down .status-icon::after {
      left: 7px;
      top: 11px;
      width: 10px;
      height: 2px;
      border: 0;
      border-radius: 2px;
      background: #fff;
      transform: none;
    }
    .status-groups {
      display: grid;
      gap: 14px;
      min-width: 0;
    }
    .status-group-title {
      margin: 8px 0;
      font-size: 18px;
      font-weight: 750;
    }
    .status-list {
      display: grid;
      gap: 8px;
      min-width: 0;
    }
    .status-service {
      display: grid;
      grid-template-columns: minmax(0, 320px) minmax(0, 1fr);
      gap: 16px;
      align-items: center;
      min-width: 0;
      margin: 0 -10px;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      border-radius: 8px;
      transition: background .15s ease;
    }
    .status-service:hover {
      background: #e8f7ef;
    }
    .status-service:last-child {
      border-bottom: 0;
    }
    .status-service-name {
      margin-bottom: 6px;
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .status-service > div {
      min-width: 0;
    }
    .status-service-badges {
      gap: 6px;
    }
    .status-bars {
      display: grid;
      grid-template-columns: repeat(48, minmax(2px, 1fr));
      gap: 3px;
      align-items: end;
      width: 100%;
      min-width: 0;
    }
    .status-segment {
      position: relative;
      height: 18px;
      border-radius: 999px;
      background: #d7dee9;
      cursor: default;
    }
    .status-segment.ok {
      background: #55d884;
    }
    .status-segment.down {
      background: #e35b5b;
    }
    .status-segment.warn {
      background: #e7a93b;
    }
    .status-segment:hover {
      z-index: 11;
    }
    .status-tooltip {
      position: absolute;
      left: 50%;
      bottom: calc(100% + 12px);
      z-index: 10;
      min-width: 168px;
      max-width: min(220px, calc(100vw - 32px));
      padding: 10px 12px;
      border-radius: 8px;
      background: #172033;
      color: #f8fafc;
      text-align: center;
      overflow-wrap: anywhere;
      box-shadow: 0 12px 28px rgb(23 32 51 / 24%);
      opacity: 0;
      pointer-events: none;
      transform: translate(-50%, 4px);
      transition: opacity .12s ease, transform .12s ease;
    }
    .status-tooltip::after {
      content: "";
      position: absolute;
      left: 50%;
      bottom: -6px;
      width: 12px;
      height: 12px;
      background: #172033;
      transform: translateX(-50%) rotate(45deg);
    }
    .status-tooltip strong {
      display: block;
      margin-bottom: 6px;
      color: #55d884;
      font-size: 15px;
    }
    .status-tooltip.down strong {
      color: #ff7878;
    }
    .status-tooltip.warn strong {
      color: #ffd166;
    }
    .status-tooltip.unknown strong {
      color: #cbd5e1;
    }
    .status-tooltip span {
      display: block;
      color: #f8fafc;
    }
    .status-segment:hover .status-tooltip {
      opacity: 1;
      transform: translate(-50%, 0);
    }
    .status-history-label {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
    }
    .status-history-label span {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .status-footer {
      color: var(--muted);
      text-align: center;
      font-size: 12px;
    }
    @media (max-width: 860px) {
      .shell {
        grid-template-columns: 1fr;
      }
      aside {
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      main {
        padding: 16px;
      }
      .grid {
        grid-template-columns: 1fr;
      }
      .table {
        display: block;
        overflow-x: auto;
      }
      .status-service {
        grid-template-columns: 1fr;
      }
      .status-summary {
        align-items: flex-start;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="stack">
        <div>
          <h1>DediRock Keep Live</h1>
          <div class="muted">Virtualizor VPS monitor</div>
        </div>
        <label>
          管理 Token
          <input id="adminToken" type="password" autocomplete="current-password" placeholder="ADMIN_TOKEN">
        </label>
        <div class="row">
          <button class="primary" id="loadBtn">连接</button>
          <button id="saveTokenBtn">记住本次会话</button>
        </div>
        <div class="notice" id="sideNotice"></div>
        <div class="panel-list" id="panelList"></div>
      </div>
    </aside>

    <main class="stack">
      <section class="card stack">
        <div class="between">
          <div>
            <h2>全局操作</h2>
            <div class="muted">Cron 会按 wrangler.toml 里的频率执行。</div>
          </div>
          <div class="row">
            <button id="addPanelBtn">新增面板</button>
            <button id="refreshBtn">拉取 VPS</button>
            <button id="checkBtn">立即检查</button>
            <button class="primary" id="saveBtn">保存配置</button>
          </div>
        </div>
        <label style="max-width: 240px;">
          默认离线阈值
          <input id="defaultFailureThreshold" type="number" min="1" step="1">
        </label>
        <div class="notice" id="mainNotice"></div>
      </section>

      <section class="card stack">
        <div class="between">
          <h2>状态总览</h2>
          <span class="muted">仅管理页可见</span>
        </div>
        <div id="statusOverview"></div>
      </section>

      <section class="card stack" id="panelEditor"></section>
      <section class="card stack">
        <div class="between">
          <h2>最近状态</h2>
          <span class="muted" id="lastRunAt">未检查</span>
        </div>
        <div id="stateTable"></div>
      </section>
      <section class="card stack">
        <div class="between">
          <h2>事件日志</h2>
          <span class="muted">最多保留最近 100 条</span>
        </div>
        <div class="row">
          <select id="eventLogType">
            <option value="recent">最近日志</option>
            <option value="important">重要日志</option>
          </select>
          <select id="eventLevelFilter">
            <option value="all">全部等级</option>
            <option value="info">Info</option>
            <option value="warn">Warn</option>
            <option value="error">Error</option>
          </select>
          <select id="eventVpsFilter">
            <option value="all">全部 VPS</option>
          </select>
          <input id="eventSearch" style="max-width: 260px;" placeholder="搜索 VPS、名称、错误">
          <button id="clearRecentLogsBtn">清除最近</button>
          <button id="clearImportantLogsBtn" class="danger">清除重要</button>
        </div>
        <div id="eventTable"></div>
      </section>
    </main>
  </div>

  <script>
    const tokenInput = document.querySelector("#adminToken");
    const sideNotice = document.querySelector("#sideNotice");
    const mainNotice = document.querySelector("#mainNotice");
    const panelList = document.querySelector("#panelList");
    const panelEditor = document.querySelector("#panelEditor");
    const statusOverview = document.querySelector("#statusOverview");
    const stateTable = document.querySelector("#stateTable");
    const eventTable = document.querySelector("#eventTable");
    const eventLogType = document.querySelector("#eventLogType");
    const eventLevelFilter = document.querySelector("#eventLevelFilter");
    const eventVpsFilter = document.querySelector("#eventVpsFilter");
    const eventSearch = document.querySelector("#eventSearch");
    const lastRunAt = document.querySelector("#lastRunAt");
    const defaultFailureThreshold = document.querySelector("#defaultFailureThreshold");

    let settings = { defaultFailureThreshold: 2, panels: [] };
    let state = { lastRunAt: null, vps: {} };
    let selectedPanelId = null;

    tokenInput.value = sessionStorage.getItem("adminToken") || "";

    document.querySelector("#saveTokenBtn").addEventListener("click", () => {
      sessionStorage.setItem("adminToken", tokenInput.value.trim());
      setNotice(sideNotice, "已保存到本次浏览器会话。");
    });
    document.querySelector("#loadBtn").addEventListener("click", loadAll);
    document.querySelector("#saveBtn").addEventListener("click", saveSettings);
    document.querySelector("#addPanelBtn").addEventListener("click", addPanel);
    document.querySelector("#refreshBtn").addEventListener("click", refreshVps);
    document.querySelector("#checkBtn").addEventListener("click", checkNow);
    eventLogType.addEventListener("change", renderEvents);
    eventLevelFilter.addEventListener("change", renderEvents);
    eventVpsFilter.addEventListener("change", renderEvents);
    eventSearch.addEventListener("input", renderEvents);
    document.querySelector("#clearRecentLogsBtn").addEventListener("click", () => clearLogs("recent"));
    document.querySelector("#clearImportantLogsBtn").addEventListener("click", () => clearLogs("important"));

    function authHeaders() {
      return {
        "Content-Type": "application/json",
        "X-Admin-Token": tokenInput.value.trim()
      };
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        ...options,
        headers: { ...authHeaders(), ...(options.headers || {}) }
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || "请求失败");
      return data;
    }

    async function loadAll() {
      try {
        setNotice(sideNotice, "读取中...");
        settings = await api("/api/settings");
        state = await api("/api/state");
        selectedPanelId = settings.panels[0]?.id || null;
        render();
        setNotice(sideNotice, "已连接。");
      } catch (error) {
        setNotice(sideNotice, error.message, true);
      }
    }

    async function saveSettings() {
      try {
        settings.defaultFailureThreshold = Number(defaultFailureThreshold.value || 2);
        collectPanelForm();
        settings = await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
        render();
        setNotice(mainNotice, "配置已保存。");
      } catch (error) {
        setNotice(mainNotice, error.message, true);
      }
    }

    async function refreshVps() {
      try {
        collectPanelForm();
        await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
        settings = await api("/api/vps/refresh", { method: "POST", body: "{}" });
        await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
        render();
        const total = settings.panels.reduce((sum, panel) => sum + panel.vps.length, 0);
        setNotice(mainNotice, "已从 Virtualizor 拉取 VPS 列表，数量：" + total);
      } catch (error) {
        setNotice(mainNotice, error.message, true);
      }
    }

    async function checkNow() {
      try {
        collectPanelForm();
        await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
        const result = await api("/api/check-now", { method: "POST", body: "{}" });
        settings = await api("/api/settings");
        state = await api("/api/state");
        render();
        setNotice(mainNotice, "检查完成，数量：" + result.checked);
      } catch (error) {
        setNotice(mainNotice, error.message, true);
      }
    }

    async function startVps(panelId, vpsId) {
      try {
        const response = await api("/api/vps/start", { method: "POST", body: JSON.stringify({ panelId, vpsId }) });
        if (response.result?.startSuppressed) {
          setNotice(mainNotice, "启动防重中，冷却至：" + response.result.startCooldownUntil);
        } else {
          setNotice(mainNotice, "已发送启动命令。");
        }
      } catch (error) {
        setNotice(mainNotice, error.message, true);
      }
    }

    async function clearLogs(scope) {
      const text = scope === "important" ? "重要日志" : "最近日志";
      if (!confirm("确认清除" + text + "？")) return;

      try {
        state = await api("/api/logs/clear", { method: "POST", body: JSON.stringify({ scope }) });
        renderEvents();
        setNotice(mainNotice, "已清除" + text + "。");
      } catch (error) {
        setNotice(mainNotice, error.message, true);
      }
    }

    function addPanel() {
      const panel = {
        id: crypto.randomUUID(),
        name: "DediRock",
        baseUrl: "https://vpanel.dedirock.com:4083",
        apiKey: "",
        apiPass: "",
        enabled: true,
        vps: []
      };
      settings.panels.push(panel);
      selectedPanelId = panel.id;
      render();
    }

    function removePanel(id) {
      settings.panels = settings.panels.filter((panel) => panel.id !== id);
      selectedPanelId = settings.panels[0]?.id || null;
      render();
    }

    function render() {
      defaultFailureThreshold.value = settings.defaultFailureThreshold || 2;
      renderPanelList();
      renderPanelEditor();
      renderStatusOverview();
      renderState();
      renderEventFilters();
      renderEvents();
    }

    function renderPanelList() {
      panelList.innerHTML = "";
      if (!settings.panels.length) {
        panelList.innerHTML = '<div class="empty">暂无面板</div>';
        return;
      }

      for (const panel of settings.panels) {
        const btn = document.createElement("button");
        btn.className = "panel-tab" + (panel.id === selectedPanelId ? " active" : "");
        btn.innerHTML = '<strong>' + escapeHtml(panel.name) + '</strong><br><span class="muted">' + escapeHtml(panel.baseUrl) + '</span>';
        btn.addEventListener("click", () => {
          collectPanelForm();
          selectedPanelId = panel.id;
          render();
        });
        panelList.appendChild(btn);
      }
    }

    function renderPanelEditor() {
      const panel = settings.panels.find((item) => item.id === selectedPanelId);
      if (!panel) {
        panelEditor.innerHTML = '<div class="empty">新增一个 Virtualizor 面板后开始配置。</div>';
        return;
      }

      panelEditor.innerHTML = \`
        <div class="between">
          <h2>面板配置</h2>
          <div class="row">
            <label class="row" style="gap: 6px; color: var(--muted);">
              <input class="switch" id="panelEnabled" type="checkbox" \${panel.enabled ? "checked" : ""}>启用
            </label>
            <button class="danger" id="removePanelBtn">删除面板</button>
          </div>
        </div>
        <div class="grid">
          <label>名称<input id="panelName" value="\${escapeAttr(panel.name)}"></label>
          <label>面板地址<input id="panelBaseUrl" value="\${escapeAttr(panel.baseUrl)}" placeholder="https://vpanel.dedirock.com:4083"></label>
          <label>API Key<input id="panelApiKey" value="\${escapeAttr(panel.apiKey)}"></label>
          <label>API Password<input id="panelApiPass" type="password" value="\${escapeAttr(panel.apiPass)}"></label>
        </div>
        <div class="between">
          <h2>VPS</h2>
          <span class="muted">通过“拉取 VPS”从 Virtualizor 获取。</span>
        </div>
        <div id="vpsTable"></div>
      \`;

      document.querySelector("#removePanelBtn").addEventListener("click", () => removePanel(panel.id));
      renderVpsTable(panel);
    }

    function renderVpsTable(panel) {
      const target = document.querySelector("#vpsTable");
      if (!panel.vps.length) {
        target.innerHTML = '<div class="empty">还没有 VPS 信息。</div>';
        return;
      }

      target.innerHTML = \`
        <table class="table">
          <thead><tr><th>启用</th><th>名称</th><th>VPS ID</th><th>IP</th><th>状态</th><th>Cron</th><th>自动启动</th><th>阈值</th><th>防重分钟</th><th>失败</th><th>最后检查</th><th>操作</th></tr></thead>
          <tbody>
            \${panel.vps.map((vps, index) => \`
              <tr>
                <td><input class="switch" data-vps-field="enabled" data-vps-index="\${index}" type="checkbox" \${vps.enabled ? "checked" : ""}></td>
                <td><input data-vps-field="name" data-vps-index="\${index}" value="\${escapeAttr(vps.name)}"></td>
                <td>\${escapeHtml(vps.id)}</td>
                <td>\${escapeHtml(vps.ip || "-")}</td>
                <td>\${statusBadge(vps.status)}</td>
                <td><input data-vps-field="cron" data-vps-index="\${index}" value="\${escapeAttr(vps.cron || "*/5 * * * *")}" placeholder="*/5 * * * *"></td>
                <td><input class="switch" data-vps-field="autoStart" data-vps-index="\${index}" type="checkbox" \${vps.autoStart ? "checked" : ""}></td>
                <td><input data-vps-field="failureThreshold" data-vps-index="\${index}" type="number" min="1" value="\${vps.failureThreshold || 2}"></td>
                <td><input data-vps-field="startCooldownMinutes" data-vps-index="\${index}" type="number" min="1" value="\${vps.startCooldownMinutes || 15}"></td>
                <td>\${vps.failureCount || 0}</td>
                <td>\${escapeHtml(vps.lastCheckedAt || "-")}</td>
                <td><button data-start-vps="\${escapeAttr(vps.id)}">启动</button></td>
              </tr>
            \`).join("")}
          </tbody>
        </table>
      \`;

      target.querySelectorAll("[data-start-vps]").forEach((button) => {
        button.addEventListener("click", () => startVps(panel.id, button.dataset.startVps));
      });
    }

    function renderStatusOverview() {
      const groups = (settings.panels || [])
        .map((panel) => ({
          panel,
          vps: (panel.vps || []).filter((vps) => vps.enabled)
        }))
        .filter((group) => group.vps.length);
      const services = groups.flatMap((group) => group.vps.map((vps) => ({ panel: group.panel, vps })));

      if (!services.length) {
        statusOverview.innerHTML = '<div class="empty">暂无启用 VPS。</div>';
        return;
      }

      const summary = summarizeStatusOverview(services);
      statusOverview.innerHTML = \`
        <div class="status-summary \${summary.level}">
          <span class="status-icon" aria-hidden="true"></span>
          <div>
            <strong>\${escapeHtml(summary.title)}</strong>
            <div class="muted">\${escapeHtml(summary.detail)}</div>
          </div>
        </div>
        <div class="status-groups">
          \${groups.map((group) => \`
            <div class="status-group">
              <div class="status-group-title">\${escapeHtml(group.panel.name || "Virtualizor Panel")}</div>
              <div class="status-list">
                \${group.vps.map((vps) => statusOverviewRow(group.panel, vps)).join("")}
              </div>
            </div>
          \`).join("")}
        </div>
        <div class="status-footer">最后更新于 \${escapeHtml(state.lastRunAt || "-")}</div>
      \`;
    }

    function statusOverviewRow(panel, vps) {
      const key = panel.id + ":" + vps.id;
      const row = (state.vps || {})[key];
      const serviceStatus = getServiceStatus(row);
      const availability = calculateAvailability(key, row);
      const history = buildStatusHistory(key, row);
      const name = vps.name || vps.hostname || vps.id;

      return \`
        <div class="status-service">
          <div>
            <div class="status-service-name">\${escapeHtml(name)}</div>
            <div class="row status-service-badges">
              <span class="badge \${serviceStatus.badgeClass}">\${escapeHtml(serviceStatus.label)}</span>
              <span class="badge">\${escapeHtml(availability)}</span>
            </div>
          </div>
          <div>
            <div class="status-bars">
              \${history.segments.map(statusSegmentHtml).join("")}
            </div>
            <div class="status-history-label">
              <span>\${escapeHtml(history.startLabel)}</span>
              <span>现在</span>
            </div>
          </div>
        </div>
      \`;
    }

    function summarizeStatusOverview(services) {
      const counts = { ok: 0, warn: 0, down: 0, unknown: 0 };

      for (const { panel, vps } of services) {
        const key = panel.id + ":" + vps.id;
        const status = getServiceStatus((state.vps || {})[key]);
        counts[status.level] += 1;
      }

      const detail = services.length + " 台启用 VPS，在线 " + counts.ok + "，异常 " + counts.down + "，启动中 " + counts.warn + "，未检查 " + counts.unknown;
      if (counts.down > 0) {
        return { level: "down", title: "存在 VPS 离线或检查失败", detail };
      }
      if (counts.warn > 0) {
        return { level: "warn", title: "存在 VPS 启动中或防重中", detail };
      }
      if (counts.unknown > 0) {
        return { level: "warn", title: "部分 VPS 尚未检查", detail };
      }
      return { level: "ok", title: "所有启用 VPS 运行正常", detail };
    }

    function getServiceStatus(row) {
      if (!row?.checkedAt) return { level: "unknown", label: "Unknown", badgeClass: "" };
      if (row.error) return { level: "down", label: "Error", badgeClass: "down" };
      if (row.online) return { level: "ok", label: "Online", badgeClass: "ok" };
      if (row.started) return { level: "warn", label: "Starting", badgeClass: "warn" };
      if (row.startSuppressed) return { level: "warn", label: "Cooldown", badgeClass: "warn" };
      return { level: "down", label: "Offline", badgeClass: "down" };
    }

    function calculateAvailability(key, row) {
      const samples = getStatusSamples(key, row).filter((sample) => sample.status !== "unknown");
      if (!samples.length) return "暂无数据";

      const ok = samples.filter((sample) => sample.status === "ok").length;
      return Math.round((ok / samples.length) * 100) + "%";
    }

    function buildStatusHistory(key, row) {
      const samples = getStatusSamples(key, row).reverse();
      const padded = Array(Math.max(0, 48 - samples.length)).fill(null).map(() => unknownStatusSample()).concat(samples).slice(-48);
      const firstKnown = padded.find((sample) => sample.at);
      const startLabel = firstKnown?.at ? relativeTime(firstKnown.at) : "暂无";

      return { segments: padded, startLabel };
    }

    function getStatusSamples(key, row) {
      const samples = getStatusEvents(key).map(eventStatusSample);
      if (!samples.length && row?.checkedAt) {
        samples.push(rowStatusSample(row));
      }
      return samples;
    }

    function getStatusEvents(key) {
      return (state.events || [])
        .filter((event) => (event.panelId + ":" + event.vpsId) === key)
        .slice(0, 48);
    }

    function eventStatusSample(event) {
      if (event.error) return statusSample("down", "异常", event.at);
      if (event.online) return statusSample("ok", "正常", event.at);
      if (event.started) return statusSample("warn", "启动中", event.at);
      if (event.startSuppressed) return statusSample("warn", "防重中", event.at);
      return statusSample("down", "离线", event.at);
    }

    function rowStatusSample(row) {
      if (row.error) return statusSample("down", "异常", row.checkedAt);
      if (row.online) return statusSample("ok", "正常", row.checkedAt);
      if (row.started) return statusSample("warn", "启动中", row.checkedAt);
      if (row.startSuppressed) return statusSample("warn", "防重中", row.checkedAt);
      return statusSample("down", "离线", row.checkedAt);
    }

    function statusSample(status, label, at) {
      return { status, label, at: at || null };
    }

    function unknownStatusSample() {
      return statusSample("unknown", "暂无数据", null);
    }

    function statusSegmentHtml(sample) {
      return \`
        <span class="status-segment \${sample.status}">
          <span class="status-tooltip \${sample.status}">
            <strong>\${escapeHtml(sample.label)}</strong>
            <span>\${escapeHtml(sample.at ? formatDateTime(sample.at) : "暂无时间")}</span>
          </span>
        </span>
      \`;
    }

    function relativeTime(value) {
      const time = new Date(value).getTime();
      if (!Number.isFinite(time)) return "-";

      const diff = Math.max(0, Date.now() - time);
      const minutes = Math.floor(diff / 60000);
      if (minutes < 1) return "刚刚";
      if (minutes < 60) return minutes + "m";

      const hours = Math.floor(minutes / 60);
      if (hours < 24) return hours + "h";

      return Math.floor(hours / 24) + "d";
    }

    function formatDateTime(value) {
      const date = new Date(value);
      if (!Number.isFinite(date.getTime())) return "-";

      const pad = (number) => String(number).padStart(2, "0");
      return [
        date.getFullYear(),
        pad(date.getMonth() + 1),
        pad(date.getDate())
      ].join("-") + " " + [
        pad(date.getHours()),
        pad(date.getMinutes()),
        pad(date.getSeconds())
      ].join(":");
    }

    function renderState() {
      lastRunAt.textContent = state.lastRunAt ? "最后检查：" + state.lastRunAt : "未检查";
      const rows = Object.values(state.vps || {});
      if (!rows.length) {
        stateTable.innerHTML = '<div class="empty">暂无检查结果。</div>';
        return;
      }

      stateTable.innerHTML = \`
        <table class="table">
          <thead><tr><th>面板</th><th>VPS</th><th>状态</th><th>失败次数</th><th>启动</th><th>错误</th><th>时间</th></tr></thead>
          <tbody>
            \${rows.map((row) => \`
              <tr>
                <td>\${escapeHtml(row.panelName || row.panelId)}</td>
                <td>\${escapeHtml(row.name || row.vpsId)}</td>
                <td>\${row.online ? '<span class="badge ok">Online</span>' : '<span class="badge down">Offline</span>'}</td>
                <td>\${row.failureCount || 0}</td>
                <td>\${startStateBadge(row)}</td>
                <td>\${escapeHtml(row.error || "-")}</td>
                <td>\${escapeHtml(row.checkedAt || "-")}</td>
              </tr>
            \`).join("")}
          </tbody>
        </table>
      \`;
    }

    function renderEventFilters() {
      const selected = eventVpsFilter.value || "all";
      const options = ['<option value="all">全部 VPS</option>'];

      for (const panel of settings.panels || []) {
        for (const vps of panel.vps || []) {
          const value = panel.id + ":" + vps.id;
          const label = [vps.name || vps.hostname || vps.id, vps.id].filter(Boolean).join(" / ");
          options.push('<option value="' + escapeAttr(value) + '">' + escapeHtml(label) + '</option>');
        }
      }

      eventVpsFilter.innerHTML = options.join("");
      eventVpsFilter.value = [...eventVpsFilter.options].some((option) => option.value === selected) ? selected : "all";
    }

    function renderEvents() {
      const source = eventLogType.value === "important" ? state.importantEvents || [] : state.events || [];
      const level = eventLevelFilter.value;
      const vpsKey = eventVpsFilter.value;
      const keyword = eventSearch.value.trim().toLowerCase();
      const rows = source.filter((row) => {
        if (level !== "all" && row.level !== level) return false;
        if (vpsKey !== "all" && (row.panelId + ":" + row.vpsId) !== vpsKey) return false;
        if (!keyword) return true;

        return [
          row.panelName,
          row.panelId,
          row.vpsId,
          row.name,
          row.type,
          row.level,
          row.error,
          summarizeStartResult(row.startResult)
        ].some((value) => String(value || "").toLowerCase().includes(keyword));
      });

      if (!rows.length) {
        eventTable.innerHTML = '<div class="empty">暂无事件日志。</div>';
        return;
      }

      eventTable.innerHTML = \`
        <table class="table">
          <thead><tr><th>时间</th><th>等级</th><th>类型</th><th>面板</th><th>VPS</th><th>状态</th><th>失败次数</th><th>启动结果</th><th>错误</th></tr></thead>
          <tbody>
            \${rows.map((row) => \`
              <tr>
                <td>\${escapeHtml(row.at || "-")}</td>
                <td>\${levelBadge(row.level)}</td>
                <td>\${eventBadge(row)}</td>
                <td>\${escapeHtml(row.panelName || row.panelId)}</td>
                <td>\${escapeHtml(row.name || row.vpsId)}</td>
                <td>\${row.online ? '<span class="badge ok">Online</span>' : '<span class="badge down">Offline</span>'}</td>
                <td>\${row.failureCount || 0}</td>
                <td>\${escapeHtml(summarizeStartResult(row.startResult))}</td>
                <td>\${escapeHtml(row.error || "-")}</td>
              </tr>
            \`).join("")}
          </tbody>
        </table>
      \`;
    }

    function collectPanelForm() {
      const panel = settings.panels.find((item) => item.id === selectedPanelId);
      if (!panel || !document.querySelector("#panelName")) return;

      panel.name = document.querySelector("#panelName").value.trim();
      panel.baseUrl = document.querySelector("#panelBaseUrl").value.trim();
      panel.apiKey = document.querySelector("#panelApiKey").value.trim();
      panel.apiPass = document.querySelector("#panelApiPass").value.trim();
      panel.enabled = document.querySelector("#panelEnabled").checked;

      document.querySelectorAll("[data-vps-field]").forEach((input) => {
        const vps = panel.vps[Number(input.dataset.vpsIndex)];
        const field = input.dataset.vpsField;
        if (!vps) return;
        if (input.type === "checkbox") {
          vps[field] = input.checked;
        } else if (input.type === "number") {
          vps[field] = Number(input.value || 1);
        } else {
          vps[field] = input.value.trim();
        }
      });
    }

    function statusBadge(status) {
      if (Number(status) === 1) return '<span class="badge ok">Online</span>';
      if (Number(status) === 0) return '<span class="badge down">Offline</span>';
      return '<span class="badge">Unknown</span>';
    }

    function eventBadge(row) {
      if (row.started) return '<span class="badge warn">Start</span>';
      if (row.startSuppressed) return '<span class="badge warn">Cooldown</span>';
      if (row.error) return '<span class="badge down">Error</span>';
      if (row.online) return '<span class="badge ok">Online</span>';
      return '<span class="badge down">Offline</span>';
    }

    function startStateBadge(row) {
      if (row.started) return '<span class="badge warn">已执行</span>';
      if (row.startSuppressed) return '<span class="badge warn">防重中</span>';
      return "-";
    }

    function levelBadge(level) {
      if (level === "error") return '<span class="badge down">Error</span>';
      if (level === "warn") return '<span class="badge warn">Warn</span>';
      return '<span class="badge ok">Info</span>';
    }

    function summarizeStartResult(value) {
      if (!value) return "-";
      if (value.done?.msg) return value.done.msg;
      if (value.done_msg) return value.done_msg;
      if (value.title) return value.title;
      return JSON.stringify(value).slice(0, 120);
    }

    function setNotice(target, message, isError = false) {
      target.textContent = message;
      target.classList.toggle("error", isError);
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;"
      }[char]));
    }

    function escapeAttr(value) {
      return escapeHtml(value);
    }
  </script>
</body>
</html>`;
