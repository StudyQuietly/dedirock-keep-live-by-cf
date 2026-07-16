const CONFIG_KEY = "settings";
const STATE_KEY = "monitor-state";
const START_LOCK_KEY_PREFIX = "start-lock:";
const MONITOR_RUN_LOCK_KEY = "monitor-run-lock";
const DEFAULT_VPS_CRON = "*/5 * * * *";
const DEFAULT_START_COOLDOWN_MINUTES = 15;
const MAX_EVENT_LOGS_PER_VPS = 100;
const MONITOR_RUN_LOCK_TTL_SECONDS = 300;
const MONITOR_CONCURRENCY = 5;
const VIRTUALIZOR_REQUEST_TIMEOUT_MS = 15000;

const DEFAULT_SETTINGS = {
  checkIntervalNote: "Worker wakes up every minute; each VPS controls its own cron schedule.",
  defaultFailureThreshold: 2,
  apiTimeout: 15,
  timezone: "local",
  notifications: {
    enabled: false,
    events: {
      offline: true,
      start: true,
      recovered: true,
      error: true
    },
    offlinePolicy: "threshold",
    channels: []
  },
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
        const settings = await getSettings(env);
        const timeoutMs = (settings.apiTimeout || 15) * 1000;
        const panel = normalizePanel(await request.json());
        const list = await listVirtualizorVps(panel, timeoutMs);
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

      if (url.pathname === "/api/notifications/test" && request.method === "POST") {
        const body = await request.json().catch(() => ({}));
        const settings = await getSettings(env);
        const result = await sendTestNotification(settings, body.channelId || null);
        return jsonResponse(result);
      }

      if (url.pathname === "/api/vps/start" && request.method === "POST") {
        const body = await request.json();
        const settings = await getSettings(env);
        const timeoutMs = (settings.apiTimeout || 15) * 1000;
        const { panel, vps } = findConfiguredVps(settings, body.panelId, body.vpsId);
        const result = await startVirtualizorVpsWithCooldown(env, panel, vps, new Date(), timeoutMs);
        return jsonResponse({ ok: true, panelId: panel.id, vpsId: vps.id, result });
      }

      return jsonResponse({ error: "Not found" }, 404);
    } catch (error) {
      return jsonResponse({ error: error.message || String(error) }, error.status || 500);
    }
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(runMonitor(env));
  }
};

async function runMonitor(env, options = {}) {
  const lock = await acquireMonitorRunLock(env);
  if (!lock.acquired) {
    return {
      ok: true,
      skipped: true,
      reason: "Monitor is already running",
      checked: 0,
      checks: [],
      activeRun: lock.activeRun
    };
  }

  try {
    return await runMonitorChecks(env, options);
  } finally {
    await releaseMonitorRunLock(env, lock.owner);
  }
}

async function runMonitorChecks(env, options = {}) {
  const settings = await getSettings(env);
  const state = await getState(env);
  const now = new Date();
  const jobs = [];

  for (const panel of settings.panels.filter((item) => item.enabled)) {
    for (const vps of panel.vps.filter((item) => item.enabled)) {
      jobs.push(() => checkConfiguredVps(env, settings, state, panel, vps, now, options));
    }
  }

  const checks = (await runWithConcurrency(jobs, MONITOR_CONCURRENCY)).filter(Boolean);
  for (const { key, result } of checks) {
    const previous = state.vps[key] || {};
    state.vps[key] = result;
    const event = appendMonitorEvent(state, result);
    if (event) {
      const notificationResults = await sendNotificationsForEvent(settings, state, event, previous);
      if (notificationResults.length) {
        event.notificationResults = notificationResults;
        if (notificationResults.some((item) => !item.ok)) {
          event.important = true;
          ensureImportantEvent(state, event);
        }
      }
    }
  }

  state.lastRunAt = now.toISOString();
  await putJson(env, STATE_KEY, state);

  return { ok: true, checked: checks.length, checks: checks.map((item) => item.result) };
}

async function checkConfiguredVps(env, settings, state, panel, vps, now, options) {
  const key = `${panel.id}:${vps.id}`;
  const previous = state.vps[key] || {};
  const failureThreshold = Number(vps.failureThreshold || settings.defaultFailureThreshold || 2);
  const result = {
    panelId: panel.id,
    panelName: panel.name,
    vpsId: vps.id,
    name: vps.name,
    cron: vps.cron,
    checkedAt: now.toISOString(),
    online: false,
    failureCount: previous.failureCount || 0,
    failureThreshold,
    started: false,
    startSuppressed: false,
    lastStartAttemptAt: previous.lastStartAttemptAt || null,
    startCooldownUntil: previous.startCooldownUntil || null,
    error: null
  };

  try {
    if (!options.force && !isCronDue(vps.cron, now)) {
      return null;
    }

    const timeoutMs = (settings.apiTimeout || 15) * 1000;
    const info = await getVirtualizorVpsInfo(panel, vps.id, timeoutMs);
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
      if (vps.autoStart && result.failureCount >= failureThreshold) {
        const startAttempt = await startVirtualizorVpsWithCooldown(env, panel, vps, now, timeoutMs);
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
    if (!error.isTimeout) {
      result.failureCount += 1;
    }
  }

  return { key, result };
}

async function runWithConcurrency(jobs, concurrency) {
  const results = new Array(jobs.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < jobs.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await jobs[index]();
    }
  }

  const workerCount = Math.min(Math.max(1, concurrency), jobs.length);
  await Promise.all(Array.from({ length: workerCount }, worker));
  return results;
}

async function acquireMonitorRunLock(env) {
  ensureKv(env);
  const now = new Date();
  const activeRun = await env.CONFIG.get(MONITOR_RUN_LOCK_KEY, "json");

  if (activeRun?.expiresAt && new Date(activeRun.expiresAt) > now) {
    return { acquired: false, activeRun };
  }

  const owner = crypto.randomUUID();
  const lock = {
    owner,
    startedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + MONITOR_RUN_LOCK_TTL_SECONDS * 1000).toISOString()
  };

  await env.CONFIG.put(MONITOR_RUN_LOCK_KEY, JSON.stringify(lock), {
    expirationTtl: MONITOR_RUN_LOCK_TTL_SECONDS
  });

  return { acquired: true, owner };
}

async function releaseMonitorRunLock(env, owner) {
  ensureKv(env);
  const activeRun = await env.CONFIG.get(MONITOR_RUN_LOCK_KEY, "json");
  if (activeRun?.owner === owner) {
    await env.CONFIG.delete(MONITOR_RUN_LOCK_KEY);
  }
}

async function refreshConfiguredVps(settings) {
  const panels = [];
  const timeoutMs = (settings.apiTimeout || 15) * 1000;

  for (const panel of settings.panels) {
    const remoteVps = await listVirtualizorVps(panel, timeoutMs);
    const existing = new Map(panel.vps.map((item) => [String(item.id), item]));
    const merged = remoteVps.map((item) => ({
      id: String(item.id),
      name: existing.get(String(item.id))?.name || item.name || item.hostname || `VPS ${item.id}`,
      hostname: item.hostname || "",
      ip: item.ip || "",
      enabled: existing.get(String(item.id))?.enabled ?? true,
      autoStart: existing.get(String(item.id))?.autoStart ?? true,
      cron: existing.get(String(item.id))?.cron || DEFAULT_VPS_CRON,
      failureThreshold: existing.get(String(item.id))?.failureThreshold || settings.defaultFailureThreshold || 2,
      startCooldownMinutes: existing.get(String(item.id))?.startCooldownMinutes || DEFAULT_START_COOLDOWN_MINUTES
    }));

    panels.push({ ...panel, vps: merged });
  }

  return { ...settings, panels };
}

async function listVirtualizorVps(panel, timeoutMs) {
  const data = await virtualizorRequest(panel, { act: "listvs" }, timeoutMs);
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

async function getVirtualizorVpsInfo(panel, vpsId, timeoutMs) {
  return virtualizorRequest(panel, { act: "vpsmanage", svs: vpsId }, timeoutMs);
}

async function startVirtualizorVps(panel, vpsId, timeoutMs) {
  return virtualizorRequest(panel, { act: "start", svs: vpsId, do: "1" }, timeoutMs);
}

async function startVirtualizorVpsWithCooldown(env, panel, vps, now, timeoutMs) {
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
    const startResult = await startVirtualizorVps(panel, vps.id, timeoutMs);
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

async function virtualizorRequest(panel, params, timeoutMs) {
  const baseUrl = normalizeBaseUrl(panel.baseUrl);
  const url = new URL("/index.php", baseUrl);

  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  url.searchParams.set("api", "json");
  url.searchParams.set("apikey", panel.apiKey);
  url.searchParams.set("apipass", panel.apiPass);

  const finalTimeoutMs = timeoutMs || VIRTUALIZOR_REQUEST_TIMEOUT_MS;
  let response;
  try {
    response = await fetch(url.toString(), {
      method: "GET",
      headers: { Accept: "application/json" },
      signal: AbortSignal.timeout(finalTimeoutMs)
    });
  } catch (error) {
    if (error.name === "TimeoutError" || error.name === "AbortError") {
      const err = new Error(`Virtualizor request timed out after ${finalTimeoutMs / 1000}s`);
      err.isTimeout = true;
      throw err;
    }
    throw error;
  }

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
  const state = await getJson(env, STATE_KEY, { lastRunAt: null, vps: {}, events: [], importantEvents: [], notifications: {} });
  return {
    lastRunAt: state.lastRunAt || null,
    vps: state.vps || {},
    events: Array.isArray(state.events) ? state.events : [],
    importantEvents: Array.isArray(state.importantEvents) ? state.importantEvents : [],
    notifications: state.notifications && typeof state.notifications === "object" ? state.notifications : {}
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

function formatDateTimeWithTimezone(value, timezone) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "-";
  const tz = (timezone && timezone !== "local") ? timezone : "Asia/Shanghai";
  try {
    const options = {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZone: tz
    };
    const formatter = new Intl.DateTimeFormat("zh-CN", options);
    const parts = formatter.formatToParts(date);
    const partMap = {};
    for (const part of parts) {
      partMap[part.type] = part.value;
    }
    return `${partMap.year}-${partMap.month}-${partMap.day} ${partMap.hour}:${partMap.minute}:${partMap.second}`;
  } catch (e) {
    return date.toISOString();
  }
}

function normalizeSettings(input) {
  return {
    checkIntervalNote: DEFAULT_SETTINGS.checkIntervalNote,
    defaultFailureThreshold: positiveInteger(input?.defaultFailureThreshold, 2),
    apiTimeout: positiveInteger(input?.apiTimeout, 15),
    timezone: typeof input?.timezone === "string" ? input.timezone : "local",
    notifications: normalizeNotifications(input?.notifications),
    panels: Array.isArray(input?.panels) ? input.panels.map(normalizePanel) : []
  };
}

function normalizeNotifications(input) {
  const events = input?.events || {};
  const offlinePolicy = ["first", "threshold", "every"].includes(input?.offlinePolicy) ? input.offlinePolicy : "threshold";
  return {
    enabled: Boolean(input?.enabled ?? false),
    events: {
      offline: Boolean(events.offline ?? true),
      start: Boolean(events.start ?? true),
      recovered: Boolean(events.recovered ?? true),
      error: Boolean(events.error ?? true)
    },
    offlinePolicy,
    channels: Array.isArray(input?.channels) ? input.channels.map(normalizeNotificationChannel) : []
  };
}

function normalizeNotificationChannel(input) {
  const provider = ["webhook", "serverChan", "pushPlus", "telegram", "feishu", "dingtalk", "wecom"].includes(input?.provider)
    ? input.provider
    : "webhook";
  return {
    id: String(input?.id || crypto.randomUUID()),
    name: String(input?.name || notificationProviderLabel(provider)).trim(),
    enabled: Boolean(input?.enabled ?? true),
    provider,
    config: normalizeNotificationConfig(provider, input?.config || {})
  };
}

function normalizeNotificationConfig(provider, input) {
  const config = {};
  if (provider === "webhook") {
    config.webhookUrl = String(input.webhookUrl || "").trim();
  }
  if (provider === "serverChan") {
    config.sendKey = String(input.sendKey || "").trim();
  }
  if (provider === "pushPlus") {
    config.token = String(input.token || "").trim();
    config.topic = String(input.topic || "").trim();
  }
  if (provider === "telegram") {
    config.botToken = String(input.botToken || "").trim();
    config.chatId = String(input.chatId || "").trim();
  }
  if (provider === "feishu" || provider === "dingtalk") {
    config.webhookUrl = String(input.webhookUrl || "").trim();
    config.secret = String(input.secret || "").trim();
  }
  if (provider === "wecom") {
    config.webhookUrl = String(input.webhookUrl || "").trim();
  }
  return config;
}

function notificationProviderLabel(provider) {
  return {
    webhook: "通用 Webhook",
    serverChan: "Server 酱",
    pushPlus: "PushPlus",
    telegram: "Telegram Bot",
    feishu: "飞书机器人",
    dingtalk: "钉钉机器人",
    wecom: "企业微信机器人"
  }[provider] || "通知渠道";
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
    enabled: Boolean(input.enabled ?? true),
    autoStart: Boolean(input.autoStart ?? true),
    cron: normalizeCronExpression(input.cron),
    failureThreshold: positiveInteger(input.failureThreshold, 2),
    startCooldownMinutes: positiveInteger(input.startCooldownMinutes, DEFAULT_START_COOLDOWN_MINUTES)
  };
}

function normalizeBaseUrl(value) {
  const trimmed = String(value || "").trim().replace(/\/+$/, "");
  if (!trimmed) return "";
  try {
    const url = new URL(trimmed);
    if (!["http:", "https:"].includes(url.protocol)) {
      throw new Error("Virtualizor panel URL must use http or https");
    }
    return `${url.protocol}//${url.host}`;
  } catch (error) {
    throw httpError(400, `Invalid Virtualizor panel URL: ${trimmed}`);
  }
}

function normalizeCronExpression(value) {
  const expression = String(value || DEFAULT_VPS_CRON).trim();
  validateCronExpression(expression);
  return expression;
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
    failureThreshold: result.failureThreshold || null,
    started: Boolean(result.started),
    startSuppressed: Boolean(result.startSuppressed),
    lastStartAttemptAt: result.lastStartAttemptAt || null,
    startCooldownUntil: result.startCooldownUntil || null,
    error: result.error || null,
    startResult: result.startResult || null,
    notificationResults: []
  };

  state.events = limitEventsPerVps([event, ...(Array.isArray(state.events) ? state.events : [])], MAX_EVENT_LOGS_PER_VPS);
  if (important) {
    state.importantEvents = limitEventsPerVps([event, ...(Array.isArray(state.importantEvents) ? state.importantEvents : [])], MAX_EVENT_LOGS_PER_VPS);
  }
  return event;
}

function limitEventsPerVps(events, maxPerVps) {
  const counts = new Map();
  const limited = [];

  for (const event of events) {
    const key = `${event.panelId}:${event.vpsId}`;
    const count = counts.get(key) || 0;
    if (count >= maxPerVps) continue;

    counts.set(key, count + 1);
    limited.push(event);
  }

  return limited;
}

function ensureImportantEvent(state, event) {
  const importantEvents = Array.isArray(state.importantEvents) ? state.importantEvents : [];
  if (importantEvents.some((item) => item.id === event.id)) {
    return;
  }
  state.importantEvents = limitEventsPerVps([event, ...importantEvents], MAX_EVENT_LOGS_PER_VPS);
}

async function sendNotificationsForEvent(settings, state, event, previous) {
  const notificationType = resolveNotificationType(event, previous);
  if (event.online && !event.error) {
    resetOutageNotificationState(state, event);
  }

  if (!notificationType) {
    return [];
  }

  const notifications = settings.notifications || normalizeNotifications(null);
  if (!notifications.enabled || !notifications.events?.[notificationType]) {
    return [];
  }

  if (!shouldNotifyEvent(notifications, state, event, notificationType)) {
    return [];
  }

  const channels = (notifications.channels || []).filter((channel) => channel.enabled);
  if (!channels.length) {
    return [];
  }

  const message = buildNotificationMessage(event, notificationType, settings.timezone);
  const timeoutMs = (settings.apiTimeout || 15) * 1000;
  const results = await Promise.all(channels.map((channel) => sendNotificationChannel(channel, message, event, timeoutMs)));
  updateNotificationState(state, event, notificationType);
  return results;
}

function resolveNotificationType(event, previous) {
  if (event.started) return "start";
  if (event.error) return "error";
  if (event.online && previous?.checkedAt && (previous.online === false || previous.error)) return "recovered";
  if (!event.online && !event.startSuppressed) return "offline";
  return null;
}

function shouldNotifyEvent(notifications, state, event, notificationType) {
  const key = `${event.panelId}:${event.vpsId}`;
  const record = state.notifications?.[key] || {};

  if (notificationType === "offline") {
    if (notifications.offlinePolicy === "every") return true;
    if (record.offlineNotified) return false;
    if (notifications.offlinePolicy === "first") return true;
    return Number(event.failureCount || 0) >= Number(event.failureThreshold || 1);
  }

  if (notificationType === "error") {
    return !record.errorNotified;
  }

  if (notificationType === "recovered") {
    return !record.recoveredNotified;
  }

  return true;
}

function resetOutageNotificationState(state, event) {
  if (!state.notifications || typeof state.notifications !== "object") {
    state.notifications = {};
  }

  const key = `${event.panelId}:${event.vpsId}`;
  const record = state.notifications[key] || {};
  record.offlineNotified = false;
  record.errorNotified = false;
  record.recoveredNotified = false;
  state.notifications[key] = record;
}

function updateNotificationState(state, event, notificationType) {
  if (!state.notifications || typeof state.notifications !== "object") {
    state.notifications = {};
  }

  const key = `${event.panelId}:${event.vpsId}`;
  const record = state.notifications[key] || {};

  if (notificationType === "offline") {
    record.offlineNotified = true;
    record.recoveredNotified = false;
    record.lastOfflineNotifiedAt = event.at;
  }

  if (notificationType === "error") {
    record.errorNotified = true;
    record.recoveredNotified = false;
    record.lastErrorNotifiedAt = event.at;
  }

  if (notificationType === "recovered") {
    record.recoveredNotified = true;
    record.lastRecoveredNotifiedAt = event.at;
  }

  if (notificationType === "start") {
    record.lastStartNotifiedAt = event.at;
  }

  state.notifications[key] = record;
}

async function sendTestNotification(settings, channelId) {
  const notifications = settings.notifications || normalizeNotifications(null);
  const channels = (notifications.channels || []).filter((channel) => {
    if (channelId) return channel.id === channelId;
    return channel.enabled;
  });

  if (!channels.length) {
    throw httpError(400, channelId ? "Notification channel not found" : "No enabled notification channels");
  }

  const now = new Date().toISOString();
  const event = {
    id: crypto.randomUUID(),
    type: "test",
    level: "info",
    at: now,
    panelName: "Test Panel",
    panelId: "test-panel",
    vpsId: "test-vps",
    name: "Test VPS",
    online: true,
    failureCount: 0,
    error: null
  };
  const message = {
    title: "VPS 通知测试",
    content: ["这是一条测试通知。", `时间：${formatDateTimeWithTimezone(now, settings.timezone)}`].join("\n")
  };
  const timeoutMs = (settings.apiTimeout || 15) * 1000;
  const results = await Promise.all(channels.map((channel) => sendNotificationChannel(channel, message, event, timeoutMs)));
  return { ok: results.every((item) => item.ok), results };
}

function buildNotificationMessage(event, notificationType, timezone) {
  const titleMap = {
    offline: "VPS 离线",
    start: "VPS 启动命令成功",
    recovered: "VPS 恢复在线",
    error: "VPS 检查错误"
  };
  const lines = [
    `面板：${event.panelName || event.panelId}`,
    `VPS：${event.name || event.vpsId}`,
    `VPS ID：${event.vpsId}`,
    `状态：${event.error ? "Error" : event.online ? "Online" : "Offline"}`,
    `失败次数：${event.failureCount || 0}`,
    `时间：${formatDateTimeWithTimezone(event.at, timezone)}`
  ];

  if (event.error) {
    lines.push(`错误：${event.error}`);
  }
  if (event.startCooldownUntil) {
    lines.push(`启动防重至：${formatDateTimeWithTimezone(event.startCooldownUntil, timezone)}`);
  }

  return {
    title: titleMap[notificationType] || "VPS 通知",
    content: lines.join("\n")
  };
}

async function sendNotificationChannel(channel, message, event, timeoutMs) {
  const sentAt = new Date().toISOString();
  try {
    await sendNotificationRequest(channel, message, event, timeoutMs);
    return notificationResult(channel, true, null, sentAt);
  } catch (error) {
    return notificationResult(channel, false, error.message || String(error), sentAt);
  }
}

function notificationResult(channel, ok, error, sentAt) {
  return {
    channelId: channel.id,
    channelName: channel.name,
    provider: channel.provider,
    ok,
    error,
    sentAt
  };
}

async function sendNotificationRequest(channel, message, event, timeoutMs) {
  const config = channel.config || {};
  if (channel.provider === "serverChan") {
    requireNotificationValue(config.sendKey, "Server 酱 SendKey");
    const body = new URLSearchParams({ title: message.title, desp: message.content });
    return checkedNotificationFetch(`https://sctapi.ftqq.com/${encodeURIComponent(config.sendKey)}.send`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body
    }, timeoutMs);
  }

  if (channel.provider === "pushPlus") {
    requireNotificationValue(config.token, "PushPlus Token");
    return checkedNotificationFetch("https://www.pushplus.plus/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token: config.token,
        title: message.title,
        content: message.content,
        topic: config.topic || undefined,
        template: "txt"
      })
    }, timeoutMs);
  }

  if (channel.provider === "telegram") {
    requireNotificationValue(config.botToken, "Telegram Bot Token");
    requireNotificationValue(config.chatId, "Telegram Chat ID");
    return checkedNotificationFetch(`https://api.telegram.org/bot${encodeURIComponent(config.botToken)}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: config.chatId, text: `${message.title}\n\n${message.content}` })
    }, timeoutMs);
  }

  if (channel.provider === "feishu") {
    requireNotificationValue(config.webhookUrl, "飞书 Webhook URL");
    const payload = {
      msg_type: "text",
      content: { text: `${message.title}\n${message.content}` }
    };
    if (config.secret) {
      const timestamp = String(Math.floor(Date.now() / 1000));
      payload.timestamp = timestamp;
      payload.sign = await signFeishu(timestamp, config.secret);
    }
    return checkedNotificationFetch(config.webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }, timeoutMs);
  }

  if (channel.provider === "dingtalk") {
    requireNotificationValue(config.webhookUrl, "钉钉 Webhook URL");
    const url = new URL(config.webhookUrl);
    if (config.secret) {
      const timestamp = String(Date.now());
      url.searchParams.set("timestamp", timestamp);
      url.searchParams.set("sign", await signDingtalk(timestamp, config.secret));
    }
    return checkedNotificationFetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ msgtype: "text", text: { content: `${message.title}\n${message.content}` } })
    }, timeoutMs);
  }

  if (channel.provider === "wecom") {
    requireNotificationValue(config.webhookUrl, "企业微信 Webhook URL");
    return checkedNotificationFetch(config.webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ msgtype: "text", text: { content: `${message.title}\n${message.content}` } })
    }, timeoutMs);
  }

  requireNotificationValue(config.webhookUrl, "Webhook URL");
  return checkedNotificationFetch(config.webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title: message.title,
      content: message.content,
      event
    })
  }, timeoutMs);
}

function requireNotificationValue(value, label) {
  if (!String(value || "").trim()) {
    throw new Error(`Missing ${label}`);
  }
}

async function checkedNotificationFetch(url, init, timeoutMs) {
  const response = await fetch(url, {
    ...init,
    signal: AbortSignal.timeout(timeoutMs || VIRTUALIZOR_REQUEST_TIMEOUT_MS)
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Notification ${response.status}: ${text.slice(0, 200)}`);
  }
  return text;
}

async function signFeishu(timestamp, secret) {
  return hmacSha256Base64("", `${timestamp}\n${secret}`);
}

async function signDingtalk(timestamp, secret) {
  return hmacSha256Base64(`${timestamp}\n${secret}`, secret);
}

async function hmacSha256Base64(message, secret) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(message));
  const bytes = new Uint8Array(signature);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function validateCronExpression(expression) {
  try {
    parseCronExpression(expression);
  } catch (error) {
    throw httpError(400, `Invalid cron expression: ${expression}`);
  }
}

function isCronDue(expression, date) {
  const fields = parseCronExpression(expression);
  const values = [
    date.getUTCMinutes(),
    date.getUTCHours(),
    date.getUTCDate(),
    date.getUTCMonth() + 1,
    date.getUTCDay()
  ];
  return fields.every((field, index) => field.has(normalizeCronValue(values[index], index === 4)));
}

function parseCronExpression(expression) {
  const parts = String(expression || DEFAULT_VPS_CRON).trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: ${expression}`);
  }

  const ranges = [
    [0, 59],
    [0, 23],
    [1, 31],
    [1, 12],
    [0, 7]
  ];

  return parts.map((part, index) => parseCronField(part, ranges[index][0], ranges[index][1], index === 4));
}

function parseCronField(field, min, max, isDayOfWeek = false) {
  const values = new Set();
  for (const segment of field.split(",")) {
    for (const value of parseCronSegment(segment.trim(), min, max, isDayOfWeek)) {
      values.add(value);
    }
  }

  return values;
}

function parseCronSegment(segment, min, max, isDayOfWeek) {
  if (!segment) throw new Error("Empty cron segment");

  const stepParts = segment.split("/");
  if (stepParts.length > 2) throw new Error("Invalid cron step");

  const [base, stepText] = stepParts;
  const step = stepText === undefined ? 1 : Number(stepText);
  if (!Number.isInteger(step) || step < 1) throw new Error("Invalid cron step");

  let start = min;
  let end = max;

  if (base !== "*") {
    if (base.includes("-")) {
      const rangeParts = base.split("-");
      if (rangeParts.length !== 2) throw new Error("Invalid cron range");

      const [rangeStart, rangeEnd] = rangeParts.map(Number);
      start = rangeStart;
      end = rangeEnd;
    } else {
      start = Number(base);
      end = start;
    }
  }

  if (!Number.isInteger(start) || !Number.isInteger(end)) throw new Error("Invalid cron range");
  if (start < min || end > max || start > end) throw new Error("Invalid cron range");

  const values = [];
  for (let value = start; value <= end; value += 1) {
    if ((value - start) % step === 0) {
      values.push(normalizeCronValue(value, isDayOfWeek));
    }
  }

  return values;
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

function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
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
    button, input, select {
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
    input, select {
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
    main.stack {
      align-content: start;
    }
    [hidden] {
      display: none !important;
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
    .event-controls {
      flex-wrap: nowrap;
      overflow-x: auto;
      padding-bottom: 2px;
    }
    .event-controls select,
    .event-controls input,
    .event-controls button {
      flex: 0 0 auto;
    }
    .event-controls select {
      width: 180px;
      min-width: 150px;
    }
    .event-controls input {
      width: 260px;
    }
    .between {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-width: 0;
      flex-wrap: wrap;
    }
    .top-nav {
      position: sticky;
      top: 0;
      z-index: 20;
      display: flex;
      align-items: center;
      align-self: start;
      gap: 8px;
      min-width: 0;
      overflow-x: auto;
      padding: 6px 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgb(255 255 255 / 95%);
      box-shadow: 0 8px 20px rgb(23 32 51 / 6%);
      backdrop-filter: blur(10px);
    }
    .top-nav button {
      flex: 0 0 auto;
      min-height: 32px;
      padding: 0 10px;
      border-color: transparent;
      background: transparent;
      color: var(--muted);
      font-weight: 700;
    }
    .top-nav button.active {
      border-color: var(--primary);
      background: var(--primary-soft);
      color: var(--primary);
      box-shadow: 0 1px 3px rgb(23 32 51 / 10%);
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
    .notification-events {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 14px;
    }
    .notification-settings {
      gap: 12px;
    }
    .notification-settings .grid {
      gap: 10px 12px;
    }
    .notification-events label {
      display: inline-flex;
      grid-template-columns: none;
      align-items: center;
      gap: 6px;
      min-height: 28px;
      color: var(--text);
      font-size: 13px;
      font-weight: 500;
    }
    .notification-channels {
      display: grid;
      gap: 10px;
    }
    .notification-channel {
      display: grid;
      gap: 10px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fafbfe;
    }
    .notification-channel .grid {
      gap: 10px 12px;
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
    .segmented {
      display: inline-flex;
      flex-wrap: wrap;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f7f9fc;
    }
    .segmented button {
      min-height: 30px;
      border-color: transparent;
      background: transparent;
      color: var(--muted);
      padding: 0 10px;
    }
    .segmented button.active {
      border-color: var(--primary);
      background: #fff;
      color: var(--primary);
      box-shadow: 0 1px 3px rgb(23 32 51 / 10%);
    }
    .table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
    }
    .event-table-scroll {
      height: 451px;
      overflow: auto;
    }
    .event-log-table {
      min-width: 1120px;
      table-layout: fixed;
    }
    .event-log-table th {
      position: sticky;
      top: 0;
      z-index: 1;
      user-select: none;
    }
    .event-log-table th,
    .event-log-table td {
      height: 41px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .event-log-table th[data-col-index] {
      position: sticky;
      padding-right: 16px;
    }
    .event-cell-text {
      display: block;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .column-resizer {
      position: absolute;
      top: 0;
      right: -3px;
      z-index: 2;
      width: 8px;
      height: 100%;
      cursor: col-resize;
      touch-action: none;
    }
    .column-resizer::after {
      content: "";
      position: absolute;
      top: 8px;
      bottom: 8px;
      left: 3px;
      width: 1px;
      background: transparent;
    }
    .column-resizer:hover::after,
    body.column-resizing .column-resizer::after {
      background: #9fb0c8;
    }
    .overflow-tooltip {
      position: fixed;
      z-index: 1000;
      max-width: min(520px, calc(100vw - 32px));
      padding: 8px 10px;
      border-radius: 6px;
      background: #172033;
      color: #f8fafc;
      font-size: 13px;
      line-height: 1.45;
      box-shadow: 0 12px 28px rgb(23 32 51 / 24%);
      overflow-wrap: anywhere;
      pointer-events: none;
      opacity: 0;
      transition: opacity .08s ease;
    }
    .overflow-tooltip.active {
      opacity: 1;
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
    .message-root {
      position: fixed;
      top: 20px;
      left: 50%;
      z-index: 1000;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
      width: min(420px, calc(100vw - 32px));
      pointer-events: none;
      transform: translateX(-50%);
    }
    .message {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 280px;
      max-width: 100%;
      min-height: 40px;
      padding: 9px 16px;
      border: 1px solid #dcdfe6;
      border-radius: 4px;
      background: #f4f4f5;
      color: #606266;
      box-shadow: 0 4px 12px rgb(0 0 0 / 12%);
      font-size: 14px;
      line-height: 1.4;
      animation: message-in 0.18s ease-out;
    }
    .message::before {
      content: "";
      flex: 0 0 auto;
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: currentColor;
    }
    .message.success {
      border-color: #e1f3d8;
      background: #f0f9eb;
      color: #67c23a;
    }
    .message.error {
      border-color: #fde2e2;
      background: #fef0f0;
      color: #f56c6c;
    }
    .message span {
      min-width: 0;
      color: #606266;
      overflow-wrap: anywhere;
    }
    @keyframes message-in {
      from {
        opacity: 0;
        transform: translateY(-12px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
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
    .pagination {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 10px;
      padding-top: 8px;
      color: var(--text);
      flex-wrap: wrap;
    }
    .pagination[hidden] {
      display: none;
    }
    .pagination-size {
      flex: 0 0 132px;
      width: 132px;
      min-width: 132px;
      height: 36px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0 34px 0 12px;
      background-color: #fff;
      background-image: url("data:image/svg+xml,%3Csvg width='16' height='16' viewBox='0 0 16 16' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M4 6L8 10L12 6' stroke='%23647089' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 12px center;
      color: var(--text);
      font: inherit;
      font-size: 14px;
      appearance: none;
    }
    .pagination-pages {
      display: flex;
      align-items: center;
      gap: 6px;
      min-width: 0;
    }
    .pagination button {
      min-width: 36px;
      height: 36px;
      border-color: transparent;
      border-radius: 8px;
      padding: 0 10px;
      font-size: 14px;
    }
    .pagination-page {
      background: transparent;
      color: var(--text);
    }
    .pagination-page.active {
      border-color: #0f63c6;
      background: #0f63c6;
      color: #fff;
      font-weight: 700;
    }
    .pagination-nav {
      background: #eaf2fb;
      color: #0f63c6;
      font-size: 22px;
      line-height: 1;
    }
    .pagination-nav:disabled {
      background: #f1f3f5;
      color: #aeb8c5;
      opacity: 1;
    }
    .pagination-ellipsis {
      min-width: 28px;
      text-align: center;
      color: var(--muted);
      font-size: 14px;
      font-weight: 700;
    }
    .pagination-jumper {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 14px;
    }
    .pagination-jumper input {
      width: 76px;
      height: 36px;
      text-align: center;
      font-size: 14px;
    }
    .pagination-jump {
      min-width: 72px;
      border-color: #0f63c6 !important;
      background: #0f63c6 !important;
      color: #fff !important;
      font-weight: 700;
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
      .pagination {
        justify-content: flex-start;
      }
      .pagination-pages {
        max-width: 100%;
        overflow-x: auto;
        padding-bottom: 2px;
      }
      .pagination-jumper input {
        width: 72px;
      }
    }
  </style>
</head>
<body>
  <div class="message-root" id="messageRoot" aria-live="polite"></div>
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
      <nav class="top-nav" id="mainNav" aria-label="一级导航">
        <button class="active" type="button" data-main-view="overview" aria-current="page">总览</button>
        <button type="button" data-main-view="manage">管理</button>
        <button type="button" data-main-view="logs">日志</button>
        <button type="button" data-main-view="settings">系统与通知设置</button>
      </nav>

      <section class="card stack" data-main-view-panel="manage">
        <div class="between">
          <div>
            <h2>快捷操作</h2>
            <div class="muted">Cron 会按 wrangler.toml 里的频率执行。</div>
          </div>
          <div class="row">
            <button id="addPanelBtn">新增面板</button>
            <button id="refreshBtn">拉取 VPS</button>
            <button id="checkBtn">立即检查</button>
            <button class="primary" id="saveBtn">保存配置</button>
          </div>
        </div>
        <div class="notice" id="mainNotice"></div>
      </section>

      <section class="card stack notification-settings" data-main-view-panel="settings">
        <div class="between">
          <div>
            <h2>系统与通知设置</h2>
            <div class="muted">一个事件会发送到所有已启用渠道。</div>
          </div>
          <div class="row">
            <button id="addNotificationChannelBtn" type="button">新增渠道</button>
            <button id="testAllNotificationsBtn" type="button">测试全部</button>
          </div>
        </div>
        <div class="grid">
          <label>
            默认离线阈值
            <input id="defaultFailureThreshold" type="number" min="1" step="1">
          </label>
          <label>
            接口超时时间 (秒)
            <input id="apiTimeout" type="number" min="1" step="1">
          </label>
          <label>
            系统显示时区
            <select id="systemTimezone">
              <option value="local">浏览器本地时间</option>
              <option value="UTC">UTC (格林威治时间)</option>
              <option value="Asia/Shanghai">Asia/Shanghai (北京时间)</option>
              <option value="Asia/Hong_Kong">Asia/Hong_Kong (香港时间)</option>
              <option value="Asia/Tokyo">Asia/Tokyo (东京时间)</option>
              <option value="America/New_York">America/New_York (美东时间)</option>
              <option value="Europe/London">Europe/London (伦敦时间)</option>
            </select>
          </label>
          <label class="row" style="gap: 6px; color: var(--muted);">
            <input class="switch" id="notificationsEnabled" type="checkbox">启用通知
          </label>
          <label>
            离线通知策略
            <select id="notificationOfflinePolicy">
              <option value="threshold">达到离线阈值</option>
              <option value="first">首次离线</option>
              <option value="every">每次离线</option>
            </select>
          </label>
        </div>
        <div class="notification-events" id="notificationEvents"></div>
        <div class="notification-channels" id="notificationChannels"></div>
      </section>

      <section class="card stack" data-main-view-panel="overview">
        <div class="between">
          <h2>状态总览</h2>
          <div class="row">
            <span class="muted">按范围查看</span>
            <div class="segmented" id="statusOverviewScope">
              <button class="active" type="button" data-status-scope="current">当前面板</button>
              <button type="button" data-status-scope="all">所有面板</button>
            </div>
          </div>
        </div>
        <div id="statusOverview"></div>
      </section>

      <section class="card stack" id="panelEditor" data-main-view-panel="manage"></section>
      <section class="card stack" data-main-view-panel="overview">
        <div class="between">
          <h2>最近状态</h2>
          <span class="muted" id="lastRunAt">未检查</span>
        </div>
        <div id="stateTable"></div>
      </section>
      <section class="card stack" data-main-view-panel="logs">
        <div class="between">
          <h2>事件日志</h2>
          <span class="muted">每台 VPS 保留最近 100 条</span>
        </div>
        <div class="row event-controls">
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
          <input id="eventSearch" placeholder="搜索 VPS、名称、错误">
          <button id="clearRecentLogsBtn">清除最近</button>
          <button id="clearImportantLogsBtn" class="danger">清除重要</button>
        </div>
        <div id="eventTable"></div>
        <div class="pagination" id="eventPagination" hidden>
          <select class="pagination-size" id="eventPageSize" aria-label="每页条数">
            <option value="10" selected>10条/页</option>
            <option value="20">20条/页</option>
            <option value="50">50条/页</option>
            <option value="100">100条/页</option>
          </select>
          <div class="pagination-pages" id="eventPageButtons"></div>
          <div class="pagination-jumper">
            <span>前往</span>
            <input id="eventPageJump" type="number" min="1" step="1" value="1" aria-label="跳转页码">
            <span>页</span>
            <button class="pagination-jump" id="eventPageJumpBtn" type="button">跳转</button>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const tokenInput = document.querySelector("#adminToken");
    const messageRoot = document.querySelector("#messageRoot");
    const mainNav = document.querySelector("#mainNav");
    const viewPanels = document.querySelectorAll("[data-main-view-panel]");
    const sideNotice = document.querySelector("#sideNotice");
    const mainNotice = document.querySelector("#mainNotice");
    const panelList = document.querySelector("#panelList");
    const panelEditor = document.querySelector("#panelEditor");
    const statusOverview = document.querySelector("#statusOverview");
    const statusOverviewScope = document.querySelector("#statusOverviewScope");
    const stateTable = document.querySelector("#stateTable");
    const eventTable = document.querySelector("#eventTable");
    const eventPagination = document.querySelector("#eventPagination");
    const eventPageButtons = document.querySelector("#eventPageButtons");
    const eventLogType = document.querySelector("#eventLogType");
    const eventLevelFilter = document.querySelector("#eventLevelFilter");
    const eventVpsFilter = document.querySelector("#eventVpsFilter");
    const eventPageSize = document.querySelector("#eventPageSize");
    const eventPageJump = document.querySelector("#eventPageJump");
    const eventPageJumpBtn = document.querySelector("#eventPageJumpBtn");
    const eventSearch = document.querySelector("#eventSearch");
    const lastRunAt = document.querySelector("#lastRunAt");
     const defaultFailureThreshold = document.querySelector("#defaultFailureThreshold");
    const apiTimeout = document.querySelector("#apiTimeout");
    const systemTimezone = document.querySelector("#systemTimezone");
    const notificationsEnabled = document.querySelector("#notificationsEnabled");
    const notificationOfflinePolicy = document.querySelector("#notificationOfflinePolicy");
    const notificationEvents = document.querySelector("#notificationEvents");
    const notificationChannels = document.querySelector("#notificationChannels");
    const AUTO_REFRESH_MS = 30000;
    const EVENT_COLUMNS = [
      { label: "时间", width: 190, minWidth: 130 },
      { label: "等级", width: 110, minWidth: 82 },
      { label: "类型", width: 120, minWidth: 88 },
      { label: "面板", width: 170, minWidth: 110 },
      { label: "VPS", width: 170, minWidth: 110 },
      { label: "状态", width: 110, minWidth: 88 },
      { label: "失败次数", width: 120, minWidth: 96 },
      { label: "启动结果", width: 170, minWidth: 110 },
      { label: "通知", width: 170, minWidth: 110 },
      { label: "错误", width: 240, minWidth: 140 }
    ];

    let settings = { defaultFailureThreshold: 2, apiTimeout: 15, timezone: "local", notifications: defaultNotifications(), panels: [] };
    let state = { lastRunAt: null, vps: {} };
    let selectedPanelId = null;
    let selectedStatusScope = "current";
    let activeMainView = "overview";
    let eventPage = 1;
    let eventColumnWidths = EVENT_COLUMNS.map((column) => column.width);
    let eventColumnResize = null;
    let overflowTooltipTarget = null;
    let autoRefreshTimer = null;
    let liveRefreshInFlight = false;
    let settingsLoaded = false;
    let autoSaveInFlight = false;
    let autoSaveQueued = false;
    let messageTimer = null;
    const overflowTooltip = document.createElement("div");
    overflowTooltip.className = "overflow-tooltip";
    document.body.appendChild(overflowTooltip);

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
    mainNav.querySelectorAll("[data-main-view]").forEach((button) => {
      button.addEventListener("click", () => setMainView(button.dataset.mainView));
    });
    document.querySelector("#addNotificationChannelBtn").addEventListener("click", addNotificationChannel);
    document.querySelector("#testAllNotificationsBtn").addEventListener("click", () => testNotifications(null));
    statusOverviewScope.querySelectorAll("[data-status-scope]").forEach((button) => {
      button.addEventListener("click", () => {
        selectedStatusScope = button.dataset.statusScope;
        renderStatusOverview();
      });
    });
    eventLogType.addEventListener("change", resetEventPageAndRender);
    eventLevelFilter.addEventListener("change", resetEventPageAndRender);
    eventVpsFilter.addEventListener("change", resetEventPageAndRender);
    eventPageSize.addEventListener("change", resetEventPageAndRender);
    eventSearch.addEventListener("input", resetEventPageAndRender);
    eventTable.addEventListener("pointerdown", startEventColumnResize);
    eventTable.addEventListener("mouseover", handleEventCellTooltip);
    eventTable.addEventListener("mousemove", moveOverflowTooltip);
    eventTable.addEventListener("mouseout", hideEventCellTooltip);
    eventPageJumpBtn.addEventListener("click", jumpEventPage);
    eventPageJump.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        jumpEventPage();
      }
    });
    defaultFailureThreshold.addEventListener("focusout", autoSaveSettings);
    apiTimeout.addEventListener("focusout", autoSaveSettings);
    systemTimezone.addEventListener("change", () => {
      autoSaveSettings();
      renderLiveData();
    });
    notificationsEnabled.addEventListener("change", autoSaveSettings);
    notificationOfflinePolicy.addEventListener("change", autoSaveSettings);
    notificationEvents.addEventListener("change", autoSaveSettings);
    notificationChannels.addEventListener("focusout", (event) => {
      if (event.target.matches("input:not([type='checkbox']):not([type='radio'])")) {
        autoSaveSettings();
      }
    });
    notificationChannels.addEventListener("change", (event) => {
      if (event.target.matches("input[type='checkbox'], select")) {
        collectNotificationForm();
        renderNotificationSettings();
        autoSaveSettings();
      }
    });
    panelEditor.addEventListener("focusout", (event) => {
      if (event.target.matches("input:not([type='checkbox']):not([type='radio']), textarea")) {
        autoSaveSettings();
      }
    });
    panelEditor.addEventListener("change", (event) => {
      if (event.target.matches("input[type='checkbox'], input[type='radio'], select")) {
        collectPanelForm();
        renderLiveData();
        autoSaveSettings();
      }
    });
    document.querySelector("#clearRecentLogsBtn").addEventListener("click", () => clearLogs("recent"));
    document.querySelector("#clearImportantLogsBtn").addEventListener("click", () => clearLogs("important"));
    applyMainView();

    function setMainView(view) {
      activeMainView = view;
      applyMainView();

      if (view === "logs") {
        renderEvents();
      }
      if (view === "overview") {
        renderStatusOverview();
        renderState();
      }
    }

    function applyMainView() {
      mainNav.querySelectorAll("[data-main-view]").forEach((button) => {
        const active = button.dataset.mainView === activeMainView;
        button.classList.toggle("active", active);
        if (active) {
          button.setAttribute("aria-current", "page");
        } else {
          button.removeAttribute("aria-current");
        }
      });
      viewPanels.forEach((panel) => {
        panel.hidden = panel.dataset.mainViewPanel !== activeMainView;
      });
    }

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
        settingsLoaded = true;
        selectedPanelId = settings.panels[0]?.id || null;
        syncSettingsFromState();
        render();
        startAutoRefresh();
        setNotice(sideNotice, "已连接。");
      } catch (error) {
        setNotice(sideNotice, error.message, true);
      }
    }

    function startAutoRefresh() {
      if (autoRefreshTimer) {
        clearInterval(autoRefreshTimer);
      }

      autoRefreshTimer = setInterval(refreshLiveState, AUTO_REFRESH_MS);
    }

    async function refreshLiveState() {
      if (!tokenInput.value.trim() || liveRefreshInFlight) return;

      liveRefreshInFlight = true;
      try {
        state = await api("/api/state");
        syncSettingsFromState();
        renderLiveData();
      } catch (error) {
        setNotice(sideNotice, error.message, true);
      } finally {
        liveRefreshInFlight = false;
      }
    }

    function syncSettingsFromState() {
      for (const panel of settings.panels || []) {
        for (const vps of panel.vps || []) {
          const row = (state.vps || {})[panel.id + ":" + vps.id];
          if (!row?.checkedAt) continue;

          if (!row.error) {
            vps.status = Number.isFinite(row.status) ? row.status : row.online ? 1 : 0;
            vps.lastOnline = row.online;
          }

          vps.lastCheckedAt = row.checkedAt;
          vps.failureCount = row.failureCount || 0;
          vps.lastStartAttemptAt = row.lastStartAttemptAt || null;
          vps.startCooldownUntil = row.startCooldownUntil || null;
        }
      }
    }

    async function saveSettings() {
      try {
        await persistSettings({ renderAfterSave: true, updateLocalSettings: true });
        showMessage("配置已保存成功。", "success");
      } catch (error) {
        showMessage("配置保存失败：" + error.message, "error");
      }
    }

    async function autoSaveSettings() {
      if (!settingsLoaded || !tokenInput.value.trim()) return;
      if (autoSaveInFlight) {
        autoSaveQueued = true;
        return;
      }

      autoSaveInFlight = true;
      try {
        do {
          autoSaveQueued = false;
          await persistSettings({ renderAfterSave: false, updateLocalSettings: false });
          showMessage("配置已自动保存成功。", "success");
        } while (autoSaveQueued);
      } catch (error) {
        autoSaveQueued = false;
        showMessage("自动保存失败：" + error.message, "error");
      } finally {
        autoSaveInFlight = false;
      }
    }

    async function persistSettings({ renderAfterSave, updateLocalSettings }) {
      settings.defaultFailureThreshold = Number(defaultFailureThreshold.value || 2);
      settings.apiTimeout = Number(apiTimeout.value || 15);
      settings.timezone = systemTimezone.value || "local";
      collectNotificationForm();
      collectPanelForm();
      const savedSettings = await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
      if (updateLocalSettings) {
        settings = savedSettings;
      }
      if (renderAfterSave) {
        render();
      }
    }

    async function refreshVps() {
      try {
        collectNotificationForm();
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
        collectNotificationForm();
        collectPanelForm();
        await api("/api/settings", { method: "PUT", body: JSON.stringify(settings) });
        const result = await api("/api/check-now", { method: "POST", body: "{}" });
        settings = await api("/api/settings");
        state = await api("/api/state");
        syncSettingsFromState();
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
          setNotice(mainNotice, "启动防重中，冷却至：" + (response.result.startCooldownUntil ? formatDateTime(response.result.startCooldownUntil) : "-"));
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

    function defaultNotifications() {
      return {
        enabled: false,
        events: { offline: true, start: true, recovered: true, error: true },
        offlinePolicy: "threshold",
        channels: []
      };
    }

    function normalizeClientNotifications(value) {
      const base = defaultNotifications();
      const events = value?.events || {};
      return {
        enabled: Boolean(value?.enabled ?? base.enabled),
        events: {
          offline: Boolean(events.offline ?? true),
          start: Boolean(events.start ?? true),
          recovered: Boolean(events.recovered ?? true),
          error: Boolean(events.error ?? true)
        },
        offlinePolicy: ["first", "threshold", "every"].includes(value?.offlinePolicy) ? value.offlinePolicy : "threshold",
        channels: Array.isArray(value?.channels) ? value.channels.map(normalizeClientChannel) : []
      };
    }

    function normalizeClientChannel(value) {
      const provider = ["webhook", "serverChan", "pushPlus", "telegram", "feishu", "dingtalk", "wecom"].includes(value?.provider) ? value.provider : "webhook";
      return {
        id: value?.id || crypto.randomUUID(),
        name: value?.name || providerLabel(provider),
        enabled: Boolean(value?.enabled ?? true),
        provider,
        config: value?.config || {}
      };
    }

    function addNotificationChannel() {
      collectNotificationForm();
      settings.notifications = normalizeClientNotifications(settings.notifications);
      settings.notifications.channels.push({
        id: crypto.randomUUID(),
        name: "通用 Webhook",
        enabled: true,
        provider: "webhook",
        config: { webhookUrl: "" }
      });
      renderNotificationSettings();
    }

    function removeNotificationChannel(id) {
      collectNotificationForm();
      settings.notifications.channels = settings.notifications.channels.filter((channel) => channel.id !== id);
      renderNotificationSettings();
      autoSaveSettings();
    }

    async function testNotifications(channelId) {
      try {
        await persistSettings({ renderAfterSave: false, updateLocalSettings: true });
        const result = await api("/api/notifications/test", {
          method: "POST",
          body: JSON.stringify({ channelId })
        });
        const failed = (result.results || []).filter((item) => !item.ok);
        if (failed.length) {
          showMessage("通知测试失败：" + failed.map((item) => item.channelName + " " + item.error).join("；"), "error");
        } else {
          showMessage("通知测试成功，渠道数：" + (result.results || []).length, "success");
        }
      } catch (error) {
        showMessage("通知测试失败：" + error.message, "error");
      }
    }

    function renderNotificationSettings() {
      const notifications = normalizeClientNotifications(settings.notifications);
      settings.notifications = notifications;
      notificationsEnabled.checked = notifications.enabled;
      notificationOfflinePolicy.value = notifications.offlinePolicy;

      const eventItems = [
        ["offline", "VPS 离线"],
        ["start", "启动命令成功"],
        ["recovered", "恢复在线"],
        ["error", "检查错误"]
      ];
      notificationEvents.innerHTML = eventItems.map((item) => {
        const key = item[0];
        const label = item[1];
        return '<label><input class="switch" data-notification-event="' + key + '" type="checkbox"' + (notifications.events[key] ? " checked" : "") + '>' + label + '</label>';
      }).join("");

      if (!notifications.channels.length) {
        notificationChannels.innerHTML = '<div class="empty">暂无通知渠道。</div>';
        return;
      }

      notificationChannels.innerHTML = notifications.channels.map((channel, index) => {
        return '<div class="notification-channel" data-channel-index="' + index + '">' +
          '<div class="between">' +
            '<div class="row">' +
              '<label class="row" style="gap: 6px; color: var(--muted);"><input class="switch" data-channel-field="enabled" type="checkbox"' + (channel.enabled ? " checked" : "") + '>启用</label>' +
              '<strong>' + escapeHtml(channel.name || providerLabel(channel.provider)) + '</strong>' +
              '<span class="badge">' + escapeHtml(providerLabel(channel.provider)) + '</span>' +
            '</div>' +
            '<div class="row">' +
              '<button type="button" data-test-channel="' + escapeAttr(channel.id) + '">测试</button>' +
              '<button class="danger" type="button" data-remove-channel="' + escapeAttr(channel.id) + '">删除</button>' +
            '</div>' +
          '</div>' +
          '<div class="grid">' +
            '<label>渠道名称<input data-channel-field="name" value="' + escapeAttr(channel.name || "") + '"></label>' +
            '<label>渠道类型' + providerSelect(channel.provider) + '</label>' +
            channelConfigFields(channel) +
          '</div>' +
        '</div>';
      }).join("");

      notificationChannels.querySelectorAll("[data-remove-channel]").forEach((button) => {
        button.addEventListener("click", () => removeNotificationChannel(button.dataset.removeChannel));
      });
      notificationChannels.querySelectorAll("[data-test-channel]").forEach((button) => {
        button.addEventListener("click", () => testNotifications(button.dataset.testChannel));
      });
    }

    function providerSelect(value) {
      return '<select data-channel-field="provider">' +
        providerOptions().map((item) => '<option value="' + item[0] + '"' + (item[0] === value ? " selected" : "") + '>' + item[1] + '</option>').join("") +
        '</select>';
    }

    function providerOptions() {
      return [
        ["webhook", "通用 Webhook"],
        ["serverChan", "Server 酱"],
        ["pushPlus", "PushPlus"],
        ["telegram", "Telegram Bot"],
        ["feishu", "飞书机器人"],
        ["dingtalk", "钉钉机器人"],
        ["wecom", "企业微信机器人"]
      ];
    }

    function providerLabel(provider) {
      const item = providerOptions().find((option) => option[0] === provider);
      return item ? item[1] : "通知渠道";
    }

    function channelConfigFields(channel) {
      const config = channel.config || {};
      if (channel.provider === "serverChan") {
        return '<label>SendKey<input data-channel-config="sendKey" value="' + escapeAttr(config.sendKey || "") + '"></label>';
      }
      if (channel.provider === "pushPlus") {
        return '<label>Token<input data-channel-config="token" value="' + escapeAttr(config.token || "") + '"></label>' +
          '<label>Topic<input data-channel-config="topic" value="' + escapeAttr(config.topic || "") + '" placeholder="可选"></label>';
      }
      if (channel.provider === "telegram") {
        return '<label>Bot Token<input data-channel-config="botToken" value="' + escapeAttr(config.botToken || "") + '"></label>' +
          '<label>Chat ID<input data-channel-config="chatId" value="' + escapeAttr(config.chatId || "") + '"></label>';
      }
      if (channel.provider === "feishu" || channel.provider === "dingtalk") {
        return '<label>Webhook URL<input data-channel-config="webhookUrl" value="' + escapeAttr(config.webhookUrl || "") + '"></label>' +
          '<label>Secret<input data-channel-config="secret" value="' + escapeAttr(config.secret || "") + '" placeholder="可选"></label>';
      }
      return '<label>Webhook URL<input data-channel-config="webhookUrl" value="' + escapeAttr(config.webhookUrl || "") + '"></label>';
    }

    function collectNotificationForm() {
      const notifications = normalizeClientNotifications(settings.notifications);
      notifications.enabled = notificationsEnabled.checked;
      notifications.offlinePolicy = notificationOfflinePolicy.value;
      notificationEvents.querySelectorAll("[data-notification-event]").forEach((input) => {
        notifications.events[input.dataset.notificationEvent] = input.checked;
      });

      notificationChannels.querySelectorAll("[data-channel-index]").forEach((item) => {
        const channel = notifications.channels[Number(item.dataset.channelIndex)];
        if (!channel) return;
        channel.config = {};
        item.querySelectorAll("[data-channel-field]").forEach((input) => {
          if (input.dataset.channelField === "enabled") {
            channel.enabled = input.checked;
          } else {
            channel[input.dataset.channelField] = input.value.trim();
          }
        });
        item.querySelectorAll("[data-channel-config]").forEach((input) => {
          channel.config[input.dataset.channelConfig] = input.value.trim();
        });
        if (!channel.name) {
          channel.name = providerLabel(channel.provider);
        }
      });

      settings.notifications = notifications;
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
      setMainView("manage");
    }

    function removePanel(id) {
      settings.panels = settings.panels.filter((panel) => panel.id !== id);
      selectedPanelId = settings.panels[0]?.id || null;
      render();
    }

    function render() {
      defaultFailureThreshold.value = settings.defaultFailureThreshold || 2;
      apiTimeout.value = settings.apiTimeout || 15;
      systemTimezone.value = settings.timezone || "local";
      settings.notifications = normalizeClientNotifications(settings.notifications);
      renderNotificationSettings();
      renderPanelList();
      renderPanelEditor();
      renderEventFilters();
      renderLiveData();
      applyMainView();
    }

    function renderLiveData() {
      renderStatusOverview();
      renderState();
      renderEvents();
      renderLiveVpsTable();
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
                <td data-vps-live="ip" data-vps-key="\${escapeAttr(panel.id + ":" + vps.id)}">\${escapeHtml(getLiveVpsDisplay(panel, vps).ip)}</td>
                <td data-vps-live="status" data-vps-key="\${escapeAttr(panel.id + ":" + vps.id)}">\${getLiveVpsDisplay(panel, vps).status}</td>
                <td><input data-vps-field="cron" data-vps-index="\${index}" value="\${escapeAttr(vps.cron || "*/5 * * * *")}" placeholder="*/5 * * * *"></td>
                <td><input class="switch" data-vps-field="autoStart" data-vps-index="\${index}" type="checkbox" \${vps.autoStart ? "checked" : ""}></td>
                <td><input data-vps-field="failureThreshold" data-vps-index="\${index}" type="number" min="1" value="\${vps.failureThreshold || 2}"></td>
                <td><input data-vps-field="startCooldownMinutes" data-vps-index="\${index}" type="number" min="1" value="\${vps.startCooldownMinutes || 15}"></td>
                <td data-vps-live="failureCount" data-vps-key="\${escapeAttr(panel.id + ":" + vps.id)}">\${getLiveVpsDisplay(panel, vps).failureCount}</td>
                <td data-vps-live="lastCheckedAt" data-vps-key="\${escapeAttr(panel.id + ":" + vps.id)}">\${escapeHtml(getLiveVpsDisplay(panel, vps).lastCheckedAt)}</td>
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

    function renderLiveVpsTable() {
      const panel = settings.panels.find((item) => item.id === selectedPanelId);
      if (!panel) return;

      const displays = new Map((panel.vps || []).map((vps) => {
        return [panel.id + ":" + vps.id, getLiveVpsDisplay(panel, vps)];
      }));

      panelEditor.querySelectorAll("[data-vps-live][data-vps-key]").forEach((cell) => {
        const display = displays.get(cell.dataset.vpsKey);
        if (!display) return;

        const value = display[cell.dataset.vpsLive];
        if (cell.dataset.vpsLive === "status") {
          cell.innerHTML = value;
        } else {
          cell.textContent = value;
        }
      });
    }

    function getLiveVpsDisplay(panel, vps) {
      const row = (state.vps || {})[panel.id + ":" + vps.id];
      const rawCheckedAt = row?.checkedAt || vps.lastCheckedAt;
      const formattedCheckedAt = rawCheckedAt && rawCheckedAt !== "-" ? formatDateTime(rawCheckedAt) : "-";
      return {
        ip: row?.ip || vps.ip || "-",
        status: row?.checkedAt ? monitorStatusBadge(row) : statusBadge(vps.status),
        failureCount: row?.checkedAt ? row.failureCount || 0 : vps.failureCount || 0,
        lastCheckedAt: formattedCheckedAt
      };
    }

    function renderStatusOverview() {
      renderStatusOverviewScope();

      const groups = getStatusOverviewGroups();
      const services = groups.flatMap((group) => group.vps.map((vps) => ({ panel: group.panel, vps })));

      if (!groups.length) {
        statusOverview.innerHTML = '<div class="empty">' + (selectedStatusScope === "all" ? "暂无面板。" : "暂无当前面板。") + '</div>';
        return;
      }

      if (!services.length) {
        statusOverview.innerHTML = '<div class="empty">' + (selectedStatusScope === "all" ? "所有面板暂无 VPS。" : "当前面板暂无 VPS。") + '</div>';
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
        <div class="status-footer">最后更新于 \${escapeHtml(state.lastRunAt && state.lastRunAt !== "-" ? formatDateTime(state.lastRunAt) : "-")}</div>
      \`;
    }

    function renderStatusOverviewScope() {
      statusOverviewScope.querySelectorAll("[data-status-scope]").forEach((button) => {
        button.classList.toggle("active", button.dataset.statusScope === selectedStatusScope);
      });
    }

    function getStatusOverviewGroups() {
      if (selectedStatusScope === "all") {
        return (settings.panels || []).map((panel) => ({ panel, vps: panel.vps || [] }));
      }

      const panel = settings.panels.find((item) => item.id === selectedPanelId);
      return panel ? [{ panel, vps: panel.vps || [] }] : [];
    }

    function statusOverviewRow(panel, vps) {
      const key = panel.id + ":" + vps.id;
      const row = (state.vps || {})[key];
      const serviceStatus = getServiceStatus(row, vps);
      const availability = calculateAvailability(key, row, vps);
      const history = buildStatusHistory(key, row, vps);
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
      const counts = { ok: 0, warn: 0, down: 0, unknown: 0, disabled: 0 };

      for (const { panel, vps } of services) {
        const key = panel.id + ":" + vps.id;
        const status = getServiceStatus((state.vps || {})[key], vps);
        counts[status.level] += 1;
      }

      const detail = services.length + " 台 VPS，在线 " + counts.ok + "，异常 " + counts.down + "，启动中 " + counts.warn + "，未检查 " + counts.unknown + "，未启用 " + counts.disabled;
      if (counts.down > 0) {
        return { level: "down", title: "存在 VPS 离线或检查失败", detail };
      }
      if (counts.warn > 0) {
        return { level: "warn", title: "存在 VPS 启动中或防重中", detail };
      }
      if (counts.unknown > 0) {
        return { level: "warn", title: "部分 VPS 尚未检查", detail };
      }
      if (counts.ok > 0) {
        return { level: "ok", title: selectedStatusScope === "all" ? "所有面板 VPS 运行正常" : "当前面板 VPS 运行正常", detail };
      }
      return { level: "warn", title: selectedStatusScope === "all" ? "所有面板 VPS 未启用" : "当前面板 VPS 未启用", detail };
    }

    function getServiceStatus(row, vps) {
      if (!vps.enabled) return { level: "disabled", label: "Disabled", badgeClass: "" };
      if (!row?.checkedAt) return { level: "unknown", label: "Unknown", badgeClass: "" };
      if (row.error) return { level: "down", label: "Error", badgeClass: "down" };
      if (row.online) return { level: "ok", label: "Online", badgeClass: "ok" };
      if (row.started) return { level: "warn", label: "Starting", badgeClass: "warn" };
      if (row.startSuppressed) return { level: "warn", label: "Cooldown", badgeClass: "warn" };
      return { level: "down", label: "Offline", badgeClass: "down" };
    }

    function calculateAvailability(key, row, vps) {
      if (!vps.enabled) return "未启用";
      const samples = getStatusSamples(key, row, vps).filter((sample) => sample.status !== "unknown");
      if (!samples.length) return "暂无数据";

      const ok = samples.filter((sample) => sample.status === "ok").length;
      return Math.round((ok / samples.length) * 100) + "%";
    }

    function buildStatusHistory(key, row, vps) {
      const samples = getStatusSamples(key, row, vps).reverse();
      const padded = Array(Math.max(0, 48 - samples.length)).fill(null).map(() => unknownStatusSample()).concat(samples).slice(-48);
      const firstKnown = padded.find((sample) => sample.at);
      const startLabel = firstKnown?.at ? relativeTime(firstKnown.at) : "暂无";

      return { segments: padded, startLabel };
    }

    function getStatusSamples(key, row, vps) {
      if (!vps.enabled) {
        return [statusSample("unknown", "未启用", null)];
      }

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

      const tz = settings.timezone || "local";
      const options = {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      };
      if (tz !== "local") {
        options.timeZone = tz;
      }

      try {
        const formatter = new Intl.DateTimeFormat("zh-CN", options);
        const parts = formatter.formatToParts(date);
        const partMap = {};
        for (const part of parts) {
          partMap[part.type] = part.value;
        }
        return \`\${partMap.year}-\${partMap.month}-\${partMap.day} \${partMap.hour}:\${partMap.minute}:\${partMap.second}\`;
      } catch (e) {
        console.error("Format timezone error", tz, e);
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
    }

    function renderState() {
      lastRunAt.textContent = state.lastRunAt ? "最后检查：" + formatDateTime(state.lastRunAt) : "未检查";
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
                <td>\${escapeHtml(row.checkedAt && row.checkedAt !== "-" ? formatDateTime(row.checkedAt) : "-")}</td>
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

    function resetEventPageAndRender() {
      eventPage = 1;
      renderEvents();
    }

    function getEventTableWidth() {
      return eventColumnWidths.reduce((sum, width) => sum + width, 0);
    }

    function renderEventColgroup() {
      return "<colgroup>" + EVENT_COLUMNS.map((column, index) => (
        '<col data-event-col="' + index + '" style="width: ' + eventColumnWidths[index] + 'px">'
      )).join("") + "</colgroup>";
    }

    function renderEventHeader() {
      return "<thead><tr>" + EVENT_COLUMNS.map((column, index) => (
        '<th data-col-index="' + index + '">' +
          escapeHtml(column.label) +
          '<span class="column-resizer" data-col-index="' + index + '" aria-hidden="true"></span>' +
        '</th>'
      )).join("") + "</tr></thead>";
    }

    function eventTextCell(value) {
      const text = String(value ?? "-");
      return '<td><span class="event-cell-text" data-overflow-text="' + escapeAttr(text) + '">' + escapeHtml(text) + '</span></td>';
    }

    function applyEventColumnWidths() {
      const table = eventTable.querySelector(".event-log-table");
      if (!table) return;

      table.style.minWidth = getEventTableWidth() + "px";
      table.querySelectorAll("col[data-event-col]").forEach((column) => {
        const index = Number(column.dataset.eventCol);
        column.style.width = eventColumnWidths[index] + "px";
      });
    }

    function startEventColumnResize(event) {
      const resizer = event.target.closest(".column-resizer");
      if (!resizer || !eventTable.contains(resizer)) return;
      if (event.pointerType === "mouse" && event.button !== 0) return;

      const index = Number(resizer.dataset.colIndex);
      if (!Number.isInteger(index)) return;

      hideOverflowTooltip();
      event.preventDefault();
      eventColumnResize = {
        index,
        startX: event.clientX,
        startWidth: eventColumnWidths[index],
        resizer
      };
      document.body.classList.add("column-resizing");
      resizer.setPointerCapture?.(event.pointerId);
      document.addEventListener("pointermove", resizeEventColumn);
      document.addEventListener("pointerup", stopEventColumnResize, { once: true });
      document.addEventListener("pointercancel", stopEventColumnResize, { once: true });
    }

    function resizeEventColumn(event) {
      if (!eventColumnResize) return;

      const column = EVENT_COLUMNS[eventColumnResize.index];
      const minWidth = column?.minWidth || 80;
      eventColumnWidths[eventColumnResize.index] = Math.max(minWidth, eventColumnResize.startWidth + event.clientX - eventColumnResize.startX);
      applyEventColumnWidths();
    }

    function stopEventColumnResize() {
      if (!eventColumnResize) return;

      document.body.classList.remove("column-resizing");
      document.removeEventListener("pointermove", resizeEventColumn);
      eventColumnResize = null;
    }

    function handleEventCellTooltip(event) {
      const target = event.target.closest("[data-overflow-text]");
      if (!target || !eventTable.contains(target)) return;
      if (!isOverflowed(target)) {
        hideOverflowTooltip();
        return;
      }

      overflowTooltipTarget = target;
      overflowTooltip.textContent = target.dataset.overflowText || target.textContent || "";
      overflowTooltip.classList.add("active");
      moveOverflowTooltip(event);
    }

    function moveOverflowTooltip(event) {
      if (!overflowTooltipTarget) return;

      const gap = 12;
      const width = overflowTooltip.offsetWidth;
      const height = overflowTooltip.offsetHeight;
      let left = event.clientX + gap;
      let top = event.clientY + gap;

      if (left + width > window.innerWidth - gap) {
        left = Math.max(gap, window.innerWidth - width - gap);
      }
      if (top + height > window.innerHeight - gap) {
        top = Math.max(gap, event.clientY - height - gap);
      }

      overflowTooltip.style.left = left + "px";
      overflowTooltip.style.top = top + "px";
    }

    function hideEventCellTooltip(event) {
      if (!overflowTooltipTarget || overflowTooltipTarget.contains(event.relatedTarget)) return;
      hideOverflowTooltip();
    }

    function hideOverflowTooltip() {
      overflowTooltipTarget = null;
      overflowTooltip.classList.remove("active");
    }

    function isOverflowed(element) {
      return element.scrollWidth > element.clientWidth || element.scrollHeight > element.clientHeight;
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
          summarizeStartResult(row.startResult),
          summarizeNotificationResults(row.notificationResults)
        ].some((value) => String(value || "").toLowerCase().includes(keyword));
      });

      if (!rows.length) {
        eventTable.innerHTML = '<div class="empty">暂无事件日志。</div>';
        eventPagination.hidden = true;
        return;
      }

      const pageSize = Number(eventPageSize.value || 10);
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      eventPage = Math.min(Math.max(1, eventPage), totalPages);

      const start = (eventPage - 1) * pageSize;
      const pageRows = rows.slice(start, start + pageSize);

      eventTable.innerHTML = \`
        <div class="event-table-scroll">
          <table class="table event-log-table" style="min-width: \${getEventTableWidth()}px;">
          \${renderEventColgroup()}
          \${renderEventHeader()}
          <tbody>
            \${pageRows.map((row) => \`
              <tr>
                \${eventTextCell(row.at && row.at !== "-" ? formatDateTime(row.at) : "-")}
                <td>\${levelBadge(row.level)}</td>
                <td>\${eventBadge(row)}</td>
                \${eventTextCell(row.panelName || row.panelId)}
                \${eventTextCell(row.name || row.vpsId)}
                <td>\${row.online ? '<span class="badge ok">Online</span>' : '<span class="badge down">Offline</span>'}</td>
                \${eventTextCell(row.failureCount || 0)}
                \${eventTextCell(summarizeStartResult(row.startResult))}
                \${eventTextCell(summarizeNotificationResults(row.notificationResults))}
                \${eventTextCell(row.error || "-")}
              </tr>
            \`).join("")}
          </tbody>
          </table>
        </div>
      \`;

      applyEventColumnWidths();
      renderEventPagination(totalPages);
    }

    function renderEventPagination(totalPages) {
      eventPagination.hidden = false;
      eventPageJump.max = String(totalPages);
      eventPageJump.value = String(eventPage);

      eventPageButtons.innerHTML = [
        '<button class="pagination-nav" data-event-page="prev" type="button" aria-label="上一页"' + (eventPage <= 1 ? " disabled" : "") + '>&#8249;</button>',
        ...getEventPageItems(totalPages).map((item) => {
          if (item === "ellipsis") {
            return '<span class="pagination-ellipsis">...</span>';
          }

          return '<button class="pagination-page' + (item === eventPage ? " active" : "") + '" data-event-page="' + item + '" type="button"' + (item === eventPage ? ' aria-current="page"' : "") + '>' + item + '</button>';
        }),
        '<button class="pagination-nav" data-event-page="next" type="button" aria-label="下一页"' + (eventPage >= totalPages ? " disabled" : "") + '>&#8250;</button>'
      ].join("");

      eventPageButtons.querySelectorAll("[data-event-page]").forEach((button) => {
        button.addEventListener("click", () => {
          const target = button.dataset.eventPage;
          if (target === "prev") {
            eventPage -= 1;
          } else if (target === "next") {
            eventPage += 1;
          } else {
            eventPage = Number(target);
          }
          renderEvents();
        });
      });
    }

    function getEventPageItems(totalPages) {
      if (totalPages <= 8) {
        return Array.from({ length: totalPages }, (_, index) => index + 1);
      }

      if (eventPage <= 4) {
        return [1, 2, 3, 4, 5, 6, "ellipsis", totalPages];
      }

      if (eventPage >= totalPages - 3) {
        return [1, "ellipsis", totalPages - 5, totalPages - 4, totalPages - 3, totalPages - 2, totalPages - 1, totalPages];
      }

      return [1, "ellipsis", eventPage - 2, eventPage - 1, eventPage, eventPage + 1, eventPage + 2, "ellipsis", totalPages];
    }

    function jumpEventPage() {
      const page = Number(eventPageJump.value);
      if (!Number.isFinite(page) || page < 1) return;

      eventPage = Math.trunc(page);
      renderEvents();
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

    function monitorStatusBadge(row) {
      if (row.error) return '<span class="badge down">Error</span>';
      if (row.online) return '<span class="badge ok">Online</span>';
      if (row.started) return '<span class="badge warn">Starting</span>';
      if (row.startSuppressed) return '<span class="badge warn">Cooldown</span>';
      return '<span class="badge down">Offline</span>';
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

    function summarizeNotificationResults(value) {
      if (!Array.isArray(value) || !value.length) return "-";
      const ok = value.filter((item) => item.ok).length;
      const failed = value.filter((item) => !item.ok);
      if (!failed.length) return "成功 " + ok + "/" + value.length;
      return "失败 " + failed.length + "/" + value.length + "：" + failed.map((item) => item.channelName + " " + item.error).join("；").slice(0, 160);
    }

    function setNotice(target, message, isError = false) {
      target.textContent = message;
      target.classList.toggle("error", isError);
    }

    function showMessage(message, type = "success") {
      if (messageTimer) {
        clearTimeout(messageTimer);
      }

      messageRoot.innerHTML = "";
      const item = document.createElement("div");
      item.className = "message " + type;
      item.innerHTML = "<span>" + escapeHtml(message) + "</span>";
      messageRoot.appendChild(item);

      messageTimer = setTimeout(() => {
        item.remove();
        messageTimer = null;
      }, 2200);
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
