import 'server-only'

import { AsyncLocalStorage } from 'node:async_hooks'
import { ProxyAgent, fetch as undiciFetch } from 'undici'
import { readJSONRecord, writeJSONRecord } from './json-record'

export type ProxyRouteVia = 'proxy' | 'free-proxy' | 'direct' | 'fallback'

export interface ProxyRouteInfo {
  via: ProxyRouteVia
  proxyMs: number
}

const routeStorage = new AsyncLocalStorage<{ info?: ProxyRouteInfo }>()
let lastRouteInfo: ProxyRouteInfo = { via: 'direct', proxyMs: 0 }

function recordRoute(via: ProxyRouteVia, proxyMs: number) {
  const info: ProxyRouteInfo = { via, proxyMs }
  lastRouteInfo = info
  const store = routeStorage.getStore()
  if (store) store.info = info
}

export function getLatestRouteInfo(): ProxyRouteInfo {
  const store = routeStorage.getStore()
  return store?.info || lastRouteInfo
}

export async function runWithRouteTracking<T>(fn: () => Promise<T>): Promise<{ result: T; info: ProxyRouteInfo }> {
  const store: { info?: ProxyRouteInfo } = {}
  const result = await routeStorage.run(store, fn)
  return {
    result,
    info: store.info || lastRouteInfo,
  }
}

export interface FreePoolConfig {
  enabled: boolean
  sourceUrl: string
}

export interface ProxyConfig {
  enabled: boolean
  urls: string[]
  freePool?: FreePoolConfig
}

export interface FreePoolStatus {
  enabled: boolean
  sourceUrl: string
  workingCount: number
  testedCount: number
  lastRefreshTime: number
  refreshing: boolean
}

const PROXY_RECORD = 'settings/proxy.json'
export const DEFAULT_FREE_PROXY_SOURCE =
  'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'

let cachedConfig: ProxyConfig = {
  enabled: false,
  urls: [],
  freePool: {
    enabled: false,
    sourceUrl: DEFAULT_FREE_PROXY_SOURCE,
  },
}
let configLoaded = false

// In-memory free proxy pool state
let freePoolWorking: string[] = []
let freePoolTestedCount = 0
let freePoolLastRefreshTime = 0
let freePoolRefreshing = false

// 10-minute quarantine for proxies that fail connection
const quarantineMap = new Map<string, number>()

function isQuarantined(uri: string): boolean {
  const until = quarantineMap.get(uri)
  if (!until) return false
  if (Date.now() > until) {
    quarantineMap.delete(uri)
    return false
  }
  return true
}

function quarantineProxy(uri: string) {
  quarantineMap.set(uri, Date.now() + 10 * 60 * 1000)
}

/**
 * Load proxy configuration from durable settings storage.
 * Defaults to enabled: false and freePool.enabled: false.
 */
export async function getProxyConfig(): Promise<ProxyConfig> {
  try {
    const data = await readJSONRecord<ProxyConfig>(PROXY_RECORD)
    if (data && typeof data === 'object') {
      cachedConfig = {
        enabled: Boolean(data.enabled),
        urls: Array.isArray(data.urls)
          ? data.urls
              .map((u) => String(u).trim())
              .filter((u) => u.startsWith('http://') || u.startsWith('https://'))
          : [],
        freePool: {
          enabled: Boolean(data.freePool?.enabled),
          sourceUrl:
            typeof data.freePool?.sourceUrl === 'string' && data.freePool.sourceUrl.trim()
              ? data.freePool.sourceUrl.trim()
              : DEFAULT_FREE_PROXY_SOURCE,
        },
      }
    } else {
      cachedConfig = {
        enabled: false,
        urls: [],
        freePool: { enabled: false, sourceUrl: DEFAULT_FREE_PROXY_SOURCE },
      }
    }
  } catch {
    cachedConfig = {
      enabled: false,
      urls: [],
      freePool: { enabled: false, sourceUrl: DEFAULT_FREE_PROXY_SOURCE },
    }
  }
  configLoaded = true
  return cachedConfig
}

/**
 * Save proxy configuration.
 */
export async function setProxyConfig(config: {
  enabled?: boolean
  urls?: string[]
  freePool?: { enabled?: boolean; sourceUrl?: string }
}): Promise<ProxyConfig> {
  const current = await getProxyConfig()
  const nextConfig: ProxyConfig = {
    enabled: typeof config.enabled === 'boolean' ? config.enabled : current.enabled,
    urls: Array.isArray(config.urls)
      ? config.urls
          .map((u) => String(u).trim())
          .filter((u) => u.startsWith('http://') || u.startsWith('https://'))
      : current.urls,
    freePool: {
      enabled:
        config.freePool && typeof config.freePool.enabled === 'boolean'
          ? config.freePool.enabled
          : Boolean(current.freePool?.enabled),
      sourceUrl:
        config.freePool && typeof config.freePool.sourceUrl === 'string' && config.freePool.sourceUrl.trim()
          ? config.freePool.sourceUrl.trim()
          : current.freePool?.sourceUrl || DEFAULT_FREE_PROXY_SOURCE,
    },
  }
  await writeJSONRecord(PROXY_RECORD, nextConfig)
  cachedConfig = nextConfig
  configLoaded = true

  // If free pool was enabled and pool is empty, kick off a refresh in background
  if (nextConfig.enabled && nextConfig.freePool?.enabled && freePoolWorking.length === 0) {
    void refreshFreeProxyPool(true)
  }

  return cachedConfig
}

/**
 * Get current free pool status for UI display.
 */
export function getFreePoolStatus(): FreePoolStatus {
  const unquarantinedCount = freePoolWorking.filter((u) => !isQuarantined(u)).length
  return {
    enabled: Boolean(cachedConfig.freePool?.enabled),
    sourceUrl: cachedConfig.freePool?.sourceUrl || DEFAULT_FREE_PROXY_SOURCE,
    workingCount: unquarantinedCount,
    testedCount: freePoolTestedCount,
    lastRefreshTime: freePoolLastRefreshTime,
    refreshing: freePoolRefreshing,
  }
}

/**
 * Refreshes the free proxy pool from the configured raw text list.
 * Tests up to 40 candidates (max 10 parallel) against https://generativelanguage.googleapis.com/ (5s timeout).
 * Keeps up to 8 working proxies.
 * Strictly maintains standard TLS certificate verification.
 */
export async function refreshFreeProxyPool(force = false): Promise<FreePoolStatus> {
  if (freePoolRefreshing) return getFreePoolStatus()
  const cfg = await getProxyConfig()
  if (!cfg.enabled || !cfg.freePool?.enabled) {
    return getFreePoolStatus()
  }

  const now = Date.now()
  if (!force && freePoolWorking.length > 0 && now - freePoolLastRefreshTime < 15 * 60 * 1000) {
    return getFreePoolStatus()
  }

  freePoolRefreshing = true
  try {
    const sourceUrl = cfg.freePool.sourceUrl || DEFAULT_FREE_PROXY_SOURCE
    const res = await fetch(sourceUrl, { signal: AbortSignal.timeout(8000) })
    if (!res.ok) {
      throw new Error(`Failed to fetch proxy list: HTTP ${res.status}`)
    }
    const text = await res.text()
    const rawLines = text
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith('#') && /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$/.test(l))

    // Shuffle and test up to 40 candidates (parallel max 10, max pool size 8)
    const candidates = rawLines.sort(() => 0.5 - Math.random()).slice(0, 40)
    let tested = 0
    const working: string[] = []

    const BATCH_SIZE = 10
    for (let i = 0; i < candidates.length && working.length < 8; i += BATCH_SIZE) {
      const batch = candidates.slice(i, i + BATCH_SIZE)
      tested += batch.length
      const batchResults = await Promise.all(
        batch.map(async (ipPort) => {
          const uri = `http://${ipPort}`
          if (isQuarantined(uri)) return null
          try {
            const agent = new ProxyAgent({
              uri,
              connect: { timeout: 5000 },
              headersTimeout: 10_000,
              bodyTimeout: 10_000,
            })
            // Verify real HTTPS handshake to Gemini endpoint with strict TLS verification
            const testRes = await undiciFetch('https://generativelanguage.googleapis.com/', {
              dispatcher: agent,
              signal: AbortSignal.timeout(5000),
            })
            if (testRes.status > 0) {
              return uri
            }
            return null
          } catch {
            return null
          }
        }),
      )

      for (const uri of batchResults) {
        if (uri && !working.includes(uri)) {
          working.push(uri)
          if (working.length >= 8) break
        }
      }
    }

    if (working.length > 0) {
      freePoolWorking = working
    } else {
      freePoolWorking = freePoolWorking.filter((u) => !isQuarantined(u))
    }
    freePoolTestedCount = tested
    freePoolLastRefreshTime = Date.now()
  } catch (err: unknown) {
    console.warn('[Gemini Proxy] Free pool refresh failed:', (err as { code?: string })?.code || 'FETCH_FAIL')
  } finally {
    freePoolRefreshing = false
  }

  return getFreePoolStatus()
}

const agentCache = new Map<string, ProxyAgent>()

function getOrCreateProxyAgent(proxyUri: string): ProxyAgent {
  let agent = agentCache.get(proxyUri)
  if (!agent) {
    agent = new ProxyAgent({
      uri: proxyUri,
      connect: {
        timeout: 5000, // 5 second connect timeout
      },
      headersTimeout: 15 * 60 * 1000, // 15-minute timeout matching Gemini requests
      bodyTimeout: 15 * 60 * 1000,    // 15-minute timeout matching Gemini requests
    })
    agentCache.set(proxyUri, agent)
  }
  return agent
}

let manualRoundRobinIndex = 0
let freeRoundRobinIndex = 0

/**
 * Resolves the next proxy for a request:
 * 1. User manual proxies have first priority.
 * 2. Free proxy pool is used second (EXCLUDED for video/file upload requests).
 * 3. Quarantined proxies are skipped.
 */
function getNextProxyForRequest(isUpload: boolean): { proxyUri: string | null; isFreePool: boolean } {
  // Priority 1: User's manual proxies
  if (cachedConfig.urls.length > 0) {
    const activeManual = cachedConfig.urls.filter((u) => !isQuarantined(u))
    if (activeManual.length > 0) {
      const selected = activeManual[manualRoundRobinIndex % activeManual.length]
      manualRoundRobinIndex = (manualRoundRobinIndex + 1) % activeManual.length
      return { proxyUri: selected, isFreePool: false }
    }
  }

  // Priority 2: Free proxy pool (only for small API requests like generateContent/chat; NEVER for uploads)
  if (cachedConfig.freePool?.enabled && !isUpload) {
    const now = Date.now()
    const activeFree = freePoolWorking.filter((u) => !isQuarantined(u))
    // If pool is empty, all quarantined, or stale (>15 min), trigger background refresh
    if (activeFree.length === 0 || now - freePoolLastRefreshTime > 15 * 60 * 1000) {
      void refreshFreeProxyPool(false)
    }

    if (activeFree.length > 0) {
      const selected = activeFree[freeRoundRobinIndex % activeFree.length]
      freeRoundRobinIndex = (freeRoundRobinIndex + 1) % activeFree.length
      return { proxyUri: selected, isFreePool: true }
    }
  }

  return { proxyUri: null, isFreePool: false }
}

const CONNECT_FAILURE_CODES = new Set([
  'UND_ERR_CONNECT_TIMEOUT',
  'ECONNREFUSED',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ECONNRESET',
  'UND_ERR_SOCKET',
])

function isProxyConnectFailure(err: unknown, init?: RequestInit): { isFailure: boolean; code: string } {
  if (init?.signal?.aborted || (err as { name?: string })?.name === 'AbortError') {
    return { isFailure: false, code: 'ABORT' }
  }

  if (!err || typeof err !== 'object') {
    return { isFailure: false, code: 'UNKNOWN' }
  }

  const errObj = err as {
    code?: string
    name?: string
    statusCode?: number
    status?: number
    cause?: {
      code?: string
      name?: string
      statusCode?: number
      status?: number
      message?: string
    }
  }

  const topCode = errObj.code || ''
  const causeCode = errObj.cause?.code || ''
  const causeStatus = errObj.cause?.statusCode ?? errObj.cause?.status ?? errObj.statusCode ?? errObj.status

  if (
    topCode === 'UND_ERR_HEADERS_TIMEOUT' ||
    topCode === 'UND_ERR_BODY_TIMEOUT' ||
    causeCode === 'UND_ERR_HEADERS_TIMEOUT' ||
    causeCode === 'UND_ERR_BODY_TIMEOUT'
  ) {
    return { isFailure: false, code: topCode || causeCode }
  }

  if (causeCode && CONNECT_FAILURE_CODES.has(causeCode)) {
    return { isFailure: true, code: causeCode }
  }
  if (topCode && CONNECT_FAILURE_CODES.has(topCode)) {
    return { isFailure: true, code: topCode }
  }

  if (causeStatus === 407) {
    return { isFailure: true, code: 'PROXY_AUTH_407' }
  }

  const msg = String(errObj.cause?.message || errObj).toLowerCase()
  if (
    msg.includes('tunneling socket could not be established') ||
    msg.includes('proxy authentication') ||
    msg.includes('connect timeout') ||
    msg.includes('econnrefused') ||
    msg.includes('enotfound')
  ) {
    return { isFailure: true, code: causeCode || topCode || 'TUNNEL_FAILURE' }
  }

  return { isFailure: false, code: causeCode || topCode || 'OTHER' }
}

/**
 * Custom fetch wrapper exclusively for Gemini requests.
 * If proxy is OFF -> routes direct via standard fetch.
 * If proxy is ON -> routes through manual proxy or free proxy pool via undiciFetch.
 * Video and file uploads (/upload/) are excluded from free proxy pool and sent direct unless manual proxy exists.
 * If proxy fails to connect -> 10-minute quarantine applied, falls back to direct connection.
 * Timeouts after connection or aborts are re-thrown and NOT retried via direct connection.
 * Google HTTP errors (429/503) are preserved as valid responses without fallback.
 * Strictly avoids logging proxy URLs or passwords.
 */
export async function geminiFetch(input: string | URL | Request, init?: RequestInit): Promise<Response> {
  if (!configLoaded) {
    try {
      await getProxyConfig()
    } catch {
      // ignore
    }
  }

  const targetUrl =
    typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.toString()
        : 'url' in input && typeof input.url === 'string'
          ? input.url
          : String(input)

  // Identify video / file upload requests (never routed through free proxy pool)
  const hasUploadHeader =
    init?.headers &&
    (typeof init.headers === 'object'
      ? Object.keys(init.headers).some((k) => k.toLowerCase().includes('upload'))
      : false)
  const isUpload =
    targetUrl.toLowerCase().includes('/upload/') ||
    targetUrl.toLowerCase().includes('resumable') ||
    Boolean(hasUploadHeader)

  if (!cachedConfig.enabled) {
    const t0 = Date.now()
    const res = await fetch(input, init)
    recordRoute('direct', Date.now() - t0)
    return res
  }

  const { proxyUri, isFreePool } = getNextProxyForRequest(isUpload)

  if (!proxyUri) {
    const t0 = Date.now()
    const res = await fetch(input, init)
    recordRoute('direct', Date.now() - t0)
    return res
  }

  const t0 = Date.now()
  try {
    const agent = getOrCreateProxyAgent(proxyUri)

    const res = (await undiciFetch(targetUrl, {
      ...init,
      dispatcher: agent,
    })) as unknown as Response

    recordRoute(isFreePool ? 'free-proxy' : 'proxy', Date.now() - t0)
    return res
  } catch (err: unknown) {
    if (init?.signal?.aborted || (err as { name?: string })?.name === 'AbortError') {
      throw err
    }

    const { isFailure, code } = isProxyConnectFailure(err, init)
    if (isFailure) {
      quarantineProxy(proxyUri)
      console.warn(`[Gemini Proxy] Connect failed (${code}) — fallback to direct connection`)
      const fallbackT0 = Date.now()
      const res = await fetch(input, init)
      recordRoute('fallback', Date.now() - fallbackT0)
      return res
    }

    throw err
  }
}

/**
 * Test connectivity for a single proxy URL using undiciFetch.
 * Returns outgoing IP and measured round-trip latency.
 * Never logs or echoes back passwords.
 */
export async function testProxyConnection(
  rawUri: string,
): Promise<{ ok: boolean; ip?: string; latencyMs: number; error?: string }> {
  const uri = (rawUri || '').trim()
  if (!uri.startsWith('http://') && !uri.startsWith('https://')) {
    return { ok: false, latencyMs: 0, error: 'Proxy URL must start with http:// or https://' }
  }

  const start = Date.now()
  try {
    const testAgent = new ProxyAgent({
      uri,
      connect: { timeout: 5000 },
      headersTimeout: 10_000,
      bodyTimeout: 10_000,
    })

    const res = (await undiciFetch('https://api.ipify.org?format=json', {
      dispatcher: testAgent,
      signal: AbortSignal.timeout(6000),
    })) as unknown as Response

    if (!res.ok) {
      return {
        ok: false,
        latencyMs: Date.now() - start,
        error: `Proxy returned HTTP status ${res.status}`,
      }
    }

    const data = (await res.json().catch(() => ({}))) as { ip?: string }
    return {
      ok: true,
      ip: data.ip || 'Unknown IP',
      latencyMs: Date.now() - start,
    }
  } catch (err: unknown) {
    const errCode =
      (err as { cause?: { code?: string }; code?: string })?.cause?.code ||
      (err as { code?: string })?.code ||
      'CONNECT_TIMEOUT'
    return {
      ok: false,
      latencyMs: Date.now() - start,
      error: `Connection failed or timed out (${errCode})`,
    }
  }
}
