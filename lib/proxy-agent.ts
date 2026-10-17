import 'server-only'

import { ProxyAgent, fetch as undiciFetch } from 'undici'
import { readJSONRecord, writeJSONRecord } from './json-record'

export interface ProxyConfig {
  enabled: boolean
  urls: string[]
}

const PROXY_RECORD = 'settings/proxy.json'

let cachedConfig: ProxyConfig = { enabled: false, urls: [] }
let configLoaded = false

/**
 * Load proxy configuration from durable settings storage (settings/proxy.json).
 * Defaults to enabled: false.
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
      }
    } else {
      cachedConfig = { enabled: false, urls: [] }
    }
  } catch {
    cachedConfig = { enabled: false, urls: [] }
  }
  configLoaded = true
  return cachedConfig
}

/**
 * Save proxy configuration.
 */
export async function setProxyConfig(config: { enabled?: boolean; urls?: string[] }): Promise<ProxyConfig> {
  const nextConfig: ProxyConfig = {
    enabled: typeof config.enabled === 'boolean' ? config.enabled : false,
    urls: Array.isArray(config.urls)
      ? config.urls
          .map((u) => String(u).trim())
          .filter((u) => u.startsWith('http://') || u.startsWith('https://'))
      : [],
  }
  await writeJSONRecord(PROXY_RECORD, nextConfig)
  cachedConfig = nextConfig
  configLoaded = true
  return cachedConfig
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

let roundRobinIndex = 0

function getNextProxyUrl(): string | null {
  if (!cachedConfig.enabled || cachedConfig.urls.length === 0) return null
  const urls = cachedConfig.urls
  const selected = urls[roundRobinIndex % urls.length]
  roundRobinIndex = (roundRobinIndex + 1) % urls.length
  return selected
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
 * If proxy is ON -> routes through round-robin HTTP/HTTPS proxy via undiciFetch.
 * If proxy fails to connect (5s connect timeout / ECONNREFUSED) -> immediately falls back to direct connection.
 * Timeouts after connection (headers/body) or aborts are re-thrown and NOT retried via direct connection.
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

  if (!cachedConfig.enabled || cachedConfig.urls.length === 0) {
    return fetch(input, init)
  }

  const proxyUri = getNextProxyUrl()
  if (!proxyUri) {
    return fetch(input, init)
  }

  try {
    const agent = getOrCreateProxyAgent(proxyUri)
    const targetUrl =
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.toString()
          : 'url' in input && typeof input.url === 'string'
            ? input.url
            : String(input)

    const res = (await undiciFetch(targetUrl, {
      ...init,
      dispatcher: agent,
    })) as unknown as Response

    return res
  } catch (err: unknown) {
    if (init?.signal?.aborted || (err as { name?: string })?.name === 'AbortError') {
      throw err
    }

    const { isFailure, code } = isProxyConnectFailure(err, init)
    if (isFailure) {
      console.warn(`[Gemini Proxy] Connect failed (${code}) — fallback to direct connection`)
      return fetch(input, init)
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
