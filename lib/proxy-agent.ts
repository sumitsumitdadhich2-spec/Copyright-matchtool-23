import 'server-only'

import { ProxyAgent } from 'undici'
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

/**
 * Custom fetch wrapper exclusively for Gemini requests.
 * If proxy is OFF -> routes direct via standard fetch.
 * If proxy is ON -> routes through round-robin HTTP/HTTPS proxy.
 * If proxy fails or times out (5s connect timeout) -> immediately falls back to direct connection.
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
    return await fetch(input, {
      ...init,
      // @ts-expect-error Node.js fetch accepts undici dispatcher
      dispatcher: agent,
    })
  } catch {
    // Proxy connect failure or timeout -> immediately fall back to direct fetch.
    // Scan never pauses or fails. Credentials are never logged.
    console.warn('[Gemini Proxy] Proxy connection failed or timed out — fallback to direct connection')
    return fetch(input, init)
  }
}

/**
 * Test connectivity for a single proxy URL.
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

    const res = await fetch('https://api.ipify.org?format=json', {
      // @ts-expect-error Node.js fetch accepts undici dispatcher
      dispatcher: testAgent,
      signal: AbortSignal.timeout(6000),
    })

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
  } catch {
    return {
      ok: false,
      latencyMs: Date.now() - start,
      error: 'Connection failed or timed out (5s)',
    }
  }
}
