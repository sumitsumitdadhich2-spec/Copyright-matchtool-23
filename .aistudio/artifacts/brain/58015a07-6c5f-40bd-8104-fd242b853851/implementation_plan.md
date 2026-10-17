# Gemini API Rotating Proxy System (Anti-503 High Demand Architecture)

Transparent outgoing proxy routing and auto-rotating IP pool for all Gemini API requests and video chunk uploads to prevent Google 503 "High Demand / Model Overloaded" errors, preserving all existing scan workflows and UI features.

## User Review & Critical Decisions

> [!IMPORTANT]
> The existing application features, scan pipelines, minute finder, and chunk orchestrator remain 100% intact. The proxy system operates at the underlying Node.js network dispatcher layer without altering any scan business logic.

- **Confirmed Decision (User Choice)**: No changes to current app functions. Add proxy support so that whenever requests are sent to Gemini API, they route through proxies.
- **Confirmed Decision (User Choice)**: Provide a Proxy Settings panel in the Settings dialog allowing custom proxy entry, alongside an automatic rotating proxy pool with seamless direct fallback so scans never freeze if a proxy drops.
- **Proxy Types Supported**: HTTP, HTTPS, SOCKS5, and Cloudflare Worker reverse-proxy URLs.
- **Safety Mechanism**: Automatic health-checking and immediate fallback to alternative proxies or direct connection on proxy timeout/refusal.

---

## 1. Overview & Core Concept

- **What It Does**: Whenever Shiva MatchTool calls Gemini API (both generateContent requests and resumable video file uploads), the request is transparently dispatched through a rotating pool of proxy IPs from different regions/countries instead of all emerging from a single server IP.
- **Target Audience**: Video forensics and copyright match operators running parallel chunk scans across 10-20 API keys created from different Google accounts.
- **Key Value**: Prevents Google Anycast clusters from registering sudden burst traffic from a single origin IP, drastically reducing HTTP 503 High Demand / Model Overload errors and distributing requests across regional endpoints.

---

## 2. User Experience & Visual Design

- **Settings Dialog Integration**:
  - A new "Proxy & Network Routing" tab/section added to the existing Settings Dialog without cluttering or altering the API Keys tab.
  - **Proxy Toggle**: Master switch (`Enable Proxy Routing: ON / OFF`).
  - **Rotation Strategy**: Choice of `Key-Affinity` (Key 2 -> Proxy A, Key 3 -> Proxy B, consistent IP per Google account) or `Round-Robin` (each new request picks the next active proxy).
  - **Proxy Pool List**: Textarea/input to view and paste custom proxies (`http://user:pass@host:port` or `socks5://host:port` or Cloudflare Worker URLs).
  - **Auto-Rotation / Built-in Pool Toggle**: One-click enable for curated reliable proxy endpoints with auto-testing.
  - **Live Connection Tester**: "Test Proxy" button with latency badge (e.g., `240ms [OK] (US-East)`) and IP display.
- **Scan Live Activity / Matrix Integration**:
  - Small, non-intrusive proxy indicator icon in the Live Activity bar or Orchestrator Matrix indicating `Proxy Active: 8 IPs rotating` with green status dot.
- **Color Tokens & Styling**:
  - Matches existing dark cinematic forensic theme (`--background: oklch(0.16 0.008 260)`, `--primary: oklch(0.76 0.16 65)` amber glow, and signal green `--success`).

---

## 3. Key Product Decisions & Trade-Offs

- **Decision 1: Network Layer Dispatcher vs SDK Rewriting**
  - *Chosen Approach*: Implement a centralized Node.js network dispatcher using `undici.ProxyAgent` and custom `fetch` agent in `lib/proxy-agent.ts`.
  - *Why*: `@google/genai` and Node's global `fetch` automatically use the global or request-level dispatcher. This guarantees that **100% of outgoing Gemini API calls and video chunk uploads** pass through the proxy without having to modify individual route files or scan pipelines.
  - *Alternatives Considered*: Modifying every individual API call site was rejected because it introduces regression risks across the 30+ scan endpoints.

- **Decision 2: Key-to-Proxy Affinity vs Pure Random Rotation**
  - *Chosen Approach*: Support both Key-Affinity (recommended for Google accounts) and Round-Robin.
  - *Why*: Google accounts maintain sessions and rate-limit tracking. When Key #3 always talks through Proxy #3 (e.g. Frankfurt IP) and Key #4 talks through Proxy #4 (e.g. Ashburn IP), Google views each key as a separate client in a separate region, preventing rapid IP flapping for the same API key.

- **Decision 3: Transparent Auto-Fallback to Direct Connection**
  - *Chosen Approach*: If a selected proxy fails to connect or times out within 5 seconds, the request is immediately retried via the next proxy or directly through the standard network.
  - *Why*: Public and third-party proxies can occasionally be flaky. A scan must NEVER abort or lose progress because of a dead proxy.

---

## 4. Technical Architecture & Data Strategy

### System Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Shiva MatchTool Server                          │
│                                                                        │
│   ┌──────────────────────┐               ┌─────────────────────────┐  │
│   │ Chunk Orchestrator   │               │ Gemini Minute Finder    │  │
│   │ (Slots 2-16 Workers) │               │ & Candidate Verifiers   │  │
│   └──────────┬───────────┘               └────────────┬────────────┘  │
│              │                                        │                │
│              ▼                                        ▼                │
│   ┌────────────────────────────────────────────────────────────────┐  │
│   │           lib/gemini.ts  &  lib/upload-client.ts               │  │
│   │     (generateContent, resumable video chunk uploads)           │  │
│   └──────────────────────────────┬─────────────────────────────────┘  │
│                                  │                                     │
│                                  ▼                                     │
│   ┌────────────────────────────────────────────────────────────────┐  │
│   │                  lib/proxy-agent.ts                            │  │
│   │  - Proxy Pool Manager (HTTP/HTTPS/SOCKS5 / CF Worker)          │  │
│   │  - Key-Slot Affinity & Round-Robin Load Balancer               │  │
│   │  - Fast Health Check & Circuit Breaker (5s timeout failover)   │  │
│   │  - Undici ProxyAgent Dispatcher Provider                       │  │
│   └───────┬──────────────────────┬──────────────────────┬──────────┘  │
└───────────┼──────────────────────┼──────────────────────┼─────────────┘
            │                      │                      │
            ▼                      ▼                      ▼
     ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
     │  Proxy IP 1 │        │  Proxy IP 2 │        │  Proxy IP 3 │
     │  (Region A) │        │  (Region B) │        │  (Region C) │
     └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
            │                      │                      │
            └──────────────────────┼──────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  Google Gemini API Edge     │
                    │  generativelanguage.        │
                    │  googleapis.com             │
                    └─────────────────────────────┘
```

### Components & File Modifications Plan

1. **`lib/proxy-agent.ts` (New Core Service)**:
   - Manages proxy list, active index, key-to-proxy mapping.
   - Creates and caches `undici.ProxyAgent` instances with custom timeouts.
   - Helper function `getProxyDispatcher(keyIndex?: number)` that returns the appropriate `ProxyAgent` or standard `Agent`.
   - Proxy health testing endpoint helper (`testProxy(url)`).
   - In-memory failover counters (temporarily quarantine dead proxies).

2. **`lib/gemini.ts` Integration**:
   - Update `getClient(apiKey: string, keySlot?: number)` and `uploadResumableWithProgress` to bind with `getProxyDispatcher(keySlot)`.
   - Ensure resumable uploads pass the custom dispatcher into the `fetch()` call options so multi-megabyte video uploads travel via the designated proxy.

3. **`lib/user-keys.ts` & `app/api/settings/route.ts`**:
   - Add storage for `proxyConfig`: `{ enabled: boolean, mode: 'affinity' | 'round-robin', customProxies: string[], usePublicPool: boolean }`.
   - New API action `POST /api/settings` with `{ testProxy: string }` to verify proxy connectivity and display outgoing IP and response time.

4. **`components/cmt/settings-dialog.tsx` & New `proxy-settings-panel.tsx`**:
   - Elegant tab or collapsible section in Settings: "Proxy & Geo-Routing (Anti-503)".
   - Status indicators, proxy tester, and copy-paste proxy list.

5. **Verification & Testing**:
   - Test proxy dispatch and fallback behavior.
   - Compile applet with `compile_applet` to verify clean build.
