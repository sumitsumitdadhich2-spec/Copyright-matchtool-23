import 'server-only'

import { GoogleGenAI } from '@google/genai'
import type {
  OrchestratorLiveState,
  OrchestratorKeySlotState,
  OrchestratorModelHealth,
} from './types'
import {
  ORCHESTRATOR_PRIMARY_MODEL,
  ORCHESTRATOR_SECONDARY_MODEL,
  ORCHESTRATOR_TERTIARY_MODEL,
  ORCHESTRATOR_CANARY_MODEL,
  ORCHESTRATOR_WORKER_KEY_SLOTS,
  ORCHESTRATOR_RESERVED_KEY_SLOTS,
  ORCHESTRATOR_SKIPPED_KEY_SLOTS,
  ORCHESTRATOR_CHUNK_TIMEOUT_MS,
  ORCHESTRATOR_CHOKE_TIMEOUT_MS,
  ORCHESTRATOR_BURST_MAX,
} from './models'
import { getApiKeyN, incrementModelUsage, forceModelExhausted } from './store'

export interface ChunkQueueItem {
  index: number
  segIndex: number
  status: 'pending' | 'assigned' | 'processing' | 'done' | 'failed' | 'parked'
  assignedKey?: number
  assignedModel?: string
  attempts: number
  lastError?: string
  startedAt?: number
}

export interface ChokeResult {
  ok: boolean
  latencyMs: number
  is503: boolean
  is429: boolean
  isTimeout: boolean
  reply?: string
  error?: string
}

export type ChunkExecutor = (
  chunkIndex: number,
  keySlot: number,
  apiKey: string,
  model: string,
  timeoutMs: number,
  signal: AbortSignal,
  segIndex: number,
) => Promise<{ ok: boolean; is503?: boolean; is429?: boolean; isTimeout?: boolean; error?: string }>

export interface OrchestratorLogEvent {
  timestamp: number
  type: 'choke' | 'chunk' | 'failover' | 'recovery' | 'info' | 'warn' | 'error'
  model?: string
  keySlot?: number
  chunkIndex?: number
  outcome?: 'success' | '503' | '429' | 'timeout' | 'parked'
  latencyMs?: number
  message: string
}

export class ChunkScanOrchestrator {
  private activeScanId: string | null = null
  private stopping = false
  private pauseRequested = false
  private chunkQueue: ChunkQueueItem[] = []
  private keyApiMap = new Map<number, string>()
  private chokeRotationIdx = 0
  private burstSlotRotationIdx = 0
  private lastBatchTime = 0
  private lastAllModelsChokeTime = Date.now()
  private logs: OrchestratorLogEvent[] = []
  private chunkExecutor: ChunkExecutor | null = null
  private droppedModelsToday = new Set<string>()
  /** Per-(key, model) cooldown timestamp (1 min TPM cooldown from request dispatch or 503) */
  private keyModelCooldown = new Map<string, number>()
  /** Per-model cooldown timestamp (1 min cooldown when burst encounters 503 high demand) */
  private modelCooldownUntil = new Map<string, number>()

  private state: OrchestratorLiveState = this.createInitialState()

  constructor() {
    this.initSlots()
  }

  private createInitialState(): OrchestratorLiveState {
    const slots: Record<number, OrchestratorKeySlotState> = {}
    for (let slot = 1; slot <= 16; slot++) {
      if ((ORCHESTRATOR_RESERVED_KEY_SLOTS as readonly number[]).includes(slot)) {
        slots[slot] = {
          slot,
          status: 'reserved',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      } else if ((ORCHESTRATOR_SKIPPED_KEY_SLOTS as readonly number[]).includes(slot)) {
        slots[slot] = {
          slot,
          status: 'skipped',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      } else {
        slots[slot] = {
          slot,
          status: 'idle',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      }
    }

    const models: Record<string, OrchestratorModelHealth> = {
      [ORCHESTRATOR_PRIMARY_MODEL]: {
        modelId: ORCHESTRATOR_PRIMARY_MODEL,
        role: 'primary',
        status: this.droppedModelsToday?.has(ORCHESTRATOR_PRIMARY_MODEL) ? 'exhausted' : 'healthy',
        effectiveParallelism: this.droppedModelsToday?.has(ORCHESTRATOR_PRIMARY_MODEL) ? 0 : (ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_PRIMARY_MODEL] || 10),
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_SECONDARY_MODEL]: {
        modelId: ORCHESTRATOR_SECONDARY_MODEL,
        role: 'secondary',
        status: this.droppedModelsToday?.has(ORCHESTRATOR_SECONDARY_MODEL) ? 'exhausted' : 'healthy',
        effectiveParallelism: this.droppedModelsToday?.has(ORCHESTRATOR_SECONDARY_MODEL) ? 0 : (ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_SECONDARY_MODEL] || 4),
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_TERTIARY_MODEL]: {
        modelId: ORCHESTRATOR_TERTIARY_MODEL,
        role: 'tertiary',
        status: this.droppedModelsToday?.has(ORCHESTRATOR_TERTIARY_MODEL) ? 'exhausted' : 'healthy',
        effectiveParallelism: this.droppedModelsToday?.has(ORCHESTRATOR_TERTIARY_MODEL) ? 0 : (ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_TERTIARY_MODEL] || 4),
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_CANARY_MODEL]: {
        modelId: ORCHESTRATOR_CANARY_MODEL,
        role: 'canary',
        status: 'healthy',
        effectiveParallelism: 1,
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
    }

    const healthyModels = [ORCHESTRATOR_PRIMARY_MODEL, ORCHESTRATOR_SECONDARY_MODEL, ORCHESTRATOR_TERTIARY_MODEL].filter(
      (m) => !this.droppedModelsToday?.has(m),
    )

    return {
      phase: 'idle',
      activeModel: healthyModels[0] || ORCHESTRATOR_PRIMARY_MODEL,
      healthyModels,
      canaryActive: false,
      slots,
      models,
      queue: {
        pending: 0,
        assigned: 0,
        processing: 0,
        done: 0,
        failed: 0,
        retrying: 0,
      },
      velocityPerHour: 0,
      estimatedTimeRemainingSec: null,
      lastMessage: 'Orchestrator initialized and ready.',
      lastUpdated: Date.now(),
    }
  }

  private initSlots() {
    for (const slot of ORCHESTRATOR_WORKER_KEY_SLOTS) {
      const k = getApiKeyN(slot)
      if (k && k.trim()) {
        this.keyApiMap.set(slot, k.trim())
      }
    }
  }

  public registerKey(slot: number, apiKey: string) {
    if ((ORCHESTRATOR_WORKER_KEY_SLOTS as readonly number[]).includes(slot)) {
      this.keyApiMap.set(slot, apiKey.trim())
    }
  }

  public setChunkExecutor(fn: ChunkExecutor) {
    this.chunkExecutor = fn
  }

  public getState(): OrchestratorLiveState {
    this.refreshQueueCounts()
    return JSON.parse(JSON.stringify(this.state))
  }

  private onLogListeners: ((event: OrchestratorLogEvent) => void)[] = []

  public onLog(listener: (event: OrchestratorLogEvent) => void): () => void {
    this.onLogListeners.push(listener)
    return () => {
      this.onLogListeners = this.onLogListeners.filter((l) => l !== listener)
    }
  }

  public getLogs(): OrchestratorLogEvent[] {
    return this.logs.slice(-200)
  }

  public log(event: OrchestratorLogEvent) {
    this.logs.push(event)
    if (this.logs.length > 500) {
      this.logs.shift()
    }
    this.state.lastMessage = `[${event.type.toUpperCase()}] ${event.message}`
    this.state.lastUpdated = Date.now()

    for (const listener of this.onLogListeners) {
      try {
        listener(event)
      } catch {
        /* ignore callback error */
      }
    }
  }

  private refreshQueueCounts() {
    let pending = 0
    let assigned = 0
    let processing = 0
    let done = 0
    let failed = 0
    let retrying = 0

    for (const item of this.chunkQueue) {
      if (item.status === 'pending') {
        pending++
        if (item.attempts > 0) retrying++
      } else if (item.status === 'assigned') assigned++
      else if (item.status === 'processing') processing++
      else if (item.status === 'done') done++
      else if (item.status === 'failed' || item.status === 'parked') failed++
    }

    this.state.queue = { pending, assigned, processing, done, failed, retrying }
  }

  /**
   * Load chunks into queue and prepare orchestrator.
   * Supports chunk index numbers or item objects with prior attempts and error details.
   */
  public prepareChunks(
    scanId: string,
    chunks: number[] | { index: number; segIndex?: number; attempts?: number; lastError?: string }[],
  ) {
    this.activeScanId = scanId
    this.stopping = false
    this.pauseRequested = false
    this.lastAllModelsChokeTime = Date.now()
    this.initSlots()

    this.chunkQueue = chunks.map((item) => {
      const idx = typeof item === 'number' ? item : item.index
      const segIdx = typeof item === 'number' ? 0 : (item.segIndex ?? 0)
      const attempts = typeof item === 'number' ? 0 : (item.attempts ?? 0)
      const lastError = typeof item === 'number' ? undefined : item.lastError
      return {
        index: idx,
        segIndex: segIdx,
        status: 'pending' as const,
        attempts,
        lastError,
      }
    })

    this.refreshQueueCounts()
    this.log({
      timestamp: Date.now(),
      type: 'info',
      message: `Loaded ${chunks.length} chunks for scan ${scanId} across ${this.keyApiMap.size} available worker keys.`,
    })
  }

  /**
   * Choke probe: tiny text request ("hy")
   * Evaluates latency: <3s = healthy, 3-15s = degrading, 15-60s = critical, >60s/503/hang = skip model.
   * Timeout 120s.
   */
  public isModelDroppedToday(model: string): boolean {
    return this.droppedModelsToday.has(model)
  }

  public dropModelForToday(model: string, reason?: string) {
    if (this.droppedModelsToday.has(model)) return
    this.droppedModelsToday.add(model)
    const mh = this.state.models[model]
    if (mh) {
      mh.status = 'exhausted'
      mh.effectiveParallelism = 0
    }
    this.state.healthyModels = this.state.healthyModels.filter((m) => m !== model)
    this.state.lastUpdated = Date.now()

    for (const apiKey of this.keyApiMap.values()) {
      forceModelExhausted(model, apiKey, 20)
    }

    this.log({
      timestamp: Date.now(),
      type: 'failover',
      model,
      outcome: '429',
      message: `[RPD EXHAUSTED] Model ${model} reached 429 Daily Quota. Model DROPPED for today — no further requests will be sent to ${model} today. Reason: ${reason ? reason.slice(0, 160) : 'Daily quota limit reached'}`,
    })
  }

  public getKeyModelCooldown(slot: number, model: string): number {
    return this.keyModelCooldown.get(`${slot}:${model}`) || 0
  }

  public setKeyModelCooldown(slot: number, model: string, durationMs: number = 60_000) {
    const until = Date.now() + durationMs
    const current = this.keyModelCooldown.get(`${slot}:${model}`) || 0
    if (until > current) {
      this.keyModelCooldown.set(`${slot}:${model}`, until)
    }
  }

  public isKeyModelInCooldown(slot: number, model: string): boolean {
    return (this.keyModelCooldown.get(`${slot}:${model}`) || 0) > Date.now()
  }

  public setModelCooldown(model: string, durationMs: number = 60_000) {
    const until = Date.now() + durationMs
    this.modelCooldownUntil.set(model, until)
    const mh = this.state.models[model]
    if (mh && mh.status !== 'exhausted') {
      mh.status = 'stopped'
      mh.effectiveParallelism = 0
    }
  }

  public isModelInCooldown(model: string): boolean {
    return (this.modelCooldownUntil.get(model) || 0) > Date.now()
  }

  public clearModelCooldown(model: string) {
    this.modelCooldownUntil.delete(model)
  }

  /**
   * Helper to inspect 429 error and determine if it's Daily Quota (RPD) vs Transient TPM/RPM.
   * If true: daily quota is exhausted (limit: 20 per day, hours of retry delay).
   * If false: short transient rate limit (TPM 250k cap, short retry in seconds).
   */
  public isDailyQuotaExceeded(errString: string): boolean {
    const lower = errString.toLowerCase()

    // Explicit Daily Metric or Quota ID markers
    if (
      lower.includes('generate_content_free_tier_requests') ||
      lower.includes('generaterequestsperday') ||
      lower.includes('requests per day') ||
      lower.includes('perday') ||
      lower.includes('per_day')
    ) {
      return true
    }

    // Daily limit 20 marker
    if (/limit:\s*20\b/i.test(errString)) {
      return true
    }

    // Parse retry delay: if hours, or seconds >= 300 (5 minutes), it is daily quota!
    if (/retry in \d+h/i.test(errString)) {
      return true
    }
    const secMatch = errString.match(/retry in (\d+)(?:\.\d+)?s/i) || errString.match(/retrydelay["']?:\s*["']?(\d+)s/i)
    if (secMatch) {
      const sec = parseFloat(secMatch[1])
      if (sec >= 300) {
        return true
      }
    }

    return false
  }

  /**
   * Helper to sleep with interrupt check every 500ms
   */
  private async sleepWithInterrupt(ms: number): Promise<void> {
    const end = Date.now() + ms
    while (Date.now() < end && !this.stopping) {
      if (this.pauseRequested) {
        await new Promise((r) => setTimeout(r, 1000))
        continue
      }
      const remain = end - Date.now()
      await new Promise((r) => setTimeout(r, Math.min(500, Math.max(50, remain))))
    }
  }

  /**
   * Choke probe: sends tiny text request ("hy").
   * Evaluates response:
   * - Must not contain 503 / high demand / overloaded / unavailable / capacity markers
   * - Must not be empty
   * - Exact reply text or exact 503 error is logged clearly
   * - If 503 or error, strictly treated as FAIL (ok = false)
   */
  public async probeChoke(model: string, overrideKeySlot?: number): Promise<ChokeResult> {
    if (this.isModelDroppedToday(model)) {
      return {
        ok: false,
        latencyMs: 0,
        is503: false,
        is429: true,
        isTimeout: false,
        error: `Model ${model} is dropped for today due to 429 RPD Daily Quota Exceeded.`,
      }
    }

    const availableSlots = Array.from(this.keyApiMap.keys()).filter((s) => {
      if (overrideKeySlot) return s === overrideKeySlot
      return true
    })

    if (availableSlots.length === 0) {
      return {
        ok: false,
        latencyMs: 0,
        is503: false,
        is429: false,
        isTimeout: false,
        error: `No API keys available to probe model ${model}`,
      }
    }

    let candidateSlots = availableSlots.filter((s) => !this.isKeyModelInCooldown(s, model))
    if (candidateSlots.length === 0) candidateSlots = availableSlots
    this.chokeRotationIdx = (this.chokeRotationIdx + 1) % candidateSlots.length
    const chosenSlot = overrideKeySlot || candidateSlots[this.chokeRotationIdx]
    const apiKey = this.keyApiMap.get(chosenSlot)

    if (!apiKey) {
      return {
        ok: false,
        latencyMs: 0,
        is503: false,
        is429: false,
        isTimeout: false,
        error: `Key slot ${chosenSlot} missing API key`,
      }
    }

    const start = Date.now()
    const abortCtrl = new AbortController()
    const timeoutHandle = setTimeout(() => abortCtrl.abort(), ORCHESTRATOR_CHOKE_TIMEOUT_MS)

    try {
      const ai = new GoogleGenAI({ apiKey })
      incrementModelUsage(model, apiKey)

      const response = await ai.models.generateContent({
        model,
        contents: 'hy',
        config: {
          maxOutputTokens: 1000,
          temperature: 0,
        },
      })

      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - start
      const cand0 = response.candidates?.[0]
      const finishReason = cand0?.finishReason

      let rawText = (response.text || '').trim()
      if (cand0?.content?.parts && Array.isArray(cand0.content.parts)) {
        for (const p of cand0.content.parts as Array<{ text?: string; thought?: boolean }>) {
          if (p.text && p.text.trim()) {
            if (!rawText || !p.thought) {
              rawText = p.text.trim()
            }
          }
        }
      }

      // If server responded cleanly with HTTP 200 without throwing 503, but rawText is still empty
      if (!rawText) {
        rawText = finishReason ? `Model responded (${finishReason})` : 'OK (Model acknowledged)'
      }

      const lowerText = rawText.toLowerCase()

      // Check if text returned by the model contains 503 / overload / high demand indications
      const textHas503 =
        lowerText.includes('503') ||
        lowerText.includes('high demand') ||
        lowerText.includes('overloaded') ||
        lowerText.includes('unavailable') ||
        lowerText.includes('capacity') ||
        lowerText.includes('spikes in demand') ||
        lowerText.includes('service unavailable') ||
        lowerText.includes('resource_exhausted') ||
        lowerText.includes('quota exceeded')

      // Strict success criterion: NO 503 markers, NOT safety blocked, candidate returned
      const ok = !textHas503 && finishReason !== 'SAFETY' && (rawText.length > 0 || Boolean(cand0))

      if (ok) {
        this.updateModelHealthFromLatency(model, latencyMs, 'success')
        this.log({
          timestamp: Date.now(),
          type: 'choke',
          model,
          keySlot: chosenSlot,
          outcome: 'success',
          latencyMs,
          message: `Choke ${model} on Key ${chosenSlot} SUCCEEDED in ${(latencyMs / 1000).toFixed(2)}s — Model reply: "${rawText.slice(0, 80)}"`,
        })

        return {
          ok: true,
          latencyMs,
          is503: false,
          is429: false,
          isTimeout: false,
          reply: rawText,
        }
      }

      // If text returned 503
      const failReason = textHas503
        ? `Model returned 503/high demand text: "${rawText.slice(0, 100)}"`
        : `Model returned blocked response (${finishReason || 'SAFETY'})`

      this.updateModelHealthFromLatency(model, latencyMs, '503')
      const slotState = this.state.slots[chosenSlot]
      if (slotState) slotState.errors503++

      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model,
        keySlot: chosenSlot,
        outcome: '503',
        latencyMs,
        message: `Choke ${model} on Key ${chosenSlot} FAILED (503) in ${(latencyMs / 1000).toFixed(2)}s — ${failReason}`,
      })

      return {
        ok: false,
        latencyMs,
        is503: true,
        is429: false,
        isTimeout: false,
        error: failReason,
        reply: rawText,
      }
    } catch (err: unknown) {
      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - start

      let errDetail = ''
      let errStatus: number | undefined

      if (err instanceof Error) {
        errDetail = err.message
      } else if (err && typeof err === 'object') {
        const obj = err as Record<string, unknown>
        errDetail = (obj.message as string) || (obj.statusText as string) || ''
        if (obj.error && typeof obj.error === 'object') {
          const inner = obj.error as Record<string, unknown>
          errDetail = (inner.message as string) || errDetail
          if (typeof inner.code === 'number') errStatus = inner.code
        }
        if (typeof obj.status === 'number') errStatus = obj.status
        if (typeof obj.code === 'number') errStatus = obj.code
        if (!errDetail) {
          try {
            errDetail = JSON.stringify(err)
          } catch {
            errDetail = String(err)
          }
        }
      } else {
        errDetail = String(err)
      }

      const lower = errDetail.toLowerCase()
      const is503 =
        errStatus === 503 ||
        lower.includes('503') ||
        lower.includes('high demand') ||
        lower.includes('unavailable') ||
        lower.includes('overloaded') ||
        lower.includes('capacity') ||
        lower.includes('spikes in demand') ||
        lower.includes('service unavailable')
      const is429 =
        errStatus === 429 ||
        lower.includes('429') ||
        lower.includes('quota') ||
        lower.includes('resource_exhausted')
      const isTimeout =
        abortCtrl.signal.aborted ||
        lower.includes('timeout') ||
        lower.includes('timed out') ||
        lower.includes('aborted')

      let outcome: '503' | '429' | 'timeout' = '503'
      if (is429) outcome = '429'
      else if (isTimeout) outcome = 'timeout'

      const isRpd = is429 && this.isDailyQuotaExceeded(errDetail)

      if (isRpd) {
        this.dropModelForToday(model, errDetail)
      } else if (is429) {
        // TPM rate limit: User rule: Model is NOT dropped for today! Only 1-min cooldown applied to this key
        this.setKeyModelCooldown(chosenSlot, model, 60_000)
        this.log({
          timestamp: Date.now(),
          type: 'warn',
          model,
          keySlot: chosenSlot,
          message: `Choke ${model} hit 429 TPM Rate Limit on Key ${chosenSlot}. Model NOT dropped for today — 1-minute cooldown applied to Key ${chosenSlot}.`,
        })
      }

      if (is503) {
        // 503 Overload: apply 1-min cooldown on model and key
        this.setModelCooldown(model, 60_000)
        this.setKeyModelCooldown(chosenSlot, model, 60_000)
      }

      this.updateModelHealthFromLatency(model, latencyMs, outcome)

      const slotState = this.state.slots[chosenSlot]
      if (slotState) {
        if (is503) slotState.errors503++
        else if (is429) slotState.errors429++
        else if (isTimeout) slotState.timeouts++
      }

      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model,
        keySlot: chosenSlot,
        outcome,
        latencyMs,
        message: `Choke ${model} on Key ${chosenSlot} FAILED (${outcome.toUpperCase()}) in ${(latencyMs / 1000).toFixed(2)}s — Error: "${errDetail.slice(0, 160)}"`,
      })

      return {
        ok: false,
        latencyMs,
        is503,
        is429,
        isTimeout,
        error: errDetail,
      }
    }
  }

  private updateModelHealthFromLatency(
    model: string,
    latencyMs: number,
    outcome: 'success' | '503' | '429' | 'timeout',
  ) {
    const mh = this.state.models[model]
    if (!mh) return

    mh.lastChokeAt = Date.now()
    mh.lastChokeLatencyMs = latencyMs
    mh.lastChokeOutcome = outcome

    if (outcome === '503') {
      mh.total503++
      mh.status = 'stopped'
      mh.effectiveParallelism = 0
    } else if (outcome === '429') {
      mh.total429++
      mh.status = 'critical'
      mh.effectiveParallelism = 1
    } else if (outcome === 'timeout') {
      mh.totalTimeouts++
      mh.status = 'stopped'
      mh.effectiveParallelism = 0
    } else {
      mh.totalSuccess++
      if (latencyMs < 3000) {
        mh.status = 'healthy'
        mh.effectiveParallelism = ORCHESTRATOR_BURST_MAX[model] || 4
      } else if (latencyMs < 15000) {
        mh.status = 'degrading'
        mh.effectiveParallelism = Math.max(1, Math.floor((ORCHESTRATOR_BURST_MAX[model] || 4) / 2))
      } else {
        mh.status = 'critical'
        mh.effectiveParallelism = 1
      }
    }

    this.state.healthyModels = Object.values(this.state.models)
      .filter((m) => m.status === 'healthy' || m.status === 'degrading')
      .map((m) => m.modelId)
  }

  /**
   * Main Orchestrator Loop:
   * 1. 3.5 Flash sentinel choke (probes every 1 min).
   * 2. If 3.5 succeeds -> immediately choke 3.6 Primary.
   * 3. If 3.6 succeeds -> launch 10 burst requests immediately (<3s).
   *    (While 3.6/3.7/3.8 burst is running, the 3.5 1-min choke loop is OFF).
   * 4. If 3.6 fails (503) -> immediately choke 3.7 Secondary.
   * 5. If 3.7 succeeds -> launch 4 burst requests immediately (<3s).
   * 6. If 3.7 fails (503) -> immediately choke 3.8 Tertiary.
   * 7. If 3.8 succeeds -> launch 4 burst requests immediately (<3s).
   * 8. If 3.8 fails (503) -> UNLIMITED WAIT (3.5 Flash 1-min choke loop restarts).
   */
  public async runOrchestrationLoop(): Promise<void> {
    this.stopping = false
    this.pauseRequested = false
    this.log({ timestamp: Date.now(), type: 'info', message: 'Starting chunk scan orchestrator loop.' })

    while (!this.stopping) {
      if (this.pauseRequested) {
        await new Promise((r) => setTimeout(r, 2000))
        continue
      }

      this.refreshQueueCounts()
      const hasUnfinishedChunks = this.chunkQueue.some(
        (c) => c.status === 'pending' || c.status === 'assigned' || c.status === 'processing' || c.status === 'failed' || c.status === 'parked',
      )
      if (!hasUnfinishedChunks) {
        this.state.phase = 'completed'
        this.log({
          timestamp: Date.now(),
          type: 'info',
          message: `All chunks have been scanned! Scan complete.`,
        })
        break
      }

      // ==============================================================
      // STEP 1: 3.5 FLASH SENTINEL CHOKE (1-min loop)
      // Probes gemini-3.5-flash with message "hy".
      // If 3.5 fails: wait 60s (1 min) and re-probe 3.5.
      // ==============================================================
      this.state.phase = 'choke_canary'
      this.state.activeModel = ORCHESTRATOR_CANARY_MODEL
      this.state.canaryActive = true
      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model: ORCHESTRATOR_CANARY_MODEL,
        message: `Phase [choke_canary]: Probing 3.5 Flash sentinel (${ORCHESTRATOR_CANARY_MODEL}) with "hy"...`,
      })

      const canaryChoke = await this.probeChoke(ORCHESTRATOR_CANARY_MODEL)

      if (!canaryChoke.ok) {
        this.log({
          timestamp: Date.now(),
          type: 'warn',
          model: ORCHESTRATOR_CANARY_MODEL,
          message: `Canary 3.5 choke failed (${canaryChoke.error || '503 Overload'}). Waiting 60s (1 min) before next 3.5 choke...`,
        })
        await this.sleepWithInterrupt(60_000)
        continue
      }

      this.state.canaryActive = false
      this.log({
        timestamp: Date.now(),
        type: 'info',
        model: ORCHESTRATOR_CANARY_MODEL,
        message: `Canary 3.5 choke SUCCEEDED (reply: "${canaryChoke.reply}"). Immediately probing Primary 3.6...`,
      })

      // ==============================================================
      // STEP 2: PRIMARY 3.6 CHOKE PROBE
      // Probes gemini-3.6-flash with message "hy".
      // If 3.6 succeeds -> clear cooldown and launch 10 burst requests immediately (<3s).
      // (3.5 1-min choke is OFF while 3.6 burst is running).
      // User rule: When 3.5 canary succeeds, ALWAYS send 1 choke probe to 3.6
      // to check if 3.6 is out of 503 overload or not!
      // ==============================================================
      if (this.isModelDroppedToday(ORCHESTRATOR_PRIMARY_MODEL)) {
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_PRIMARY_MODEL,
          message: `Primary 3.6 is DROPPED for today (429 RPD Quota Exceeded). Skipping 3.6 directly to Secondary 3.7...`,
        })
      } else {
        this.state.phase = 'choke_primary'
        this.state.activeModel = ORCHESTRATOR_PRIMARY_MODEL
        this.log({
          timestamp: Date.now(),
          type: 'choke',
          model: ORCHESTRATOR_PRIMARY_MODEL,
          message: `Phase [choke_primary]: Probing Primary model ${ORCHESTRATOR_PRIMARY_MODEL} with "hy" to check if out of 503 overload...`,
        })

        const primaryChoke = await this.probeChoke(ORCHESTRATOR_PRIMARY_MODEL)

        if (primaryChoke.ok) {
          this.clearModelCooldown(ORCHESTRATOR_PRIMARY_MODEL)
          this.state.phase = 'burst_primary'
          this.log({
            timestamp: Date.now(),
            type: 'info',
            model: ORCHESTRATOR_PRIMARY_MODEL,
            message: `Phase [burst_primary]: Primary 3.6 confirmed out of 503 overload (reply: "${primaryChoke.reply}"). Launching 10 burst requests immediately within 3s (3.5 choke is OFF during burst)...`,
          })
          const burstResult = await this.executeBurst(ORCHESTRATOR_PRIMARY_MODEL, 10)
          if (burstResult.successes > 0) {
            continue
          } else if (burstResult.errors503 > 0) {
            this.log({
              timestamp: Date.now(),
              type: 'failover',
              model: ORCHESTRATOR_PRIMARY_MODEL,
              message: `Primary 3.6 burst failed with 503 High Demand across chunks. Immediately failing over to Secondary 3.7...`,
            })
            // Fall through to Step 3 (Secondary 3.7)
          } else {
            continue
          }
        } else {
          this.log({
            timestamp: Date.now(),
            type: 'failover',
            model: ORCHESTRATOR_PRIMARY_MODEL,
            message: `Primary 3.6 choke failed (${primaryChoke.error || '503 High Demand'} — 3.6 still in overload). Immediately probing Secondary 3.7...`,
          })
        }
      }

      // ==============================================================
      // STEP 3: SECONDARY 3.7 CHOKE PROBE (on 3.6 failure, dropped, or 503)
      // Probes gemini-3.7-flash with message "hy".
      // If 3.7 succeeds -> clear cooldown and launch 4 burst requests immediately (<3s).
      // (3.5 1-min choke is OFF while 3.7 burst is running).
      // ==============================================================
      if (this.isModelDroppedToday(ORCHESTRATOR_SECONDARY_MODEL)) {
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_SECONDARY_MODEL,
          message: `Secondary 3.7 is DROPPED for today (429 RPD Quota Exceeded). Skipping 3.7 directly to Tertiary 3.8...`,
        })
      } else {
        this.state.phase = 'choke_secondary'
        this.state.activeModel = ORCHESTRATOR_SECONDARY_MODEL
        this.log({
          timestamp: Date.now(),
          type: 'choke',
          model: ORCHESTRATOR_SECONDARY_MODEL,
          message: `Phase [choke_secondary]: Probing Secondary model ${ORCHESTRATOR_SECONDARY_MODEL} with "hy" to check if out of 503 overload...`,
        })

        const secChoke = await this.probeChoke(ORCHESTRATOR_SECONDARY_MODEL)

        if (secChoke.ok) {
          this.clearModelCooldown(ORCHESTRATOR_SECONDARY_MODEL)
          this.state.phase = 'burst_secondary'
          this.log({
            timestamp: Date.now(),
            type: 'info',
            model: ORCHESTRATOR_SECONDARY_MODEL,
            message: `Phase [burst_secondary]: Secondary 3.7 confirmed out of 503 overload (reply: "${secChoke.reply}"). Launching 4 burst requests immediately within 3s (3.5 choke is OFF during burst)...`,
          })
          const burstResult = await this.executeBurst(ORCHESTRATOR_SECONDARY_MODEL, 4)
          if (burstResult.successes > 0) {
            continue
          } else if (burstResult.errors503 > 0) {
            this.log({
              timestamp: Date.now(),
              type: 'failover',
              model: ORCHESTRATOR_SECONDARY_MODEL,
              message: `Secondary 3.7 burst failed with 503 High Demand across chunks. Immediately failing over to Tertiary 3.8...`,
            })
            // Fall through to Step 4 (Tertiary 3.8)
          } else {
            continue
          }
        } else {
          this.log({
            timestamp: Date.now(),
            type: 'failover',
            model: ORCHESTRATOR_SECONDARY_MODEL,
            message: `Secondary 3.7 choke failed (${secChoke.error || '503 High Demand'} — 3.7 still in overload). Immediately probing Tertiary 3.8...`,
          })
        }
      }

      // ==============================================================
      // STEP 4: TERTIARY 3.8 CHOKE PROBE (on 3.7 failure, dropped, or 503)
      // Probes gemini-3.8-flash with message "hy".
      // If 3.8 succeeds -> clear cooldown and launch 4 burst requests immediately (<3s).
      // (3.5 1-min choke is OFF while 3.8 burst is running).
      // ==============================================================
      if (this.isModelDroppedToday(ORCHESTRATOR_TERTIARY_MODEL)) {
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_TERTIARY_MODEL,
          message: `Tertiary 3.8 is DROPPED for today (429 RPD Quota Exceeded). All worker models exhausted.`,
        })
      } else {
        this.state.phase = 'choke_tertiary'
        this.state.activeModel = ORCHESTRATOR_TERTIARY_MODEL
        this.log({
          timestamp: Date.now(),
          type: 'choke',
          model: ORCHESTRATOR_TERTIARY_MODEL,
          message: `Phase [choke_tertiary]: Probing Tertiary model ${ORCHESTRATOR_TERTIARY_MODEL} with "hy" to check if out of 503 overload...`,
        })

        const tertChoke = await this.probeChoke(ORCHESTRATOR_TERTIARY_MODEL)

        if (tertChoke.ok) {
          this.clearModelCooldown(ORCHESTRATOR_TERTIARY_MODEL)
          this.state.phase = 'burst_tertiary'
          this.log({
            timestamp: Date.now(),
            type: 'info',
            model: ORCHESTRATOR_TERTIARY_MODEL,
            message: `Phase [burst_tertiary]: Tertiary 3.8 confirmed out of 503 overload (reply: "${tertChoke.reply}"). Launching 4 burst requests immediately within 3s (3.5 choke is OFF during burst)...`,
          })
          const burstResult = await this.executeBurst(ORCHESTRATOR_TERTIARY_MODEL, 4)
          if (burstResult.successes > 0) {
            continue
          } else if (burstResult.errors503 > 0) {
            this.log({
              timestamp: Date.now(),
              type: 'failover',
              model: ORCHESTRATOR_TERTIARY_MODEL,
              message: `Tertiary 3.8 burst failed with 503 High Demand across chunks. All worker models in 503 overload.`,
            })
            // Fall through to Step 5
          } else {
            continue
          }
        } else {
          this.log({
            timestamp: Date.now(),
            type: 'failover',
            model: ORCHESTRATOR_TERTIARY_MODEL,
            message: `Tertiary 3.8 choke failed (${tertChoke.error || '503 High Demand'}). All worker models in 503 overload.`,
          })
        }
      }

      // ==============================================================
      // STEP 5: UNLIMITED WAIT (All available worker models failed with 503 or dropped)
      // Resumes 3.5 Flash 1-min choke loop.
      // ==============================================================
      const allDropped =
        this.isModelDroppedToday(ORCHESTRATOR_PRIMARY_MODEL) &&
        this.isModelDroppedToday(ORCHESTRATOR_SECONDARY_MODEL) &&
        this.isModelDroppedToday(ORCHESTRATOR_TERTIARY_MODEL)

      this.log({
        timestamp: Date.now(),
        type: 'warn',
        message: allDropped
          ? `All worker models (3.6, 3.7, 3.8) are DROPPED for today (429 RPD Daily Quota Exceeded). Entering UNLIMITED WAIT until daily reset.`
          : `All worker models (3.6, 3.7, 3.8) are in 503 / High Demand. Entering UNLIMITED WAIT — resuming 3.5 Flash 1-min choke loop.`,
      })

      this.state.phase = 'recovery_watch'
      this.state.canaryActive = true
      this.state.activeModel = ORCHESTRATOR_CANARY_MODEL
      await this.sleepWithInterrupt(60_000)
    }
  }

  /**
   * Execute Burst on a given model:
   * - Parallelism: 10 on 3.6, 4 on 3.7, 4 on 3.8.
   * - IMMEDIATE LAUNCH: all requests dispatched within 3 seconds (<200ms stagger between worker starts).
   * - While burst is executing, 3.5 1-min choke is OFF.
   * - If any chunk encounters 503:
   *     NO retry on this burst, NO cooldown on key. Chunk is queued for next cycle.
   */
  private async executeBurst(
    model: string,
    burstCountOverride?: number,
  ): Promise<{ launched: number; successes: number; errors503: number; errors429: number; timeouts: number }> {
    const defaultMax = model === ORCHESTRATOR_PRIMARY_MODEL ? 10 : 4
    const maxParallel = burstCountOverride || ORCHESTRATOR_BURST_MAX[model] || defaultMax

    // Reset failed/parked chunks back to pending so they are retried first in this cycle
    let retriedCount = 0
    for (const c of this.chunkQueue) {
      if (c.status === 'failed' || c.status === 'parked') {
        c.status = 'pending'
        c.assignedKey = undefined
        c.assignedModel = undefined
        retriedCount++
      }
    }
    if (retriedCount > 0) {
      this.log({
        timestamp: Date.now(),
        type: 'info',
        model,
        message: `New burst cycle: ${retriedCount} previously-failed chunk(s) re-queued for retry (prioritized first).`,
      })
      this.refreshQueueCounts()
    }

    this.lastBatchTime = Date.now()

    // Find available worker key slots that are currently idle (slots 2..13, 15, 16)
    // and NOT in 1-minute TPM cooldown for this model
    const availableSlots = Array.from(this.keyApiMap.keys()).filter((s) => {
      const slotState = this.state.slots[s]
      if (!slotState || slotState.status === 'processing' || slotState.status === 'reserved' || slotState.status === 'skipped') {
        return false
      }
      if (this.isKeyModelInCooldown(s, model)) {
        return false
      }
      slotState.status = 'idle'
      return true
    })

    if (availableSlots.length === 0) {
      let minRemainingMs = 60_000
      for (const s of this.keyApiMap.keys()) {
        const cd = this.getKeyModelCooldown(s, model)
        if (cd > Date.now()) {
          minRemainingMs = Math.min(minRemainingMs, cd - Date.now())
        }
      }
      this.log({
        timestamp: Date.now(),
        type: 'warn',
        model,
        message: `All worker keys for ${model} are in 1-min TPM cooldown (${Math.ceil(minRemainingMs / 1000)}s remaining). Waiting for keys to cool down...`,
      })
      await this.sleepWithInterrupt(Math.min(minRemainingMs, 5000))
      return { launched: 0, successes: 0, errors503: 0, errors429: 0, timeouts: 0 }
    }

    // Rotate starting slot so all worker keys get equal opportunities
    this.burstSlotRotationIdx = (this.burstSlotRotationIdx + 1) % availableSlots.length
    const rotatedSlots = [
      ...availableSlots.slice(this.burstSlotRotationIdx),
      ...availableSlots.slice(0, this.burstSlotRotationIdx),
    ]

    const slotsToUse = rotatedSlots.slice(0, maxParallel)

    // RAPID LAUNCH: all burst requests must be dispatched within 3 seconds!
    // Stagger is 150-200ms per slot so all 10 requests fire within 2.0 seconds.
    const launchStaggerMs = slotsToUse.length > 1
      ? Math.min(200, Math.floor(2200 / slotsToUse.length))
      : 0

    this.log({
      timestamp: Date.now(),
      type: 'info',
      model,
      message: `Launching ${slotsToUse.length} parallel burst requests on ${model} across Keys [${slotsToUse.join(', ')}] within 3s...`,
    })

    let totalSuccesses = 0
    let total503 = 0
    let total429 = 0
    let totalTimeouts = 0

    const inFlightPromises: Promise<void>[] = []

    for (let i = 0; i < slotsToUse.length; i++) {
      if (this.stopping) break
      const slot = slotsToUse[i]

      // Rapid launch stagger (<200ms) to spread initial handshake within 2-3s window
      if (i > 0 && launchStaggerMs > 0) {
        await new Promise((r) => setTimeout(r, launchStaggerMs))
      }
      if (this.stopping) break

      const slotTask = (async () => {
        while (!this.stopping && !this.pauseRequested) {
          const nextChunk = this.getNextChunkForAssignment(slot, model)
          if (!nextChunk) break

          const outcome = await this.processChunkOnSlot(nextChunk, slot, model)
          if (outcome === 'success') {
            totalSuccesses++
          } else if (outcome === '503') {
            total503++
            this.log({
              timestamp: Date.now(),
              type: 'warn',
              model,
              keySlot: slot,
              message: `503 High Demand on Key ${slot} — 1-minute cooldown applied to Key ${slot} for ${model} (TPM safety). Chunk #${nextChunk.index} queued for next cycle.`,
            })
            break
          } else if (outcome === '429') {
            total429++
            break
          } else {
            totalTimeouts++
            break
          }
        }
      })()

      inFlightPromises.push(slotTask)
    }

    if (inFlightPromises.length > 0) {
      await Promise.all(inFlightPromises)
    }

    if (total503 > 0 && totalSuccesses === 0) {
      this.setModelCooldown(model, 60_000)
      this.log({
        timestamp: Date.now(),
        type: 'failover',
        model,
        outcome: '503',
        message: `All ${total503} chunk request(s) on ${model} returned 503 High Demand. 1-minute cooldown applied to ${model}.`,
      })
    }

    return {
      launched: slotsToUse.length,
      successes: totalSuccesses,
      errors503: total503,
      errors429: total429,
      timeouts: totalTimeouts,
    }
  }

  /**
   * Continuous pipeline chunk processor with strict 1-chunk-per-key locking.
   */
  private async processChunkOnSlot(
    chunk: ChunkQueueItem,
    slot: number,
    model: string,
  ): Promise<'success' | '503' | '429' | 'timeout'> {
    const slotState = this.state.slots[slot]
    const apiKey = this.keyApiMap.get(slot)
    if (!slotState || !apiKey) return '503'

    chunk.status = 'processing'
    chunk.assignedKey = slot
    chunk.assignedModel = model
    chunk.startedAt = Date.now()
    chunk.attempts++

    // Set 1-minute cooldown from request dispatch time (TPM 250k protection)
    this.setKeyModelCooldown(slot, model, 60_000)

    slotState.status = 'processing'
    slotState.activeModel = model
    slotState.activeChunk = chunk.index
    slotState.chunkStartedAt = Date.now()
    this.state.lastUpdated = Date.now()

    this.log({
      timestamp: Date.now(),
      type: 'chunk',
      model,
      keySlot: slot,
      chunkIndex: chunk.index,
      message: `Chunk #${chunk.index} (Minute ${(chunk.segIndex ?? 0) + 1}) assigned to Key ${slot} on ${model} (Attempt ${chunk.attempts}).`,
    })

    const abortCtrl = new AbortController()
    const timeoutHandle = setTimeout(() => abortCtrl.abort(), ORCHESTRATOR_CHUNK_TIMEOUT_MS)
    const startTime = Date.now()

    let outcome: 'success' | '503' | '429' | 'timeout' = 'success'
    let errorDetail = ''

    try {
      let result = { ok: true, is503: false, is429: false, isTimeout: false, error: '' }

      if (this.chunkExecutor) {
        result = await this.chunkExecutor(
          chunk.index,
          slot,
          apiKey,
          model,
          ORCHESTRATOR_CHUNK_TIMEOUT_MS,
          abortCtrl.signal,
          chunk.segIndex ?? 0,
        )
      } else {
        // Default simulated work / live call placeholder
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }

      clearTimeout(timeoutHandle)

      if (result.ok) {
        outcome = 'success'
        chunk.status = 'done'
        slotState.successCount++
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome: 'success',
          latencyMs: Date.now() - startTime,
          message: `Chunk #${chunk.index} (Minute ${(chunk.segIndex ?? 0) + 1}) completed successfully on Key ${slot} (${model}) in ${((Date.now() - startTime) / 1000).toFixed(1)}s.`,
        })
      } else {
        if (result.is429) outcome = '429'
        else if (result.isTimeout || abortCtrl.signal.aborted) outcome = 'timeout'
        else outcome = '503'
        errorDetail = result.error || 'Unknown error'
        throw new Error(errorDetail)
      }
    } catch (err: unknown) {
      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - startTime
      const msg = err instanceof Error ? err.message : String(err)
      const lower = msg.toLowerCase()

      if (lower.includes('429') || lower.includes('quota')) outcome = '429'
      else if (abortCtrl.signal.aborted || lower.includes('timeout')) outcome = 'timeout'
      else outcome = '503'

      if (outcome === '503') {
        slotState.errors503++
        this.updateModelHealthFromLatency(model, latencyMs, '503')
        // User rule: 1-minute cooldown on this key for this model
        this.setKeyModelCooldown(slot, model, 60_000)
      } else if (outcome === '429') {
        slotState.errors429++
        this.updateModelHealthFromLatency(model, latencyMs, '429')
        this.setKeyModelCooldown(slot, model, 60_000)
      } else if (outcome === 'timeout') {
        slotState.timeouts++
        this.updateModelHealthFromLatency(model, latencyMs, 'timeout')
      }

      // Check if 429 is Daily Quota Exceeded (RPD) vs TPM Rate Limit
      const isDailyQuota = outcome === '429' && this.isDailyQuotaExceeded(msg)

      if (isDailyQuota) {
        this.dropModelForToday(model, msg)
        // User rule: 429 RPD exhausted -> model dropped for today, and return chunk BACK to queue immediately
        chunk.status = 'pending'
        chunk.assignedKey = undefined
        chunk.assignedModel = undefined
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome: '429',
          latencyMs,
          message: `Chunk #${chunk.index} hit 429 Daily Quota Exceeded (RPD) on ${model}. Model DROPPED for today — chunk returned to queue for failover retry.`,
        })
      } else if (outcome === '429') {
        // TPM (Tokens Per Minute) 250k limit hit!
        // User rule: Model is NOT dropped for today! Only 1-minute cooldown applied to this key, chunk returned to queue
        chunk.status = 'pending'
        chunk.assignedKey = undefined
        chunk.assignedModel = undefined
        this.setKeyModelCooldown(slot, model, 60_000)
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome: '429',
          latencyMs,
          message: `Chunk #${chunk.index} hit 429 TPM Rate Limit (250K cap) on Key ${slot}. Model NOT dropped for today. 1-minute cooldown applied to Key ${slot} — chunk returned to queue for next ready key/failover.`,
        })
      } else {
        // 503 and transient errors: re-queue for next burst cycle retry
        chunk.status = 'pending'
        chunk.assignedKey = undefined
        chunk.assignedModel = undefined
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome,
          latencyMs,
          message: `Chunk #${chunk.index} encountered ${outcome.toUpperCase()} (High Demand / Overloaded) on Key ${slot}. 1-minute cooldown applied to Key ${slot}. Re-queued for retry in next burst cycle.`,
        })
      }
    } finally {
      // User rule: from request finish / error time, ensure 1-min cooldown on this key for this model
      this.setKeyModelCooldown(slot, model, 60_000)
      slotState.status = 'idle'
      slotState.activeChunk = null
      slotState.chunkStartedAt = null
      slotState.lastOutcome = outcome
      slotState.lastLatencyMs = Date.now() - startTime
      this.refreshQueueCounts()
      this.state.lastUpdated = Date.now()
    }

    return outcome
  }

  private getNextChunkForAssignment(slot: number, model: string): ChunkQueueItem | null {
    // Retry queue first, then new chunks
    let target = this.chunkQueue.find((c) => c.status === 'pending' && c.attempts > 0)
    if (!target) {
      target = this.chunkQueue.find((c) => c.status === 'pending')
    }
    if (target) {
      target.status = 'assigned'
      target.assignedKey = slot
      target.assignedModel = model
      this.refreshQueueCounts()
      return target
    }
    return null
  }

  /**
   * Unlimited Wait Recovery Watch:
   * Only 3.5 canary polls (1 per minute).
   * When 3.5 recovers -> choke 3.6 (primary).
   * If 3.6 recovers -> resume primary burst 10.
   */
  private async enterUnlimitedWait(): Promise<void> {
    this.state.phase = 'recovery_watch'
    this.state.canaryActive = true
    this.state.activeModel = ORCHESTRATOR_CANARY_MODEL
    this.state.lastMessage = 'Unlimited wait active: polling 3.5 canary every 60s for capacity recovery.'
    this.state.lastUpdated = Date.now()

    while (!this.stopping) {
      if (this.pauseRequested) {
        await new Promise((r) => setTimeout(r, 2000))
        continue
      }

      this.log({
        timestamp: Date.now(),
        type: 'recovery',
        model: ORCHESTRATOR_CANARY_MODEL,
        message: `Recovery watch: polling 3.5 canary probe...`,
      })

      const canaryProbe = await this.probeChoke(ORCHESTRATOR_CANARY_MODEL)

      if (canaryProbe.ok) {
        this.log({
          timestamp: Date.now(),
          type: 'recovery',
          model: ORCHESTRATOR_CANARY_MODEL,
          message: `Canary 3.5 succeeded! Testing Primary 3.6 confirmation...`,
        })

        const primaryConfirm = await this.probeChoke(ORCHESTRATOR_PRIMARY_MODEL)
        if (primaryConfirm.ok) {
          this.log({
            timestamp: Date.now(),
            type: 'recovery',
            model: ORCHESTRATOR_PRIMARY_MODEL,
            message: `Primary 3.6 capacity restored! Resuming primary burst pipeline.`,
          })
          this.state.canaryActive = false
          return
        } else {
          this.log({
            timestamp: Date.now(),
            type: 'recovery',
            model: ORCHESTRATOR_PRIMARY_MODEL,
            message: `Primary 3.6 still in high demand / 503. Continuing canary 3.5 recovery watch...`,
          })
        }
      }

      // Wait 60 seconds before next canary poll
      for (let i = 0; i < 60; i++) {
        if (this.stopping) break
        await new Promise((r) => setTimeout(r, 1000))
      }
    }
  }

  private async reChokeAllModels(): Promise<void> {
    this.log({
      timestamp: Date.now(),
      type: 'info',
      message: `15-minute periodic health check: re-probing all model tiers.`,
    })
    const models = [
      ORCHESTRATOR_PRIMARY_MODEL,
      ORCHESTRATOR_SECONDARY_MODEL,
      ORCHESTRATOR_TERTIARY_MODEL,
    ]
    for (const m of models) {
      await this.probeChoke(m)
    }
  }

  public stop(reason?: string) {
    this.stopping = true
    this.state.phase = 'idle'
    if (reason && reason.trim()) {
      this.log({ timestamp: Date.now(), type: 'info', message: reason })
    }
  }

  public reset() {
    this.stopping = false
    this.pauseRequested = false
    this.chunkQueue = []
    this.state = this.createInitialState()
  }

  public pause() {
    this.pauseRequested = true
    this.log({ timestamp: Date.now(), type: 'info', message: 'Orchestrator paused.' })
  }

  public resume() {
    this.pauseRequested = false
    this.log({ timestamp: Date.now(), type: 'info', message: 'Orchestrator resumed.' })
  }
}

// Global orchestrator singleton instance for the app
export const globalOrchestrator = new ChunkScanOrchestrator()
export { ChunkScanOrchestrator as OrchestratorCore }
