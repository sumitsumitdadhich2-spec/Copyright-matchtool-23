import 'server-only'

import {
  apiKeyHash,
  getModelUsage,
  setModelExhausted,
  isModelDailyQuotaExhausted,
  geminiUsageDay,
  checkDailyReset,
  cleanseStartupQuotas,
} from './store'
import { pacingIntervalMs, RATE_COOLDOWN_MS, CHUNK_COOLDOWN_MS, displayModelName } from './models'

export interface CandidateLane {
  apiKey: string
  keyIdx: number
  modelId: string
  slot?: number
  rpd?: number
}

interface LaneWaiter {
  scanId: string
  scanTitle: string
  operation: string
  resolve: (releaseFn: (actualVideoSec?: number, cooldownOverrideMs?: number) => void) => void
  reject: (err: Error) => void
  isStopping?: () => boolean
}

export interface VerifyModelState {
  successCount: number // 0, 1, 2 (resets to 0 when it hits 3)
  cooldownUntil: number // 1-minute cooldown timestamp (after 3 successes or 429)
  retryLockSlot: number | null // slot holding 429 retry lock, or null
  retryLockId: string | null // identifier of request holding 429 retry lock
}

export interface ChunkModelState {
  cooldownUntil: number // 1-minute cooldown timestamp (after 1 request or 429)
  retryLockId: string | null // identifier of chunk request holding 429 retry lock
}

interface GlobalLaneState {
  laneKey: string
  keyHash: string
  keyIdx: number
  modelId: string
  slot: number
  activeScanId: string | null
  activeScanTitle: string | null
  activeOperation: string | null
  activeSince: number | null
  lastCompletedAt?: number | null
  lastOperation: string | null
  lastOperationVideoSec: number | null
  nextFreeAt: number
  cooldownUntil: number
  consecutiveQuotaErrors?: number
  firstRpdErrorAt?: number
  isExhausted: boolean
  waiters: LaneWaiter[]
}

interface BreakerFailureEvent {
  time: number
  keyHash: string
  failureClass: 'rate' | 'overloaded' | 'rpd'
}

export interface ActiveKeyExecution {
  keyHash: string
  keyIdx: number
  scanId: string
  scanTitle: string
  modelId: string
  operation: string
  slot: number
  activeSince: number
}

class GlobalGeminiCoordinator {
  private lanes = new Map<string, GlobalLaneState>()
  private verifyModelStates = new Map<string, VerifyModelState>()
  private chunkModelStates = new Map<string, ChunkModelState>()
  /** Global Key-level exclusive lock: guarantees that on ANY single API key, NO two models or requests can EVER run in parallel! */
  private activeKeyLocks = new Map<string, ActiveKeyExecution>()
  /** Global Key-level pacing buffer: minimum spacing between ANY consecutive requests on the same key */
  private keyNextFreeAt = new Map<string, number>()
  private currentActiveDay = geminiUsageDay()
  private breakerEvents: BreakerFailureEvent[] = []
  private globalPauseUntil: number = 0
  private breakerBackoffMinutes = [2, 4, 8, 10]
  private breakerBackoffIndex = 0
  private activePauseClass: 'rate' | 'overloaded' | 'rpd' | null = null
  private isProbeInFlight = false
  private probeLaneKey: string | null = null
  private exhaustedLogSet = new Set<string>()
  private rateSpikeLogUntil = new Map<string, number>()

  /**
   * Checks if the date has rolled over (midnight Pacific Time).
   * Automatically clears all exhaustion flags across all lanes so the new day's quota is instantly active!
   */
  public checkDayRollover(): boolean {
    const today = geminiUsageDay()
    if (today !== this.currentActiveDay) {
      console.log(`[Global Coordinator] Daily quota rollover detected (${this.currentActiveDay} -> ${today}). Resetting all lane exhaustion flags!`)
      this.currentActiveDay = today
      this.exhaustedLogSet.clear()
      this.rateSpikeLogUntil.clear()
      this.activeKeyLocks.clear()
      this.keyNextFreeAt.clear()
      for (const lane of this.lanes.values()) {
        lane.isExhausted = false
        lane.cooldownUntil = 0
        delete lane.firstRpdErrorAt
      }
      this.verifyModelStates.clear()
      this.chunkModelStates.clear()
      checkDailyReset()
      return true
    }
    return false
  }

  /**
   * Helper to retrieve or create the Verifier model-level state
   * (tracking 3-success batch count, mandatory 1-min cooldown, and 429 retry priority lock).
   */
  public getVerifyModelState(apiKey: string, modelId: string): VerifyModelState {
    const key = `${apiKeyHash(apiKey)}:${modelId}`
    let state = this.verifyModelStates.get(key)
    if (!state) {
      state = {
        successCount: 0,
        cooldownUntil: 0,
        retryLockSlot: null,
        retryLockId: null,
      }
      this.verifyModelStates.set(key, state)
    }
    return state
  }

  /**
   * Helper to retrieve or create the Chunk model-level state
   * (tracking mandatory 1-min cooldown after 1 request and 429 retry priority lock).
   */
  public getChunkModelState(apiKey: string, modelId: string): ChunkModelState {
    const key = `${apiKeyHash(apiKey)}:${modelId}`
    let state = this.chunkModelStates.get(key)
    if (!state) {
      state = {
        cooldownUntil: 0,
        retryLockId: null,
      }
      this.chunkModelStates.set(key, state)
    }
    return state
  }

  /**
   * Instant, zero-wait quota check:
   * Verifies if a model on a given API key has exhausted its daily quota (RPD)
   * using coordinator in-memory lane state and cached counters.json.
   */
  public isModelExhausted(apiKey: string, modelId: string, rpdCap: number = 500): boolean {
    this.checkDayRollover()
    const used = getModelUsage(modelId, apiKey)
    const lane = this.getOrCreateLane(apiKey, modelId, 0)
    // FINAL DECISION: Setting quota is the sole authority for daily exhaustion!
    if (used < rpdCap) {
      lane.isExhausted = false
      return false
    }
    const exhaustedInStore = isModelDailyQuotaExhausted(modelId, apiKey, rpdCap)
    lane.isExhausted = exhaustedInStore
    return exhaustedInStore
  }

  private getLaneKey(apiKey: string, modelId: string, slot: number = 0): string {
    return `${apiKeyHash(apiKey)}:${modelId}:${slot}`
  }

  private getOrCreateLane(apiKey: string, modelId: string, slot: number = 0, keyIdx?: number): GlobalLaneState {
    const key = this.getLaneKey(apiKey, modelId, slot)
    let lane = this.lanes.get(key)
    if (!lane) {
      lane = {
        laneKey: key,
        keyHash: apiKeyHash(apiKey),
        keyIdx: typeof keyIdx === 'number' && keyIdx > 0 ? keyIdx : 0,
        modelId,
        slot,
        activeScanId: null,
        activeScanTitle: null,
        activeOperation: null,
        activeSince: null,
        lastCompletedAt: null,
        lastOperation: null,
        lastOperationVideoSec: null,
        nextFreeAt: 0,
        cooldownUntil: 0,
        isExhausted: false,
        waiters: [],
      }
      this.lanes.set(key, lane)
    }
    if (typeof keyIdx === 'number' && keyIdx > 0) {
      lane.keyIdx = keyIdx
    }
    return lane
  }

  /** Check if a lane is currently in use by ANY scan, in cooldown/pacing, or exhausted */
  public isLaneBusy(apiKey: string, modelId: string, slot: number = 0, rpdCap: number = 500): {
    busy: boolean
    exhausted?: boolean
    activeScanId?: string
    activeScanTitle?: string
    activeOperation?: string
    waitSec?: number
    cooling?: boolean
  } {
    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    const now = Date.now()
    const used = getModelUsage(modelId, apiKey)
    const kh = apiKeyHash(apiKey)

    // FINAL DECISION: Only setting quota determines daily exhaustion!
    if (used < rpdCap) {
      lane.isExhausted = false
    } else if (lane.isExhausted || isModelDailyQuotaExhausted(modelId, apiKey, rpdCap)) {
      lane.isExhausted = true
      return {
        busy: true,
        exhausted: true,
        activeOperation: 'Exhausted for today',
      }
    }

    // KEY-LEVEL EXCLUSIVE LOCK:
    // If THIS API key is currently executing ANY request (even on another model like 3.8 vs 3.6 or verifier),
    // this lane is strictly BUSY! Never allow 2 concurrent requests on the same key!
    const activeKeyLock = this.activeKeyLocks.get(kh)
    if (activeKeyLock) {
      return {
        busy: true,
        activeScanId: activeKeyLock.scanId,
        activeScanTitle: activeKeyLock.scanTitle,
        activeOperation: `Key ${activeKeyLock.keyIdx || lane.keyIdx || ''} busy: ${displayModelName(activeKeyLock.modelId)} running (${activeKeyLock.operation})`,
      }
    }

    // KEY-LEVEL PACING BUFFER:
    const keyNextFree = this.keyNextFreeAt.get(kh) || 0
    if (keyNextFree > now) {
      const waitSec = Math.ceil((keyNextFree - now) / 1000)
      return {
        busy: true,
        cooling: true,
        waitSec,
        activeOperation: `Key ${lane.keyIdx} pacing buffer (${waitSec}s)`,
      }
    }

    if (lane.activeScanId) {
      return {
        busy: true,
        activeScanId: lane.activeScanId,
        activeScanTitle: lane.activeScanTitle || undefined,
        activeOperation: lane.activeOperation || undefined,
      }
    }

    if (lane.cooldownUntil > now) {
      return {
        busy: true,
        cooling: true,
        waitSec: Math.ceil((lane.cooldownUntil - now) / 1000),
      }
    }

    if (lane.nextFreeAt > now) {
      return {
        busy: true,
        waitSec: Math.ceil((lane.nextFreeAt - now) / 1000),
      }
    }

    return { busy: false }
  }

  /**
   * Check if an API key has any active lanes currently executing in another scan.
   * Useful for load-balancing parallel scans so that each scan prefers idle keys.
   */
  public isKeyActiveInOtherScan(apiKey: string, currentScanId: string): boolean {
    const hash = apiKeyHash(apiKey)
    const activeKey = this.activeKeyLocks.get(hash)
    if (activeKey && activeKey.scanId !== currentScanId) {
      return true
    }
    for (const lane of this.lanes.values()) {
      if (lane.keyHash === hash && lane.activeScanId !== null && lane.activeScanId !== currentScanId) {
        return true
      }
    }
    return false
  }

  /**
   * Reset all in-memory lane exhaustion and cooldown flags.
   * Called when user manually resets daily counters via Settings.
   */
  public resetAllLanes(): void {
    this.activeKeyLocks.clear()
    this.keyNextFreeAt.clear()
    for (const lane of this.lanes.values()) {
      lane.isExhausted = false
      delete lane.firstRpdErrorAt
      lane.cooldownUntil = 0
      lane.nextFreeAt = 0
    }
    this.verifyModelStates.clear()
    this.chunkModelStates.clear()
    this.exhaustedLogSet.clear()
    try {
      cleanseStartupQuotas()
    } catch {}
    console.log('[Global Coordinator] All lane exhaustion, cooldown states, and verify/chunk batch states reset.')
  }

  /**
   * Acquire an exclusive lock on a (Key × Model × Slot) lane across ALL scans in the entire application.
   * If another scan is using the lane or if the lane is in TPM pacing / 429 cooldown,
   * this will wait and yield gracefully without triggering duplicate requests or 429 collisions.
   */
  public async acquireLane(opts: {
    scanId: string
    scanTitle?: string
    apiKey: string
    keyIdx?: number
    modelId: string
    slot?: number
    operation: string
    videoSeconds?: number
    rpd?: number
    isVerify?: boolean
    verifyLockId?: string
    isChunk?: boolean
    chunkLockId?: string
    onWait?: (msg: string, waitSec: number) => void
    isStopping?: () => boolean
  }): Promise<(actualVideoSec?: number) => void> {
    const {
      scanId,
      scanTitle = scanId,
      apiKey,
      keyIdx = 1,
      modelId,
      slot = 0,
      operation,
      videoSeconds = 60,
      rpd = 500,
      isVerify = false,
      verifyLockId,
      isChunk = false,
      chunkLockId,
      onWait,
      isStopping,
    } = opts

    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot, keyIdx)

    // Per user rule: Do NOT block models when RPD is reached — allow request to proceed anyway!
    if (this.isModelExhausted(apiKey, modelId, rpd)) {
      lane.isExhausted = true
    }

    return new Promise<(actualVideoSec?: number, cooldownOverrideMs?: number) => void>((resolve, reject) => {
      const tryAcquireOrQueue = async () => {
        if (isStopping && isStopping()) {
          reject(new Error('Stop requested — lane acquisition cancelled'))
          return
        }

        if (this.isModelExhausted(apiKey, modelId, rpd)) {
          lane.isExhausted = true
        }

        const now = Date.now()

        // Verifier-specific batch rules & 429 priority retry locks (Coordinator level):
        if (isVerify) {
          const vmState = this.getVerifyModelState(apiKey, modelId)

          // 1. If 429 Priority Retry Lock is active on this model:
          // ONLY the request holding this retry lock is permitted through!
          if (vmState.retryLockId !== null) {
            // Auto-clear stale lock if cooldown has passed by > 5 seconds
            if (now > vmState.cooldownUntil + 5000) {
              vmState.retryLockId = null
              vmState.retryLockSlot = null
            } else {
              const isAuthorizedRetry =
                (verifyLockId && verifyLockId === vmState.retryLockId) ||
                slot === vmState.retryLockSlot
              if (!isAuthorizedRetry) {
                const waitMs = Math.max(1000, vmState.cooldownUntil - now)
                setTimeout(() => {
                  if (isStopping && isStopping()) {
                    reject(new Error('Stop requested while waiting for 429 priority retry'))
                    return
                  }
                  void tryAcquireOrQueue()
                }, Math.min(2000, waitMs))
                return
              }
            }
          }

          // 2. Check Verifier 1-minute cooldown (from 3 successes or 429 rate limit):
          if (vmState.cooldownUntil > now) {
            const waitMs = vmState.cooldownUntil - now
            const waitSec = Math.ceil(waitMs / 1000)
            const cdReason = vmState.retryLockId !== null
              ? '429 rate limit cooldown'
              : 'mandatory 30s cooldown after 3 successful requests'
            onWait?.(
              `[Global Coordinator] Key ${lane.keyIdx} · ${displayModelName(modelId)} in ${cdReason} (${waitSec}s remaining). Prepared clips ready in memory...`,
              waitSec,
            )
            setTimeout(() => {
              if (isStopping && isStopping()) {
                reject(new Error('Stop requested during verifier cooldown'))
                return
              }
              void tryAcquireOrQueue()
            }, waitMs + 50)
            return
          }
        }

        // Chunk-specific batch rules & 429 priority retry locks (Coordinator level):
        if (isChunk) {
          const cmState = this.getChunkModelState(apiKey, modelId)

          // 1. If 429 Priority Retry Lock is active on this model:
          // ONLY the request holding this retry lock is permitted through!
          if (cmState.retryLockId !== null) {
            // Auto-clear stale lock if cooldown has passed by > 5 seconds
            if (now > cmState.cooldownUntil + 5000) {
              cmState.retryLockId = null
            } else {
              const isAuthorizedRetry = Boolean(chunkLockId && chunkLockId === cmState.retryLockId)
              if (!isAuthorizedRetry) {
                const waitMs = Math.max(1000, cmState.cooldownUntil - now)
                setTimeout(() => {
                  if (isStopping && isStopping()) {
                    reject(new Error('Stop requested while waiting for 429 priority chunk retry'))
                    return
                  }
                  void tryAcquireOrQueue()
                }, Math.min(2000, waitMs))
                return
              }
            }
          }

          // 2. Check Chunk 30-second cooldown (from 1 success or 429 rate limit):
          if (cmState.cooldownUntil > now) {
            const waitMs = cmState.cooldownUntil - now
            const waitSec = Math.ceil(waitMs / 1000)
            const cdReason = cmState.retryLockId !== null
              ? '429 rate limit cooldown'
              : 'mandatory 30s cooldown (RPM/TPM protection)'
            onWait?.(
              `[Global Coordinator] Key ${lane.keyIdx} · ${displayModelName(modelId)} in ${cdReason} (${waitSec}s remaining). Next chunk locked & preparing in background...`,
              waitSec,
            )
            setTimeout(() => {
              if (isStopping && isStopping()) {
                reject(new Error('Stop requested during chunk cooldown'))
                return
              }
              void tryAcquireOrQueue()
            }, waitMs + 50)
            return
          }
        }

        // KEY-LEVEL EXCLUSIVE LOCK CHECK:
        // If THIS API key is currently in use by ANY model/operation across the whole application,
        // it MUST wait until that request completely finishes to protect TPM and RPM limits.
        const kh = lane.keyHash
        const activeKeyLock = this.activeKeyLocks.get(kh)
        if (activeKeyLock) {
          const waitMsg = `[Global Coordinator] Key ${lane.keyIdx} is currently busy with ${displayModelName(activeKeyLock.modelId)} (${activeKeyLock.operation}). Key-level lock active to protect TPM/RPM.`
          onWait?.(waitMsg, 2)
          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested while waiting for key-level exclusive lock'))
              return
            }
            void tryAcquireOrQueue()
          }, 1000)
          return
        }

        // KEY-LEVEL PACING BUFFER:
        const keyNextFree = this.keyNextFreeAt.get(kh) || 0
        if (keyNextFree > now) {
          const waitMs = keyNextFree - now
          const waitSec = Math.ceil(waitMs / 1000)
          onWait?.(
            `[Global Coordinator] Key ${lane.keyIdx} pacing buffer (${waitSec}s remaining). Pacing request...`,
            waitSec,
          )
          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested during key pacing buffer'))
              return
            }
            void tryAcquireOrQueue()
          }, waitMs + 20)
          return
        }

        // If lane is currently active in another scan OR there are earlier waiters queued
        const hasOtherActive = lane.activeScanId !== null
        const isQueuedBehindOthers = lane.waiters.length > 0 && lane.waiters[0]?.scanId !== scanId

        if (hasOtherActive || isQueuedBehindOthers) {
          const waitMsg = hasOtherActive
            ? `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} is busy in Scan "${lane.activeScanTitle || lane.activeScanId}" (${lane.activeOperation || 'working'}). Waiting for lane to become free...`
            : `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} is queued behind other scans. Waiting turn...`

          onWait?.(waitMsg, 5)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Check 429 Cooldown
        if (lane.cooldownUntil > now) {
          const waitMs = lane.cooldownUntil - now
          const waitSec = Math.ceil(waitMs / 1000)
          onWait?.(
            `[Global Coordinator] Key ${lane.keyIdx} · ${displayModelName(modelId)} is in 429 rate cooldown (${waitSec}s remaining). Waiting for rate limit reset...`,
            waitSec,
          )

          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested during cooldown'))
              return
            }
            void this.processNext(lane)
          }, waitMs + 50)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Check TPM pacing interval
        if (lane.nextFreeAt > now) {
          const waitMs = lane.nextFreeAt - now
          const waitSec = Math.ceil(waitMs / 1000)
          onWait?.(
            `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} pacing wait (${waitSec}s for TPM quota). Pacing request...`,
            waitSec,
          )

          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested during pacing wait'))
              return
            }
            void this.processNext(lane)
          }, waitMs + 20)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Re-read lane's cooldown and global pause state before acquiring exclusively
        const checkNow = Date.now()
        if (lane.cooldownUntil > checkNow || lane.nextFreeAt > checkNow || this.activeKeyLocks.has(kh)) {
          void tryAcquireOrQueue()
          return
        }

        // Lock is free! Acquire exclusively now both on lane and at KEY level.
        lane.activeScanId = scanId
        lane.activeScanTitle = scanTitle
        lane.activeOperation = operation
        lane.activeSince = Date.now()

        this.activeKeyLocks.set(kh, {
          keyHash: kh,
          keyIdx: lane.keyIdx,
          scanId,
          scanTitle,
          modelId,
          operation,
          slot,
          activeSince: Date.now(),
        })

        const releaseFn = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
          this.releaseLane(lane, actualVideoSec ?? videoSeconds, cooldownOverrideMs)
        }

        resolve(releaseFn)
      }

      void tryAcquireOrQueue()
    })
  }

  /**
   * Dynamically search across multiple candidate lanes (different API keys and/or models).
   * 1. If any candidate lane is immediately free (not in use, not cooling, not pacing, not exhausted),
   *    grab that free lane instantly with ZERO wait!
   * 2. If all candidate lanes are busy, poll/re-evaluate EVERY 1 SECOND across ALL candidates.
   *    As soon as ANY lane (e.g. Key 3 · 3.8, or Key 2 · 3.6) frees up first,
   *    immediately shift to that newly freed lane and acquire it!
   */
  public async acquireFirstAvailableLane(opts: {
    scanId: string
    scanTitle?: string
    candidates: CandidateLane[]
    operation: string
    videoSeconds?: number
    onWait?: (msg: string, waitSec: number, candidateSummary: string) => void
    isStopping?: () => boolean
  }): Promise<{
    selected: CandidateLane
    release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void
  }> {
    const {
      scanId,
      scanTitle = scanId,
      candidates,
      operation,
      videoSeconds = 60,
      onWait,
      isStopping,
    } = opts

    if (!candidates || candidates.length === 0) {
      throw new Error('No candidate lanes provided for execution')
    }

    let lastLoggedWaitMsg = ''

    while (true) {
      if (isStopping && isStopping()) {
        throw new Error('Stop requested — lane acquisition cancelled')
      }

      const now = Date.now()

      this.checkDayRollover()

      // 1. Prioritize non-exhausted candidates, but if all reached quota, keep all candidates eligible
      // per user rule: "rpd khtm hone pr un model ko block mat krna req fir bhi kr ske"
      const nonExhausted = candidates.filter((c) => {
        return !this.isModelExhausted(c.apiKey, c.modelId, c.rpd || 500)
      })
      const availableCandidates = nonExhausted.length > 0 ? nonExhausted : candidates

      // Sort candidates to prioritize same-key multi-model usage before switching keys:
      // Group by keyIdx ascending, and test available models on the current key first
      const sortedCandidates = [...availableCandidates].sort((a, b) => {
        if (a.keyIdx !== b.keyIdx) return a.keyIdx - b.keyIdx
        const aUsage = getModelUsage(a.modelId, a.apiKey)
        const bUsage = getModelUsage(b.modelId, b.apiKey)
        return aUsage - bUsage
      })

      // 2. Check for immediately FREE lanes (no active scan, key not locked, no cooldown, no pacing wait, no waiters)
      for (const cand of sortedCandidates) {
        const lane = this.getOrCreateLane(cand.apiKey, cand.modelId, cand.slot || 0, cand.keyIdx)
        const candKeyHash = apiKeyHash(cand.apiKey)
        const isKeyFree = !this.activeKeyLocks.has(candKeyHash) && (this.keyNextFreeAt.get(candKeyHash) || 0) <= now
        const isFree =
          isKeyFree &&
          lane.activeScanId === null &&
          lane.cooldownUntil <= now &&
          lane.nextFreeAt <= now &&
          lane.waiters.length === 0

        if (isFree) {
          // Immediately grab this free lane!
          lane.activeScanId = scanId
          lane.activeScanTitle = scanTitle
          lane.activeOperation = operation
          lane.activeSince = Date.now()

          this.activeKeyLocks.set(candKeyHash, {
            keyHash: candKeyHash,
            keyIdx: lane.keyIdx,
            scanId,
            scanTitle,
            modelId: cand.modelId,
            operation,
            slot: cand.slot || 0,
            activeSince: Date.now(),
          })

          const release = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
            this.releaseLane(lane, actualVideoSec ?? videoSeconds, cooldownOverrideMs)
          }

          return { selected: cand, release }
        }
      }

      // 3. None are immediately free. Calculate estimated shortest wait time across all candidate lanes
      const waits = sortedCandidates.map((c) => {
        const lane = this.getOrCreateLane(c.apiKey, c.modelId, c.slot || 0, c.keyIdx)
        const cdWait = Math.max(0, lane.cooldownUntil - now)
        const paceWait = Math.max(0, lane.nextFreeAt - now)
        const activeWait = lane.activeScanId ? 4000 : 0
        const totalWait = Math.max(cdWait, paceWait, activeWait)
        return { c, lane, totalWait }
      })

      waits.sort((a, b) => a.totalWait - b.totalWait)
      const shortest = waits[0]
      const waitSec = Math.max(1, Math.ceil(shortest.totalWait / 1000))

      const candidateSummary = availableCandidates
        .map((c) => `Key ${c.keyIdx} (${c.modelId})`)
        .slice(0, 4)
        .join(', ')

      const waitMsg = `[Global Coordinator] All candidate lanes busy (${candidateSummary}${availableCandidates.length > 4 ? '...' : ''}). Re-checking every 1s for the first available lane (next free ~${waitSec}s)...`

      if (waitMsg !== lastLoggedWaitMsg) {
        lastLoggedWaitMsg = waitMsg
        onWait?.(waitMsg, waitSec, candidateSummary)
      }

      // 4. Sleep for 1 second (1000ms), then re-evaluate the whole pool immediately!
      await new Promise((r) => setTimeout(r, 1000))
    }
  }

  private releaseLane(lane: GlobalLaneState, videoSeconds: number, cooldownOverrideMs?: number) {
    const paceMs = cooldownOverrideMs !== undefined
      ? cooldownOverrideMs
      : (videoSeconds >= 50 ? CHUNK_COOLDOWN_MS : pacingIntervalMs(videoSeconds))
    const now = Date.now()
    lane.lastCompletedAt = now
    lane.nextFreeAt = now + paceMs
    lane.cooldownUntil = Math.max(lane.cooldownUntil, now + paceMs)
    lane.lastOperation = lane.activeOperation
    lane.lastOperationVideoSec = videoSeconds
    lane.activeScanId = null
    lane.activeScanTitle = null
    lane.activeOperation = null
    lane.activeSince = null

    // Release key-level exclusive lock
    this.activeKeyLocks.delete(lane.keyHash)
    // Enforce key-level minimum pacing buffer (minimum 4s spacing between consecutive requests on this key)
    this.keyNextFreeAt.set(lane.keyHash, Math.max(this.keyNextFreeAt.get(lane.keyHash) || 0, now + 4_000))

    // Process next waiter in queue after pacing expires (or schedule it)
    if (lane.waiters.length > 0) {
      setTimeout(() => {
        void this.processNext(lane)
      }, paceMs + 20)
    }
  }

  private async processNext(lane: GlobalLaneState) {
    if (lane.activeScanId !== null) return // still busy
    if (this.activeKeyLocks.has(lane.keyHash)) {
      // Key still locked by another request, retry check after 500ms
      setTimeout(() => {
        void this.processNext(lane)
      }, 500)
      return
    }

    while (lane.waiters.length > 0) {
      const next = lane.waiters.shift()
      if (!next) break

      if (next.isStopping && next.isStopping()) {
        next.reject(new Error('Stop requested while queued'))
        continue
      }

      const now = Date.now()
      if (lane.cooldownUntil > now) {
        // Still in cooldown, put back and wait
        lane.waiters.unshift(next)
        const waitMs = lane.cooldownUntil - now
        setTimeout(() => {
          void this.processNext(lane)
        }, waitMs + 50)
        return
      }

      if (lane.nextFreeAt > now) {
        // Still in pacing interval, put back and wait
        lane.waiters.unshift(next)
        const waitMs = lane.nextFreeAt - now
        setTimeout(() => {
          void this.processNext(lane)
        }, waitMs + 20)
        return
      }

      const keyNextFree = this.keyNextFreeAt.get(lane.keyHash) || 0
      if (keyNextFree > now) {
        lane.waiters.unshift(next)
        const waitMs = keyNextFree - now
        setTimeout(() => {
          void this.processNext(lane)
        }, waitMs + 20)
        return
      }

      // Lane and key are free to take!
      lane.activeScanId = next.scanId
      lane.activeScanTitle = next.scanTitle
      lane.activeOperation = next.operation
      lane.activeSince = Date.now()

      this.activeKeyLocks.set(lane.keyHash, {
        keyHash: lane.keyHash,
        keyIdx: lane.keyIdx,
        scanId: next.scanId,
        scanTitle: next.scanTitle,
        modelId: lane.modelId,
        operation: next.operation,
        slot: lane.slot,
        activeSince: Date.now(),
      })

      const releaseFn = (actualVideoSec?: number) => {
        this.releaseLane(lane, actualVideoSec ?? 60)
      }

      next.resolve(releaseFn)
      return
    }
  }

  /** Record successful request on this lane — resets consecutive error counters */
  public recordSuccess(apiKey: string, modelId: string, slot: number = 0) {
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    lane.consecutiveQuotaErrors = 0
    delete lane.firstRpdErrorAt

    if (this.activePauseClass !== null) {
      console.log(`[Global Coordinator] Provider probe succeeded! Resuming normal ramp-up across all keys.`)
      this.activePauseClass = null
      this.isProbeInFlight = false
      this.probeLaneKey = null
      this.breakerBackoffIndex = 0
      this.breakerEvents = []
    }
  }

  /** Record breaker failure — keeps key-level diagnostics without freezing healthy keys/models */
  public recordBreakerFailure(
    apiKey: string,
    modelId: string,
    failureClass: 'rate' | 'overloaded' | 'rpd',
    recordedUsage: number,
    rpdCap: number,
  ) {
    if (recordedUsage >= rpdCap) return
    const now = Date.now()
    const kh = apiKeyHash(apiKey)
    this.breakerEvents.push({ time: now, keyHash: kh, failureClass })
    this.breakerEvents = this.breakerEvents.filter((e) => now - e.time <= 60_000)
  }

  /** Report a 429 Rate Limit error on a lane across the entire app */
  public reportRateLimit(apiKey: string, modelId: string, cooldownMs: number = RATE_COOLDOWN_MS, slot: number = 0) {
    const kh = apiKeyHash(apiKey)
    const now = Date.now()
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    const coolUntil = now + cooldownMs
    lane.cooldownUntil = Math.max(lane.cooldownUntil, coolUntil)

    // Key-level pacing buffer: give the API key at least a 5s breather
    this.keyNextFreeAt.set(kh, Math.max(this.keyNextFreeAt.get(kh) || 0, now + 5_000))

    // Cooldown only slots for THIS specific model on this API key.
    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.cooldownUntil = Math.max(other.cooldownUntil, coolUntil)
      }
    }

    const spikeKey = `${kh}|${modelId}`
    if (now >= (this.rateSpikeLogUntil.get(spikeKey) || 0)) {
      this.rateSpikeLogUntil.set(spikeKey, coolUntil)
      console.log(
        `[Global Coordinator] 429 Lock on ${modelId} (Key ${lane.keyIdx}, Hash: ${kh.slice(0, 6)}): Locked for ${(cooldownMs / 1000).toFixed(1)}s (until ${new Date(coolUntil).toLocaleTimeString()}). All other requests blocked on this model.`,
      )
    }
  }

  /**
   * Report 429 rate limit on a VERIFIER request.
   * 1. Sets an exclusive retry lock for this request/slot on (key × model).
   * 2. Sets a dynamic cooldown across all slots of this model (Google delay + safety buffer).
   * 3. Guarantees that when cooldown expires, ONLY this request is allowed to retry first!
   */
  public reportVerifyRateLimit(
    apiKey: string,
    modelId: string,
    slot: number,
    verifyLockId?: string,
    cooldownMs: number = RATE_COOLDOWN_MS,
  ): void {
    const kh = apiKeyHash(apiKey)
    const now = Date.now()
    const coolUntil = now + cooldownMs
    const vmState = this.getVerifyModelState(apiKey, modelId)

    vmState.retryLockSlot = slot
    vmState.retryLockId = verifyLockId || `slot-${slot}`
    vmState.cooldownUntil = coolUntil

    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.cooldownUntil = Math.max(other.cooldownUntil, coolUntil)
      }
    }

    console.log(
      `[Global Coordinator] [429 VERIFIER LOCK] ${modelId} (Key hash: ${kh.slice(0, 6)}, Slot ${slot}): Exclusive priority retry lock held for "${vmState.retryLockId}". ${(cooldownMs / 1000).toFixed(1)}s lock applied (until ${new Date(coolUntil).toLocaleTimeString()}).`,
    )
  }

  /**
   * Record a successful VERIFY request on this (key × model).
   * 1. If this was the 429-retry request, clear the exclusive retry lock and set successCount = 1 (releasing remaining slots).
   * 2. If normal request, increment successCount.
   * 3. When count hits 3, trigger a mandatory 1-minute cooldown (60s) on this (key × model) across ALL slots, and reset count to 0!
   */
  public recordVerifySuccess(
    apiKey: string,
    modelId: string,
    slot: number = 0,
    verifyLockId?: string,
  ): {
    cooldownTriggered: boolean
    cooldownUntil: number
    currentCount: number
    retryCleared: boolean
  } {
    const kh = apiKeyHash(apiKey)
    const vmState = this.getVerifyModelState(apiKey, modelId)
    this.recordSuccess(apiKey, modelId, slot)

    // Check if this cleared a pending 429 retry lock
    if (vmState.retryLockId !== null) {
      if ((verifyLockId && verifyLockId === vmState.retryLockId) || slot === vmState.retryLockSlot) {
        vmState.retryLockId = null
        vmState.retryLockSlot = null
        vmState.successCount = 1 // 1 of 3 in batch completed
        console.log(`[Global Coordinator] 429 priority retry cleared on ${modelId} (Key hash: ${kh.slice(0, 6)}). Releasing remaining slots (1/3 in batch).`)
        return {
          cooldownTriggered: false,
          cooldownUntil: 0,
          currentCount: 1,
          retryCleared: true,
        }
      }
    }

    vmState.successCount = (vmState.successCount || 0) + 1

    if (vmState.successCount >= 3) {
      vmState.successCount = 0
      const now = Date.now()
      const cooldownMs = 30_000 // 30 seconds (reduced from 60s per user instruction)
      const coolUntil = now + cooldownMs
      vmState.cooldownUntil = coolUntil

      for (const other of this.lanes.values()) {
        if (other.keyHash === kh && other.modelId === modelId) {
          other.cooldownUntil = Math.max(other.cooldownUntil, coolUntil)
        }
      }

      console.log(
        `[Global Coordinator] 3 successful verify requests completed on ${modelId} (Key hash: ${kh.slice(0, 6)}). Enforcing mandatory 30-second cooldown across all slots.`,
      )
      return {
        cooldownTriggered: true,
        cooldownUntil: coolUntil,
        currentCount: 3,
        retryCleared: false,
      }
    }

    return {
      cooldownTriggered: false,
      cooldownUntil: 0,
      currentCount: vmState.successCount,
      retryCleared: false,
    }
  }

  /** Clear verify retry lock if request fails permanently or is cancelled */
  public clearVerifyRetryLock(apiKey: string, modelId: string, verifyLockId?: string): void {
    const vmState = this.getVerifyModelState(apiKey, modelId)
    if (vmState.retryLockId && (!verifyLockId || vmState.retryLockId === verifyLockId)) {
      vmState.retryLockId = null
      vmState.retryLockSlot = null
    }
  }

  /**
   * Report 429 rate limit on a CHUNK mapping request.
   * 1. Sets an exclusive retry lock for this specific chunk on (key × model).
   * 2. Sets a dynamic cooldown across this model (Google delay + safety buffer).
   * 3. Guarantees that when cooldown expires, ONLY this request is allowed to retry first!
   */
  public reportChunkRateLimit(
    apiKey: string,
    modelId: string,
    chunkLockId?: string,
    cooldownMs: number = 60_000,
  ): void {
    const kh = apiKeyHash(apiKey)
    const now = Date.now()
    const coolUntil = now + cooldownMs
    const cmState = this.getChunkModelState(apiKey, modelId)

    cmState.retryLockId = chunkLockId || 'chunk-retry'
    cmState.cooldownUntil = coolUntil

    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.cooldownUntil = Math.max(other.cooldownUntil, coolUntil)
      }
    }

    console.log(
      `[Global Coordinator] [429 CHUNK LOCK] ${modelId} (Key hash: ${kh.slice(0, 6)}): Exclusive priority retry lock held for "${cmState.retryLockId}". ${(cooldownMs / 1000).toFixed(1)}s lock applied (until ${new Date(coolUntil).toLocaleTimeString()}).`,
    )
  }

  /**
   * Record a successful CHUNK mapping request on this (key × model).
   * 1. If this was the 429-retry request, clear the exclusive retry lock.
   * 2. Every 1 successful chunk mapping request gets a mandatory 1-minute (60s) cooldown
   *    on this (key × model) across ALL slots due to the ~200k+ tokens TPM limit!
   */
  public recordChunkSuccess(
    apiKey: string,
    modelId: string,
    chunkLockId?: string,
  ): {
    cooldownTriggered: boolean
    cooldownUntil: number
    retryCleared: boolean
  } {
    const kh = apiKeyHash(apiKey)
    const cmState = this.getChunkModelState(apiKey, modelId)
    this.recordSuccess(apiKey, modelId, 0)

    let retryCleared = false
    if (cmState.retryLockId !== null) {
      if (!chunkLockId || chunkLockId === cmState.retryLockId) {
        cmState.retryLockId = null
        retryCleared = true
        console.log(`[Global Coordinator] Chunk 429 priority retry cleared on ${modelId} (Key hash: ${kh.slice(0, 6)}).`)
      }
    }

    const now = Date.now()
    const cooldownMs = 30_000 // 30 seconds mandatory cooldown after 1 request (RPM/TPM protection)
    const coolUntil = now + cooldownMs
    cmState.cooldownUntil = coolUntil

    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.cooldownUntil = Math.max(other.cooldownUntil, coolUntil)
      }
    }

    console.log(
      `[Global Coordinator] 1 chunk mapping request completed on ${modelId} (Key hash: ${kh.slice(0, 6)}). Enforcing mandatory 30-second cooldown (RPM/TPM protection). Next chunk locked and preparing in background.`,
    )

    return {
      cooldownTriggered: true,
      cooldownUntil: coolUntil,
      retryCleared,
    }
  }

  /** Clear chunk retry lock if request fails permanently or is cancelled */
  public clearChunkRetryLock(apiKey: string, modelId: string, chunkLockId?: string): void {
    const cmState = this.getChunkModelState(apiKey, modelId)
    if (cmState.retryLockId && (!chunkLockId || cmState.retryLockId === chunkLockId)) {
      cmState.retryLockId = null
    }
  }

  /**
   * Smart Quota/Rate Limit handler:
   * 1. Never disable the entire API key! Only isolate this specific model on this key.
   * 2. Temporary rate limit spikes (429 RPM/TPM) or server high demand (503) are NEVER daily exhausted!
   *    They are put in cooldown for the exact Google delay + 5s buffer lock, and retried.
   * 3. Mark exhausted ONLY if (a) used >= rpdCap, or (b) error.kind === 'rpd' from structured PerDay quotaId
   *    AND this lane had a previous 'rpd' error at least 10 minutes earlier.
   */
  public handleQuotaOrRateError(
    apiKey: string,
    modelId: string,
    slot: number = 0,
    rpdCap: number = 20,
    errorOrKind?: unknown,
    cooldownMsOverride?: number,
    keyIdx?: number,
  ): {
    action: 'cooldown' | 'exhausted'
    waitSec: number
    reason: string
  } {
    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot, keyIdx)
    const used = getModelUsage(modelId, apiKey)
    const effectiveCooldownMs = cooldownMsOverride !== undefined && cooldownMsOverride > 0
      ? cooldownMsOverride
      : CHUNK_COOLDOWN_MS

    const errKind: string =
      typeof errorOrKind === 'object' && errorOrKind !== null && 'kind' in (errorOrKind as Record<string, unknown>)
        ? String((errorOrKind as { kind: string }).kind)
        : typeof errorOrKind === 'string'
          ? errorOrKind
          : errorOrKind === true
            ? 'rpd'
            : 'rate'

    // 1. FINAL DECISION: Only if actual recorded usage has reached or exceeded the setting quota cap AND Google did not specify a short retry window is it truly exhausted!
    // If Google specifically returns a short retryDelay (<= 5 min), Google's API Gateway is saying this window will refill and can be retried!
    if (used >= rpdCap && (!cooldownMsOverride || cooldownMsOverride > 300_000)) {
      this.reportExhausted(apiKey, modelId, slot, rpdCap)
      return {
        action: 'exhausted',
        waitSec: 0,
        reason: `Daily setting quota limit reached (${used}/${rpdCap} RPD) on ${modelId} (Key ${lane.keyIdx || keyIdx || 1})`,
      }
    }

    // 2. If used < rpdCap, this is strictly a temporary rate/quota spike (429 / RPM / TPM / 503).
    // The final exhaustion decision belongs ONLY to the setting quota (used >= rpdCap).
    // NEVER mark daily exhausted when recorded usage is below the setting quota!
    lane.isExhausted = false
    delete lane.firstRpdErrorAt
    const failureClass: 'rate' | 'overloaded' = errKind === 'overloaded' ? 'overloaded' : 'rate'
    this.reportRateLimit(apiKey, modelId, effectiveCooldownMs, slot)
    this.recordBreakerFailure(apiKey, modelId, failureClass, used, rpdCap)
    const waitSec = Math.ceil(effectiveCooldownMs / 1000)

    const label = failureClass === 'overloaded' ? '503 overloaded' : 'Rate/RPM/TPM limit spike'
    return {
      action: 'cooldown',
      waitSec,
      reason: `${label} on ${modelId} (Key ${lane.keyIdx || keyIdx || 1}, used ${used}/${rpdCap} RPD) — locked for ${waitSec}s (+5s buffer) before retry`,
    }
  }

  /** Report that a model's daily quota has been exhausted across the entire app */
  public reportExhausted(apiKey: string, modelId: string, slot: number = 0, rpdCap: number = 20) {
    const used = getModelUsage(modelId, apiKey)
    if (used < rpdCap) {
      console.warn(`[Global Coordinator] Ignored reportExhausted for Key ${slot} (${modelId}) because recorded usage (${used}) has not reached setting quota (${rpdCap} RPD).`)
      return
    }
    const kh = apiKeyHash(apiKey)
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    lane.isExhausted = true

    // Mark ALL slots for this model on this API key as exhausted!
    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.isExhausted = true
        // Reject all queued waiters on this exhausted lane immediately with an error so they don't wait forever!
        while (other.waiters.length > 0) {
          const waiter = other.waiters.shift()
          if (waiter) {
            waiter.reject(new Error(`[Global Coordinator] Key ${other.keyIdx} (${modelId}) daily setting quota (${rpdCap} RPD) reached`))
          }
        }
      }
    }

    const day = geminiUsageDay()
    const logKey = `${day}|${kh}|${modelId}`
    if (!this.exhaustedLogSet.has(logKey)) {
      this.exhaustedLogSet.add(logKey)
      console.log(`[Global Coordinator] Key ${lane.keyIdx} (${modelId}) daily setting quota (${rpdCap} RPD) reached for today (${day}).`)
    }

    // Persist to counters.json so subsequent workers/processes know this model is quota-capped today
    try {
      setModelExhausted(modelId, apiKey, rpdCap)
    } catch {}
  }

  /** Get snapshot summary of all active/busy lanes across the application */
  public getSnapshot(): Array<{
    laneKey: string
    keyIdx: number
    modelId: string
    activeScanId: string | null
    activeScanTitle: string | null
    activeOperation: string | null
    waitingCount: number
    cooling: boolean
    pacingWaitSec: number
  }> {
    const now = Date.now()
    return Array.from(this.lanes.values()).map((l) => ({
      laneKey: l.laneKey,
      keyIdx: l.keyIdx,
      modelId: l.modelId,
      activeScanId: l.activeScanId,
      activeScanTitle: l.activeScanTitle,
      activeOperation: l.activeOperation,
      waitingCount: l.waiters.length,
      cooling: l.cooldownUntil > now,
      pacingWaitSec: Math.max(0, Math.ceil((Math.max(l.nextFreeAt, l.cooldownUntil) - now) / 1000)),
    }))
  }
}

// Global Singleton instance shared across the entire Node.js server process
const globalCoordinatorKey = Symbol.for('__global_gemini_coordinator__')
const globalObj = globalThis as unknown as { [globalCoordinatorKey]?: GlobalGeminiCoordinator }

if (!globalObj[globalCoordinatorKey]) {
  globalObj[globalCoordinatorKey] = new GlobalGeminiCoordinator()
}

export const globalGeminiCoordinator = globalObj[globalCoordinatorKey]!
