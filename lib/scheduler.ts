import path from 'node:path'
import fs from 'node:fs'
import type { Scan, ChunkState, ChunkMatch, CandidateGroup, ShortSegmentState, ScanReport, ShortCoverage, ChunkTokenUsage, LogEntry } from './types'
import { globalOrchestrator } from './orchestrator-core'
import {
  MODEL_POOL,
  CHUNK_MODEL_POOL,
  VERIFY_MODEL_POOL,
  RESCAN_MODEL_POOL,
  MAX_QUALITY_RETRIES,
  MODEL_MIN_INTERVAL_MS,
  RATE_COOLDOWN_MS,
  CHUNK_COOLDOWN_MS,
  CHUNK_SECONDS,
  pacingIntervalMs,
  displayModelName,
  type ModelSpec,
} from './models'
import {
  getScan,
  saveScan,
  addLog,
  getModelUsage,
  incrementModelUsage,
  setModelExhausted,
  checkDailyReset,
  geminiUsageDay,
  scanMediaDir,
  listScans,
  cleanseStartupQuotas,
} from './store'
import { chunkPath, cleanupClips, extractClipPrecise, extractSegment, sanitizeVideoMute, segmentPath } from './ffmpeg'
import { chunkOverlapsSegRange } from './segment-range'
import {
  ensureIndex,
  createIndexTask,
  pollTaskUntilReady,
  fetchVideoEmbeddings,
  loadEmbeddings,
  saveEmbeddings,
  computePrefilterChunks,
  TL_SIMILARITY_THRESHOLD,
} from './twelvelabs'
import {
  getClient,
  uploadVideo,
  deleteFileQuiet,
  cleanupOrphanedGeminiFiles,
  mapChunkRequest,
  CHUNK_MAP_TIMEOUT_MS,
  CHUNK_MAP_PROMPT,
  CHUNK_MAP_SANITIZED_PROMPT,
  parseChunkMatches,
  verifyRequest,
  rescanRequest,
  parseVerdict,
  parseRescanMatch,
  isSuspiciousChunkOutput,
  GeminiError,
  classifyError,
  extractGoogleRetryDelayMs,
  calculateEffectiveCooldownMs,
} from './gemini'
import { applyGroupMatches, bestRejectedCandidate, groupMatchOrigin, originTag, sameShortSegment } from './candidate-pick'
import { computeShortCoverage, coverageLine } from './short-coverage'
import { globalGeminiCoordinator, CandidateLane } from './global-gemini-coordinator'
import { globalChunkScanCoordinator, registerSchedulerChunkChecker } from './chunk-scan-coordinator'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Max real attempts per chunk before it is marked failed (infra/transient/rate errors never count). */
const MAX_CHUNK_ATTEMPTS = 6

/** One API key lane (1-20). Gemini Files API uploads are PER KEY,
 *  so each lane keeps its own uploaded short-SEGMENT URIs (one per minute). */
interface KeyLane {
  idx: number
  apiKey: string
  ai: GoogleGenAI
  /** uploaded short-segment URIs keyed by segment index */
  segUris: Map<number, string>
  /** per-segment in-flight upload locks so two workers never double-upload */
  segUriPromises: Map<number, Promise<string>>
  /** PIPELINING: pre-cut + pre-uploaded movie chunks on THIS key —
   *  chunkIndex -> in-flight/finished upload. Consumed (removed) on use,
   *  because every chunk upload is one-shot (deleted after its request). */
  chunkUploads: Map<number, Promise<{ uri: string; name: string }>>
  /** verifier groups currently in flight on THIS key (capped by VERIFY_PER_LANE) */
  verifyActive: number
  /** verifier groups in flight per model on THIS key (capped by VERIFY_CONCURRENCY_PER_MODEL) */
  verifyActiveByModel: Map<string, number>
}

/** Concurrency per model on each API key: 3 requests per model slot.
 * Across 16 keys × 2 verify models = 96 parallel verify requests across the pool!
 * (Up to 102 parallel requests if 17 keys are active).
 * 2 models × 3 requests = 6 simultaneous requests per key. */
const VERIFY_CONCURRENCY_PER_MODEL = 3

/** Max verifier groups in flight per key: 2 models × 3 requests = 6 simultaneous requests per key. */
const VERIFY_PER_LANE = VERIFY_MODEL_POOL.length * VERIFY_CONCURRENCY_PER_MODEL

interface Job {
  scan: Scan
  lanes: KeyLane[]
  resume: boolean
  /** PREFETCH OWNERSHIP: chunkIndex -> lane idx that pre-uploaded it. One lane
   *  per chunk, so the same chunk is never uploaded on 5 keys at once. Workers
   *  pull "their" pre-uploaded chunks first (lane affinity). */
  prefetchOwner: Map<number, number>
  /** OPTIONAL Twelve Labs key — enables the embedding pre-filter. null = normal full scan. */
  tlKey: string | null
  /** the short segment (minute) currently being scanned — workers read this */
  seg: ShortSegmentState | null
  /** chunk queue (indexes) for the CURRENT segment */
  queue: number[]
  inFlight: Set<number>
  /** verification phase: candidate-group queue (indexes into scan.candidateGroups) */
  verifyQueue: number[]
  verifyInFlight: Set<number>
  /** rescan phase: groups whose normal candidates were all rejected and are waiting for rescan */
  rescanQueue: number[]
  /** true once ALL minutes' chunk workers have drained — the GLOBAL verify
   *  workers keep waiting for new candidates until this flips to true. */
  chunkPhaseDone: boolean
  /** set when a rejected group revived early-stop-skipped chunks — triggers an extra scan pass */
  earlyStopRevived: boolean
  laneFirstSuccess: Set<number>
  stopping: boolean
  /** pacing state keyed by `${laneIdx}|${modelId}`: earliest time the next request may be sent.
   *  Set after every request from its actual token size so every model runs at full TPM capacity. */
  nextFreeAt: Record<string, number>
  cooldownUntil: Record<string, number>
  /** buffer cooldown per chunkIndex to prevent rapid-fire token spikes when an attempt fails */
  chunkRetryCooldown: Map<number, { availableAt: number; reason: string; failedModel: string; failedKey: number }>
  dirty: boolean
  saverTimer: ReturnType<typeof setInterval> | null
}

/** Pacing buffer delay (ms) applied when a chunk fails, preventing rapid-fire cascading token spikes */
export const CHUNK_RETRY_BUFFER_MS = 6_000

/** Max attempts (non-rate errors) per candidate group before it is kept as unverified. */
const MAX_GROUP_ATTEMPTS = 4

/** Format seconds as mm:ss.mmm for logs. */
function ts(sec: number): string {
  const m = Math.floor(sec / 60)
  const s = sec - m * 60
  return `${String(m).padStart(2, '0')}:${s.toFixed(3).padStart(6, '0')}`
}
const fmtTime = ts

/** ABSOLUTE original-movie window of a chunk: chunks cover ONLY the confirmed
 *  trim range, so every chunk's absolute start = trimStart + index * 60. */
function chunkAbsWindow(scan: Scan, chunkIndex: number): { start: number; end: number } {
  const trimStart = scan.movieTrimStart ?? 0
  const rangeEnd = scan.movieTrimEnd ?? scan.movieDuration ?? Number.POSITIVE_INFINITY
  const start = trimStart + chunkIndex * CHUNK_SECONDS
  return { start, end: Math.min(start + CHUNK_SECONDS, rangeEnd) }
}

class Scheduler {
  jobs = new Map<string, Job>()
  private dailyResetTimer: ReturnType<typeof setInterval> | null = null
  private modelTokenRateEMA = new Map<string, number>()

  constructor() {
    // Periodically check if a new day has arrived (at midnight Pacific Time / new date)
    this.dailyResetTimer = setInterval(() => {
      if (checkDailyReset()) {
        this.onDailyReset()
      }
    }, 30_000)

    // Periodic Gemini Storage Cleanup every 15 minutes: sweeps old orphaned files (>24 hours) from Gemini Files API
    setInterval(() => {
      for (const [, job] of this.jobs.entries()) {
        for (const lane of job.lanes) {
          void cleanupOrphanedGeminiFiles(lane.apiKey, 24 * 60 * 60_000)
        }
      }
    }, 15 * 60_000)
  }

  /** Called whenever the date rolls over or a new day is detected.
   * Wakes up any waiting jobs, clears lane exhausted states, and resets cooldowns. */
  onDailyReset() {
    for (const [, job] of this.jobs.entries()) {
      addLog(
        job.scan,
        'success',
        `[Daily Quota Reset] New calendar day detected — all Gemini free-tier daily quotas have reset! Resuming workers with fresh capacity.`,
      )
      for (const lane of job.lanes) {
        const laneState = job.scan.keyLanes.find((l) => l.idx === lane.idx)
        if (laneState && laneState.status !== 'error') {
          for (const ms of laneState.models) {
            if (ms.state === 'exhausted') ms.state = 'idle'
          }
        }
      }
      job.cooldownUntil = {}
      this.mark(job)
    }
  }

  isRunning(scanId: string) {
    return this.jobs.has(scanId)
  }

  /**
   * BACKUP CHECK: In-memory live scheduler check.
   * Returns whether a scan is currently running its chunk mapping phase.
   * Remains true during minute-to-minute transitions (Minute 1 -> Minute 2)
   * because job.chunkPhaseDone is only set once ALL selected minutes complete!
   */
  isScanActivelyChunkScanning(scanId: string): boolean {
    const job = this.jobs.get(scanId)
    if (!job) return false
    if (job.stopping) return false
    return !job.chunkPhaseDone
  }

  /** Synthesize/repair scan.shortSegments. Migration shim: old scans (short was
   *  trimmed to 1 minute) become a single segment that ADOPTS the existing
   *  scan.chunks array by reference, so all prior chunk states are preserved. */
  private ensureSegments(scan: Scan) {
    const dur = scan.shortDuration || CHUNK_SECONDS
    const segCount = Math.max(1, Math.ceil(dur / CHUNK_SECONDS))
    if (!scan.shortSegments || scan.shortSegments.length === 0) {
      scan.shortSegments = Array.from({ length: segCount }, (_, i) => ({
        index: i,
        start: i * CHUNK_SECONDS,
        end: Math.min((i + 1) * CHUNK_SECONDS, dur),
        status: 'pending' as const,
        chunks: i === 0 && Array.isArray(scan.chunks) && scan.chunks.length > 0 ? scan.chunks : [],
      }))
    }
    for (const seg of scan.shortSegments) {
      if (!Array.isArray(seg.chunks)) seg.chunks = []
      while (seg.chunks.length < scan.chunkCount) {
        seg.chunks.push({ index: seg.chunks.length, status: 'pending', attempts: 0 })
      }
      if (seg.chunks.length > scan.chunkCount) seg.chunks = seg.chunks.slice(0, scan.chunkCount)
    }
    if (scan.currentShortSegment === undefined) scan.currentShortSegment = 0
  }

  async start(
    scanId: string,
    resume: boolean,
    userApiKeys?: string[],
    tlApiKey?: string | null,
  ): Promise<{ ok: boolean; error?: string }> {
    if (this.jobs.has(scanId)) return { ok: false, error: 'Scan already running' }
    // PER-USER KEYS ONLY: the route passes the logged-in user's own keys
    // (from their private Blob file). There is NO shared/global fallback —
    // every user scans strictly on their own keys.
    const apiKeys = userApiKeys ?? []
    if (apiKeys.length === 0) return { ok: false, error: 'No Gemini API key configured for YOUR account. Add your key in Settings first.' }
    const scan = getScan(scanId)
    if (!scan) return { ok: false, error: 'Scan not found' }
    if (!scan.shortDuration || !scan.movieDuration || scan.chunkCount === 0) {
      return { ok: false, error: 'Both videos must be uploaded and chunked before scanning.' }
    }

    // Segments: synthesize/repair the per-minute structure (migration shim for old scans).
    this.ensureSegments(scan)
    const segments = scan.shortSegments!

    // Reset orphaned "scanning" chunks + resume cancelled ones ACROSS ALL segments.
    // PER-MINUTE MOVIE RANGE: chunks outside a minute's chosen movie range are
    // marked cancelled (skipped) so only in-range chunks consume API quota.
    for (const seg of segments) {
      // TWELVE LABS PRE-FILTER: the persisted per-minute chunk selection is only
      // honoured while the user's TL key is set. No key = feature fully off —
      // clear the selection so EVERYTHING scans (normal full scan, zero impact).
      if (!tlApiKey && Array.isArray(seg.prefilterChunks)) delete seg.prefilterChunks
      for (const c of seg.chunks) {
        const inRange = chunkOverlapsSegRange(scan, seg, c.index)
        const isGenuinelyFinished = c.status === 'match' || c.status === 'no_match'

        // Any chunk without genuine result (match / no_match) that is in range must be revived to pending
        if (!isGenuinelyFinished) {
          if (c.status === 'scanning' || c.status === 'failed' || (c.status as string) === 'done' || (c.status as string) === 'error' || (c.status as string) === 'parked') {
            c.status = 'pending'
          }
          // 'cancelled' is ONLY ever set by skip logic (range, pre-filter, or
          // EARLY-STOP), so revive cancelled chunks that fall inside the CURRENT
          // range — on fresh starts too, not just Resume. Otherwise changing a
          // minute's movie range after a run leaves the newly chosen part
          // permanently skipped. EXCEPTION: early-stop-skipped chunks of a DONE
          // minute stay skipped — us minute ke saare matches confirm ho chuke hain.
          if (c.status === 'cancelled' && inRange && !(c.skippedEarlyStop && seg.status === 'done')) {
            c.status = 'pending'
            delete c.skippedEarlyStop
          }
        }
        if (c.status === 'pending' && !inRange) c.status = 'cancelled'
        // Re-apply the persisted pre-filter selection (resume path). A fresh
        // pre-filter run inside runScan() recomputes/overrides this anyway.
        if (c.status === 'pending' && Array.isArray(seg.prefilterChunks) && !seg.prefilterChunks.includes(c.index)) {
          c.status = 'cancelled'
        }
      }
      if (seg.status === 'scanning' || seg.status === 'verifying') seg.status = 'pending'
      // If ANY chunk in this segment is still pending or not genuinely finished (not match/no_match/cancelled),
      // the segment CANNOT be marked done!
      const hasUnfinishedChunks = seg.chunks.some((c) => c.status !== 'match' && c.status !== 'no_match' && c.status !== 'cancelled')
      if (hasUnfinishedChunks) {
        seg.status = 'pending'
      }
    }
    // MINUTE SELECTION: only segments with selected !== false take part in the
    // scan (default = all selected). Unselected minutes are skipped entirely.
    const selectedSegs = segments.filter((s) => s.selected !== false)
    const pendingChunks = selectedSegs.reduce((n, s) => n + s.chunks.filter((c) => c.status === 'pending').length, 0)
    const allSettled = selectedSegs.every((s) => s.chunks.every((c) => c.status !== 'pending' && c.status !== 'scanning'))

    // Verification-only resume: all chunks already mapped but candidate groups
    // still have pending verifier/rescan work (or matches were never verified).
    const hasVerifyWork =
      (scan.candidateGroups || []).some((g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning') ||
      (scan.matches || []).some((m) => m.verified === undefined) ||
      (!scan.candidateGroups?.length && (scan.matches || []).length > 0 && allSettled)

    if (pendingChunks === 0 && !hasVerifyWork) return { ok: false, error: 'No pending chunks to scan.' }

    // Mirror the first incomplete SELECTED segment's chunks so the UI shows the right minute.
    const firstIncomplete = selectedSegs.find((s) => s.status !== 'done') || selectedSegs[0] || segments[0]
    scan.currentShortSegment = firstIncomplete.index
    scan.chunks = firstIncomplete.chunks

    const isNewDay = checkDailyReset()
    if (isNewDay) {
      addLog(
        scan,
        'success',
        `[Daily Quota Reset] New date detected (${geminiUsageDay()}) — all Gemini daily quotas reset to fresh state.`,
      )
    }

    // Proactive Storage Sweep: clean any orphaned Gemini files older than 24 hours to preserve cache for 24h
    for (const k of apiKeys) {
      void cleanupOrphanedGeminiFiles(k, 24 * 60 * 60_000)
    }

    if (!Array.isArray(scan.matches)) scan.matches = []
    scan.status = 'scanning'
    scan.error = null
    if (!scan.startedAt) scan.startedAt = Date.now()

    // MATCHTOOL ORCHESTRATOR ALLOCATION:
    // Key 1 is strictly reserved for the owner's manual use. Slot 14 does not exist (skip).
    // Active chunk workers run on slots 2..13, 15, 16.
    const allKeysWithIdx = apiKeys.map((k, i) => ({ key: k, idx: i + 1 }))
    const activeChunkKeys = allKeysWithIdx.filter((x) => x.idx > 1 && x.idx !== 14)

    const lanes: KeyLane[] = activeChunkKeys.map(({ key: k, idx }) => ({
      idx,
      apiKey: k,
      ai: getClient(k),
      segUris: new Map(),
      segUriPromises: new Map(),
      chunkUploads: new Map(),
      verifyActive: 0,
      verifyActiveByModel: new Map(),
    }))

    // Fast cleanse of any false exhaustion flags before starting
    try {
      cleanseStartupQuotas()
    } catch {}

    // Sync scan.keyLanes: ensure any model whose daily usage is under RPD is reset to idle
    if (!Array.isArray(scan.keyLanes)) scan.keyLanes = []
    for (const lane of lanes) {
      let laneState = scan.keyLanes.find((l) => l.idx === lane.idx)
      if (!laneState) {
        laneState = {
          idx: lane.idx,
          status: 'idle',
          models: MODEL_POOL.map((m) => ({ id: m.id, state: 'idle' })),
        }
        scan.keyLanes.push(laneState)
      } else if (laneState.status !== 'error') {
        for (const m of laneState.models) {
          const spec = MODEL_POOL.find((item) => item.id === m.id)
          const usage = getModelUsage(m.id, lane.apiKey)
          if (spec) {
            if (usage < spec.rpd) {
              if (m.state === 'exhausted') m.state = 'idle'
            } else {
              m.state = 'exhausted'
            }
          }
        }
      }
    }

    const isScanResuming =
      Boolean(resume) ||
      Boolean(scan.background?.resume) ||
      scan.status === 'stopped' ||
      scan.status === 'error' ||
      (scan.shortSegments?.some((s) => s.chunks.some((c) => c.status === 'done' || c.status === 'parked')) ?? false) ||
      (scan.chunks?.some((c) => c.status === 'done' || c.status === 'parked') ?? false)

    const minuteNote = segments.length > 1 ? ` across ${segments.length} short minutes (scanned sequentially)` : ''
    addLog(
      scan,
      'info',
      isScanResuming
        ? `Resuming scan: ${pendingChunks} chunk(s) pending${minuteNote} via OrchestratorCore (choke→burst pipeline)`
        : `Scan started: ${pendingChunks} chunk(s) queued${minuteNote} across ${CHUNK_MODEL_POOL.length} chunk models (${CHUNK_MODEL_POOL.map((m) => m.id).join(', ')}) × ${lanes.length} API key(s) — one prompt per chunk`,
    )

    const job: Job = {
      scan,
      lanes,
      resume: isScanResuming,
      prefetchOwner: new Map(),
      tlKey: tlApiKey || null,
      seg: null,
      queue: [],
      inFlight: new Set(),
      verifyQueue: [],
      verifyInFlight: new Set(),
      rescanQueue: [],
      chunkPhaseDone: false,
      earlyStopRevived: false,
      laneFirstSuccess: new Set(),
      stopping: false,
      nextFreeAt: {},
      cooldownUntil: {},
      chunkRetryCooldown: new Map(),
      dirty: true,
      saverTimer: null,
    }
    this.jobs.set(scanId, job)
    // Persist on an interval instead of every mutation (logs update very often).
    job.saverTimer = setInterval(() => {
      if (job.dirty) {
        job.dirty = false
        try {
          saveScan(job.scan)
        } catch {
          /* ignore */
        }
      }
    }, 800)
    saveScan(scan)

    void this.runScan(job).catch((err) => {
      addLog(job.scan, 'error', `Fatal scheduler error: ${err instanceof Error ? err.message : String(err)}`)
      job.scan.status = 'error'
      job.scan.error = err instanceof Error ? err.message : String(err)
      this.finish(job)
    })
    return { ok: true }
  }

  stop(scanId: string): { ok: boolean; error?: string } {
    globalChunkScanCoordinator.releaseChunkScanLock(scanId, true)
    globalChunkScanCoordinator.removeFromQueue(scanId)
    globalOrchestrator.stop()
    const job = this.jobs.get(scanId)
    if (!job) return { ok: false, error: 'Scan is not running' }
    job.stopping = true
    // INSTANT PARTIAL RESULTS: status TURANT 'stopped' + partial report yahin
    // ban jaata hai — user ko export/preview ke liye in-flight requests ke
    // drain hone ka wait NAHI karna padta. Baad me aane wale in-flight results
    // bhi scan.matches me merge hote rehte hain (periodic saver unhe save karta
    // hai), aur runScan ka stop-checkpoint report ko dobara refresh kar deta hai.
    job.scan.status = 'stopped'
    this.buildPartialReport(job)
    addLog(
      job.scan,
      'warn',
      'Stop requested — ab tak ke results (unverified samet) TURANT export/preview ke liye ready. In-flight requests background me settle ho rahi hain; Resume wahi se continue karega.',
    )
    job.dirty = false
    try {
      saveScan(job.scan)
    } catch {
      /* periodic saver will retry */
    }
    return { ok: true }
  }

  /** Update verifier enabled on a running job (e.g. from the UI button). */
  setVerifierEnabled(scanId: string, enabled: boolean) {
    const job = this.jobs.get(scanId)
    if (job) {
      job.scan.verifierEnabled = enabled
      if (!enabled) {
        // If turned off while running, clear the verify queue and settle pending groups as unverified
        job.verifyQueue = []
        for (const g of job.scan.candidateGroups || []) {
          if (g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning') {
            g.status = 'unverified'
            applyGroupMatches(job.scan, g)
          }
        }
        this.mark(job)
      }
    }
  }

  /** Apply a user candidate pick to a running job in memory, keeping in-flight state in sync. */
  applyUserPick(
    scanId: string,
    groupId: string,
    candidateIndex: number | null,
    viaRescan = false,
    extra?: {
      shortStart?: number
      shortEnd?: number
      movieStart?: number
      movieEnd?: number
      chunkIndex?: number
      model?: string
    },
  ): { ok: boolean; error?: string } {
    const job = this.jobs.get(scanId)
    if (!job) return { ok: false, error: 'Scan is not running' }
    job.scan.candidateGroups = job.scan.candidateGroups || []
    let g = job.scan.candidateGroups.find((x) => x.id === groupId)
    if (!g && extra?.shortStart != null && extra?.shortEnd != null) {
      g = job.scan.candidateGroups.find((x) => sameShortSegment(x.shortStart, x.shortEnd, extra.shortStart!, extra.shortEnd!))
    }
    if (!g) {
      if (extra?.shortStart != null && extra?.shortEnd != null && extra?.movieStart != null && extra?.movieEnd != null) {
        g = {
          id: groupId || `g-${Date.now()}`,
          shortStart: extra.shortStart,
          shortEnd: extra.shortEnd,
          status: 'confirmed',
          confirmedIndex: 0,
          confirmedViaRescan: false,
          candidates: [
            {
              shortStart: extra.shortStart,
              shortEnd: extra.shortEnd,
              movieStart: extra.movieStart,
              movieEnd: extra.movieEnd,
              chunkIndex: extra.chunkIndex ?? 0,
              model: extra.model ?? 'gemini-3.7-flash',
              verdict: 'same',
              rescan: 'none',
            },
          ],
        }
        job.scan.candidateGroups.push(g)
      } else {
        return { ok: false, error: 'Candidate group not found' }
      }
    }

    if (candidateIndex === null) {
      delete g.userPick
      applyGroupMatches(job.scan, g)
      addLog(
        job.scan,
        'info',
        `User choice cleared for short ${fmtTime(g.shortStart)}–${fmtTime(g.shortEnd)} — AI verdict (${g.status}) restored`,
      )
    } else {
      let idx = Number(candidateIndex)
      if (!Number.isInteger(idx) || idx < 0 || idx >= g.candidates.length) {
        if (extra?.movieStart != null && extra?.movieEnd != null) {
          const found = g.candidates.findIndex(
            (c) => Math.abs(c.movieStart - extra.movieStart!) < 0.5 && Math.abs(c.movieEnd - extra.movieEnd!) < 0.5,
          )
          if (found >= 0) {
            idx = found
          } else {
            idx = g.candidates.length
            g.candidates.push({
              shortStart: extra.shortStart ?? g.shortStart,
              shortEnd: extra.shortEnd ?? g.shortEnd,
              movieStart: extra.movieStart,
              movieEnd: extra.movieEnd,
              chunkIndex: extra.chunkIndex ?? 0,
              model: extra.model ?? 'gemini-3.7-flash',
              verdict: 'same',
              rescan: 'none',
            })
          }
        } else {
          return { ok: false, error: 'Invalid candidate index' }
        }
      }
      const c = g.candidates[idx]
      if (viaRescan && (c.rescanMovieStart == null || c.rescanMovieEnd == null)) {
        return { ok: false, error: 'This candidate has no rescan window' }
      }
      g.userPick = { index: idx, viaRescan, at: Date.now() }
      applyGroupMatches(job.scan, g)
      const pickedWin = viaRescan
        ? `${fmtTime(c.rescanMovieStart!)}–${fmtTime(c.rescanMovieEnd!)} (rescan)`
        : `${fmtTime(c.movieStart)}–${fmtTime(c.movieEnd)}`
      addLog(
        job.scan,
        'success',
        `User choice applied: short ${fmtTime(g.shortStart)}–${fmtTime(g.shortEnd)} → movie ${pickedWin} (Candidate ${idx + 1} of ${g.candidates.length})`,
      )
    }
    this.mark(job)
    saveScan(job.scan, { immediate: true })
    return { ok: true }
  }

  /** MANUAL chunk retry: reset one chunk and re-run its mapping (chunk models only).
   *  Works while a scan is running (re-queues on the live job) AND after it has
   *  finished (restarts the scheduler in resume mode; the chunk file is re-cut
   *  from the movie automatically if it was cleaned up). */
  async retryChunk(
    scanId: string,
    chunkIndex: number,
    segmentIndex?: number,
    userApiKeys?: string[],
  ): Promise<{ ok: boolean; error?: string }> {
    const job = this.jobs.get(scanId)
    const scan = job ? job.scan : getScan(scanId)
    if (!scan) return { ok: false, error: 'Scan not found' }

    // Resolve the target segment (default: the current/selected minute).
    let seg: ShortSegmentState | null = null
    let chunk: ChunkState | undefined
    if (scan.shortSegments?.length) {
      const si = segmentIndex ?? scan.currentShortSegment ?? 0
      seg = scan.shortSegments[si] ?? null
      if (!seg) return { ok: false, error: `Short minute ${si} not found` }
      chunk = seg.chunks[chunkIndex]
    } else {
      chunk = scan.chunks?.[chunkIndex]
    }
    if (!chunk) return { ok: false, error: `Chunk ${chunkIndex} not found` }

    const isFailedChunk = chunk.status === 'failed' || chunk.status === 'policy_blocked'

    // RESCAN LOCK: a manual retry on matched/no_match chunks waits while the verification
    // queue has pending candidates. But a FAILED chunk has no pending verifications and
    // must be immediately retryable so the user is never stuck!
    const pendingVerify = (scan.candidateGroups || []).filter(
      (g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning',
    ).length
    if (pendingVerify > 0 && !isFailedChunk) {
      return {
        ok: false,
        error: `Verification in progress — ${pendingVerify} candidate group(s) pending. Retry tabhi milega jab saare candidates verify ho jayen${job ? '' : ' (Resume karke verification poori karo)'}.`,
      }
    }

    const isActiveSeg = !seg || !job || job.seg === seg
    if ((chunk.status === 'scanning' || job?.inFlight.has(chunkIndex)) && isActiveSeg) {
      return { ok: false, error: `Chunk ${chunkIndex} is currently in flight — wait for it to finish` }
    }
    if (job && isActiveSeg && job.queue.includes(chunkIndex)) {
      return { ok: false, error: `Chunk ${chunkIndex} is already queued` }
    }
    if (job && !isActiveSeg) {
      return {
        ok: false,
        error: `Scan is busy on minute ${(job.seg?.index ?? 0) + 1} — retry this chunk of minute ${(seg?.index ?? 0) + 1} after the scan finishes`,
      }
    }

    // Reset the chunk and wipe its old evidence — ONLY within this segment's short window.
    const segStart = seg ? seg.start : 0
    const segEnd = seg ? seg.end : Number.POSITIVE_INFINITY
    chunk.status = 'pending'
    chunk.attempts = 0
    chunk.qualityRetries = 0
    chunk.policyRetried = false
    chunk.matches = []
    scan.matches = (scan.matches || []).filter(
      (mm) => !(mm.chunkIndex === chunkIndex && mm.shortStart >= segStart && mm.shortStart < segEnd),
    )
    if (scan.candidateGroups?.length) {
      scan.candidateGroups = scan.candidateGroups.filter(
        (g) => !(g.shortStart >= segStart && g.shortStart < segEnd && g.candidates.some((c) => c.chunkIndex === chunkIndex)),
      )
    }
    if (seg && seg.status === 'done') seg.status = 'pending'
    addLog(
      scan,
      'info',
      seg && (scan.shortSegments?.length ?? 0) > 1
        ? `Manual retry: minute ${seg.index + 1} · chunk ${chunkIndex} reset and re-queued for chunk-model mapping`
        : `Manual retry: chunk ${chunkIndex} reset and re-queued for chunk-model mapping`,
    )

    if (job) {
      job.queue.push(chunkIndex)
      this.mark(job)
      return { ok: true }
    }

    // No live job — persist the reset state and restart in resume mode
    // using the SAME user's own keys that were passed in.
    if (scan.status === 'done' || scan.status === 'stopped' || scan.status === 'error') scan.status = 'stopped'
    saveScan(scan)
    return this.start(scanId, true, userApiKeys)
  }

  private mark(job: Job) {
    job.dirty = true
  }

  /** STOP-AWARE sleep: pacing waits 1 minute tak ke ho sakte hain — ye helper
   *  chhote slices me sota hai aur Stop dabate hi turant exit ho jaata hai,
   *  taaki workers minute-bhar latke na rahen. */
  private async stoppableSleep(job: Job, ms: number) {
    const until = Date.now() + ms
    while (!job.stopping) {
      const left = until - Date.now()
      if (left <= 0) return
      await sleep(Math.min(500, left))
    }
  }

  /** Persist a verbatim Gemini response or diagnostic error on a chunk (drives the UI raw-output expander).
   *  Bounded: 20KB per entry, max 16 entries per chunk (oldest dropped). */
  private recordChunkOutput(
    chunk: ChunkState | undefined,
    model: string,
    text?: string,
    tokens?: ChunkTokenUsage,
    status: 'success' | 'error' = 'success',
    error?: string,
  ) {
    if (!chunk || (!text && !error)) return
    if (!chunk.rawOutputs) chunk.rawOutputs = []
    chunk.rawOutputs.push({
      model,
      t: Date.now(),
      text: text && text.length > 20_000 ? `${text.slice(0, 20_000)}\n... [truncated]` : (text || ''),
      tokens,
      status,
      error,
    })
    if (chunk.rawOutputs.length > 16) chunk.rawOutputs.splice(0, chunk.rawOutputs.length - 16)
  }

  /** modelStates key: lane 1 uses the plain model id (drives the Model Pool board);
   *  lanes 2-20 keep their own suffixed entries so keys never overwrite each other. */
  private stateKey(lane: KeyLane, m: ModelSpec) {
    return lane.idx === 1 ? m.id : `${m.id}@${lane.idx}`
  }

  private rateKey(lane: KeyLane, m: ModelSpec) {
    return `${lane.idx}|${m.id}`
  }

  private paceSlotKey(lane: KeyLane, m: ModelSpec, slot: number = 0) {
    return `${lane.idx}|${m.id}|${slot}`
  }

  private modelState(job: Job, lane: KeyLane, m: ModelSpec) {
    const key = this.stateKey(lane, m)
    const used = getModelUsage(m.id, lane.apiKey)
    if (!job.scan.modelStates[key]) {
      job.scan.modelStates[key] = { state: 'idle', currentChunk: null, cooldownUntil: null, usedToday: used }
    }
    const s = job.scan.modelStates[key]
    s.usedToday = used
    return s
  }

  /** Per-segment cut locks so concurrent lanes never cut the SAME segment file simultaneously. */
  private segCutLocks = new Map<string, Promise<string>>()

  /** Ensure the short-minute segment file exists and is cleanly extracted. */
  private async ensureSegmentFile(scan: Scan, seg: ShortSegmentState): Promise<string> {
    const mediaDir = scanMediaDir(scan.id)
    const segDir = path.join(mediaDir, 'segments')
    const file = segmentPath(segDir, seg.index)
    if (fs.existsSync(file)) {
      try {
        if (fs.statSync(file).size > 1024) return file
      } catch {
        /* re-extract */
      }
    }
    const key = `${scan.id}|seg|${seg.index}`
    let p = this.segCutLocks.get(key)
    if (!p) {
      p = (async () => {
        fs.mkdirSync(segDir, { recursive: true })
        addLog(scan, 'info', `Minute ${seg.index + 1}: preparing short segment ${ts(seg.start)}–${ts(seg.end)}`)
        await extractSegment(path.join(mediaDir, 'short.mp4'), seg.start, seg.end, file)
        return file
      })().finally(() => this.segCutLocks.delete(key))
      this.segCutLocks.set(key, p)
    }
    return p
  }

  /** Upload ONE short-minute segment file per key lane (Files API uploads are per key).
   *  The segment file is cleanly cut under an extraction lock so concurrent lanes never collide. */
  private async ensureSegmentUri(job: Job, lane: KeyLane, seg: ShortSegmentState): Promise<string> {
    const existing = lane.segUris.get(seg.index)
    if (existing) return existing
    let p = lane.segUriPromises.get(seg.index)
    if (!p) {
      p = (async () => {
        const file = await this.ensureSegmentFile(job.scan, seg)
        addLog(job.scan, 'info', `Uploading short minute ${seg.index + 1} to the scanner (key ${lane.idx})...`)
        this.mark(job)
        const f = await uploadVideo(lane.ai, file)
        lane.segUris.set(seg.index, f.uri)
        addLog(job.scan, 'success', `Short minute ${seg.index + 1} ready on the scanner (key ${lane.idx})`)
        this.mark(job)
        return f.uri
      })().catch((err) => {
        lane.segUriPromises.delete(seg.index)
        throw err
      })
      lane.segUriPromises.set(seg.index, p)
    }
    return p
  }

  /** Per-chunk cut locks so two lanes/workers never cut the SAME chunk file at once. */
  private cutLocks = new Map<string, Promise<string>>()

  /** Ensure the chunk's local file exists (re-cut from the movie if missing).
   *  Cut exactly once even when multiple lanes need it at the same time. */
  private async ensureChunkFile(scan: Scan, chunkIndex: number): Promise<string> {
    const chunksDir = path.join(scanMediaDir(scan.id), 'chunks')
    const chunkFile = chunkPath(chunksDir, chunkIndex)
    if (fs.existsSync(chunkFile)) return chunkFile
    const key = `${scan.id}|${chunkIndex}`
    let p = this.cutLocks.get(key)
    if (!p) {
      p = (async () => {
        // TRIM-AWARE: chunk N starts at trimStart + N*60 in the ORIGINAL movie.
        const { start: cs, end: ce } = chunkAbsWindow(scan, chunkIndex)
        fs.mkdirSync(chunksDir, { recursive: true })

        // Check if another scan of the same movie has this chunk already cut
        const otherScans = listScans().filter((s) => s.id !== scan.id && s.movieName === scan.movieName)
        for (const os of otherScans) {
          const fullOs = getScan(os.id)
          if (!fullOs || fullOs.movieSize !== scan.movieSize) continue
          const osWin = chunkAbsWindow(fullOs, chunkIndex)
          if (Math.abs(osWin.start - cs) < 0.1 && Math.abs(osWin.end - ce) < 0.1) {
            const candFile = chunkPath(path.join(scanMediaDir(os.id), 'chunks'), chunkIndex)
            if (fs.existsSync(candFile) && fs.statSync(candFile).size > 0) {
              try {
                fs.linkSync(candFile, chunkFile)
              } catch {
                try {
                  fs.copyFileSync(candFile, chunkFile)
                } catch {
                  // ignore
                }
              }
              if (fs.existsSync(chunkFile)) {
                addLog(scan, 'info', `Chunk ${chunkIndex}: reused from scan ${os.id} (no re-cut)`)
                return chunkFile
              }
            }
          }
        }

        addLog(scan, 'info', `Chunk ${chunkIndex}: chunk file missing — re-cutting ${cs}s–${ce}s from movie`)
        await extractClipPrecise(path.join(scanMediaDir(scan.id), 'movie.mp4'), cs, ce, chunkFile)
        return chunkFile
      })().finally(() => this.cutLocks.delete(key))
      this.cutLocks.set(key, p)
    }
    return p
  }

  /** Start (or join) the upload of one movie chunk on this key lane.
   *  Used both by the active worker and by background prefetch. */
  private startChunkUpload(job: Job, lane: KeyLane, chunkIndex: number, prefetch: boolean): Promise<{ uri: string; name: string }> {
    let p = lane.chunkUploads.get(chunkIndex)
    if (p) return p
    p = (async () => {
      // Fast pre-flight check: do not waste bandwidth or Files API uploads if all chunk models on this key are exhausted
      const hasActiveModel = CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd))
      if (!hasActiveModel) {
        throw new Error(`All chunk models on key ${lane.idx} are exhausted today — upload skipped`)
      }
      const file = await this.ensureChunkFile(job.scan, chunkIndex)
      if (prefetch) {
        addLog(job.scan, 'info', `Pipeline: chunk ${chunkIndex} pre-uploading in background (key ${lane.idx}) — ready before its turn`)
        this.mark(job)
      }
      return uploadVideo(lane.ai, file)
    })()
    lane.chunkUploads.set(chunkIndex, p)
    // A failed prefetch must never poison the cache — the consumer re-uploads fresh.
    p.catch(() => lane.chunkUploads.delete(chunkIndex))
    return p
  }

  /** Consume (and remove) this lane's upload for a chunk — one-shot use,
   *  because the worker deletes the remote file right after its request. */
  private takeChunkUpload(job: Job, lane: KeyLane, chunkIndex: number): Promise<{ uri: string; name: string }> {
    const p = this.startChunkUpload(job, lane, chunkIndex, false)
    lane.chunkUploads.delete(chunkIndex)
    // The chunk is now being consumed — release the claim so a re-queue (retry)
    // can be prefetched fresh by whichever lane is free.
    job.prefetchOwner.delete(chunkIndex)
    return p
  }

  /** Try to claim a chunk for pre-upload / exclusive lock on this lane. Returns false when another
   *  lane already owns it (its upload is in flight / ready there). */
  private claimPrefetch(job: Job, lane: KeyLane, chunkIndex: number): boolean {
    const owner = job.prefetchOwner.get(chunkIndex)
    if (owner !== undefined && owner !== lane.idx) return false
    job.prefetchOwner.set(chunkIndex, lane.idx)
    return true
  }

  /**
   * LOCK & PREPARE NEXT CHUNK (user request: "Us model key ko 30wait pr daal do lekin baground me sab kuch tyar krke rakho. Chunk uske liye or jo chunk uske dene wale ho lock krke rakho."):
   * Immediately called when a chunk request completes and enters 30s cooldown.
   * Locks the next candidate chunk exclusively for this key lane, pre-cuts it via FFmpeg,
   * and uploads it to Gemini Files API in the background.
   */
  private lockAndPrepareNextChunk(job: Job, lane: KeyLane): number | null {
    if (job.stopping) return null
    const hasActiveModel = CHUNK_MODEL_POOL.some(
      (m) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd),
    )
    if (!hasActiveModel) return null

    // Find the next available chunk for this lane in job.queue
    let targetCi: number | null = null
    for (const ci of job.queue) {
      if (lane.chunkUploads.has(ci) || job.prefetchOwner.get(ci) === lane.idx) {
        targetCi = ci
        break
      }
      if (!job.prefetchOwner.has(ci)) {
        targetCi = ci
        break
      }
    }

    if (targetCi === null) return null

    // Lock exclusively for this lane:
    job.prefetchOwner.set(targetCi, lane.idx)

    // Pre-cut and start upload immediately in the background:
    if (!lane.chunkUploads.has(targetCi)) {
      void this.startChunkUpload(job, lane, targetCi, true).catch(() => {
        if (job.prefetchOwner.get(targetCi) === lane.idx) {
          job.prefetchOwner.delete(targetCi)
        }
      })
    }

    return targetCi
  }

  /** LANE AFFINITY & LOCK RESPECT: pull the next chunk for this lane.
   *  Respects chunkRetryCooldown to prevent rapid-fire retries after errors.
   *  1. Prefer chunk already pre-uploaded by THIS key.
   *  2. Prefer chunk pre-locked specifically for THIS key.
   *  3. Unclaimed chunk in queue.
   *  4. If all remaining chunks in queue are locked by other active lanes, do not steal them. */
  private pullChunkFor(job: Job, lane: KeyLane): number | undefined {
    if (job.queue.length === 0) return undefined
    const now = Date.now()
    const isEligible = (ci: number) => {
      const info = job.chunkRetryCooldown?.get(ci)
      return !info || info.availableAt <= now
    }

    let pos = job.queue.findIndex((ci) => isEligible(ci) && lane.chunkUploads.has(ci))
    if (pos < 0) pos = job.queue.findIndex((ci) => isEligible(ci) && job.prefetchOwner.get(ci) === lane.idx)
    if (pos < 0) pos = job.queue.findIndex((ci) => isEligible(ci) && !job.prefetchOwner.has(ci))
    if (pos < 0) {
      const otherActiveOwners = job.lanes.filter(
        (l) => l.idx !== lane.idx && CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(l.apiKey, m.id, m.rpd)),
      )
      if (otherActiveOwners.length > 0) return undefined
      pos = job.queue.findIndex((ci) => isEligible(ci))
    }
    if (pos < 0) return undefined
    return job.queue.splice(pos, 1)[0]
  }

  /** PIPELINING: while this lane's model is busy analyzing, pre-cut + pre-upload
   *  the next queued chunks on the SAME key so the next analysis starts with
   *  ZERO upload wait. Depth 2 keeps bandwidth + Files API usage sane.
   *  Each chunk is claimed by ONE lane — before this, every lane prefetched the
   *  same 2 queue-head chunks (chunk 66 uploading on keys 1, 5, 3, 2, 6 at once). */
  private prefetchNextChunks(job: Job, lane: KeyLane) {
    if (job.stopping) return
    // Pre-flight check: if this key has no models with quota remaining, skip prefetch
    const hasActiveModel = CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd))
    if (!hasActiveModel) return

    const PREFETCH_DEPTH = 2
    let owned = 0
    for (const ci of job.queue) {
      if (owned >= PREFETCH_DEPTH) break
      if (lane.chunkUploads.has(ci)) {
        owned++
        continue
      }
      if (!this.claimPrefetch(job, lane, ci)) continue
      owned++
      void this.startChunkUpload(job, lane, ci, true).catch(() => job.prefetchOwner.delete(ci))
    }
  }

  /** NEXT-MINUTE BACKGROUND PREP: while minute N is being scanned/verified,
   *  get minute N+1 fully ready in the background so it starts with ZERO wait:
   *  1) cut minute N+1's short-segment file + upload it to Gemini on EVERY key
   *     lane (the big serial cost between minutes),
   *  2) pre-cut minute N+1's pending movie-chunk files locally (free, no API).
   *  Everything is cached (segUris / chunk files on disk), so even if the scan
   *  stops, Resume picks the prepared work right back up. All failures are
   *  silent — the normal on-demand path re-does anything that failed. */
  private prepareNextSegment(job: Job, segments: ShortSegmentState[], currentIndex: number) {
    if (job.stopping) return
    const next = segments.find(
      (s) =>
        s.index > currentIndex &&
        s.selected !== false &&
        s.chunks.some((c) => c.status === 'pending'),
    )
    if (!next) return
    // Segment upload on every lane (kept in segUris cache — reused, never deleted).
    let announced = false
    for (const lane of job.lanes) {
      // Skip upload on keys where all models are exhausted
      const hasActiveModel = CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd))
      if (!hasActiveModel) continue

      if (lane.segUris.has(next.index) || lane.segUriPromises.has(next.index)) continue
      if (!announced) {
        announced = true
        addLog(job.scan, 'info', `Pipeline: minute ${next.index + 1} background me ready ho raha hai (segment upload + chunk cutting) — minute ${currentIndex + 1} complete hote hi turant start hoga`)
        this.mark(job)
      }
      void this.ensureSegmentUri(job, lane, next).catch(() => {})
    }
    // Pre-cut the next minute's pending chunk files locally (ffmpeg only, no API
    // quota). Sequential to keep CPU/disk pressure low while the scan runs.
    // CONFIDENCE ORDER: highest-confidence chunks cut first — wahi sabse pehle
    // scan honge, isliye unka file + upload sabse pehle ready hona chahiye.
    const pendingIdx = this.orderByConfidence(
      next,
      next.chunks.filter((c) => c.status === 'pending').map((c) => c.index),
    )
    void (async () => {
      for (const ci of pendingIdx) {
        if (job.stopping) return
        try {
          await this.ensureChunkFile(job.scan, ci)
        } catch {
          /* silent — on-demand path re-cuts when the chunk's turn comes */
        }
      }
    })()
    // NEXT-MINUTE CHUNK PRE-UPLOAD (user request: "next minute ke chunks upload
    // karke READY rakho"): next minute ke top highest-confidence chunks Gemini
    // Files API par pehle se upload ho jaate hain — minute start hote hi pehli
    // request ZERO upload wait ke saath jaati hai. DISTRIBUTED: har chunk sirf
    // EK lane par (round-robin), same chunk saari keys par nahi — worker lane
    // affinity se apna pre-uploaded chunk hi uthata hai. Unused uploads finish()
    // me delete ho jaate hain, Files API clean rahta hai.
    const preUpload = pendingIdx.slice(0, Math.min(pendingIdx.length, job.lanes.length))
    preUpload.forEach((ci, i) => {
      const lane = job.lanes[i % job.lanes.length]
      const hasActiveModel = CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd))
      if (!hasActiveModel) return
      if (lane.chunkUploads.has(ci) || !this.claimPrefetch(job, lane, ci)) return
      void this.startChunkUpload(job, lane, ci, true).catch(() => job.prefetchOwner.delete(ci))
    })
  }

  // ---------- Twelve Labs pre-filter (optional, accuracy-first) ----------

  /** OPTIONAL embedding pre-filter. Runs ONLY when the user's Twelve Labs key
   *  is set AND the movie's segment embeddings were saved at index time.
   *  Decides WHICH movie chunks each short minute is scanned against — the
   *  Gemini pipeline itself (prompts, chunk cutting, models, verification) is
   *  completely untouched. ANY error here = silent fallback to a normal full
   *  scan; the scan NEVER fails because of Twelve Labs. */
  private async applyTwelveLabsPrefilter(job: Job) {
    const { scan } = job
    const allSegs = scan.shortSegments || []
    const selectedSegs = allSegs.filter((s) => s.selected !== false)
    const countPending = () =>
      selectedSegs.reduce((n, s) => n + s.chunks.filter((c) => c.status === 'pending').length, 0)

    /** Fall back to the normal FULL scan: drop every pre-filter selection and
     *  revive pre-filter-cancelled chunks (range-skipped chunks stay skipped). */
    const fullScan = (reason: string, attempted: boolean) => {
      for (const seg of allSegs) {
        if (Array.isArray(seg.prefilterChunks)) delete seg.prefilterChunks
        delete seg.chunkConfidence
        delete seg.tlWindows
        for (const c of seg.chunks) {
          if (c.status === 'cancelled' && chunkOverlapsSegRange(scan, seg, c.index)) c.status = 'pending'
        }
        if (seg.status === 'done' && seg.chunks.some((c) => c.status === 'pending')) seg.status = 'pending'
      }
      const total = countPending()
      scan.prefilter = { mode: 'full', selectedChunks: total, totalChunks: total, reason, at: Date.now() }
      if (attempted) addLog(scan, 'warn', `Twelve Labs pre-filter skipped (${reason}) — normal FULL scan chal raha hai (accuracy 100% safe)`)
      this.mark(job)
    }

    if (!job.tlKey) {
      // No key = feature off. Don't even set scan.prefilter noise beyond mode.
      fullScan('Twelve Labs key not set', false)
      return
    }

    try {
      // 1) Movie embeddings MUST already be saved (one-time indexing via the button).
      const movieEmb = await loadEmbeddings(scan.id, 'movie')
      if (!movieEmb) {
        fullScan('movie Twelve Labs par indexed nahi hai', true)
        return
      }

      // 2) Short embeddings: reuse the cached copy, else upload + poll + save once.
      let shortEmb = await loadEmbeddings(scan.id, 'short')
      if (!shortEmb) {
        const shortFile = path.join(scanMediaDir(scan.id), 'short.mp4')
        if (!fs.existsSync(shortFile)) {
          fullScan('short video file missing locally', true)
          return
        }
        addLog(scan, 'info', 'Twelve Labs pre-filter: short video upload + indexing (embeddings ke liye)...')
        this.mark(job)
        const indexId = movieEmb.indexId || (await ensureIndex(job.tlKey))
        const { taskId } = await createIndexTask(job.tlKey, indexId, shortFile)
        const videoId = await pollTaskUntilReady(job.tlKey, taskId, { intervalMs: 5000, timeoutMs: 30 * 60_000 })
        const segs = await fetchVideoEmbeddings(job.tlKey, indexId, videoId)
        shortEmb = { indexId, videoId, savedAt: Date.now(), segments: segs }
        await saveEmbeddings(scan.id, 'short', shortEmb)
        addLog(scan, 'success', `Twelve Labs pre-filter: short ke ${segs.length} segment embedding(s) ready (cached for resume)`)
      }

      // 3) Cosine-similarity matching (threshold 0.82, ±1 buffer chunks).
      const result = computePrefilterChunks(scan, selectedSegs, shortEmb.segments, movieEmb.segments)
      if (!result.perSegment) {
        // ACCURACY RULE: some short segment matched nowhere — never trust the
        // pre-filter in that case. Full scan.
        fullScan(result.reason || 'low-confidence pre-filter', true)
        return
      }

      // 4) Apply the selection: revive selected chunks, cancel unselected pending
      //    ones. Range-skipped chunks stay skipped (existing quota-saver logic).
      const totalBefore = selectedSegs.reduce(
        (n, s) => n + s.chunks.filter((c) => c.status === 'pending' || c.status === 'cancelled').length,
        0,
      )
      for (const seg of selectedSegs) {
        const set = result.perSegment.get(seg.index) ?? new Set<number>()
        seg.prefilterChunks = [...set].sort((a, b) => a - b)
        // CONFIDENCE (high→low ordering) + EXPECTED WINDOWS (early-stop system):
        // persist both so Resume keeps the same ordering + early-stop behaviour.
        const conf = result.confidence?.get(seg.index)
        seg.chunkConfidence = conf ? Object.fromEntries([...conf.entries()].map(([k, v]) => [String(k), v])) : undefined
        const windows = result.expectedWindows?.get(seg.index)
        seg.tlWindows = windows && windows.length > 0 ? windows : undefined
        for (const c of seg.chunks) {
          const inRange = chunkOverlapsSegRange(scan, seg, c.index)
          if (c.status === 'cancelled' && set.has(c.index) && inRange) c.status = 'pending'
          if (c.status === 'pending' && !set.has(c.index)) c.status = 'cancelled'
        }
        if (seg.status === 'done' && seg.chunks.some((c) => c.status === 'pending')) seg.status = 'pending'
      }
      const selected = countPending()
      scan.prefilter = {
        mode: 'prefiltered',
        selectedChunks: selected,
        totalChunks: Math.max(totalBefore, selected),
        at: Date.now(),
      }
      addLog(
        scan,
        'success',
        `Twelve Labs pre-filter: ${selected} of ${Math.max(totalBefore, selected)} chunks selected (threshold ${TL_SIMILARITY_THRESHOLD}, ±1 buffer chunks) — sirf yehi chunks Gemini ko jayenge`,
      )
      // PER-MINUTE BREAKDOWN (UI + logs): har minute ke planned chunks + expected windows.
      for (const seg of selectedSegs) {
        const n = seg.chunks.filter((c) => c.status === 'pending').length
        if (n > 0) {
          addLog(
            scan,
            'info',
            `Twelve Labs plan — Minute ${seg.index + 1}: ${n} chunk(s) scan honge${seg.tlWindows?.length ? `, ${seg.tlWindows.length} expected match window(s) (early-stop active: sab windows confirm hote hi bache chunks skip)` : ''}`,
          )
        }
      }
      this.mark(job)
    } catch (err) {
      // ANY Twelve Labs failure = silent fallback. The scan itself never fails.
      fullScan(err instanceof Error ? err.message.slice(0, 160) : String(err), true)
    }
  }

  private async runScan(job: Job) {
    const { scan } = job
    const segments = scan.shortSegments || []
    const multi = segments.length > 1

    // 1. Wait until all chunks of this scan are cut before starting chunk scan!
    if (scan.chunkingProgress !== undefined && (scan.chunkingProgress < 100 || scan.status === 'chunking')) {
      addLog(scan, 'info', `Waiting for movie chunking to finish (100%) before chunk scan... (currently ${scan.chunkingProgress}%)`)
      while (!job.stopping) {
        const fresh = getScan(scan.id)
        if (!fresh) break
        const pct = fresh.chunkingProgress ?? 100
        const segPct = fresh.shortSegmentingProgress ?? 100
        if (pct >= 100 && segPct >= 100 && fresh.status !== 'chunking') break
        await sleep(1000)
      }
    }
    if (job.stopping) {
      scan.status = 'stopped'
      this.buildPartialReport(job)
      this.finish(job)
      return
    }

    // 2. ACQUIRE EXCLUSIVE GLOBAL CHUNK SCAN LOCK (1-scan-at-a-time global lock & queue, checking every 1s)
    let releaseChunkLock: (() => void) | null = null
    try {
      releaseChunkLock = await globalChunkScanCoordinator.acquireChunkScanLock({
        scanId: scan.id,
        scanTitle: scan.shortName || scan.id,
        onWait: (msg) => {
          addLog(scan, 'info', msg)
          this.mark(job)
        },
        isStopping: () => job.stopping,
      })
    } catch {
      if (job.stopping) {
        scan.status = 'stopped'
        this.buildPartialReport(job)
        this.finish(job)
        return
      }
    }

    try {
      // OPTIONAL Twelve Labs pre-filter: decides which chunks to scan. Errors
      // here can never fail the scan — worst case is a normal full scan.
      await this.applyTwelveLabsPrefilter(job)
      if (job.stopping) {
        scan.status = 'stopped'
        this.buildPartialReport(job)
        this.finish(job)
        return
      }

    // Reset transient verify states from an interrupted run + queue any groups
    // still pending from a previous session (verification-only resume).
    this.prepareVerifyState(scan)
    this.enqueueNewGroups(job, true)

    // GLOBAL VERIFY PIPELINE: verify workers start ONCE and keep running in the
    // background for the WHOLE scan — chunk models aur verify models alag hain,
    // isliye minute N ki verification chalte hue minute N+1 ke chunks scan hote
    // hain (user request: sab chunks settle hote hi aage badho, verification ka
    // wait mat karo — wo background me chalti rahegi).
    let verifyPhase: Promise<unknown> = Promise.resolve()
    const startVerifyWorkers = () => {
      if (scan.verifierEnabled === false) {
        verifyPhase = Promise.resolve()
        return
      }
      const ws: Promise<void>[] = []
      for (const lane of job.lanes) {
        for (const m of VERIFY_MODEL_POOL) {
          for (let slot = 0; slot < VERIFY_CONCURRENCY_PER_MODEL; slot++) {
            ws.push(this.verifyWorker(job, lane, m, slot))
          }
        }
      }
      verifyPhase = Promise.all(ws)
    }
    job.chunkPhaseDone = false
    startVerifyWorkers()

    // OUTER PASSES: agar early-stop ke baad koi group REJECT ho jaye aur uske
    // minute ke skipped chunks wapas pending ho jayen, to ek aur pass chalega.
    const MAX_PASSES = 6
    for (let pass = 0; pass < MAX_PASSES; pass++) {
      job.earlyStopRevived = false

      // MULTI-MINUTE CHUNK PASSES:
      // The OrchestratorCore scans all selected minute segments unifiedly so that when
      // a burst of 10 requests (or 4 requests on 3.7/3.8) is available, it pulls pending chunks
      // across all remaining minutes rather than being bottlenecked to just 1 minute!
      const activeSegments = segments.filter((s) => s.selected !== false)

      // Revive any chunks that do not have real result (match / no_match) and are not legitimately cancelled
      for (const seg of activeSegments) {
        for (const c of seg.chunks) {
          const inRange = chunkOverlapsSegRange(scan, seg, c.index)
          const isGenuinelyFinished = c.status === 'match' || c.status === 'no_match'
          if (!isGenuinelyFinished && inRange && !c.skippedEarlyStop) {
            c.status = 'pending'
          }
        }
      }

      const totalPendingChunks = activeSegments.reduce(
        (sum, s) => sum + s.chunks.filter((c) => c.status === 'pending').length,
        0,
      )

      if (totalPendingChunks > 0 && !job.stopping) {
        if (!job.stopping) scan.status = 'scanning'
        addLog(
          scan,
          'info',
          `Chunk scan starting across ${activeSegments.length} selected minute segment(s) (${totalPendingChunks} pending chunks total) — Orchestrator handles choke→burst cycle across all minutes without per-minute bottleneck.`,
        )
        this.mark(job)

        // MATCHTOOL CHUNK SCAN ORCHESTRATOR: Choke (3.5 -> 3.6 -> 3.7 -> 3.8) -> Burst (10/4/4 within 3s) -> Unlimited Recovery Watch
        await this.runOrchestratorChunkScan(job, activeSegments)
        // Catch any candidates merged by the very last chunk.
        this.enqueueNewGroups(job)
      }

      if (job.stopping) break

      // Clean up any stray scanning status
      for (const seg of activeSegments) {
        for (const c of seg.chunks) {
          if (c.status === 'scanning') c.status = 'pending'
        }
      }

      // Update segment statuses
      for (const seg of activeSegments) {
        const segGroups = (scan.candidateGroups || []).filter((g) => g.shortStart < seg.end && g.shortEnd > seg.start)
        const unresolved =
          scan.verifierEnabled === false
            ? []
            : segGroups.filter((g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning')
        const hasUnfinishedChunks = seg.chunks.some((c) => c.status !== 'match' && c.status !== 'no_match' && c.status !== 'cancelled')
        seg.status = hasUnfinishedChunks ? 'pending' : (unresolved.length > 0 ? 'verifying' : 'done')
      }
      this.mark(job)

      // Only stop if ALL models across ALL lanes are genuinely exhausted for today
      const anyActiveModel = job.lanes.some((l) =>
        CHUNK_MODEL_POOL.some((m) => !globalGeminiCoordinator.isModelExhausted(l.apiKey, m.id, m.rpd))
      )
      if (!anyActiveModel) {
        addLog(scan, 'error', `All models across all ${job.lanes.length} API keys are exhausted for today — cannot continue chunk scan.`)
        break
      }

      const anyRemainingPending = activeSegments.some((s) => s.chunks.some((c) => c.status === 'pending'))
      if (!anyRemainingPending) {
        break
      }

      // All minutes' chunk phases done (or quota-blocked) — let the verify
      // pipeline drain fully now.
      job.seg = null
      job.chunkPhaseDone = true
      // STOP GUARD: stop ke baad status kabhi 'stopped' se wapas 'verifying'
      // nahi hota — warna export/preview panel phir chhup jaata.
      if (
        !job.stopping &&
        scan.verifierEnabled !== false &&
        (job.verifyQueue.length > 0 || job.verifyInFlight.size > 0)
      ) {
        scan.status = 'verifying'
        this.mark(job)
      }
      await verifyPhase

      // Rescan phase: runs ONLY after ALL normal candidate verification completes.
      // Strictly uses gemini-3.5-flash and gemini-3-flash-preview ONLY.
      if (!job.stopping && scan.verifierEnabled !== false && job.rescanQueue.length > 0) {
        await this.runRescanPhase(job)
      }

      // EARLY-STOP REVIVE PASS: kisi rejected group ne skipped chunks wapas
      // pending kiye? To unhe scan karne ke liye ek aur pass chalao.
      const revivedPending = segments.some(
        (s) => s.selected !== false && s.chunks.some((c) => c.status === 'pending'),
      )
      if (job.stopping || !job.earlyStopRevived || !revivedPending) break
      addLog(scan, 'warn', `Early-stop revive: rejected match ki wajah se kuch skipped chunks wapas queue me hain — extra scan pass ${pass + 2} start`)
      job.chunkPhaseDone = false
      startVerifyWorkers()
    }

    // All work over. Persist final model states.
    for (const lane of job.lanes) {
      for (const m of MODEL_POOL) {
        const s = this.modelState(job, lane, m)
        if (s.state === 'active' || s.state === 'waiting') s.state = 'idle'
        s.currentChunk = null
      }
    }

    if (job.stopping) {
      scan.status = 'stopped'
      this.buildPartialReport(job)
      addLog(scan, 'warn', 'Scan stopped. Pending chunks saved — use Resume to continue. Ab tak ke results (unverified samet) export/preview ke liye ready hain.')
      this.finish(job)
      return
    }

    const groups = scan.candidateGroups || []
    const leftover = groups.filter((g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning')
    const chunksLeft = segments.some((s) => s.chunks.some((c) => c.status === 'pending' || c.status === 'scanning'))

    // Quota ran out mid-scan/mid-verification: keep the scan resumable instead of finishing.
    if (leftover.length > 0 || chunksLeft) {
      scan.status = 'stopped'
      this.buildPartialReport(job)
      addLog(
        scan,
        'warn',
        leftover.length > 0
          ? `Verification paused: ${leftover.length} group(s) still pending (daily quota exhausted?). Use Resume to continue.`
          : 'Scan paused: chunks still pending (daily quota exhausted?). Use Resume to continue.',
      )
      this.finish(job)
      return
    }

    for (const seg of segments) if (seg.status !== 'done') seg.status = 'done'

    scan.status = 'done'
    scan.finishedAt = Date.now()
    scan.matches.sort((a, b) => a.shortStart - b.shortStart || a.movieStart - b.movieStart)
    scan.report = this.reportStats(job, false)
    addLog(
      scan,
      'success',
      `Scan complete: ${scan.matches.length} matched segment(s)${multi ? ` across ${segments.length} short minute(s)` : ''} — ${scan.prefilter?.mode === 'prefiltered' ? 'Twelve Labs pre-filtered scan' : 'Full scan'}`,
    )
    // COVERAGE: kitna short actually merge me ja raha hai — MISSING hisse LOUD.
    this.logCoverage(job)
    cleanupClips(path.join(scanMediaDir(scan.id), 'clips'))
    addLog(scan, 'info', 'Temporary clip files cleaned up (movie chunks saved for future scan reuse)')
    this.finish(job)
  } finally {
    if (releaseChunkLock) releaseChunkLock()
  }
}

  /**
   * MATCHTOOL ORCHESTRATOR CHUNK SCANNER (choke -> burst -> failover -> recovery)
   * Handles chunk scanning across all active minute segments unifiedly using OrchestratorCore (lib/orchestrator-core.ts).
   * Automatically queues chunks from subsequent minutes so full 10-burst (or 4-burst on 3.7/3.8) parallelism is utilized.
   */
  private async runOrchestratorChunkScan(job: Job, targetSegments: ShortSegmentState[]) {
    const { scan } = job
    // Reset orchestrator state cleanly
    globalOrchestrator.reset()

    // 1. Register active worker keys into orchestrator (slots 2..13, 15, 16)
    for (const lane of job.lanes) {
      globalOrchestrator.registerKey(lane.idx, lane.apiKey)
    }

    // 2. Load all unfinished chunks across ALL target segments
    const allPendingChunks: { index: number; segIndex: number; attempts: number; lastError?: string }[] = []

    for (const seg of targetSegments) {
      for (const c of seg.chunks) {
        const isFinished = c.status === 'match' || c.status === 'no_match'
        if (!isFinished && c.status !== 'cancelled' && !c.skippedEarlyStop) {
          c.status = 'pending'
          allPendingChunks.push({
            index: c.index,
            segIndex: seg.index,
            attempts: c.requestCount || c.attempts || 0,
            lastError: c.lastError,
          })
        }
      }
    }

    if (allPendingChunks.length === 0) {
      addLog(scan, 'info', `[choke_primary] All chunks across all selected minutes are already complete.`)
      return
    }

    addLog(
      scan,
      'info',
      `[choke_primary] Initializing OrchestratorCore across ${targetSegments.length} minute segment(s) (${allPendingChunks.length} chunk(s) pending/failed across ${job.lanes.length} worker keys) — starting choke→burst cycle`,
    )

    // Load chunks into OrchestratorCore
    globalOrchestrator.prepareChunks(scan.id, allPendingChunks)

    // PRE-WARMING PIPELINE (Zero wait): Pre-upload short minute segments across all keys
    // and pre-cut pending chunk files in the background so when choke succeeds,
    // burst requests fire with ZERO wait!
    for (const lane of job.lanes) {
      for (const seg of targetSegments) {
        if (seg.chunks.some((c) => c.status === 'pending')) {
          void this.ensureSegmentUri(job, lane, seg).catch(() => {})
        }
      }
    }
    for (const item of allPendingChunks) {
      void this.ensureChunkFile(scan, item.index).catch(() => {})
    }

    // Stream logs to scan.logs and sync scan.orchestrator
    const unsubscribeLog = globalOrchestrator.onLog((event) => {
      const state = globalOrchestrator.getState()
      scan.orchestrator = state
      const level: LogEntry['level'] =
        event.type === 'error'
          ? 'error'
          : event.type === 'warn'
            ? 'warn'
            : event.outcome === 'success'
              ? 'success'
              : 'info'
      addLog(scan, level, `[${state.phase}] ${event.message}`)
      job.dirty = true
    })

    // Setup chunk executor
    globalOrchestrator.setChunkExecutor(
      async (chunkIndex, keySlot, apiKey, model, timeoutMs, signal, segIndex) => {
        if (job.stopping) {
          return { ok: false, isTimeout: true, error: 'Scan stopped by user' }
        }

        const seg = targetSegments.find((s) => s.index === segIndex) || scan.shortSegments?.[segIndex] || job.seg
        if (!seg) {
          return { ok: false, error: `Minute segment ${segIndex} not found` }
        }

        scan.currentShortSegment = seg.index
        job.seg = seg
        scan.chunks = seg.chunks

        const lane = job.lanes.find((l) => l.idx === keySlot) || {
          idx: keySlot,
          apiKey,
          ai: getClient(apiKey),
          segUris: new Map(),
          segUriPromises: new Map(),
          chunkUploads: new Map(),
          verifyActive: 0,
          verifyActiveByModel: new Map(),
        }

        const modelSpec =
          MODEL_POOL.find((m) => m.id === model) ||
          CHUNK_MODEL_POOL.find((m) => m.id === model) || {
            id: model,
            label: model,
            rpm: 5,
            rpd: 20,
            tpm: 1_000_000,
            contextWindow: 1_000_000,
            color: 'emerald',
            role: 'primary',
          }

        try {
          const c = seg.chunks[chunkIndex] || scan.chunks[chunkIndex]
          if (c && c.status !== 'match' && c.status !== 'no_match') {
            c.status = 'pending'
          }
          await this.processChunk(job, lane, modelSpec, chunkIndex, seg)

          const finishedChunk = seg.chunks[chunkIndex] || scan.chunks[chunkIndex]
          const isSuccess = finishedChunk && (finishedChunk.status === 'match' || finishedChunk.status === 'no_match')
          if (!isSuccess) {
            const lastErr = finishedChunk?.lastError || 'Chunk scan did not complete with match or no_match'
            const lower = lastErr.toLowerCase()
            const is503 =
              lower.includes('503') ||
              lower.includes('high demand') ||
              lower.includes('overloaded') ||
              lower.includes('unavailable') ||
              lower.includes('capacity') ||
              lower.includes('spikes in demand')
            const is429 =
              lower.includes('429') ||
              lower.includes('quota') ||
              lower.includes('resource_exhausted')
            const isTimeout =
              signal.aborted ||
              lower.includes('timeout') ||
              lower.includes('timed out')

            return {
              ok: false,
              is503,
              is429,
              isTimeout,
              error: lastErr,
            }
          }

          // Check if this minute segment has completed all its chunks
          const hasUnfinished = seg.chunks.some((c) => c.status !== 'match' && c.status !== 'no_match' && c.status !== 'cancelled')
          if (!hasUnfinished && seg.status !== 'done') {
            const segGroups = (scan.candidateGroups || []).filter((g) => g.shortStart < seg.end && g.shortEnd > seg.start)
            const unresolved =
              scan.verifierEnabled === false
                ? []
                : segGroups.filter((g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning')
            seg.status = unresolved.length > 0 ? 'verifying' : 'done'
            addLog(scan, 'success', `Minute ${seg.index + 1}/${targetSegments.length} chunk scan complete — candidates are verifying in background.`)
          }

          // Catch any candidates merged by this chunk
          this.enqueueNewGroups(job)
          return { ok: true }
        } catch (err: unknown) {
          const c = seg.chunks[chunkIndex] || scan.chunks[chunkIndex]
          if (c && c.status !== 'match' && c.status !== 'no_match') {
            c.status = 'pending'
          }
          const msg = err instanceof Error ? err.message : String(err)
          const lower = msg.toLowerCase()
          const is503 =
            lower.includes('503') ||
            lower.includes('high demand') ||
            lower.includes('overloaded') ||
            lower.includes('unavailable') ||
            lower.includes('capacity') ||
            lower.includes('spikes in demand')
          const is429 =
            lower.includes('429') ||
            lower.includes('quota') ||
            lower.includes('resource_exhausted')
          const isTimeout =
            signal.aborted ||
            lower.includes('timeout') ||
            lower.includes('timed out')

          return {
            ok: false,
            is503,
            is429,
            isTimeout,
            error: msg,
          }
        }
      },
    )

    try {
      await globalOrchestrator.runOrchestrationLoop()
    } finally {
      unsubscribeLog()
      scan.orchestrator = globalOrchestrator.getState()
      job.dirty = true
      this.mark(job)
    }
  }

  // ---------- Candidate + Verifier pipeline ----------

  /** Group all chunk-phase matches by short segment: matches from different chunks that claim the
   *  same short-video scene (lesser or greater duration) become candidates of ONE group.
   *  Candidates are unlimited — every distinct movie window or chunk finding is saved. */
  private buildCandidateGroups(scan: Scan) {
    const groups: CandidateGroup[] = scan.candidateGroups || []
    const sorted = [...(scan.matches || [])]
      .sort((a, b) => a.shortStart - b.shortStart || a.movieStart - b.movieStart)
    for (const m of sorted) {
      let g = groups.find((x) => sameShortSegment(x.shortStart, x.shortEnd, m.shortStart, m.shortEnd))
      if (!g) {
        g = {
          id: `g${groups.length}-${Math.random().toString(36).slice(2, 8)}`,
          shortStart: m.shortStart,
          shortEnd: m.shortEnd,
          status: m.verified ? 'confirmed' : 'pending',
          candidates: [],
          confirmedIndex: m.verified ? 0 : null,
          confirmedViaRescan: false,
          attempts: 0,
          // PROVENANCE: gap-backup matches carry their origin; everything else is the chunk scan.
          origin: m.origin ?? 'chunk',
          originWindow: m.originWindow,
        }
        groups.push(g)
      }
      // Check if this exact chunk match is already a candidate in this group
      const dup = g.candidates.some(
        (c) =>
          c.chunkIndex === m.chunkIndex &&
          Math.abs(c.movieStart - m.movieStart) < 0.5 &&
          Math.abs(c.movieEnd - m.movieEnd) < 0.5,
      )
      if (!dup) {
        g.candidates.push({
          shortStart: m.shortStart,
          shortEnd: m.shortEnd,
          movieStart: m.movieStart,
          movieEnd: m.movieEnd,
          chunkIndex: m.chunkIndex,
          model: m.model,
          confidence: m.confidence,
          verdict: m.verified ? 'same' : m.rejected ? 'different' : 'pending',
          rescan: 'none',
        })
        // If this match has longer coverage for the scene and is confirmed/active, update group range
        const mDur = m.shortEnd - m.shortStart
        const gDur = g.shortEnd - g.shortStart
        if (mDur > gDur && (m.verified || (m.confidence || 0) >= 0.5)) {
          g.shortStart = Math.min(g.shortStart, m.shortStart)
          g.shortEnd = Math.max(g.shortEnd, m.shortEnd)
        }
        // New evidence for an already-finished group (manual chunk retry) — reopen it if unverified.
        if (g.status === 'rejected' || g.status === 'unverified') g.status = 'pending'
      }

      // CRITICAL: If m has userPick, ensure g records it as userPick so applyGroupMatches preserves it!
      if (m.userPick) {
        const pickIdx = g.candidates.findIndex(
          (c) =>
            Math.abs(c.movieStart - m.movieStart) < 0.5 &&
            Math.abs(c.movieEnd - m.movieEnd) < 0.5,
        )
        if (pickIdx >= 0) {
          g.userPick = { index: pickIdx, viaRescan: !!m.viaRescan, at: Date.now() }
          g.status = 'confirmed'
          g.confirmedIndex = pickIdx
        }
      }
    }
    scan.candidateGroups = groups
    // Keep scan.matches cleanly synchronized: exactly one active match per candidate group
    for (const g of groups) {
      applyGroupMatches(scan, g)
    }
  }

  /** Reset transient candidate-group states left over from an interrupted run. */
  private prepareVerifyState(scan: Scan) {
    for (const g of scan.candidateGroups || []) {
      if (g.status === 'verifying' || g.status === 'rescanning') g.status = 'pending'
      for (const c of g.candidates) {
        if (c.verdict === 'verifying') c.verdict = 'pending'
        if (c.rescan === 'rescanning') c.rescan = 'pending'
        if (c.rescanVerdict === 'verifying') c.rescanVerdict = 'pending'
      }
    }
  }

  // ---------- TL confidence ordering + EARLY-STOP quick-confirm system ----------

  /** TwelveLabs confidence of a chunk within a minute (higher = scan/verify first). */
  private chunkConf(seg: ShortSegmentState, chunkIndex: number): number {
    return seg.chunkConfidence?.[String(chunkIndex)] ?? -1
  }

  /** Order chunk indexes by TL confidence HIGH → LOW (ties: natural order). */
  private orderByConfidence(seg: ShortSegmentState, indexes: number[]): number[] {
    return [...indexes].sort((a, b) => this.chunkConf(seg, b) - this.chunkConf(seg, a) || a - b)
  }

  /** Find the short minute a short-video timestamp belongs to. */
  private segForShortTime(scan: Scan, t: number): ShortSegmentState | null {
    return (scan.shortSegments || []).find((s) => t >= s.start && t < s.end) || null
  }

  /** Verify-priority of a candidate group = best TL confidence among its chunks. */
  private groupConfidence(scan: Scan, g: CandidateGroup): number {
    const seg = this.segForShortTime(scan, g.shortStart)
    if (!seg) return -1
    let best = -1
    for (const c of g.candidates) best = Math.max(best, this.chunkConf(seg, c.chunkIndex))
    return best
  }

  /** EARLY-STOP QUICK-CONFIRM (quota saver, accuracy-safe):
   *  Jab CURRENT minute ke SAB TwelveLabs expected windows par candidate mil
   *  chuke hain AUR har matched chunk ka kam se kam EK segment verifier se
   *  SAME confirm ho chuka hai — to us minute ke bache hue chunks scan karna
   *  band (cancelled + skippedEarlyStop). Baaki matches background me poori
   *  tarah verify hote rehte hain. Agar baad me koi group reject ho jaye to
   *  reviveEarlyStopSkipped() skipped chunks wapas queue me daal deta hai. */
  private checkEarlyStop(job: Job) {
    const { scan } = job
    const seg = job.seg
    if (!seg || job.stopping) return
    const windows = seg.tlWindows
    if (!windows || windows.length === 0) return
    // Kuch skip karne layak bacha bhi hai?
    if (!seg.chunks.some((c) => c.status === 'pending')) return

    const groups = (scan.candidateGroups || []).filter(
      (g) => g.shortStart < seg.end && g.shortEnd > seg.start && g.status !== 'rejected',
    )
    // 1) COVERAGE: har expected TL window par kam se kam ek candidate group ho.
    for (const w of windows) {
      if (!groups.some((g) => g.shortStart < w.end && g.shortEnd > w.start)) return
    }
    // 2) QUICK-CONFIRM: har contributing chunk ka apna 1 candidate SAME confirm ho
    //    (poore group ki jagah sirf 1 segment per chunk — baaki background me).
    const matchedChunks = new Set<number>()
    for (const g of groups) for (const c of g.candidates) matchedChunks.add(c.chunkIndex)
    if (matchedChunks.size === 0) return
    for (const ci of matchedChunks) {
      const confirmed = groups.some((g) =>
        g.candidates.some((c) => c.chunkIndex === ci && (c.verdict === 'same' || c.rescanVerdict === 'same')),
      )
      if (!confirmed) return
    }

    // Sab conditions poori — is minute ke bache chunks skip karo.
    let saved = 0
    for (const c of seg.chunks) {
      if (c.status === 'pending') {
        c.status = 'cancelled'
        c.skippedEarlyStop = true
        saved++
      }
    }
    job.queue = job.queue.filter((ci) => seg.chunks[ci]?.status === 'pending')
    seg.earlyStopSavedChunks = saved
    if (saved > 0) {
      addLog(
        scan,
        'success',
        `EARLY-STOP — Minute ${seg.index + 1}: sab ${windows.length} expected window(s) covered + har matched chunk (${matchedChunks.size}) ka 1 segment SAME confirm — bache ${saved} chunk(s) skip, quota saved. Baaki matches background me verify ho rahe hain.`,
      )
      this.mark(job)
    }
  }

  /** Group REJECT hone par: agar uske minute me early-stop se skip hue chunks
   *  hain aur us short window ka ab koi zinda candidate nahi bacha, to skipped
   *  chunks wapas pending karke scan me daalo — sahi match ab bhi mil sakta hai. */
  private reviveEarlyStopSkipped(job: Job, g: CandidateGroup) {
    const { scan } = job
    const seg = this.segForShortTime(scan, g.shortStart)
    if (!seg) return
    const skipped = seg.chunks.filter((c) => c.status === 'cancelled' && c.skippedEarlyStop)
    if (skipped.length === 0) return
    // Kya is short window par ab bhi koi non-rejected group hai? Ho to revive ki zaroorat nahi.
    const stillCovered = (scan.candidateGroups || []).some(
      (x) => x !== g && x.status !== 'rejected' && sameShortSegment(x.shortStart, x.shortEnd, g.shortStart, g.shortEnd),
    )
    if (stillCovered) return
    for (const c of skipped) {
      c.status = 'pending'
      delete c.skippedEarlyStop
    }
    delete seg.earlyStopSavedChunks
    if (seg.status === 'done' || seg.status === 'verifying') seg.status = 'pending'
    job.earlyStopRevived = true
    // Agar yehi minute abhi bhi active hai to chunks LIVE queue me wapas jaate
    // hain; warna scan-loop ka agla pass unhe utha lega.
    if (job.seg === seg) {
      job.queue.push(...this.orderByConfidence(seg, skipped.map((c) => c.index)))
    }
    addLog(
      scan,
      'warn',
      `Minute ${seg.index + 1}: match reject hone se coverage toot gayi — early-stop se skip hue ${skipped.length} chunk(s) wapas scan queue me (accuracy first)`,
    )
    this.mark(job)
  }

  /** Incrementally (re)build candidate groups from fresh unverified matches and
   *  push every pending group that is not already queued or in flight onto the
   *  verify queue. Called after EVERY finished chunk so verification starts the
   *  moment a chunk's candidates exist — the verify pipeline never waits for
   *  the whole chunk phase. Safe against double-queueing (single-threaded). */
  private enqueueNewGroups(job: Job, logResume = false) {
    const { scan } = job
    this.buildCandidateGroups(scan)
    if (scan.verifierEnabled === false) {
      // Verifier OFF: settle all pending candidate groups as unverified immediately
      for (const g of scan.candidateGroups || []) {
        if (g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning') {
          g.status = 'unverified'
          this.applyGroupResult(job, g)
        }
      }
      return
    }
    const groups = scan.candidateGroups || []
    let added = 0
    for (let i = 0; i < groups.length; i++) {
      if (groups[i].status !== 'pending') continue
      if (job.verifyQueue.includes(i) || job.verifyInFlight.has(i)) continue
      job.verifyQueue.push(i)
      added++
    }
    if (added > 0) {
      // ORDER 1 — CHUNK COVERAGE FIRST (early-stop accelerator): jis group ke
      // chunks me se koi bhi abhi tak quick-confirm NAHI hua, wo group PEHLE
      // verify hota hai — taaki har matched chunk ka 1 segment jaldi se jaldi
      // SAME confirm ho aur early-stop turant fire kare. Ek hi chunk ke kai
      // groups ko baar-baar pehle verify karne me quota lagta hai, coverage nahi badhti.
      // ORDER 2 — CONFIDENCE (TwelveLabs): high→low.
      const confirmedChunks = new Set<number>()
      for (const g of groups) {
        for (const c of g.candidates) {
          if (c.verdict === 'same' || c.rescanVerdict === 'same') confirmedChunks.add(c.chunkIndex)
        }
      }
      const coversNewChunk = (g: CandidateGroup): number =>
        g.candidates.some((c) => !confirmedChunks.has(c.chunkIndex)) ? 1 : 0
      job.verifyQueue.sort(
        (a, b) =>
          coversNewChunk(groups[b]) - coversNewChunk(groups[a]) ||
          this.groupConfidence(scan, groups[b]) - this.groupConfidence(scan, groups[a]),
      )
    }
    if (added > 0) {
      addLog(
        scan,
        'info',
        logResume
          ? `Verification resume: ${added} candidate group(s) still pending — queued across ${job.lanes.length} API key(s) × ${VERIFY_MODEL_POOL.length} verify models (${VERIFY_CONCURRENCY_PER_MODEL} req/model in parallel)`
          : `Verify pipeline: ${added} new candidate group(s) queued (${job.verifyQueue.length} waiting, ${job.verifyInFlight.size} in flight) — chunk models keep scanning in parallel`,
      )
      this.mark(job)
    }
  }

  /** One verifier worker per (key lane × model × concurrency slot) — pulls whole groups off the queue. */
  private async verifyWorker(job: Job, lane: KeyLane, m: ModelSpec, slot: number = 0) {
    const { scan } = job
    while (true) {
      if (job.stopping || scan.verifierEnabled === false) return
      const st = this.modelState(job, lane, m)

      if (globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd)) {
        if (st.state !== 'exhausted') {
          st.state = 'exhausted'
          st.currentChunk = null
          this.mark(job)
        }
        return
      }

      const cool = job.cooldownUntil[this.rateKey(lane, m)] || 0
      const modelActive = lane.verifyActiveByModel?.get(m.id) || 0

      if (cool > Date.now()) {
        st.state = 'cooling'
        st.cooldownUntil = cool
        this.mark(job)
        await sleep(Math.min(2000, cool - Date.now()))
        continue
      } else {
        st.cooldownUntil = null
      }

      // PER-KEY & PER-MODEL VERIFIER CAP:
      // 2 models × 3 requests = 6 parallel requests per key (30 requests across 5 keys).
      if (modelActive >= VERIFY_CONCURRENCY_PER_MODEL || lane.verifyActive >= VERIFY_PER_LANE) {
        if (job.verifyQueue.length === 0 && job.verifyInFlight.size === 0 && job.chunkPhaseDone) return
        st.state = 'waiting'
        await sleep(250)
        continue
      }

      const gi = job.verifyQueue.shift()
      if (gi === undefined) {
        // PIPELINE PARALLELISM: while the chunk phase is still running, verify
        // workers idle-wait — new candidate groups arrive as chunks finish.
        // Only exit once the chunk phase is done AND nothing is in flight AND no groups remain pending.
        if (job.verifyInFlight.size === 0 && job.chunkPhaseDone) {
          const unverifiedLeft = (scan.candidateGroups || []).some(
            (item) => item.status === 'pending',
          )
          if (unverifiedLeft) {
            this.enqueueNewGroups(job)
            await sleep(250)
            continue
          }
          if (st.state !== 'idle') {
            st.state = 'idle'
            this.mark(job)
          }
          return
        }
        st.state = 'waiting'
        await sleep(150)
        continue
      }

      const g = (scan.candidateGroups || [])[gi]
      if (!g || (g.status !== 'pending' && g.status !== 'verifying' && g.status !== 'rescanning')) continue

      // SUPERSEDED: a confirmed group already covers this short window (adjacent
      // chunks 66/67 both "found" the same short second at the boundary). Skip
      // the verifier call and settle it as rejected so it never lands in the
      // report table / export as a competing "no" row.
      const winner = this.confirmedCovering(scan, g)
      if (winner) {
        this.supersedeGroup(job, g, winner)
        continue
      }

      job.verifyInFlight.add(gi)
      lane.verifyActive++
      lane.verifyActiveByModel.set(m.id, (lane.verifyActiveByModel.get(m.id) || 0) + 1)
      st.state = 'active'
      this.mark(job)

      try {
        await this.processGroup(job, lane, m, g, slot)
      } catch (err) {
        const e = err instanceof GeminiError ? err : classifyError(err)
        if (e.kind === 'invalid_key') {
          const laneState = job.scan.keyLanes.find((l) => l.idx === lane.idx)
          if (laneState) {
            laneState.status = 'error'
            laneState.lastError = 'API key invalid or expired'
          }
          job.verifyQueue.push(gi)
          addLog(scan, 'error', `Verifier: API Key ${lane.idx} is invalid/expired — disabled for this scan; group ${g.id} re-queued for another key`)
        } else if (e.kind === 'rpd') {
          const used = getModelUsage(m.id, lane.apiKey)
          if (used >= m.rpd) {
            globalGeminiCoordinator.reportExhausted(lane.apiKey, m.id, 0, m.rpd)
            setModelExhausted(m.id, lane.apiKey, m.rpd)
            const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
            if (laneState) {
              const ms = laneState.models.find((item) => item.id === m.id)
              if (ms) ms.state = 'exhausted'
            }
            job.verifyQueue.push(gi) // another (key × model) worker retries the same work
            addLog(scan, 'warn', `Verifier: ${m.id} (key ${lane.idx}) model daily setting quota reached (${used}/${m.rpd} RPD) — group ${g.id} re-queued for another worker (key ${lane.idx}'s other models remain active)`)
          } else {
            const coolUntil = Date.now() + RATE_COOLDOWN_MS
            job.cooldownUntil[this.rateKey(lane, m)] = coolUntil
            globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, RATE_COOLDOWN_MS, slot)
            st.state = 'cooling'
            st.cooldownUntil = coolUntil
            job.verifyQueue.push(gi)
            addLog(scan, 'warn', `Verifier: rate limit on ${displayModelName(m.id)} (key ${lane.idx}, used ${used}/${m.rpd} RPD) — group ${g.id} re-queued; model cooling for 1 min`)
          }
        } else if (e.kind === 'rate') {
          const coolUntil = Date.now() + RATE_COOLDOWN_MS
          job.cooldownUntil[this.rateKey(lane, m)] = coolUntil
          globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, RATE_COOLDOWN_MS, slot)
          st.state = 'cooling'
          st.cooldownUntil = coolUntil
          job.verifyQueue.push(gi)
          addLog(scan, 'warn', `Verifier: rate limit on ${displayModelName(m.id)} (key ${lane.idx}) — group ${g.id} re-queued; model cooling for 1 min`)
        } else {
          g.attempts += 1
          if (g.attempts >= MAX_GROUP_ATTEMPTS) {
            g.status = 'unverified'
            this.applyGroupResult(job, g)
            addLog(scan, 'error', `Group ${g.id} (short ${ts(g.shortStart)}–${ts(g.shortEnd)}) could not be verified after ${g.attempts} attempts — original match kept, flagged unverified: ${e.message.slice(0, 120)} ${originTag(groupMatchOrigin(g, false), g.candidates[0]?.chunkIndex, g.originWindow)}`)
          } else {
            job.verifyQueue.push(gi)
            addLog(scan, 'warn', `Verifier attempt ${g.attempts} failed for group ${g.id} on ${m.id} (key ${lane.idx}) — re-queued: ${e.message.slice(0, 120)}`)
          }
        }
        this.mark(job)
      } finally {
        job.verifyInFlight.delete(gi)
        globalGeminiCoordinator.clearVerifyRetryLock(lane.apiKey, m.id, g.id)
        lane.verifyActive = Math.max(0, lane.verifyActive - 1)
        const rem = Math.max(0, (lane.verifyActiveByModel.get(m.id) || 1) - 1)
        lane.verifyActiveByModel.set(m.id, rem)
        if (rem === 0 && st.state === 'active') st.state = 'idle'
        this.mark(job)
      }
    }
  }

  /** Fraction of `inner` (short-video window) covered by `outer`. */
  private coverage(innerStart: number, innerEnd: number, outerStart: number, outerEnd: number): number {
    const len = innerEnd - innerStart
    if (len <= 0) return 0
    const overlap = Math.min(innerEnd, outerEnd) - Math.max(innerStart, outerStart)
    return overlap <= 0 ? 0 : overlap / len
  }

  /** A CONFIRMED group whose short window covers ≥ 80% of this group's short window, if any. */
  private confirmedCovering(scan: Scan, g: CandidateGroup): CandidateGroup | null {
    for (const other of scan.candidateGroups || []) {
      if (other === g || other.status !== 'confirmed') continue
      if (this.coverage(g.shortStart, g.shortEnd, other.shortStart, other.shortEnd) >= 0.8) return other
    }
    return null
  }

  /** Settle a group without a verifier call because a confirmed group already
   *  owns its short window. Counted as rejected (so totals still add up) and
   *  every candidate row is dropped from scan.matches. */
  private supersedeGroup(job: Job, g: CandidateGroup, winner: CandidateGroup) {
    const { scan } = job
    g.status = 'rejected'
    for (const c of g.candidates) {
      if (c.verdict === 'pending' || c.verdict === 'verifying') {
        c.verdict = 'different'
        c.verifierReason = `superseded — short ${ts(winner.shortStart)}–${ts(winner.shortEnd)} already confirmed (group ${winner.id})`
      }
    }
    this.applyGroupResult(job, g)
    addLog(
      scan,
      'info',
      `Group ${g.id} (short ${ts(g.shortStart)}–${ts(g.shortEnd)}) superseded by confirmed group ${winner.id} — ${g.candidates.length} duplicate candidate(s) dropped, no verifier call`,
    )
  }

  /** After a group is CONFIRMED: drop competing rows for the same short window.
   *  Adjacent chunks (66/67, 68/69 — 1-min boundary overlap) both report the same
   *  short second, so a confirmed 0:06–0:07 used to sit next to two "no" rows
   *  (1:07:00, 1:07:07) in the table and export. Pending/unfinished groups that the
   *  winner covers are settled as superseded; loose unverified matches are removed. */
  private pruneSuperseded(job: Job, winner: CandidateGroup) {
    const { scan } = job
    for (const g of scan.candidateGroups || []) {
      if (g === winner || g.status === 'confirmed') continue
      if (g.status !== 'pending' && g.status !== 'unverified') continue
      if (this.coverage(g.shortStart, g.shortEnd, winner.shortStart, winner.shortEnd) >= 0.8) {
        this.supersedeGroup(job, g, winner)
      }
    }
    scan.matches = (scan.matches || []).filter(
      (m) => m.verified === true || m.userPick === true || this.coverage(m.shortStart, m.shortEnd, winner.shortStart, winner.shortEnd) < 0.8,
    )
  }

  /** Pace + count one verifier/rescan request on this (key × model × slot) lane.
   *  Pacing is sized from the ACTUAL video seconds so small verify clips only
   *  wait seconds while full 1-minute rescans wait the whole minute — full TPM capacity. */
  private async paceAndSend<T>(
    job: Job,
    lane: KeyLane,
    m: ModelSpec,
    videoSeconds: number,
    fn: () => Promise<T>,
    slot: number = 0,
    isVerify: boolean = false,
    verifyLockId?: string,
  ): Promise<T> {
    // User rule: Do not block models even if RPD is reached ("rpd khtm hone pr un model ko block mat krna req fir bhi kr ske")
    const pk = this.paceSlotKey(lane, m, slot)
    const rk = this.rateKey(lane, m)
    const st = this.modelState(job, lane, m)

    let releaseGlobalLock: ((sec?: number) => void) | null = null
    try {
      releaseGlobalLock = await globalGeminiCoordinator.acquireLane({
        scanId: job.scan.id,
        scanTitle: job.scan.shortName || job.scan.id,
        apiKey: lane.apiKey,
        keyIdx: lane.idx,
        modelId: m.id,
        slot,
        operation: `${isVerify ? 'Verify' : 'Chunk/Rescan'} on ${m.id}`,
        videoSeconds,
        rpd: m.rpd || 500,
        isVerify,
        verifyLockId,
        onWait: (msg) => {
          st.state = 'waiting'
          addLog(job.scan, 'info', msg)
          this.mark(job)
        },
        isStopping: () => job.stopping,
      })

      const wait = (job.nextFreeAt[pk] || 0) - Date.now()
      if (wait > 0) {
        st.state = 'waiting'
        this.mark(job)
        await this.stoppableSleep(job, wait)
      }
      // STOP CHECK: quota consume karne se PEHLE nikal jao — 'rate' kind se group
      // bina attempt-penalty ke re-queue hota hai aur worker loop stopping par exit karta hai.
      if (job.stopping) throw new GeminiError('rate', 'Stop requested — request cancelled before send')
      st.state = 'active'
      job.nextFreeAt[pk] = Date.now() + pacingIntervalMs(videoSeconds)
      try {
        const rawRes = await fn()
        st.usedToday = incrementModelUsage(m.id, lane.apiKey)
        if (isVerify) {
          const outcome = globalGeminiCoordinator.recordVerifySuccess(lane.apiKey, m.id, slot, verifyLockId)
          if (outcome.cooldownTriggered) {
            job.cooldownUntil[rk] = outcome.cooldownUntil
            job.cooldownUntil[pk] = outcome.cooldownUntil
            st.state = 'cooling'
            st.cooldownUntil = outcome.cooldownUntil
            addLog(
              job.scan,
              'info',
              `Verifier: 3 successful requests completed on ${displayModelName(m.id)} (key ${lane.idx}) — 30s cooldown started. Next requests will be prepared and held ready.`,
            )
          } else if (outcome.retryCleared) {
            addLog(
              job.scan,
              'success',
              `Verifier: 429 priority retry cleared on ${displayModelName(m.id)} (key ${lane.idx}). Priority lock released; continuing remaining slots (${outcome.currentCount}/3 in batch).`,
            )
          }
        } else {
          globalGeminiCoordinator.recordSuccess(lane.apiKey, m.id, slot)
        }
        this.mark(job)
        return rawRes
      } catch (err) {
        const e = err instanceof GeminiError ? err : classifyError(err)
        if (e.kind === 'rate' || e.kind === 'rpd') {
          const googleDelayMs = extractGoogleRetryDelayMs(err) ?? e.retryDelayMs
          const { effectiveCooldownMs } = calculateEffectiveCooldownMs(googleDelayMs, isVerify ? RATE_COOLDOWN_MS : CHUNK_COOLDOWN_MS)
          const outcome = globalGeminiCoordinator.handleQuotaOrRateError(
            lane.apiKey,
            m.id,
            slot,
            m.rpd || 20,
            e.kind === 'rpd',
            effectiveCooldownMs,
            lane.idx,
          )
          const used = getModelUsage(m.id, lane.apiKey)
          if (outcome.action === 'exhausted' && used >= (m.rpd || 20)) {
            setModelExhausted(m.id, lane.apiKey, m.rpd)
            st.state = 'exhausted'
          } else {
            job.cooldownUntil[pk] = Date.now() + effectiveCooldownMs
            st.state = 'cooling'
          }
        }
        if (isVerify) {
          globalGeminiCoordinator.clearVerifyRetryLock(lane.apiKey, m.id, verifyLockId)
        }
        throw err
      }
    } finally {
      if (releaseGlobalLock) releaseGlobalLock(videoSeconds)
    }
  }

  /** BUSY-RETRY & 1-MINUTE RATE-LIMIT COOLDOWN (verify/rescan):
   *  Primary request is sent with the already-uploaded clips.
   *  On 429 rate limit: holds the prepared clips ready in memory, enforces a 1-minute (60s)
   *  cooldown on that (key × model) lane (notifying globalGeminiCoordinator and the UI),
   *  and as soon as the 1 minute expires, immediately sends the prepared request without re-extracting or re-uploading.
   *  On 503/server overload: retries after 2s backoff. */
  private async sendWithClipBackup(
    job: Job,
    lane: KeyLane,
    filePath: string,
    mainUri: string,
    uploadedNames: string[],
    send: (uri: string) => Promise<string>,
    busyLabel: string,
    m?: ModelSpec,
    slot: number = 0,
    isVerify: boolean = false,
    verifyLockId?: string,
  ): Promise<string> {
    let rateRetries = 0
    const maxRateRetries = 2

    while (true) {
      if (job.stopping) throw new GeminiError('rate', 'Stop requested')
      try {
        return await send(mainUri)
      } catch (err) {
        const e = err instanceof GeminiError ? err : classifyError(err)

        if (e.kind === 'rate' && m) {
          rateRetries++
          const rk = this.rateKey(lane, m)
          const pk = this.paceSlotKey(lane, m, slot)
          const st = this.modelState(job, lane, m)

          const googleDelayMs = extractGoogleRetryDelayMs(err) ?? e.retryDelayMs
          const { effectiveCooldownMs } = calculateEffectiveCooldownMs(googleDelayMs, RATE_COOLDOWN_MS)

          if (rateRetries > maxRateRetries) {
            job.cooldownUntil[rk] = Date.now() + effectiveCooldownMs
            job.cooldownUntil[pk] = Date.now() + effectiveCooldownMs
            if (isVerify) {
              globalGeminiCoordinator.clearVerifyRetryLock(lane.apiKey, m.id, verifyLockId)
              globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, effectiveCooldownMs, slot)
            } else {
              globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, effectiveCooldownMs, slot)
            }
            st.state = 'cooling'
            st.cooldownUntil = Date.now() + effectiveCooldownMs
            this.mark(job)
            throw err
          }

          // DYNAMIC COOLDOWN (Google requested delay + 5s buffer):
          const cooldownMs = effectiveCooldownMs
          const coolUntil = Date.now() + cooldownMs

          job.cooldownUntil[rk] = coolUntil
          job.cooldownUntil[pk] = coolUntil
          if (isVerify) {
            globalGeminiCoordinator.reportVerifyRateLimit(lane.apiKey, m.id, slot, verifyLockId, cooldownMs)
          } else {
            globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, cooldownMs, slot)
          }

          st.state = 'cooling'
          st.cooldownUntil = coolUntil
          this.mark(job)

          const googleNote = googleDelayMs ? ` (Google requested ${(googleDelayMs / 1000).toFixed(1)}s + 5s safety buffer)` : ''
          addLog(
            job.scan,
            'warn',
            `Verifier: 429 rate limit on ${displayModelName(m.id)} (key ${lane.idx})${googleNote} — giving ${(cooldownMs / 1000).toFixed(1)}s lock. Priority Lock held; will send first when lock expires (attempt ${rateRetries}/${maxRateRetries}).`,
          )

          const waitMs = coolUntil - Date.now()
          if (waitMs > 0) {
            await this.stoppableSleep(job, waitMs)
          }

          if (job.stopping) {
            if (isVerify) globalGeminiCoordinator.clearVerifyRetryLock(lane.apiKey, m.id, verifyLockId)
            throw new GeminiError('rate', 'Stop requested during verifier cooldown')
          }

          st.cooldownUntil = null
          st.state = 'active'
          this.mark(job)

          addLog(
            job.scan,
            'info',
            `Verifier: ${(cooldownMs / 1000).toFixed(1)}s lock ended for ${displayModelName(m.id)} (key ${lane.idx}) — sending priority retry now!`,
          )
          continue
        }

        // On temporary server errors (503/overload), wait 2s and retry with the active clip URI
        const transient = /overload|busy|503|500|internal|try again|temporarily/i.test(e.message)
        if (!transient) throw err

        addLog(job.scan, 'warn', `${busyLabel}: API server busy (503/overload) — retrying in 2s...`)
        this.mark(job)
        await sleep(2000)
        return await send(mainUri)
      }
    }
  }

  /** Full verify → rescan → re-verify pipeline for ONE candidate group.
   *  All clips are cut with millisecond precision and sent to Gemini at 24 fps. */
  private async processGroup(job: Job, lane: KeyLane, m: ModelSpec, g: CandidateGroup, slot: number = 0) {
    const { scan } = job
    const mediaDir = scanMediaDir(scan.id)
    const clipsDir = path.join(mediaDir, 'clips')
    if (!fs.existsSync(clipsDir)) fs.mkdirSync(clipsDir, { recursive: true })
    const shortDur = Math.max(1, g.shortEnd - g.shortStart)

    // ---- PADDING (only for very small segments) ----
    // Segments shorter than 1.5s are too small for Gemini to compare reliably
    // (a 0.4s clip is ~10 frames). Pad BOTH clips equally on each side so the
    // model gets enough context, and tell it EXACTLY where the real target
    // window sits inside the padded clips (so extra padded scenes never confuse it).
    // Final report timestamps stay ORIGINAL — padding is for verification only.
    const segDur = g.shortEnd - g.shortStart
    const needsPad = segDur < 1.5
    const PAD_EACH = 0.75
    const shortTotal = scan.shortDuration || g.shortEnd
    const padBefore = needsPad ? Math.min(PAD_EACH, Math.max(0, g.shortStart)) : 0
    const padAfter = needsPad ? Math.min(PAD_EACH, Math.max(0, shortTotal - g.shortEnd)) : 0
    const tgtStart = padBefore
    const tgtEnd = padBefore + segDur
    const verifyPadNote = needsPad
      ? `IMPORTANT — PADDING NOTE (dhyan se padho):\nDono clips me asli TARGET SEGMENT sirf ${ts(tgtStart)} se ${ts(tgtEnd)} tak hai (har clip ki apni clock par). Is window ke pehle aur baad ka content sirf PADDING hai jo context ke liye joda gaya hai — wahan alag scene ho sakta hai, us se CONFUSE mat hona. SAME/DIFFERENT ka faisla SIRF target window ${ts(tgtStart)}–${ts(tgtEnd)} ke frames aur audio par karo. Agar target window ka footage exact same hai to VERDICT: SAME do, chahe padding area me kuch bhi ho.`
      : undefined

    // VERIFY models: gemini-3.5-flash-lite + gemini-3.1-flash-lite ONLY (500 RPD each).
    // Instant pre-flight check: ensure quota remaining before cutting clips or uploading
    const pickVerifyModel = (): ModelSpec => {
      if (!globalGeminiCoordinator.isModelExhausted(lane.apiKey, m.id, m.rpd)) return m
      const fallback = VERIFY_MODEL_POOL.find((x) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, x.id, x.rpd))
      if (!fallback) {
        throw new GeminiError(
          'other',
          `Verify models (${VERIFY_MODEL_POOL.map((x) => x.id).join(', ')}) exhausted on key ${lane.idx} — group re-queued for another key`,
        )
      }
      return fallback
    }

    g.status = 'verifying'
    this.mark(job)

    // Cut + upload the short-video segment ONCE for this group (per-key upload).
    // Small segments are cut WITH padding; the padding note tells the model where the target is.
    const shortClipFile = path.join(clipsDir, `${g.id}-short.mp4`)
    await extractClipPrecise(path.join(mediaDir, 'short.mp4'), g.shortStart - padBefore, g.shortEnd + padAfter, shortClipFile)
    const shortClip = await uploadVideo(lane.ai, shortClipFile)
    const uploadedNames: string[] = [shortClip.name]

    try {
      // ----- STEP 1: verify each candidate's exact movie window -----
      for (let i = 0; i < g.candidates.length; i++) {
        if (job.stopping) return
        const c = g.candidates[i]
        if (c.verdict === 'same' || c.verdict === 'different') continue
        c.verdict = 'verifying'
        this.mark(job)

        const movieClipFile = path.join(clipsDir, `${g.id}-c${i}-movie.mp4`)
        await extractClipPrecise(path.join(mediaDir, 'movie.mp4'), Math.max(0, c.movieStart - padBefore), c.movieEnd + padAfter, movieClipFile)
        const movieClip = await uploadVideo(lane.ai, movieClipFile)
        uploadedNames.push(movieClip.name)

        const vm = pickVerifyModel()
        addLog(scan, 'info', `Verify: short ${ts(g.shortStart)}–${ts(g.shortEnd)} vs movie ${ts(c.movieStart)}–${ts(c.movieEnd)}${needsPad ? ' (padded)' : ''} on ${vm.id} (key ${lane.idx})`)
        const clipSecs = shortDur + padBefore + padAfter + Math.max(1, c.movieEnd - c.movieStart) + padBefore + padAfter
        const raw = await this.paceAndSend(
          job,
          lane,
          vm,
          clipSecs,
          () =>
            this.sendWithClipBackup(
              job,
              lane,
              movieClipFile,
              movieClip.uri,
              uploadedNames,
              (uri) => verifyRequest(lane.ai, vm.id, shortClip.uri, uri, verifyPadNote),
              `Verify short ${ts(g.shortStart)}–${ts(g.shortEnd)} on ${vm.id} (key ${lane.idx})`,
              vm,
              slot,
              true,
              g.id,
            ),
          slot,
          true,
          g.id,
        )
        const v = parseVerdict(raw)
        if (!v) throw new GeminiError('other', 'Verifier gave no clear VERDICT line')

        c.verifierModel = vm.id
        c.verifierReason = v.reason
        c.verdict = v.same ? 'same' : 'different'
        this.mark(job)

        if (v.same) {
          g.status = 'confirmed'
          g.confirmedIndex = i
          g.confirmedViaRescan = false
          this.applyGroupResult(job, g)
          addLog(scan, 'success', `CONFIRMED: short ${ts(g.shortStart)}–${ts(g.shortEnd)} = movie ${ts(c.movieStart)}–${ts(c.movieEnd)} (verifier: ${vm.id}) ${originTag(groupMatchOrigin(g, false), c.chunkIndex, g.originWindow)}`)
          // EARLY-STOP QUICK-CONFIRM: is chunk ka 1 segment SAME confirm hua —
          // check karo ki current minute ke bache chunks ab skip ho sakte hain.
          this.checkEarlyStop(job)
          return
        }
        addLog(scan, 'warn', `Verifier says DIFFERENT for movie ${ts(c.movieStart)}–${ts(c.movieEnd)}${g.candidates.length > i + 1 ? ' — checking next candidate' : ''}`)
      }

      // All normal candidates failed verification:
      // Rescan does NOT start inline during normal verification!
      // Group is queued for the separate Rescan Phase (which runs ONLY after all normal candidate verification completes).
      g.status = 'rescanning'
      const gi = (scan.candidateGroups || []).indexOf(g)
      if (gi >= 0 && !job.rescanQueue.includes(gi)) {
        job.rescanQueue.push(gi)
      }
      this.mark(job)
      addLog(
        scan,
        'info',
        `All ${g.candidates.length} candidate(s) rejected for short ${ts(g.shortStart)}–${ts(g.shortEnd)} — queued for Rescan phase (will run after all normal verification completes)`,
      )
      return
    } finally {
      for (const n of uploadedNames) void deleteFileQuiet(lane.ai, n)
      try {
        for (const f of fs.readdirSync(clipsDir)) {
          if (f.startsWith(g.id)) fs.unlinkSync(path.join(clipsDir, f))
        }
      } catch {
        /* ignore */
      }
    }
  }

  /**
   * Rescan phase for ONE candidate group:
   * Runs ONLY after all normal candidate verification has completed.
   * Model is strictly restricted to RESCAN_MODEL_POOL: ['gemini-3.5-flash', 'gemini-3-flash-preview'].
   * DYNAMIC FREE LANE: Searches across ALL keys × rescan models to find a free lane, locks it,
   * and if 429 occurs, cools that model and switches immediately to another free key/model!
   * Re-verify uses the exact same verifier verdict logic.
   */
  private async rescanCandidateGroup(job: Job, g: CandidateGroup): Promise<void> {
    const { scan } = job
    const mediaDir = scanMediaDir(scan.id)
    const clipsDir = path.join(mediaDir, 'clips')
    if (!fs.existsSync(clipsDir)) fs.mkdirSync(clipsDir, { recursive: true })
    const shortDur = Math.max(1, g.shortEnd - g.shortStart)
    const segDur = g.shortEnd - g.shortStart
    const needsPad = segDur < 1.5
    const PAD_EACH = 0.75
    const shortTotal = scan.shortDuration || g.shortEnd
    const padBefore = needsPad ? Math.min(PAD_EACH, Math.max(0, g.shortStart)) : 0
    const padAfter = needsPad ? Math.min(PAD_EACH, Math.max(0, shortTotal - g.shortEnd)) : 0
    const tgtStart = padBefore
    const tgtEnd = padBefore + segDur
    const verifyPadNote = needsPad
      ? `IMPORTANT — PADDING NOTE (dhyan se padho):\nDono clips me asli TARGET SEGMENT sirf ${ts(tgtStart)} se ${ts(tgtEnd)} tak hai (har clip ki apni clock par). Is window ke pehle aur baad ka content sirf PADDING hai jo context ke liye joda gaya hai — wahan alag scene ho sakta hai, us se CONFUSE mat hona. SAME/DIFFERENT ka faisla SIRF target window ${ts(tgtStart)}–${ts(tgtEnd)} ke frames aur audio par karo. Agar target window ka footage exact same hai to VERDICT: SAME do, chahe padding area me kuch bhi ho.`
      : undefined
    const rescanPadNote = needsPad
      ? `IMPORTANT — PADDING NOTE (dhyan se padho):\nVideo 1 me asli TARGET SEGMENT sirf ${ts(tgtStart)} se ${ts(tgtEnd)} tak hai (Video 1 ki apni clock par). Uske pehle aur baad ka content sirf PADDING hai — context ke liye joda gaya hai. HISSA 1 me poora Video 1 map karo, lekin MATCH sirf TARGET window ${ts(tgtStart)}–${ts(tgtEnd)} ke liye do — matched movie window ki duration EXACTLY ${segDur.toFixed(3)}s honi chahiye (padding wali duration NAHI).`
      : undefined

    const shortClipFile = path.join(clipsDir, `${g.id}-short.mp4`)
    await extractClipPrecise(path.join(mediaDir, 'short.mp4'), g.shortStart - padBefore, g.shortEnd + padAfter, shortClipFile)

    // Build rescan candidate lanes across ALL keys in job.lanes × RESCAN_MODEL_POOL
    const rescanCandidateLanes: CandidateLane[] = []
    for (const lane of job.lanes) {
      for (const rm of RESCAN_MODEL_POOL) {
        rescanCandidateLanes.push({
          apiKey: lane.apiKey,
          keyIdx: lane.idx,
          modelId: rm.id,
          rpd: rm.rpd || 500,
          slot: 0,
        })
      }
    }

    // Build verifier candidate lanes across ALL keys in job.lanes × VERIFY_MODEL_POOL
    const verifyCandidateLanes: CandidateLane[] = []
    for (const lane of job.lanes) {
      for (const vm of VERIFY_MODEL_POOL) {
        verifyCandidateLanes.push({
          apiKey: lane.apiKey,
          keyIdx: lane.idx,
          modelId: vm.id,
          rpd: vm.rpd || 500,
          slot: 0,
        })
      }
    }

    try {
      g.status = 'rescanning'
      this.mark(job)

      for (let i = 0; i < g.candidates.length; i++) {
        if (job.stopping) return
        const c = g.candidates[i]
        if (c.rescan === 'not_found' || c.rescanVerdict === 'different') continue

        if (c.rescan !== 'found') {
          c.rescan = 'rescanning'
          this.mark(job)
          const { start: chunkStart, end: chunkEnd } = chunkAbsWindow(scan, c.chunkIndex)

          let chunkFile = chunkPath(path.join(mediaDir, 'chunks'), c.chunkIndex)
          if (!fs.existsSync(chunkFile)) {
            chunkFile = path.join(clipsDir, `${g.id}-c${i}-chunk.mp4`)
            await extractClipPrecise(path.join(mediaDir, 'movie.mp4'), chunkStart, chunkEnd, chunkFile)
          }

          const hintLocalStart = Math.max(0, c.movieStart - chunkStart)
          const hintLocalEnd = Math.max(0, c.movieEnd - chunkStart)
          const rescanHintNote =
            hintLocalEnd > hintLocalStart
              ? `HINT — PEHLE YAHAN DEKHO: chunk-mapping ne Video 2 me ${ts(hintLocalStart)}–${ts(hintLocalEnd)} ke aas-paas match hone ka dawa kiya tha (verifier ne exact wahi window reject ki thi — boundaries galat ho sakti hain). PASS 1 me SABSE PEHLE is region ko ±10 second ke saath frame-by-frame check karo — sahi match aksar isi ke aas-paas thoda shift hua hota hai. LEKIN agar wahan exact match NA mile to poora Video 2 (audio samet) zaroor scan karo — hint galat bhi ho sakta hai.`
              : undefined

          let rescanFound: { start: number; end: number } | null = null
          const MAX_RESCAN_TRIES = Math.min(4, rescanCandidateLanes.length)

          for (let tryIdx = 0; tryIdx < MAX_RESCAN_TRIES; tryIdx++) {
            if (job.stopping) return

            // Dynamically acquire the FIRST free lane across all keys and models!
            let acquired: { selected: CandidateLane; release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void }
            try {
              acquired = await globalGeminiCoordinator.acquireFirstAvailableLane({
                scanId: scan.id,
                scanTitle: scan.shortName || scan.id,
                candidates: rescanCandidateLanes,
                operation: `Rescan group ${g.id} chunk ${c.chunkIndex}`,
                videoSeconds: shortDur + padBefore + padAfter + (chunkEnd - chunkStart),
                onWait: (msg) => {
                  addLog(scan, 'info', msg)
                  this.mark(job)
                },
                isStopping: () => job.stopping,
              })
            } catch (acqErr) {
              addLog(scan, 'warn', `Rescan: all rescan candidate lanes exhausted or stopped: ${acqErr instanceof Error ? acqErr.message : String(acqErr)}`)
              break
            }

            const { selected, release } = acquired
            const chosenLane = job.lanes.find((l) => l.apiKey === selected.apiKey) || job.lanes[0]
            const chosenRm = RESCAN_MODEL_POOL.find((m) => m.id === selected.modelId) || RESCAN_MODEL_POOL[0]

            addLog(
              scan,
              'info',
              `Rescan: locked free lane ${chosenRm.id} (key ${chosenLane.idx}) for short ${ts(g.shortStart)}–${ts(g.shortEnd)} inside chunk ${c.chunkIndex}`,
            )

            let shortUp: { uri: string; name: string } | null = null
            let chunkUp: { uri: string; name: string } | null = null

            try {
              shortUp = await uploadVideo(chosenLane.ai, shortClipFile)
              chunkUp = await uploadVideo(chosenLane.ai, chunkFile)

              const raw = await rescanRequest(
                chosenLane.ai,
                chosenRm.id,
                shortUp.uri,
                chunkUp.uri,
                rescanPadNote,
                rescanHintNote,
              )

              // Succeeded! Release lane and record usage
              release(shortDur + padBefore + padAfter + (chunkEnd - chunkStart))
              incrementModelUsage(chosenRm.id, chosenLane.apiKey)
              globalGeminiCoordinator.recordSuccess(chosenLane.apiKey, chosenRm.id, 0)

              rescanFound = parseRescanMatch(raw)
              break
            } catch (reqErr) {
              const re = classifyError(reqErr)
              if (re.kind === 'rate') {
                const googleDelayMs = extractGoogleRetryDelayMs(reqErr) ?? re.retryDelayMs
                const { effectiveCooldownMs } = calculateEffectiveCooldownMs(googleDelayMs, 60_000)

                // 429 Rate limit on this (key × model): Lock this model for Google delay + 5s buffer and switch immediately to another free lane!
                release(0, effectiveCooldownMs)
                globalGeminiCoordinator.handleQuotaOrRateError(chosenLane.apiKey, chosenRm.id, 0, chosenRm.rpd || 500, false, effectiveCooldownMs, chosenLane.idx)
                const googleNote = googleDelayMs ? ` (Google requested ${(googleDelayMs / 1000).toFixed(1)}s + 5s buffer)` : ''
                addLog(
                  scan,
                  'warn',
                  `Rescan: 429 rate limit on ${chosenRm.id} (key ${chosenLane.idx})${googleNote} — model locked for ${(effectiveCooldownMs / 1000).toFixed(1)}s. Switching immediately to another free key/model (attempt ${tryIdx + 1}/${MAX_RESCAN_TRIES})...`,
                )
                continue
              } else {
                release(0, 2000)
                addLog(
                  scan,
                  'warn',
                  `Rescan error on ${chosenRm.id} (key ${chosenLane.idx}): ${re.message.slice(0, 100)} — trying another free key/model (attempt ${tryIdx + 1}/${MAX_RESCAN_TRIES})...`,
                )
                continue
              }
            } finally {
              if (shortUp) void deleteFileQuiet(chosenLane.ai, shortUp.name)
              if (chunkUp) void deleteFileQuiet(chosenLane.ai, chunkUp.name)
            }
          }

          if (!rescanFound) {
            c.rescan = 'not_found'
            this.mark(job)
            addLog(scan, 'info', `Rescan of chunk ${c.chunkIndex}: NOT FOUND`)
            continue
          }

          c.rescan = 'found'
          c.rescanMovieStart = chunkStart + rescanFound.start
          c.rescanMovieEnd = chunkStart + rescanFound.end
          this.mark(job)
        }

        // Re-verify the freshly found rescan window
        c.rescanVerdict = 'verifying'
        this.mark(job)
        const reFile = path.join(clipsDir, `${g.id}-c${i}-rescan.mp4`)
        await extractClipPrecise(
          path.join(mediaDir, 'movie.mp4'),
          Math.max(0, c.rescanMovieStart! - padBefore),
          c.rescanMovieEnd! + padAfter,
          reFile,
        )

        const reSecs = shortDur + padBefore + padAfter + Math.max(1, c.rescanMovieEnd! - c.rescanMovieStart!) + padBefore + padAfter
        let reVerifyVerdict: { same: boolean; reason: string } | null = null

        const MAX_VERIFY_TRIES = Math.min(5, verifyCandidateLanes.length)
        for (let vTry = 0; vTry < MAX_VERIFY_TRIES; vTry++) {
          if (job.stopping) return

          let acquiredV: { selected: CandidateLane; release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void }
          try {
            acquiredV = await globalGeminiCoordinator.acquireFirstAvailableLane({
              scanId: scan.id,
              scanTitle: scan.shortName || scan.id,
              candidates: verifyCandidateLanes,
              operation: `Re-verify rescan window ${g.id} candidate ${i}`,
              videoSeconds: reSecs,
              onWait: (msg) => {
                addLog(scan, 'info', msg)
                this.mark(job)
              },
              isStopping: () => job.stopping,
            })
          } catch (acqVErr) {
            addLog(scan, 'warn', `Re-verify: all verifier lanes busy or exhausted: ${acqVErr instanceof Error ? acqVErr.message : String(acqVErr)}`)
            break
          }

          const { selected: selV, release: releaseV } = acquiredV
          const chosenLaneV = job.lanes.find((l) => l.apiKey === selV.apiKey) || job.lanes[0]
          const chosenRvm = VERIFY_MODEL_POOL.find((m) => m.id === selV.modelId) || VERIFY_MODEL_POOL[0]

          addLog(
            scan,
            'info',
            `Re-verify rescan window movie ${ts(c.rescanMovieStart!)}–${ts(c.rescanMovieEnd!)}${needsPad ? ' (padded)' : ''} locked on ${chosenRvm.id} (key ${chosenLaneV.idx})`,
          )

          let shortUpV: { uri: string; name: string } | null = null
          let reUpV: { uri: string; name: string } | null = null

          try {
            shortUpV = await uploadVideo(chosenLaneV.ai, shortClipFile)
            reUpV = await uploadVideo(chosenLaneV.ai, reFile)

            const raw2 = await verifyRequest(chosenLaneV.ai, chosenRvm.id, shortUpV.uri, reUpV.uri, verifyPadNote)
            releaseV(reSecs)
            incrementModelUsage(chosenRvm.id, chosenLaneV.apiKey)
            globalGeminiCoordinator.recordSuccess(chosenLaneV.apiKey, chosenRvm.id, 0)

            const v2 = parseVerdict(raw2)
            if (v2) {
              reVerifyVerdict = v2
              break
            }
          } catch (vErr) {
            const re = classifyError(vErr)
            if (re.kind === 'rate') {
              releaseV(0, 60_000)
              globalGeminiCoordinator.handleQuotaOrRateError(
                chosenLaneV.apiKey,
                chosenRvm.id,
                0,
                chosenRvm.rpd || 500,
                false,
                60_000,
                chosenLaneV.idx,
              )
              addLog(
                scan,
                'warn',
                `Re-verify: 429 rate limit on ${chosenRvm.id} (key ${chosenLaneV.idx}) — cooling 60s. Switching immediately to another free verifier lane...`,
              )
              continue
            } else {
              releaseV(0, 2000)
              addLog(
                scan,
                'warn',
                `Re-verify error on ${chosenRvm.id} (key ${chosenLaneV.idx}): ${re.message.slice(0, 100)} — switching to another lane...`,
              )
              continue
            }
          } finally {
            if (shortUpV) void deleteFileQuiet(chosenLaneV.ai, shortUpV.name)
            if (reUpV) void deleteFileQuiet(chosenLaneV.ai, reUpV.name)
          }
        }

        if (!reVerifyVerdict) {
          c.rescanVerdict = 'different'
          c.rescanReason = 'Verifier gave no clear verdict across retries'
          this.mark(job)
          continue
        }

        c.rescanReason = reVerifyVerdict.reason
        c.rescanVerdict = reVerifyVerdict.same ? 'same' : 'different'
        this.mark(job)

        if (reVerifyVerdict.same) {
          g.status = 'confirmed'
          g.confirmedIndex = i
          g.confirmedViaRescan = true
          this.applyGroupResult(job, g)
          addLog(
            scan,
            'success',
            `CONFIRMED via rescan: short ${ts(g.shortStart)}–${ts(g.shortEnd)} = movie ${ts(c.rescanMovieStart!)}–${ts(c.rescanMovieEnd!)} ${originTag(groupMatchOrigin(g, true), c.chunkIndex, g.originWindow)}`,
          )
          this.checkEarlyStop(job)
          return
        }
        addLog(scan, 'warn', `Rescan window rejected by verifier — final DIFFERENT for candidate ${i} (chunk ${c.chunkIndex})`)
      }

      // If all candidates in this group failed rescan:
      g.status = 'rejected'
      this.applyGroupResult(job, g)
      this.reviveEarlyStopSkipped(job, g)
      const best = bestRejectedCandidate(g)
      const bestWin = best
        ? best.viaRescan
          ? `${ts(best.c.rescanMovieStart!)}–${ts(best.c.rescanMovieEnd!)}`
          : `${ts(best.c.movieStart)}–${ts(best.c.movieEnd)}`
        : 'none'
      addLog(
        scan,
        'warn',
        `REJECTED (kept): short ${ts(g.shortStart)}–${ts(g.shortEnd)} — verifier and rescan both rejected; best candidate movie ${bestWin} kept in merge, flagged rejected`,
      )
      this.mark(job)
    } finally {
      try {
        for (const f of fs.readdirSync(clipsDir)) {
          if (f.startsWith(g.id)) fs.unlinkSync(path.join(clipsDir, f))
        }
      } catch {
        /* ignore */
      }
    }
  }

  /**
   * Run the Rescan Phase across all candidate groups that failed normal verification.
   * Runs in PARALLEL workers across all available free key/model lanes!
   */
  private async runRescanPhase(job: Job): Promise<void> {
    const { scan } = job
    if (job.stopping || scan.verifierEnabled === false || job.rescanQueue.length === 0) return

    const totalToRescan = job.rescanQueue.length
    const concurrency = Math.min(
      totalToRescan,
      Math.max(1, job.lanes.length * RESCAN_MODEL_POOL.length),
    )
    addLog(
      scan,
      'info',
      `Normal candidate verification complete! Starting Parallel Rescan Phase for ${totalToRescan} candidate group(s) across free key/model lanes (concurrency: ${concurrency}, models: ${RESCAN_MODEL_POOL.map((m) => m.id).join(', ')})...`,
    )
    scan.status = 'verifying'
    this.mark(job)

    const worker = async () => {
      while (job.rescanQueue.length > 0 && !job.stopping) {
        const gi = job.rescanQueue.shift()
        if (gi === undefined) break
        const g = (scan.candidateGroups || [])[gi]
        if (!g || g.status !== 'rescanning') continue

        // Check if superseded
        const winner = this.confirmedCovering(scan, g)
        if (winner) {
          this.supersedeGroup(job, g, winner)
          continue
        }

        try {
          await this.rescanCandidateGroup(job, g)
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          addLog(scan, 'error', `Rescan error for group ${g.id}: ${msg.slice(0, 140)}`)
          g.status = 'rejected'
          this.applyGroupResult(job, g)
          this.mark(job)
        }
      }
    }

    const workers = Array.from({ length: concurrency }, () => worker())
    await Promise.all(workers)

    addLog(scan, 'success', `Rescan Phase completed for all ${totalToRescan} candidate group(s).`)
    this.mark(job)
  }

  /**
   * The ONE coverage line: how much of the short the current match set covers,
   * with the exact MISSING list. warn when anything is missing, success at 100 %.
   * Logged at scan end and with every partial report; the gap-backup pass logs
   * it again afterwards with the recovered seconds.
   */
  private logCoverage(job: Job, extra?: string): ShortCoverage {
    const { scan } = job
    const cov = computeShortCoverage(scan)
    const line = coverageLine(cov, extra)
    addLog(scan, line.level, line.msg)
    this.mark(job)
    return cov
  }

  /** Rewrite scan.matches for a finished group:
   *  confirmed → ONE verified match (rescan window when confirmedViaRescan),
   *  rejected → best candidate KEPT in the merge, flagged rejected (verified=false),
   *  unverified → original candidate windows kept, flagged verified=false. */
  private applyGroupResult(job: Job, g: CandidateGroup) {
    const { scan } = job
    // USER PICK (preview/compare "Make main") wins over the AI verdict — shared
    // helper so a Resume never overwrites the user's choice.
    applyGroupMatches(scan, g)
    this.mark(job)
  }

  /** PARTIAL report on Stop/pause: jitna kaam ho chuka hai wo TURANT export/preview
   *  ke liye available rahe — SAME rule as a finished scan: verified matches +
   *  unverified/pending candidates (verified flag na hone par bhi) sab included.
   *  Resume phir bhi wahi se continue karta hai (pending chunks/groups untouched). */
  private buildPartialReport(job: Job) {
    const { scan } = job
    if (!Array.isArray(scan.matches)) scan.matches = []
    scan.matches.sort((a, b) => a.shortStart - b.shortStart || a.movieStart - b.movieStart)
    scan.report = this.reportStats(job, true)
    const r = scan.report
    if (scan.matches.length > 0) {
      addLog(
        scan,
        'info',
        `Partial results saved: ${scan.matches.length} match(es) (verified + unverified dono) — export/preview ab available hai, Resume karne par scan wahi se continue hoga`,
      )
      this.logCoverage(job, '(partial — Resume se aur cover hoga)')
    }
    // Make the gaps LOUD: a partial report used to say "Chunks failed: 0" while
    // re-queued chunks (fetch failed / 429) never ran, and in-flight verifier
    // groups vanished from the confirmed/rejected/unverified totals.
    if ((r.chunksPending || 0) > 0 || (r.groupsPending || 0) > 0) {
      addLog(
        scan,
        'warn',
        `INCOMPLETE: ${r.chunksPending || 0} chunk(s) never scanned (pending/re-queued) · ${r.groupsPending || 0} candidate group(s) still verifying/rescanning — ye report ke totals me alag dikhte hain, Resume se complete hoga`,
      )
    }
    this.mark(job)
  }

  /** Report stats shared by the final and partial report. EVERY chunk and EVERY
   *  candidate group lands in exactly one bucket, so the totals add up:
   *  chunks = scanned + failed + pending, groups = confirmed + rejected + unverified + pending. */
  private reportStats(job: Job, partial: boolean): ScanReport {
    const { scan } = job
    const segments = scan.shortSegments || []
    const allChunks = segments.flatMap((s) => s.chunks)
    const groups = scan.candidateGroups || []
    const now = Date.now()
    const matches = scan.matches || []
    // PROVENANCE totals: chunk / rescan / gap-backup / user.
    const originCounts: Partial<Record<NonNullable<ChunkMatch['origin']>, number>> = {}
    for (const m of matches) {
      const o = m.origin ?? 'chunk'
      originCounts[o] = (originCounts[o] || 0) + 1
    }
    const gapGroups = groups.filter((g) => g.origin === 'gap-backup')
    const gb = scan.gapBackup
    const gapBackup =
      gapGroups.length > 0 || (gb && gb.status !== 'idle')
        ? {
            candidates: gb?.candidates.length ?? 0,
            confirmed: gapGroups.filter((g) => g.status === 'confirmed').length,
            rejectedKept: gapGroups.filter((g) => g.status === 'rejected' && !g.superseded).length,
            unverified: gapGroups.filter((g) => g.status === 'unverified').length,
            unresolved: gb?.parts.filter((p) => p.result === 'unresolved').length ?? 0,
          }
        : undefined
    return {
      totalScanTimeMs: (scan.finishedAt || now) - (scan.startedAt || now),
      chunksScanned: allChunks.filter((c) => c.status === 'match' || c.status === 'no_match').length,
      chunksFailed: allChunks.filter((c) => c.status === 'failed').length,
      // pending (never started / re-queued after fetch failed or 429), scanning
      // (in flight at Stop) and cancelled all mean "this chunk was NOT scanned".
      chunksPending: allChunks.filter((c) => c.status === 'pending' || c.status === 'scanning' || c.status === 'cancelled').length,
      partial: partial || undefined,
      modelsUsed: MODEL_POOL.filter((m) => job.lanes.some((l) => getModelUsage(m.id, l.apiKey) > 0)).map((m) => m.id),
      matches: scan.matches,
      timeoutStats: scan.timeoutStats,
      groupsTotal: groups.length,
      groupsConfirmed: groups.filter((g) => g.status === 'confirmed').length,
      groupsRejected: groups.filter((g) => g.status === 'rejected').length,
      groupsUnverified: groups.filter((g) => g.status === 'unverified').length,
      groupsPending: groups.filter((g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning').length,
      matchesRejectedKept: matches.filter((m) => m.rejected === true && m.userPick !== true).length,
      originCounts,
      gapBackup,
      coverage: computeShortCoverage(scan),
      // How the chunk set was chosen for THIS run (results themselves are 100% Gemini).
      prefilterMode:
        scan.prefilter?.mode === 'prefiltered' ? 'twelvelabs' : scan.geminiPrescan?.appliedMinutes?.length ? 'gemini' : 'full',
      prefilterSelected: scan.prefilter?.selectedChunks,
      prefilterTotal: scan.prefilter?.totalChunks,
    }
  }

  private finish(job: Job) {
    // Pipelining cleanup: delete any pre-uploaded chunks that were never consumed
    // (best effort — Files API entries expire on their own anyway).
    for (const lane of job.lanes) {
      for (const p of lane.chunkUploads.values()) {
        p.then((f) => deleteFileQuiet(lane.ai, f.name)).catch(() => {})
      }
      lane.chunkUploads.clear()
      // Background sweep on this lane's API key to keep storage clean
      void cleanupOrphanedGeminiFiles(lane.apiKey, 24 * 60 * 60_000)
    }
    if (job.saverTimer) clearInterval(job.saverTimer)
    saveScan(job.scan)
    this.jobs.delete(job.scan.id)
    globalChunkScanCoordinator.releaseChunkScanLock(job.scan.id)
  }

  /** Merge a chunk's parsed matches into the scan-level list. Replaces ONLY this
   *  chunk's old entries WITHIN the given short-minute window — the same movie
   *  chunk can legitimately hold matches from other short minutes. */
  private mergeMatches(scan: Scan, chunkIndex: number, matches: ChunkMatch[], seg: ShortSegmentState | null) {
    const segStart = seg ? seg.start : 0
    const segEnd = seg ? seg.end : Number.POSITIVE_INFINITY
    scan.matches = (scan.matches || []).filter(
      (m) => !(m.chunkIndex === chunkIndex && m.shortStart >= segStart && m.shortStart < segEnd && !m.userPick),
    )
    scan.matches.push(...matches)
    scan.matches.sort((a, b) => a.shortStart - b.shortStart || a.movieStart - b.movieStart)
    // Synchronize candidate groups immediately so overlapping matches from other chunks
    // become alternative candidates of that scene instead of colliding on the timeline
    this.buildCandidateGroups(scan)
  }

  /** Helper to re-queue a failed chunk with a mandatory 6-second pacing buffer to prevent rapid-fire token spikes */
  private requeueFailedChunk(job: Job, chunkIndex: number, failedModel: string, failedKeyIdx: number, reason: string, minutePrefix: string) {
    const chunk = job.scan.chunks[chunkIndex]
    if (chunk) chunk.status = 'pending'
    job.queue.push(chunkIndex)
    job.chunkRetryCooldown.set(chunkIndex, {
      availableAt: Date.now() + CHUNK_RETRY_BUFFER_MS,
      reason: reason.slice(0, 80),
      failedModel,
      failedKey: failedKeyIdx,
    })
    addLog(
      job.scan,
      'info',
      `${minutePrefix}Chunk ${chunkIndex}: 6s pacing buffer active after ${displayModelName(failedModel)} (key ${failedKeyIdx}) attempt — preventing rapid-fire token spikes before next handoff/retry.`,
    )
  }

  /** Worker: one per KEY LANE.
   *  STRICT 1-WORKER-PER-KEY: exactly ONE chunk request is active on an API key at a time.
   *  Rotates to fallback models sequentially if primary is cooling or exhausted. */
  private async laneWorker(job: Job, lane: KeyLane) {
    const modelCount = CHUNK_MODEL_POOL.length
    if (modelCount === 0) return

    while (true) {
      if (job.stopping) return

      // IMMEDIATE MINUTE COMPLETION CHECK:
      if (job.queue.length === 0 && job.inFlight.size === 0) {
        for (const item of CHUNK_MODEL_POOL) {
          const st = this.modelState(job, lane, item)
          if (st.state !== 'idle' && st.state !== 'exhausted') {
            st.state = 'idle'
            st.currentChunk = null
          }
        }
        this.mark(job)
        return
      }

      // Determine active model for this key lane:
      // Primary model rotated by key index, or fallback if exhausted for today.
      const primaryIdx = lane.idx % modelCount
      const primaryModel = CHUNK_MODEL_POOL[primaryIdx]
      const secondaryModels = CHUNK_MODEL_POOL.filter((_, idx) => idx !== primaryIdx)

      let m = primaryModel
      if (globalGeminiCoordinator.isModelExhausted(lane.apiKey, primaryModel.id, primaryModel.rpd)) {
        const fallback = secondaryModels.find((cand) => !globalGeminiCoordinator.isModelExhausted(lane.apiKey, cand.id, cand.rpd))
        if (!fallback) {
          // All models exhausted for today on this API key!
          const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
          if (laneState) laneState.status = 'exhausted'
          for (const item of CHUNK_MODEL_POOL) {
            const st = this.modelState(job, lane, item)
            st.state = 'exhausted'
            st.currentChunk = null
          }
          this.mark(job)
          return
        }
        m = fallback
      }

      const st = this.modelState(job, lane, m)

      // Cooldown check (RPM/TPM-type 429).
      const cool = job.cooldownUntil[this.rateKey(lane, m)] || 0
      if (cool > Date.now()) {
        if (job.queue.length === 0 && job.inFlight.size === 0) {
          if (st.state !== 'idle' && st.state !== 'exhausted') {
            st.state = 'idle'
            st.currentChunk = null
            this.mark(job)
          }
          return
        }
        st.state = 'cooling'
        st.cooldownUntil = cool
        st.currentChunk = null
        this.mark(job)
        // PIPELINING: During 30s cooldown, ensure next chunk is locked and preparing in background!
        this.lockAndPrepareNextChunk(job, lane)
        this.prefetchNextChunks(job, lane)
        await sleep(Math.min(2000, cool - Date.now()))
        continue
      }
      st.cooldownUntil = null

      // Global Coordinator Availability check:
      // If this key or model is currently busy (working or in pacing delay), wait 1 second
      const laneBusy = globalGeminiCoordinator.isLaneBusy(lane.apiKey, m.id, 0)
      if (laneBusy.busy) {
        if (job.queue.length === 0 && job.inFlight.size === 0) {
          if (st.state !== 'idle' && st.state !== 'exhausted') {
            st.state = 'idle'
            st.currentChunk = null
            this.mark(job)
          }
          return
        }
        st.state = laneBusy.cooling ? 'cooling' : 'waiting'
        st.currentChunk = null
        await sleep(1000)
        continue
      }

      // Pull next chunk (filters out chunks in retry buffer cooldown)
      const chunkIndex = this.pullChunkFor(job, lane)
      if (chunkIndex === undefined) {
        if (job.inFlight.size === 0 && job.queue.length === 0) {
          if (st.state !== 'idle') {
            st.state = 'idle'
            st.currentChunk = null
            this.mark(job)
          }
          return
        }
        st.state = 'waiting'
        st.currentChunk = null
        await sleep(1000)
        continue
      }

      // Process this chunk on model `m`
      try {
        await this.processChunk(job, lane, m, chunkIndex)
      } catch {
        /* error handled & recorded inside processChunk */
      }
    }
  }

  /** Process a single chunk on a specified key lane and model */
  private async processChunk(job: Job, lane: KeyLane, m: ModelSpec, chunkIndex: number, segOverride?: ShortSegmentState) {
    const { scan } = job
    const st = this.modelState(job, lane, m)

    const seg = segOverride || job.seg
    if (!seg) return
    const chunk = seg.chunks[chunkIndex] || scan.chunks[chunkIndex]
    if (!chunk || chunk.status !== 'pending') return
    const minutePrefix = (scan.shortSegments?.length ?? 0) > 1 ? `Minute ${seg.index + 1} · ` : ''
      job.inFlight.add(chunkIndex)
      chunk.status = 'scanning'
      chunk.model = m.id
      st.state = 'active'
      st.currentChunk = chunkIndex
      this.mark(job)

      const segDuration = seg.end - seg.start
      const movieChunkDuration = 60
      const totalVideoDurationSec = segDuration + movieChunkDuration
      const currentEmaRate = this.modelTokenRateEMA.get(m.id) ?? 260
      let promptChars = CHUNK_MAP_PROMPT.length

      let chunkFileName: string | null = null
      /** consumed backup uploads (deleted in finally) */
      const backupNames: string[] = []
      let releaseGlobalLock: ((sec?: number, cooldownOverrideMs?: number) => void) | null = null
      const chunkLockId = `chunk-${seg.index}-${chunkIndex}`
      try {
        releaseGlobalLock = await globalGeminiCoordinator.acquireLane({
          scanId: scan.id,
          scanTitle: scan.shortName || scan.id,
          apiKey: lane.apiKey,
          keyIdx: lane.idx,
          modelId: m.id,
          slot: 0,
          operation: `Chunk ${chunkIndex} map`,
          videoSeconds: 60,
          rpd: m.rpd || 500,
          isChunk: true,
          chunkLockId,
          onWait: (msg) => {
            st.state = 'waiting'
            addLog(scan, 'info', msg)
            this.mark(job)
          },
          isStopping: () => job.stopping,
        })

        // PARALLEL UPLOADS: the short-minute segment + THIS movie chunk upload
        // AT THE SAME TIME (Promise.all) — and the per-model pacing wait
        // (TPM ≈ 1 request/min per model per key) overlaps with those uploads
        // too, so nothing waits on anything it doesn't have to.
        const rk = this.rateKey(lane, m)
        const uploadsP = Promise.all([
          this.ensureSegmentUri(job, lane, seg),
          this.takeChunkUpload(job, lane, chunkIndex),
        ])
        // Rejection is handled at the await below — this only prevents an
        // unhandled-rejection crash while the pacing sleep is running.
        uploadsP.catch(() => {})

        // PIPELINING: start pre-cutting + pre-uploading the NEXT queued chunks
        // on this key in the background right away.
        this.prefetchNextChunks(job, lane)

        const wait = (job.nextFreeAt[rk] || 0) - Date.now()
        if (wait > 0) {
          st.state = 'waiting'
          this.mark(job)
          await this.stoppableSleep(job, wait)
          st.state = 'active'
          this.mark(job)
        }

        // STOP CHECK: request bhejne / quota consume karne se PEHLE nikal jao —
        // chunk wapas 'pending' ho jaata hai (no attempt penalty), Resume par wahi se chalega.
        if (job.stopping) {
          globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
          chunk.status = 'pending'
          this.mark(job)
          throw new Error('Scan stopped by user')
        }

        const [shortUri, uploaded] = await uploadsP
        chunkFileName = uploaded.name

        job.nextFreeAt[rk] = Date.now() + MODEL_MIN_INTERVAL_MS

        addLog(scan, 'info', `${minutePrefix}Chunk ${chunkIndex}: mapping short → movie minute ${chunkIndex} on ${m.id} (key ${lane.idx})`)
        chunk.model = m.id
        chunk.keyIdx = lane.idx
        // PIPELINING: while Gemini analyzes THIS chunk, the next chunk's
        // cut + upload runs in the background — analysis khatam hote hi agla
        // segment ready milta hai, wait zero.
        this.prefetchNextChunks(job, lane)
        let raw: string
        const alreadySanitized = Boolean(chunk.policyRetried)
        let effectiveUploadedUri = uploaded.uri
        let effectiveShortUri = shortUri
        const effectivePrompt = alreadySanitized ? CHUNK_MAP_SANITIZED_PROMPT : undefined

        if (alreadySanitized) {
          const mediaDir = scanMediaDir(scan.id)
          const sanitizedDir = path.join(mediaDir, 'sanitized')
          const sanitizedChunkFile = path.join(sanitizedDir, `chunk-${String(chunkIndex).padStart(4, '0')}-muted.mp4`)
          if (fs.existsSync(sanitizedChunkFile)) {
            const sanitizedUploaded = await uploadVideo(lane.ai, sanitizedChunkFile)
            backupNames.push(sanitizedUploaded.name)
            effectiveUploadedUri = sanitizedUploaded.uri
          }
          const sanitizedShortFile = path.join(sanitizedDir, `short-${String(seg.index + 1).padStart(4, '0')}-muted.mp4`)
          if (fs.existsSync(sanitizedShortFile)) {
            const sanitizedShortUp = await uploadVideo(lane.ai, sanitizedShortFile)
            backupNames.push(sanitizedShortUp.name)
            effectiveShortUri = sanitizedShortUp.uri
          }
        }

        // Request-size guard: before calling mapChunkRequest, log estimated input tokens
        promptChars = (effectivePrompt || CHUNK_MAP_PROMPT).length
        const estimatedTokens = Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4)

        if (estimatedTokens > 200_000) {
          addLog(
            scan,
            'warn',
            `${minutePrefix}Chunk ${chunkIndex}: Large input size warning! Estimated ${estimatedTokens.toLocaleString()} tokens > 200,000 limit (${totalVideoDurationSec.toFixed(1)}s video) on ${m.id}`,
          )
        }

        let lastTokenUsage: ChunkTokenUsage | undefined

        while (true) {
          if (job.stopping) {
            globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
            chunk.status = 'pending'
            this.mark(job)
            throw new Error('Scan stopped by user')
          }

          try {
            // Request Attempt: Call Gemini with 240s timeout and 30s heartbeat
            const mapRes = await mapChunkRequest(
              lane.ai,
              m.id,
              effectiveShortUri,
              effectiveUploadedUri,
              effectivePrompt,
              CHUNK_MAP_TIMEOUT_MS,
              (elapsedSec) => {
                addLog(
                  scan,
                  'info',
                  `${minutePrefix}Chunk ${chunkIndex}: ${displayModelName(m.id)} (key ${lane.idx}) analyzing video in progress (${elapsedSec}s elapsed, input: ~${estimatedTokens.toLocaleString()} tokens)...`,
                )
                this.mark(job)
              },
            )
            raw = mapRes.text

            let actualRate = currentEmaRate
            let shortVideoTokens = Math.round(segDuration * currentEmaRate)
            let chunkVideoTokens = Math.round(movieChunkDuration * currentEmaRate)
            let isGoogleVerified = false

            if (mapRes.promptTokenCount && totalVideoDurationSec > 0) {
              const actualPromptTokens = mapRes.promptTokenCount
              const videoTokens = Math.max(0, actualPromptTokens - promptChars / 4)
              actualRate = videoTokens / totalVideoDurationSec
              const prevEMA = this.modelTokenRateEMA.get(m.id) ?? 260
              const alpha = 0.2
              this.modelTokenRateEMA.set(m.id, alpha * actualRate + (1 - alpha) * prevEMA)

              shortVideoTokens = Math.round(segDuration * actualRate)
              chunkVideoTokens = Math.round(movieChunkDuration * actualRate)
              isGoogleVerified = true
            }

            const promptTokens = Math.round(promptChars / 4)
            const outputTokens = mapRes.candidatesTokenCount ?? 0
            const totalTokens = mapRes.totalTokenCount ?? (shortVideoTokens + chunkVideoTokens + promptTokens + outputTokens)
            const durationSec = (mapRes.elapsedMs / 1000).toFixed(1)

            lastTokenUsage = {
              shortVideoTokens,
              chunkVideoTokens,
              promptTokens,
              outputTokens,
              totalTokens,
              shortDurationSec: Number(segDuration.toFixed(1)),
              chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
              ratePerSec: Math.round(actualRate),
              isGoogleVerified,
              isError: false,
            }

            const used = incrementModelUsage(m.id, lane.apiKey)
            st.usedToday = used
            chunk.requestCount = (chunk.requestCount || 0) + 1
            job.laneFirstSuccess.add(lane.idx)

            // 1 successful chunk mapping request -> Mandatory 30-second (30s) cooldown
            // and clear 429 priority lock if it was held!
            const outcome = globalGeminiCoordinator.recordChunkSuccess(lane.apiKey, m.id, chunkLockId)
            job.cooldownUntil[rk] = outcome.cooldownUntil
            job.nextFreeAt[rk] = outcome.cooldownUntil
            st.state = 'cooling'
            st.cooldownUntil = outcome.cooldownUntil

            // LOCK & PREPARE NEXT CHUNK: Reserve the next chunk exclusively for this key lane,
            // pre-cut via FFmpeg and pre-upload to Gemini Files API during this 30s wait!
            const lockedCi = this.lockAndPrepareNextChunk(job, lane)
            const lockMsg = lockedCi !== null ? ` — Chunk ${lockedCi} locked & preparing in background` : ''

            if (outcome.retryCleared) {
              addLog(
                scan,
                'success',
                `${minutePrefix}Chunk ${chunkIndex}: 429 priority retry succeeded on ${displayModelName(m.id)} (key ${lane.idx}) in ${durationSec}s. Lock cleared; 30s cooldown started${lockMsg}.`,
              )
            } else {
              addLog(
                scan,
                'info',
                `${minutePrefix}Chunk ${chunkIndex}: 1 chunk mapping request completed on ${displayModelName(m.id)} (key ${lane.idx}) in ${durationSec}s — 30s cooldown started (RPM/TPM protection)${lockMsg}.`,
              )
            }
            this.mark(job)
            break
          } catch (reqErr) {
            const re = classifyError(reqErr, { keyIdx: lane.idx, model: m.id, requestKind: 'chunk_map' })
            const googleDelayMs = extractGoogleRetryDelayMs(reqErr) ?? re.retryDelayMs
            const isPolicyBlocked =
              re.kind === 'policy_blocked' ||
              /prohibited_content|blocked_by_safety|safety_ratings_blocked|prompt block reason/i.test(re.message)

            chunk.requestCount = (chunk.requestCount || 0) + 1
            this.mark(job)

            if (re.kind === 'timeout' || /timed out|timeout/i.test(re.message)) {
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              const timeoutSec = Math.round(CHUNK_MAP_TIMEOUT_MS / 1000)

              // Count usage on the key & model because Gemini server accepted and processed the request (consuming daily quota)
              const usedTimeout = incrementModelUsage(m.id, lane.apiKey)
              st.usedToday = usedTimeout
              globalGeminiCoordinator.recordUsage(lane.apiKey, m.id)

              // Record timeout statistics on the scan
              if (!scan.timeoutStats) {
                scan.timeoutStats = {
                  totalTimeouts: 0,
                  byModel: {},
                  byKey: {},
                  wastedEstimatedTokens: 0,
                }
              }
              scan.timeoutStats.totalTimeouts = (scan.timeoutStats.totalTimeouts || 0) + 1
              scan.timeoutStats.byModel[m.id] = (scan.timeoutStats.byModel[m.id] || 0) + 1
              scan.timeoutStats.byKey[lane.idx] = (scan.timeoutStats.byKey[lane.idx] || 0) + 1
              scan.timeoutStats.wastedEstimatedTokens =
                (scan.timeoutStats.wastedEstimatedTokens || 0) +
                Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4)

              const errTokenUsageTimeout: ChunkTokenUsage = {
                shortVideoTokens: Math.round(segDuration * currentEmaRate),
                chunkVideoTokens: Math.round(movieChunkDuration * currentEmaRate),
                promptTokens: Math.round(promptChars / 4),
                outputTokens: 0,
                totalTokens: Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4),
                shortDurationSec: Number(segDuration.toFixed(1)),
                chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
                ratePerSec: Math.round(currentEmaRate),
                isGoogleVerified: false,
                isError: true,
                errorMessage: `Request Timeout: No response from Gemini API after ${timeoutSec}s (Counted against quota: ${usedTimeout} used today)`,
              }
              this.recordChunkOutput(
                chunk,
                m.id,
                `[REQUEST TIMEOUT DIAGNOSTIC]\nModel: ${m.id} (Key ${lane.idx})\nTimeout Limit: ${timeoutSec}s\nEstimated Input Tokens: ~${Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4).toLocaleString()} (Short: ${Math.round(segDuration * currentEmaRate).toLocaleString()}tok, Chunk: ${Math.round(movieChunkDuration * currentEmaRate).toLocaleString()}tok, Prompt: ${Math.round(promptChars / 4)}tok)\nStatus: Gemini API did not complete response within ${timeoutSec}s (Counted on key ${lane.idx}: ${usedTimeout} used today)\nAction: Aborting and switching to next model\nError: ${reqErr instanceof Error ? reqErr.message : String(reqErr)}\nTimestamp: ${new Date().toISOString()}`,
                errTokenUsageTimeout,
                'error',
                reqErr instanceof Error ? reqErr.message : String(reqErr),
              )
              addLog(
                scan,
                'warn',
                `${minutePrefix}Chunk ${chunkIndex}: ${displayModelName(m.id)} (key ${lane.idx}) timed out after ${timeoutSec}s (est. input: ~${estimatedTokens.toLocaleString()} tokens) — request counted against quota (${usedTimeout} used today); aborting and switching to next model!`,
              )
              // Put this model lane on 1-min cooldown so other models or keys pick up this chunk
              job.cooldownUntil[rk] = Date.now() + 60_000
              st.state = 'cooling'
              st.cooldownUntil = Date.now() + 60_000
              this.mark(job)
              throw reqErr
            } else if (re.kind === 'overloaded') {
              // Count each 503 high-demand attempt against this key & model quota
              incrementModelUsage(m.id, lane.apiKey)
              addLog(scan, 'warn', `${m.id} (key ${lane.idx}): 503 overloaded — no retry, queued for next burst cycle`)
              const errTokenUsage503: ChunkTokenUsage = {
                shortVideoTokens: Math.round(segDuration * currentEmaRate),
                chunkVideoTokens: Math.round(movieChunkDuration * currentEmaRate),
                promptTokens: Math.round(promptChars / 4),
                outputTokens: 0,
                totalTokens: Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4),
                shortDurationSec: Number(segDuration.toFixed(1)),
                chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
                ratePerSec: Math.round(currentEmaRate),
                isGoogleVerified: false,
                isError: true,
                errorMessage: '503 Overloaded (no retry, queued for next burst cycle)',
              }
              this.recordChunkOutput(
                chunk,
                m.id,
                `[503 OVERLOADED ERROR]\nModel: ${m.id} (Key ${lane.idx})\nStatus: Model experiencing high demand — no retry, queued for next burst cycle\nMessage: ${reqErr instanceof Error ? reqErr.message : String(reqErr)}`,
                errTokenUsage503,
                'error',
                reqErr instanceof Error ? reqErr.message : String(reqErr),
              )
              this.mark(job)
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              throw reqErr
            } else if (isPolicyBlocked && !chunk.policyRetried) {
              chunk.policyRetried = true
              addLog(
                scan,
                'warn',
                `${minutePrefix}Chunk ${chunkIndex}: Flagged by Google Policy (PROHIBITED_CONTENT) on ${m.id} (key ${lane.idx}) — triggering 1 sanitized retry with Audio Stripped (-an Mute) + Neutral Frame Alignment prompt...`,
              )
              this.mark(job)

              // Prepare sanitized muted chunk video
              const mediaDir = scanMediaDir(scan.id)
              const sanitizedDir = path.join(mediaDir, 'sanitized')
              fs.mkdirSync(sanitizedDir, { recursive: true })
              const originalChunkFile = chunkPath(path.join(mediaDir, 'chunks'), chunkIndex)
              const sanitizedChunkFile = path.join(sanitizedDir, `chunk-${String(chunkIndex).padStart(4, '0')}-muted.mp4`)

              if (!fs.existsSync(sanitizedChunkFile) && fs.existsSync(originalChunkFile)) {
                await sanitizeVideoMute(originalChunkFile, sanitizedChunkFile)
              }

              const fileToUpload = fs.existsSync(sanitizedChunkFile) ? sanitizedChunkFile : originalChunkFile
              const sanitizedUploaded = await uploadVideo(lane.ai, fileToUpload)
              backupNames.push(sanitizedUploaded.name)

              // Prepare sanitized muted short video to remove audio triggers from short clip as well
              let sanitizedShortUri = shortUri
              try {
                const originalShortFile = await this.ensureSegmentFile(scan, seg)
                const sanitizedShortFile = path.join(sanitizedDir, `short-${String(seg.index + 1).padStart(4, '0')}-muted.mp4`)
                if (!fs.existsSync(sanitizedShortFile) && fs.existsSync(originalShortFile)) {
                  await sanitizeVideoMute(originalShortFile, sanitizedShortFile)
                }
                if (fs.existsSync(sanitizedShortFile)) {
                  const sanitizedShortUp = await uploadVideo(lane.ai, sanitizedShortFile)
                  backupNames.push(sanitizedShortUp.name)
                  sanitizedShortUri = sanitizedShortUp.uri
                }
              } catch (muteErr) {
                console.warn(`Could not sanitize short segment audio: ${muteErr}`)
              }

              // Request Attempt 2: Sanitized Retry with neutral prompt
              try {
                const sanitizedRes = await mapChunkRequest(lane.ai, m.id, sanitizedShortUri, sanitizedUploaded.uri, CHUNK_MAP_SANITIZED_PROMPT)
                raw = sanitizedRes.text
                const promptCharsSanitized = CHUNK_MAP_SANITIZED_PROMPT.length
                let actualRateSanitized = currentEmaRate
                let shortVideoTokensSanitized = Math.round(segDuration * currentEmaRate)
                let chunkVideoTokensSanitized = Math.round(movieChunkDuration * currentEmaRate)
                let isGoogleVerifiedSanitized = false
                if (sanitizedRes.promptTokenCount && totalVideoDurationSec > 0) {
                  const actualPromptTokens = sanitizedRes.promptTokenCount
                  const videoTokens = Math.max(0, actualPromptTokens - promptCharsSanitized / 4)
                  actualRateSanitized = videoTokens / totalVideoDurationSec
                  shortVideoTokensSanitized = Math.round(segDuration * actualRateSanitized)
                  chunkVideoTokensSanitized = Math.round(movieChunkDuration * actualRateSanitized)
                  isGoogleVerifiedSanitized = true
                }
                const promptTokensSanitized = Math.round(promptCharsSanitized / 4)
                const outputTokensSanitized = sanitizedRes.candidatesTokenCount ?? 0
                const totalTokensSanitized = sanitizedRes.totalTokenCount ?? (shortVideoTokensSanitized + chunkVideoTokensSanitized + promptTokensSanitized + outputTokensSanitized)
                lastTokenUsage = {
                  shortVideoTokens: shortVideoTokensSanitized,
                  chunkVideoTokens: chunkVideoTokensSanitized,
                  promptTokens: promptTokensSanitized,
                  outputTokens: outputTokensSanitized,
                  totalTokens: totalTokensSanitized,
                  shortDurationSec: Number(segDuration.toFixed(1)),
                  chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
                  ratePerSec: Math.round(actualRateSanitized),
                  isGoogleVerified: isGoogleVerifiedSanitized,
                  isError: false,
                }
                const usedRetry = incrementModelUsage(m.id, lane.apiKey)
                st.usedToday = usedRetry
                chunk.requestCount = (chunk.requestCount || 0) + 1
                job.laneFirstSuccess.add(lane.idx)
                const outcome = globalGeminiCoordinator.recordChunkSuccess(lane.apiKey, m.id, chunkLockId)
                job.cooldownUntil[rk] = outcome.cooldownUntil
                job.nextFreeAt[rk] = outcome.cooldownUntil
                st.state = 'cooling'
                st.cooldownUntil = outcome.cooldownUntil
                addLog(scan, 'success', `${minutePrefix}Chunk ${chunkIndex}: Sanitized retry succeeded on ${m.id} (key ${lane.idx}) after policy flag bypass`)
                this.mark(job)
                break
              } catch (retryErr) {
                chunk.requestCount = (chunk.requestCount || 0) + 1
                this.mark(job)
                globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
                throw retryErr
              }
            } else if (googleDelayMs && googleDelayMs > 120_000) {
              // Rate limit with delay > 120s:
              // Per user rule: Do NOT treat as daily exhaustion! Daily exhaustion only happens when used >= m.rpd (e.g. 20 RPD in settings).
              // Clear chunk lock, give THIS model/lane strictly a 1-minute cooldown (60s),
              // and release chunk immediately to another model/lane!
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              const cooldownMs = 60_000 // Sirf 1 minute ka cooldown
              globalGeminiCoordinator.reportRateLimit(lane.apiKey, m.id, cooldownMs, 0)
              job.cooldownUntil[rk] = Date.now() + cooldownMs
              st.state = 'cooling'
              st.cooldownUntil = Date.now() + cooldownMs
              this.mark(job)
              addLog(
                scan,
                'warn',
                `${minutePrefix}Chunk ${chunkIndex}: Rate limit on ${displayModelName(m.id)} (key ${lane.idx}) — giving 1 min cooldown; handing chunk to another model!`,
              )
              throw reqErr
            } else if (re.kind === 'rpd') {
              // Daily Quota (RPD): Google confirmed daily requests exceeded (e.g. limit: 20 reached).
              // Drop this model for today and re-queue the chunk back to pending queue immediately!
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              globalGeminiCoordinator.reportExhausted(lane.apiKey, m.id, 0, m.rpd || 20)
              setModelExhausted(m.id, lane.apiKey, m.rpd || 20, true)
              st.state = 'exhausted'
              globalOrchestrator.dropModelForToday(m.id, reqErr instanceof Error ? reqErr.message : String(reqErr))
              chunk.status = 'pending'
              chunk.assignedKey = undefined
              chunk.assignedModel = undefined
              this.mark(job)
              addLog(
                scan,
                'warn',
                `${minutePrefix}Chunk ${chunkIndex}: 429 Daily Quota Exceeded on ${displayModelName(m.id)} (key ${lane.idx}) — model dropped for today; chunk returned to pending queue!`,
              )
              throw reqErr
            } else if (re.kind === 'rate') {
              addLog(scan, 'warn', `${m.id} (key ${lane.idx}): 429 rate limit — no retry, queued for next burst cycle`)
              const errTokenUsage429: ChunkTokenUsage = {
                shortVideoTokens: Math.round(segDuration * currentEmaRate),
                chunkVideoTokens: Math.round(movieChunkDuration * currentEmaRate),
                promptTokens: Math.round(promptChars / 4),
                outputTokens: 0,
                totalTokens: Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4),
                shortDurationSec: Number(segDuration.toFixed(1)),
                chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
                ratePerSec: Math.round(currentEmaRate),
                isGoogleVerified: false,
                isError: true,
                errorMessage: '429 Rate Limit (no retry, queued for next burst cycle)',
              }
              this.recordChunkOutput(
                chunk,
                m.id,
                `[429 RATE LIMIT EXCEEDED]\nModel: ${m.id} (Key ${lane.idx})\nStatus: 429 rate limit — no retry, queued for next burst cycle\nMessage: ${reqErr instanceof Error ? reqErr.message : String(reqErr)}`,
                errTokenUsage429,
                'error',
                reqErr instanceof Error ? reqErr.message : String(reqErr),
              )
              this.mark(job)
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              throw reqErr
            } else {
              globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
              throw reqErr
            }
          }
        }

        this.recordChunkOutput(chunk, m.id, raw, lastTokenUsage, 'success')

        // Model timestamps are LOCAL to the 1-minute segment file — shift them by
        // seg.start so every stored match carries ABSOLUTE short-video seconds.
        // Movie offset is TRIM-AWARE: reported movie times stay absolute to the ORIGINAL movie.
        // ORDERING SAFETY: the segment file is physically at most (seg.end - seg.start)
        // seconds long, so any local timestamp beyond that is a model hallucination.
        // Clamp into the segment window (and drop fully-outside matches) so a
        // minute-2 match can NEVER land at a wrong absolute time and break ordering.
        const segLocalDur = seg.end - seg.start
        const matches = parseChunkMatches(raw, chunkIndex, chunkAbsWindow(scan, chunkIndex).start, m.id)
          .filter((mm) => mm.shortStart < segLocalDur - 0.02 && mm.shortEnd > 0)
          .map((mm) => ({
            ...mm,
            shortStart: seg.start + Math.min(Math.max(0, mm.shortStart), segLocalDur),
            shortEnd: seg.start + Math.min(Math.max(0, mm.shortEnd), segLocalDur),
          }))
          .filter((mm) => mm.shortEnd - mm.shortStart > 0.02)

        // FALSE-RESULT DETECTOR: no NOT FOUND anywhere / fixed-offset A-to-Z
        // extrapolation => auto retry ONCE, then accept whatever comes.
        const suspicion = isSuspiciousChunkOutput(raw, matches)
        if (suspicion && (chunk.qualityRetries || 0) < MAX_QUALITY_RETRIES) {
          chunk.qualityRetries = (chunk.qualityRetries || 0) + 1
          chunk.status = 'pending'
          chunk.matches = []
          job.queue.push(chunkIndex)
          addLog(scan, 'warn', `${minutePrefix}Chunk ${chunkIndex}: SUSPICIOUS output on ${m.id} — ${suspicion}. Auto-retry ${chunk.qualityRetries}/${MAX_QUALITY_RETRIES} queued`)
          this.mark(job)
          return
        }
        if (suspicion) {
          addLog(scan, 'warn', `Chunk ${chunkIndex}: output still suspicious after ${MAX_QUALITY_RETRIES} auto-retry (${suspicion}) — accepting result, use manual Retry if needed`)
        }

        chunk.matches = matches
        chunk.status = matches.length > 0 ? 'match' : 'no_match'
        this.mergeMatches(scan, chunkIndex, matches, seg)
        // PIPELINE PARALLELISM: this chunk's candidates go to the verify queue
        // IMMEDIATELY — the concurrent verify workers pick them up while this
        // chunk model moves straight on to the next chunk.
        if (matches.length > 0) this.enqueueNewGroups(job)

        if (matches.length > 0) {
          addLog(scan, 'success', `${minutePrefix}Chunk ${chunkIndex}: ${matches.length} matched segment(s) found`)
        } else {
          addLog(scan, 'info', `${minutePrefix}Chunk ${chunkIndex}: no segments found in this minute`)
        }
        this.mark(job)
      } catch (err) {
        const e = err instanceof GeminiError ? err : classifyError(err, { keyIdx: lane.idx, model: m.id, requestKind: 'chunk_map' })

        // Always record error & diagnostic in chunk output so user can click 'AI output' and see what happened.
        const outerErrTokens: ChunkTokenUsage = {
          shortVideoTokens: Math.round(segDuration * currentEmaRate),
          chunkVideoTokens: Math.round(movieChunkDuration * currentEmaRate),
          promptTokens: Math.round(promptChars / 4),
          outputTokens: 0,
          totalTokens: Math.round(totalVideoDurationSec * currentEmaRate + promptChars / 4),
          shortDurationSec: Number(segDuration.toFixed(1)),
          chunkDurationSec: Number(movieChunkDuration.toFixed(1)),
          ratePerSec: Math.round(currentEmaRate),
          isGoogleVerified: false,
          isError: true,
          errorMessage: e.message.slice(0, 160),
        }
        this.recordChunkOutput(
          chunk,
          m.id,
          `[ERROR / DIAGNOSTIC]\nModel: ${m.id} (Key ${lane.idx})\nError: ${e.message}\nTime: ${new Date().toISOString()}`,
          outerErrTokens,
          'error',
          e.message,
        )

        // INFRASTRUCTURE / TRANSIENT ERROR CHECK:
        // Quota limits, rate limits, model exhaustions, empty responses, temporary 503s, and network/upload hiccups
        // are properties of the API key/Gemini service — NEVER the chunk video itself!
        // These MUST NOT consume chunk.attempts, so transient bursts never mark a chunk as 'failed'!
        const isPolicyBlocked =
          e.kind === 'policy_blocked' ||
          /prohibited_content|blocked_by_safety|safety_ratings_blocked|prompt block reason/i.test(e.message)

        const isTransientInfra =
          isPolicyBlocked ||
          e.kind === 'overloaded' ||
          e.kind === 'model_unavailable' ||
          e.kind === 'rate' ||
          e.kind === 'rpd' ||
          e.kind === 'unavailable' ||
          e.kind === 'invalid_key' ||
          e.kind === 'empty' ||
          e.kind === 'timeout' ||
          /empty model response|empty response|no text returned|blockreason|safety|file upload|processing timed out|socket|econnreset|etimedout|500|502|503|504|overload|timed out|timeout/i.test(e.message)

        if (!isTransientInfra) {
          chunk.attempts = (chunk.attempts || 0) + 1
        }

        if (isPolicyBlocked) {
          chunk.status = 'policy_blocked'
          // CRITICAL QUOTA SAVER: Do NOT re-queue this chunk on other keys!
          // The sanitized retry also hit Google hard policy — stop retries for this chunk to protect API keys quota!
          addLog(
            scan,
            'warn',
            `${minutePrefix}Chunk ${chunkIndex}: Flagged by Google Policy (PROHIBITED_CONTENT) after sanitized retry on ${m.id} (key ${lane.idx}) — automatic retries stopped to protect API keys quota; moving to next chunk`,
          )
        } else if (chunk.attempts >= MAX_CHUNK_ATTEMPTS) {
          chunk.status = 'failed'
          addLog(scan, 'error', `${minutePrefix}Chunk ${chunkIndex} reached max retry limit (${chunk.attempts}/${MAX_CHUNK_ATTEMPTS} attempts) — stopped: ${e.message.slice(0, 140)}`)
        } else if (e.kind === 'invalid_key') {
          const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
          if (laneState) {
            laneState.status = 'error'
            laneState.lastError = 'API key invalid or expired'
          }
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, 'API key invalid/expired', minutePrefix)
          addLog(scan, 'error', `API Key ${lane.idx} is invalid/expired — disabled for this scan; Chunk ${chunkIndex} re-queued for another key`)
        } else if (e.kind === 'overloaded') {
          // Count 503 attempt against this model & key quota
          incrementModelUsage(m.id, lane.apiKey)
          const quotaOutcome = globalGeminiCoordinator.handleQuotaOrRateError(
            lane.apiKey,
            m.id,
            0,
            m.rpd || 20,
            e,
            undefined,
            lane.idx,
          )
          const waitMs = quotaOutcome.waitSec * 1000
          job.cooldownUntil[this.rateKey(lane, m)] = Date.now() + waitMs
          const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
          if (laneState) {
            const ms = laneState.models.find((item) => item.id === m.id)
            if (ms) ms.state = 'cooling'
          }
          addLog(scan, 'warn', `${m.id} (key ${lane.idx}): 503 overloaded — switching to another lane; Chunk ${chunkIndex} re-queued.`)
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, '503 overloaded', minutePrefix)
        } else if (e.kind === 'rpd' || e.kind === 'rate') {
          const quotaOutcome = globalGeminiCoordinator.handleQuotaOrRateError(
            lane.apiKey,
            m.id,
            0,
            m.rpd || 20,
            e,
            undefined,
            lane.idx,
          )
          const used = getModelUsage(m.id, lane.apiKey)
          if (quotaOutcome.action === 'exhausted' && used >= (m.rpd || 20)) {
            setModelExhausted(m.id, lane.apiKey, m.rpd)
            const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
            if (laneState) {
              const ms = laneState.models.find((item) => item.id === m.id)
              if (ms) ms.state = 'exhausted'
            }
            addLog(scan, 'warn', `${m.id} (key ${lane.idx}): ${quotaOutcome.reason}. Model set aside today; remaining models on key ${lane.idx} continue. Chunk ${chunkIndex} re-queued.`)
          } else {
            const coolMs = quotaOutcome.waitSec > 0 ? quotaOutcome.waitSec * 1000 : CHUNK_COOLDOWN_MS
            job.cooldownUntil[this.rateKey(lane, m)] = Date.now() + coolMs
            const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
            if (laneState) {
              const ms = laneState.models.find((item) => item.id === m.id)
              if (ms) ms.state = 'cooling'
            }
            addLog(scan, 'warn', `${m.id} (key ${lane.idx}): ${quotaOutcome.reason}. Chunk ${chunkIndex} re-queued.`)
          }
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, quotaOutcome.reason, minutePrefix)
        } else if (e.kind === 'timeout' || /timed out|timeout/i.test(e.message)) {
          const laneState = job.scan.keyLanes?.find((l) => l.idx === lane.idx)
          if (laneState) {
            const ms = laneState.models.find((item) => item.id === m.id)
            if (ms) ms.state = 'cooling'
          }
          addLog(scan, 'warn', `${m.id} (key ${lane.idx}): Request timed out after 10 minutes without response — Chunk ${chunkIndex} re-queued for next model/lane.`)
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, 'Request timeout', minutePrefix)
        } else if (e.kind === 'empty') {
          addLog(scan, 'warn', `${minutePrefix}Chunk ${chunkIndex}: empty response on ${m.id} (key ${lane.idx}) — diagnostic saved in AI Output; chunk safely re-queued for another worker`)
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, 'Empty response', minutePrefix)
        } else {
          addLog(scan, 'warn', `Chunk ${chunkIndex} attempt ${chunk.attempts}/${MAX_CHUNK_ATTEMPTS} failed on ${m.id} (key ${lane.idx}) — re-queued: ${e.message.slice(0, 120)}`)
          this.requeueFailedChunk(job, chunkIndex, m.id, lane.idx, e.message.slice(0, 80), minutePrefix)
        }
        this.mark(job)
        throw e
      } finally {
        globalGeminiCoordinator.clearChunkRetryLock(lane.apiKey, m.id, chunkLockId)
        if (releaseGlobalLock) releaseGlobalLock(60, CHUNK_COOLDOWN_MS)
        // The short video is reused across chunks; the chunk upload is one-shot.
        if (chunkFileName) void deleteFileQuiet(lane.ai, chunkFileName)
        // Backup uploads: used copies — sab delete.
        for (const n of backupNames) void deleteFileQuiet(lane.ai, n)
        job.inFlight.delete(chunkIndex)
        st.currentChunk = null
        if (st.state === 'active') st.state = 'idle'
        this.mark(job)
      }
    }
  }

const globalForScheduler = globalThis as unknown as { __cmtScheduler?: Scheduler }
export const scheduler = globalForScheduler.__cmtScheduler || (globalForScheduler.__cmtScheduler = new Scheduler())

registerSchedulerChunkChecker((scanId: string) => {
  return scheduler.isScanActivelyChunkScanning(scanId)
})
