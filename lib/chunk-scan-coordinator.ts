import 'server-only'

import { getScan, saveScan, addLog } from './store'
import { chunkOverlapsSegRange } from './segment-range'
import type { Scan } from './types'

export interface ChunkQueueWaiter {
  scanId: string
  scanTitle: string
  ticket: number
  enqueuedAt: number
  isStopping?: () => boolean
}

export interface ChunkScanLockInfo {
  activeScanId: string | null
  activeScanTitle: string | null
  activeSince: number | null
  totalWaiting: number
  queue: Array<{ scanId: string; scanTitle: string; ticket: number; position: number }>
}

export interface ScanChunkAudit {
  totalSelectedMinutes: number
  completedMinutes: number
  currentMinuteIndex: number
  totalExpectedChunks: number
  completedChunks: number
  pendingChunks: number
  scanningChunks: number
  isFullyDone: boolean
  isTransitioningMinutes: boolean
  description: string
}

export type SchedulerChunkChecker = (scanId: string) => boolean

let registeredSchedulerChecker: SchedulerChunkChecker | null = null

/**
 * Register the live in-memory scheduler checker.
 * Completely eliminates circular module dependencies while providing
 * 100% authoritative live process status.
 */
export function registerSchedulerChunkChecker(checker: SchedulerChunkChecker) {
  registeredSchedulerChecker = checker
}

/**
 * PRIMARY CHUNK AUDITOR:
 * Audits whether ALL expected movie chunks across ALL selected short minutes
 * have genuinely finished scanning.
 *
 * Notice / Check:
 * When Minute 1 chunks complete, Minute 2 still has pending chunks.
 * This auditor detects that total pending chunks > 0 and marks
 * isFullyDone = false and isTransitioningMinutes = true!
 */
export function evaluateScanChunkProgress(scan: Scan): ScanChunkAudit {
  const segments = scan.shortSegments || []
  const selectedSegs = segments.length > 0 ? segments.filter((s) => s.selected !== false) : []

  if (selectedSegs.length === 0) {
    const chunks = scan.chunks || []
    const pending = chunks.filter((c) => c.status === 'pending').length
    const scanning = chunks.filter((c) => c.status === 'scanning').length
    const done = chunks.filter(
      (c) => c.status === 'done' || c.status === 'policy_blocked' || (c.status === 'cancelled' && c.skippedEarlyStop),
    ).length
    const isDone = chunks.length > 0 && pending === 0 && scanning === 0
    return {
      totalSelectedMinutes: 1,
      completedMinutes: isDone ? 1 : 0,
      currentMinuteIndex: 0,
      totalExpectedChunks: chunks.length,
      completedChunks: done,
      pendingChunks: pending,
      scanningChunks: scanning,
      isFullyDone: isDone,
      isTransitioningMinutes: false,
      description: isDone ? 'All chunks complete' : `${done}/${chunks.length} chunks done (${pending} pending)`,
    }
  }

  let totalExpected = 0
  let totalCompleted = 0
  let totalPending = 0
  let totalScanning = 0
  let completedMinutes = 0
  let transitioning = false

  const currMinIdx = scan.currentShortSegment ?? 0

  for (let i = 0; i < selectedSegs.length; i++) {
    const seg = selectedSegs[i]
    let segExpected = 0
    let segCompleted = 0
    let segPending = 0
    let segScanning = 0

    const chunks = seg.chunks || []
    for (const c of chunks) {
      if (chunkOverlapsSegRange(scan, seg, c.index)) {
        segExpected++
        if (c.status === 'done' || c.status === 'policy_blocked' || (c.status === 'cancelled' && c.skippedEarlyStop)) {
          segCompleted++
        } else if (c.status === 'scanning') {
          segScanning++
        } else if (c.status === 'pending') {
          segPending++
        }
      }
    }

    totalExpected += segExpected
    totalCompleted += segCompleted
    totalPending += segPending
    totalScanning += segScanning

    const isSegDone = seg.status === 'done' || (segExpected > 0 && segPending === 0 && segScanning === 0)
    if (isSegDone) {
      completedMinutes++
    }

    // Key check: If an earlier minute (e.g. Minute 1) has finished its chunks,
    // but any subsequent minute (e.g. Minute 2) still has pending chunks:
    if (i < selectedSegs.length - 1 && isSegDone) {
      const subsequentHasPending = selectedSegs
        .slice(i + 1)
        .some((nextSeg) =>
          (nextSeg.chunks || []).some((c) => c.status === 'pending' && chunkOverlapsSegRange(scan, nextSeg, c.index)),
        )
      if (subsequentHasPending) {
        transitioning = true
      }
    }
  }

  const isFullyDone =
    selectedSegs.length > 0 && completedMinutes === selectedSegs.length && totalPending === 0 && totalScanning === 0

  let description = ''
  if (transitioning) {
    description = `Minute ${currMinIdx + 1}/${selectedSegs.length} chunks finished. Agla minute start ho raha hai (${totalPending} chunks remaining). Scan 2 queue me wait karega.`
  } else if (isFullyDone) {
    description = `All ${selectedSegs.length} short minute(s) completed (${totalCompleted}/${totalExpected} chunks scanned).`
  } else {
    description = `Scanning minute ${currMinIdx + 1}/${selectedSegs.length}: ${totalCompleted}/${totalExpected} chunks complete, ${totalPending} pending, ${totalScanning} in-flight.`
  }

  return {
    totalSelectedMinutes: selectedSegs.length,
    completedMinutes,
    currentMinuteIndex: currMinIdx,
    totalExpectedChunks: totalExpected,
    completedChunks: totalCompleted,
    pendingChunks: totalPending,
    scanningChunks: totalScanning,
    isFullyDone,
    isTransitioningMinutes: transitioning,
    description,
  }
}

class ChunkScanCoordinator {
  private activeScanId: string | null = null
  private activeScanTitle: string | null = null
  private activeSince: number | null = null
  private lastActiveScanId: string | null = null
  private waitingQueue: ChunkQueueWaiter[] = []
  private nextTicket: number = 1
  private watchdogTimer: NodeJS.Timeout | null = null

  constructor() {
    this.startWatchdog()
  }

  /**
   * 1-second global monitor.
   * Every 1000ms, verifies if the currently active chunk scan is still genuinely
   * running. Uses both primary chunk count audit AND in-memory scheduler state
   * so transitions between Minute 1 and Minute 2 never prematurely drop the lock!
   */
  private startWatchdog() {
    if (this.watchdogTimer) return
    this.watchdogTimer = setInterval(() => {
      this.checkActiveScanLiveness()
      this.refreshQueuePositions()
    }, 1000)
    // Don't prevent Node process exit
    if (typeof this.watchdogTimer.unref === 'function') {
      this.watchdogTimer.unref()
    }
  }

  private checkActiveScanLiveness() {
    if (!this.activeScanId) return
    try {
      const scan = getScan(this.activeScanId)
      if (!scan) {
        console.warn(`[ChunkScanCoordinator] Active scan ${this.activeScanId} missing from store — releasing lock`)
        this.clearActiveLock()
        return
      }

      // Check 1: User explicitly stopped or scan failed with error
      if (scan.status === 'stopped' || scan.status === 'error') {
        console.log(`[ChunkScanCoordinator] Active scan ${this.activeScanId} status is "${scan.status}" — releasing lock`)
        this.clearActiveLock()
        return
      }

      // Check 2 (BACKUP METHOD 1 - LIVE IN-MEMORY SCHEDULER STATE):
      // If scheduler is actively running chunk scan (even during minute-to-minute transitions),
      // NEVER release the lock!
      if (registeredSchedulerChecker && registeredSchedulerChecker(this.activeScanId)) {
        return
      }

      // Check 3 (PRIMARY REQUIREMENT - CHUNK AUDIT ACROSS ALL MINUTES):
      // Verify whether all expected chunks across all short minutes have genuinely finished.
      const audit = evaluateScanChunkProgress(scan)
      if (!audit.isFullyDone) {
        // Chunks are NOT fully done! (e.g. Minute 1 completed, but Minute 2 still has pending chunks)
        // Keep lock held!
        return
      }

      // Check 4: Terminal status confirmation
      if (scan.status === 'done' || (scan.status === 'verifying' && audit.isFullyDone)) {
        console.log(
          `[ChunkScanCoordinator] Active scan ${this.activeScanId} fully completed all chunks (${audit.completedChunks}/${audit.totalExpectedChunks}) — releasing lock`,
        )
        this.clearActiveLock()
      }
    } catch {
      // ignore
    }
  }

  private clearActiveLock() {
    if (this.activeScanId) {
      this.lastActiveScanId = this.activeScanId
    }
    this.activeScanId = null
    this.activeScanTitle = null
    this.activeSince = null
  }

  private refreshQueuePositions() {
    for (let i = 0; i < this.waitingQueue.length; i++) {
      const item = this.waitingQueue[i]
      const pos = i + 1
      try {
        const s = getScan(item.scanId)
        if (
          s &&
          (!s.chunkQueue ||
            s.chunkQueue.position !== pos ||
            s.chunkQueue.totalWaiting !== this.waitingQueue.length ||
            s.chunkQueue.activeScanId !== this.activeScanId)
        ) {
          s.chunkQueue = {
            ticket: item.ticket,
            position: pos,
            totalWaiting: this.waitingQueue.length,
            activeScanId: this.activeScanId,
            activeScanTitle: this.activeScanTitle,
            enqueuedAt: item.enqueuedAt,
          }
          saveScan(s)
        }
      } catch {
        // ignore
      }
    }
  }

  /**
   * Returns whether ANY scan is currently running its chunk scan,
   * or if scanId is specified, whether that specific scan holds the lock.
   */
  public isChunkScanActive(scanId?: string): boolean {
    if (scanId) {
      return this.activeScanId === scanId
    }
    return this.activeScanId !== null
  }

  public getActiveChunkScan(): { scanId: string; scanTitle: string; activeSince: number } | null {
    if (!this.activeScanId) return null
    return {
      scanId: this.activeScanId,
      scanTitle: this.activeScanTitle || this.activeScanId,
      activeSince: this.activeSince || Date.now(),
    }
  }

  public getQueuePosition(scanId: string): {
    position: number
    ticket: number
    totalWaiting: number
    activeScanId: string | null
  } | null {
    const idx = this.waitingQueue.findIndex((w) => w.scanId === scanId)
    if (idx === -1) return null
    return {
      position: idx + 1,
      ticket: this.waitingQueue[idx].ticket,
      totalWaiting: this.waitingQueue.length,
      activeScanId: this.activeScanId,
    }
  }

  public getLockInfo(): ChunkScanLockInfo {
    return {
      activeScanId: this.activeScanId,
      activeScanTitle: this.activeScanTitle,
      activeSince: this.activeSince,
      totalWaiting: this.waitingQueue.length,
      queue: this.waitingQueue.map((w, idx) => ({
        scanId: w.scanId,
        scanTitle: w.scanTitle,
        ticket: w.ticket,
        position: idx + 1,
      })),
    }
  }

  /**
   * Acquire the exclusive Global Chunk Scan Lock.
   * If another scan is already running chunk scan, this scan waits in FIFO order.
   * Checks every 1 second in the background.
   */
  public async acquireChunkScanLock(params: {
    scanId: string
    scanTitle?: string
    onWait?: (message: string, position: number, ticket: number, activeScanId: string | null) => void
    isStopping?: () => boolean
  }): Promise<() => void> {
    const { scanId, scanTitle = scanId, onWait, isStopping } = params

    // Re-entrant: if this scan ALREADY holds the lock, return immediately
    if (this.activeScanId === scanId) {
      return () => this.releaseChunkScanLock(scanId)
    }

    // Check if free and no one in queue ahead of us
    if (this.activeScanId === null && this.waitingQueue.length === 0) {
      // Double check that previous active scan is not transitioning between minutes
      let canTakeImmediate = true
      if (this.lastActiveScanId && this.lastActiveScanId !== scanId) {
        const prevScan = getScan(this.lastActiveScanId)
        if (prevScan && prevScan.status !== 'stopped' && prevScan.status !== 'error') {
          const isPrevSchedulerLive = registeredSchedulerChecker ? registeredSchedulerChecker(this.lastActiveScanId) : false
          const prevAudit = evaluateScanChunkProgress(prevScan)
          if (isPrevSchedulerLive || !prevAudit.isFullyDone) {
            canTakeImmediate = false
            this.activeScanId = this.lastActiveScanId
            this.activeScanTitle = prevScan.shortName || this.lastActiveScanId
          }
        }
      }

      if (canTakeImmediate) {
        this.activeScanId = scanId
        this.activeScanTitle = scanTitle
        this.activeSince = Date.now()
        this.lastActiveScanId = scanId
        const s = getScan(scanId)
        if (s) {
          delete s.chunkQueue
          saveScan(s)
        }
        return () => this.releaseChunkScanLock(scanId)
      }
    }

    // Otherwise, assign a ticket and enqueue in FIFO order
    let waiter = this.waitingQueue.find((w) => w.scanId === scanId)
    if (!waiter) {
      waiter = {
        scanId,
        scanTitle,
        ticket: this.nextTicket++,
        enqueuedAt: Date.now(),
        isStopping,
      }
      this.waitingQueue.push(waiter)
    }

    const s = getScan(scanId)
    const position = this.waitingQueue.findIndex((w) => w.scanId === scanId) + 1
    if (s) {
      s.chunkQueue = {
        ticket: waiter.ticket,
        position,
        totalWaiting: this.waitingQueue.length,
        activeScanId: this.activeScanId,
        activeScanTitle: this.activeScanTitle,
        enqueuedAt: waiter.enqueuedAt,
      }
      addLog(
        s,
        'info',
        `Global Chunk Scan Queue: Position #${position} (Ticket #${waiter.ticket}) — Scan "${this.activeScanTitle || this.activeScanId}" ka chunk scan chal raha hai. Chunks ready hain, bari aane par auto-lock lag kar start hoga (har 1s background me check ho raha hai).`,
      )
      saveScan(s)
    }

    let lastLogTime = 0
    let lastReportedPos = -1

    // 1-second monitoring loop
    while (true) {
      if (isStopping && isStopping()) {
        this.removeFromQueue(scanId)
        throw new Error('Scan stop requested while waiting for chunk scan lock')
      }

      // Check active scan liveness
      this.checkActiveScanLiveness()

      // Is lock free AND is this scan first in line?
      if (this.activeScanId === null && this.waitingQueue[0]?.scanId === scanId) {
        // DOUBLE CHECK BEFORE GRANTING LOCK:
        // Ensure that NO other scan is still actively running or transitioning between minutes!
        let canTakeLock = true
        if (this.lastActiveScanId && this.lastActiveScanId !== scanId) {
          const prevScan = getScan(this.lastActiveScanId)
          if (prevScan && prevScan.status !== 'stopped' && prevScan.status !== 'error') {
            const isPrevSchedulerLive = registeredSchedulerChecker ? registeredSchedulerChecker(this.lastActiveScanId) : false
            const prevAudit = evaluateScanChunkProgress(prevScan)
            if (isPrevSchedulerLive || !prevAudit.isFullyDone) {
              // The previous scan is still processing chunks or transitioning from Minute 1 to Minute 2!
              canTakeLock = false
              this.activeScanId = this.lastActiveScanId
              this.activeScanTitle = prevScan.shortName || this.lastActiveScanId
              console.log(
                `[ChunkScanCoordinator] Re-asserted lock for scan ${this.lastActiveScanId} during minute transition (${prevAudit.pendingChunks} chunks pending). Scan ${scanId} remains in queue.`,
              )
            }
          }
        }

        if (canTakeLock) {
          this.waitingQueue.shift()
          this.activeScanId = scanId
          this.activeScanTitle = scanTitle
          this.activeSince = Date.now()
          this.lastActiveScanId = scanId

          const freshScan = getScan(scanId)
          if (freshScan) {
            delete freshScan.chunkQueue
            addLog(freshScan, 'success', `Chunk scan lock acquired (Ticket #${waiter.ticket})! Chunks scanning phase shuru ho raha hai.`)
            saveScan(freshScan, { immediate: true })
          }
          return () => this.releaseChunkScanLock(scanId)
        }
      }

      // If activeScanId matches this scan somehow
      if (this.activeScanId === scanId) {
        this.removeFromQueue(scanId)
        return () => this.releaseChunkScanLock(scanId)
      }

      const currentPos = this.waitingQueue.findIndex((w) => w.scanId === scanId) + 1
      const now = Date.now()

      if (currentPos !== lastReportedPos || now - lastLogTime >= 10_000) {
        lastReportedPos = currentPos
        lastLogTime = now

        let detailMsg = ''
        if (this.activeScanId) {
          const actScan = getScan(this.activeScanId)
          if (actScan) {
            const actAudit = evaluateScanChunkProgress(actScan)
            if (actAudit.isTransitioningMinutes) {
              detailMsg = ` — Active: "${this.activeScanTitle}" (Minute ${actAudit.currentMinuteIndex + 1}/${actAudit.totalSelectedMinutes} complete, agla minute start ho raha hai — ${actAudit.pendingChunks} chunks pending)`
            } else {
              detailMsg = ` — Active: "${this.activeScanTitle}" (${actAudit.completedChunks}/${actAudit.totalExpectedChunks} chunks complete, ${actAudit.pendingChunks} pending)`
            }
          }
        }

        const msg = `Global Chunk Scan Queue: Position #${currentPos} of ${this.waitingQueue.length} (Ticket #${waiter.ticket})${detailMsg}. Monitoring every 1s...`
        if (onWait) {
          onWait(msg, currentPos, waiter.ticket, this.activeScanId)
        }
        const currentScan = getScan(scanId)
        if (currentScan) {
          currentScan.chunkQueue = {
            ticket: waiter.ticket,
            position: currentPos,
            totalWaiting: this.waitingQueue.length,
            activeScanId: this.activeScanId,
            activeScanTitle: this.activeScanTitle,
            enqueuedAt: waiter.enqueuedAt,
          }
          saveScan(currentScan)
        }
      }

      // Strict requirement: check every 1 second
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }

  /**
   * Release the chunk scan lock held by scanId.
   * If force is false, verifies whether the scan still has pending chunks or is running in scheduler.
   */
  public releaseChunkScanLock(scanId: string, force: boolean = false) {
    if (this.activeScanId === scanId) {
      if (!force) {
        const scan = getScan(scanId)
        const isSchedulerLive = registeredSchedulerChecker ? registeredSchedulerChecker(scanId) : false
        const audit = scan ? evaluateScanChunkProgress(scan) : null

        if (isSchedulerLive || (audit && !audit.isFullyDone)) {
          console.warn(
            `[ChunkScanCoordinator] Blocked premature lock release for scan ${scanId}: ` +
              `scheduler active=${isSchedulerLive}, chunks complete=${audit?.isFullyDone ?? false} ` +
              `(${audit?.pendingChunks ?? 0} chunks pending across minutes). Keeping lock held!`,
          )
          return
        }
      }

      console.log(`[ChunkScanCoordinator] Released chunk scan lock for scan ${scanId}`)
      this.clearActiveLock()
    }
    this.removeFromQueue(scanId)
  }

  /**
   * Remove a scan from the waiting queue if stopped or cancelled.
   */
  public removeFromQueue(scanId: string) {
    const idx = this.waitingQueue.findIndex((w) => w.scanId === scanId)
    if (idx !== -1) {
      this.waitingQueue.splice(idx, 1)
      const s = getScan(scanId)
      if (s && s.chunkQueue) {
        delete s.chunkQueue
        saveScan(s)
      }
    }
    this.refreshQueuePositions()
  }
}

export const globalChunkScanCoordinator = new ChunkScanCoordinator()
