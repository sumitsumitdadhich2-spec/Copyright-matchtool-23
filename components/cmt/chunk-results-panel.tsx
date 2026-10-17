'use client'

import { useEffect, useState } from 'react'
import { useSWRConfig } from 'swr'
import {
  ChevronDown,
  ChevronRight,
  Clock3,
  FileText,
  RotateCcw,
  AlertTriangle,
  CheckCircle2,
  Film,
  Video,
  Calculator,
  Sparkles,
  Copy,
  Check,
} from 'lucide-react'
import type { Scan, ChunkState, ChunkRawOutput } from '@/lib/types'
import { fmtTime } from '@/lib/format'
import { displayModelName } from '@/lib/models'

const CHUNK_SECONDS = 60

/** Chunks to display for a selected short minute: the active minute is mirrored
 *  into scan.chunks (live states); other minutes come from shortSegments[i].chunks. */
function chunksForSegment(scan: Scan, segIdx: number): ChunkState[] {
  const segs = scan.shortSegments
  if (!segs || segs.length === 0) return scan.chunks || []
  if (segIdx === (scan.currentShortSegment ?? 0)) return scan.chunks || segs[segIdx]?.chunks || []
  return segs[segIdx]?.chunks ?? []
}

export function ChunkResultsPanel({ scan }: { scan: Scan }) {
  const segs = scan.shortSegments || []
  const multi = segs.length > 1
  const [selected, setSelected] = useState<number | null>(null)
  const activeSeg = scan.currentShortSegment ?? 0
  const segIdx = selected ?? activeSeg

  useEffect(() => {
    setSelected(null)
  }, [scan.id, segs.length])

  const chunks = chunksForSegment(scan, segIdx) || []
  const visible = chunks.filter(
    (c) => c && (c.status !== 'pending' || (c.matches?.length ?? 0) > 0 || (c.rawOutputs?.length ?? 0) > 0),
  )
  if (visible.length === 0 && !multi) return null

  return (
    <section aria-label="Chunk timeline results" className="panel">
      <div className="flex flex-wrap items-center gap-2">
        <Clock3 className="size-4 text-primary" aria-hidden />
        <h2 className="text-sm font-semibold">Chunk Timeline — Matches Per Minute</h2>
        {multi && (
          <span className="rounded-full bg-primary/15 px-2 py-0.5 font-mono text-[10px] text-primary">
            short minute {segIdx + 1}/{segs.length}
          </span>
        )}
        <span className="ml-auto font-mono text-xs text-muted-foreground">
          {visible.length}/{scan.chunkCount} chunk(s)
        </span>
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        Har movie minute ke liye ek hi AI call: short video + wo chunk. HISSA 2 ki matched lines yahan dikhti
        hain (movie time global hai), aur full raw output arrow se dekh sakte ho.
      </p>

      {multi && (
        <div className="mt-2 flex flex-wrap gap-1.5" role="tablist" aria-label="Short video minutes">
          {segs.map((seg) => {
            const isSel = seg.index === segIdx
            return (
              <button
                key={seg.index}
                type="button"
                role="tab"
                aria-selected={isSel}
                onClick={() => setSelected(seg.index === activeSeg ? null : seg.index)}
                title={`Short ${fmtTime(seg.start)}–${fmtTime(seg.end)} — ${seg.status}`}
                className={`flex items-center gap-1 rounded-full border px-2.5 py-0.5 font-mono text-[10px] transition-colors ${
                  isSel
                    ? 'border-primary bg-primary/15 text-primary'
                    : 'border-input text-muted-foreground hover:bg-secondary'
                }`}
              >
                Min {seg.index + 1}
                {seg.status === 'done' && <span className="text-success" aria-hidden>✓</span>}
                {(seg.status === 'scanning' || seg.status === 'verifying') && (
                  <span className="inline-block size-1.5 animate-pulse rounded-full bg-primary" aria-hidden />
                )}
              </button>
            )
          })}
        </div>
      )}

      {visible.length === 0 ? (
        <p className="mt-3 text-xs text-muted-foreground">Is minute ke liye abhi koi chunk result nahi hai.</p>
      ) : (
        <div className="mt-3 flex flex-col gap-1.5">
          {visible.map((c) => (
            <ChunkRow key={c.index} scan={scan} chunk={c} segIdx={multi ? segIdx : undefined} />
          ))}
        </div>
      )}
    </section>
  )
}

function ChunkRow({ scan, chunk, segIdx }: { scan: Scan; chunk: ChunkState; segIdx?: number }) {
  const [open, setOpen] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState<string | null>(null)
  const { mutate } = useSWRConfig()
  // TRIM OFFSET: chunks cover ONLY the confirmed trim range, so absolute
  // original-movie time = trimStart + index * 60 (capped at trimEnd).
  const trimStart = scan.movieTrimStart ?? 0
  const rangeEnd = scan.movieTrimEnd ?? scan.movieDuration ?? Number.POSITIVE_INFINITY
  const base = trimStart + chunk.index * CHUNK_SECONDS
  const end = Math.min(base + CHUNK_SECONDS, rangeEnd)
  const matches = chunk.matches || []
  const raws = chunk.rawOutputs || []
  const scanning = chunk.status === 'scanning'
  // RESCAN LOCK (UI): retry stays disabled while ANY candidate group is still
  // pending/verifying/rescanning — verification queue must fully drain first.
  const pendingVerify = (scan.candidateGroups || []).filter(
    (g) => g.status === 'pending' || g.status === 'verifying' || g.status === 'rescanning',
  ).length
  const isFailed = chunk.status === 'failed' || chunk.status === 'policy_blocked'
  const isPolicyBlocked = chunk.status === 'policy_blocked'
  const canRetry = !scanning && chunk.status !== 'pending' && (isFailed || pendingVerify === 0)
  const retryTitle = isPolicyBlocked
    ? 'Google Policy Blocked chunk — manual retry if needed'
    : isFailed
      ? 'Is failed chunk ko dobara queue me daal kar rescan karo'
      : pendingVerify > 0
        ? `Verification in progress — ${pendingVerify} candidate group(s) pending. Rescan tabhi milega jab saare candidates verify ho jayen.`
        : 'Is chunk ko dobara chunk models se map karwao'

  async function retry() {
    if (retrying) return
    setRetrying(true)
    setRetryError(null)
    try {
      const segParam = segIdx !== undefined ? `?segment=${segIdx}` : ''
      const res = await fetch(`/api/scans/${scan.id}/chunks/${chunk.index}/retry${segParam}`, { method: 'POST' })
      const j = (await res.json()) as { ok: boolean; error?: string }
      if (!j.ok) setRetryError(j.error || 'Retry failed')
      void mutate(`/api/scans/${scan.id}`)
    } catch {
      setRetryError('Retry failed — network error')
    } finally {
      setRetrying(false)
    }
  }

  return (
    <div className="rounded-md border border-border bg-background">
      <div className="flex flex-wrap items-center gap-2 px-3 py-2">
        <span className="font-mono text-xs font-semibold text-foreground">
          {fmtTime(base)} – {fmtTime(end)}
        </span>
        <span className="font-mono text-[10px] text-muted-foreground">chunk {chunk.index}</span>
        {scanning && <span className="rounded-full bg-primary/15 px-2 py-0.5 text-[10px] text-primary">scanning…</span>}
        {chunk.status === 'no_match' && matches.length === 0 && (
          <span className="text-[11px] text-muted-foreground">no matches in this minute</span>
        )}
        {chunk.status === 'failed' && <span className="text-[11px] text-destructive">failed</span>}
        {chunk.status === 'policy_blocked' && (
          <span className="rounded-full border border-amber-500/30 bg-amber-500/15 px-2 py-0.5 text-[10px] font-medium text-amber-500">
            Flagged by Google Policy (PROHIBITED_CONTENT)
          </span>
        )}
        <div className="flex flex-wrap items-center gap-1.5">
          {matches.map((f, i) => (
            <span
              key={`${f.shortStart}-${f.movieStart}-${i}`}
              title={`Short ${fmtTime(f.shortStart)}–${fmtTime(f.shortEnd)} → Movie ${fmtTime(f.movieStart)}–${fmtTime(f.movieEnd)} · ${displayModelName(f.model)}`}
              className="inline-flex items-center gap-1 rounded-full border border-primary/30 bg-primary/15 px-2 py-0.5 font-mono text-[10px] text-primary"
            >
              {fmtTime(f.shortStart)}–{fmtTime(f.shortEnd)}
              <span aria-hidden>→</span>
              <span className="font-semibold">
                {fmtTime(f.movieStart)}–{fmtTime(f.movieEnd)}
              </span>
            </span>
          ))}
        </div>
        <button
          type="button"
          onClick={() => void retry()}
          disabled={!canRetry || retrying}
          title={retryTitle}
          className="ml-auto flex items-center gap-1 rounded-md border border-input px-2 py-1 text-[11px] font-medium hover:bg-secondary disabled:opacity-30"
        >
          <RotateCcw className={`size-3.5 ${retrying ? 'animate-spin' : ''}`} aria-hidden />
          {retrying ? 'Retrying…' : isFailed ? 'Retry failed' : pendingVerify > 0 ? `Verify pending (${pendingVerify})` : 'Retry'}
        </button>
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          disabled={raws.length === 0}
          aria-expanded={open}
          className="flex items-center gap-1 rounded-md border border-input px-2 py-1 text-[11px] font-medium hover:bg-secondary disabled:opacity-30"
        >
          {open ? <ChevronDown className="size-3.5" aria-hidden /> : <ChevronRight className="size-3.5" aria-hidden />}
          AI output {raws.length > 0 ? `(${raws.length})` : ''}
        </button>
      </div>
      {retryError && (
        <p className="border-t border-border px-3 py-1.5 text-[11px] text-destructive" role="alert">
          {retryError}
        </p>
      )}
      {open && raws.length > 0 && (
        <div className="flex flex-col gap-2.5 border-t border-border px-3 py-2.5">
          {raws.map((r, i) => (
            <ChunkRawCard key={`${r.t}-${i}`} r={r} scan={scan} segIdx={segIdx} />
          ))}
        </div>
      )}
    </div>
  )
}

function ChunkTokenMetrics({
  raw,
  scan,
  segIdx,
}: {
  raw: ChunkRawOutput
  scan: Scan
  segIdx?: number
}) {
  const isError =
    raw.status === 'error' ||
    raw.tokens?.isError ||
    raw.text.startsWith('[ERROR') ||
    raw.text.includes('429 RATE LIMIT') ||
    raw.text.includes('503 OVERLOADED')

  // Use stored tokens or fallback calculation for older entries
  const segs = scan.shortSegments
  const activeSegIdx = segIdx ?? scan.currentShortSegment ?? 0
  const seg = segs && segs[activeSegIdx]
  const segDur = seg ? Math.max(1, seg.end - seg.start) : 60
  const chunkDur = 60
  const defaultRate = 260
  const promptToks = 1200
  const outToks = isError ? 0 : Math.round(raw.text.length / 4)

  const tokens = raw.tokens || {
    shortVideoTokens: Math.round(segDur * defaultRate),
    chunkVideoTokens: Math.round(chunkDur * defaultRate),
    promptTokens: promptToks,
    outputTokens: outToks,
    totalTokens: Math.round((segDur + chunkDur) * defaultRate + promptToks + outToks),
    shortDurationSec: Number(segDur.toFixed(1)),
    chunkDurationSec: chunkDur,
    ratePerSec: defaultRate,
    isGoogleVerified: false,
    isError,
    errorMessage: isError ? 'Request error' : undefined,
  }

  return (
    <div className="space-y-2 border-b border-border bg-muted/20 p-2.5">
      {/* 5 Data Metric Cards */}
      <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-5">
        {/* Short Video */}
        <div className="flex flex-col justify-between rounded-md border border-border/80 bg-background/90 p-2">
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Video className="size-3.5 text-blue-500" aria-hidden />
            <span className="text-[10px] font-semibold uppercase tracking-wider">Short Video</span>
          </div>
          <div className="mt-1">
            <span className="font-mono text-sm font-bold text-foreground">
              {tokens.shortVideoTokens.toLocaleString()}
            </span>
            <span className="ml-1 text-[10px] text-muted-foreground">tok</span>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{tokens.shortDurationSec}s @ 24 fps</p>
        </div>

        {/* Chunk 1m Video */}
        <div className="flex flex-col justify-between rounded-md border border-border/80 bg-background/90 p-2">
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Film className="size-3.5 text-indigo-500" aria-hidden />
            <span className="text-[10px] font-semibold uppercase tracking-wider">Chunk Video</span>
          </div>
          <div className="mt-1">
            <span className="font-mono text-sm font-bold text-foreground">
              {tokens.chunkVideoTokens.toLocaleString()}
            </span>
            <span className="ml-1 text-[10px] text-muted-foreground">tok</span>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{tokens.chunkDurationSec}s (1 min chunk)</p>
        </div>

        {/* Prompt */}
        <div className="flex flex-col justify-between rounded-md border border-border/80 bg-background/90 p-2">
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <FileText className="size-3.5 text-amber-500" aria-hidden />
            <span className="text-[10px] font-semibold uppercase tracking-wider">Prompt / Rule</span>
          </div>
          <div className="mt-1">
            <span className="font-mono text-sm font-bold text-foreground">
              {tokens.promptTokens.toLocaleString()}
            </span>
            <span className="ml-1 text-[10px] text-muted-foreground">tok</span>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">Instructions</p>
        </div>

        {/* Output */}
        <div
          className={`flex flex-col justify-between rounded-md border p-2 ${
            isError ? 'border-destructive/30 bg-destructive/10' : 'border-border/80 bg-background/90'
          }`}
        >
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Sparkles className={`size-3.5 ${isError ? 'text-destructive' : 'text-emerald-500'}`} aria-hidden />
            <span className="text-[10px] font-semibold uppercase tracking-wider">Output Tokens</span>
          </div>
          <div className="mt-1">
            <span className={`font-mono text-sm font-bold ${isError ? 'text-destructive' : 'text-foreground'}`}>
              {tokens.outputTokens.toLocaleString()}
            </span>
            <span className="ml-1 text-[10px] text-muted-foreground">tok</span>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">
            {isError ? '0 (Error / No Output)' : 'Generated Text'}
          </p>
        </div>

        {/* Total Tokens */}
        <div className="col-span-2 flex flex-col justify-between rounded-md border border-primary/40 bg-primary/10 p-2 sm:col-span-1">
          <div className="flex items-center gap-1.5 text-primary">
            <Calculator className="size-3.5" aria-hidden />
            <span className="text-[10px] font-bold uppercase tracking-wider">Total Tokens</span>
          </div>
          <div className="mt-1">
            <span className="font-mono text-sm font-black text-primary">
              {tokens.totalTokens.toLocaleString()}
            </span>
            <span className="ml-1 text-[10px] text-primary/80">tok</span>
          </div>
          <p className="mt-0.5 text-[10px] font-medium text-primary/80">
            {tokens.isGoogleVerified ? 'Google Verified ✓' : `Rate ~${tokens.ratePerSec || 260} tok/s`}
          </p>
        </div>
      </div>

      {/* Formula & Calculation Breakdown Banner */}
      <div
        className={`flex flex-wrap items-center justify-between gap-1.5 rounded border px-2 py-1 text-[10px] font-mono ${
          isError
            ? 'border-destructive/30 bg-destructive/15 text-destructive'
            : 'border-border/70 bg-background text-muted-foreground'
        }`}
      >
        <div className="flex flex-wrap items-center gap-1">
          <span className="font-semibold text-foreground">Token Calculation:</span>
          <span>Short ({tokens.shortVideoTokens.toLocaleString()})</span>
          <span>+</span>
          <span>Chunk ({tokens.chunkVideoTokens.toLocaleString()})</span>
          <span>+</span>
          <span>Prompt ({tokens.promptTokens.toLocaleString()})</span>
          {tokens.outputTokens > 0 && (
            <>
              <span>+</span>
              <span>Output ({tokens.outputTokens.toLocaleString()})</span>
            </>
          )}
          <span>=</span>
          <span className="font-bold text-foreground">
            {tokens.totalTokens.toLocaleString()} Total Tokens
          </span>
        </div>
        {isError && (
          <span className="text-[10px] font-sans font-medium text-destructive">
            ⚠️ Input videos prepared & sent; model hit error before response.
          </span>
        )}
      </div>
    </div>
  )
}

function ChunkRawCard({
  r,
  scan,
  segIdx,
}: {
  r: ChunkRawOutput
  scan: Scan
  segIdx?: number
}) {
  const [copied, setCopied] = useState(false)
  const isError =
    r.status === 'error' ||
    r.tokens?.isError ||
    r.text.startsWith('[ERROR') ||
    r.text.includes('429 RATE LIMIT') ||
    r.text.includes('503 OVERLOADED')

  async function copyText() {
    try {
      await navigator.clipboard.writeText(r.text)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      /* ignore */
    }
  }

  return (
    <div
      className={`rounded-md border bg-card transition-colors ${
        isError ? 'border-destructive/40 shadow-sm' : 'border-border'
      }`}
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-border bg-muted/40 px-2.5 py-1.5">
        <FileText className="size-3.5 text-muted-foreground" aria-hidden />
        <span className="font-mono text-xs font-semibold text-foreground">{displayModelName(r.model)}</span>
        {isError ? (
          <span className="inline-flex items-center gap-1 rounded-full border border-destructive/30 bg-destructive/15 px-2 py-0.5 text-[10px] font-medium text-destructive">
            <AlertTriangle className="size-3" aria-hidden />
            Attempt Failed (429 / 503 / Error)
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/15 px-2 py-0.5 text-[10px] font-medium text-emerald-600 dark:text-emerald-400">
            <CheckCircle2 className="size-3" aria-hidden />
            Analysis Succeeded
          </span>
        )}
        <span className="ml-auto font-mono text-[10px] text-muted-foreground">
          {new Date(r.t).toLocaleTimeString()}
        </span>
        <button
          type="button"
          onClick={copyText}
          title="Copy output text"
          className="flex items-center gap-1 rounded border border-input bg-background px-1.5 py-0.5 text-[10px] text-muted-foreground hover:bg-secondary"
        >
          {copied ? <Check className="size-3 text-emerald-500" /> : <Copy className="size-3" />}
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>

      {/* Token Metrics Section */}
      <ChunkTokenMetrics raw={r} scan={scan} segIdx={segIdx} />

      {/* Raw Text Output */}
      <pre
        className={`max-h-80 overflow-auto whitespace-pre-wrap break-words p-2.5 font-mono text-[10px] leading-relaxed ${
          isError ? 'bg-destructive/5 text-destructive-foreground' : 'text-foreground'
        }`}
      >
        {r.text}
      </pre>
    </div>
  )
}
