'use client'

import { useState } from 'react'
import useSWR from 'swr'
import {
  KeyRound,
  RefreshCw,
  ShieldAlert,
  Zap,
} from 'lucide-react'
import type { OrchestratorLiveState, OrchestratorLogEvent } from '@/lib/types'

const fetcher = (url: string) => fetch(url).then((res) => res.json())

export function OrchestratorMatrixPanel() {
  const { data, mutate, isValidating } = useSWR<{
    state: OrchestratorLiveState
    logs: OrchestratorLogEvent[]
    timestamp: number
  }>('/api/orchestrator', fetcher, {
    refreshInterval: 3000,
  })

  const [probingModel, setProbingModel] = useState<string | null>(null)
  const [probeResult, setProbeResult] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'matrix' | 'models' | 'logs'>('matrix')

  const state = data?.state
  const logs = data?.logs || []

  async function triggerProbe(modelId: string) {
    setProbingModel(modelId)
    setProbeResult(null)
    try {
      const res = await fetch('/api/orchestrator', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'probe', model: modelId }),
      })
      const json = await res.json()
      if (json.ok && json.result) {
        const r = json.result
        const latency = (r.latencyMs / 1000).toFixed(2)
        if (r.ok) {
          setProbeResult(`✅ ${modelId} Choke OK in ${latency}s — Reply: "${r.reply || 'hy'}"`)
        } else {
          setProbeResult(`⚠️ ${modelId} Choke Failed [${r.is503 ? '503' : 'Error'}]: ${r.error || '503 High Demand'} (${latency}s)`)
        }
      } else {
        setProbeResult(`❌ Error: ${json.error || 'Failed'}`)
      }
      void mutate()
    } catch (err: unknown) {
      setProbeResult(`❌ Probe failed: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      setProbingModel(null)
    }
  }

  // Key slot list: 1 to 16
  const allSlots = Array.from({ length: 16 }, (_, i) => i + 1)

  return (
    <section aria-label="Chunk Scan Orchestrator Matrix" className="panel relative overflow-hidden border-primary/30">
      {/* Header bar */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 pb-3">
        <div className="flex items-center gap-2.5">
          <div className="flex size-8 items-center justify-center rounded-lg bg-primary/20 text-primary">
            <Zap className="size-4 animate-pulse" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold tracking-tight">Chunk Scan Orchestrator Matrix</h2>
              <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-[10px] font-semibold text-emerald-400">
                1-Chunk-Per-Key
              </span>
            </div>
            <p className="text-xs text-muted-foreground">
              3.5 Sentinel (1-min loop) • Primary 3.6 (Burst 10) • Secondary 3.7 (Burst 4) • Tertiary 3.8 (Burst 4) • Instant &lt;3s Dispatch
            </p>
          </div>
        </div>

        {/* Tab switchers & refresh */}
        <div className="flex items-center gap-2">
          <div className="flex rounded-lg border border-border/80 bg-background/80 p-0.5 text-xs font-medium">
            <button
              type="button"
              onClick={() => setActiveTab('matrix')}
              className={`rounded-md px-2.5 py-1 transition-all ${activeTab === 'matrix' ? 'bg-primary text-primary-foreground font-semibold shadow-xs' : 'text-muted-foreground hover:text-foreground'}`}
            >
              Key Matrix (1–16)
            </button>
            <button
              type="button"
              onClick={() => setActiveTab('models')}
              className={`rounded-md px-2.5 py-1 transition-all ${activeTab === 'models' ? 'bg-primary text-primary-foreground font-semibold shadow-xs' : 'text-muted-foreground hover:text-foreground'}`}
            >
              Model Tiers
            </button>
            <button
              type="button"
              onClick={() => setActiveTab('logs')}
              className={`rounded-md px-2.5 py-1 transition-all ${activeTab === 'logs' ? 'bg-primary text-primary-foreground font-semibold shadow-xs' : 'text-muted-foreground hover:text-foreground'}`}
            >
              Orchestrator Logs
            </button>
          </div>

          <button
            type="button"
            onClick={() => void mutate()}
            disabled={isValidating}
            className="flex size-8 items-center justify-center rounded-lg border border-input bg-card text-muted-foreground hover:text-foreground"
            title="Refresh Orchestrator Matrix"
          >
            <RefreshCw className={`size-3.5 ${isValidating ? 'animate-spin text-primary' : ''}`} />
          </button>
        </div>
      </div>

      {/* Real-time Summary Telemetry Bar */}
      <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4 md:grid-cols-6 text-xs">
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Phase</span>
          <p className="mt-0.5 font-bold capitalize text-primary">
            {state?.phase ? state.phase.replace(/_/g, ' ') : 'Idle / Standby'}
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Active Model</span>
          <p className="mt-0.5 font-mono font-bold text-foreground truncate">
            {state?.activeModel || 'gemini-3.6-flash'}
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Queue (Pending / Done)</span>
          <p className="mt-0.5 font-mono font-bold text-emerald-400">
            {state?.queue?.pending ?? 0} pend / {state?.queue?.done ?? 0} done
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Retrying / Parked</span>
          <p className="mt-0.5 font-mono font-bold text-amber-400">
            {state?.queue?.retrying ?? 0} retry / {state?.queue?.failed ?? 0} parked
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Canary (3.5)</span>
          <p className="mt-0.5 font-bold">
            {state?.canaryActive ? (
              <span className="text-amber-400 animate-pulse">Polling (Recovery Watch)</span>
            ) : (
              <span className="text-muted-foreground">Standby (Ready)</span>
            )}
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-card/60 p-2">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Worker Capacity</span>
          <p className="mt-0.5 font-mono font-bold text-foreground">
            14 Active Slots
          </p>
        </div>
      </div>

      {/* Choke Probe Quick Action Bar */}
      <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border/60 bg-secondary/30 p-2.5 text-xs">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="font-semibold text-muted-foreground mr-1">Choke Test:</span>
          <button
            type="button"
            disabled={Boolean(probingModel)}
            onClick={() => void triggerProbe('gemini-3.6-flash')}
            className="rounded-md border border-primary/40 bg-primary/10 px-2.5 py-1 text-xs font-semibold text-primary hover:bg-primary/20 transition-all"
          >
            {probingModel === 'gemini-3.6-flash' ? 'Probing 3.6...' : 'Choke 3.6 (Primary)'}
          </button>
          <button
            type="button"
            disabled={Boolean(probingModel)}
            onClick={() => void triggerProbe('gemini-3.7-flash')}
            className="rounded-md border border-amber-500/40 bg-amber-500/10 px-2.5 py-1 text-xs font-semibold text-amber-300 hover:bg-amber-500/20 transition-all"
          >
            {probingModel === 'gemini-3.7-flash' ? 'Probing 3.7...' : 'Choke 3.7 (Secondary)'}
          </button>
          <button
            type="button"
            disabled={Boolean(probingModel)}
            onClick={() => void triggerProbe('gemini-3.8-flash')}
            className="rounded-md border border-orange-500/40 bg-orange-500/10 px-2.5 py-1 text-xs font-semibold text-orange-300 hover:bg-orange-500/20 transition-all"
          >
            {probingModel === 'gemini-3.8-flash' ? 'Probing 3.8...' : 'Choke 3.8 (Tertiary)'}
          </button>
          <button
            type="button"
            disabled={Boolean(probingModel)}
            onClick={() => void triggerProbe('gemini-3.5-flash')}
            className="rounded-md border border-sky-500/40 bg-sky-500/10 px-2.5 py-1 text-xs font-semibold text-sky-300 hover:bg-sky-500/20 transition-all"
          >
            {probingModel === 'gemini-3.5-flash' ? 'Probing 3.5...' : 'Probe 3.5 Canary'}
          </button>
        </div>

        {probeResult && (
          <span className="font-mono text-xs font-medium text-foreground bg-card/80 px-2.5 py-0.5 rounded-md border border-border">
            {probeResult}
          </span>
        )}
      </div>

      {/* Main Tab Content */}
      <div className="mt-3">
        {activeTab === 'matrix' && (
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 md:grid-cols-8">
            {allSlots.map((slot) => {
              const isKey1 = slot === 1
              const isSlot14 = slot === 14
              const slotData = state?.slots?.[slot]
              const isProcessing = slotData?.status === 'processing'
              const chunkIdx = slotData?.activeChunk

              if (isKey1) {
                return (
                  <div
                    key={slot}
                    className="flex flex-col justify-between rounded-lg border border-purple-500/40 bg-purple-950/20 p-2.5 shadow-xs"
                  >
                    <div>
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-xs font-bold text-purple-300">Key 1</span>
                        <KeyRound className="size-3 text-purple-400" />
                      </div>
                      <span className="mt-1.5 inline-block rounded-md bg-purple-500/20 px-1.5 py-0.5 text-[9px] font-semibold text-purple-200">
                        OWNER RESERVED
                      </span>
                    </div>
                    <p className="mt-3 text-[10px] text-purple-300/70">Manual chat only. Chunk workers locked out.</p>
                  </div>
                )
              }

              if (isSlot14) {
                return (
                  <div
                    key={slot}
                    className="flex flex-col justify-between rounded-lg border border-dashed border-border/40 bg-muted/20 p-2.5 opacity-60"
                  >
                    <div>
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-xs font-bold text-muted-foreground">Slot 14</span>
                        <ShieldAlert className="size-3 text-muted-foreground" />
                      </div>
                      <span className="mt-1.5 inline-block rounded-md bg-muted px-1.5 py-0.5 text-[9px] font-semibold text-muted-foreground">
                        SKIPPED
                      </span>
                    </div>
                    <p className="mt-3 text-[10px] text-muted-foreground">Slot does not exist. Permanently skipped.</p>
                  </div>
                )
              }

              // Normal worker key slots: 2..13, 15, 16
              const statusCls = isProcessing
                ? 'border-emerald-500/60 bg-emerald-950/30 ring-1 ring-emerald-500/30'
                : 'border-border/70 bg-card/50'

              return (
                <div
                  key={slot}
                  className={`flex flex-col justify-between rounded-lg border p-2.5 transition-all hover:border-primary/40 ${statusCls}`}
                >
                  <div>
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-xs font-bold text-foreground">Key {slot}</span>
                      {isProcessing ? (
                        <span className="flex size-2 rounded-full bg-emerald-400 animate-ping" />
                      ) : (
                        <span className="size-2 rounded-full bg-muted-foreground/40" />
                      )}
                    </div>

                    <div className="mt-1.5">
                      {isProcessing ? (
                        <div className="rounded-md bg-emerald-500/20 px-1.5 py-0.5 text-[10px] font-mono font-bold text-emerald-300">
                          Chunk #{chunkIdx}
                        </div>
                      ) : (
                        <span className="text-[10px] text-muted-foreground font-mono">Idle (Ready)</span>
                      )}
                    </div>
                  </div>

                  <div className="mt-3 border-t border-border/40 pt-1.5 text-[10px]">
                    <div className="flex items-center justify-between text-muted-foreground font-mono">
                      <span>503: {slotData?.errors503 ?? 0}</span>
                      <span>429: {slotData?.errors429 ?? 0}</span>
                      <span>T/O: {slotData?.timeouts ?? 0}</span>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        )}

        {activeTab === 'models' && (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            {/* Primary Model */}
            <div className="rounded-lg border border-emerald-500/40 bg-emerald-950/20 p-3.5">
              <div className="flex items-center justify-between">
                <span className="font-mono text-xs font-bold text-emerald-300">gemini-3.6-flash</span>
                <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-[10px] font-bold text-emerald-300">
                  PRIMARY (Burst 10)
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">Most stable worker • Continuous pipeline • 20 RPD</p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Max Parallelism</span>
                  <p className="font-mono font-bold text-foreground">10 parallel</p>
                </div>
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Window Post-Choke</span>
                  <p className="font-mono font-bold text-emerald-400">45+ seconds</p>
                </div>
              </div>
            </div>

            {/* Secondary Model */}
            <div className="rounded-lg border border-amber-500/40 bg-amber-950/20 p-3.5">
              <div className="flex items-center justify-between">
                <span className="font-mono text-xs font-bold text-amber-300">gemini-3.7-flash</span>
                <span className="rounded-full bg-amber-500/20 px-2 py-0.5 text-[10px] font-bold text-amber-300">
                  SECONDARY (Burst 4)
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">Immediate 503 failover • 20 RPD</p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Max Parallelism</span>
                  <p className="font-mono font-bold text-foreground">4 parallel</p>
                </div>
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Window Post-Choke</span>
                  <p className="font-mono font-bold text-amber-400">11s–60s (Flickery)</p>
                </div>
              </div>
            </div>

            {/* Tertiary Model */}
            <div className="rounded-lg border border-orange-500/40 bg-orange-950/20 p-3.5">
              <div className="flex items-center justify-between">
                <span className="font-mono text-xs font-bold text-orange-300">gemini-3.8-flash</span>
                <span className="rounded-full bg-orange-500/20 px-2 py-0.5 text-[10px] font-bold text-orange-300">
                  TERTIARY (Burst 4)
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">Parallel with 3.7 or last resort • 20 RPD</p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Max Parallelism</span>
                  <p className="font-mono font-bold text-foreground">4 parallel</p>
                </div>
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Spikes With 3.7</span>
                  <p className="font-mono font-bold text-orange-400">70% correlation</p>
                </div>
              </div>
            </div>

            {/* Canary Probe Model */}
            <div className="rounded-lg border border-sky-500/40 bg-sky-950/20 p-3.5">
              <div className="flex items-center justify-between">
                <span className="font-mono text-xs font-bold text-sky-300">gemini-3.5-flash</span>
                <span className="rounded-full bg-sky-500/20 px-2 py-0.5 text-[10px] font-bold text-sky-300">
                  CANARY ONLY
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">Never scans chunks • 1 probe/min in recovery watch • 500 RPD</p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Primary Correlation</span>
                  <p className="font-mono font-bold text-sky-400">88% agreement</p>
                </div>
                <div className="rounded bg-background/60 p-2">
                  <span className="text-[10px] text-muted-foreground">Lead Time</span>
                  <p className="font-mono font-bold text-sky-400">Recovers 3–4m early</p>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'logs' && (
          <div className="max-h-60 overflow-y-auto rounded-lg border border-border/80 bg-background/90 p-2.5 font-mono text-xs">
            {logs.length === 0 ? (
              <p className="text-muted-foreground italic">No orchestrator events recorded yet.</p>
            ) : (
              logs.map((evt, idx) => (
                <div key={idx} className="flex items-start gap-2 py-0.5 border-b border-border/20 last:border-0">
                  <span className="shrink-0 text-muted-foreground">
                    {new Date(evt.timestamp).toLocaleTimeString()}
                  </span>
                  <span
                    className={`shrink-0 rounded px-1 text-[9px] font-bold uppercase ${
                      evt.type === 'failover'
                        ? 'bg-amber-500/20 text-amber-300'
                        : evt.type === 'recovery'
                          ? 'bg-sky-500/20 text-sky-300'
                          : evt.type === 'choke'
                            ? 'bg-primary/20 text-primary'
                            : 'bg-muted text-muted-foreground'
                    }`}
                  >
                    {evt.type}
                  </span>
                  <span className="text-foreground">{evt.message}</span>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </section>
  )
}
