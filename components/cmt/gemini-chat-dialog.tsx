'use client'

import { useState, useRef, useEffect } from 'react'
import useSWR from 'swr'
import Markdown from 'react-markdown'
import {
  Sparkles,
  X,
  Send,
  RotateCcw,
  Video,
  Settings2,
  AlertTriangle,
  Copy,
  Check,
  Loader2,
  FileVideo,
  KeyRound,
  Cpu,
  ChevronDown,
  RefreshCw,
  Plus,
  Clock,
  Columns,
  Maximize2,
  Minimize2,
} from 'lucide-react'
import { fetcher } from '@/lib/format'

interface KeySlotInfo {
  slot: number
  label: string
}

interface KeysResponse {
  slots: KeySlotInfo[]
  hasEnvKey: boolean
  hasAnyKey: boolean
}

export interface ChatMessage {
  id: string
  role: 'user' | 'model'
  content: string
  timestamp: number
  sentAt?: number
  respondedAt?: number
  durationMs?: number
  videoInfo?: {
    name: string
    sizeStr: string
    fps: number
    timelapse: number
  }
  isError?: boolean
  errorDetail?: string
  is503?: boolean
  modelUsed?: string
  keyUsed?: string
}

export interface ChatPaneState {
  id: string
  title: string
  selectedModel: string
  keySelection: string // 'auto' | slot number as string e.g. "1", "2"
  messages: ChatMessage[]
  inputPrompt: string
  loading: boolean
  elapsedSec: number
  currentSentAt: number | null
  attachedVideo: File | null
  fps: number
  timelapse: number
  showVideoSettings: boolean
}

const AVAILABLE_MODELS = [
  { id: 'gemini-3.7-flash', name: 'Gemini 3.7 Flash', tag: '3.7-shiva · Fast Forensic' },
  { id: 'gemini-3.8-flash', name: 'Gemini 3.8 Flash', tag: '3.8-shiva · Deep Forensic (Recommended)' },
  { id: 'gemini-3.6-flash', name: 'Gemini 3.6 Flash', tag: '3.6-shiva · Robust Baseline' },
  { id: 'gemini-3.5-flash', name: 'Gemini 3.5 Flash', tag: '3.5-shiva · High Quota' },
  { id: 'gemini-3.5-flash-lite', name: 'Gemini 3.5 Flash Lite', tag: '3.5-shiva-lite · 500 RPD' },
  { id: 'gemini-3.1-pro-preview', name: 'Gemini 3.1 Pro', tag: 'Deep Reasoning' },
  { id: 'gemini-3.1-flash-lite', name: 'Gemini 3.1 Flash Lite', tag: '3.1-shiva-lite · High Speed' },
  { id: 'gemini-2.5-flash', name: 'Gemini 2.5 Flash', tag: 'Multimodal' },
  { id: 'gemini-2.5-pro', name: 'Gemini 2.5 Pro', tag: 'Deep Multimodal' },
]

const FPS_PRESETS = [24, 1, 2, 5, 0.5]
const TIMELAPSE_PRESETS = [1, 2, 5, 10, 20, 30]

function formatClockTime(timestamp?: number): string {
  if (!timestamp) return '--:--:--'
  try {
    const d = new Date(timestamp)
    return d.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return '--:--:--'
  }
}

function formatDuration(ms?: number): string {
  if (ms === undefined || ms === null || isNaN(ms)) return '0s'
  if (ms < 1000) return `${ms}ms`
  const totalSec = (ms / 1000).toFixed(1)
  if (ms < 60_000) return `${totalSec}s`
  const minutes = Math.floor(ms / 60_000)
  const remSec = Math.round((ms % 60_000) / 1000)
  return `${minutes}m ${remSec}s (${totalSec}s)`
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

interface GeminiChatDialogProps {
  open: boolean
  onClose: () => void
}

export function GeminiChatDialog({ open, onClose }: GeminiChatDialogProps) {
  const { data: keysData, mutate: refreshKeys } = useSWR<KeysResponse>('/api/chat/keys', fetcher)

  // Multi-pane state: start with 2 side-by-side comparison panes by default
  const [panes, setPanes] = useState<ChatPaneState[]>([
    {
      id: 'pane-1',
      title: 'Chat 1 (3.7-Flash)',
      selectedModel: 'gemini-3.7-flash',
      keySelection: 'auto',
      messages: [],
      inputPrompt: '',
      loading: false,
      elapsedSec: 0,
      currentSentAt: null,
      attachedVideo: null,
      fps: 24,
      timelapse: 1,
      showVideoSettings: false,
    },
    {
      id: 'pane-2',
      title: 'Chat 2 (3.8-Flash)',
      selectedModel: 'gemini-3.8-flash',
      keySelection: '2',
      messages: [],
      inputPrompt: '',
      loading: false,
      elapsedSec: 0,
      currentSentAt: null,
      attachedVideo: null,
      fps: 24,
      timelapse: 1,
      showVideoSettings: false,
    },
  ])

  // UI state
  const [copiedId, setCopiedId] = useState<string | null>(null)
  const [syncingKeys, setSyncingKeys] = useState<boolean>(false)
  const [syncMsg, setSyncMsg] = useState<string | null>(null)
  const [isFullscreen, setIsFullscreen] = useState<boolean>(true)
  const [broadcastPrompt, setBroadcastPrompt] = useState<string>('')
  const [broadcastSending, setBroadcastSending] = useState<boolean>(false)

  // Last sent request refs per pane
  const lastRequestRefs = useRef<Record<string, { prompt: string; video: File | null; fps: number; timelapse: number }>>({})
  const timerRefs = useRef<Record<string, NodeJS.Timeout>>({})

  // Update timers per loading pane
  useEffect(() => {
    panes.forEach((pane) => {
      if (pane.loading && !timerRefs.current[pane.id]) {
        timerRefs.current[pane.id] = setInterval(() => {
          setPanes((prevPanes) =>
            prevPanes.map((p) => (p.id === pane.id ? { ...p, elapsedSec: p.elapsedSec + 1 } : p)),
          )
        }, 1000)
      } else if (!pane.loading && timerRefs.current[pane.id]) {
        clearInterval(timerRefs.current[pane.id])
        delete timerRefs.current[pane.id]
      }
    })

    return () => {
      // cleanup all on unmount
    }
  }, [panes])

  // Refresh available keys when opened
  useEffect(() => {
    if (open) {
      void refreshKeys()
    }
  }, [open, refreshKeys])

  if (!open) return null

  async function handleSyncKeys() {
    setSyncingKeys(true)
    setSyncMsg(null)
    try {
      const res = await fetch('/api/chat/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'resync' }),
      })
      if (res.ok) {
        await refreshKeys()
        setSyncMsg('All API Keys synced from Settings! Ready for side-by-side test.')
        setTimeout(() => setSyncMsg(null), 3500)
      }
    } catch {
      // ignore
    } finally {
      setSyncingKeys(false)
    }
  }

  function addParallelPane() {
    const nextIdx = panes.length + 1
    const modelOptions = ['gemini-3.6-flash', 'gemini-3.5-flash', 'gemini-3.7-flash', 'gemini-3.8-flash']
    const nextModel = modelOptions[(nextIdx - 1) % modelOptions.length]
    const slotString = String(Math.min(nextIdx, (keysData?.slots?.length || 1)))
    const newPane: ChatPaneState = {
      id: `pane-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
      title: `Chat ${nextIdx} (${nextModel.replace('gemini-', '')})`,
      selectedModel: nextModel,
      keySelection: slotString,
      messages: [],
      inputPrompt: '',
      loading: false,
      elapsedSec: 0,
      currentSentAt: null,
      attachedVideo: null,
      fps: 24,
      timelapse: 1,
      showVideoSettings: false,
    }
    setPanes((prev) => [...prev, newPane])
  }

  function removePane(paneId: string) {
    if (panes.length <= 1) return
    if (timerRefs.current[paneId]) {
      clearInterval(timerRefs.current[paneId])
      delete timerRefs.current[paneId]
    }
    setPanes((prev) => prev.filter((p) => p.id !== paneId))
  }

  function updatePane(paneId: string, updates: Partial<ChatPaneState>) {
    setPanes((prev) => prev.map((p) => (p.id === paneId ? { ...p, ...updates } : p)))
  }

  function clearPaneMessages(paneId: string) {
    updatePane(paneId, { messages: [], inputPrompt: '', attachedVideo: null, loading: false })
  }

  async function handleCopy(id: string, text: string) {
    try {
      await navigator.clipboard.writeText(text)
      setCopiedId(id)
      setTimeout(() => setCopiedId(null), 2000)
    } catch {
      // ignore
    }
  }

  // Send request for a specific pane
  async function sendPaneMessage(paneId: string, customPrompt?: string, customVideo?: File | null) {
    const pane = panes.find((p) => p.id === paneId)
    if (!pane) return

    const promptToSend = customPrompt !== undefined ? customPrompt : pane.inputPrompt.trim()
    const videoToSend = customVideo !== undefined ? customVideo : pane.attachedVideo

    if (!promptToSend && !videoToSend) return
    if (pane.loading) return

    const sentAt = Date.now()
    const userMessageId = `user-${paneId}-${sentAt}`
    const newUserMsg: ChatMessage = {
      id: userMessageId,
      role: 'user',
      content: promptToSend || (videoToSend ? 'Analyze this video clip.' : ''),
      timestamp: sentAt,
      sentAt,
      videoInfo: videoToSend
        ? {
            name: videoToSend.name,
            sizeStr: formatFileSize(videoToSend.size),
            fps: pane.fps,
            timelapse: pane.timelapse,
          }
        : undefined,
    }

    lastRequestRefs.current[paneId] = {
      prompt: promptToSend,
      video: videoToSend,
      fps: pane.fps,
      timelapse: pane.timelapse,
    }

    // Set loading state for this pane
    updatePane(paneId, {
      messages: [...pane.messages, newUserMsg],
      inputPrompt: '',
      attachedVideo: null,
      loading: true,
      elapsedSec: 0,
      currentSentAt: sentAt,
    })

    // Build history for backend
    const historyPayload = pane.messages
      .filter((m) => !m.isError)
      .map((m) => ({
        role: m.role,
        content: m.content,
      }))

    try {
      let res: Response
      if (videoToSend) {
        const formData = new FormData()
        formData.append('prompt', promptToSend)
        formData.append('model', pane.selectedModel)
        formData.append('keySelection', pane.keySelection === 'auto' ? 'auto' : 'slot')
        if (pane.keySelection !== 'auto') {
          formData.append('keySlot', pane.keySelection)
        }
        formData.append('fps', String(pane.fps))
        formData.append('timelapse', String(pane.timelapse))
        formData.append('history', JSON.stringify(historyPayload))
        formData.append('video', videoToSend)

        res = await fetch('/api/chat', {
          method: 'POST',
          body: formData,
        })
      } else {
        res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            prompt: promptToSend,
            model: pane.selectedModel,
            keySelection: pane.keySelection === 'auto' ? 'auto' : 'slot',
            keySlot: pane.keySelection !== 'auto' ? Number(pane.keySelection) : null,
            fps: pane.fps,
            timelapse: pane.timelapse,
            history: historyPayload,
          }),
        })
      }

      const respondedAt = Date.now()
      const data = await res.json().catch(() => ({}))
      const durationMs = respondedAt - sentAt

      if (!res.ok || data.error) {
        const errorMsg = data.error || 'Gemini request failed'
        const is503 = Boolean(data.is503) || errorMsg.includes('503') || errorMsg.includes('high demand')
        const botErrorMsg: ChatMessage = {
          id: `bot-err-${paneId}-${respondedAt}`,
          role: 'model',
          content: '',
          timestamp: respondedAt,
          sentAt,
          respondedAt,
          durationMs,
          isError: true,
          is503,
          errorDetail: errorMsg,
          modelUsed: pane.selectedModel,
          keyUsed: data.keyUsed || (pane.keySelection === 'auto' ? 'Key 1' : `Key ${pane.keySelection}`),
        }
        setPanes((prev) =>
          prev.map((p) => (p.id === paneId ? { ...p, messages: [...p.messages, botErrorMsg], loading: false } : p)),
        )
      } else {
        const botMsg: ChatMessage = {
          id: `bot-${paneId}-${respondedAt}`,
          role: 'model',
          content: data.reply || 'No text output returned.',
          timestamp: respondedAt,
          sentAt,
          respondedAt,
          durationMs,
          modelUsed: data.model || pane.selectedModel,
          keyUsed: data.keyUsed || (pane.keySelection === 'auto' ? 'Key 1' : `Key ${pane.keySelection}`),
        }
        setPanes((prev) =>
          prev.map((p) => (p.id === paneId ? { ...p, messages: [...p.messages, botMsg], loading: false } : p)),
        )
      }
    } catch (err: unknown) {
      const respondedAt = Date.now()
      const durationMs = respondedAt - sentAt
      const errorMsg = err instanceof Error ? err.message : String(err)
      const is503 = errorMsg.includes('503') || errorMsg.includes('high demand')
      const botErrorMsg: ChatMessage = {
        id: `bot-err-${paneId}-${respondedAt}`,
        role: 'model',
        content: '',
        timestamp: respondedAt,
        sentAt,
        respondedAt,
        durationMs,
        isError: true,
        is503,
        errorDetail: errorMsg,
        modelUsed: pane.selectedModel,
        keyUsed: pane.keySelection === 'auto' ? 'Key 1' : `Key ${pane.keySelection}`,
      }
      setPanes((prev) =>
        prev.map((p) => (p.id === paneId ? { ...p, messages: [...p.messages, botErrorMsg], loading: false } : p)),
      )
    } finally {
      updatePane(paneId, { loading: false })
    }
  }

  // Send prompt to all open panes simultaneously
  async function handleBroadcastSend() {
    const text = broadcastPrompt.trim()
    if (!text || broadcastSending) return
    setBroadcastSending(true)
    try {
      const promises = panes.map((pane) => sendPaneMessage(pane.id, text))
      setBroadcastPrompt('')
      await Promise.all(promises)
    } finally {
      setBroadcastSending(false)
    }
  }

  // Grid columns based on pane count
  const gridClasses =
    panes.length === 1
      ? 'grid-cols-1'
      : panes.length === 2
        ? 'grid-cols-1 lg:grid-cols-2'
        : panes.length === 3
          ? 'grid-cols-1 lg:grid-cols-3'
          : 'grid-cols-1 md:grid-cols-2 xl:grid-cols-4'

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Gemini AI Studio Chat"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-1 sm:p-2 backdrop-blur-md animate-in fade-in duration-200"
    >
      <div
        className={`flex flex-col overflow-hidden rounded-2xl border border-zinc-800 bg-zinc-950 shadow-2xl transition-all ${
          isFullscreen ? 'h-[98vh] w-[99vw] max-w-[1900px]' : 'h-[92vh] w-full max-w-6xl'
        }`}
      >
        {/* TOP HEADER */}
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-zinc-800 bg-zinc-900/95 px-4 py-2.5 backdrop-blur">
          <div className="flex items-center gap-2.5">
            <span className="flex size-8 shrink-0 items-center justify-center rounded-xl bg-gradient-to-tr from-amber-500 via-orange-500 to-amber-300 text-zinc-950 shadow-md shadow-amber-500/20">
              <Sparkles className="size-4" />
            </span>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-sm font-semibold tracking-tight text-zinc-100">
                  Gemini <span className="text-amber-400">AI Studio Chat</span>
                </h2>
                <span className="flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-semibold text-amber-300">
                  <Columns className="size-3" />
                  Side-by-Side Parallel Test ({panes.length} active)
                </span>
                <span className="hidden sm:inline-flex rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-300">
                  Exact Time Lapse & 503 Quota Sync
                </span>
              </div>
              <p className="hidden md:block text-[11px] text-zinc-400">
                All parallel chats stay open simultaneously side-by-side · Compare response latency, speed, and accuracy in real time
              </p>
            </div>
          </div>

          {/* Top Global Controls */}
          <div className="flex flex-wrap items-center gap-2">
            {/* Add Parallel Pane Button */}
            <button
              type="button"
              onClick={addParallelPane}
              title="Add another side-by-side parallel chat to compare multiple models & keys"
              className="flex items-center gap-1.5 rounded-lg border border-amber-500/40 bg-amber-500/15 px-3 py-1.5 text-xs font-semibold text-amber-300 hover:bg-amber-500/25 transition-colors shadow-sm"
            >
              <Plus className="size-3.5" />
              <span>+ Add Side-by-Side Chat</span>
            </button>

            {/* Sync from Settings Button */}
            <button
              type="button"
              onClick={handleSyncKeys}
              disabled={syncingKeys}
              title="Copy / Sync all keys from Settings into Chat pool"
              className="flex items-center gap-1 rounded-lg border border-zinc-800 bg-zinc-900 px-2.5 py-1.5 text-xs font-medium text-zinc-300 hover:border-amber-500/40 hover:bg-zinc-800 hover:text-amber-300 transition-colors"
            >
              <RefreshCw className={`size-3 text-amber-400 ${syncingKeys ? 'animate-spin' : ''}`} />
              <span className="hidden lg:inline">Sync Keys</span>
            </button>

            {/* Fullscreen Toggle */}
            <button
              type="button"
              onClick={() => setIsFullscreen(!isFullscreen)}
              title={isFullscreen ? 'Restore standard size' : 'Expand full width'}
              className="rounded-lg border border-zinc-800 bg-zinc-900 p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
            >
              {isFullscreen ? <Minimize2 className="size-4" /> : <Maximize2 className="size-4" />}
            </button>

            {/* Close Button */}
            <button
              type="button"
              onClick={onClose}
              aria-label="Close Chat"
              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
            >
              <X className="size-5" />
            </button>
          </div>
        </div>

        {/* Sync message banner */}
        {syncMsg && (
          <div className="flex items-center gap-2 border-b border-emerald-900/40 bg-emerald-950/40 px-4 py-1.5 text-xs text-emerald-200 animate-in fade-in">
            <Check className="size-4 text-emerald-400 shrink-0" />
            <span>{syncMsg}</span>
          </div>
        )}

        {/* BROADCAST BAR: Send 1 prompt to ALL parallel panes simultaneously */}
        {panes.length > 1 && (
          <div className="flex items-center gap-2 border-b border-zinc-800 bg-zinc-900/60 px-4 py-2">
            <span className="flex items-center gap-1 text-[11px] font-semibold text-amber-400 shrink-0">
              <Sparkles className="size-3" />
              ⚡ Send to All {panes.length} Panes:
            </span>
            <input
              type="text"
              value={broadcastPrompt}
              onChange={(e) => setBroadcastPrompt(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault()
                  void handleBroadcastSend()
                }
              }}
              placeholder={`Type a prompt here to test all ${panes.length} models/keys side-by-side simultaneously... (Press Enter)`}
              className="flex-1 rounded-lg border border-zinc-800 bg-zinc-950 px-3 py-1 text-xs text-zinc-200 placeholder:text-zinc-600 focus:border-amber-500 focus:outline-none"
            />
            <button
              type="button"
              onClick={handleBroadcastSend}
              disabled={broadcastSending || !broadcastPrompt.trim()}
              className="flex items-center gap-1 rounded-lg bg-amber-500 px-3 py-1 text-xs font-bold text-zinc-950 hover:bg-amber-400 disabled:opacity-40 transition-colors shrink-0 shadow"
            >
              {broadcastSending ? <Loader2 className="size-3.5 animate-spin" /> : <Send className="size-3.5" />}
              <span>Send to All</span>
            </button>
          </div>
        )}

        {/* SIDE-BY-SIDE PANES CONTAINER */}
        <div className={`grid flex-1 overflow-hidden p-2 gap-2 sm:gap-3 ${gridClasses}`}>
          {panes.map((pane, paneIndex) => (
            <ChatPaneComponent
              key={pane.id}
              pane={pane}
              paneIndex={paneIndex}
              totalPanes={panes.length}
              keysData={keysData}
              copiedId={copiedId}
              onUpdate={(updates) => updatePane(pane.id, updates)}
              onRemove={() => removePane(pane.id)}
              onClear={() => clearPaneMessages(pane.id)}
              onSend={(prompt, video) => sendPaneMessage(pane.id, prompt, video)}
              onCopy={handleCopy}
            />
          ))}
        </div>
      </div>
    </div>
  )
}

interface ChatPaneComponentProps {
  pane: ChatPaneState
  paneIndex: number
  totalPanes: number
  keysData?: KeysResponse
  copiedId: string | null
  onUpdate: (updates: Partial<ChatPaneState>) => void
  onRemove: () => void
  onClear: () => void
  onSend: (prompt?: string, video?: File | null) => void
  onCopy: (id: string, text: string) => void
}

function ChatPaneComponent({
  pane,
  paneIndex,
  totalPanes,
  keysData,
  copiedId,
  onUpdate,
  onRemove,
  onClear,
  onSend,
  onCopy,
}: ChatPaneComponentProps) {
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Auto-scroll when messages update
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [pane.messages, pane.loading])

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (file) {
      onUpdate({ attachedVideo: file, showVideoSettings: true })
    }
  }

  function removeAttachedVideo() {
    onUpdate({ attachedVideo: null })
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  const modelInfo = AVAILABLE_MODELS.find((m) => m.id === pane.selectedModel)

  return (
    <div className="flex h-full flex-col overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/50 shadow-md">
      {/* PANE HEADER */}
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-800 bg-zinc-900/90 px-3 py-2">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="flex size-5 shrink-0 items-center justify-center rounded-md bg-amber-500/20 text-amber-400 font-mono text-[11px] font-bold">
            {paneIndex + 1}
          </span>
          <span className="font-semibold text-xs text-zinc-200 truncate">
            {pane.title || `Chat ${paneIndex + 1}`}
          </span>
        </div>

        {/* Model and Key Controls (Independent per Pane) */}
        <div className="flex items-center gap-1.5 flex-wrap">
          {/* Model Selector */}
          <div className="flex items-center gap-1 rounded-md border border-zinc-800 bg-zinc-950 px-2 py-1 shadow-inner">
            <Cpu className="size-3 text-zinc-400 shrink-0" />
            <select
              value={pane.selectedModel}
              onChange={(e) => onUpdate({ selectedModel: e.target.value })}
              disabled={pane.loading}
              aria-label="Choose Model"
              className="bg-transparent text-[11px] font-medium text-zinc-200 focus:outline-none cursor-pointer max-w-[140px] truncate"
            >
              {AVAILABLE_MODELS.map((m) => (
                <option key={m.id} value={m.id} className="bg-zinc-900 text-zinc-200">
                  {m.name}
                </option>
              ))}
            </select>
          </div>

          {/* Key Selector — Strictly shows Key 1, Key 2, etc. NO raw key string ever! */}
          <div className="flex items-center gap-1 rounded-md border border-zinc-800 bg-zinc-950 px-2 py-1 shadow-inner">
            <KeyRound className="size-3 text-amber-400 shrink-0" />
            <select
              value={pane.keySelection}
              onChange={(e) => onUpdate({ keySelection: e.target.value })}
              disabled={pane.loading}
              aria-label="Choose API Key"
              className="bg-transparent text-[11px] font-medium text-zinc-200 focus:outline-none cursor-pointer"
            >
              <option value="auto" className="bg-zinc-900 text-zinc-200">
                Key 1 (Default)
              </option>
              {(keysData?.slots || []).map((s) => (
                <option key={s.slot} value={String(s.slot)} className="bg-zinc-900 text-zinc-200">
                  Key {s.slot}
                </option>
              ))}
            </select>
          </div>

          {/* Clear chat */}
          <button
            type="button"
            onClick={onClear}
            disabled={pane.loading || pane.messages.length === 0}
            title="Clear this chat"
            className="rounded p-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-200 disabled:opacity-30 transition-colors"
          >
            <RotateCcw className="size-3.5" />
          </button>

          {/* Remove Pane (if total > 1) */}
          {totalPanes > 1 && (
            <button
              type="button"
              onClick={onRemove}
              title="Close this side-by-side pane"
              className="rounded p-1 text-zinc-400 hover:bg-red-950/60 hover:text-red-400 transition-colors"
            >
              <X className="size-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* MESSAGES SCROLL AREA */}
      <div className="flex-1 overflow-y-auto p-3 space-y-3">
        {pane.messages.length === 0 && (
          <div className="flex h-full flex-col items-center justify-center text-center p-4">
            <div className="size-10 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-amber-400 mb-2">
              <Sparkles className="size-5" />
            </div>
            <h3 className="text-xs font-semibold text-zinc-200 mb-1">
              Ready on {modelInfo?.name || pane.selectedModel}
            </h3>
            <p className="text-[11px] text-zinc-500 max-w-xs mb-3">
              Key: {pane.keySelection === 'auto' ? 'Key 1' : `Key ${pane.keySelection}`} · Exact time lapse & duration will be recorded
            </p>
            <div className="flex flex-wrap gap-1.5 justify-center max-w-sm">
              {[
                'Hy, test connection & latency',
                'What is your model version?',
                'Explain how you analyze video timestamps',
              ].map((suggestion) => (
                <button
                  key={suggestion}
                  type="button"
                  onClick={() => onSend(suggestion)}
                  className="rounded-lg border border-zinc-800 bg-zinc-900/80 px-2 py-1 text-[11px] text-zinc-300 hover:border-amber-500/40 hover:bg-zinc-800 transition-colors"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Message stream */}
        {pane.messages.map((msg) => (
          <div
            key={msg.id}
            className={`flex flex-col ${msg.role === 'user' ? 'items-end' : 'items-start'}`}
          >
            {/* USER MESSAGE BUBBLE */}
            {msg.role === 'user' && (
              <div className="max-w-[90%] rounded-2xl rounded-tr-sm bg-amber-500/15 border border-amber-500/30 p-3 text-zinc-100 shadow-sm">
                <div className="flex items-center justify-between gap-2 mb-1 pb-1 border-b border-amber-500/20 text-[10px] font-mono text-amber-300/80">
                  <span className="font-semibold">User</span>
                  <span className="flex items-center gap-1">
                    <Clock className="size-2.5" />
                    Sent at {formatClockTime(msg.sentAt || msg.timestamp)}
                  </span>
                </div>

                {/* Video Info if attached */}
                {msg.videoInfo && (
                  <div className="mb-2 flex flex-wrap items-center gap-2 rounded-lg border border-amber-500/40 bg-zinc-950/70 p-2 text-xs">
                    <FileVideo className="size-3.5 text-amber-400 shrink-0" />
                    <span className="font-semibold text-zinc-200 truncate max-w-[150px]">
                      {msg.videoInfo.name}
                    </span>
                    <span className="text-[10px] text-zinc-400">({msg.videoInfo.sizeStr})</span>
                    <div className="ml-auto flex items-center gap-1">
                      <span className="rounded bg-amber-500/20 px-1 py-0.5 text-[9px] font-mono text-amber-300">
                        {msg.videoInfo.fps} FPS
                      </span>
                      <span className="rounded bg-zinc-800 px-1 py-0.5 text-[9px] font-mono text-zinc-300">
                        {msg.videoInfo.timelapse}x
                      </span>
                    </div>
                  </div>
                )}

                <p className="whitespace-pre-wrap text-xs sm:text-sm leading-relaxed">{msg.content}</p>
              </div>
            )}

            {/* MODEL MESSAGE BUBBLE */}
            {msg.role === 'model' && (
              <div className="w-full max-w-[95%]">
                {msg.isError ? (
                  /* ERROR CARD (e.g. 503 High Demand or 429) */
                  <div className="rounded-xl border border-red-500/40 bg-red-950/30 p-3 text-zinc-100 shadow">
                    <div className="flex items-center justify-between gap-2 text-red-400 mb-1.5 pb-1 border-b border-red-900/40">
                      <div className="flex items-center gap-1.5">
                        <AlertTriangle className="size-3.5 shrink-0" />
                        <span className="text-xs font-bold uppercase tracking-wider">
                          {msg.is503 ? '503 High Demand Spike' : 'Gemini Error'}
                        </span>
                      </div>
                      <span className="rounded bg-red-950 border border-red-800/60 px-1.5 py-0.5 text-[10px] font-mono text-red-300">
                        {msg.modelUsed || pane.selectedModel}
                      </span>
                    </div>

                    <p className="text-xs text-red-200 font-mono bg-black/50 p-2 rounded-lg border border-red-900/50 whitespace-pre-wrap mb-2">
                      {msg.errorDetail}
                    </p>

                    {/* TIME LAPSE & REQ COUNT DIAGNOSTIC BAR */}
                    <div className="flex flex-wrap items-center gap-1.5 text-[10px] font-mono text-red-300/90 bg-red-950/40 p-1.5 rounded-lg border border-red-900/30 mb-2">
                      <span className="flex items-center gap-1">
                        <Clock className="size-3 text-amber-400" />
                        Sent: <strong>{formatClockTime(msg.sentAt)}</strong>
                      </span>
                      <span>➔</span>
                      <span>
                        Replied/Failed: <strong>{formatClockTime(msg.respondedAt)}</strong>
                      </span>
                      <span className="rounded bg-amber-500/20 border border-amber-500/40 px-1.5 py-0.5 font-bold text-amber-300 ml-auto">
                        ⏱️ Time Lapse: {formatDuration(msg.durationMs)}
                      </span>
                    </div>

                    <div className="flex flex-wrap items-center gap-2 text-xs">
                      <button
                        type="button"
                        onClick={() => {
                          const last = lastRequestRefs.current[pane.id]
                          if (last) {
                            void onSend(last.prompt, last.video)
                          } else {
                            void onSend(pane.messages[pane.messages.length - 2]?.content)
                          }
                        }}
                        className="flex items-center gap-1 rounded bg-red-600 px-2.5 py-1 text-xs font-semibold text-white hover:bg-red-500 transition-colors shadow"
                      >
                        <RotateCcw className="size-3" /> Retry Request
                      </button>
                      <span className="text-[10px] text-zinc-400">
                        {msg.is503 ? 'Counted as 1 request on quota' : 'Logged'}
                      </span>
                    </div>
                  </div>
                ) : (
                  /* NORMAL GEMINI REPLY */
                  <div className="rounded-xl border border-zinc-800 bg-zinc-900/80 p-3 text-zinc-100 shadow-sm">
                    {/* Header */}
                    <div className="flex items-center justify-between border-b border-zinc-800 pb-1.5 mb-2">
                      <div className="flex items-center gap-1.5">
                        <span className="flex size-4 items-center justify-center rounded bg-amber-500/20 text-amber-400">
                          <Sparkles className="size-2.5" />
                        </span>
                        <span className="text-xs font-semibold text-zinc-300">Gemini</span>
                        {msg.modelUsed && (
                          <span className="rounded bg-zinc-800 px-1.5 py-0.5 text-[10px] font-mono text-zinc-400">
                            {msg.modelUsed}
                          </span>
                        )}
                        {msg.keyUsed && (
                          <span className="rounded bg-zinc-800/80 border border-zinc-700/50 px-1.5 py-0.5 text-[10px] font-mono text-amber-300">
                            {msg.keyUsed}
                          </span>
                        )}
                      </div>

                      <button
                        type="button"
                        onClick={() => onCopy(msg.id, msg.content)}
                        className="flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] text-zinc-400 hover:bg-zinc-800 hover:text-zinc-200 transition-colors"
                        title="Copy text"
                      >
                        {copiedId === msg.id ? (
                          <>
                            <Check className="size-3 text-emerald-400" />
                            <span className="text-emerald-400">Copied</span>
                          </>
                        ) : (
                          <>
                            <Copy className="size-3" />
                            <span>Copy</span>
                          </>
                        )}
                      </button>
                    </div>

                    {/* Markdown Body */}
                    <div className="prose prose-invert max-w-none text-xs text-zinc-200 leading-relaxed space-y-1.5 [&>ul]:list-disc [&>ul]:pl-4 [&>ol]:list-decimal [&>ol]:pl-4 [&>pre]:bg-zinc-950 [&>pre]:border [&>pre]:border-zinc-800 [&>pre]:p-2.5 [&>pre]:rounded-lg [&>code]:bg-zinc-800 [&>code]:px-1 [&>code]:py-0.5 [&>code]:rounded [&>code]:text-amber-300">
                      <Markdown>{msg.content}</Markdown>
                    </div>

                    {/* TIME LAPSE DIAGNOSTIC FOOTER */}
                    <div className="flex flex-wrap items-center gap-1.5 mt-2.5 pt-2 border-t border-zinc-800 text-[10px] font-mono text-zinc-400">
                      <span className="flex items-center gap-1 text-zinc-300">
                        <Clock className="size-2.5 text-amber-400" />
                        Sent: <strong>{formatClockTime(msg.sentAt)}</strong>
                      </span>
                      <span>➔</span>
                      <span className="flex items-center gap-1 text-zinc-300">
                        Reply: <strong>{formatClockTime(msg.respondedAt)}</strong>
                      </span>
                      <span className="rounded bg-amber-500/15 border border-amber-500/30 px-1.5 py-0.5 font-bold text-amber-300 ml-auto">
                        ⏱️ Time Lapse: {formatDuration(msg.durationMs)}
                      </span>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}

        {/* LOADING INDICATOR FOR THIS PANE */}
        {pane.loading && (
          <div className="flex items-center gap-2.5 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-zinc-200 shadow animate-pulse">
            <Loader2 className="size-4 animate-spin text-amber-400 shrink-0" />
            <div className="min-w-0">
              <p className="text-xs font-semibold text-amber-300">
                {pane.attachedVideo
                  ? `Analyzing video on ${pane.selectedModel} (${pane.fps} FPS)...`
                  : `Waiting for ${pane.selectedModel} response...`}
              </p>
              <div className="flex items-center gap-2 text-[10px] font-mono text-zinc-400 mt-0.5">
                <span>Sent at {formatClockTime(pane.currentSentAt || Date.now())}</span>
                <span>·</span>
                <span className="font-bold text-amber-400">⏱️ Elapsed: {pane.elapsedSec}s</span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* ATTACHED VIDEO CONFIGURATION DRAWER (if video attached in this pane) */}
      {pane.attachedVideo && (
        <div className="border-t border-zinc-800 bg-zinc-950/90 p-2.5 px-3">
          <div className="flex items-center justify-between gap-2 mb-1.5">
            <div className="flex items-center gap-1.5 min-w-0">
              <FileVideo className="size-3.5 text-amber-400 shrink-0" />
              <span className="text-[11px] font-semibold text-zinc-200 truncate max-w-[180px]">
                {pane.attachedVideo.name}
              </span>
              <span className="text-[10px] text-zinc-500">
                ({formatFileSize(pane.attachedVideo.size)})
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                type="button"
                onClick={() => onUpdate({ showVideoSettings: !pane.showVideoSettings })}
                className="flex items-center gap-1 text-[10px] text-amber-400 hover:text-amber-300"
              >
                <Settings2 className="size-3" />
                <span>FPS ({pane.fps})</span>
                <ChevronDown className={`size-2.5 ${pane.showVideoSettings ? 'rotate-180' : ''}`} />
              </button>
              <button
                type="button"
                onClick={removeAttachedVideo}
                className="rounded p-0.5 text-zinc-400 hover:text-red-400"
              >
                <X className="size-3.5" />
              </button>
            </div>
          </div>

          {pane.showVideoSettings && (
            <div className="rounded-lg border border-zinc-800 bg-zinc-900/90 p-2 text-[11px] space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-zinc-400">Sampling Rate:</span>
                <div className="flex items-center gap-1">
                  {FPS_PRESETS.map((p) => (
                    <button
                      key={p}
                      type="button"
                      onClick={() => onUpdate({ fps: p })}
                      className={`rounded px-1.5 py-0.5 text-[10px] font-mono ${
                        pane.fps === p ? 'bg-amber-500 font-bold text-zinc-950' : 'bg-zinc-800 text-zinc-300'
                      }`}
                    >
                      {p} fps
                    </button>
                  ))}
                </div>
              </div>
              <div className="flex items-center justify-between border-t border-zinc-800 pt-1.5">
                <span className="text-zinc-400">Timelapse Speed:</span>
                <div className="flex items-center gap-1">
                  {TIMELAPSE_PRESETS.map((t) => (
                    <button
                      key={t}
                      type="button"
                      onClick={() => onUpdate({ timelapse: t })}
                      className={`rounded px-1.5 py-0.5 text-[10px] font-mono ${
                        pane.timelapse === t ? 'bg-amber-500 font-bold text-zinc-950' : 'bg-zinc-800 text-zinc-300'
                      }`}
                    >
                      {t}x
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* BOTTOM INPUT BAR FOR THIS PANE */}
      <div className="border-t border-zinc-800 bg-zinc-900/95 p-2.5">
        <form
          onSubmit={(e) => {
            e.preventDefault()
            onSend()
          }}
          className="flex items-end gap-1.5"
        >
          <input
            ref={fileInputRef}
            type="file"
            accept="video/*"
            className="hidden"
            onChange={handleFileSelect}
          />

          {/* Video Attachment Button */}
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            disabled={pane.loading}
            title="Attach video clip"
            className={`flex size-8 shrink-0 items-center justify-center rounded-lg border transition-all ${
              pane.attachedVideo
                ? 'border-amber-500 bg-amber-500/20 text-amber-300'
                : 'border-zinc-800 bg-zinc-800/60 text-zinc-400 hover:text-zinc-200'
            }`}
          >
            <Video className="size-3.5" />
          </button>

          {/* Textarea Input */}
          <div className="flex-1 rounded-lg border border-zinc-800 bg-zinc-950 px-2.5 py-1.5 focus-within:border-amber-500/70 transition-all">
            <textarea
              value={pane.inputPrompt}
              onChange={(e) => onUpdate({ inputPrompt: e.target.value })}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  onSend()
                }
              }}
              disabled={pane.loading}
              placeholder={
                pane.attachedVideo
                  ? `Prompt for ${pane.attachedVideo.name}...`
                  : `Message ${modelInfo?.name || pane.selectedModel}...`
              }
              rows={1}
              className="w-full resize-none bg-transparent text-xs text-zinc-100 placeholder:text-zinc-600 focus:outline-none max-h-24"
            />
          </div>

          {/* Send Button */}
          <button
            type="submit"
            disabled={pane.loading || (!pane.inputPrompt.trim() && !pane.attachedVideo)}
            title="Send"
            className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-amber-500 text-zinc-950 font-bold hover:bg-amber-400 disabled:opacity-40 transition-all shadow"
          >
            {pane.loading ? <Loader2 className="size-3.5 animate-spin" /> : <Send className="size-3.5" />}
          </button>
        </form>
      </div>
    </div>
  )
}
