import { NextRequest, NextResponse } from 'next/server'
import { getSession } from '@/lib/users'
import { globalOrchestrator } from '@/lib/orchestrator-core'
import { getApiKeyN } from '@/lib/store'
import { ORCHESTRATOR_WORKER_KEY_SLOTS } from '@/lib/models'

export const runtime = 'nodejs'

export async function GET() {
  const session = await getSession()
  if (!session) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  // Ensure available keys from store are populated
  for (const slot of ORCHESTRATOR_WORKER_KEY_SLOTS) {
    const k = getApiKeyN(slot)
    if (k && k.trim()) {
      globalOrchestrator.registerKey(slot, k.trim())
    }
  }

  const state = globalOrchestrator.getState()
  const logs = globalOrchestrator.getLogs()

  return NextResponse.json({
    state,
    logs,
    timestamp: Date.now(),
  })
}

export async function POST(req: NextRequest) {
  const session = await getSession()
  if (!session) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  try {
    const body = await req.json()
    const { action, model, keySlot, scanId, chunks } = body

    // Populate keys
    for (const slot of ORCHESTRATOR_WORKER_KEY_SLOTS) {
      const k = getApiKeyN(slot)
      if (k && k.trim()) {
        globalOrchestrator.registerKey(slot, k.trim())
      }
    }

    if (action === 'probe') {
      const targetModel = model || 'gemini-3.6-flash'
      const slotNum = typeof keySlot === 'number' ? keySlot : undefined
      const result = await globalOrchestrator.probeChoke(targetModel, slotNum)
      return NextResponse.json({ ok: true, result, state: globalOrchestrator.getState() })
    }

    if (action === 'start') {
      if (!scanId || !Array.isArray(chunks)) {
        return NextResponse.json({ error: 'scanId and chunks array are required' }, { status: 400 })
      }
      globalOrchestrator.prepareChunks(scanId, chunks)
      void globalOrchestrator.runOrchestrationLoop()
      return NextResponse.json({ ok: true, message: 'Orchestrator started', state: globalOrchestrator.getState() })
    }

    if (action === 'pause') {
      globalOrchestrator.pause()
      return NextResponse.json({ ok: true, state: globalOrchestrator.getState() })
    }

    if (action === 'resume') {
      globalOrchestrator.resume()
      return NextResponse.json({ ok: true, state: globalOrchestrator.getState() })
    }

    if (action === 'stop') {
      globalOrchestrator.stop()
      return NextResponse.json({ ok: true, state: globalOrchestrator.getState() })
    }

    return NextResponse.json({ error: 'Unknown action' }, { status: 400 })
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err)
    return NextResponse.json({ error: msg }, { status: 500 })
  }
}
