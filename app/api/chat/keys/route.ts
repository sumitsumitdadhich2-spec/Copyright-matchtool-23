import { NextRequest, NextResponse } from 'next/server'
import { getSession } from '@/lib/users'
import {
  syncAndGetChatKeysMap,
  forceSyncFromNormalSettings,
  setChatKeyN,
  clearChatKeyN,
} from '@/lib/chat-keys'
import { MAX_API_KEYS } from '@/lib/store'

export const runtime = 'nodejs'

export async function GET() {
  const session = await getSession()
  if (!session) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const keysMap = await syncAndGetChatKeysMap(session.username)

  const configuredSlots: { slot: number; label: string; maskedKey: string }[] = []
  for (let n = 1; n <= MAX_API_KEYS; n++) {
    const key = keysMap[String(n)]
    if (key && key.trim()) {
      configuredSlots.push({
        slot: n,
        label: `Key ${n}`,
        maskedKey: `Key ${n}`,
      })
    }
  }

  const hasEnvKey = Boolean(process.env.GEMINI_API_KEY && process.env.GEMINI_API_KEY.trim())

  return NextResponse.json({
    slots: configuredSlots,
    maxSlots: MAX_API_KEYS,
    hasEnvKey,
    hasAnyKey: configuredSlots.length > 0 || hasEnvKey,
  })
}

export async function POST(req: NextRequest) {
  const session = await getSession()
  if (!session) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  try {
    const body = await req.json()
    const action = body.action || 'set'

    if (action === 'resync') {
      await forceSyncFromNormalSettings(session.username)
      return NextResponse.json({ ok: true, message: 'Chat keys re-synced from Settings.' })
    }

    if (action === 'delete') {
      const slot = Number(body.slot)
      if (!slot || slot < 1 || slot > MAX_API_KEYS) {
        return NextResponse.json({ error: 'Invalid slot' }, { status: 400 })
      }
      await clearChatKeyN(session.username, slot)
      return NextResponse.json({ ok: true, message: `Chat key ${slot} removed.` })
    }

    if (action === 'set') {
      const slot = Number(body.slot)
      const key = String(body.key || '').trim()
      if (!slot || slot < 1 || slot > MAX_API_KEYS) {
        return NextResponse.json({ error: 'Invalid slot' }, { status: 400 })
      }
      if (!key) {
        return NextResponse.json({ error: 'Key cannot be empty' }, { status: 400 })
      }
      await setChatKeyN(session.username, slot, key)
      return NextResponse.json({ ok: true, message: `Chat key ${slot} saved.` })
    }

    return NextResponse.json({ error: 'Unknown action' }, { status: 400 })
  } catch (err) {
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Failed to update chat key' },
      { status: 500 },
    )
  }
}
