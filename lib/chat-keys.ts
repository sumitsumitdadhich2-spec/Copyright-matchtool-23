import 'server-only'

import { MAX_API_KEYS, getApiKeyN } from './store'
import { getUserKeyN } from './user-keys'
import { readJSONRecord, writeJSONRecord } from './json-record'

// ---------------------------------------------------------------------------
// CHAT-SPECIFIC API KEYS — Completely separated from normal scan settings!
// Stored in: auth/chat-keys/<username>.json
// - Zero usage calculation / counting
// - Zero exhaustion tracking
// - Normal settings keys are automatically copied over so the user never
//   has to type or paste keys twice.
// ---------------------------------------------------------------------------

const CHAT_KEYS_PREFIX = 'auth/chat-keys/'
const NORMAL_KEYS_PREFIX = 'auth/keys/'

type KeyMap = Record<string, string>

function cacheKey(username: string): string {
  return username.trim().toLowerCase()
}

function chatRecordFor(username: string): string {
  return `${CHAT_KEYS_PREFIX}${cacheKey(username)}.json`
}

function normalRecordFor(username: string): string {
  return `${NORMAL_KEYS_PREFIX}${cacheKey(username)}.json`
}

async function readChatRecord(username: string): Promise<KeyMap> {
  const data = await readJSONRecord<KeyMap>(chatRecordFor(username))
  return data && typeof data === 'object' ? data : {}
}

async function writeChatRecord(username: string, keys: KeyMap): Promise<void> {
  await writeJSONRecord(chatRecordFor(username), keys)
}

async function readNormalRecord(username: string): Promise<KeyMap> {
  const data = await readJSONRecord<KeyMap>(normalRecordFor(username))
  return data && typeof data === 'object' ? data : {}
}

/**
 * Synchronizes normal settings keys into the chat keys store if missing,
 * ensuring automatic availability without double-entry.
 * Checks user-keys, general store settings, and process.env.
 * Returns the merged chat keys map.
 */
export async function syncAndGetChatKeysMap(username: string): Promise<KeyMap> {
  const chatKeys = await readChatRecord(username)
  const normalKeys = await readNormalRecord(username)

  let changed = false
  const updated: KeyMap = { ...chatKeys }

  // Auto-copy any key that exists in user settings or general settings into chat keys
  for (let n = 1; n <= MAX_API_KEYS; n++) {
    const slotStr = String(n)
    if (!updated[slotStr] || !updated[slotStr].trim()) {
      const userKey = await getUserKeyN(username, n)
      const normalKey = normalKeys[slotStr]
      const storeKey = getApiKeyN(n)
      const candidateKey = (userKey || normalKey || storeKey || '').trim()
      if (candidateKey) {
        updated[slotStr] = candidateKey
        changed = true
      }
    }
  }

  // If slot 1 is still empty but environment key exists, default slot 1 to process.env.GEMINI_API_KEY
  if (!updated['1'] && process.env.GEMINI_API_KEY?.trim()) {
    updated['1'] = process.env.GEMINI_API_KEY.trim()
    changed = true
  }

  if (changed) {
    await writeChatRecord(username, updated)
  }

  return updated
}

/**
 * Force copy/re-sync all keys from normal settings into the chat keys store.
 */
export async function forceSyncFromNormalSettings(username: string): Promise<KeyMap> {
  const chatKeys = await readChatRecord(username)
  const normalKeys = await readNormalRecord(username)

  const updated: KeyMap = { ...chatKeys }
  for (let n = 1; n <= MAX_API_KEYS; n++) {
    const slotStr = String(n)
    const userKey = await getUserKeyN(username, n)
    const normalKey = normalKeys[slotStr]
    const storeKey = getApiKeyN(n)
    const candidateKey = (userKey || normalKey || storeKey || '').trim()
    if (candidateKey) {
      updated[slotStr] = candidateKey
    }
  }

  if (!updated['1'] && process.env.GEMINI_API_KEY?.trim()) {
    updated['1'] = process.env.GEMINI_API_KEY.trim()
  }

  await writeChatRecord(username, updated)
  return updated
}

/**
 * Read specific chat key for slot n (1-20).
 */
export async function getChatKeyN(username: string, n: number): Promise<string | null> {
  const keys = await syncAndGetChatKeysMap(username)
  return keys[String(n)] || null
}

/**
 * Save a key specifically for chat in slot n (1-20).
 * Does NOT touch normal settings.
 */
export async function setChatKeyN(username: string, n: number, key: string): Promise<void> {
  const keys = await readChatRecord(username)
  await writeChatRecord(username, { ...keys, [String(n)]: key.trim() })
}

/**
 * Remove a key from chat keys in slot n.
 * Does NOT touch normal settings.
 */
export async function clearChatKeyN(username: string, n: number): Promise<void> {
  const keys = { ...(await readChatRecord(username)) }
  delete keys[String(n)]
  await writeChatRecord(username, keys)
}

/**
 * All configured chat keys in slot order, de-duplicated.
 */
export async function getAllChatApiKeys(username: string): Promise<string[]> {
  const keys = await syncAndGetChatKeysMap(username)
  const out: string[] = []
  for (let n = 1; n <= MAX_API_KEYS; n++) {
    const k = keys[String(n)]
    if (k && !out.includes(k)) out.push(k)
  }
  return out
}
