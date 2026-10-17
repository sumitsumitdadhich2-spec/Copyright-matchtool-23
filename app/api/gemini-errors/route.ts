import { NextResponse } from 'next/server'
import { getRecentGeminiErrors } from '@/lib/gemini'

export const dynamic = 'force-dynamic'

export async function GET() {
  const errors = getRecentGeminiErrors(50)
  return NextResponse.json({ errors })
}
