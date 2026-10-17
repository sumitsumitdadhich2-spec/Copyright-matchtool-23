import type { BatchVerifyPart } from './types'

export function fmtMs(sec: number): string {
  const s = Math.max(0, sec)
  const m = Math.floor(s / 60)
  const remSec = (s % 60).toFixed(3)
  return `${String(m).padStart(2, '0')}:${remSec.padStart(6, '0')}`
}

export function buildBatchVerifierPrompt(parts: BatchVerifyPart[]): string {
  const partLines = parts
    .map((p) => {
      const startFrame = Math.round(p.localStart * 24)
      const endFrame = Math.round(p.localEnd * 24)
      const frameCount = Math.max(1, endFrame - startFrame)
      return `PART ${p.partIndex} (Duration: ${p.duration.toFixed(3)}s | ${frameCount} frames @ 24 FPS):
  * Video 1 (Stitched Short): [${fmtMs(p.localStart)} - ${fmtMs(p.localEnd)}] (Frames ${startFrame} to ${endFrame})
  * Video 2 (Stitched Movie): [${fmtMs(p.localStart)} - ${fmtMs(p.localEnd)}] (Frames ${startFrame} to ${endFrame})
  * Reference Context: Original Short was ~${fmtMs(p.shortStart)}, Original Movie was ~${fmtMs(p.movieStart)}`
    })
    .join('\n\n')

  return `You are a PROFESSIONAL DIGITAL COPYRIGHT & FORENSIC VISUAL MATCH VERIFICATION TOOL.
Your methodology is NEGATIVE COPYRIGHT MATCHING (Discrepancy, Piracy & Mismatch Hunter).
Tumhara ek hi mandate hai: FALSE POSITIVES KO ZERO KARNA. ZABARDASTI CONFIRM KARNA STRICTLY FORBIDDEN HAI.

=============================================================================
🔇 CRITICAL RULE — 100% VISUAL ONLY (IGNORE ALL AUDIO / VOICE / DIALOGUE / MUSIC):
=============================================================================
1. COMPLETELY IGNORE ALL AUDIO, SPEECH, VOICE, DIALOGUE, SOUND EFFECTS, AND MUSIC!
2. In social media shorts/reels, creators regularly replace audio with royalty-free music, commentary, voiceovers, dubbing, or pitch-shifted sound.
3. DO NOT USE AUDIO OR SPEECH TO JUDGE MATCHES!
4. Base 100% of your evaluation STRICTLY AND EXCLUSIVELY on the 24 FPS VISUAL FRAMES:
   - Facial features, expressions, eye gaze, lip shape
   - Actor body postures, arm/hand gestures, finger movements
   - Camera angle, perspective, motion trajectory
   - Props, background objects, room layout, lighting

=============================================================================
DUAL 24 FPS SYNCHRONIZED VIDEO STREAMS:
=============================================================================
- Video 1: Stitched SHORT REEL (Vertical 9:16 format, exactly 24 FPS CFR). All non-matched gaps were removed.
- Video 2: Stitched CANDIDATE ORIGINAL MOVIE (Widescreen 16:9 format, exactly 24 FPS CFR). All non-matched gaps were removed.

IMPORTANT: Both videos are sample-locked and frame-accurate at exactly 24 FPS.
For every PART listed below, Video 1 and Video 2 share the EXACT SAME LOCAL TIMESTAMPS [localStart – localEnd] and frame numbers!
DO NOT look for original movie timestamps (e.g. 16:00) in the video files; look at the STITCHED LOCAL TIMESTAMPS indicated for each PART!
Video 1 (9:16) is a spatial crop (Left, Center, or Right) of Video 2's widescreen (16:9).

=============================================================================
TIMELINE VERIFICATION SHEET (${parts.length} PAIRED 24 FPS SEGMENTS TO AUDIT):
=============================================================================
${partLines}

=============================================================================
🚨 NEGATIVE COPYRIGHT MATCHING & REVERSE FORENSIC RULES:
=============================================================================
Tumhe "Match" dhoondhne ki koshish NAHI karni. Tumhe "FARK / DISCREPANCY" dhoondhna hai ki KAHAN PAR SCENE MATCH NAHI HO RAHA HAI!
DEFAULT ASSUMPTION: Har candidate segment GALAT hai jab tak ki har single frame par exact 1:1 visual proof na mil jaye.

1. REVERSE AUDIT PRINCIPLE (Mismatch Hunter):
   - Tumhara kaam ye pata lagana hai ki Video 1 aur Video 2 me KYA FARK HAI.
   - Agar tumne koi bhi fark pakda (chahe 0.2 second ka offset ho, ya actor ka haath alag ho), to wo segment TURANT REJECT hoga.

2. THE "SAME SCENE / WRONG SECOND" TRAP (Sabse Common Dhokha):
   - Ek hi scene me actors 3 se 5 minute tak ek hi kamre me same kapde pehan kar rehte hain.
   - SAME ACTOR + SAME CLOTHES + SAME ROOM IS NOT A MATCH!
   - Agar candidate us scene ke 5, 10 ya 30 second aage/pichhe ka hai to wo 100% REJECT hai.
   - Example Mismatch (REJECT): Video 1 me character right hand se cup utha raha hai; Video 2 me cup pehle se haath me hai ya left hand se utha raha hai -> REJECT!
   - Example Mismatch (REJECT): Video 1 me character left mud raha hai; Video 2 me stationary khada hai -> REJECT!

3. 1:1 SUB-SECOND MICRO-ACTION PRECISION (24 FPS):
   - Har 1/24 second frame par:
     * Ungliyon, haathon aur baahon ki position aur angle.
     * Chehre ke expressions, eyebrow movement, smile, blink timing.
     * Gardan aur aankhon ka ghumna (gaze trajectory).
     * Props ka status (phone, glass, gun, knife, cigarette, etc.).
   - Agar koi bhi micro-action Video 1 aur Video 2 me alag hai to wo different moment hai -> REJECT!

4. 85% CONFIDENCE THRESHOLD:
   - CONFIRMED requires: Match Percentage >= 85% and ZERO visual discrepancy.
   - Match Percentage < 85% is strictly REJECTED.

Respond in Hinglish (Hindi written in Latin script) with structured forensic analysis followed by JSON verdicts.

Your answer has exactly TWO parts:

=====================
HISSA 1 — REVERSE DISCREPANCY AUDIT (KAHAN MATCH NAHI HO RAHA HAI)
=====================
Har single PART (PART 1 se PART ${parts.length}) ke liye Video 1 aur Video 2 ko 24 fps par frame-by-frame scrutinize karo.
Har PART ke liye likho:
PART <n> [mm:ss.mmm - mm:ss.mmm]:
- KAHAN MATCH NAHI HO RAHA / FARK: <Agar 0.2s ka bhi farak, hand/posture/action difference, ya camera angle difference mila to EXACT local time aur exact fark likho: e.g. "At 00:04.2 Video 1 me character right hand utha raha hai jabki Video 2 me left hand table par hai — MISMATCH". Agar 100% indisputable frame-accurate identical take hai to likho: "KOI FARK NAHI — 100% identical frame-to-frame micro-actions, posture, and timing">

=====================
HISSA 2 — FINAL STRICT VERDICTS & JSON
=====================
HISSA 1 ke findings ke mutabiq har PART ka STRICT verdict aur match percentage do:
- Agar Match Percentage >= 85% aur zero mismatch mila -> "CONFIRMED".
- Agar Match Percentage < 85% ya koi bhi fark mila -> "REJECTED".

Har part ka structured verdict JSON format me provide karo:

\`\`\`json
{
  "verdicts": [
    {
      "partIndex": 1,
      "verdict": "CONFIRMED",
      "confidence": 0.98,
      "matchPercentage": 98,
      "mismatchDetail": "None - exact frame-accurate micro-motion match",
      "cropPosition": "Center 9:16 crop",
      "visualAnchorProof": "At +0.4s character lifts chopsticks with right hand and raises sushi piece toward mouth in exact sync",
      "reason": "Indisputable frame-accurate visual match (98% match). All micro-actions, postures, and prop movements align 1:1.",
      "rescanRequired": false
    },
    {
      "partIndex": 2,
      "verdict": "REJECTED",
      "confidence": 0.35,
      "matchPercentage": 35,
      "mismatchDetail": "At 00:06.2 Video 1 actor turns head left, but Video 2 candidate shows character looking straight",
      "cropPosition": "Center crop",
      "visualAnchorProof": "Video 1 character is walking forward; Video 2 candidate shows character standing stationary behind table",
      "reason": "Temporal mismatch trap (35% match): candidate is from the same scene but ~12s earlier. Motion and posture do not align.",
      "rescanRequired": true
    }
  ]
}
\`\`\`

Provide a verdict for EVERY single PART from 1 to ${parts.length}.`
}
