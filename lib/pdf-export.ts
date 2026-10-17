import { jsPDF } from 'jspdf'
import type { Scan, LogEntry } from './types'
import { fmtTime } from './format'

export interface PdfExportOptions {
  /** If true (default), exports ALL logs from the scan. If false, uses logsToExport */
  allLogs?: boolean
  /** Optional custom subset of logs to export (e.g. filtered view) */
  logsToExport?: LogEntry[]
  /** Optional label or subtitle */
  label?: string
}

function getLogCategory(msg: string, level: string): { label: string; bg: [number, number, number]; text: [number, number, number] } {
  const m = (msg || '').toLowerCase()
  if (m.includes('[batch verifier]') || m.includes('batch verify') || m.includes('stitched')) {
    return { label: 'BATCH', bg: [124, 58, 237], text: [255, 255, 255] } // Violet
  }
  if (m.includes('render') || m.includes('export') || m.includes('padding') || m.includes('ffmpeg')) {
    return { label: 'RENDER', bg: [217, 119, 6], text: [255, 255, 255] } // Amber
  }
  if (m.includes('[rescan') || m.includes('rescan:')) {
    return { label: 'RESCAN', bg: [8, 145, 178], text: [255, 255, 255] } // Cyan
  }
  if (m.includes('chunk ') || m.includes('chunking') || m.includes('mapping short')) {
    return { label: 'SCAN', bg: [16, 185, 129], text: [255, 255, 255] } // Emerald
  }
  if (level === 'error') {
    return { label: 'ERROR', bg: [220, 38, 38], text: [255, 255, 255] } // Red
  }
  if (level === 'warn') {
    return { label: 'ALERT', bg: [234, 88, 12], text: [255, 255, 255] } // Orange
  }
  if (level === 'success') {
    return { label: 'SUCCESS', bg: [22, 163, 74], text: [255, 255, 255] } // Green
  }
  return { label: 'SYSTEM', bg: [71, 85, 105], text: [255, 255, 255] } // Slate
}

/**
 * Generates a clean, professional, publication-grade PDF containing all scan logs and metadata.
 * Works seamlessly client-side and triggers an instant browser download.
 */
export async function downloadScanLogsPdf(scan: Scan, options?: PdfExportOptions): Promise<void> {
  const all = options?.allLogs !== false
  const logs: LogEntry[] = all
    ? Array.isArray(scan.logs) ? scan.logs : []
    : options?.logsToExport || (Array.isArray(scan.logs) ? scan.logs : [])

  const doc = new jsPDF({
    orientation: 'portrait',
    unit: 'mm',
    format: 'a4',
  })

  const pageWidth = doc.internal.pageSize.getWidth() // 210mm
  const pageHeight = doc.internal.pageSize.getHeight() // 297mm
  const marginLeft = 14
  const marginRight = 14
  const contentWidth = pageWidth - marginLeft - marginRight // 182mm
  const marginBottom = 18

  let y = 14

  // --- TOP BRAND HEADER BAR ---
  doc.setFillColor(15, 23, 42) // Dark Slate #0f172a
  doc.roundedRect(marginLeft, y, contentWidth, 24, 2.5, 2.5, 'F')

  doc.setFont('helvetica', 'bold')
  doc.setFontSize(14)
  doc.setTextColor(248, 250, 252) // #f8fafc
  doc.text('CLIP MOVIE TRACKER', marginLeft + 5, y + 8)

  doc.setFont('helvetica', 'normal')
  doc.setFontSize(8.5)
  doc.setTextColor(148, 163, 184) // #94a3b8
  doc.text('AI-Powered Precision Video Matching & Scene Sync Engine', marginLeft + 5, y + 14)

  // Scan Status Badge on top-right of header bar
  const statusStr = (scan.status || 'unknown').toUpperCase()
  let statusBg: [number, number, number] = [71, 85, 105]
  if (scan.status === 'completed') statusBg = [22, 163, 74]
  else if (scan.status === 'stopped') statusBg = [217, 119, 6]
  else if (scan.status === 'scanning' || scan.status === 'verifying') statusBg = [37, 99, 235]
  else if (scan.status === 'error') statusBg = [220, 38, 38]

  const statusBadgeWidth = 32
  doc.setFillColor(statusBg[0], statusBg[1], statusBg[2])
  doc.roundedRect(pageWidth - marginRight - statusBadgeWidth - 5, y + 4.5, statusBadgeWidth, 7, 1.5, 1.5, 'F')
  doc.setFont('helvetica', 'bold')
  doc.setFontSize(8)
  doc.setTextColor(255, 255, 255)
  doc.text(statusStr, pageWidth - marginRight - (statusBadgeWidth / 2) - 5, y + 9.2, { align: 'center' })

  // Sub-status timestamp
  doc.setFont('helvetica', 'normal')
  doc.setFontSize(7.5)
  doc.setTextColor(203, 213, 225)
  const exportDateStr = new Date().toLocaleString()
  doc.text(`Exported: ${exportDateStr}`, pageWidth - marginRight - 5, y + 18, { align: 'right' })

  y += 28

  // --- SCAN METADATA SUMMARY CARD ---
  doc.setFillColor(248, 250, 252) // Light Slate #f8fafc
  doc.setDrawColor(226, 232, 240) // Border #e2e8f0
  doc.setLineWidth(0.3)
  doc.roundedRect(marginLeft, y, contentWidth, 24, 2, 2, 'FD')

  // Row 1 of Metadata
  doc.setFont('helvetica', 'bold')
  doc.setFontSize(9)
  doc.setTextColor(15, 23, 42)
  const scanTitle = scan.shortName || `Scan #${scan.id.slice(0, 10)}`
  doc.text(`Scan: ${scanTitle}`, marginLeft + 4, y + 6)

  doc.setFont('helvetica', 'normal')
  doc.setFontSize(8)
  doc.setTextColor(71, 85, 105)
  doc.text(`Scan ID: ${scan.id}`, marginLeft + 4, y + 11.5)

  // Durations & Stats
  const shortDurText = scan.shortDuration ? fmtTime(scan.shortDuration) : 'N/A'
  const movieDurText = scan.movieDuration ? fmtTime(scan.movieDuration) : 'N/A'
  const confirmedCount = scan.candidateGroups?.filter((g) => g.status === 'confirmed').length ?? 0
  const matchesCount = scan.matches?.length ?? 0

  doc.text(`Short Duration: ${shortDurText}  ·  Movie Duration: ${movieDurText}`, marginLeft + 4, y + 17)

  // Right column stats
  const warnCount = logs.filter((l) => l.level === 'warn').length
  const errCount = logs.filter((l) => l.level === 'error').length

  doc.setFont('helvetica', 'bold')
  doc.setTextColor(15, 23, 42)
  doc.text(`Total Logged Events: ${logs.length}`, pageWidth - marginRight - 4, y + 6, { align: 'right' })

  doc.setFont('helvetica', 'normal')
  doc.setTextColor(71, 85, 105)
  doc.text(`Confirmed: ${confirmedCount}  ·  Candidates: ${matchesCount}`, pageWidth - marginRight - 4, y + 11.5, { align: 'right' })
  doc.text(`Alerts: ${warnCount}  ·  Errors: ${errCount}`, pageWidth - marginRight - 4, y + 17, { align: 'right' })

  y += 28

  // Section title
  doc.setFont('helvetica', 'bold')
  doc.setFontSize(10.5)
  doc.setTextColor(15, 23, 42)
  const logsHeader = options?.label ? `Activity Logs (${options.label})` : `Complete Chronological Activity Logs (${logs.length} Events)`
  doc.text(logsHeader, marginLeft, y)

  y += 4

  // Table header line
  doc.setFillColor(241, 245, 249) // #f1f5f9
  doc.rect(marginLeft, y, contentWidth, 6, 'F')
  doc.setFont('helvetica', 'bold')
  doc.setFontSize(7.5)
  doc.setTextColor(100, 116, 139)
  doc.text('TIMESTAMP', marginLeft + 3, y + 4.2)
  doc.text('CATEGORY', marginLeft + 28, y + 4.2)
  doc.text('EVENT DETAILS & MODEL DIAGNOSTICS', marginLeft + 54, y + 4.2)

  y += 7.5

  // --- LOG ENTRIES LOOP ---
  const msgColWidth = contentWidth - 54 - 2

  doc.setFont('courier', 'normal')
  doc.setFontSize(7.2)

  for (let idx = 0; idx < logs.length; idx++) {
    const log = logs[idx]
    if (!log) continue

    const timeStr = log.t ? new Date(log.t).toTimeString().split(' ')[0] : '--:--:--'
    const cat = getLogCategory(log.msg || '', log.level || 'info')

    // Split message into multiple lines if needed
    doc.setFont('courier', 'normal')
    doc.setFontSize(7)
    const msgLines: string[] = doc.splitTextToSize(log.msg || '', msgColWidth)
    const lineHeight = 3.6
    const rowHeight = Math.max(6, msgLines.length * lineHeight + 2)

    // Check if adding this row exceeds page boundary
    if (y + rowHeight > pageHeight - marginBottom) {
      doc.addPage()
      y = 14

      // Page continuation mini-header
      doc.setFont('helvetica', 'italic')
      doc.setFontSize(7.5)
      doc.setTextColor(148, 163, 184)
      doc.text(`ClipMovieTracker · Scan #${scan.id.slice(0, 10)} Logs (continued)`, marginLeft, y)

      y += 4
      doc.setFillColor(241, 245, 249)
      doc.rect(marginLeft, y, contentWidth, 5.5, 'F')
      doc.setFont('helvetica', 'bold')
      doc.setFontSize(7.2)
      doc.setTextColor(100, 116, 139)
      doc.text('TIMESTAMP', marginLeft + 3, y + 3.8)
      doc.text('CATEGORY', marginLeft + 28, y + 3.8)
      doc.text('EVENT DETAILS & MODEL DIAGNOSTICS', marginLeft + 54, y + 3.8)
      y += 7
    }

    // Row alternating background
    if (idx % 2 === 1) {
      doc.setFillColor(250, 250, 250)
      doc.rect(marginLeft, y - 1, contentWidth, rowHeight, 'F')
    }

    // Left border indicator for warns and errors
    if (log.level === 'error') {
      doc.setFillColor(239, 68, 68)
      doc.rect(marginLeft, y - 1, 1.2, rowHeight, 'F')
    } else if (log.level === 'warn') {
      doc.setFillColor(245, 158, 11)
      doc.rect(marginLeft, y - 1, 1.2, rowHeight, 'F')
    } else if (log.level === 'success') {
      doc.setFillColor(34, 197, 94)
      doc.rect(marginLeft, y - 1, 1.2, rowHeight, 'F')
    }

    // Timestamp
    doc.setFont('courier', 'normal')
    doc.setFontSize(7)
    doc.setTextColor(100, 116, 139)
    doc.text(timeStr, marginLeft + 3, y + 2.5)

    // Category Badge
    doc.setFillColor(cat.bg[0], cat.bg[1], cat.bg[2])
    doc.roundedRect(marginLeft + 26, y - 0.6, 22, 4.4, 0.8, 0.8, 'F')
    doc.setFont('helvetica', 'bold')
    doc.setFontSize(6.2)
    doc.setTextColor(cat.text[0], cat.text[1], cat.text[2])
    doc.text(cat.label, marginLeft + 37, y + 2.4, { align: 'center' })

    // Message text
    doc.setFont('courier', 'normal')
    doc.setFontSize(6.8)
    if (log.level === 'error') {
      doc.setTextColor(185, 28, 28) // Dark red
    } else if (log.level === 'warn') {
      doc.setTextColor(180, 83, 9) // Dark amber
    } else {
      doc.setTextColor(30, 41, 59) // Dark slate
    }

    let msgY = y + 2.4
    for (const line of msgLines) {
      doc.text(line, marginLeft + 54, msgY)
      msgY += lineHeight
    }

    y += rowHeight + 0.8
  }

  // --- FOOTER ON ALL PAGES ---
  const totalPages = doc.internal.getNumberOfPages()
  for (let page = 1; page <= totalPages; page++) {
    doc.setPage(page)

    doc.setDrawColor(226, 232, 240)
    doc.setLineWidth(0.3)
    doc.line(marginLeft, pageHeight - 11, pageWidth - marginRight, pageHeight - 11)

    doc.setFont('helvetica', 'normal')
    doc.setFontSize(7.5)
    doc.setTextColor(148, 163, 184)
    doc.text(`ClipMovieTracker · Scan ID: ${scan.id.slice(0, 14)}...`, marginLeft, pageHeight - 7)

    doc.setFont('helvetica', 'bold')
    doc.text(`Page ${page} of ${totalPages}`, pageWidth - marginRight, pageHeight - 7, { align: 'right' })
  }

  // Generate safe filename and trigger download
  const cleanTitle = (scan.shortName || 'scan')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .slice(0, 20)
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  const filename = `${cleanTitle}-logs-${timestamp}.pdf`

  doc.save(filename)
}
