/* eslint-disable @typescript-eslint/no-require-imports */
// Preloaded into the production server:  node --require ./server-timeouts.cjs server.js
// (see Dockerfile CMD). Next.js' standalone server creates a plain Node
// http.Server and never touches its timeouts, so Node's defaults apply:
//
//   requestTimeout = 300 000 ms  → any request whose BODY takes longer than
//                                  5 minutes to arrive is killed with a 408.
//
// A single-stream video upload of a few GB routinely takes longer than that,
// so lift the limit for this process. Header timeouts stay in place (slowloris
// protection) and Caddy in front still owns the public-facing timeouts.
const http = require('node:http')

const createServer = http.createServer
http.createServer = function createServerWithUploadTimeouts(...args) {
  const server = createServer.apply(this, args)
  server.requestTimeout = 0 // no limit on body transfer time
  server.headersTimeout = 65_000 // headers must still arrive within 65 s
  server.keepAliveTimeout = 65_000 // keep-alive idle > Caddy's dial/idle defaults
  return server
}

// Configure undici dispatcher for outbound Gemini fetch calls (15 min timeouts)
try {
  const { setGlobalDispatcher, Agent } = require('undici')
  setGlobalDispatcher(
    new Agent({
      headersTimeout: 15 * 60 * 1000,
      bodyTimeout: 15 * 60 * 1000,
      connectTimeout: 60 * 1000,
    }),
  )
} catch {
  // ignore
}
