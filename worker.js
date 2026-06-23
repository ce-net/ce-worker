#!/usr/bin/env node
// ce-worker — a native, headless CE compute node.
//
// It does exactly what the in-browser node (web/site/node.html) does, but as a
// background process: connect to a ce-hub over WebSocket, advertise this
// machine's capacity, and run WASM tasks pushed to it. No browser required.
//
// The same file runs on macOS, Linux, and Windows. Point it at any ce-hub:
//   node worker.js --hub wss://ce-net.com/hub
//   node worker.js --hub ws://127.0.0.1:8970         # a local hub
//
// On Node 22+ the global WebSocket is used (zero dependencies). On older Node
// (e.g. the relay's Node 20) install the standard `ws` package and it is used
// automatically: `npm i ws`.

import os from 'node:os'
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { performance } from 'node:perf_hooks'

// ---- WebSocket implementation (global on Node 22+, else the `ws` package) ----
let WS = globalThis.WebSocket
if (!WS) {
  try { WS = (await import('ws')).WebSocket } catch { /* fall through */ }
}
if (!WS) {
  console.error('ce-worker: no WebSocket available. Use Node 22+, or run `npm i ws` in this directory.')
  process.exit(1)
}

// ---- args / env ----
const argv = process.argv.slice(2)
function arg(name, def) {
  const pfx = `--${name}=`
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === `--${name}`) return argv[i + 1]
    if (argv[i].startsWith(pfx)) return argv[i].slice(pfx.length)
  }
  return def
}
const HUB = (arg('hub', process.env.CE_WORKER_HUB) || 'wss://ce-net.com/hub').replace(/\/$/, '')
const NAME = arg('name', process.env.CE_WORKER_NAME) || os.hostname()
const HB_MS = 10_000

// ---- stable node id (persisted) ----
function loadId() {
  const file = process.env.CE_WORKER_ID_FILE ||
    path.join(os.homedir(), '.local', 'share', 'ce', 'worker-id')
  try {
    const v = fs.readFileSync(file, 'utf8').trim()
    if (/^[0-9a-f]{64}$/.test(v)) return v
  } catch { /* generate below */ }
  const id = crypto.randomBytes(32).toString('hex')
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.writeFileSync(file, id + '\n', { mode: 0o600 })
  } catch { /* non-fatal: id is still stable for this process */ }
  return id
}
const ID = loadId()

// ---- capability detection (mirrors node.html's `detect()` shape) ----
function cpuBench() {
  // ~50 ms busy loop; report throughput in Mops/s, comparable to the browser bench.
  const t0 = performance.now()
  let x = 0, ops = 0
  while (performance.now() - t0 < 50) {
    for (let i = 0; i < 100_000; i++) { x = (x + i * 1.000001) % 9_999_991 }
    ops += 100_000
  }
  const secs = (performance.now() - t0) / 1000
  return Math.round((ops / 1e6 / secs) * 10) / 10
}
function detectCaps() {
  let storage_gb = 0
  try {
    const s = fs.statfsSync(os.homedir())
    storage_gb = Math.round((s.bsize * s.blocks) / 1e9 * 10) / 10
  } catch { /* optional */ }
  return {
    cores: os.cpus().length,
    ram_gb: Math.round(os.totalmem() / 1e9 * 10) / 10,
    storage_gb,
    gpu: '',                 // native CPU worker; GPU jobs go through the CE node
    webgpu: false,
    vram_mb: 0,
    platform: `${os.platform()}-${os.arch()} node/${process.versions.node} (${NAME})`,
    cpu_mark: cpuBench(),
  }
}

// ---- WASM job execution (mirrors node.html `runJob`) ----
async function runJob(job) {
  const t0 = performance.now()
  try {
    const bytes = Buffer.from(job.module_b64 || '', 'base64')
    const { instance } = await WebAssembly.instantiate(bytes, {})
    const fn = instance.exports[job.func]
    if (typeof fn !== 'function') throw new Error(`no export "${job.func}"`)
    let r = fn(...(job.args || []))
    if (typeof r === 'bigint') r = r.toString()
    return { ok: true, value: String(r), ms: Math.round((performance.now() - t0) * 100) / 100 }
  } catch (e) {
    return { ok: false, value: '', ms: Math.round((performance.now() - t0) * 100) / 100, error: String(e?.message || e) }
  }
}

// ---- connection loop ----
let caps = detectCaps()
let ws = null, hb = null, tasks = 0, backoff = 2000

function log(...a) { console.log(new Date().toISOString(), ...a) }

function connect() {
  const url = HUB + '/node'
  log(`connecting to ${url}`)
  ws = new WS(url)

  ws.onopen = () => {
    backoff = 2000
    caps = detectCaps()
    ws.send(JSON.stringify({ t: 'hello', id: ID, caps }))
    log(`online — id=${ID.slice(0, 12)}… cores=${caps.cores} ram=${caps.ram_gb}GB cpu_mark=${caps.cpu_mark}`)
    clearInterval(hb)
    hb = setInterval(() => { try { ws.send(JSON.stringify({ t: 'hb' })) } catch { /* ignore */ } }, HB_MS)
  }

  ws.onmessage = async (ev) => {
    let m
    try { m = JSON.parse(typeof ev.data === 'string' ? ev.data : ev.data.toString()) } catch { return }
    if (m.t === 'welcome') { log('registered with hub'); return }
    if (m.t === 'job') {
      const res = await runJob(m)
      try { ws.send(JSON.stringify({ t: 'result', jid: m.jid, ...res })) } catch { /* dropped */ }
      tasks++
      log(`task ${m.func}(${(m.args || []).join(',')}) -> ${res.ok ? res.value : 'ERR ' + res.error} (${res.ms}ms, total ${tasks})`)
    }
  }

  ws.onerror = (e) => { log('socket error', String(e?.message || e?.error || e || '')) }
  ws.onclose = () => {
    clearInterval(hb)
    log(`disconnected — reconnecting in ${backoff}ms`)
    setTimeout(connect, backoff)
    backoff = Math.min(backoff * 2, 30_000)
  }
}

process.on('SIGINT', () => { log('shutting down'); try { ws?.close() } catch { /* ignore */ } process.exit(0) })
process.on('SIGTERM', () => { try { ws?.close() } catch { /* ignore */ } process.exit(0) })

log(`ce-worker starting — hub=${HUB} name=${NAME}`)
connect()
