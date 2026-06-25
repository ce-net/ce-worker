#!/usr/bin/env node
// ce-worker — a native, headless CE compute node.
//
// It does exactly what the in-browser node (web/site/node.html) does, but as a
// background process: advertise this machine's capacity to the mesh and run WASM
// (or js) tasks pushed to it. No browser required.
//
// Transport: the CE mesh (libp2p request/reply on /ce/rpc/1), NOT a hub WebSocket.
// The worker attaches to a LOCAL ce node over its HTTP+SSE API and becomes a real
// mesh service:
//   * it `register`s the service name "ce-worker" on the DHT, so schedulers can
//     `locate("ce-worker")` and pick an instance;
//   * it `advertise`s capability tags (cores:N, ram:NN, plus any extra --tags) so
//     tag-filtered discovery (e.g. require "gpu") finds the right workers;
//   * it `serve`s the "ce-worker/run" request topic: each request payload is a job
//     (the same {func,args,module_b64} / {lang:"js",code,...} shape the hub used to
//     push over its socket), the reply payload is the JSON result.
//
// A client reaches it over the mesh with @ce-net/sdk's `call`:
//   call(ce, "ce-worker", "ce-worker/run", jobBytes)  // libp2p, relay/NAT-traversed
//
// The same file runs on macOS, Linux, and Windows. Point it at any local node:
//   node worker.js                                   # http://127.0.0.1:8844
//   node worker.js --node http://127.0.0.1:8844 --tags gpu,region:eu
//   CE_NODE_URL=http://127.0.0.1:8844 node worker.js
//
// Requires the @ce-net/sdk package (declared in package.json). Node 20+.

import os from 'node:os'
import fs from 'node:fs'
import { performance } from 'node:perf_hooks'

import { CeClient, serve, register } from '@ce-net/sdk'

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

// Local ce node API base URL the worker attaches to (its window onto the mesh).
const NODE_URL = (arg('node', process.env.CE_NODE_URL) || 'http://127.0.0.1:8844').replace(/\/$/, '')
const NAME = arg('name', process.env.CE_WORKER_NAME) || os.hostname()
// Service name schedulers `locate()` to find compute workers.
const SERVICE = arg('service', process.env.CE_WORKER_SERVICE) || 'ce-worker'
// Mesh request topic this worker answers job requests on.
const RUN_TOPIC = `${SERVICE}/run`
// Extra capability tags to advertise (comma-separated), e.g. "gpu,region:eu".
const EXTRA_TAGS = (arg('tags', process.env.CE_WORKER_TAGS) || '')
  .split(',').map((s) => s.trim()).filter(Boolean)
// How often to re-advertise the service + tags (DHT provider records expire).
const ADVERTISE_MS = 60_000

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

// ---- WASM/js job execution (mirrors node.html `runJob`; transport-agnostic) ----
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

// ---- mesh wiring ----
const enc = new TextEncoder()
const dec = new TextDecoder()

function log(...a) { console.log(new Date().toISOString(), ...a) }

// Capability tags advertised so tag-filtered discovery (locate require_tags) works:
// hardware truth derived from caps, plus any operator-supplied --tags.
function capabilityTags(caps) {
  return [`cores:${caps.cores}`, `ram:${caps.ram_gb}`, ...EXTRA_TAGS]
}

// The mesh request handler: decode a job request, run it, return the JSON result.
// `req.from` is the authenticated requester NodeId; a real deployment would verify a
// ce-cap chain here before computing. Always returns a reply so the caller's
// mesh.request never blocks to timeout.
async function handleRun(req) {
  let job
  try {
    job = JSON.parse(dec.decode(req.payload))
  } catch (e) {
    return enc.encode(JSON.stringify({ ok: false, value: '', ms: 0, error: `bad job payload: ${String(e?.message || e)}` }))
  }
  const res = await runJob(job)
  tasks++
  log(`task ${job.func}(${(job.args || []).join(',')}) from ${req.from.slice(0, 12)}… -> ${res.ok ? res.value : 'ERR ' + res.error} (${res.ms}ms, total ${tasks})`)
  return enc.encode(JSON.stringify({ jid: job.jid, ...res }))
}

let tasks = 0

async function main() {
  const ce = new CeClient({ baseUrl: NODE_URL })
  const ac = new AbortController()

  process.on('SIGINT', () => { log('shutting down'); ac.abort(); process.exit(0) })
  process.on('SIGTERM', () => { ac.abort(); process.exit(0) })

  const caps = detectCaps()
  log(`ce-worker starting — node=${NODE_URL} name=${NAME} service=${SERVICE}`)
  log(`capacity — cores=${caps.cores} ram=${caps.ram_gb}GB cpu_mark=${caps.cpu_mark}`)

  // Confirm the local node is reachable before we announce ourselves to the mesh.
  try {
    const s = await ce.status()
    log(`attached to local node — id=${String(s.nodeId || s.node_id || '').slice(0, 12)}… height=${s.height ?? '?'}`)
  } catch (e) {
    log(`WARNING: local node at ${NODE_URL} not reachable yet (${String(e?.message || e)}); serve loop will retry`)
  }

  // Advertise the service name + capability tags on a loop (records expire). `register`
  // re-advertises the bare "ce-worker" service so schedulers can `locate` it; the tags
  // loop adds discoverable capability tags for tag-filtered selection.
  const advertiseTags = async () => {
    while (!ac.signal.aborted) {
      try { await ce.tags.advertiseAll(capabilityTags(caps)) }
      catch (e) { log('tag advertise failed; will retry', String(e?.message || e)) }
      await new Promise((r) => setTimeout(r, ADVERTISE_MS))
    }
  }

  log(`online — serving "${RUN_TOPIC}" over the mesh; discoverable as service "${SERVICE}"`)

  // Run the three loops concurrently until aborted: re-advertise the service, the tags,
  // and serve inbound job requests over the mesh.
  await Promise.all([
    register(ce, SERVICE, ADVERTISE_MS, {
      signal: ac.signal,
      onWarn: (m, d) => log(m, String(d ?? '')),
    }),
    advertiseTags(),
    serve(ce, [RUN_TOPIC], handleRun, {
      signal: ac.signal,
      onWarn: (m, d) => log(m, String(d ?? '')),
    }),
  ])
}

// Exported for tests; importing this module does not start the serve loop unless run
// directly (so a smoke test can exercise the compute path without a live node).
export { runJob, detectCaps, capabilityTags, handleRun, RUN_TOPIC, SERVICE }

const RUN_DIRECTLY = process.argv[1] && import.meta.url === `file://${process.argv[1]}`
if (RUN_DIRECTLY) {
  main().catch((e) => { log('fatal', String(e?.stack || e)); process.exit(1) })
}
