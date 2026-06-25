// Smoke test for ce-worker's compute path + mesh request handler.
//
// No live node required: it exercises the same `runJob` / `handleRun` the mesh serve
// loop calls, proving the transport change (hub-WSS -> mesh) left the compute path
// intact. Run: `node test/smoke.test.js` (also wired into `npm test`).

import assert from 'node:assert/strict'
import { runJob, handleRun, capabilityTags, detectCaps, RUN_TOPIC, SERVICE } from '../worker.js'

// A minimal valid WebAssembly module exporting `add(i32,i32)->i32`, hand-assembled
// so the test has zero build deps. (wat: (func (export "add") (param i32 i32) (result i32)
// local.get 0 local.get 1 i32.add))
const ADD_WASM = new Uint8Array([
  0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic + version
  0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, // type: (i32,i32)->i32
  0x03, 0x02, 0x01, 0x00, // func section: one func of type 0
  0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00, // export "add" -> func 0
  0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, 0x0b, // code: get0 get1 add end
])
const MODULE_B64 = Buffer.from(ADD_WASM).toString('base64')

const enc = new TextEncoder()
const dec = new TextDecoder()

let passed = 0
function ok(name) { passed++; console.log(`ok - ${name}`) }

// 1) Constants are the expected mesh service/topic.
assert.equal(SERVICE, 'ce-worker')
assert.equal(RUN_TOPIC, 'ce-worker/run')
ok('mesh service/topic constants')

// 2) runJob executes a real WASM export and returns the right value + shape.
{
  const r = await runJob({ func: 'add', args: [40, 2], module_b64: MODULE_B64 })
  assert.equal(r.ok, true, `runJob failed: ${r.error}`)
  assert.equal(r.value, '42')
  assert.equal(typeof r.ms, 'number')
  ok('runJob computes add(40,2) = 42')
}

// 3) A missing export is reported as a clean job error, not a throw.
{
  const r = await runJob({ func: 'nope', args: [], module_b64: MODULE_B64 })
  assert.equal(r.ok, false)
  assert.match(r.error, /no export/)
  ok('runJob reports missing export as ok:false')
}

// 4) handleRun decodes a mesh request payload, runs it, and replies with JSON bytes.
{
  const job = { jid: 'j7', func: 'add', args: [1, 2], module_b64: MODULE_B64 }
  const reply = await handleRun({ from: 'a'.repeat(64), topic: RUN_TOPIC, payload: enc.encode(JSON.stringify(job)) })
  assert.ok(reply instanceof Uint8Array)
  const out = JSON.parse(dec.decode(reply))
  assert.equal(out.ok, true)
  assert.equal(out.value, '3')
  assert.equal(out.jid, 'j7')
  ok('handleRun replies with the JSON result over the mesh')
}

// 5) A malformed payload yields a clean error reply, never an unhandled throw.
{
  const reply = await handleRun({ from: 'b'.repeat(64), topic: RUN_TOPIC, payload: enc.encode('not json') })
  const out = JSON.parse(dec.decode(reply))
  assert.equal(out.ok, false)
  assert.match(out.error, /bad job payload/)
  ok('handleRun handles malformed payloads')
}

// 6) capabilityTags derives discoverable hardware tags from caps.
{
  const tags = capabilityTags(detectCaps())
  assert.ok(tags.some((t) => t.startsWith('cores:')))
  assert.ok(tags.some((t) => t.startsWith('ram:')))
  ok('capabilityTags advertises cores/ram tags')
}

console.log(`\n${passed} checks passed`)
