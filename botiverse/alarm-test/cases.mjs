#!/usr/bin/env node
// Cluster-mode alarm cases on one machine. Each case starts its own fresh cluster of local processes
// (ports 8900+, shared directory under /tmp), so cases don't share state. Exit code 1 if any case fails.
//   node botiverse/alarm-test/cases.mjs [case ...]      WORKERD=<binary> to use another build
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const WORKERD = process.env.WORKERD ?? join(here, "../../bazel-bin/src/workerd/server/workerd");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function cluster(n) {
  const dir = mkdtempSync(join(tmpdir(), "alarm-test-"));
  const nodes = [];
  for (let i = 0; i < n; i++) {
    const log = [];
    const child = spawn(WORKERD, ["serve", "--experimental", "--verbose", join(here, "config.capnp"),
      `--directory-path=do-storage=${dir}`, `--socket-addr=http=127.0.0.1:${8900 + i}`],
      { env: { ...process.env, NODE: `n${i}` }, stdio: ["ignore", "ignore", "pipe"] });
    child.stderr.on("data", (d) => log.push(String(d)));
    nodes.push({ i, port: 8900 + i, child, log, alive: true });
  }
  // up and listening
  for (const nd of nodes) for (let t = 0; t < 50; t++) { try { await fetch(`http://127.0.0.1:${nd.port}/state?do=__ping`); break; } catch { await sleep(100); } }
  await sleep(1500); // a leader is elected
  const api = {
    nodes, dir,
    // Retries: right after a holder dies, the first request routed to its objects fails while the
    // cluster notices (upstream behaviour, also retried by upstream's takeover test).
    get: async (nd, path) => {
      for (let t = 0; ; t++) {
        try { const r = await fetch(`http://127.0.0.1:${nd.port}/${path}`); if (r.status < 500 || t >= 10) return await r.json(); }
        catch (e) { if (t >= 10) throw e; }
        await sleep(300);
      }
    },
    kill: (nd) => { nd.child.kill("SIGKILL"); nd.alive = false; },
    log: () => nodes.map((nd) => `--- n${nd.i}\n` + nd.log.join("").split("\n").filter((l) => /warning|error|leader/i.test(l) && !/symbolizer/.test(l)).slice(-8).join("\n")).join("\n"),
    stop: () => { for (const nd of nodes) if (nd.alive) nd.child.kill("SIGKILL"); rmSync(dir, { recursive: true, force: true }); },
  };
  return api;
}
// The process that most recently logged becoming leader and is still alive.
function leaderOf(c) {
  let best = null, bestAt = -1;
  for (const nd of c.nodes) {
    if (!nd.alive) continue;
    const text = nd.log.join(""), at = text.lastIndexOf("now the alarm leader");
    if (at >= 0 && (best === null || at > bestAt)) { best = nd; bestAt = at; }
  }
  return best;
}
const others = (c, ex) => c.nodes.filter((nd) => nd.alive && nd !== ex);
async function waitFired(c, nd, name, want, ms) {
  const end = Date.now() + ms; let s;
  while (Date.now() < end) { s = await c.get(nd, `state?do=${name}`); if (s.fired.length >= want) break; await sleep(200); }
  return s;
}

const CASES = {
  async "moved several times: runs once, at the latest time"(c) {
    const L = leaderOf(c), [a, b] = others(c, L);
    await c.get(a, "set?do=m&ms=10000"); await c.get(b, "set?do=m&ms=1000"); const last = await c.get(a, "set?do=m&ms=3000");
    await sleep(1800);
    const early = await c.get(b, "state?do=m");
    if (early.fired.length) return `ran at a superseded time: ${JSON.stringify(early.fired)}`;
    const s = await waitFired(c, b, "m", 1, 6000); await sleep(1500);
    const f = (await c.get(b, "state?do=m")).fired;
    if (f.length !== 1) return `fired ${f.length} times`;
    if (f[0].at < last.set) return `ran before the latest time (${f[0].at} < ${last.set})`;
  },
  async "handler throws twice: retried with backoff, then succeeds"(c) {
    const L = leaderOf(c), [a] = others(c, L);
    await c.get(a, "throw?do=t&n=2"); await c.get(a, "set?do=t&ms=300");
    const s = await waitFired(c, a, "t", 3, 15000);
    if (s.fired.length !== 3) return `fired ${s.fired.length} times: ${JSON.stringify(s.fired)}`;
    if (!s.fired[1].isRetry || !s.fired[2].isRetry) return `retries not marked: ${JSON.stringify(s.fired)}`;
  },
  async "handler re-arms its own alarm (setAlarm inside alarm())"(c) {
    const L = leaderOf(c), [a, b] = others(c, L);
    await c.get(a, "set?do=r&ms=500&rearm=2");
    const s = await waitFired(c, b, "r", 3, 12000); await sleep(2500);
    const f = (await c.get(b, "state?do=r"));
    if (f.fired.length !== 3) return `want 3 runs (1 + 2 re-arms), got ${f.fired.length}`;
    if (f.alarm !== null) return `alarm still set after the last run: ${f.alarm}`;
  },
  async "deleteAlarm before it is due: never runs"(c) {
    const L = leaderOf(c), [a, b] = others(c, L);
    await c.get(a, "set?do=d&ms=1500"); await c.get(b, "del?do=d");
    await sleep(4000);
    const f = (await c.get(a, "state?do=d")).fired;
    if (f.length) return `ran after deleteAlarm: ${JSON.stringify(f)}`;
  },
  async "holder killed before the alarm is due: leader claims the object, runs once"(c) {
    const L = leaderOf(c), [a] = others(c, L);
    await c.get(a, "set?do=k&ms=2000"); c.kill(a);
    const s = await waitFired(c, L, "k", 1, 15000); await sleep(2500);
    const f = (await c.get(L, "state?do=k")).fired;
    if (f.length !== 1) return `fired ${f.length} times: ${JSON.stringify(f)}`;
  },
  async "holder killed in the middle of the handler: re-run elsewhere, recorded once"(c) {
    const L = leaderOf(c), [a] = others(c, L);
    await c.get(a, "set?do=h&ms=500&slow=3000");
    await sleep(1500); c.kill(a); // a is inside alarm(), before it records anything
    const s = await waitFired(c, L, "h", 1, 20000); await sleep(2500);
    const f = (await c.get(L, "state?do=h")).fired;
    if (f.length !== 1) return `recorded ${f.length} runs: ${JSON.stringify(f)}`;
  },
  async "leader killed while it waits for a running handler: runs once"(c) {
    const L = leaderOf(c), [a, b] = others(c, L);
    await c.get(a, "set?do=l&ms=500&slow=3000");
    await sleep(1500); c.kill(L); // the delivery is in flight; a keeps running the handler
    const s = await waitFired(c, b, "l", 1, 15000); await sleep(3000);
    const f = (await c.get(b, "state?do=l")).fired;
    if (f.length !== 1) return `ran ${f.length} times: ${JSON.stringify(f)}`;
    if (!leaderOf(c)) return "no new leader";
  },
  async "leader killed with an alarm pending: another takes over, runs once"(c) {
    const L = leaderOf(c), [a, b] = others(c, L);
    await c.get(b, "set?do=p&ms=3000"); c.kill(L);
    const s = await waitFired(c, b, "p", 1, 12000); await sleep(2500);
    const f = (await c.get(b, "state?do=p")).fired;
    if (f.length !== 1) return `fired ${f.length} times`;
  },
  async "300 alarms due at the same time: each runs exactly once"(c) {
    const live = others(c, null), N = 300;
    await Promise.all(Array.from({ length: N }, (_, k) => c.get(live[k % live.length], `set?do=many${k}&ms=2500`)));
    await sleep(9000);
    const counts = await Promise.all(Array.from({ length: N }, (_, k) => c.get(live[(k + 1) % live.length], `state?do=many${k}`).then((s) => s.fired.length)));
    const bad = counts.map((n, k) => [k, n]).filter(([, n]) => n !== 1);
    if (bad.length) return `${bad.length}/${N} did not run exactly once (e.g. ${JSON.stringify(bad.slice(0, 5))})`;
  },
  async "all processes set 600 alarms at once: no SQLITE_BUSY errors, all run"(c) {
    const live = others(c, null), N = 600;
    const res = await Promise.all(Array.from({ length: N }, (_, k) =>
      fetch(`http://127.0.0.1:${live[k % live.length].port}/set?do=busy${k}&ms=3000`).then((r) => r.status)));
    const failed = res.filter((s) => s !== 200).length;
    await sleep(10000);
    const counts = await Promise.all(Array.from({ length: N }, (_, k) => c.get(live[k % live.length], `state?do=busy${k}`).then((s) => s.fired.length)));
    const bad = counts.filter((n) => n !== 1).length;
    const busy = c.nodes.some((nd) => nd.log.join("").includes("SQLITE_BUSY"));
    if (failed || bad || busy) return `setAlarm failed ${failed}/${N}, not run exactly once ${bad}/${N}, SQLITE_BUSY in logs: ${busy}`;
  },
};

const only = process.argv.slice(2);
let failures = 0;
for (const [name, fn] of Object.entries(CASES)) {
  if (only.length && !only.some((o) => name.includes(o))) continue;
  const c = await cluster(3);
  let err;
  try {
    if (!leaderOf(c)) err = "no alarm leader elected";
    else err = await fn(c);
  } catch (e) { err = String(e?.stack ?? e); }
  if (err) { failures++; console.log(`FAIL ${name}: ${err}\n${c.log()}`); } else console.log(`PASS ${name}`);
  c.stop();
  await sleep(500);
}
console.log(failures ? `${failures} failed` : "all passed");
process.exit(failures ? 1 : 0);
