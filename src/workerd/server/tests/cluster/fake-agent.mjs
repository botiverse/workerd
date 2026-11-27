// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
//
// A fake external liveness provider (`ClusterConfig.liveness`,
// src/workerd/server/cluster/agent.rs) for cluster-test.mjs: one per
// "machine", each serving `liveness.sock` in a directory of its own, sharing
// one world, so that each sees every machine's instances as a provider that
// reads every machine's lease in the bucket would. Leases are kept in memory,
// not in the bucket; the test says when a process exited (`exited`), which a
// real provider sees for itself.

import { createServer } from 'node:http';
import { join } from 'node:path';
import { rm } from 'node:fs/promises';

export class FakeAgents {
  // machine -> { expiresMs, up, instances: Map<id, { address, draining }> }
  machines = new Map();
  servers = new Map();
  // Every call as `{ machine, path, body }`.
  calls = [];

  // `validForMs`: each grant; `leaseMs`: a machine lease's time.
  constructor({ validForMs, leaseMs }) {
    this.validForMs = validForMs;
    this.leaseMs = leaseMs;
  }

  // Starts machine `machine`'s provider in `dir`, keeping what it held.
  async start(machine, dir) {
    let m = this.machines.get(machine);
    if (m === undefined) {
      m = { up: true, expiresMs: 0, instances: new Map() };
      this.machines.set(machine, m);
    }
    m.up = true;
    const path = join(dir, 'liveness.sock');
    await rm(path, { force: true });
    const server = createServer((req, res) => {
      let text = '';
      req.on('data', (c) => (text += c));
      req.on('end', () => {
        let body = {};
        try {
          body = text ? JSON.parse(text) : {};
        } catch {}
        this.calls.push({ machine, path: req.url, body });
        const [status, answer] = this.#answer(machine, req.url, body);
        const out = JSON.stringify(answer);
        res.writeHead(status, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(out),
        });
        res.end(out);
      });
    });
    const sockets = new Set();
    server.on('connection', (s) => {
      sockets.add(s);
      s.on('close', () => sockets.delete(s));
    });
    await new Promise((resolve) => server.listen(path, resolve));
    this.servers.set(machine, { server, sockets });
    return path;
  }

  // The provider of `machine` dies: its socket and connections close. Its
  // machine's lease runs out unless it starts again.
  async kill(machine) {
    const s = this.servers.get(machine);
    if (s === undefined) return;
    this.servers.delete(machine);
    for (const socket of s.sockets) socket.destroy();
    await new Promise((resolve) => s.server.close(resolve));
    this.machines.get(machine).up = false;
  }

  // Instance `id`'s process exited: its machine's lease drops it.
  exited(id) {
    for (const m of this.machines.values()) m.instances.delete(id);
  }

  // The lease of `machine`: renewed while its provider is up.
  #expires(m) {
    if (m.up) m.expiresMs = Date.now() + this.leaseMs;
    return m.expiresMs;
  }

  #view() {
    const instances = [];
    for (const [machine, m] of this.machines) {
      const expiresMs = this.#expires(m);
      for (const [instance, i] of m.instances) {
        instances.push({ instance, machine, expiresMs, ...i });
      }
    }
    return instances;
  }

  #answer(machine, url, body) {
    const m = this.machines.get(machine);
    switch (url) {
      case '/v1/register':
        m.instances.set(body.instance, {
          address: body.address,
          draining: !!body.draining,
        });
        return [200, { validForMs: this.validForMs, machine }];
      case '/v1/heartbeat': {
        const i = m.instances.get(body.instance);
        if (i === undefined) return [404, { error: 'no such instance' }];
        i.draining = !!body.draining;
        return [200, { validForMs: this.validForMs }];
      }
      case '/v1/deregister':
        m.instances.delete(body.instance);
        return [200, {}];
      case '/v1/watch':
        return [200, { ageMs: 0, instances: this.#view() }];
      case '/v1/probe':
        return [
          200,
          {
            ageMs: 0,
            instance:
              this.#view().find((i) => i.instance === body.instance) ?? null,
          },
        ];
      default:
        return [404, { error: 'no such call' }];
    }
  }

  async close() {
    for (const machine of [...this.servers.keys()]) await this.kill(machine);
  }
}
