// Alarm semantics test for cluster-mode workerd. Every process runs this same config; NODE (from the
// environment) says which process handled what.
import { DurableObject } from "cloudflare:workers";

export class A extends DurableObject {
  async fetch(req) {
    const u = new URL(req.url), s = this.ctx.storage;
    if (u.pathname === "/set") { const at = Date.now() + +u.searchParams.get("ms"); await s.setAlarm(at); return Response.json({ node: this.env.NODE, set: at }); }
    if (u.pathname === "/del") { await s.deleteAlarm(); return Response.json({ node: this.env.NODE, deleted: true }); }
    if (u.pathname === "/throw") { await s.put("throws", +u.searchParams.get("n")); return Response.json({ node: this.env.NODE }); }
    if (u.pathname === "/state") return Response.json({ node: this.env.NODE, alarm: await s.getAlarm(), fired: (await s.get("fired")) ?? [] });
    return new Response("?", { status: 404 });
  }
  async alarm(info) {
    const s = this.ctx.storage, fired = (await s.get("fired")) ?? [], throws = (await s.get("throws")) ?? 0;
    fired.push({ node: this.env.NODE, at: Date.now(), retry: info?.retryCount ?? 0, isRetry: !!info?.isRetry });
    await s.put("fired", fired);
    if (throws > 0) { await s.put("throws", throws - 1); throw new Error("planned failure"); }
  }
}
export default {
  async fetch(req, env) {
    const u = new URL(req.url), name = u.searchParams.get("do") ?? "x";
    return env.A.get(env.A.idFromName(name)).fetch(req);
  },
};
