// Alarm semantics test for cluster-mode workerd. Every process runs this same config; NODE (from the
// environment) says which process handled what.
import { DurableObject } from "cloudflare:workers";

export class A extends DurableObject {
  async fetch(req) {
    const u = new URL(req.url), s = this.ctx.storage, p = (k) => u.searchParams.get(k);
    if (u.pathname === "/set") {
      // rearm=N: the handler sets the next alarm itself, N more times; slow=ms: the handler takes that long
      // (before it records anything); del=1: the handler deletes its alarm instead of letting it end.
      if (p("rearm")) await s.put("rearm", +p("rearm"));
      if (p("slow")) await s.put("slow", +p("slow"));
      const at = Date.now() + +p("ms");
      await s.setAlarm(at);
      return Response.json({ node: this.env.NODE, set: at });
    }
    if (u.pathname === "/del") { await s.deleteAlarm(); return Response.json({ node: this.env.NODE, deleted: true }); }
    if (u.pathname === "/throw") { await s.put("throws", +p("n")); return Response.json({ node: this.env.NODE }); }
    if (u.pathname === "/state") return Response.json({ node: this.env.NODE, alarm: await s.getAlarm(), fired: (await s.get("fired")) ?? [] });
    return new Response("?", { status: 404 });
  }
  async alarm(info) {
    const s = this.ctx.storage;
    const slow = (await s.get("slow")) ?? 0;
    if (slow) await new Promise((r) => setTimeout(r, slow));
    const fired = (await s.get("fired")) ?? [], throws = (await s.get("throws")) ?? 0, rearm = (await s.get("rearm")) ?? 0;
    fired.push({ node: this.env.NODE, at: Date.now(), retry: info?.retryCount ?? 0, isRetry: !!info?.isRetry });
    await s.put("fired", fired);
    if (throws > 0) { await s.put("throws", throws - 1); throw new Error("planned failure"); }
    if (rearm > 0) { await s.put("rearm", rearm - 1); await s.setAlarm(Date.now() + 1000); }
  }
}
export default {
  async fetch(req, env) {
    const u = new URL(req.url), name = u.searchParams.get("do") ?? "x";
    return env.A.get(env.A.idFromName(name)).fetch(req);
  },
};
