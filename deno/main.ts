// Petuh vs Kot — reachable front for the room server.
//
// Russian ISPs kill TLS to *.workers.dev, so players connect here (deno.net) and this app pipes every
// WebSocket to the Cloudflare Worker + Durable Object in ../src/index.js, where each room lives exactly
// once — no matter how many Deno isolates/regions serve the players. Same URL scheme as the Worker:
//
//   GET  /turn                      -> { iceServers, turn } (Cloudflare TURN creds if TURN_KEY_ID/TURN_TOKEN are set, else STUN)
//   GET  /room/<code>/check         -> proxied to the Worker
//   WS   /room/<code>?role=host|guest -> piped to the Worker (close codes 4404/4409 for missing/full rooms)
//   GET  /health                    -> diagnostics
//   GET/POST /podval/board/<day>    -> proxied to the ПОДВАЛ leaderboard Worker (same reason: workers.dev is unreachable from RU)

const UPSTREAM = Deno.env.get("UPSTREAM") || "https://petuh-relay.ifonpromax.workers.dev";
const BOARD_UPSTREAM = Deno.env.get("BOARD_UPSTREAM") || "https://podval-board.ifonpromax.workers.dev";
const CODE_RE = /^[a-z0-9]{5}$/;
const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "*" };
const STUN = [{ urls: ["stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"] }];
const json = (o: unknown, status = 200) =>
  new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...CORS } });
const ISO_ID = Math.random().toString(36).slice(2, 8);
let pipes = 0;

async function turnCreds() {
  const id = Deno.env.get("TURN_KEY_ID"), token = Deno.env.get("TURN_TOKEN");
  if (!id || !token) return { iceServers: STUN, turn: false };
  try {
    const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${id}/credentials/generate-ice-servers`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ ttl: 3 * 3600 }),
    });
    if (!r.ok) throw new Error("turn api " + r.status);
    const j = await r.json();
    const servers = Array.isArray(j.iceServers) ? j.iceServers : [j.iceServers];
    return { iceServers: [...STUN, ...servers], turn: true };
  } catch (e) {
    return { iceServers: STUN, turn: false, error: String((e as Error)?.message || e) };
  }
}

async function roomCheck(code: string): Promise<{ host: boolean; guest: boolean } | null> {
  try {
    const r = await fetch(`${UPSTREAM}/room/${code}/check`);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
  if (url.pathname === "/health") {
    const up = await roomCheck("zzzzz");
    return json({ iso: ISO_ID, region: Deno.env.get("DENO_REGION") || null, pipes, upstream: !!up });
  }
  if (url.pathname === "/turn") return json(await turnCreds());

  // ПОДВАЛ daily leaderboard: plain HTTP pass-through to its own Worker
  const bm = url.pathname.match(/^\/podval\/board\/(\d{8})$/);
  if (bm) {
    if (req.method !== "GET" && req.method !== "POST") return json({ error: "method" }, 405);
    try {
      const body = req.method === "POST" ? await req.text() : undefined;
      if (body && body.length > 2048) return json({ error: "too big" }, 413);
      const r = await fetch(`${BOARD_UPSTREAM}/board/${bm[1]}`, { method: req.method, headers: { "content-type": "application/json" }, body });
      return new Response(await r.text(), { status: r.status, headers: { "content-type": "application/json", ...CORS } });
    } catch (e) {
      return json({ error: "upstream", detail: String((e as Error)?.message || e) }, 502);
    }
  }

  const m = url.pathname.match(/^\/room\/([a-z0-9]{5})(\/check)?$/);
  if (!m || !CODE_RE.test(m[1])) return new Response("petuh-vs-kot relay: ok", { headers: { "content-type": "text/plain", ...CORS } });
  const code = m[1];
  if (m[2]) { const p = await roomCheck(code); return p ? json(p) : json({ error: "upstream" }, 502); }

  if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("expected websocket", { status: 426, headers: CORS });
  const role = url.searchParams.get("role");
  if (role !== "host" && role !== "guest") return new Response("bad role", { status: 400 });

  // room rules are checked here so the client gets a clear close code instead of a failed handshake
  const p = await roomCheck(code);
  const reject = !p ? 4502 : (p.host && role === "host") ? 4409 : (!p.host && role === "guest") ? 4404 : (p.guest && role === "guest") ? 4409 : 0;

  const { socket: client, response } = Deno.upgradeWebSocket(req);
  if (reject) {
    client.onopen = () => { try { client.close(reject, reject === 4404 ? "no such room" : reject === 4409 ? "room taken" : "upstream down"); } catch { /* ignore */ } };
    return response;
  }

  // pipe client <-> upstream Durable Object
  const up = new WebSocket(`${UPSTREAM.replace(/^http/, "ws")}/room/${code}?role=${role}`);
  const queue: string[] = [];
  let upOpen = false, done = false;
  const finish = (code = 1000, reason = "") => {
    if (done) return; done = true; pipes--;
    try { client.close(code, reason); } catch { /* ignore */ }
    try { up.close(); } catch { /* ignore */ }
  };
  pipes++;
  up.onopen = () => { upOpen = true; for (const s of queue.splice(0)) up.send(s); };
  up.onmessage = (e) => { if (typeof e.data === "string") { try { client.send(e.data); } catch { finish(); } } };
  up.onclose = (e) => finish(e.code >= 4000 ? e.code : 1000, e.reason || "upstream closed");
  up.onerror = () => finish(4502, "upstream error");
  client.onmessage = (e) => {
    if (typeof e.data !== "string" || e.data.length > 16384) return;
    if (upOpen) { try { up.send(e.data); } catch { finish(); } } else queue.push(e.data);
  };
  client.onclose = () => finish();
  client.onerror = () => finish();
  return response;
});
