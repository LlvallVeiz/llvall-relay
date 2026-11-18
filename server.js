// Llvall stream relay — Render free Web Service (Node 18+, no deps).
// Ported from the Cloudflare Worker resolver (cloudflare-worker/worker.js),
// which ported the Vercel resolver (deploy/lvalmusic/api/_lib/resolve.js).
// Same InnerTube ladder, LEADS WITH VISIONOS (answered in ~1s in the
// sandbox while WEB_REMIX burned 3-5s cold and never returned a URL).
// Unlike the Worker, this server RELAYS audio bytes: the browser cannot
// stream IP-bound googlevideo URLs directly (round-3 finding), so
// /stream resolves server-side and pipes the bytes through, forwarding
// the client's Range header (206 + Content-Range/Content-Length/
// Content-Type). The Vercel /api/* handlers are thin proxies to this
// service; the player calls same-origin /api/resolve + /api/stream.
//
// Endpoints:
//   GET /resolve?videoId=<11-char id> -> {ok:true, url, itag, mimeType,
//       expiresAt, source} or {ok:false, reason}
//   GET /stream?videoId=<id>           -> audio bytes, Range forwarded
//   GET /health                        -> 200 {"ok":true} (keep-alive ping)
//   GET /                             -> 200 {"ok":true,"service":"llvall-relay"}
// CORS: allows https://lvalmusic.vercel.app. Listens on process.env.PORT.

'use strict';

const http = require('node:http');
const { Readable } = require('node:stream');

const CACHE_TTL_MS = 3.5 * 3600 * 1000; // 3.5h — URLs live ~6h, be conservative
const VISITOR_TTL_MS = 24 * 3600 * 1000; // visitorData cache 24h
const INNERTUBE_BUDGET_MS = 8000; // total InnerTube budget per resolve
const PER_CLIENT_TIMEOUT_MS = 3000;
const GLOBAL_BUDGET_MS = 9500; // hard stop per resolve (incl. fallbacks)
const RELAY_CONNECT_TIMEOUT_MS = 15000; // upstream connect/headers only (not body)

const ALLOWED_ORIGIN = 'https://lvalmusic.vercel.app';

const CHROME_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36';

// Client versions taken from yt-dlp master (yt_dlp/extractor/youtube/_base.py,
// INNERTUBE_CLIENTS table, fetched 2026-11-18). They rotate; update weekly.
// NOTE (yt-dlp comment, 2026-08-17): with ANDROID_VR 1.65.10 ALL formats
// including itag 18 have been observed 403'd. It stays in the ladder only
// because the report orders it before WEB_EMBEDDED_PLAYER, which is the
// reliable PO-token-exempt fallback.
const CLIENTS = [
  {
    key: 'VISIONOS',
    clientName: 'VISIONOS',
    clientVersion: '1.02',
    deviceMake: 'Apple',
    deviceModel: 'RealityDevice17,1',
    userAgent:
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 15_7_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15',
    osName: 'visionOS',
    osVersion: '26.5.23O471',
  },
  {
    key: 'WEB_REMIX',
    clientName: 'WEB_REMIX',
    clientVersion: '1.20260707.12.00',
    userAgent: CHROME_UA,
  },
  {
    key: 'ANDROID_VR',
    clientName: 'ANDROID_VR',
    clientVersion: '1.65.10',
    deviceMake: 'Oculus',
    deviceModel: 'Quest 3',
    androidSdkVersion: 32,
    userAgent:
      'com.google.android.apps.youtube.vr.oculus/1.65.10 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip',
    osName: 'Android',
    osVersion: '12L',
  },
  {
    key: 'WEB_EMBEDDED_PLAYER',
    clientName: 'WEB_EMBEDDED_PLAYER',
    clientVersion: '2.20260708.00.00',
    userAgent:
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15,gzip(gfe)',
  },
];

const FALLBACKS = [
  { name: 'piped:private.coffee', url: (id) => `https://api.piped.private.coffee/streams/${id}` },
  { name: 'piped:kavin.rocks', url: (id) => `https://pipedapi.kavin.rocks/streams/${id}` },
  { name: 'invidious:chocolatemoo53', url: (id) => `https://yt.chocolatemoo53.com/api/v1/videos/${id}` },
];

const cache = new Map(); // videoId -> { url, itag, mimeType, expiresAt, source }
let visitorData = null;
let visitorDataAt = 0;

function isValidVideoId(id) {
  return typeof id === 'string' && /^[A-Za-z0-9_-]{11}$/.test(id);
}

function computeExpiresAt(url) {
  const cap = Date.now() + CACHE_TTL_MS;
  try {
    const exp = new URL(url).searchParams.get('expire');
    if (exp) {
      const ms = Number(exp) * 1000;
      if (Number.isFinite(ms) && ms > Date.now()) return new Date(Math.min(ms, cap)).toISOString();
    }
  } catch (_) {
    /* ignore malformed URL */
  }
  return new Date(cap).toISOString();
}

function getCached(videoId) {
  const entry = cache.get(videoId);
  if (!entry) return null;
  if (Date.parse(entry.expiresAt) <= Date.now()) {
    cache.delete(videoId);
    return null;
  }
  return entry;
}

async function fetchVisitorData() {
  if (visitorData && Date.now() - visitorDataAt < VISITOR_TTL_MS) return visitorData;
  try {
    const res = await fetch('https://www.youtube.com/', {
      signal: AbortSignal.timeout(3000),
      headers: { 'User-Agent': CHROME_UA, 'Accept-Language': 'en-US,en;q=0.9' },
    });
    const html = await res.text();
    const m = html.match(/"visitorData":"([^"]+)"/);
    if (m) {
      visitorData = m[1];
      visitorDataAt = Date.now();
      return visitorData;
    }
  } catch (_) {
    /* proceed without visitorData — some clients still work */
  }
  return null;
}

function pickAudioFormat(formats) {
  if (!Array.isArray(formats)) return null;
  const audioOnly = formats.filter(
    (f) => f && typeof f.url === 'string' && f.url && typeof f.mimeType === 'string' && f.mimeType.startsWith('audio/')
  );
  // itag 251 (Opus/webm) first, then 140 (AAC/mp4), then any audio-only.
  for (const itag of [251, 140]) {
    const f = audioOnly.find((x) => x.itag === itag);
    if (f) return f;
  }
  if (audioOnly.length) {
    audioOnly.sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0));
    return audioOnly[0];
  }
  // Last resort: muxed itag 18 (video+audio) if it has a plain url.
  const muxed = formats.find((f) => f && f.itag === 18 && typeof f.url === 'string' && f.url);
  return muxed || null;
}

async function tryInnertubeClient(videoId, client, visitor, deadline) {
  const remaining = deadline - Date.now();
  if (remaining <= 0) return { ok: false, reason: 'budget-exhausted' };
  const timeout = Math.min(PER_CLIENT_TIMEOUT_MS, remaining);

  const clientCtx = { clientName: client.clientName, clientVersion: client.clientVersion, hl: 'en', gl: 'US' };
  if (visitor) clientCtx.visitorData = visitor;
  if (client.deviceMake) clientCtx.deviceMake = client.deviceMake;
  if (client.deviceModel) clientCtx.deviceModel = client.deviceModel;
  if (client.androidSdkVersion) clientCtx.androidSdkVersion = client.androidSdkVersion;
  if (client.osName) clientCtx.osName = client.osName;
  if (client.osVersion) clientCtx.osVersion = client.osVersion;

  let res;
  try {
    res = await fetch('https://youtubei.googleapis.com/youtubei/v1/player', {
      method: 'POST',
      signal: AbortSignal.timeout(timeout),
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': client.userAgent,
        'Accept-Language': 'en-US,en;q=0.9',
        Origin: 'https://www.youtube.com',
        Referer: 'https://www.youtube.com/',
      },
      body: JSON.stringify({
        context: { client: clientCtx },
        videoId,
        contentCheckOk: true,
        racyCheckOk: true,
      }),
    });
  } catch (err) {
    return { ok: false, reason: 'fetch-error', detail: String(err && err.name ? err.name : err) };
  }

  if (!res.ok) return { ok: false, reason: 'http-' + res.status };

  let data;
  try {
    data = await res.json();
  } catch (_) {
    return { ok: false, reason: 'bad-json' };
  }

  const status = data && data.playabilityStatus && data.playabilityStatus.status;
  const reasonText =
    (data && data.playabilityStatus && (data.playabilityStatus.reason || '')) || '';
  if (status === 'LOGIN_REQUIRED' && /not a bot/i.test(reasonText)) {
    return { ok: false, reason: 'bot-challenge', detail: reasonText };
  }
  if (status !== 'OK' && !(status === 'UNPLAYABLE' && data.streamingData)) {
    return { ok: false, reason: 'playability-' + (status || 'unknown'), detail: reasonText };
  }

  const formats = data.streamingData && data.streamingData.adaptiveFormats;
  const fmt = pickAudioFormat(formats);
  if (!fmt) {
    // SABR-only (no plain url) or signatureCipher-only: skip, do NOT decipher.
    return { ok: false, reason: 'no-plain-url' };
  }

  return {
    ok: true,
    url: fmt.url,
    itag: fmt.itag,
    mimeType: fmt.mimeType,
    expiresAt: computeExpiresAt(fmt.url),
    source: 'innertube:' + client.key,
  };
}

async function tryPiped(videoId, fallback, deadline) {
  const remaining = deadline - Date.now();
  if (remaining <= 0) return { ok: false, reason: 'budget-exhausted' };
  try {
    const res = await fetch(fallback.url(videoId), {
      signal: AbortSignal.timeout(Math.min(3000, remaining)),
      headers: { 'User-Agent': CHROME_UA, Accept: 'application/json' },
    });
    if (!res.ok) return { ok: false, reason: 'http-' + res.status };
    const data = await res.json();
    const streams = Array.isArray(data.audioStreams) ? data.audioStreams : [];
    const withUrl = streams.filter((s) => s && typeof s.url === 'string' && s.url);
    if (!withUrl.length) return { ok: false, reason: 'no-audio-streams' };
    withUrl.sort((a, b) => {
      const score = (s) => (/opus/i.test(s.mimeType || '') ? 3 : /aac|mp4/i.test(s.mimeType || '') ? 2 : 1);
      return score(b) - score(a) || (b.bitrate || 0) - (a.bitrate || 0);
    });
    const best = withUrl[0];
    return {
      ok: true,
      url: best.url,
      itag: best.itag || null,
      mimeType: best.mimeType || null,
      expiresAt: computeExpiresAt(best.url),
      source: fallback.name,
    };
  } catch (err) {
    return { ok: false, reason: 'fetch-error', detail: String(err && err.name ? err.name : err) };
  }
}

async function tryInvidious(videoId, fallback, deadline) {
  const remaining = deadline - Date.now();
  if (remaining <= 0) return { ok: false, reason: 'budget-exhausted' };
  try {
    const res = await fetch(fallback.url(videoId), {
      signal: AbortSignal.timeout(Math.min(3000, remaining)),
      headers: { 'User-Agent': CHROME_UA, Accept: 'application/json' },
    });
    if (!res.ok) return { ok: false, reason: 'http-' + res.status };
    const data = await res.json();
    const formats = Array.isArray(data.adaptiveFormats) ? data.adaptiveFormats : [];
    const fmt = pickAudioFormat(formats);
    if (!fmt) return { ok: false, reason: 'no-plain-url' };
    return {
      ok: true,
      url: fmt.url,
      itag: fmt.itag,
      mimeType: fmt.mimeType,
      expiresAt: computeExpiresAt(fmt.url),
      source: fallback.name,
    };
  } catch (err) {
    return { ok: false, reason: 'fetch-error', detail: String(err && err.name ? err.name : err) };
  }
}

// Resolve a videoId to a playable audio URL.
// Returns { ok:true, url, itag, mimeType, expiresAt, source, cached }
// or { ok:false, reason } ('bot-challenge' | 'no-stream' | 'invalid-videoId').
async function resolve(videoId, opts) {
  const forceFresh = !!(opts && opts.forceFresh);
  if (!isValidVideoId(videoId)) return { ok: false, reason: 'invalid-videoId' };

  if (!forceFresh) {
    const hit = getCached(videoId);
    if (hit) return { ok: true, ...hit, cached: true };
  }

  const startedAt = Date.now();
  const globalDeadline = startedAt + GLOBAL_BUDGET_MS;
  const innertubeDeadline = startedAt + INNERTUBE_BUDGET_MS;

  const visitor = await fetchVisitorData();

  let sawBotChallenge = false;
  for (const client of CLIENTS) {
    if (Date.now() >= innertubeDeadline) break;
    const r = await tryInnertubeClient(videoId, client, visitor, innertubeDeadline);
    if (r.ok) {
      const entry = { url: r.url, itag: r.itag, mimeType: r.mimeType, expiresAt: r.expiresAt, source: r.source };
      cache.set(videoId, entry);
      return { ok: true, ...entry, cached: false };
    }
    if (r.reason === 'bot-challenge') sawBotChallenge = true;
  }

  for (const fb of FALLBACKS) {
    if (Date.now() >= globalDeadline) break;
    const r = fb.name.startsWith('invidious')
      ? await tryInvidious(videoId, fb, globalDeadline)
      : await tryPiped(videoId, fb, globalDeadline);
    if (r.ok) {
      const entry = { url: r.url, itag: r.itag, mimeType: r.mimeType, expiresAt: r.expiresAt, source: r.source };
      cache.set(videoId, entry);
      return { ok: true, ...entry, cached: false };
    }
  }

  return { ok: false, reason: sawBotChallenge ? 'bot-challenge' : 'no-stream' };
}

// --- HTTP layer ---

const HOP_HEADERS = ['content-range', 'content-length', 'content-type', 'accept-ranges'];

function setCors(req, res) {
  const origin = req.headers.origin;
  if (origin === ALLOWED_ORIGIN) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(body);
}

function log(endpoint, videoId, fields) {
  // Never log the googlevideo URL.
  console.log(JSON.stringify({ endpoint, videoId, ...fields, ts: new Date().toISOString() }));
}

async function handleResolve(req, res, videoId) {
  const startedAt = Date.now();
  const result = await resolve(videoId);
  if (!result.ok) {
    log('resolve', videoId, { ok: false, reason: result.reason, ms: Date.now() - startedAt });
    sendJson(res, 200, { ok: false, reason: result.reason });
    return;
  }
  log('resolve', videoId, {
    ok: true,
    source: result.source,
    itag: result.itag,
    cached: !!result.cached,
    ms: Date.now() - startedAt,
  });
  sendJson(res, 200, {
    ok: true,
    url: result.url,
    itag: result.itag,
    mimeType: result.mimeType,
    expiresAt: result.expiresAt,
    source: result.source,
  });
}

async function handleStream(req, res, videoId) {
  const startedAt = Date.now();
  const range = req.headers.range || 'bytes=0-';

  // The relay timeout applies ONLY until upstream headers arrive; the body
  // then streams without an abort timer (a full song can take longer than
  // any fixed timeout to download). If the client disconnects, cancel upstream.
  const relayOnce = async (resolved) => {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), RELAY_CONNECT_TIMEOUT_MS);
    let upstream;
    try {
      upstream = await fetch(resolved.url, {
        signal: ctrl.signal,
        headers: { Range: range, 'User-Agent': CHROME_UA },
      });
    } finally {
      clearTimeout(timer);
    }
    if (!upstream.ok) return { ok: false, status: upstream.status };
    const status = range && upstream.status === 206 ? 206 : upstream.status;
    res.writeHead(status, {
      'Cache-Control': 'no-store',
      ...(Object.fromEntries(
        HOP_HEADERS.map((h) => [h, upstream.headers.get(h)]).filter(([, v]) => v)
      )),
      ...(!upstream.headers.get('accept-ranges') ? { 'Accept-Ranges': 'bytes' } : {}),
    });
    const bytes = Number(upstream.headers.get('content-length') || 0);
    await new Promise((done, fail) => {
      const nodeStream = Readable.fromWeb(upstream.body);
      nodeStream.on('error', fail);
      res.on('error', fail);
      res.on('close', () => {
        // Client went away mid-stream (normal for Range requests: seek,
        // pause, element teardown). Destroy the pipe; do NOT call
        // upstream.body.cancel() — fromWeb() locks the stream and cancel()
        // on a locked stream throws and kills the process.
        nodeStream.destroy();
        done();
      });
      nodeStream.pipe(res);
      res.on('finish', done);
    });
    return { ok: true, status, bytes };
  };

  try {
    let resolved = await resolve(videoId);
    if (!resolved.ok) {
      log('stream', videoId, { ok: false, reason: resolved.reason, ms: Date.now() - startedAt });
      if (!res.headersSent) sendJson(res, 502, { ok: false, reason: resolved.reason });
      return;
    }

    let r = await relayOnce(resolved);
    if (!r.ok) {
      // Upstream rejected the URL (expired / IP-bound / 403): re-resolve
      // once bypassing the cache, then retry once.
      const fresh = await resolve(videoId, { forceFresh: true });
      if (!fresh.ok) {
        log('stream', videoId, { ok: false, reason: fresh.reason, ms: Date.now() - startedAt });
        if (!res.headersSent) sendJson(res, 502, { ok: false, reason: fresh.reason });
        return;
      }
      r = await relayOnce(fresh);
      if (!r.ok) {
        log('stream', videoId, { ok: false, reason: 'upstream-' + r.status, ms: Date.now() - startedAt });
        if (!res.headersSent) sendJson(res, 502, { ok: false, reason: 'upstream-' + r.status });
        return;
      }
    }

    log('stream', videoId, { ok: true, status: r.status, bytes: r.bytes, ms: Date.now() - startedAt });
  } catch (err) {
    log('stream', videoId, { ok: false, reason: 'internal-error', ms: Date.now() - startedAt });
    if (!res.headersSent) sendJson(res, 502, { ok: false, reason: 'no-stream' });
  }
}

const server = http.createServer(async (req, res) => {
  setCors(req, res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  const url = new URL(req.url, 'http://localhost');
  const videoId = url.searchParams.get('videoId');

  try {
    if (req.method === 'GET' && url.pathname === '/health') {
      sendJson(res, 200, { ok: true });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/') {
      sendJson(res, 200, { ok: true, service: 'llvall-relay' });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/resolve') {
      if (!videoId) {
        sendJson(res, 400, { ok: false, reason: 'invalid-videoId' });
        return;
      }
      await handleResolve(req, res, videoId);
      return;
    }
    if (req.method === 'GET' && url.pathname === '/stream') {
      if (!videoId) {
        sendJson(res, 400, { ok: false, reason: 'invalid-videoId' });
        return;
      }
      await handleStream(req, res, videoId);
      return;
    }
    sendJson(res, 404, { ok: false, reason: 'not-found' });
  } catch (err) {
    console.error(JSON.stringify({ endpoint: 'server', error: String(err && err.stack || err) }));
    if (!res.headersSent) sendJson(res, 500, { ok: false, reason: 'internal-error' });
  }
});

const PORT = Number(process.env.PORT || 10000);
server.listen(PORT, '0.0.0.0', () => {
  console.log(JSON.stringify({ service: 'llvall-relay', listening: true, port: PORT }));
});
