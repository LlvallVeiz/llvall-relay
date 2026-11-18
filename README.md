# llvall-relay

Stream relay for the Llvall web player (https://lvalmusic.vercel.app).

Web playback needs a server that resolves **and** relays audio: direct
googlevideo URLs are IP-bound (the browser's IP is not the resolving
server's IP, so they 403), and Cloudflare Workers cannot relay bytes.

- `GET /resolve?videoId=<id>` → `{ok:true, url, itag, mimeType, expiresAt, source}` or `{ok:false, reason}`
- `GET /stream?videoId=<id>` → relays audio bytes, forwards the client's `Range` header (206 + `Content-Range` / `Content-Length` / `Content-Type`)
- `GET /health` → `200 {"ok":true}` (keep-alive ping target)

InnerTube ladder (VISIONOS first), itag 251 → 140 → 18, plain `url` only.
Piped/Invidious fallback if all InnerTube clients fail. No dependencies —
pure Node built-ins.

## Deploy (Render, free)

1. Create a Web Service from this repo (or use `render.yaml`):
   - Environment: Node, Plan: Free, Region: Singapore
   - Build command: `npm install`
   - Start command: `node server.js`
2. Note the service URL, e.g. `https://llvall-relay.onrender.com`.
3. Point the Vercel `/api/resolve` and `/api/stream` handlers at it
   (thin proxies in `~/workspace/llvall-android/deploy/lvalmusic/api/`).
4. Keep-alive: an UptimeRobot monitor (free, 5 min) pinging `/health`
   prevents the free tier's 15-minute sleep. Free tier budget: 750
   instance-hours/month; a 24/7 service uses ~720h — do not add other
   services to the same Render account.
