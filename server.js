/*
 * Robot relay server
 * ----------------------------------------------------------------------------
 * Purpose: the ESP32-CAM's own MJPEG stream only works on its local Wi-Fi.
 * This server sits in the cloud so the Android app can see the camera feed
 * from anywhere: the ESP32 POSTs a JPEG every ~300ms, the app fetches the
 * latest one over HTTP (simple polling) or subscribes over WebSocket (pushed
 * as soon as it arrives -- lower latency, preferred for the app).
 *
 * Deploy target: any Node host (Render, Railway, Fly.io free tiers all work).
 * Start command: npm install && npm start
 * Required env var: RELAY_AUTH_TOKEN  (must match the token you set in the
 * ESP32's WiFiManager config portal, and in the Android app's config)
 *
 * Endpoints:
 *   POST /frame                       ESP32 -> server   (auth required)
 *     headers: X-Auth-Token, X-Device-Id
 *     body   : raw JPEG bytes (Content-Type: image/jpeg)
 *
 *   GET  /frame/:deviceId              server -> app     (polling)
 *     returns the latest JPEG bytes, or 404 if nothing received yet /
 *     the frame is older than STALE_MS (robot likely offline)
 *
 *   GET  /frame/:deviceId/meta         server -> app
 *     returns { ageMs, online }
 *
 *   WS   /ws?device=<id>&token=<tok>   server -> app     (push, lower latency)
 *     server sends a binary WebSocket message containing the raw JPEG
 *     every time a new frame arrives for that device
 * ----------------------------------------------------------------------------
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const AUTH_TOKEN = process.env.RELAY_AUTH_TOKEN || 'change-me';
const STALE_MS = 5000;   // if no frame in this long, the device is reported offline

const app = express();
app.use(express.raw({ type: 'image/jpeg', limit: '2mb' }));

// deviceId -> { buffer, ts }
const latestFrames = new Map();
// deviceId -> Set<WebSocket>
const subscribers = new Map();

function checkToken(req, res) {
  const tok = req.get('X-Auth-Token') || req.query.token;
  if (tok !== AUTH_TOKEN) {
    res.status(401).json({ error: 'bad or missing token' });
    return false;
  }
  return true;
}

// ---- ESP32 -> server ----
app.post('/frame', (req, res) => {
  if (!checkToken(req, res)) return;
  const deviceId = req.get('X-Device-Id') || 'default';
  if (!req.body || !req.body.length) return res.status(400).json({ error: 'empty body' });

  const entry = { buffer: req.body, ts: Date.now() };
  latestFrames.set(deviceId, entry);

  const subs = subscribers.get(deviceId);
  if (subs) {
    for (const ws of subs) {
      if (ws.readyState === ws.OPEN) ws.send(entry.buffer);
    }
  }
  res.status(204).end();
});

// ---- server -> app (polling) ----
app.get('/frame/:deviceId', (req, res) => {
  if (!checkToken(req, res)) return;
  const entry = latestFrames.get(req.params.deviceId);
  if (!entry || Date.now() - entry.ts > STALE_MS) {
    return res.status(404).json({ error: 'no recent frame for this device' });
  }
  res.set('Content-Type', 'image/jpeg');
  res.set('Cache-Control', 'no-store');
  res.send(entry.buffer);
});

app.get('/frame/:deviceId/meta', (req, res) => {
  if (!checkToken(req, res)) return;
  const entry = latestFrames.get(req.params.deviceId);
  const ageMs = entry ? Date.now() - entry.ts : null;
  res.json({ online: ageMs !== null && ageMs <= STALE_MS, ageMs });
});

app.get('/', (_req, res) => res.send('Robot relay server is running.'));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const deviceId = url.searchParams.get('device') || 'default';
  const token = url.searchParams.get('token');

  if (token !== AUTH_TOKEN) {
    ws.close(1008, 'bad token');
    return;
  }

  if (!subscribers.has(deviceId)) subscribers.set(deviceId, new Set());
  subscribers.get(deviceId).add(ws);

  const entry = latestFrames.get(deviceId);
  if (entry && Date.now() - entry.ts <= STALE_MS) ws.send(entry.buffer);

  const pingInterval = setInterval(() => {          // ADD THIS BLOCK
    if (ws.readyState === ws.OPEN) ws.ping();
    else clearInterval(pingInterval);
  }, 20000);

  ws.on('close', () => {
    clearInterval(pingInterval);                      // ADD THIS LINE
    subscribers.get(deviceId)?.delete(ws);
  });
});
