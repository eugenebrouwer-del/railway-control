/**
 * server.js — bigtrainset.com cloud server
 * Changes: 2-min drive slots, country detection, stats tracking,
 *          inactivity auto-stop (1 min), new admin /stats endpoint
 *          + multi-Pi support (main Pi + cab Pi)
 *          + camera frames removed — now served directly via WebRTC/HLS
 *            from home streaming server (stream.bigtrainset.com)
 *          + /api/turn endpoint — serves fresh Twilio TURN credentials
 */

const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
let geoip;
try { geoip = require('geoip-lite'); } catch(e) { console.warn('geoip-lite not available — country detection disabled'); }
const fs     = require('fs');
const path   = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static('public'));

// ── Configuration ─────────────────────────────────────────────────
const PI_SECRET          = process.env.PI_SECRET    || 'xK9mQ2vR8nL4pJ7w';
const ADMIN_TOKEN        = process.env.ADMIN_TOKEN  || 'changeme';
const TWILIO_SID         = process.env.TWILIO_SID   || 'ACf7280aea2dcd5c64ba20e51994871ac6';
const TWILIO_TOKEN       = process.env.TWILIO_TOKEN || 'c0c1dbb2e7457d1d6c0c813694012451';
const SLOT_DURATION_MS   = 120_000;   // 2 minutes per driver
const INACTIVITY_MS      = 60_000;    // stop train after 1 min of no commands
const INACTIVITY_WARN_MS = 50_000;    // warn driver at 50 s (10 s before stop)
const MAX_QUEUE          = 20;
const STATS_FILE         = path.join(__dirname, 'stats.json');

// ── Twilio TURN credentials cache ─────────────────────────────────
// Twilio tokens expire every 24h — cache and refresh automatically
let turnCache = null;
let turnCacheTime = 0;
const TURN_CACHE_MS = 20 * 60 * 60 * 1000; // refresh every 20h

async function getTwilioTURN() {
  const now = Date.now();
  if (turnCache && (now - turnCacheTime) < TURN_CACHE_MS) {
    return turnCache;
  }
  try {
    const auth = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString('base64');
    const resp = await fetch(
      `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Tokens.json`,
      { method: 'POST', headers: { 'Authorization': `Basic ${auth}` } }
    );
    if (!resp.ok) throw new Error(`Twilio API ${resp.status}`);
    const data = await resp.json();
    turnCache = data.ice_servers;
    turnCacheTime = now;
    console.log('[turn] Refreshed Twilio TURN credentials');
    return turnCache;
  } catch (err) {
    console.error('[turn] Failed to fetch Twilio credentials:', err.message);
    return null;
  }
}

// Pre-fetch on startup
getTwilioTURN();

// ── Persistent stats ──────────────────────────────────────────────
let stats = {
  totalVisitors:   0,
  totalDrivingMs:  0,
  countryCounts:   {},
  sessionStart:    new Date().toISOString()
};

try {
  if (fs.existsSync(STATS_FILE)) {
    const saved = JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'));
    stats = { ...stats, ...saved };
  }
} catch (e) {
  console.log('No existing stats file, starting fresh.');
}

function saveStats() {
  try { fs.writeFileSync(STATS_FILE, JSON.stringify(stats, null, 2)); }
  catch (e) { console.error('Stats save failed:', e.message); }
}
setInterval(saveStats, 5 * 60_000);

// ── Country lookup helpers ────────────────────────────────────────
const COUNTRY_NAMES = {
  AU:'Australia',  US:'United States', GB:'United Kingdom', DE:'Germany',
  FR:'France',     JP:'Japan',         CA:'Canada',         NZ:'New Zealand',
  IN:'India',      CN:'China',         BR:'Brazil',         IT:'Italy',
  ES:'Spain',      NL:'Netherlands',   SE:'Sweden',         NO:'Norway',
  DK:'Denmark',    FI:'Finland',       CH:'Switzerland',    AT:'Austria',
  BE:'Belgium',    PL:'Poland',        PT:'Portugal',       RU:'Russia',
  KR:'South Korea',SG:'Singapore',     HK:'Hong Kong',      MX:'Mexico',
  AR:'Argentina',  ZA:'South Africa',  AE:'UAE',            TH:'Thailand',
  MY:'Malaysia',   ID:'Indonesia',     PH:'Philippines',    VN:'Vietnam',
  TW:'Taiwan',     IE:'Ireland',       CZ:'Czech Republic', GR:'Greece',
  HU:'Hungary',    RO:'Romania',       IL:'Israel',         TR:'Turkey',
  CL:'Chile',      CO:'Colombia',      PK:'Pakistan',       BD:'Bangladesh',
  XX:'Unknown'
};

function countryFromSocket(socket) {
  if (!geoip) return { code: 'XX', name: 'Unknown', city: '' };
  const ip =
    socket.handshake.headers['cf-connecting-ip'] ||
    (socket.handshake.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
    socket.handshake.address;
  const geo = geoip.lookup(ip);
  if (geo && geo.country) {
    return { code: geo.country, name: COUNTRY_NAMES[geo.country] || geo.country, city: geo.city || '' };
  }
  return { code: 'XX', name: 'Unknown', city: '' };
}

// ── Runtime state ─────────────────────────────────────────────────
const piSockets  = new Map();
let queue        = [];
let activeSlot   = null;
let currentTrain = { speed: 0, direction: 'fwd' };
let viewers      = 0;

function mainPi() { return piSockets.get('main') || null; }

// ── Queue helpers ─────────────────────────────────────────────────
function broadcastQueueState() {
  const activeSocket = activeSlot
    ? io.sockets.sockets.get(activeSlot.socketId)
    : null;
  const activeDriver = activeSocket?.country || null;

  queue.forEach((s, idx) => {
    s.emit('queue:state', {
      queueLength:  queue.length,
      myPosition:   idx + 1,
      activeId:     activeSlot?.socketId || null,
      msLeft:       activeSlot
                      ? Math.max(0, SLOT_DURATION_MS - (Date.now() - activeSlot.startTime))
                      : 0,
      viewerCount:  viewers,
      activeDriver
    });
  });
}

function stopTrain() {
  currentTrain = { speed: 0, direction: currentTrain.direction };
  if (mainPi()) mainPi().emit('train:control', currentTrain);
  io.emit('train:update', currentTrain);
}

function clearSlotTimers() {
  if (!activeSlot) return;
  ['slotTimer','inactivityTimer','inactivityWarnTimer','warningTimer']
    .forEach(t => { if (activeSlot[t]) clearTimeout(activeSlot[t]); });
}

function resetInactivityTimer() {
  if (!activeSlot) return;
  ['inactivityTimer','inactivityWarnTimer'].forEach(t => {
    if (activeSlot[t]) clearTimeout(activeSlot[t]);
  });

  activeSlot.inactivityWarnTimer = setTimeout(() => {
    const s = io.sockets.sockets.get(activeSlot?.socketId);
    if (s) s.emit('train:inactivity-warning', { secondsLeft: 10 });
  }, INACTIVITY_WARN_MS);

  activeSlot.inactivityTimer = setTimeout(() => {
    console.log(`[slot] Inactivity timeout for ${activeSlot?.socketId}`);
    const s = io.sockets.sockets.get(activeSlot?.socketId);
    if (s) s.emit('train:inactivity-stop');
    stopTrain();
  }, INACTIVITY_MS);
}

function activateNextSlot() {
  if (queue.length === 0 || !mainPi() || activeSlot) return;

  const socket = queue[0];
  activeSlot = {
    socketId:  socket.id,
    startTime: Date.now(),
    slotTimer: null, warningTimer: null,
    inactivityTimer: null, inactivityWarnTimer: null
  };

  activeSlot.warningTimer = setTimeout(() => {
    const s = io.sockets.sockets.get(activeSlot?.socketId);
    if (s) s.emit('queue:warning', { secondsLeft: 10 });
  }, SLOT_DURATION_MS - 10_000);

  activeSlot.slotTimer = setTimeout(() => endSlot(socket.id, true), SLOT_DURATION_MS);

  resetInactivityTimer();

  socket.emit('queue:active', {
    durationMs: SLOT_DURATION_MS,
    country:    socket.country
  });

  console.log(`[slot] Activated for ${socket.id} (${socket.country?.name})`);
  broadcastQueueState();
}

function endSlot(socketId, moveToBack = false) {
  if (!activeSlot || activeSlot.socketId !== socketId) return;

  stats.totalDrivingMs += Date.now() - activeSlot.startTime;
  saveStats();

  clearSlotTimers();
  activeSlot = null;
  stopTrain();

  const socket = io.sockets.sockets.get(socketId);
  if (socket) {
    socket.emit('queue:slotended');
    if (moveToBack) {
      const idx = queue.indexOf(socket);
      if (idx !== -1) { queue.splice(idx, 1); queue.push(socket); }
    }
  }

  console.log(`[slot] Ended for ${socketId}`);
  setTimeout(activateNextSlot, 1000);
  broadcastQueueState();
}

// ── Socket.IO ─────────────────────────────────────────────────────
io.on('connection', (socket) => {
  const isPi = socket.handshake.auth?.secret === PI_SECRET;

  if (!isPi) {
    socket.country = countryFromSocket(socket);
    stats.totalVisitors++;
    stats.countryCounts[socket.country.code] =
      (stats.countryCounts[socket.country.code] || 0) + 1;
    viewers++;

    console.log(`[visitor] connected from ${socket.country.name} (${socket.id})`);

    setTimeout(() => {
      if (!io.sockets.sockets.has(socket.id)) return;
      if (queue.length >= MAX_QUEUE) { socket.emit('queue:full'); return; }
      queue.push(socket);
      if (!activeSlot && mainPi()) activateNextSlot();
      else broadcastQueueState();
    }, 1000);
  }

  socket.on('pi:register', (data) => {
    if (data?.secret !== PI_SECRET) { socket.disconnect(); return; }
    const deviceId = data.device || 'main';
    piSockets.set(deviceId, socket);
    console.log(`[pi] registered: ${deviceId}`);
    io.emit('pi:connected');
    if (deviceId === 'main' && queue.length > 0 && !activeSlot) {
      setTimeout(activateNextSlot, 1000);
    }
  });

  socket.on('pong', () => {});

  socket.on('train:control', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) {
      socket.emit('queue:notactive');
      return;
    }
    currentTrain = { speed: data.speed, direction: data.direction };
    if (mainPi()) mainPi().emit('train:control', currentTrain);
    io.emit('train:update', currentTrain);
    resetInactivityTimer();
  });

  socket.on('train:function', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) return;
    if (mainPi()) mainPi().emit('train:function', data);
  });

  socket.on('ping', () => socket.emit('pong'));

  socket.on('disconnect', () => {
    let disconnectedDevice = null;
    for (const [deviceId, s] of piSockets.entries()) {
      if (s === socket) { disconnectedDevice = deviceId; break; }
    }

    if (disconnectedDevice) {
      piSockets.delete(disconnectedDevice);
      console.log(`[pi] disconnected: ${disconnectedDevice}`);
      if (disconnectedDevice === 'main') {
        io.emit('pi:disconnected');
        clearSlotTimers();
        activeSlot = null;
        stopTrain();
        broadcastQueueState();
      }
    } else {
      viewers = Math.max(0, viewers - 1);
      const idx = queue.indexOf(socket);
      if (idx !== -1) queue.splice(idx, 1);
      if (activeSlot?.socketId === socket.id) endSlot(socket.id, false);
      else broadcastQueueState();
    }
  });
});

// ── HTTP endpoints ────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({
  ok: true,
  piConnected:  piSockets.has('main'),
  cabConnected: piSockets.has('cab'),
  queueLength:  queue.length,
  viewers,
  uptime:       process.uptime()
}));

// ── TURN credentials endpoint ─────────────────────────────────────
// Returns fresh Twilio ICE/TURN server credentials
// Cached for 20h, auto-refreshed before expiry
app.get('/api/turn', async (req, res) => {
  try {
    const iceServers = await getTwilioTURN();
    if (!iceServers) {
      // Fallback STUN only if Twilio fails
      return res.json({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
    }
    res.set('Cache-Control', 'no-store');
    res.json({ iceServers });
  } catch (err) {
    console.error('[turn] endpoint error:', err.message);
    res.status(500).json({ error: 'Failed to get TURN credentials' });
  }
});

function requireAdmin(req, res, next) {
  if (req.headers['x-admin-token'] !== ADMIN_TOKEN)
    return res.status(403).json({ error: 'Forbidden' });
  next();
}

app.post('/admin/estop', requireAdmin, (req, res) => {
  stopTrain();
  if (mainPi()) mainPi().emit('train:estop');
  io.emit('queue:estop');
  clearSlotTimers();
  activeSlot = null;
  queue = [];
  res.json({ ok: true, message: 'ESTOP sent, queue cleared' });
});

app.get('/admin/stats', requireAdmin, (req, res) => {
  const hours = (stats.totalDrivingMs / 3_600_000).toFixed(2);
  const countryList = Object.entries(stats.countryCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([code, count]) => ({
      code, count,
      name: COUNTRY_NAMES[code] || code,
      flag: `https://flagcdn.com/32x24/${code.toLowerCase()}.png`
    }));

  res.json({
    totalVisitors:      stats.totalVisitors,
    totalDrivingHours:  parseFloat(hours),
    totalDrivingMs:     stats.totalDrivingMs,
    sessionStart:       stats.sessionStart,
    currentViewers:     viewers,
    currentQueueLength: queue.length,
    piConnected:        piSockets.has('main'),
    cabConnected:       piSockets.has('cab'),
    countryCounts:      countryList
  });
});

// ── Start ─────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`bigtrainset server running on port ${PORT}`));
