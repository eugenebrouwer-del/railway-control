/**
 * server.js — bigtrainset.com cloud server
 * Hosted on Hetzner VPS (5.223.70.185)
 * Changes: 2-min drive slots, country detection, stats tracking,
 *          inactivity auto-stop (1 min), admin endpoints
 *          + multi-Pi support (main Pi + cab Pi)
 *          + camera frames removed — served directly via WebRTC/HLS
 *            from home streaming server (stream.bigtrainset.com)
 *          + /api/turn endpoint — serves coturn TURN credentials
 *          + freight train added (DCC 1). Passenger train is DCC 3.
 *            Both trains are controlled by whoever holds the active
 *            drive slot — one visitor, one slot, two trains. There is
 *            no separate freight queue; freight just rides along on
 *            the existing passenger queue/slot system.
 *          + accessory:control stub for Windmill (DCC 20) / Building Lights (DCC 21)
 *            — logs only until Massoth DIMAX USB integration is wired up on the Pi
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
const SLOT_DURATION_MS   = 120_000;   // 2 minutes per driver — covers BOTH trains
const INACTIVITY_MS      = 60_000;    // stop trains after 1 min of no commands
const INACTIVITY_WARN_MS = 50_000;    // warn driver at 50s (10s before stop)
const MAX_QUEUE          = 20;
const STATS_FILE         = path.join(__dirname, 'stats.json');

// ── DCC address map ────────────────────────────────────────────────
// Both trains are controlled by the same person during their drive slot.
// Passenger train has the cab cam; freight does not. Accessories are
// separate (Massoth DIMAX, not queue-gated, not yet wired up on the Pi).
const DCC = {
  FREIGHT_TRAIN:    1,
  PASSENGER_TRAIN:  3,
  WINDMILL:         20,
  BUILDING_LIGHTS:  21
};

// ── coturn TURN server credentials ───────────────────────────────
// Running on this Hetzner VPS at 5.223.70.185
// No expiry — permanent credentials
const TURN_ICE_SERVERS = [
  { urls: 'stun:5.223.70.185:3478' },
  {
    urls: [
      'turn:5.223.70.185:3478?transport=udp',
      'turn:5.223.70.185:3478?transport=tcp',
      'turn:5.223.70.185:5349?transport=tcp'
    ],
    username:   'bigtrainset',
    credential: 'traintime123'
  }
];

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
const piSockets   = new Map();
let queue         = [];
let activeSlot     = null;
let currentTrain   = { dcc: DCC.PASSENGER_TRAIN, speed: 0, direction: 'fwd' };
let currentFreight = { dcc: DCC.FREIGHT_TRAIN,   speed: 0, direction: 'fwd' };
let viewers        = 0;

function mainPi() { return piSockets.get('main') || null; }

// ── Queue helpers ──────────────────────────────────────────────────
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

// Stops BOTH trains. Used for inactivity timeout, slot end, Pi disconnect,
// and the admin e-stop — anywhere we need to guarantee nothing is left
// running once a slot is no longer active.
function stopAllTrains() {
  currentTrain   = { dcc: DCC.PASSENGER_TRAIN, speed: 0, direction: currentTrain.direction };
  currentFreight = { dcc: DCC.FREIGHT_TRAIN,   speed: 0, direction: currentFreight.direction };
  if (mainPi()) {
    mainPi().emit('train:control', currentTrain);
    mainPi().emit('train:control', currentFreight);
  }
  io.emit('train:update', currentTrain);
  io.emit('freight:update', currentFreight);
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
    stopAllTrains();
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

  // One slot, two trains — the driver gets control of both for the
  // same duration. The cab cam belongs to the passenger train; freight
  // has no camera but drives in parallel during the same window.
  socket.emit('queue:active', {
    durationMs: SLOT_DURATION_MS,
    country:    socket.country
  });

  console.log(`[slot] Activated for ${socket.id} (${socket.country?.name}) — controls both trains`);
  broadcastQueueState();
}

function endSlot(socketId, moveToBack = false) {
  if (!activeSlot || activeSlot.socketId !== socketId) return;

  stats.totalDrivingMs += Date.now() - activeSlot.startTime;
  saveStats();

  clearSlotTimers();
  activeSlot = null;
  stopAllTrains();

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

  // ── Cab camera (Reolink Lumus, bridged from GEEKOM) ─────────────────────
  // The GEEKOM bridge script registers as device "cab" via pi:register,
  // then streams JPEG frames here. We relay them to visitors as
  // cam:cab_frame, mirroring the cam:frame / cam:frame1 pattern already
  // used for cam1/cam2.
  socket.on('pi:cab_frame', (data) => {
    if (!isPi) return; // only the registered cab device may publish frames
    io.emit('cam:cab_frame', data);
  });

  // ── Passenger train (DCC 3) — only the active slot holder ───────────────
  socket.on('train:control', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) {
      socket.emit('queue:notactive');
      return;
    }
    currentTrain = { dcc: DCC.PASSENGER_TRAIN, speed: data.speed, direction: data.direction };
    if (mainPi()) mainPi().emit('train:control', currentTrain);
    io.emit('train:update', currentTrain);
    resetInactivityTimer();
  });

  socket.on('train:function', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) return;
    if (mainPi()) mainPi().emit('train:function', { dcc: DCC.PASSENGER_TRAIN, ...data });
    resetInactivityTimer();
  });

  socket.on('train:estop', () => {
    if (!activeSlot || activeSlot.socketId !== socket.id) return;
    stopAllTrains();
  });

  // ── Freight train (DCC 1) — same slot holder as passenger, same window ──
  // Not a separate queue. Whoever has the active passenger slot also
  // controls freight; their inactivity timer covers both trains.
  socket.on('freight:control', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) {
      socket.emit('queue:notactive');
      return;
    }
    const speed = Math.max(0, Math.min(100, Number(data?.speed) || 0));
    const direction = data?.direction === 'rev' ? 'rev' : 'fwd';
    currentFreight = { dcc: DCC.FREIGHT_TRAIN, speed, direction };
    if (mainPi()) mainPi().emit('train:control', currentFreight);
    io.emit('freight:update', currentFreight);
    resetInactivityTimer();
  });

  socket.on('freight:function', (data) => {
    if (!activeSlot || activeSlot.socketId !== socket.id) return;
    if (mainPi()) mainPi().emit('train:function', { dcc: DCC.FREIGHT_TRAIN, ...data });
    resetInactivityTimer();
  });

  socket.on('freight:estop', () => {
    if (!activeSlot || activeSlot.socketId !== socket.id) return;
    stopAllTrains();
  });

  // ── Accessories (DCC 20/21 via Massoth DIMAX) ────────────────────────────
  // STUB: Massoth DIMAX USB integration on the Pi is not yet wired up.
  // Accessories are NOT queue-gated — anyone can flip the windmill/lights
  // at any time, same as originally planned, independent of drive slots.
  // This currently just logs and echoes state back to visitors so the UI
  // doesn't error out. Replace the console.log with an actual call to the
  // Pi-side DIMAX driver once that's built, e.g.:
  //   if (mainPi()) mainPi().emit('accessory:control', data);
  // and have the Pi forward the DCC accessory packet over USB to the DIMAX.
  socket.on('accessory:control', (data) => {
    if (isPi) return;
    const known = Object.values(DCC).includes(data?.dcc);
    if (!known) {
      console.warn('[accessory] unknown DCC address in request:', data);
      return;
    }
    console.log(`[accessory:STUB] ${data.name} (DCC ${data.dcc}) -> ${data.action} — Massoth DIMAX not yet connected, no hardware action taken`);
    io.emit('accessory:update', data);
    // TODO once DIMAX is wired up over USB on the Pi:
    // if (mainPi()) mainPi().emit('accessory:control', data);
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
        stopAllTrains();
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
  freight:      currentFreight,
  uptime:       process.uptime()
}));

// ── TURN credentials endpoint ─────────────────────────────────────
// Returns coturn ICE server credentials — no expiry, permanent
app.get('/api/turn', (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({ iceServers: TURN_ICE_SERVERS });
});

function requireAdmin(req, res, next) {
  if (req.headers['x-admin-token'] !== ADMIN_TOKEN)
    return res.status(403).json({ error: 'Forbidden' });
  next();
}

app.post('/admin/estop', requireAdmin, (req, res) => {
  stopAllTrains();
  if (mainPi()) mainPi().emit('train:estop');
  io.emit('queue:estop');
  clearSlotTimers();
  activeSlot = null;
  queue = [];
  res.json({ ok: true, message: 'ESTOP sent (both trains), queue cleared' });
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
    freight:            currentFreight,
    countryCounts:      countryList
  });
});

// ── Start ─────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`bigtrainset server running on port ${PORT}`));
