// ═══════════════════════════════════════════════════════════════
//  server.js — Railway Control Cloud Server
//  Handles: visitor WebSockets, queue management,
//           Pi controller registration, camera frame relay,
//           static file serving
// ═══════════════════════════════════════════════════════════════

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const path       = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*' },
  pingInterval: 10000,
  pingTimeout:  5000,
  maxHttpBufferSize: 2e6, // 2MB — needed for camera frames
});

// ── Serve index.html + assets from /public ───────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── Config ───────────────────────────────────────────────────
const SLOT_MS   = 60_000;
const WARN_MS   = 10_000;
const MAX_QUEUE = 20;
const PI_SECRET = process.env.PI_SECRET   || 'changeme';
const ADM_TOKEN = process.env.ADMIN_TOKEN || 'adminchangeme';

// ── State ────────────────────────────────────────────────────
let piSocket     = null;
let queue        = [];
let slotTimer    = null;
let warnTimer    = null;
let slotStarted  = null;
let currentSpeed = 0;
let currentDir   = 'fwd';
let viewerCount  = 0;

const activeId = () => queue[0] ?? null;

// ── Queue helpers ─────────────────────────────────────────────
function broadcastState() {
  const msLeft = slotStarted
    ? Math.max(0, SLOT_MS - (Date.now() - slotStarted))
    : 0;
  queue.forEach((id, idx) => {
    io.to(id).emit('queue:state', {
      queueLength: queue.length,
      myPosition:  idx + 1,
      activeId:    activeId(),
      msLeft,
      viewerCount,
      speed:       currentSpeed,
      direction:   currentDir,
    });
  });
}

function startSlot() {
  const id = activeId();
  console.log(`startSlot — activeId=${id} queue=${queue.length}`);
  if (!id) return;
  slotStarted = Date.now();
  io.to(id).emit('queue:active', { durationMs: SLOT_MS });
  warnTimer = setTimeout(() => {
    io.to(id).emit('queue:warning', { secondsLeft: WARN_MS / 1000 });
  }, SLOT_MS - WARN_MS);
  slotTimer = setTimeout(endSlot, SLOT_MS);
  broadcastState();
}

function endSlot() {
  stopTrain();
  clearTimers();
  if (queue.length > 0) {
    const done = queue.shift();
    io.to(done).emit('queue:slotended');
    queue.push(done);
  }
  if (queue.length > 0) startSlot();
  broadcastState();
}

function stopTrain() {
  currentSpeed = 0;
  if (piSocket) piSocket.emit('train:control', { speed: 0, direction: currentDir });
  io.emit('train:update', { speed: 0, direction: currentDir });
}

function clearTimers() {
  clearTimeout(slotTimer);
  clearTimeout(warnTimer);
  slotTimer = warnTimer = slotStarted = null;
}

// ── Socket connections ────────────────────────────────────────
io.on('connection', socket => {
  viewerCount++;

  // ── Pi controller handshake ──────────────────────────────
  socket.on('pi:register', ({ secret }) => {
    if (secret !== PI_SECRET) {
      console.log('Pi rejected — wrong secret');
      socket.disconnect();
      return;
    }
    piSocket = socket;
    console.log('✓ Pi connected');
    io.emit('pi:connected');

    // ── Camera frame relay ──
    // Pi sends base64 JPEG frames → server relays to all visitors
    socket.on('pi:frame', (data) => {
      io.emit('cam:frame', data);
    });
 socket.on('pi:frame1', (data) => {
      io.emit('cam:frame1', data);
    });
    
    socket.on('disconnect', () => {
      piSocket = null;
      stopTrain();
      io.emit('pi:disconnected');
      console.log('✗ Pi disconnected — train stopped');
    });

    // Pi does not join the visitor queue — stop here
    return;
  });

  // ── Visitor queue join ───────────────────────────────────
  // Wait 1 second to allow Pi to register before treating as visitor
  setTimeout(() => {
    // Skip if this turned out to be the Pi socket
    if (piSocket && piSocket.id === socket.id) return;
    // Skip if visitor already disconnected
    if (!io.sockets.sockets.get(socket.id)) return;

    if (queue.length >= MAX_QUEUE) {
      socket.emit('queue:full');
      viewerCount--;
      return;
    }

    console.log(`Visitor joined — id=${socket.id} queue=${queue.length + 1}`);
    queue.push(socket.id);
    socket.emit('train:update', { speed: currentSpeed, direction: currentDir });
    if (queue.length === 1) startSlot();
    else broadcastState();
  }, 1000);

  // ── Train control ────────────────────────────────────────
  socket.on('train:control', ({ speed, direction }) => {
    if (socket.id !== activeId()) {
      socket.emit('queue:notactive');
      return;
    }
    currentSpeed = Math.max(0, Math.min(100, Math.round(speed)));
    currentDir   = direction === 'rev' ? 'rev' : 'fwd';
    if (piSocket) piSocket.emit('train:control', { speed: currentSpeed, direction: currentDir });
    io.emit('train:update', { speed: currentSpeed, direction: currentDir });
  });

  socket.on('train:function', ({ fn, state }) => {
    if (socket.id !== activeId()) return;
    if (piSocket) piSocket.emit('train:function', { fn, state });
  });

  // ── Visitor disconnect ───────────────────────────────────
  socket.on('disconnect', () => {
    viewerCount--;
    const wasActive = activeId() === socket.id;
    queue = queue.filter(id => id !== socket.id);
    if (wasActive) {
      stopTrain();
      clearTimers();
      if (queue.length > 0) startSlot();
    }
    broadcastState();
  });
});

// ── Admin endpoints ───────────────────────────────────────────
app.post('/admin/estop', (req, res) => {
  if (req.headers['x-admin-token'] !== ADM_TOKEN)
    return res.status(401).json({ error: 'Unauthorized' });
  stopTrain();
  clearTimers();
  queue = [];
  io.emit('queue:estop');
  res.json({ ok: true });
});

app.get('/health', (_, res) => res.json({
  ok: true,
  piConnected: !!piSocket,
  queueLength: queue.length,
  viewers: viewerCount,
}));

// ── Start ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Railway server on :${PORT}`));
