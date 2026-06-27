require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 1e6
});

app.use(express.json({ limit: '50kb' }));
app.use(express.static(path.join(__dirname, '../public'), {
  maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0,
  etag: true
}));

// ─── MongoDB ───────────────────────────────────────────────────────────────
const MONGODB_URI = process.env.MONGODB_URI;
let dbConnected = false;

if (MONGODB_URI && !MONGODB_URI.includes('username:password')) {
  mongoose.connect(MONGODB_URI)
    .then(() => { dbConnected = true; console.log('✅ MongoDB connected'); })
    .catch(err => console.warn('⚠️ MongoDB not connected, running without DB:', err.message));
}

const SessionSchema = new mongoose.Schema({
  sessionId: String,
  user1: { socketId: String, country: String, gender: String },
  user2: { socketId: String, country: String, gender: String },
  startedAt: { type: Date, default: Date.now },
  endedAt: Date,
  duration: Number,
  flagged: { type: Boolean, default: false },
  flagReason: String
});
const Session = dbConnected ? mongoose.model('Session', SessionSchema) : null;

const FeedbackSchema = new mongoose.Schema({
  name: String,
  email: String,
  message: String,
  path: String,
  userAgent: String,
  createdAt: { type: Date, default: Date.now }
});
const Feedback = dbConnected ? mongoose.model('Feedback', FeedbackSchema) : null;

// ─── In-Memory State ───────────────────────────────────────────────────────
// waitingPool: Map<socketId, { socket, gender, country, filters, joinedAt }>
const waitingPool = new Map();

// activePairs: Map<socketId, { partner: socketId, sessionId, session: SessionData }>
const activePairs = new Map();

// adminSockets: Set<socketId>
const adminSockets = new Set();

// userMeta: Map<socketId, { gender, country, joinedAt, peerId? }>
const userMeta = new Map();

const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'Admin@1234';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'admin@example.com';

// ─── Helpers ───────────────────────────────────────────────────────────────
function getStats() {
  return {
    online: userMeta.size,
    waiting: waitingPool.size,
    active: activePairs.size / 2,
    admins: adminSockets.size
  };
}

function broadcastStats() {
  io.emit('stats', getStats());
}

function notifyAdmins(event, data) {
  adminSockets.forEach(adminId => {
    const adminSocket = io.sockets.sockets.get(adminId);
    if (adminSocket) adminSocket.emit(event, data);
  });
}

function wantsGender(preference, actual) {
  return preference === 'any' || preference === actual;
}

function wantsCountry(preference, actual) {
  return preference === 'any' || preference === actual;
}

function findMatch(socket, myMeta, myFilters) {
  for (const [waitingId, waiting] of waitingPool) {
    if (waitingId === socket.id) continue;

    const theirMeta = waiting.meta || {};
    const theirFilters = waiting.filters || {};

    const iWantThem = wantsGender(myFilters.gender, theirMeta.gender) && wantsCountry(myFilters.country, theirMeta.country);
    const theyWantMe = wantsGender(theirFilters.gender, myMeta.gender) && wantsCountry(theirFilters.country, myMeta.country);

    if (iWantThem && theyWantMe) {
      return { matchId: waitingId, matchSocket: waiting.socket, matchMeta: theirMeta, matchFilters: theirFilters };
    }
  }
  return null;
}

async function createSession(socket1, socket2, meta1, meta2) {
  const sessionId = uuidv4();

  const sessionData = {
    sessionId,
    user1: { socketId: socket1.id, country: meta1.country, gender: meta1.gender },
    user2: { socketId: socket2.id, country: meta2.country, gender: meta2.gender },
    startedAt: new Date()
  };

  activePairs.set(socket1.id, { partner: socket2.id, sessionId, sessionData });
  activePairs.set(socket2.id, { partner: socket1.id, sessionId, sessionData });

  // Tell each user who is the "caller" (initiates WebRTC offer)
  socket1.emit('matched', {
    sessionId,
    partnerCountry: meta2.country,
    partnerGender: meta2.gender,
    role: 'caller'
  });

  socket2.emit('matched', {
    sessionId,
    partnerCountry: meta1.country,
    partnerGender: meta1.gender,
    role: 'callee'
  });

  // Notify admins
  notifyAdmins('session_started', {
    sessionId,
    user1: { id: socket1.id, ...meta1 },
    user2: { id: socket2.id, ...meta2 },
    startedAt: sessionData.startedAt
  });

  // Save to DB
  if (Session) {
    try { await new Session(sessionData).save(); } catch (e) { /* silent */ }
  }

  broadcastStats();
  return sessionId;
}

async function endSession(socketId, reason = 'disconnect') {
  const pair = activePairs.get(socketId);
  if (!pair) return;

  const { partner, sessionId, sessionData } = pair;
  activePairs.delete(socketId);
  activePairs.delete(partner);

  const partnerSocket = io.sockets.sockets.get(partner);
  if (partnerSocket) {
    partnerSocket.emit('partner_disconnected', { reason });
  }

  const duration = Math.round((Date.now() - new Date(sessionData.startedAt).getTime()) / 1000);

  notifyAdmins('session_ended', { sessionId, duration, reason });

  if (Session) {
    try {
      await Session.updateOne(
        { sessionId },
        { endedAt: new Date(), duration }
      );
    } catch (e) { /* silent */ }
  }

  broadcastStats();
}

// ─── HTTP Routes ───────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// Admin page at a hidden path
app.get('/admin-panel-x7k2', (req, res) => {
  res.sendFile(path.join(__dirname, '../admin/index.html'));
});

app.get('/api/stats', (req, res) => {
  res.json(getStats());
});

app.get('/robots.txt', (req, res) => {
  const base = `${req.protocol}://${req.get('host')}`;
  res.type('text/plain').send(`User-agent: *\nAllow: /\nDisallow: /admin-panel-x7k2\nSitemap: ${base}/sitemap.xml\n`);
});

app.get('/sitemap.xml', (req, res) => {
  const base = `${req.protocol}://${req.get('host')}`;
  res.type('application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n  <url><loc>${base}/</loc><priority>1.0</priority><changefreq>daily</changefreq></url>\n</urlset>`);
});

app.post('/api/feedback', async (req, res) => {
  const { name = '', email = '', message = '', path: pagePath = '' } = req.body || {};
  if (typeof message !== 'string' || message.trim().length < 3) {
    return res.status(400).json({ ok: false, error: 'Please write the problem first.' });
  }
  const doc = {
    name: String(name).slice(0, 80),
    email: String(email).slice(0, 120),
    message: String(message).slice(0, 1000),
    path: String(pagePath).slice(0, 200),
    userAgent: String(req.get('user-agent') || '').slice(0, 300)
  };
  if (Feedback) { try { await new Feedback(doc).save(); } catch (e) {} }
  notifyAdmins('feedback_received', { ...doc, createdAt: new Date() });
  res.json({ ok: true, adminEmail: ADMIN_EMAIL });
});

// ─── Socket.IO ─────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`🔌 Connected: ${socket.id}`);

  // ── Admin auth ──────────────────────────────────────────────────────────
  socket.on('admin_auth', ({ username, password }) => {
    if (username === ADMIN_USERNAME && password === ADMIN_PASSWORD) {
      adminSockets.add(socket.id);
      socket.emit('admin_auth_success');

      // Send current state
      const activeSessions = [];
      const seen = new Set();
      for (const [sid, pair] of activePairs) {
        if (seen.has(pair.sessionId)) continue;
        seen.add(pair.sessionId);
        const meta1 = userMeta.get(sid) || {};
        const meta2 = userMeta.get(pair.partner) || {};
        activeSessions.push({
          sessionId: pair.sessionId,
          user1: { id: sid, ...meta1 },
          user2: { id: pair.partner, ...meta2 },
          startedAt: pair.sessionData.startedAt
        });
      }
      socket.emit('admin_state', {
        stats: getStats(),
        activeSessions,
        waitingUsers: [...waitingPool.keys()].map(id => ({ id, ...waitingPool.get(id).filters }))
      });
    } else {
      socket.emit('admin_auth_fail');
    }
  });

  // Admin requests to spy on a session (receive WebRTC stream)
  socket.on('admin_spy_request', ({ sessionId }) => {
    if (!adminSockets.has(socket.id)) return;
    // Find users in this session
    for (const [sid, pair] of activePairs) {
      if (pair.sessionId === sessionId) {
        const userSocket = io.sockets.sockets.get(sid);
        if (userSocket) userSocket.emit('admin_spy_join', { adminId: socket.id, sessionId });
      }
    }
  });

  // Forward admin spy signaling
  socket.on('admin_spy_offer', ({ targetId, sdp, sessionId }) => {
    const targetSocket = io.sockets.sockets.get(targetId);
    if (targetSocket) targetSocket.emit('admin_spy_offer', { adminId: socket.id, fromId: socket.id, sdp, sessionId });
  });

  socket.on('admin_spy_answer', ({ adminId, sdp }) => {
    const adminSocket = io.sockets.sockets.get(adminId);
    if (adminSocket) adminSocket.emit('admin_spy_answer', { fromId: socket.id, sdp });
  });

  socket.on('admin_spy_ice', ({ targetId, candidate }) => {
    const targetSocket = io.sockets.sockets.get(targetId);
    if (targetSocket) targetSocket.emit('admin_spy_ice', { adminId: socket.id, fromId: socket.id, candidate });
  });

  socket.on('admin_spy_ice_answer', ({ adminId, candidate }) => {
    const adminSocket = io.sockets.sockets.get(adminId);
    if (adminSocket) adminSocket.emit('admin_spy_ice_answer', { fromId: socket.id, candidate });
  });

  socket.on('admin_flag_session', async ({ sessionId, reason }) => {
    if (!adminSockets.has(socket.id)) return;
    if (Session) {
      try { await Session.updateOne({ sessionId }, { flagged: true, flagReason: reason }); } catch (e) { }
    }
    notifyAdmins('session_flagged', { sessionId, reason });

    // Kick the users in this session
    for (const [sid, pair] of activePairs) {
      if (pair.sessionId === sessionId) {
        const s = io.sockets.sockets.get(sid);
        if (s) s.emit('kicked', { reason: 'Removed by moderator for policy violation.' });
      }
    }
  });

  // ── User join ───────────────────────────────────────────────────────────
  socket.on('user_join', ({ gender, country }) => {
    const safeGender = ['male', 'female', 'couples', 'any'].includes(gender) ? gender : 'any';
    const safeCountry = typeof country === 'string' ? country.slice(0, 3) : 'any';

    userMeta.set(socket.id, {
      gender: safeGender,
      country: safeCountry,
      joinedAt: new Date()
    });

    broadcastStats();
  });

  // ── Find match ──────────────────────────────────────────────────────────
  socket.on('find_match', async ({ gender, country, filterGender, filterCountry }) => {
    // Remove from any existing pair
    await endSession(socket.id, 'skipped');

    // Remove from waiting pool
    waitingPool.delete(socket.id);

    const myFilters = {
      gender: ['male', 'female', 'couples', 'any'].includes(filterGender) ? filterGender : 'any',
      country: typeof filterCountry === 'string' ? filterCountry.slice(0, 3) : 'any'
    };

    const myMeta = {
      gender: ['male', 'female', 'couples', 'any'].includes(gender) ? gender : 'any',
      country: typeof country === 'string' ? country.slice(0, 3) : 'any'
    };

    userMeta.set(socket.id, { ...myMeta, joinedAt: new Date() });

    const match = findMatch(socket, myMeta, myFilters);

    if (match) {
      waitingPool.delete(match.matchId);
      const matchMeta = userMeta.get(match.matchId) || match.matchMeta || match.matchFilters;
      await createSession(socket, match.matchSocket, myMeta, matchMeta);
    } else {
      waitingPool.set(socket.id, { socket, meta: myMeta, filters: myFilters, joinedAt: new Date() });
      socket.emit('waiting');
      notifyAdmins('user_waiting', { id: socket.id, ...myMeta, ...myFilters });
    }
    broadcastStats();
  });

  // ── WebRTC signaling ────────────────────────────────────────────────────
  socket.on('offer', ({ sdp }) => {
    const pair = activePairs.get(socket.id);
    if (!pair) return;
    const partnerSocket = io.sockets.sockets.get(pair.partner);
    if (partnerSocket) partnerSocket.emit('offer', { sdp });
  });

  socket.on('answer', ({ sdp }) => {
    const pair = activePairs.get(socket.id);
    if (!pair) return;
    const partnerSocket = io.sockets.sockets.get(pair.partner);
    if (partnerSocket) partnerSocket.emit('answer', { sdp });
  });

  socket.on('ice_candidate', ({ candidate }) => {
    const pair = activePairs.get(socket.id);
    if (!pair) return;
    const partnerSocket = io.sockets.sockets.get(pair.partner);
    if (partnerSocket) partnerSocket.emit('ice_candidate', { candidate });
  });

  // ── Chat messages ───────────────────────────────────────────────────────
  socket.on('chat_message', ({ text }) => {
    const pair = activePairs.get(socket.id);
    if (!pair) return;
    if (typeof text !== 'string' || text.length > 500) return;

    const partnerSocket = io.sockets.sockets.get(pair.partner);
    if (partnerSocket) partnerSocket.emit('chat_message', { text: text.trim(), from: 'stranger' });

    // Relay to admins
    notifyAdmins('chat_message', {
      sessionId: pair.sessionId,
      from: socket.id,
      text: text.trim(),
      at: new Date()
    });
  });

  // ── Skip / Next ─────────────────────────────────────────────────────────
  socket.on('skip', async () => {
    await endSession(socket.id, 'skipped');
  });

  // ── Disconnect ──────────────────────────────────────────────────────────
  socket.on('disconnect', async () => {
    console.log(`🔌 Disconnected: ${socket.id}`);
    adminSockets.delete(socket.id);
    waitingPool.delete(socket.id);
    await endSession(socket.id, 'disconnect');
    userMeta.delete(socket.id);
    notifyAdmins('user_left', { id: socket.id });
    broadcastStats();
  });
});

// ─── Start ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`🔒 Admin panel: http://localhost:${PORT}/admin-panel-x7k2`);
});
