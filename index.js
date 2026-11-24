// ============================================================
// 🔇 TERMINAL SILENCE — Zero logs on server
// ============================================================
if (process.env.DEBUG !== '1') {
    const noop = () => {};
    console.log = noop;
    console.info = noop;
    console.warn = noop;
    console.error = noop;
    console.debug = noop;
}

// ============================================================
// ESM Imports + Safe Loaders for Both Libraries
// ============================================================
import express from 'express';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import WebSocket from 'ws';
import { createRequire } from 'module';

global.WebSocket = WebSocket;
const require = createRequire(import.meta.url);

let MetaClient;
try {
    const mm = await import('meta-messenger.js');
    MetaClient = mm.Client || mm.default?.Client;
} catch (_) {}
if (!MetaClient) {
    const mmCjs = require('meta-messenger.js');
    MetaClient = mmCjs.Client || mmCjs.default?.Client;
}

let FBClient;
try {
    const fb = await import('fb-messenger-e2ee');
    FBClient = fb.FBClient || fb.default?.FBClient;
} catch (_) {}
if (!FBClient) {
    const fbCjs = require('fb-messenger-e2ee');
    FBClient = fbCjs.FBClient || fbCjs.default?.FBClient;
}

const app = express();
const PORT = process.env.PORT || 3000;
const DB_FILE = './tasks.json';
const SESSION_DIR = './e2ee_sessions';

if (!fs.existsSync(SESSION_DIR)) {
    fs.mkdirSync(SESSION_DIR, { recursive: true });
}

// Optional JSONBin persistence
const JSONBIN_BIN_ID = process.env.JSONBIN_BIN_ID || '';
const JSONBIN_API_KEY = process.env.JSONBIN_API_KEY || '';

// ==================== SAFE CONSTANTS ====================
const CONNECT_TIMEOUT_MS = 40000;
const SEND_TIMEOUT_MS = 20000;
const PRIMARY_RETRIES = 2;
const BACKUP_RETRIES = 1;
const MAX_CONNECT_FAILS = 8;
const MAX_CONSECUTIVE_SEND_FAILS = 5;
const SEND_COOLDOWN_MS = 600000; // 10 min cool-down if send fails repeatedly
const MIN_SAFE_DELAY_SEC = 15;
const JITTER_MIN_MS = 2000;
const JITTER_MAX_MS = 6000;
const BACKOFF_STEPS_MS = [120000, 300000, 600000, 900000]; // 2m, 5m, 10m, 15m
const SAVE_MIN_INTERVAL_MS = 30000;
const SAVE_DEBOUNCE_MS = 5000;
const MAX_LOG_LINES = 60;
const RESTORE_STAGGER_MS = 6000;

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ==================== STATE ====================
const activeTasks = new Map();

let isShuttingDown = false;
let saveTimer = null;
let saveInProgress = false;
let lastSaveAt = 0;
let saveDirty = false;

// ==================== ANTI-CRASH ====================
process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

// ==================== HELPERS ====================
function parseRawCookies(cookieString) {
    const cookies = {};
    if (!cookieString || !cookieString.trim()) return cookies;
    const parts = cookieString.split(';');
    for (const part of parts) {
        const trimmed = part.trim();
        if (!trimmed) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key = trimmed.slice(0, eqIdx).trim();
        const value = trimmed.slice(eqIdx + 1).trim();
        if (key) cookies[key] = value;
    }
    return cookies;
}

// Raw cookie string -> .facebook.com appState array (including 'fr') for fb-messenger-e2ee
function rawCookiesToFacebookAppState(rawCookieString) {
    const appState = [];
    const seen = new Set();
    if (!rawCookieString || !rawCookieString.trim()) return appState;

    const parts = rawCookieString.split(';');
    for (const part of parts) {
        const trimmed = part.trim();
        if (!trimmed) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key = trimmed.slice(0, eqIdx).trim();
        const value = trimmed.slice(eqIdx + 1).trim();

        if (!key || seen.has(key)) continue;
        seen.add(key);

        appState.push({
            key,
            value,
            domain: '.facebook.com',
            path: '/',
            hostOnly: false,
            secure: true,
            httpOnly: key !== 'c_user',
            sameSite: 'none'
        });
    }
    return appState;
}

function getSafeDelayMs(delaySec) {
    const safeSec = Math.max(MIN_SAFE_DELAY_SEC, parseInt(delaySec) || MIN_SAFE_DELAY_SEC);
    const jitter = Math.floor(Math.random() * (JITTER_MAX_MS - JITTER_MIN_MS + 1)) + JITTER_MIN_MS;
    return (safeSec * 1000) + jitter;
}

function getUptimeString(startTime) {
    const diff = Date.now() - startTime;
    const d = Math.floor(diff / 86400000);
    const h = Math.floor((diff % 86400000) / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    return `${d} Days, ${h} Hours, ${m} Mins`;
}

function pushLog(task, msg) {
    task.logs.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
    if (task.logs.length > MAX_LOG_LINES) {
        task.logs.splice(0, task.logs.length - MAX_LOG_LINES);
    }
}

function withTimeout(promise, ms, label) {
    let timer;
    const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timeout ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
}

async function interruptibleSleep(ms) {
    const step = 1000;
    for (let elapsed = 0; elapsed < ms; elapsed += step) {
        if (isShuttingDown) return false;
        await new Promise(r => setTimeout(r, Math.min(step, ms - elapsed)));
    }
    return true;
}

function backoffFor(failCount) {
    if (failCount <= 0) return BACKOFF_STEPS_MS[0];
    const idx = Math.min(failCount - 1, BACKOFF_STEPS_MS.length - 1);
    return BACKOFF_STEPS_MS[idx];
}

function closeClientSafe(client) {
    if (!client) return;
    try {
        if (typeof client.close === 'function') client.close();
        else if (typeof client.disconnect === 'function') client.disconnect();
    } catch (_) {}
}

// ==================== SAVE (DEBOUNCED) ====================
function scheduleSave(force = false) {
    saveDirty = true;

    if (force) {
        if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
        return doSave();
    }

    if (saveTimer) return;

    const wait = Math.max(SAVE_DEBOUNCE_MS, SAVE_MIN_INTERVAL_MS - (Date.now() - lastSaveAt));
    saveTimer = setTimeout(() => {
        saveTimer = null;
        doSave();
    }, wait);
}

async function doSave() {
    if (saveInProgress) { saveDirty = true; return; }
    saveInProgress = true;
    saveDirty = false;
    lastSaveAt = Date.now();

    const out = {};
    for (const [id, t] of activeTasks.entries()) {
        out[id] = {
            mode: t.mode === 'normal' ? 'normal' : 'e2ee',
            cookies: t.cookies,
            backupCookies: t.backupCookies || '',
            threadId: t.threadId,
            hatersName: t.hatersName,
            messages: t.messages,
            delaySec: t.delaySec,
            status: t.status,
            startTime: t.startTime,
            logs: t.logs.slice(-MAX_LOG_LINES),
            activeCookieSource: t.activeCookieSource || 'primary'
        };
    }

    try {
        if (JSONBIN_BIN_ID && JSONBIN_API_KEY) {
            await fetch(`https://api.jsonbin.io/v3/b/${JSONBIN_BIN_ID}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Master-Key': JSONBIN_API_KEY
                },
                body: JSON.stringify(out)
            });
        } else {
            fs.writeFileSync(DB_FILE, JSON.stringify(out, null, 2));
        }
    } catch (_) {
        // silent
    } finally {
        saveInProgress = false;
        if (saveDirty) scheduleSave(false);
    }
}

// ==================== RESTORE ====================
async function loadTasks() {
    try {
        let dataRecord = null;
        if (JSONBIN_BIN_ID && JSONBIN_API_KEY) {
            const res = await fetch(`https://api.jsonbin.io/v3/b/${JSONBIN_BIN_ID}/latest`, {
                headers: { 'X-Master-Key': JSONBIN_API_KEY }
            });
            const j = await res.json();
            dataRecord = j.record;
        } else if (fs.existsSync(DB_FILE)) {
            dataRecord = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        }

        if (!dataRecord) return;

        const toRestore = [];
        for (const [taskId, taskData] of Object.entries(dataRecord)) {
            if (taskData.status === 'running') {
                activeTasks.set(taskId, {
                    ...taskData,
                    mode: taskData.mode === 'normal' ? 'normal' : 'e2ee',
                    delaySec: Math.max(MIN_SAFE_DELAY_SEC, parseInt(taskData.delaySec) || MIN_SAFE_DELAY_SEC),
                    client: null,
                    forceReconnect: false,
                    connectFailCount: 0,
                    sendFailCount: 0
                });
                toRestore.push(taskId);
            }
        }

        for (let i = 0; i < toRestore.length; i++) {
            const id = toRestore[i];
            if (i > 0) await new Promise(r => setTimeout(r, RESTORE_STAGGER_MS));
            if (isShuttingDown) break;
            runPersistentTask(id);
        }
    } catch (_) {
        // silent
    }
}

// ==================== HEALTH ====================
app.head('/', (req, res) => res.status(200).end());
app.get('/ping', (req, res) => res.send('Pong'));
app.get('/health', (req, res) => res.json({
    status: 'ONLINE',
    uptime: Math.round(process.uptime()),
    isShuttingDown,
    activeTasks: activeTasks.size,
    taskIds: Array.from(activeTasks.keys())
}));

// ==================== HTML UI ====================
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html lang="hi"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>24/7 Messenger Bot (Safe Mode)</title>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap" rel="stylesheet">
<style>
body{font-family:Poppins,sans-serif;background:linear-gradient(135deg,#fff0f3,#ffe5ec);color:#2b2d42;padding:20px;margin:0;min-height:100vh}
.c{max-width:680px;margin:auto;background:#fff;padding:30px;border-radius:20px;box-shadow:0 15px 35px rgba(255,105,135,.15);border:1px solid #ffd1dc}
h2{text-align:center;margin:0 0 5px;font-size:24px}
.b{text-align:center;background:linear-gradient(135deg,#ff758c,#ff7eb3);color:#fff;display:block;padding:5px 15px;border-radius:20px;font-size:12px;font-weight:600;margin:0 auto 20px;width:fit-content}
label{font-weight:600;margin-top:15px;display:block;font-size:14px}
input,textarea,select{width:100%;padding:12px;margin-top:6px;border-radius:10px;border:1.5px solid #ffd1dc;background:#fff9fa;box-sizing:border-box;font-family:Poppins;font-size:14px}
textarea{height:80px;resize:vertical}
.fb{margin-top:6px;background:#fff5f7;border:1.5px dashed #ff477e;padding:12px;border-radius:10px;text-align:center;cursor:pointer}
.fb input{display:none}
.fl{color:#ff477e;font-weight:500;font-size:13px;cursor:pointer}
button{padding:14px;border:none;border-radius:10px;font-weight:600;cursor:pointer;font-size:15px;width:100%;margin-top:15px;color:#fff}
.bs{background:linear-gradient(135deg,#ff477e,#ff1f59)}
.bc{background:linear-gradient(135deg,#3b82f6,#2563eb)}
.bt{background:linear-gradient(135deg,#ff6b6b,#ee5253)}
.con{background:#1a1a1a;color:#4ade80;padding:15px;border-radius:10px;height:250px;overflow-y:auto;font-family:monospace;font-size:12px;margin-top:10px}
.tb{margin-top:30px;border-top:1.5px dashed #ffd1dc;padding:15px;background:#fafafa;border-radius:15px}
.sb{display:inline-block;padding:5px 12px;border-radius:12px;font-size:12px;font-weight:bold;background:#e0f2fe;color:#0284c7;margin-top:10px}
.sb.err{background:#fee2e2;color:#dc2626}
.sb.ok{background:#dcfce7;color:#16a34a}
.note{font-size:12px;color:#6b7280;margin-top:4px}
</style></head><body><div class="c">
<h2>⚡ 24/7 Messenger Bot ⚡</h2>
<span class="b">DEVELOPER: VAMPIRE RULEX OWNER RAJ MISHRA • SAFE PACING</span>
<form id="f">
<label>Chat Mode:</label>
<select name="mode">
<option value="e2ee" selected>🔐 E2EE Chat (normal facebook cookies daalna)</option>
<option value="normal">💬 Normal Group/Chat (dc id facebook cookies)</option>
</select>
<label>Primary Raw Cookies (Required):</label>
<textarea name="cookies" placeholder="c_user=...; xs=...; fr=...;" required></textarea>
<label>Backup Raw Cookies (Optional):</label>
<textarea name="backupCookies" placeholder="Agar primary fail ho jaye to ye use hongi..."></textarea>
<label>Target ID:</label>
<input type="text" name="threadId" placeholder="E2EE / Normal Group or User ID" required>
<label>Haters Name (Prefix):</label>
<input type="text" name="hatersName" placeholder="Optional">
<label>Messages:</label>
<div class="fb" onclick="document.getElementById('mf').click()">
<span class="fl" id="fl">📁 Upload Messages File</span>
<input type="file" id="mf" accept=".txt" onchange="loadF(event)">
</div>
<textarea name="messages" id="mb" placeholder="Hello&#10;Test" required></textarea>
<label>Delay (Seconds):</label>
<input type="number" name="delay" value="20" min="15" required>
<div class="note">Safe mode mein minimum 15s delay</div>
<button type="submit" class="bs">🚀 Start 24/7 Safe Task</button>
</form>
<div class="tb">
<h3>🔍 Task Control</h3>
<label>Task ID:</label>
<input type="text" id="tid" placeholder="Paste Task ID">
<div style="display:flex;gap:10px">
<button type="button" class="bc" onclick="check()">👁️ Check</button>
<button type="button" class="bt" onclick="del()">🗑️ Delete</button>
</div>
<div style="margin-top:15px;border-top:1px dashed #ffd1dc;padding-top:10px">
<label>Update Primary Raw Cookies:</label>
<textarea id="nc" placeholder="Nayi primary cookies" style="height:50px"></textarea>
<label>Update Backup Raw Cookies:</label>
<textarea id="nb" placeholder="Nayi backup cookies" style="height:50px"></textarea>
<button type="button" class="bc" onclick="upd()">🔄 Update Cookies</button>
</div>
<div id="si" class="sb" style="display:none"></div>
<div class="con" id="cl">Waiting...</div>
</div></div>
<script>
let iv;
function loadF(e){const f=e.target.files[0];if(!f)return;document.getElementById('fl').innerText="📄 "+f.name;const r=new FileReader();r.onload=x=>document.getElementById('mb').value=x.target.result;r.readAsText(f);}
document.getElementById('f').addEventListener('submit',async e=>{
e.preventDefault();
const fd=new FormData(e.target);
const r=await fetch('/start-task',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(Object.fromEntries(fd))});
const j=await r.json();
if(j.success){alert("Task ID: "+j.taskId+"\\n\\nIse save karein!");document.getElementById('tid').value=j.taskId;check();}
else alert('Error: '+j.error);
});
function check(){
const id=document.getElementById('tid').value.trim();
if(!id)return alert('Task ID daaliye!');
if(iv)clearInterval(iv);
iv=setInterval(async()=>{
try{
const r=await fetch('/logs/'+id);const d=await r.json();
const cl=document.getElementById('cl'),si=document.getElementById('si');
if(d.success){
si.style.display="block";
si.className="sb"+(d.status==='error'?' err':(d.status==='running'?' ok':''));
si.innerHTML="🟢 "+d.status.toUpperCase()+" | ⏱️ "+d.uptime+" | "+d.activeCookieSource+" | "+(d.mode==='normal'?'💬 NORMAL':'🔐 E2EE');
cl.innerHTML=d.logs.join('<br>');cl.scrollTop=cl.scrollHeight;
if(d.status==='error'||d.status==='stopped'){clearInterval(iv);}
}else{clearInterval(iv);si.style.display="none";cl.innerHTML=d.message||"Not found!";}
}catch(e){}
},2000);
}
async function del(){
const id=document.getElementById('tid').value.trim();
if(!id)return alert('Task ID daaliye!');
if(confirm("STOP aur DELETE karein?")){
const r=await fetch('/stop-task/'+id,{method:'POST'});const j=await r.json();
alert(j.message);if(iv)clearInterval(iv);
document.getElementById('cl').innerHTML="Deleted.";document.getElementById('si').style.display="none";
}
}
async function upd(){
const id=document.getElementById('tid').value.trim();
const nc=document.getElementById('nc').value.trim();
const nb=document.getElementById('nb').value.trim();
if(!id)return alert('Task ID daaliye!');
if(!nc&&!nb)return alert('Cookies daaliye!');
const r=await fetch('/update-cookies/'+id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({newCookies:nc,newBackup:nb})});
const j=await r.json();alert(j.message);
if(j.success){document.getElementById('nc').value='';document.getElementById('nb').value='';}
}
</script></body></html>`);
});

// ==================== START TASK ====================
app.post('/start-task', async (req, res) => {
    try {
        const { mode, cookies, backupCookies, threadId, hatersName, messages, delay } = req.body;

        if (!cookies || !cookies.trim()) return res.status(400).json({ success: false, error: 'Cookies required' });
        if (!threadId || !threadId.trim()) return res.status(400).json({ success: false, error: 'Thread ID required' });
        if (!messages || !messages.trim()) return res.status(400).json({ success: false, error: 'Messages required' });

        const messageList = messages.split('\n').map(m => m.trim()).filter(Boolean);
        if (messageList.length === 0) return res.status(400).json({ success: false, error: 'No valid messages' });

        const taskMode = mode === 'normal' ? 'normal' : 'e2ee';
        if (taskMode === 'e2ee' && !FBClient) {
            return res.status(500).json({ success: false, error: 'fb-messenger-e2ee not installed' });
        }
        if (taskMode === 'normal' && !MetaClient) {
            return res.status(500).json({ success: false, error: 'meta-messenger.js not installed' });
        }

        const taskId = crypto.randomBytes(4).toString('hex');
        const safeDelaySec = Math.max(MIN_SAFE_DELAY_SEC, parseInt(delay) || MIN_SAFE_DELAY_SEC);

        let backup = (backupCookies || '').trim();
        if (backup && backup === cookies.trim()) backup = '';

        const task = {
            mode: taskMode,
            cookies: cookies.trim(),
            backupCookies: backup,
            threadId: threadId.trim(),
            hatersName: (hatersName || '').trim(),
            messages: messageList,
            delaySec: safeDelaySec,
            logs: [],
            status: 'running',
            startTime: Date.now(),
            activeCookieSource: 'primary',
            forceReconnect: false,
            client: null,
            connectFailCount: 0,
            sendFailCount: 0
        };

        pushLog(task, `Task ${taskId} created (${taskMode === 'normal' ? '💬 NORMAL' : '🔐 E2EE'}, min ${safeDelaySec}s + jitter).`);
        if (backup) pushLog(task, '🛡️ Backup cookies enabled.');

        activeTasks.set(taskId, task);

        await scheduleSave(true);
        res.json({ success: true, taskId });

        runPersistentTask(taskId);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// ==================== UPDATE COOKIES ====================
app.post('/update-cookies/:taskId', async (req, res) => {
    const task = activeTasks.get(req.params.taskId);
    if (!task || task.status !== 'running') {
        return res.status(404).json({ success: false, message: 'Task not found or not running.' });
    }

    if (req.body.newCookies && req.body.newCookies.trim()) {
        task.cookies = req.body.newCookies.trim();
    }
    if (req.body.newBackup !== undefined) {
        let newBackup = (req.body.newBackup || '').trim();
        if (newBackup && newBackup === task.cookies) newBackup = '';
        task.backupCookies = newBackup;
    }

    task.forceReconnect = true;
    task.activeCookieSource = 'primary';
    task.connectFailCount = 0;
    task.sendFailCount = 0;
    pushLog(task, '🔄 Cookies updated by user. Reconnecting safely...');

    await scheduleSave(true);
    res.json({ success: true, message: 'Cookies updated! Bot next cycle mein nayi cookies use karega.' });
});

// ==========================================
// 🚀 PERSISTENT TASK RUNNER (SAFE MODE)
// ==========================================
async function runPersistentTask(taskId) {
    const task = activeTasks.get(taskId);
    if (!task) return;

    let msgIndex = 0;
    let loopCount = 1;
    const isNormal = task.mode === 'normal';

    async function tryConnect(rawCookieString, slotName) {
        let client = null;
        try {
            if (isNormal) {
                const cookiesObj = parseRawCookies(rawCookieString);
                if (!cookiesObj.c_user || !cookiesObj.xs) {
                    throw new Error('c_user ya xs cookie missing hai');
                }

                client = new MetaClient(cookiesObj, {
                    platform: 'messenger',
                    enableE2EE: false,
                    autoReconnect: false,
                    logLevel: 'none'
                });

                await withTimeout(client.connect(), CONNECT_TIMEOUT_MS, 'connect');
                return client;
            }

            const appStateArr = rawCookiesToFacebookAppState(rawCookieString);
            const hasCUser = appStateArr.some(c => c.key === 'c_user');
            const hasXs = appStateArr.some(c => c.key === 'xs');
            if (!hasCUser || !hasXs) {
                throw new Error('c_user ya xs cookie missing hai');
            }

            const tempAppStatePath = path.join(SESSION_DIR, `appstate_${taskId}_${slotName}.json`);
            const sessionPath = path.join(SESSION_DIR, `session_${taskId}_${slotName}.json`);
            const devicePath = path.join(SESSION_DIR, `device_${taskId}_${slotName}.json`);

            fs.writeFileSync(tempAppStatePath, JSON.stringify(appStateArr, null, 2));

            client = new FBClient({
                appStatePath: tempAppStatePath,
                sessionStorePath: sessionPath,
                platform: 'facebook'
            });

            const { userId } = await withTimeout(client.connect(), CONNECT_TIMEOUT_MS, 'connect');
            await withTimeout(client.connectE2EE(devicePath, userId), CONNECT_TIMEOUT_MS, 'connectE2EE');
            return client;
        } catch (e) {
            closeClientSafe(client);
            throw e;
        }
    }

    async function doDisconnect() {
        const c = task.client;
        task.client = null;
        closeClientSafe(c);
    }

    async function sendToTarget(client, text) {
        if (isNormal) {
            if (!/^\d+$/.test(task.threadId)) {
                throw new Error('Normal mode ke liye numeric Thread ID chahiye');
            }
            return client.sendMessage(BigInt(task.threadId), text);
        }
        return client.sendMessage({
            threadId: task.threadId,
            text
        });
    }

    while (activeTasks.has(taskId) && task.status === 'running' && !isShuttingDown) {

        if (task.forceReconnect) {
            task.forceReconnect = false;
            await doDisconnect();
        }

        // ========== CONNECT PHASE ==========
        if (!task.client) {
            let connected = false;
            const tag = isNormal ? 'NORMAL' : 'E2EE';

            for (let i = 1; i <= PRIMARY_RETRIES; i++) {
                if (!activeTasks.has(taskId) || task.status !== 'running' || isShuttingDown) return;

                pushLog(task, `🔌 [PRIMARY/${tag}] Attempt ${i}/${PRIMARY_RETRIES}...`);
                await scheduleSave(false);

                try {
                    const c = await tryConnect(task.cookies, 'primary');
                    task.client = c;
                    task.activeCookieSource = 'primary';
                    task.connectFailCount = 0;
                    pushLog(task, `✅ Primary ${tag} connected!`);
                    await scheduleSave(true);
                    connected = true;
                    break;
                } catch (e) {
                    pushLog(task, `⚠️ Primary ${i}/${PRIMARY_RETRIES} failed: ${e.message}`);
                    await scheduleSave(false);
                    if (i < PRIMARY_RETRIES) {
                        const ok = await interruptibleSleep(8000);
                        if (!ok) return;
                    }
                }
            }

            if (!connected && task.backupCookies && task.backupCookies.trim()) {
                pushLog(task, '🔄 Primary fail. Backup try...');
                await scheduleSave(false);

                for (let i = 1; i <= BACKUP_RETRIES; i++) {
                    if (!activeTasks.has(taskId) || task.status !== 'running' || isShuttingDown) return;

                    pushLog(task, `🔌 [BACKUP/${tag}] Attempt ${i}/${BACKUP_RETRIES}...`);
                    await scheduleSave(false);

                    try {
                        const c = await tryConnect(task.backupCookies, 'backup');
                        task.client = c;
                        task.activeCookieSource = 'backup';
                        task.connectFailCount = 0;
                        pushLog(task, `✅ Backup ${tag} connected!`);
                        await scheduleSave(true);
                        connected = true;
                        break;
                    } catch (e) {
                        pushLog(task, `⚠️ Backup ${i}/${BACKUP_RETRIES} failed: ${e.message}`);
                        await scheduleSave(false);
                        if (i < BACKUP_RETRIES) {
                            const ok = await interruptibleSleep(8000);
                            if (!ok) return;
                        }
                    }
                }
            }

            if (!connected) {
                task.connectFailCount++;
                task.activeCookieSource = 'primary';

                if (task.connectFailCount >= MAX_CONNECT_FAILS) {
                    task.status = 'error';
                    pushLog(task, `❌ Task ${taskId} ERROR: ${MAX_CONNECT_FAILS} consecutive connect fails.`);
                    pushLog(task, '💡 Account/cookies check karein aur safe time baad nayi cookies update karein.');
                    await scheduleSave(true);
                    return;
                }

                const wait = backoffFor(task.connectFailCount);
                pushLog(task, `⏸️ Safe backoff (${task.connectFailCount}/${MAX_CONNECT_FAILS}): ${Math.round(wait / 1000)}s wait...`);
                await scheduleSave(false);

                const ok = await interruptibleSleep(wait);
                if (!ok) return;
                continue;
            }
        }

        // ========== SEND PHASE ==========
        const rawMsg = task.messages[msgIndex] || 'Hello';
        const finalMessage = task.hatersName ? `${task.hatersName} ${rawMsg}` : rawMsg;

        try {
            await withTimeout(
                sendToTarget(task.client, finalMessage),
                SEND_TIMEOUT_MS,
                'sendMessage'
            );

            const tag = isNormal ? 'NORMAL' : 'E2EE';
            pushLog(task, `🚀 [${task.activeCookieSource.toUpperCase()}/${tag}] Sent: ${finalMessage}`);
            task.sendFailCount = 0;

            msgIndex++;
            if (msgIndex >= task.messages.length) {
                msgIndex = 0;
                loopCount++;
                pushLog(task, `🔄 Round ${loopCount} started...`);
            }

            await scheduleSave(false);

            const waitMs = getSafeDelayMs(task.delaySec);
            const ok = await interruptibleSleep(waitMs);
            if (!ok) return;

        } catch (e) {
            task.sendFailCount++;
            pushLog(task, `⚠️ Send error (${task.sendFailCount}/${MAX_CONSECUTIVE_SEND_FAILS}): ${e.message}`);
            await doDisconnect();

            if (task.sendFailCount >= MAX_CONSECUTIVE_SEND_FAILS) {
                task.sendFailCount = 0;
                pushLog(task, `🛑 Repeated send errors—safe cool-down ${SEND_COOLDOWN_MS / 60000} min...`);
                await scheduleSave(true);
                const ok = await interruptibleSleep(SEND_COOLDOWN_MS);
                if (!ok) return;
            } else {
                const waitMs = 15000 * task.sendFailCount;
                await scheduleSave(false);
                const ok = await interruptibleSleep(waitMs);
                if (!ok) return;
            }
        }
    }

    await doDisconnect();
}

// ==================== LOGS ====================
app.get('/logs/:taskId', (req, res) => {
    const t = activeTasks.get(req.params.taskId);
    if (t) {
        const src = t.activeCookieSource === 'backup' ? '🍪 BACKUP' : '🍪 PRIMARY';
        res.json({
            success: true,
            mode: t.mode === 'normal' ? 'normal' : 'e2ee',
            status: t.status,
            uptime: getUptimeString(t.startTime),
            activeCookieSource: src,
            logs: t.logs
        });
    } else {
        res.json({ success: false, message: 'Task not found.' });
    }
});

// ==================== STOP TASK ====================
app.post('/stop-task/:taskId', async (req, res) => {
    const id = req.params.taskId;
    const t = activeTasks.get(id);
    if (!t) return res.status(404).json({ success: false, message: 'Task not found.' });

    t.status = 'stopped';
    closeClientSafe(t.client);
    t.client = null;

    activeTasks.delete(id);
    await scheduleSave(true);

    res.json({ success: true, message: `Task ${id} deleted!` });
});

// ==================== GRACEFUL SHUTDOWN ====================
async function gracefulShutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    for (const [id, t] of activeTasks.entries()) {
        t.status = 'stopped';
        closeClientSafe(t.client);
        t.client = null;
    }

    try { await doSave(); } catch (_) {}

    await new Promise(r => setTimeout(r, 500));
    process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown());
process.on('SIGTERM', () => gracefulShutdown());

// ==================== BOOT ====================
app.listen(PORT, '0.0.0.0', async () => {
    await loadTasks();
});
