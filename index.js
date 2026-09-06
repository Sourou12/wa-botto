// ============================================================
// 🤖 WHATSAPP BOT - VERSION COMPLÈTE EN UN SEUL FICHIER
// ✅ Fonctionne sur Render/Heroku sans configuration complexe
// ============================================================

const mongoose = require('mongoose');
const express = require('express');
const { 
    makeWASocket, 
    useMongoDBAuthState, 
    DisconnectReason,
    delay
} = require('@whiskeysockets/baileys');
const pino = require('pino');

// Chargement optionnel de dotenv (ne crashera pas si absent)
try { require('dotenv').config(); } catch(e) {}

// ============================================================
// ⚙️ CONFIGURATION
// ============================================================

const CONFIG = {
    MONGO_URI: process.env.MONGO_URI || 'mongodb://localhost:27017/whatsapp_bot',
    PAIRING_NUMBER: process.env.PAIRING_NUMBER || '',
    PORT: process.env.PORT || 3000,
    
    TIMEOUTS: {
        MAX_RETRIES: 10,
        CONNECT_MS: 300000,
        QUERY_MS: 600000,
        KEEPALIVE_MS: 45000,
        INIT_MAX_WAIT_MS: 90000,
        SEND_TIMEOUT_MS: 20000
    }
};

// ============================================================
// 📦 VARIABLES GLOBALES
// ============================================================

let sock = null;
let isBotStarting = false;
let isReady = false;
let isFullyInitialized = false;
let retryCount = 0;
let reconnectTimeout = null;
let pairingCodeRequested = false;
let currentQR = null;
let qrGeneratedAt = null;
let initTimeoutHandle = null;
let sendQueue = [];

// ============================================================
// 🌐 SERVEUR EXPRESS
// ============================================================

const app = express();
app.use(express.json());

app.get('/', (req, res) => {
    res.json({ 
        service: 'WhatsApp Bot', 
        status: 'running',
        endpoints: ['/api/health', '/api/send', '/api/qr']
    });
});

app.get('/api/health', (req, res) => {
    const wsState = sock?.ws?.readyState;
    res.json({
        status: isFullyInitialized && wsState === 1 ? 'OK' : isReady ? 'DEGRADED' : 'DOWN',
        ready: isReady,
        initialized: isFullyInitialized,
        socket: !!sock,
        wsState: wsState,
        retries: retryCount,
        queueSize: sendQueue.length,
        time: new Date().toISOString()
    });
});

app.post('/api/send', async (req, res) => {
    try {
        const { jid, message } = req.body;
        if (!jid || !message) return res.status(400).json({ error: 'jid et message requis' });
        
        if (!isFullyInitialized || !sock || sock.ws?.readyState !== 1) {
            sendQueue.push({ jid, message, time: Date.now() });
            return res.json({ queued: true, message: 'Message en attente de connexion' });
        }
        
        const result = await Promise.race([
            sock.sendMessage(jid, message),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), CONFIG.TIMEOUTS.SEND_TIMEOUT_MS))
        ]);
        
        res.json({ success: true, id: result?.key?.id });
        
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/qr', (req, res) => {
    res.json({ 
        qr: currentQR || null, 
        active: !!currentQR && (Date.now() - qrGeneratedAt) < 20000 
    });
});

// ============================================================
// 🛠️ UTILITAIRES
// ============================================================

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function getDelay() {
    let d = 15000 * Math.pow(2, retryCount);
    d = Math.min(d, 300000);
    return Math.round(d + (d * 0.25 * (Math.random() * 2 - 1)));
}

// ============================================================
// 🔌 CONNEXION WHATSAPP
// ============================================================

async function connectWhatsApp() {
    if (isBotStarting) return null;
    
    if (retryCount > CONFIG.TIMEOUTS.MAX_RETRIES) {
        console.log('💥 Max retries - Attente 2min...');
        isBotStarting = false;
        reconnectTimeout = setTimeout(() => { retryCount = 0; connectWhatsApp(); }, 120000);
        return null;
    }
    
    isBotStarting = true;
    isFullyInitialized = false;
    
    if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
    
    try {
        console.log('\n' + '='.repeat(50));
        console.log(`🔐 CONNEXION WHATSAPP (#${retryCount + 1})`);
        console.log('='.repeat(50));

        // MongoDB
        console.log('🗄️ MongoDB...');
        if (mongoose.connection.readyState !== 1) {
            await mongoose.connect(CONFIG.MONGO_URI, {
                serverSelectionTimeoutMS: 30000,
                socketTimeoutMS: 120000
            });
        }
        console.log('✅ MongoDB OK\n');

        // Auth
        const { state, saveCreds } = await useMongoDBAuthState();

        // Cleanup ancien socket
        if (sock) {
            try {
                sock.ev.removeAllListeners();
                if (sock.ws?.readyState === 1) sock.ws.close(1000, 'Reconnect');
            } catch(e) {}
            sock = null;
            isReady = false;
            await sleep(5000);
        }

        if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }

        // Création socket
        console.log('📱 Création socket...\n');
        
        sock = makeWASocket({
            auth: state,
            usePairingCode: true,
            syncFullHistory: false,
            shouldSyncHistoryMessage: () => false,
            
            connectTimeoutMs: CONFIG.TIMEOUTS.CONNECT_MS,
            queryTimeoutMs: CONFIG.TIMEOUTS.QUERY_MS,
            keepAliveIntervalMs: CONFIG.TIMEOUTS.KEEPALIVE_MS,
            
            browser: ["Ubuntu", "Chrome", "20.0"],
            logger: pino({ level: 'warn' }),
            markOnlineOnConnect: false,
            retryRequestDelayMs: 10000,
            maxMsgRetryCount: 5
        });

        // === EVENT LISTENERS ===
        
        sock.ev.on('creds.update', saveCreds);

        let errorHandled = false;

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            // QR
            if (qr) {
                currentQR = qr;
                qrGeneratedAt = Date.now();
                console.log('📷 QR Code généré');
            }

            // Pairing code
            if (qr && !sock.authState.creds.registered && !pairingCodeRequested) {
                pairingCodeRequested = true;
                await sleep(5000);
                
                try {
                    const num = CONFIG.PAIRING_NUMBER.replace(/[^0-9]/g, '');
                    if (!num || num.length < 10) throw new Error('Numéro invalide');
                    
                    const code = await sock.requestPairingCode(num);
                    console.log(`\n🔑 CODE: ${code}\n📱 NUMÉRO: ${num}\n`);
                } catch(e) {
                    console.error('❌ Pairing:', e.message);
                    pairingCodeRequested = false;
                }
            }

            // Close
            if (connection === 'close') {
                isReady = false;
                isFullyInitialized = false;
                isBotStarting = false;
                errorHandled = false;
                currentQR = null;
                
                if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                
                const code = lastDisconnect?.error?.output?.statusCode;
                const reconnect = code !== DisconnectReason.loggedOut;
                
                console.log(`\n❌ Fermée (${code}) - Reconnect: ${reconnect}`);
                
                if (reconnect) {
                    retryCount++;
                    const d = getDelay();
                    console.log(`🔄 Retry dans ${(d/1000).toFixed(1)}s\n`);
                    reconnectTimeout = setTimeout(connectWhatsApp, d);
                } else {
                    console.log('⛔ Logged out\n');
                    retryCount = 0;
                }
            }

            // Open
            if (connection === 'open') {
                isReady = true;
                isBotStarting = false;
                retryCount = 0;
                errorHandled = false;
                currentQR = null;
                
                console.log(`\n✅ CONNEXION RÉUSSIE !`);
                console.log(`👤 ${sock.user?.name || '?'}`);
                console.log(`📱 ${sock.user?.id || '?'}`);
                console.log(`\n⏳ Initialisation... (${CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS/1000}s max)\n`);

                // Timeout si l'init bloque trop longtemps
                initTimeoutHandle = setTimeout(async () => {
                    if (!isFullyInitialized && isReady) {
                        console.warn('⚠️ Init trop longue - Reconnexion...\n');
                        await forceReconnect();
                    }
                }, CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS);
            }
        });

        // Erreurs socket
        sock.ev.on('error', async (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            const isInitError = stack.includes('chats.js') || stack.includes('fetchProps');

            // Timeout pendant l'init des chats → RECONNEXION FORCÉE
            if ((msg.includes('Timed Out') || stack.includes('Timed Out')) && isInitError) {
                if (!errorHandled) {
                    errorHandled = true;
                    console.warn('\n⚠️ Timeout chats.js → Reconnexion forcée\n');
                    
                    if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                    await forceReconnect();
                }
                return;
            }

            // Autres timeouts (non critiques)
            if (msg.includes('Timed Out')) {
                console.warn('⚠️ Timeout (normal)\n');
                return;
            }

            // Stream errors (normaux)
            if (msg.includes('stream') || msg.includes('conflict')) return;

            // Presence warning (inoffensif)
            if (msg.includes('no name present')) return;

            console.error('❌ Socket error:', msg.substring(0, 150));
        });

        // Messages entrants
        sock.ev.on('messages.upsert', async ({ messages }) => {
            // Marquer comme initialisé quand on reçoit un message
            if (!isFullyInitialized && isReady && sock?.ws?.readyState === 1) {
                isFullyInitialized = true;
                if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                console.log('✅ Initialisation confirmée par message reçu\n');
                processQueue();
            }

            if (!isReady || !messages) return;

            for (const m of messages.filter(x => !x.notificationType && !x.key.fromMe)) {
                try {
                    const from = m.key.remoteJid;
                    const body = m.message?.conversation || m.message?.extendedTextMessage?.text || '';
                    console.log(`📩 ${from}: ${body.substring(0, 50)}`);
                    
                    // VOTRE LOGIQUE ICI (ex: auto-réponse)
                    // if (body === 'ping') await sendMessageSafe(from, { text: 'pong!' });
                    
                } catch(e) {
                    console.error('❌ Msg error:', e.message);
                }
            }
        });

        console.log('✅ Socket créé\n');
        return sock;

    } catch(err) {
        console.error('\n💥 ERREUR:', err.message);
        
        isBotStarting = false;
        isFullyInitialized = false;
        retryCount++;
        
        const d = getDelay();
        console.log(`🔄 Retry dans ${(d/1000).toFixed(1)}s\n`);
        setTimeout(connectWhatsApp, d);
        return null;
    }
}

// ============================================================
// 🔧 FONCTIONS AUXILIAIRES
// ============================================================

async function forceReconnect() {
    console.log('🔁 Reconnexion forcée...\n');
    
    try {
        if (sock) {
            sock.ev.removeAllListeners();
            if (sock.ws?.readyState === 1) sock.ws.close(4001, 'Force reconnect');
        }
    } catch(e) {}
    
    sock = null;
    isReady = false;
    isFullyInitialized = false;
    isBotStarting = false;
    
    if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
    if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }
    
    await sleep(5000);
    await connectWhatsApp();
}

async function sendMessageSafe(jid, message) {
    if (!isFullyInitialized || !sock || sock.ws?.readyState !== 1) {
        sendQueue.push({ jid, message, time: Date.now() });
        console.log(`📤 Enfilé → ${jid}`);
        return null;
    }

    try {
        const result = await Promise.race([
            sock.sendMessage(jid, message),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 20000))
        ]);
        console.log(`✅ Envoyé → ${jid}`);
        return result;
    } catch(err) {
        console.error(`❌ Échec envoi → ${jid}:`, err.message);
        sendQueue.push({ jid, message, time: Date.now() });
        return null;
    }
}

function processQueue() {
    if (sendQueue.length === 0 || !isFullyInitialized) return;
    
    console.log(`\n📬 File d'attente: ${sendQueue.length} message(s)\n`);
    
    while (sendQueue.length > 0) {
        const item = sendQueue.shift();
        sendMessageSafe(item.jid, item.message).catch(() => {});
    }
}

// ============================================================
// 🚀 DÉMARRAGE
// ============================================================

async function start() {
    console.log('\n' + '🚀'.repeat(30));
    console.log(' WHATSAPP BOT - DÉMARRAGE');
    console.log('🚀'.repeat(30));
    console.log(`⏰ ${new Date().toISOString()}\n`);

    // Démarrer serveur HTTP (CRITIQUE pour Render)
    const server = app.listen(CONFIG.PORT, () => {
        console.log(`🌐 Serveur: http://localhost:${CONFIG.PORT}\n`);
    });

    server.keepAliveTimeout = 120000;

    // Gestion signaux
    process.on('SIGTERM', () => { console.log('👋 SIGTERM'); process.exit(0); });
    process.on('SIGINT', () => { console.log('👋 SIGINT'); process.exit(0); });
    
    // Ne pas crasher sur erreurs non gérées
    process.on('unhandledRejection', (r) => console.error('💥 Unhandled:', r));
    process.on('uncaughtException', (e) => console.error('💥 Uncaught:', e.message));

    // Démarrer WhatsApp
    await connectWhatsApp();

    // Health check périodique
    setInterval(() => {
        const state = isFullyInitialized ? '✅' : isReady ? '⚠️' : '❌';
        console.log(`${new Date().toISOString()} | ${state} | Queue: ${sendQueue.length}`);
    }, 300000);

    console.log('✅ Système prêt !\n');
}

// LANCER
start();
