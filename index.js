// ============================================================
// 🔐 WHATSAPP BOT v3.1 - VERSION STABLE SANS DOTENV
// ============================================================

const mongoose = require('mongoose');
const { 
    makeWASocket, 
    useMongoDBAuthState, 
    DisconnectReason,
    delay
} = require('@whiskeysockets/baileys');
const pino = require('pino');

// Chargement optionnel de dotenv
try {
    require('dotenv').config();
} catch (e) {
    // dotenv non disponible - OK si variables déjà définies dans l'environnement
}

// ============================================================
// ⚙️ CONFIGURATION
// ============================================================

const CONFIG = {
    MONGO_URI: process.env.MONGO_URI || 'mongodb://localhost:27017/whatsapp_bot',
    PAIRING_NUMBER: process.env.PAIRING_NUMBER || '',
    
    TIMEOUTS: {
        MAX_RETRIES: 10,
        CONNECT_MS: 300000,
        QUERY_MS: 600000,
        KEEPALIVE_MS: 45000,
        INIT_MAX_WAIT_MS: 90000,
        SEND_MESSAGE_TIMEOUT_MS: 20000,
    },
    
    RETRY: {
        BASE_DELAY_MS: 15000,
        MAX_DELAY_MS: 300000,
        BACKOFF_FACTOR: 2,
        JITTER_PERCENT: 0.25
    }
};

// ============================================================
// 📦 ÉTAT GLOBAL
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
let connectionOpenCount = 0;
let initTimeoutHandle = null;
let lastMessageSentTime = null;
let sendQueue = [];

// ============================================================
// 🛠️ UTILITAIRES
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getProgressiveDelay() {
    const { BASE_DELAY_MS, MAX_DELAY_MS, BACKOFF_FACTOR, JITTER_PERCENT } = CONFIG.RETRY;
    
    let delay = BASE_DELAY_MS * Math.pow(BACKOFF_FACTOR, retryCount);
    delay = Math.min(delay, MAX_DELAY_MS);
    
    const jitter = delay * JITTER_PERCENT * (Math.random() * 2 - 1);
    delay = Math.round(delay + jitter);
    
    return Math.max(delay, BASE_DELAY_MS);
}

function checkSocketHealth() {
    const health = {
        timestamp: new Date().toISOString(),
        healthy: false,
        ready: false,
        fullyInitialized: false,
        canSend: false,
        
        details: {
            hasSocket: false,
            wsState: null,
            wsStateText: 'unknown',
            hasAuth: false,
            userId: null,
            userName: null,
            retryCount: retryCount,
            connectionCount: connectionOpenCount,
            uptime: null,
            lastMessageSent: lastMessageSentTime
        }
    };
    
    if (!sock) return health;
    
    health.details.hasSocket = true;
    const wsState = sock.ws?.readyState;
    health.details.wsState = wsState;
    health.details.wsStateText = {0:'connecting',1:'open',2:'closing',3:'closed'}[wsState] || `unknown(${wsState})`;
    health.details.hasAuth = !!sock.authState?.creds?.registered;
    health.details.userId = sock.user?.id || null;
    health.details.userName = sock.user?.name || null;
    
    health.ready = isReady && wsState === 1;
    health.fullyInitialized = isFullyInitialized;
    health.canSend = health.ready && health.fullyInitialized && health.details.hasAuth;
    health.healthy = health.canSend;
    
    return health;
}

function queueMessage(jid, message, options = {}) {
    sendQueue.push({ jid, message, options, timestamp: Date.now(), attempts: 0 });
    console.log(`📤 Message enfilé (${sendQueue.length} en attente) → ${jid}`);
}

async function processSendQueue() {
    if (sendQueue.length === 0 || !canSendMessage()) return;
    
    console.log(`\n📬 Traitement file: ${sendQueue.length} message(s)`);
    
    while (sendQueue.length > 0) {
        const item = sendQueue.shift();
        item.attempts++;
        
        try {
            await sendMessageInternal(item.jid, item.message, item.options);
            console.log(`✅ Message traité → ${item.jid}`);
            await sleep(500);
        } catch (err) {
            if (item.attempts < 3) {
                sendQueue.unshift(item);
                break;
            }
            console.error(`❌ Abandon après ${item.attempts} tentatives`);
        }
    }
}

function canSendMessage() {
    return checkSocketHealth().canSend;
}

// ============================================================
// 🚀 ENVOI MESSAGES
// ============================================================

async function sendMessage(jid, message, options = {}) {
    if (!jid || typeof jid !== 'string') throw new Error('JID invalide ou manquant');
    if (!message || typeof message !== 'object') throw new Error('Message invalide ou manquant');
    
    if (!canSendMessage()) {
        console.log(`⏳ Socket pas prêt - Message enfilé pour ${jid}`);
        queueMessage(jid, message, options);
        return { queued: true, jid, timestamp: Date.now() };
    }
    
    return sendMessageInternal(jid, message, options);
}

async function sendMessageInternal(jid, message, options = {}) {
    const { SEND_MESSAGE_TIMEOUT_MS } = CONFIG.TIMEOUTS;
    
    console.log(`\n📤 ENVOI → ${jid}`);
    console.log(`   Type: ${Object.keys(message)[0] || 'unknown'}`);
    
    try {
        const result = await Promise.race([
            sock.sendMessage(jid, message, options),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error(`Timeout après ${SEND_MESSAGE_TIMEOUT_MS / 1000}s`)), SEND_MESSAGE_TIMEOUT_MS)
            )
        ]);
        
        lastMessageSentTime = new Date().toISOString();
        
        console.log(`✅ MESSAGE ENVOYÉ !`);
        console.log(`   ID: ${result?.key?.id || 'N/A'}\n`);
        
        setTimeout(processSendQueue, 1000);
        return result;
        
    } catch (err) {
        console.error(`❌ ÉCHEC ENVOI → ${jid}: ${err.message}`);
        await handleSendError(err, jid, message, options);
        throw err;
    }
}

async function handleSendError(err, jid, originalMessage, originalOptions) {
    const msg = err.message || '';
    
    if (msg.includes('Timed Out') || msg.includes('timeout')) {
        isFullyInitialized = false;
        if (retryCount >= 3) scheduleReconnect();
    } else if (msg.includes('Connection Closed') || msg.includes('closed') || msg.includes('not open')) {
        isReady = false;
        isFullyInitialized = false;
        scheduleReconnect();
    } else if (msg.includes('403') || msg.includes('forbidden')) {
        console.error('   ⛔ Accès refusé - Vérifiez que le numéro n\'a pas bloqué le bot');
    } else if (msg.includes('428') || msg.includes('precondition') || msg.includes('initialization')) {
        isFullyInitialized = false;
        scheduleReconnect();
    }
}

function scheduleReconnect() {
    if (reconnectTimeout) clearTimeout(reconnectTimeout);
    
    const delay = getProgressiveDelay();
    console.log(`\n🔄 Reconnexion dans ${(delay / 1000).toFixed(1)}s`);
    reconnectTimeout = setTimeout(() => connectWhatsApp(), delay);
}

// ============================================================
// 🔌 CONNEXION PRINCIPALE
// ============================================================

async function connectWhatsApp() {
    if (isBotStarting) {
        console.log('⚠️ Connexion déjà en cours, skip...');
        return null;
    }
    
    if (retryCount > CONFIG.TIMEOUTS.MAX_RETRIES) {
        console.error(`💥 Max retries atteint (${CONFIG.TIMEOUTS.MAX_RETRIES})`);
        isBotStarting = false;
        
        reconnectTimeout = setTimeout(() => {
            retryCount = 0;
            connectWhatsApp();
        }, 120000);
        
        return null;
    }
    
    isBotStarting = true;
    isFullyInitialized = false;
    
    if (initTimeoutHandle) {
        clearTimeout(initTimeoutHandle);
        initTimeoutHandle = null;
    }
    
    try {
        console.log('\n' + '='.repeat(60));
        console.log(`🔐 CONNEXION WHATSAPP v3.1`);
        console.log(`📊 Tentative #${retryCount + 1}/${CONFIG.TIMEOUTS.MAX_RETRIES + 1}`);
        console.log(`🕐 ${new Date().toISOString()}`);
        console.log('='.repeat(60) + '\n');

        // MongoDB
        console.log('🗄️ Connexion MongoDB...');
        
        if (mongoose.connection.readyState !== 1) {
            await mongoose.connect(CONFIG.MONGO_URI, {
                serverSelectionTimeoutMS: 30000,
                socketTimeoutMS: 120000,
                maxPoolSize: 10,
                bufferCommands: false
            });
        }
        console.log('✅ MongoDB connecté !\n');

        // Auth
        const { state, saveCreds } = await useMongoDBAuthState();

        // Cleanup ancien socket
        if (sock) {
            console.log('🔄 Cleanup ancien socket...');
            
            try {
                sock.ev.removeAllListeners('connection.update');
                sock.ev.removeAllListeners('creds.update');
                sock.ev.removeAllListeners('error');
                sock.ev.removeAllListeners('messages.upsert');
                
                if (sock.ws?.readyState === 1) {
                    sock.ws.close(1000, 'Reconnexion planifiée');
                }
            } catch (e) {
                console.log('   ⚠️', e.message);
            }
            
            sock = null;
            isReady = false;
            isFullyInitialized = false;
            await sleep(5000);
        }

        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }

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
            
            retryRequestDelayMs: 10000,
            maxMsgRetryCount: 5,
            
            browser: ["Ubuntu", "Chrome", "20.0.04"],
            
            logger: pino({ level: 'warn' }),
            markOnlineOnConnect: false
        });

        // Event listeners
        
        sock.ev.on('creds.update', async (creds) => {
            try { await saveCreds(creds); } catch (e) { console.error('❌ Erreur creds:', e.message); }
        });

        let connectionErrorHandled = false;
        
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                currentQR = qr;
                qrGeneratedAt = Date.now();
                console.log('📷 QR généré');
            }

            if (qr && !sock.authState.creds.registered && !pairingCodeRequested) {
                pairingCodeRequested = true;
                await sleep(5000);

                try {
                    const cleanNumber = CONFIG.PAIRING_NUMBER.replace(/[^0-9]/g, '');
                    
                    if (!cleanNumber || cleanNumber.length < 10) {
                        throw new Error(`Numéro invalide: ${CONFIG.PAIRING_NUMBER}`);
                    }
                    
                    const code = await sock.requestPairingCode(cleanNumber);
                    
                    console.log('\n' + '='.repeat(50));
                    console.log(`📱 NUMÉRO : ${cleanNumber}`);
                    console.log(`🔑 CODE : ${code}`);
                    console.log('='.repeat(50) + '\n');
                    
                } catch (err) {
                    console.error('❌ Pairing:', err.message);
                    pairingCodeRequested = false;
                }
            }

            if (connection === 'close') {
                isReady = false;
                isFullyInitialized = false;
                isBotStarting = false;
                pairingCodeRequested = false;
                currentQR = null;
                qrGeneratedAt = null;
                connectionErrorHandled = false;
                
                if (initTimeoutHandle) {
                    clearTimeout(initTimeoutHandle);
                    initTimeoutHandle = null;
                }
                
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                
                console.log('\n❌ Connexion fermée (Status: ' + (statusCode || 'N/A') + ')');
                
                if (shouldReconnect) {
                    retryCount++;
                    const delay = getProgressiveDelay();
                    console.log(`🔄 Reconnexion dans ${(delay / 1000).toFixed(1)}s (#${retryCount})\n`);
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), delay);
                } else {
                    console.log('⛔ Logged out\n');
                    retryCount = 0;
                }
            }

            if (connection === 'open') {
                isReady = true;
                isBotStarting = false;
                retryCount = 0;
                connectionErrorHandled = false;
                connectionOpenCount++;
                currentQR = null;
                qrGeneratedAt = null;
                
                console.log('\n✅'.repeat(30));
                console.log(' CONNEXION RÉUSSIE !');
                console.log('✅'.repeat(30));
                console.log(`👤 ${sock.user?.name || 'Sans nom'}`);
                console.log(`📱 ${sock.user?.id || 'inconnu'}`);
                console.log(`🔢 #${connectionOpenCount}`);
                console.log(`\n⏳ Attente initialisation... (${CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS / 1000}s max)\n`);
                
                initTimeoutHandle = setTimeout(async () => {
                    if (!isFullyInitialized && isReady) {
                        console.warn('\n⚠️ Init trop longue - Reconnexion forcée\n');
                        await forceCleanReconnect();
                    }
                }, CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS);
            }
        });

        // Gestion erreurs
        sock.ev.on('error', async (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            const isErrorInitChats = stack.includes('chats.js') || stack.includes('fetchProps') || stack.includes('executeInitQueries');
            
            if ((msg.includes('Timed Out') || stack.includes('Timed Out')) && isErrorInitChats) {
                if (!connectionErrorHandled) {
                    connectionErrorHandled = true;
                    
                    console.warn('\n⚠️ Timeout chats.js détecté - Reconnexion forcée\n');
                    
                    if (initTimeoutHandle) {
                        clearTimeout(initTimeoutHandle);
                        initTimeoutHandle = null;
                    }
                    
                    await forceCleanReconnect();
                }
                return;
            }
            
            if (msg.includes('Timed Out') || stack.includes('Timed Out')) {
                console.warn('⚠️ Timeout socket (non-critique)\n');
                return;
            }
            
            if (msg.includes('stream') || msg.includes('conflict')) {
                console.warn('⚠️ Stream error (normal)\n');
                return;
            }
            
            if (msg.includes('no name present')) {
                return; // Silencieux
            }
            
            console.error('❌ Socket error:', msg.substring(0, 200));
        });

        // Messages entrants
        sock.ev.on('messages.upsert', async ({ messages }) => {
            if (!isFullyInitialized && isReady && sock?.ws?.readyState === 1) {
                console.log('📨 Message reçu → Init confirmée');
                markAsFullyInitialized();
            }
            
            if (!isReady || !messages) return;
            
            try {
                for (const msg of messages.filter(m => !m.notificationType && !m.key.fromMe)) {
                    // Votre logique ici
                    console.log(`📩 De ${msg.key.remoteJid}: ${(msg.message?.conversation || '').substring(0, 50)}`);
                }
            } catch (e) {
                console.error('❌ Erreur traitement:', e.message);
            }
        });

        console.log('✅ Socket créé - Attente événements...\n');
        return sock;

    } catch (err) {
        console.error('\n💥 ERREUR CRITIQUE:', err.message);
        
        isBotStarting = false;
        isFullyInitialized = false;
        pairingCodeRequested = false;
        retryCount++;
        
        const delay = getProgressiveDelay();
        console.error(`🔄 Retry dans ${(delay / 1000).toFixed(1)}s (#${retryCount})\n`);
        
        setTimeout(() => connectWhatsApp(), delay);
        return null;
    }
}

// ============================================================
// 🔧 FONCTIONS AUXILIAIRES
// ============================================================

function markAsFullyInitialized() {
    if (!isFullyInitialized) {
        isFullyInitialized = true;
        
        if (initTimeoutHandle) {
            clearTimeout(initTimeoutHandle);
            initTimeoutHandle = null;
        }
        
        console.log('\n🎉 SOCKET PLEINEMENT INITIALISÉ - Prêt à envoyer !\n');
        setTimeout(processSendQueue, 2000);
    }
}

async function forceCleanReconnect() {
    console.log('\n🔁 RECONNEXION FORCÉE...\n');
    
    try {
        if (sock) {
            sock.ev.removeAllListeners();
            if (sock.ws?.readyState === 1) {
                sock.ws.close(4001, 'Timeout init - Reconnexion');
            }
        }
        
        sock = null;
        isReady = false;
        isFullyInitialized = false;
        isBotStarting = false;
        pairingCodeRequested = false;
        
        if (initTimeoutHandle) {
            clearTimeout(initTimeoutHandle);
            initTimeoutHandle = null;
        }
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }
        
        await sleep(5000);
        await connectWhatsApp();
        
    } catch (err) {
        console.error('❌ Erreur reconnexion:', err.message);
        retryCount++;
        setTimeout(() => connectWhatsApp(), getProgressiveDelay());
    }
}

// ============================================================
// 🌐 API ROUTES
// ============================================================

function setupAPIRoutes(app) {
    app.get('/api/health', (req, res) => {
        const health = checkSocketHealth();
        res.json({
            service: 'whatsapp-bot-v3.1',
            status: health.healthy ? 'OK' : health.ready ? 'DEGRADED' : 'DOWN',
            ...health,
            queuedMessages: sendQueue.length
        });
    });
    
    app.post('/api/send', async (req, res) => {
        try {
            const { jid, message, options } = req.body;
            if (!jid || !message) return res.status(400).json({ error: 'jid et message requis' });
            
            const result = await sendMessage(jid, message, options);
            res.json({ success: true, result });
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });
    
    app.get('/api/qr', (req, res) => {
        if (!currentQR) return res.json({ qr: null, status: 'waiting' });
        
        res.json({
            qr: currentQR,
            ageSeconds: Math.round((Date.now() - qrGeneratedAt) / 1000),
            expired: (Date.now() - qrGeneratedAt) > 20000,
            status: 'active'
        });
    });
    
    console.log('✅ Routes API: /health, /send, /qr');
}

// ============================================================
// 🚀 DÉMARRAGE
// ============================================================

async function startBot() {
    console.log('\n🚀 WHATSAPP BOT v3.1 - Démarrage...');
    console.log(`🕐 ${new Date().toISOString()}\n`);
    
    await connectWhatsApp();
    
    // Health check toutes les 5 min
    setInterval(() => {
        const h = checkSocketHealth();
        console.log(`${new Date().toISOString()} | ${h.healthy ? '✅' : h.ready ? '⚠️' : '❌'} | Init:${h.fullyInitialized} | Queue:${sendQueue.length}`);
    }, 300000);
}

// ============================================================
// 📦 EXPORTS
// ============================================================

module.exports = {
    connectWhatsApp,
    startBot,
    setupAPIRoutes,
    sendMessage,
    canSendMessage,
    checkSocketHealth,
    get isReady() { return isReady; },
    get isFullyInitialized() { return isFullyInitialized; },
    getConnectionInfo: () => ({
        sock: !!sock,
        isReady,
        isFullyInitialized,
        retryCount,
        connectionOpenCount,
        queuedMessages: sendQueue.length,
        lastMessageSent: lastMessageSentTime
    })
};
