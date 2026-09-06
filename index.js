// ============================================================
// 🤖 WHATSAPP BOT v3.2 - COMPATIBLE BAILEYS 6.7.9
// ✅ Corrigé: useMongoDBAuthState
// ============================================================

const mongoose = require('mongoose');
const express = require('express');
const { 
    makeWASocket, 
    DisconnectReason,
    useMultiFileAuthState,
    makeCacheableSignalKeyStore
} = require('@whiskeysockets/baileys');
const pino = require('pino');

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
// 🗄️ MODÈLE MONGODB POUR LES CRÉDENTIELS
// ============================================================

// Schéma pour stocker les crédentiels d'authentification
const AuthSchema = new mongoose.Schema({
    key: { type: String, required: true, unique: true },
    value: { type: mongoose.Schema.Types.Mixed, required: true }
}, { collection: 'wa_auth_store' });

const AuthModel = mongoose.model('Auth', AuthSchema);

/**
 * Crée un store d'authentification compatible avec Baileys utilisant MongoDB
 */
async function useMongoAuthState() {
    console.log('🔑 Chargement auth state depuis MongoDB...');
    
    const readData = {};
    const saveData = {};
    
    // Charger toutes les données depuis MongoDB
    try {
        const docs = await AuthModel.find({});
        for (const doc of docs) {
            // Reconstruire Buffer si nécessaire
            let value = doc.value;
            if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
                value = Buffer.from(value.data);
            }
            readData[doc.key] = value;
        }
        console.log(`   ✅ ${Object.keys(readData).length} clés chargées`);
    } catch(e) {
        console.log('   ℹ️  Pas de données existantes (première connexion?)');
    }
    
    // Fonction de sauvegarde
    const saveCreds = async (creds) => {
        try {
            const data = { ...readData, ...creds };
            
            for (const [key, value] of Object.entries(data)) {
                const valueToSave = value instanceof Buffer ? 
                    { type: 'Buffer', data: Array.from(value) } : value;
                
                await AuthModel.findOneAndUpdate(
                    { key },
                    { key, value: valueToSave },
                    { upsert: true }
                );
            }
            
            Object.assign(readData, creds);
            console.log('💾 Créds sauvegardés dans MongoDB');
        } catch(err) {
            console.error('❌ Erreur sauvegarde creds:', err.message);
        }
    };
    
    return {
        state: {
            creds: readData.creds || {},
            keys: {
                get: async (type, ids) => {
                    const result = {};
                    for (const id of ids) {
                        const key = `${type}-${id}`;
                        if (readData[key]) {
                            result[id] = readData[key];
                        }
                    }
                    return result;
                },
                set: async (data) => {
                    for (const type in data) {
                        for (const id in data[type]) {
                            const key = `${type}-${id}`;
                            readData[key] = data[type][id];
                            
                            // Sauvegarder immédiatement en DB
                            const value = readData[key];
                            const valueToSave = value instanceof Buffer ? 
                                { type: 'Buffer', data: Array.from(value) } : value;
                            
                            await AuthModel.findOneAndUpdate(
                                { key },
                                { key, value: valueToSave },
                                { upsert: true }
                            ).catch(() => {});
                        }
                    }
                }
            }
        },
        saveCreds
    };
}

// ============================================================
// 🌐 SERVEUR EXPRESS
// ============================================================

const app = express();
app.use(express.json());

app.get('/', (req, res) => {
    res.json({ 
        service: 'WhatsApp Bot v3.2', 
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
            return res.json({ queued: true, message: 'Message en attente' });
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

        // ✅ CORRECTION ICI : Utiliser notre propre fonction au lieu de useMongoDBAuthState
        console.log('🔑 Chargement auth state...');
        const { state, saveCreds } = await useMongoAuthState();
        console.log('✅ Auth state chargé\n');

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
            printQRInTerminal: false,  // On gère nous-même le QR
            
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

            // QR Code
            if (qr) {
                currentQR = qr;
                qrGeneratedAt = Date.now();
                console.log('\n📷 QR CODE GÉNÉRÉ !');
                console.log('   Disponible via GET /api/qr\n');
            }

            // Pairing code
            if (qr && !sock.authState.creds.registered && !pairingCodeRequested) {
                pairingCodeRequested = true;
                await sleep(5000);
                
                try {
                    const num = CONFIG.PAIRING_NUMBER.replace(/[^0-9]/g, '');
                    if (!num || num.length < 10) throw new Error('Numéro invalide');
                    
                    const code = await sock.requestPairingCode(num);
                    
                    console.log('\n' + '='.repeat(50));
                    console.log(`📱 NUMÉRO CIBLE : ${num}`);
                    console.log(`🔑 CODE D'APPAIRAGE : ${code}`);
                    console.log('=' .repeat(50));
                    console.log('\n⏰ Entrez ce code rapidement (expire en ~20s)\n');
                    
                } catch(e) {
                    console.error('❌ Erreur pairing code:', e.message);
                    pairingCodeRequested = false;
                }
            }

            // Connexion fermée
            if (connection === 'close') {
                isReady = false;
                isFullyInitialized = false;
                isBotStarting = false;
                errorHandled = false;
                currentQR = null;
                
                if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                
                const code = lastDisconnect?.error?.output?.statusCode;
                const reconnect = code !== DisconnectReason.loggedOut;
                
                console.log(`\n❌ Connexion fermée (Status: ${code || 'N/A'})`);
                console.log(`   Reconnexion auto: ${reconnect ? 'OUI ✅' : 'NON ❌'}\n`);
                
                if (reconnect) {
                    retryCount++;
                    const d = getDelay();
                    console.log(`🔄 Reconnexion dans ${(d/1000).toFixed(1)}s (retry #${retryCount})\n`);
                    reconnectTimeout = setTimeout(connectWhatsApp, d);
                } else {
                    console.log('⛔ Déconnexion volontaire (logged out)');
                    console.log('   → Supprimez la collection "wa_auth_store" et redémarrez\n');
                    retryCount = 0;
                }
            }

            // Connexion ouverte ✅
            if (connection === 'open') {
                isReady = true;
                isBotStarting = false;
                retryCount = 0;
                errorHandled = false;
                currentQR = null;
                
                console.log('\n✅'.repeat(30));
                console.log('  CONNEXION WHATSAPP RÉUSSIE !');
                console.log('✅'.repeat(30));
                console.log(`\n👤 Nom: ${sock.user?.name || 'Sans nom'}`);
                console.log(`📱 JID: ${sock.user?.id || 'Inconnu'}`);
                console.log(`\n⏳ Attente finalisation initialisation...`);
                console.log(`   (Max ${CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS / 1000}s avant reconnexion si échec)\n`);

                // Timeout si l'init bloque trop longtemps
                initTimeoutHandle = setTimeout(async () => {
                    if (!isFullyInitialized && isReady) {
                        console.warn('\n⚠️ Initialisation trop longue (>90s)');
                        console.warn('→ Reconnexion forcée planifiée...\n');
                        await forceReconnect();
                    }
                }, CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS);
            }
        });

        // Gestion erreurs socket
        sock.ev.on('error', async (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            const isInitError = stack.includes('chats.js') || stack.includes('fetchProps') || stack.includes('executeInitQueries');

            // ⭐ Timeout pendant l'init des chats → RECONNEXION FORCÉE
            if ((msg.includes('Timed Out') || stack.includes('Timed Out')) && isInitError) {
                if (!errorHandled) {
                    errorHandled = true;
                    
                    console.warn('\n' + '⚠️'.repeat(35));
                    console.warn('  TIMEOUT SYNCHRO CHATS DÉTECTÉ');
                    console.warn('  → Socket instable pour envoi');
                    console.warn('  → Reconnexion forcée immédiate');
                    console.warn('⚠️'.repeat(35) + '\n');
                    
                    if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                    await forceReconnect();
                }
                return;
            }

            // Autres timeouts (non critiques)
            if (msg.includes('Timed Out')) {
                console.warn('⚠️ Timeout socket (non-critique)\n');
                return;
            }

            // Stream errors (normaux)
            if (msg.includes('stream') || msg.includes('conflict') || msg.includes('Stream removed')) {
                return; // Silencieux
            }

            // Presence warning (inoffensif)
            if (msg.includes('no name present')) {
                return; // Silencieux
            }

            console.error('❌ Erreur socket inattendue:');
            console.error(`   Type: ${error.constructor.name}`);
            console.error(`   Message: ${msg.substring(0, 200)}\n`);
        });

        // Messages entrants
        sock.ev.on('messages.upsert', async ({ messages }) => {
            // Marquer comme initialisé quand on reçoit un message (preuve que ça marche)
            if (!isFullyInitialized && isReady && sock?.ws?.readyState === 1) {
                isFullyInitialized = true;
                if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
                
                console.log('\n' + '🎉'.repeat(25));
                console.log('  SOCKET PLEINEMENT INITIALISÉ !');
                console.log('  ✅ Prêt à envoyer/recevoir des messages');
                console.log('🎉'.repeat(25) + '\n');
                
                processQueue();
            }

            if (!isReady || !messages) return;

            for (const m of messages.filter(x => !x.notificationType && !x.key.fromMe)) {
                try {
                    const from = m.key.remoteJid;
                    const body = m.message?.conversation || m.message?.extendedTextMessage?.text || '';
                    console.log(`\n📩 Message reçu de ${from}:`);
                    console.log(`   "${body.substring(0, 100)}"\n`);
                    
                    // ════════════════════════════════════════
                    // VOTRE LOGIQUE DE TRAITEMENT ICI
                    // ════════════════════════════════════════
                    
                    // Exemple: Auto-réponse simple
                    /*
                    if (body.toLowerCase() === 'ping') {
                        await sendMessageSafe(from, { text: 'pong! 🏓' });
                    }
                    else if (body.toLowerCase() === 'heure') {
                        await sendMessageSafe(from, { text: `Il est ${new Date().toLocaleTimeString('fr-FR')}` });
                    }
                    */
                    
                } catch(e) {
                    console.error('❌ Erreur traitement message:', e.message);
                }
            }
        });

        console.log('✅ Socket WhatsApp créé avec succès\n');
        console.log('⏳ En attente des événements de connexion...\n');

        return sock;

    } catch(err) {
        console.error('\n' + '💥'.repeat(30));
        console.error(` ERREUR CRITIQUE: ${err.message}`);
        console.error('💥'.repeat(30) + '\n');
        
        isBotStarting = false;
        isFullyInitialized = false;
        pairingCodeRequested = false;
        retryCount++;
        
        const d = getDelay();
        console.error(`🔄 Nouvelle tentative dans ${(d/1000).toFixed(1)}s (retry #${retryCount})\n`);
        setTimeout(connectWhatsApp, d);
        return null;
    }
}

// ============================================================
// 🔧 FONCTIONS AUXILIAIRES
// ============================================================

async function forceReconnect() {
    console.log('\n🔁 DÉBUT RECONNEXION FORCÉE PROPRE...\n');
    
    try {
        if (sock) {
            console.log('   1/4 Suppression listeners...');
            sock.ev.removeAllListeners();
            
            if (sock.ws?.readyState === 1) {
                console.log('   2/4 Fermeture WebSocket...');
                sock.ws.close(4001, 'Timeout init - Reconnexion forcée');
            }
        }
        
        console.log('   3/4 Reset état global...');
        sock = null;
        isReady = false;
        isFullyInitialized = false;
        isBotStarting = false;
        pairingCodeRequested = false;
        
        if (initTimeoutHandle) { clearTimeout(initTimeoutHandle); initTimeoutHandle = null; }
        if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }
        
        console.log('   4/4 Attente 5s...');
        await sleep(5000);
        
        console.log('🚀 Relancement connexion...\n');
        await connectWhatsApp();
        
    } catch(err) {
        console.error('❌ Erreur reconnexion forcée:', err.message);
        retryCount++;
        setTimeout(connectWhatsApp, getDelay());
    }
}

/**
 * Envoie un message de manière sécurisée
 */
async function sendMessageSafe(jid, message) {
    if (!isFullyInitialized || !sock || sock.ws?.readyState !== 1) {
        sendQueue.push({ jid, message, time: Date.now() });
        console.log(`📤 Message enfilé (${sendQueue.length} en attente) → ${jid}`);
        return null;
    }

    try {
        console.log(`\n📤 ENVOI → ${jid}`);
        
        const result = await Promise.race([
            sock.sendMessage(jid, message),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Timeout après 20s')), 20000)
            )
        ]);
        
        console.log(`✅ MESSAGE ENVOYÉ AVEC SUCCÈS !`);
        console.log(`   ID: ${result?.key?.id}\n`);
        
        return result;
        
    } catch(err) {
        console.error(`❌ ÉCHEC ENVOI vers ${jid}:`);
        console.error(`   ${err.message}\n`);
        
        sendQueue.push({ jid, message, time: Date.now() });
        return null;
    }
}

/**
 * Traite la file d'attente des messages
 */
function processQueue() {
    if (sendQueue.length === 0 || !isFullyInitialized) return;
    
    console.log(`\n📬 Traitement file d'attante: ${sendQueue.length} message(s)\n`);
    
    while (sendQueue.length > 0) {
        const item = sendQueue.shift();
        sendMessageSafe(item.jid, item.message).catch(() => {});
    }
}

// ============================================================
// 🚀 DÉMARRAGE
// ============================================================

async function start() {
    console.log('\n' + '🚀'.repeat(35));
    console.log('  WHATSAPP BOT v3.2 - DÉMARRAGE');
    console.log('🚀'.repeat(35));
    console.log(`\n⏰ Heure: ${new Date().toISOString()}`);
    console.log(`🎯 Version: Compatible Baileys 6.7.9`);
    console.log(`📍 Env: ${process.env.NODE_ENV || 'development'}\n`);

    // Serveur HTTP (CRITIQUE pour Render/heroku)
    const server = app.listen(CONFIG.PORT, () => {
        console.log(`🌐 Serveur HTTP démarré sur port ${CONFIG.PORT}`);
        console.log(`   • Health: http://localhost:${CONFIG.PORT}/api/health`);
        console.log(`   • Send:   http://localhost:${CONFIG.PORT}/api/send`);
        console.log(`   • QR:     http://localhost:${CONFIG.PORT}/api/qr\n`);
    });

    server.keepAliveTimeout = 120000; // 2 min (important pour Render)

    // Gestion signaux système
    process.on('SIGTERM', () => { console.log('\n👋 SIGTERM reçu'); process.exit(0); });
    process.on('SIGINT', () => { console.log('\n👋 SIGINT reçu'); process.exit(0); });
    
    // Ne pas crasher sur erreurs non gérées
    process.on('unhandledRejection', (r) => console.error('💥 Unhandled Rejection:', r));
    process.on('uncaughtException', (e) => console.error('💥 Uncaught Exception:', e.message));

    // Démarrer WhatsApp
    await connectWhatsApp();

    // Health check périodique (toutes les 5 min)
    setInterval(() => {
        const status = isFullyInitialized ? '✅ OK' : isReady ? '⚠️ DEGRADED' : '❌ DOWN';
        const queueInfo = sendQueue.length > 0 ? ` | Queue: ${sendQueue.length}` : '';
        console.log(`${new Date().toISOString()} | ${status}${queueInfo}`);
    }, 300000);

    console.log('✨ Système prêt et en attente de connexions...\n');
}

// LANCER LE TOUT
start();
