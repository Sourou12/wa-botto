/**
 * WHATSAPP BOT v3.2.3 - RENDER/NORTHFLANK OPTIMIZED ULTIMATE (CORRIGÉ)
 * 
 * ✅ Corrections v3.2.2 :
 * - Code orphelin supprimé (handlers en dehors de fonction)
 * - Redéclaration pairingCodeRequested corrigée
 * - currentQR / qrGeneratedAt mis à jour correctement
 * - Sauvegarde bulk job corrigée (findOneAndUpdate par jobId)
 * - currentIndex mis à jour AVANT envoi (évite doublons)
 * - Status final n'écrase plus 'cancelled'
 * - Timeout ajouté dans boucle d'attente reconnexion
 * - sock.end() remplacé par sock.ws?.close()
 * - reconnectTimeout stocké et nettoyé
 * - Gestion DisconnectReason.loggedOut
 * - startIndex ajouté au schéma Mongoose
 *
 * ✅ Nouveautés v3.2.3 :
 * - Reconnexion automatique même après logged-out (401) : suppression
 *   des credentials MongoDB + relance connectWhatsApp() -> nouveau code
 *   de parrainage généré automatiquement dans les logs (à ressaisir
 *   manuellement, WhatsApp l'exige, mais le bot ne reste plus bloqué)
 * - Rate limiting resserré pour un usage secondaire (~1000 msg/semaine,
 *   Baileys en canal complémentaire à l'API Cloud officielle) :
 *   plafond quotidien abaissé, pauses longues plus fréquentes
 */

const { 
    default: makeWASocket, 
    initAuthCreds, 
    BufferJSON, 
    DisconnectReason,
    proto,
    makeCacheableSignalKeyStore,
    useMultiFileAuthState
} = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const QRCode = require('qrcode');
const express = require('express');
const mongoose = require('mongoose');
const pino = require('pino');

const app = express();
app.use(express.json({ limit: '5mb' }));

// ==================== MIDDLEWARES ====================
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ==================== CONFIGURATION ====================
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI;

// ⭐ CONFIGURATION TIMEOUTS
const TIMEOUT_CONFIG = {
    BASE_CONNECT_TIMEOUT: 120000,
    BASE_QUERY_TIMEOUT: 120000,
    MAX_TIMEOUT: 180000,
    KEEP_ALIVE_INTERVAL: 25000,
    RETRY_DELAY_BASE: 10000,
    RETRY_DELAY_MAX: 60000,
    MAX_RETRIES: 5
};

// ⭐ NUMÉRO DE TÉLÉPHONE POUR LE CODE DE PARRAINAGE
// Remplacez par le numéro qui doit recevoir le code, ou définissez-le dans les variables d'environnement
const PAIRING_NUMBER = process.env.PAIRING_NUMBER || '2290140443431';

// ==================== VARIABLES GLOBALES ====================
let sock = null;
let isReady = false;
let connectionOpenCount = 0;
let isBotStarting = false;
let reconnectTimeout = null;
let retryCount = 0;
let currentQR = null;
let qrGeneratedAt = null;
let pairingCodeRequested = false;

// ==================== MODÈLE MONGODB ====================
const AuthSchema = new mongoose.Schema({
    _id: { type: String, required: true },
    data: { type: String, required: true }
});
const AuthModel = mongoose.models.AuthState || mongoose.model('AuthState', AuthSchema);

const BulkJobSchema = new mongoose.Schema({
    jobId: { type: String, unique: true },
    items: [{ number: String, message: String }],
    currentIndex: { type: Number, default: 0 },
    startIndex: { type: Number, default: 0 },
    sentCount: { type: Number, default: 0 },
    failedCount: { type: Number, default: 0 },
    results: [{
        number: String,
        jid: String,
        status: String,
        id_message: String,
        error: String,
        timestamp: Date
    }],
    status: { 
        type: String, 
        enum: ['pending', 'running', 'completed', 'cancelled', 'paused_daily_limit', 'failed'],
        default: 'pending'
    },
    cancelled: { type: Boolean, default: false },
    config: {
        minDelaySec: Number,
        maxDelaySec: Number,
        batchSize: Number,
        batchPauseMinutes: Number,
        dailyLimit: Number
    },
    sentToday: { type: Number, default: 0 },
    currentDay: String,
    startedAt: Date,
    finishedAt: Date,
    nextSendAt: Date,
    createdAt: { type: Date, default: Date.now }
}, { collection: 'bulkjobs' });

const BulkJobModel = mongoose.models.BulkJob || mongoose.model('BulkJob', BulkJobSchema);
let bulkJob = null;
let isProcessing = false;

// ==================== AUTH MONGODB ====================
async function useMongoDBAuthState() {
    console.log('📦 Initialisation auth MongoDB...');
    
    const readData = async (id) => {
        try {
            const doc = await AuthModel.findById(id).lean().maxTimeMS(10000);
            if (!doc) return undefined;
            return JSON.parse(doc.data, BufferJSON.reviver);
        } catch (err) {
            console.error(`❌ Erreur lecture Mongo (${id}):`, err.message);
            return undefined;
        }
    };

    const writeData = async (id, data) => {
        try {
            if (data === undefined || data === null) {
                await AuthModel.findByIdAndDelete(id);
            } else {
                const value = JSON.stringify(data, BufferJSON.replacer);
                await AuthModel.findByIdAndUpdate(id, { data: value }, { upsert: true, new: true });
            }
        } catch (err) {
            console.error(`❌ Erreur écriture Mongo (${id}):`, err.message);
        }
    };

    let creds = await readData('creds');
    if (!creds) {
        console.log('🆕 Nouvelle session - Génération creds initiaux...');
        creds = initAuthCreds();
    } else {
        console.log('✅ Session existante chargée depuis MongoDB');
    }

    const saveCreds = async () => await writeData('creds', creds);

    const keys = {
        get: async (type, ids) => {
            const data = {};
            await Promise.allSettled(ids.map(async (id) => {
                try {
                    let value = await readData(`${type}-${id}`);
                    if (type === 'app-state-sync-key' && value && typeof value === 'object') {
                        try { value = proto.Message.AppStateSyncKeyData.fromObject(value); } catch(e) {}
                    }
                    data[id] = value;
                } catch (error) {
                    data[id] = undefined;
                }
            }));
            return data;
        },
        
        set: async (data) => {
            const tasks = [];
            for (const category in data) {
                for (const id in data[category]) {
                    const value = data[category][id];
                    const key = `${category}-${id}`;
                    tasks.push(value ? writeData(key, value) : AuthModel.findByIdAndDelete(key));
                }
            }
            await Promise.allSettled(tasks);
        }
    };

    return { state: { creds, keys }, saveCreds };
}

// ==================== UTILITAIRES ====================
function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');
    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) cleanNumber = '229' + cleanNumber.slice(2);
    else if (cleanNumber.length === 8) cleanNumber = '229' + cleanNumber;
    cleanNumber = cleanNumber.replace(/^\+/, '');
    return `${cleanNumber}@s.whatsapp.net`;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function randomDelayMs(minSeconds, maxSeconds) {
    const min = Math.ceil(minSeconds * 1000);
    const max = Math.floor(maxSeconds * 1000);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function todayStr() {
    return new Date().toISOString().slice(0, 10);
}

function getProgressiveDelay() {
    const delay = Math.min(
        TIMEOUT_CONFIG.RETRY_DELAY_BASE * Math.pow(1.5, retryCount),
        TIMEOUT_CONFIG.RETRY_DELAY_MAX
    );
    return Math.round(delay);
}

function getAdaptiveTimeout() {
    const timeout = Math.min(
        TIMEOUT_CONFIG.BASE_CONNECT_TIMEOUT + (retryCount * 15000),
        TIMEOUT_CONFIG.MAX_TIMEOUT
    );
    return Math.round(timeout);
}

// ==================== CONFIG RATE LIMITING ====================
// ⚙️ Ajusté v3.2.3 pour usage SECONDAIRE (Baileys en complément de l'API Cloud,
// volume cible : ~1000 messages/semaine, soit ~140/jour en moyenne)
const RATE_CONFIG = {
    MIN_DELAY_SEC: 8,
    MAX_DELAY_SEC: 20,
    BATCH_SIZE: 8,                    // était 10
    BATCH_PAUSE_MINUTES: 5,           // était 3
    LONG_BREAK_EVERY: 20,             // était 40 - pause longue plus fréquente
    LONG_BREAK_MIN_MINUTES: 8,
    LONG_BREAK_MAX_MINUTES: 15,
    DEFAULT_DAILY_LIMIT: 150,         // était 500 - plafond réaliste pour 1000/semaine étalé
    MAX_RETRIES: 3
};

// Plafond dur absolu accepté par l'API même si un appelant tente de forcer plus haut
const HARD_DAILY_LIMIT_CAP = 200; // était 1000

// ==================== CONNEXION WHATSAPP (VERSION CORRIGÉE) ====================
async function connectWhatsApp() {
    if (isBotStarting) {
        console.log('⚠️ Connexion déjà en cours, skip...');
        return null;
    }
    
    if (retryCount > TIMEOUT_CONFIG.MAX_RETRIES) {
        console.error(`💥 Max retries atteint (${TIMEOUT_CONFIG.MAX_RETRIES}) - Attente manuelle ou reset`);
        isBotStarting = false;
        
        reconnectTimeout = setTimeout(() => {
            retryCount = 0;
            console.log('🔄 Reset retry count - Nouvelle tentative autorisée');
            connectWhatsApp();
        }, 60000);
        
        return null;
    }
    
    isBotStarting = true;
    
    try {
        console.log('\n' + '='.repeat(50));
        console.log(`🔐 CONNEXION WHATSAPP...`);
        console.log(`📊 Tentative #${retryCount + 1}/${TIMEOUT_CONFIG.MAX_RETRIES + 1}`);
        console.log(`⏱️ Timeout configuré: ${getAdaptiveTimeout() / 1000}s`);
        console.log('='.repeat(50) + '\n');

        console.log('🗄️ Connexion MongoDB Atlas...');
        await mongoose.connect(MONGO_URI, {
            serverSelectionTimeoutMS: 15000,
            socketTimeoutMS: 60000,
            maxPoolSize: 10,
            bufferCommands: false
        });
        console.log('✅ MongoDB connecté !\n');

        const { state, saveCreds } = await useMongoDBAuthState();

        if (sock) {
            console.log('🔄 Fermeture ancienne connexion...');
            try { 
                sock.ev.removeAllListeners();
                sock.ws?.close();
            } catch(e) { 
                console.log('⚠️ Erreur fermeture socket:', e.message);
            }
            sock = null;
            isReady = false;
            await sleep(3000);
        }

        // Annuler reconnexion planifiée si existante
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }

        console.log('📱 Création socket WhatsApp...');
        const currentTimeout = getAdaptiveTimeout();
        
        sock = makeWASocket({
            auth: state,
            usePairingCode: true, // Active la prise en charge du code de parrainage
            syncFullHistory: false,
            shouldSyncHistoryMessage: () => false,
            
            browser: ["Ubuntu", "Chrome", "20.0.04"],
            
            connectTimeoutMs: currentTimeout,
            keepAliveIntervalMs: TIMEOUT_CONFIG.KEEP_ALIVE_INTERVAL,
            queryTimeoutMs: currentTimeout,
            
            logger: pino({ level: 'warn' }),
            markOnlineOnConnect: false,
            retryRequestDelayMs: 5000,
            maxMsgRetryCount: 3
        });

        // Sauvegarde des crédentiels
        sock.ev.on('creds.update', saveCreds);

        // Gestion connection.update
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            // Mise à jour du QR pour la route /qr (secours)
            if (qr) {
                currentQR = qr;
                qrGeneratedAt = Date.now();
            }

            // N'exécute la demande de code QU'UNE SEULE FOIS
            if (qr && !sock.authState.creds.registered && !pairingCodeRequested) {
                pairingCodeRequested = true;
                await sleep(3000);

                try {
                    const cleanNumber = PAIRING_NUMBER.replace(/[^0-9]/g, '');
                    const code = await sock.requestPairingCode(cleanNumber);
                    
                    console.log('\n' + '='.repeat(40));
                    console.log(`📱 NUMÉRO CIBLE : ${cleanNumber}`);
                    console.log(`🔑 CODE D'APPAIRAGE UNIQUE : ${code}`);
                    console.log('='.repeat(40) + '\n');
                } catch (err) {
                    console.error('❌ Erreur génération Pairing Code:', err.message);
                    pairingCodeRequested = false;
                }
            }

            if (connection === 'close') {
                isReady = false;
                isBotStarting = false;
                pairingCodeRequested = false;
                currentQR = null;
                qrGeneratedAt = null;
                
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                console.log(`❌ Connexion fermée (Status: ${statusCode}).`);
                
                if (shouldReconnect) {
                    retryCount++;
                    const delay = getProgressiveDelay();
                    console.log(`🔄 Reconnexion dans ${delay / 1000}s...`);
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), delay);
                } else {
                    // ⭐ v3.2.3 : sur logged-out (401), on ne reste plus bloqué.
                    // On supprime les anciennes credentials puis on relance la
                    // connexion : un nouveau code de parrainage sera généré
                    // automatiquement dans les logs (à ressaisir manuellement
                    // sur le téléphone, WhatsApp l'exige dans ce cas).
                    console.log('⛔ Session invalidée (logged out) - Réinitialisation + reconnexion auto...');
                    retryCount = 0;

                    try {
                        await AuthModel.deleteMany({});
                        console.log('🗑️ Anciennes credentials supprimées de MongoDB');
                    } catch (e) {
                        console.error('❌ Erreur suppression credentials:', e.message);
                    }

                    console.log('🔄 Nouvelle tentative de connexion dans 5s (nouveau code de parrainage à venir)...');
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), 5000);
                }
            }

            if (connection === 'open') {
                isReady = true;
                isBotStarting = false;
                retryCount = 0;
                connectionOpenCount++;
                currentQR = null;
                qrGeneratedAt = null;
                console.log('\n✅ CONNEXION WHATSAPP RÉUSSIE !\n');
            }
        });

        // ✅ Gestion erreurs socket
        sock.ev.on('error', (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            
            if (msg.includes('Timed Out') || stack.includes('Timed Out')) {
                console.warn('⚠️ [TIMEOUT] Erreur timeout socket (normal en hébergement cloud)');
            }
            else if (msg.includes('stream') || msg.includes('conflict')) {
                console.warn('⚠️ [STREAM] Erreur stream (normale en hébergement cloud)');
            }
            else if (msg.includes('init queries') || stack.includes('chats.js')) {
                console.warn('⚠️ [INIT] Erreur initialisation queries (timeout probable)');
            }
            else {
                console.error('❌ [ERROR] Erreur socket inattendue:');
                console.error('   Type:', error.constructor.name);
                console.error('   Message:', msg.substring(0, 150));
            }
        });

        console.log('✅ Socket WhatsApp créé avec succès\n');
        console.log('⏳ Attente des événements de connexion...\n');

        return sock;

    } catch (err) {
        console.error('\n' + '💥'.repeat(25));
        console.log(' ERREUR CRITIQUE LORS DE LA CRÉATION DU SOCKET');
        console.log('💥'.repeat(25) + '\n');
        
        console.error('Type d\'erreur:', err.constructor.name);
        console.error('Message:', err.message);
        console.error('');
        
        isBotStarting = false;
        pairingCodeRequested = false;
        retryCount++;
        
        const delay = getProgressiveDelay();
        console.log(`🔄 Planification nouvelle tentative dans ${delay / 1000}s (retry #${retryCount})\n`);
        
        setTimeout(() => connectWhatsApp(), delay);
        
        return null;
    }
}


// ==================== PROCESSING BULK JOB ====================
async function processBulkJob() {
    if (!bulkJob || isProcessing) return;
    
    isProcessing = true;
    bulkJob.status = 'running';
    
    console.log('\n' + '🚀'.repeat(25));
    console.log(' DÉMARRAGE ENVOI BULK MESSAGES');
    console.log('🚀'.repeat(25) + `\n`);
    console.log(`📊 Total messages: ${bulkJob.items.length}`);
    console.log(`⏱️ Délai configuré: ${bulkJob.config?.minDelaySec || RATE_CONFIG.MIN_DELAY_SEC}s - ${bulkJob.config?.maxDelaySec || RATE_CONFIG.MAX_DELAY_SEC}s`);

    try {
        for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
            // Vérification annulation
            if (bulkJob.cancelled) {
                if (bulkJob.status !== 'paused_daily_limit') {
                    bulkJob.status = 'cancelled';
                }
                console.log('\n🛑 Job annulé par utilisateur');
                break;
            }

            // Attente connexion active
            while (!isReady && !bulkJob.cancelled) {
                console.log('⏳ Attente reconnexion WhatsApp...');
                await sleep(15000);
                
                if (!sock) {
                    console.log('⚠️ Socket indisponible - En attente...');
                    continue;
                }
            }
            
            if (bulkJob.cancelled) break;

            // Gestion limite quotidienne
            if (todayStr() !== bulkJob.currentDay) {
                bulkJob.currentDay = todayStr();
                bulkJob.sentToday = 0;
            }

            const dailyLimit = bulkJob.config?.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT;
            if (bulkJob.sentToday >= dailyLimit) {
                console.log(`\n⏸️ Limite quotidienne atteinte (${dailyLimit} messages)`);
                console.log('   Pause jusqu\'à demain ou reset manuel...\n');
                
                bulkJob.status = 'paused_daily_limit';
                
                try { 
                    await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
                } catch(e) {}
                
                await sleep(15 * 60 * 1000); // 15 minutes
                
                if (!bulkJob.cancelled) {
                    bulkJob.sentToday = 0;
                    bulkJob.status = 'running';
                    console.log('▶️ Reprise après pause quotidienne\n');
                }
            }

            const item = bulkJob.items[i];
            bulkJob.currentIndex = i;

            try {
                const jid = formatNumber(item.number);
                
                // Simulation comportement humain
                try {
                    await sock.sendPresenceUpdate('composing', jid);
                    await sleep(1500 + Math.floor(Math.random() * 2500));
                    await sock.sendPresenceUpdate('paused', jid);
                } catch(e) {
                    // Ignorer les erreurs de présence
                }

                const progressText = `[${i+1}/${bulkJob.items.length}]`;
                console.log(`📤 ${progressText} → ${item.number}`);

                // Envoi avec timeout de sécurité
                const sendPromise = sock.sendMessage(jid, { text: item.message });
                const timeoutPromise = new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Send timeout')), 30000)
                );
                
                const result = await Promise.race([sendPromise, timeoutPromise]);
                
                bulkJob.results.push({ 
                    number: item.number, 
                    jid, 
                    status: 'success', 
                    id_message: result?.key?.id, 
                    timestamp: new Date() 
                });
                bulkJob.sentCount++;
                bulkJob.sentToday++;

                // Sauvegarde périodique (tous les 10 messages)
                if (bulkJob.sentCount % 10 === 0) {
                    try { 
                        await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
                        console.log(`   💾 Sauvegarde intermédiaire (${bulkJob.sentCount} envoyés)`);
                    } catch(e) {}

                    // Log de progression toutes les 50 messages
                    if (bulkJob.sentCount % 50 === 0) {
                        const percent = ((i + 1) / bulkJob.items.length * 100).toFixed(1);
                        console.log(`\n📊 Progression: ${percent}% (${i+1}/${bulkJob.items.length})\n`);
                    }
                }

            } catch (error) {
                const errorMsg = error?.message || 'Erreur inconnue';
                console.error(`   ❌ Échec envoi à ${item.number}: ${errorMsg}`);
                
                bulkJob.results.push({ 
                    number: item.number, 
                    status: 'error', 
                    error: errorMsg, 
                    timestamp: new Date() 
                });
                bulkJob.failedCount++;
                
                // Pause supplémentaire en cas d'erreur de connexion
                if (errorMsg.toLowerCase().includes('timeout') || 
                    errorMsg.toLowerCase().includes('disconnect') ||
                    errorMsg.toLowerCase().includes('connection')) {
                    console.log('   ⏳ Pause 10s suite à erreur de connexion...');
                    await sleep(10000);
                }
            }

            // Délai entre messages (rate limiting intelligent)
            if (i < bulkJob.items.length - 1) {
                const msgsSinceStart = i - (bulkJob.startIndex || 0) + 1;
                let delayMs;
                let delayReason = '';
                
                // Longue pause tous les X messages
                if (msgsSinceStart > 0 && msgsSinceStart % RATE_CONFIG.LONG_BREAK_EVERY === 0) {
                    const longBreakMin = RATE_CONFIG.LONG_BREAK_MIN_MINUTES;
                    const longBreakMax = RATE_CONFIG.LONG_BREAK_MAX_MINUTES;
                    const randomLongBreak = longBreakMin + Math.random() * (longBreakMax - longBreakMin);
                    delayMs = randomLongBreak * 60000;
                    delayReason = `☕ Longue pause (${randomLongBreak.toFixed(1)}min)`;
                }
                // Pause batch
                else if (bulkJob.config?.batchSize && msgsSinceStart % bulkJob.config.batchSize === 0) {
                    const batchPause = bulkJob.config.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES;
                    delayMs = batchPause * 60000;
                    delayReason = `📦 Pause batch (${batchPause}min)`;
                }
                // Délai normal aléatoire
                else {
                    const minDel = bulkJob.config?.minDelaySec || RATE_CONFIG.MIN_DELAY_SEC;
                    const maxDel = bulkJob.config?.maxDelaySec || RATE_CONFIG.MAX_DELAY_SEC;
                    delayMs = randomDelayMs(minDel, maxDel);
                    delayReason = `⏱️ Délai normal (${(delayMs/1000).toFixed(1)}s)`;
                }
                
                if (delayReason) {
                    console.log(`   ${delayReason}`);
                }
                
                await sleep(delayMs);
            }
        }

        // Finalisation
        if (bulkJob.status !== 'cancelled' && bulkJob.status !== 'paused_daily_limit') {
            bulkJob.status = 'completed';
        }
        bulkJob.finishedAt = new Date();
        
        // Sauvegarde finale
        try { 
            await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
        } catch(e) {}
        
        console.log('\n' + '✅'.repeat(25));
        console.log(' ENVOI BULK TERMINÉ AVEC SUCCÈS !');
        console.log('✅'.repeat(25));
        console.log(`\n📊 RÉCAPITULATIF:`);
        console.log(`   ✅ Messages envoyés: ${bulkJob.sentCount}`);
        console.log(`   ❌ Messages échoués: ${bulkJob.failedCount}`);
        console.log(`   📈 Taux de réussite: ${((bulkJob.sentCount / bulkJob.items.length) * 100).toFixed(1)}%`);
        console.log(`   ⏰ Durée totale: ${Math.round((Date.now() - new Date(bulkJob.startedAt).getTime()) / 1000)} secondes\n`);

    } catch (error) {
        console.error('\n' + '💥'.repeat(25));
        console.log(' ERREUR CRITIQUE LORS DU TRAITEMENT');
        console.log('💥'.repeat(25) + '\n');
        console.error(error);
        
        bulkJob.status = 'failed';
        
        try { 
            await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
        } catch(e) {}
        
    } finally {
        isProcessing = false;
    }
}

// ==================== ROUTES API ====================

// Affichage du QR code sous forme d'image (fallback / secours)
app.get('/qr', async (req, res) => {
    if (isReady) {
        return res.send(`
            <html><body style="font-family:sans-serif;text-align:center;padding:40px">
                <h2>✅ Bot déjà connecté</h2>
                <p>Aucun QR à scanner — le bot est déjà lié à WhatsApp.</p>
            </body></html>
        `);
    }

    if (!currentQR) {
        return res.send(`
            <html><head><meta http-equiv="refresh" content="3"></head>
            <body style="font-family:sans-serif;text-align:center;padding:40px">
                <h2>⏳ En attente du QR...</h2>
                <p>Rechargement automatique dans 3 secondes.</p>
            </body></html>
        `);
    }

    const ageSeconds = Math.round((Date.now() - qrGeneratedAt) / 1000);
    if (ageSeconds > 18) {
        return res.send(`
            <html><head><meta http-equiv="refresh" content="2"></head>
            <body style="font-family:sans-serif;text-align:center;padding:40px">
                <h2>⌛ QR expiré, régénération...</h2>
                <p>Rechargement automatique dans 2 secondes.</p>
            </body></html>
        `);
    }

    try {
        const qrImage = await QRCode.toDataURL(currentQR, { width: 400, margin: 2 });
        res.send(`
            <html><head><meta http-equiv="refresh" content="5"></head>
            <body style="font-family:sans-serif;text-align:center;padding:40px">
                <h2>📸 Scannez ce QR code</h2>
                <img src="${qrImage}" alt="QR Code WhatsApp" style="border:4px solid #333;border-radius:8px" />
                <p>WhatsApp > Paramètres > Appareils liés > Lier un appareil</p>
                <p style="color:#888">Actualisation automatique toutes les 5s — âge du QR : ${ageSeconds}s</p>
            </body></html>
        `);
    } catch (e) {
        res.status(500).send('Erreur génération QR: ' + e.message);
    }
});

// ==================== WEBHOOK META (WhatsApp Business Cloud API) ====================
const META_VERIFY_TOKEN = process.env.META_VERIFY_TOKEN || 'change_this_token';

app.get('/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode === 'subscribe' && token === META_VERIFY_TOKEN) {
        console.log('✅ Webhook Meta vérifié avec succès');
        return res.status(200).send(challenge);
    }
    console.log('❌ Échec de vérification webhook Meta (token attendu vs reçu ne correspondent pas)');
    return res.sendStatus(403);
});

app.post('/webhook', (req, res) => {
    console.log('📩 Événement webhook Meta reçu:', JSON.stringify(req.body).substring(0, 300));
    res.sendStatus(200);
});

// Health check simple
app.get('/ping', (req, res) => res.status(200).send('pong'));

// Health check détaillé
app.get('/health', (req, res) => {
    const healthStatus = {
        status: isReady ? 'healthy' : 'degraded',
        whatsapp: { 
            connected: isReady, 
            connections: connectionOpenCount,
            retryCount: retryCount,
            socketExists: !!sock,
            user: sock?.user?.id ? sock.user.id.split(':')[0] : null
        },
        system: { 
            uptime: Math.round(process.uptime()),
            memory: {
                used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
                total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024)
            },
            nodeVersion: process.version,
            platform: `${process.platform} ${process.arch}`
        },
        mongodb: {
            connected: mongoose.connection.readyState === 1,
            state: ['disconnected', 'connected', 'connecting', 'disconnecting'][mongoose.connection.readyState] || 'unknown'
        },
        job: bulkJob ? { 
            id: bulkJob.jobId, 
            status: bulkJob.status, 
            sent: bulkJob.sentCount,
            total: bulkJob.items?.length || 0,
            progress: bulkJob.items?.length > 0 ? 
                ((bulkJob.currentIndex + 1) / bulkJob.items.length * 100).toFixed(1) + '%' : '0%'
        } : null,
        config: {
            currentTimeout: `${getAdaptiveTimeout() / 1000}s`,
            nextRetryDelay: `${getProgressiveDelay() / 1000}s`,
            maxRetries: TIMEOUT_CONFIG.MAX_RETRIES,
            dailyLimitDefault: RATE_CONFIG.DEFAULT_DAILY_LIMIT,
            dailyLimitHardCap: HARD_DAILY_LIMIT_CAP
        },
        timestamp: new Date().toISOString(),
        version: '3.2.3'
    };
    
    res.status(isReady ? 200 : 503).json(healthStatus);
});

// Route racine
app.get('/', (req, res) => {
    res.json({
        service: 'WhatsApp Bot',
        version: '3.2.3 (Reconnexion auto + Rate limiting canal secondaire)',
        status: isReady ? '🟢 Connected' : '🟡 Waiting Pairing / QR',
        description: 'API WhatsApp (canal secondaire, complément API Cloud) avec bulk messaging, rate limiting et persistance MongoDB',
        features: [
            '✅ Code de Parrainage Auto (Pairing Code)',
            '✅ Fallback QR code si échec',
            '✅ Anti-timeout 408',
            '✅ Anti-boucle infinie',
            '✅ Retry intelligent progressif',
            '✅ Reconnexion auto même après logged-out (nouveau code auto-généré)',
            '✅ Bulk messaging optimisé (plafonné pour usage secondaire)',
            '✅ Rate limiting humain',
            '✅ Persistance MongoDB'
        ],
        endpoints: {
            health: { method: 'GET', path: '/health', description: 'Statut détaillé du système' },
            qr: { method: 'GET', path: '/qr', description: 'Afficher le QR code (secours)' },
            sendMessage: { method: 'POST', path: '/send-message', description: 'Envoyer un message simple' },
            sendBulk: { method: 'POST', path: '/send-bulk-messages', description: 'Démarrer un envoi bulk' },
            status: { method: 'GET', path: '/bulk-status', description: 'Statut du job en cours' },
            cancel: { method: 'POST', path: '/bulk-cancel', description: 'Annuler le job en cours' },
            resetAuth: { method: 'GET', path: '/reset-auth', description: 'Réinitialiser l\'authentification' },
            checkAuth: { method: 'GET', path: '/check-auth', description: 'Vérifier l\'état de l\'auth' }
        },
        quickStart: {
            step1: 'Le code de parrainage s\'affiche automatiquement dans les logs au démarrage',
            step2: 'Si le code expire, le QR code est accessible via /qr',
            step3: 'Utilisez POST /send-message pour tester',
            step4: 'Utilisez POST /send-bulk-messages pour les envois multiples'
        }
    });
});

// Envoi message simple
app.post('/send-message', async (req, res) => {
    if (!isReady || !sock) {
        return res.status(503).json({ 
            success: false,
            error: 'Bot non connecté', 
            hint: 'Attendez la connexion ou vérifiez les logs pour le code de parrainage',
            status: isReady ? 'degraded' : 'offline',
            action: 'Vérifiez /health ou les logs'
        });
    }
    
    const rawNumber = req.body.number || req.body.phone;
    const message = req.body.message || req.body.text;
    
    if (!rawNumber || !message) {
        return res.status(400).json({ 
            success: false,
            error: 'Champs requis manquants',
            required: ['number (ou phone)', 'message (ou text)'],
            example: {
                number: '01XXXXXXXX',
                message: 'Votre message ici'
            }
        });
    }
    
    try {
        const jid = formatNumber(rawNumber);
        console.log(`\n📨 Nouvel envoi simple vers ${rawNumber}`);
        
        const result = await sock.sendMessage(jid, { text: message });
        
        console.log(`✅ Message envoyé avec succès - ID: ${result?.key?.id}\n`);
        
        res.json({ 
            success: true, 
            jid, 
            id: result?.key?.id,
            to: rawNumber,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('❌ Erreur envoi simple:', error.message);
        res.status(500).json({ 
            success: false,
            error: error.message,
            hint: 'Vérifiez le format du numéro (01XXXXXXXX ou +229XXXXXXXX)'
        });
    }
});

// Envoi bulk messages
app.post('/send-bulk-messages', async (req, res) => {
    if (!isReady || !sock) {
        return res.status(503).json({ 
            success: false,
            error: 'Bot non connecté',
            hint: 'Connectez d\'abord le bot',
            action: 'Vérifiez /health'
        });
    }
    
    if (bulkJob && ['running', 'pending'].includes(bulkJob.status)) {
        return res.status(409).json({ 
            success: false,
            error: 'Un job est déjà en cours',
            currentJob: {
                id: bulkJob.jobId,
                status: bulkJob.status,
                sent: bulkJob.sentCount,
                total: bulkJob.items?.length || 0
            },
            actions: [
                'Consultez GET /bulk-status pour suivre le job actuel',
                'Utilisez POST /bulk-cancel pour l\'annuler'
            ]
        });
    }
    
    const messages = req.body.messages;
    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({ 
            success: false,
            error: 'Le champ "messages" est requis et doit être un tableau non vide',
            example: {
                messages: [
                    { number: '01XX123456', message: 'Bonjour !' },
                    { phone: '229XX789012', text: 'Message test' }
                ]
            }
        });
    }
    
    if (messages.length > 1000) {
        return res.status(400).json({ 
            success: false,
            error: 'Limite dépassée: maximum 1000 messages par job',
            received: messages.length,
            suggestion: 'Divisez en plusieurs jobs si nécessaire'
        });
    }
    
    const jobId = Date.now().toString() + '_' + Math.random().toString(36).substr(2, 9);
    
    bulkJob = {
        jobId: jobId,
        items: messages
            .map(m => ({ 
                number: m.number || m.phone, 
                message: m.message || m.text 
            }))
            .filter(m => m.number && m.message),
        currentIndex: 0, 
        startIndex: 0, 
        sentCount: 0, 
        failedCount: 0,
        results: [], 
        status: 'pending', 
        cancelled: false,
        config: {
            // ⚙️ Plafond quotidien borné par HARD_DAILY_LIMIT_CAP quoi qu'il arrive
            dailyLimit: Math.min(req.body.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT, HARD_DAILY_LIMIT_CAP),
            batchSize: req.body.batchSize || RATE_CONFIG.BATCH_SIZE,
            batchPauseMinutes: Math.max(req.body.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES, 3),
            minDelaySec: Math.max(req.body.minDelaySeconds || RATE_CONFIG.MIN_DELAY_SEC, 8),
            maxDelaySec: req.body.maxDelaySeconds || RATE_CONFIG.MAX_DELAY_SEC
        },
        sentToday: 0, 
        currentDay: todayStr(),
        startedAt: new Date()
    };
    
    if (bulkJob.items.length === 0) {
        return res.status(400).json({ 
            success: false,
            error: 'Aucun message valide après validation',
            reason: 'Tous les éléments sont invalides (champs number/message manquants)',
            formatRequired: { number: 'string', message: 'string' }
        });
    }
    
    try { 
        await new BulkJobModel(bulkJob).save(); 
        console.log(`\n💾 Job créé et sauvegardé: ${jobId}`);
        console.log(`   📧 Messages valides: ${bulkJob.items.length}/${messages.length}\n`);
    } catch(e) {
        console.error('⚠️ Erreur sauvegarde job:', e.message);
    }
    
    setImmediate(processBulkJob);
    
    const avgDelay = ((RATE_CONFIG.MIN_DELAY_SEC + RATE_CONFIG.MAX_DELAY_SEC) / 2);
    const estimatedTotalMs = bulkJob.items.length * avgDelay * 1000;
    const estimatedMinutes = Math.round(estimatedTotalMs / 60000);
    
    res.status(202).json({
        success: true, 
        jobId: bulkJob.jobId,
        accepted: bulkJob.items.length,
        rejected: messages.length - bulkJob.items.length,
        estimatedTime: {
            minutes: estimatedMinutes,
            optimistic: Math.round(estimatedMinutes * 0.7),
            pessimistic: Math.round(estimatedMinutes * 1.3)
        },
        config: bulkJob.config,
        endpoints: {
            status: `/bulk-status?jobId=${bulkJob.jobId}`,
            cancel: { method: 'POST', path: '/bulk-cancel' }
        },
        warnings: [
            `Plafond quotidien appliqué: ${bulkJob.config.dailyLimit} messages/jour max (canal secondaire)`,
            'Les délais sont aléatoires pour simuler un comportement humain',
            'Le job continue même si vous fermez la connexion API'
        ],
        info: 'Le traitement a démarré en arrière-plan'
    });
});

// Statut bulk job
app.get('/bulk-status', async (req, res) => {
    if (!bulkJob) {
        return res.json({ 
            status: 'idle', 
            message: 'Aucun job en cours',
            hint: 'Créez un job avec POST /send-bulk-messages'
        });
    }
    
    const total = bulkJob.items?.length || 1;
    const current = bulkJob.currentIndex + 1;
    const percent = ((current / total) * 100).toFixed(1);
    
    res.json({
        jobId: bulkJob.jobId,
        status: bulkJob.status,
        statusEmoji: {
            pending: '⏳',
            running: '🚀',
            completed: '✅',
            cancelled: '🛑',
            paused_daily_limit: '⏸️',
            failed: '❌'
        }[bulkJob.status] || '❓',
        progress: {
            current: current,
            total: total,
            percent: `${percent}%`,
            remaining: total - current
        },
        stats: {
            sent: bulkJob.sentCount,
            failed: bulkJob.failedCount,
            sentToday: bulkJob.sentToday,
            dailyLimit: bulkJob.config?.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT,
            successRate: total > 0 ? ((bulkJob.sentCount / current) * 100).toFixed(1) + '%' : 'N/A'
        },
        timing: {
            startedAt: bulkJob.startedAt,
            runningForSeconds: bulkJob.startedAt ? 
                Math.round((Date.now() - new Date(bulkJob.startedAt).getTime()) / 1000) : null,
            estimatedRemaining: bulkJob.status === 'running' ?
                `${Math.round((total - current) * 15 / 60)} minutes` : null
        },
        cancelled: bulkJob.cancelled,
        canCancel: ['running', 'pending', 'paused_daily_limit'].includes(bulkJob.status),
        actions: {
            cancel: 'POST /bulk-cancel',
            details: 'Les résultats complets sont disponibles une fois terminé'
        }
    });
});

// Annulation job
app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob) {
        return res.status(400).json({ 
            success: false,
            error: 'Aucun job actif à annuler',
            hint: 'Créez un job d\'abord avec POST /send-bulk-messages'
        });
    }
    
    if (!['running', 'pending', 'paused_daily_limit'].includes(bulkJob.status)) {
        return res.status(400).json({ 
            success: false,
            error: `Impossible d'annuler un job en statut "${bulkJob.status}"`,
            currentStatus: bulkJob.status,
            cancellableStatuses: ['running', 'pending', 'paused_daily_limit']
        });
    }
    
    bulkJob.cancelled = true;
    
    console.log(`\n🛑 Demande d'annulation reçue pour le job ${bulkJob.jobId}`);
    
    res.json({ 
        success: true, 
        message: 'Annulation demandée avec succès',
        jobId: bulkJob.jobId,
        info: 'Le job s\'arrêtera prochainement (au prochain message)',
        stats: {
            sentBeforeCancel: bulkJob.sentCount,
            remaining: (bulkJob.items?.length || 0) - bulkJob.currentIndex,
            totalProcessed: bulkJob.currentIndex
        },
        note: 'Les messages déjà envoyés ne peuvent pas être annulés'
    });
});

// Reset auth
app.get('/reset-auth', async (req, res) => {
    try {
        console.log('\n' + '='.repeat(50));
        console.log('🗑️ DEMANDE DE RÉINITIALISATION AUTH');
        console.log('='.repeat(50) + '\n');
        
        if (bulkJob) {
            bulkJob.cancelled = true;
            console.log('📤 Job en cours marqué comme annulé');
        }
        
        if (sock) { 
            try { 
                sock.ev.removeAllListeners();
                sock.ws?.close(); 
                console.log('📱 Socket fermé');
            } catch(e) { 
                console.log('⚠️ Erreur fermeture:', e.message);
            } 
            sock = null; 
            isReady = false; 
        }
        
        const deleteResult = await AuthModel.deleteMany({});
        console.log(`🗑️ ${deleteResult.deletedCount} document(s) auth supprimé(s)`);
        
        isBotStarting = false;
        connectionOpenCount = 0;
        retryCount = 0;
        pairingCodeRequested = false;
        
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
            console.log('⏹️ Reconnexion planifiée annulée');
        }
        
        res.json({
            success: true,
            message: 'Authentification réinitialisée avec succès !',
            actionsPerformed: [
                `✅ Socket fermé`,
                `✅ ${deleteResult.deletedCount} credentials supprimées de MongoDB`,
                `✅ Variables internes reset`,
                `✅ Compteur retries remis à zéro`
            ],
            nextSteps: [
                '1. Attendre 10-15 secondes',
                '2. Consulter GET /health pour vérifier le statut',
                '3. Regarder les LOGS pour le nouveau CODE DE PARRAINAGE',
                '4. Si code expiré, un QR code est disponible sur /qr'
            ],
            autoReconnect: 'Reconnexion automatique dans 5 secondes...',
            timestamp: new Date().toISOString()
        });
        
        setTimeout(() => {
            console.log('\n🔄 Démarrage reconnexion post-reset...');
            connectWhatsApp();
        }, 5000);
        
    } catch (error) {
        console.error('❌ Erreur critique reset auth:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message,
            hint: 'Vérifiez la connexion MongoDB'
        });
    }
});

// Vérifier état auth
app.get('/check-auth', async (req, res) => {
    try {
        const count = await AuthModel.countDocuments();
        const hasCreds = await AuthModel.exists({ _id: 'creds' });
        
        const status = {
            mongodb_connected: mongoose.connection.readyState === 1,
            docs_count: count,
            hasCredentials: !!hasCreds,
            healthy: count >= 3,
            details: {
                creds_document: !!hasCreds,
                keys_count: Math.max(0, count - 1),
                estimated_session: hasCreds ? 'existante' : 'inexistante'
            },
            recommendations: []
        };
        
        if (count < 3) {
            status.recommendations.push('Session incomplète - Essayez /reset-auth');
            status.recommendations.push('Ou attendez la prochaine connexion automatique');
        } else {
            status.recommendations.push('Authentification OK - Le bot devrait se connecter');
            status.recommendations.push('Si pas connecté, vérifiez /health');
        }
        
        res.json(status);
    } catch (e) {
        res.status(500).json({ 
            error: e.message,
            mongodb_connected: false,
            hint: 'Vérifiez MONGO_URI dans les variables d\'environnement'
        });
    }
});

// ==================== GESTION ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
    console.error('💥 Erreur non gérée:', err.stack);
    res.status(500).json({ 
        success: false,
        error: 'Erreur interne serveur',
        message: process.env.NODE_ENV === 'development' ? err.message : 'Contactez l\'administrateur',
        timestamp: new Date().toISOString()
    });
});

// Route 404
app.use((req, res) => {
    res.status(404).json({ 
        success: false,
        error: 'Endpoint non trouvé',
        path: req.path,
        method: req.method,
        availableEndpoints: {
            root: 'GET /',
            health: ['GET /health', 'GET /ping'],
            pair: 'GET /qr (Secours QR)',
            messaging: ['POST /send-message', 'POST /send-bulk-messages'],
            jobs: ['GET /bulk-status', 'POST /bulk-cancel'],
            auth: ['GET /reset-auth', 'GET /check-auth']
        },
        documentation: 'Visitez GET / pour la documentation complète'
    });
});

// ==================== DÉMARRAGE SERVEUR ====================
app.listen(PORT, async () => {
    console.log(`
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║   🤖 WHATSAPP BOT v3.2.3                             ║
║   ─────────────────────                               ║
║   Version: RECONNEXION AUTO + RATE LIMITING SECONDAIRE ║
║   Statut:  PRÊT                                      ║
║                                                       ║
║   ┌─────────────────────────────────────────────────┐ ║
║   │  Serveur: http://localhost:${PORT.toString().padEnd(19)}│ ║
║   │  Node:    ${process.version.padEnd(35)}│ ║
║   │  PID:     ${process.pid.toString().padEnd(37)}│ ║
║   └─────────────────────────────────────────────────┘ ║
║                                                       ║
║   ✅ Fonctionnalités:                                 ║
║   • Code de Parrainage Auto                           ║
║   • Anti-Timeout 408                                  ║
║   • Anti-Boucle Infinie                               ║
║   • Retry Intelligent                                  ║
║   • Reconnexion auto même après logged-out            ║
║   • Bulk Messaging plafonné (usage secondaire)         ║
║   • Rate Limiting Humain                             ║
║   • Persistance MongoDB                              ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
    `);

    try {
        await mongoose.connect(MONGO_URI, { 
            serverSelectionTimeoutMS: 15000,
            socketTimeoutMS: 60000
        });
        console.log('✅ MongoDB Atlas connecté au démarrage');
    } catch (e) {
        console.error('❌ Erreur connexion MongoDB initiale:', e.message);
        console.log('⏳ Retente automatiquement au démarrage du bot...\n');
    }

    const startupDelay = 10000;
    
    console.log(`\n⏳ Démarrage du bot WhatsApp dans ${startupDelay / 1000} secondes...`);
    console.log('   (Délai de stabilisation)\n');
    
    setTimeout(() => {
        console.log('▶️ Initialisation de la connexion WhatsApp...\n');
        connectWhatsApp();
    }, startupDelay);
});

// ==================== GRACEFUL SHUTDOWN ====================
process.on('SIGTERM', async () => {
    console.log('\n' + '='.repeat(50));
    console.log('🛑 SIGNAL SIGTERM REÇU - Arrêt gracieux en cours...');
    console.log('='.repeat(50) + '\n');
    
    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
        console.log('⏹️ Timeout de reconnexion annulé');
    }
    
    if (bulkJob) {
        bulkJob.cancelled = true;
        console.log('📤 Job bulk marqué comme annulé');
    }
    
    if (sock) {
        try {
            sock.ev.removeAllListeners();
            sock.ws?.close();
            console.log('📱 Socket WhatsApp fermé proprement');
        } catch(e) {
            console.log('⚠️ Erreur fermeture socket:', e.message);
        }
    }
    
    try {
        await mongoose.connection.close();
        console.log('🗄️ MongoDB déconnecté');
    } catch(e) {
        console.log('⚠️ Erreur fermeture MongoDB:', e.message);
    }
    
    console.log('\n✅ Arrêt gracieux complété\n');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('\n\n🛑 SIGINT reçu (Ctrl+C)');
    console.log('Arrêt immédiat...\n');
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('\n💥 UNCAUGHT EXCEPTION:', error.constructor.name);
    console.error('Message:', error.message);
    
    if (error.message.includes('Timed Out') || error.message.includes('Timed out')) {
        console.log('⚠️ Timeout ignoré - La reconnexion va gérer cela automatiquement\n');
    } else {
        console.error('Stack:', error.stack);
        console.log('\n⏳ Arrêt dans 5 secondes...\n');
        setTimeout(() => process.exit(1), 5000);
    }
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('\n💥 UNHANDLED REJECTION:');
    console.error('Reason:', reason);
    console.error('Promise:', promise);
    console.log('');
});
