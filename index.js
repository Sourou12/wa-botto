// ============================================================
// 🔐 WHATSAPP BOT - VERSION PRODUCTION STABLE v3.0
// ✅ Corrigé: Timeout chats.js, envoi messages, reconnexion
// ============================================================

require('dotenv').config();
const mongoose = require('mongoose');
const { 
    makeWASocket, 
    useMongoDBAuthState, 
    DisconnectReason,
    delay,
    makeCacheableSignalKeyStore,
    proto
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const { Boom } = require('@hapi/boom');

// ============================================================
// ⚙️ CONFIGURATION
// ============================================================

const CONFIG = {
    MONGO_URI: process.env.MONGO_URI || 'mongodb://localhost:27017/whatsapp_bot',
    PAIRING_NUMBER: process.env.PAIRING_NUMBER || '',
    
    TIMEOUTS: {
        MAX_RETRIES: 10,
        CONNECT_MS: 300000,       // 5 min
        QUERY_MS: 600000,         // 10 min (CRITIQUE pour chats.js)
        KEEPALIVE_MS: 45000,      // 45s
        INIT_MAX_WAIT_MS: 90000,  // 90s max pour l'init (après: reconnexion)
        SEND_MESSAGE_TIMEOUT_MS: 20000, // 20s timeout par message envoyé
    },
    
    RETRY: {
        BASE_DELAY_MS: 15000,     // 15s
        MAX_DELAY_MS: 300000,     // 5 min
        BACKOFF_FACTOR: 2,
        JITTER_PERCENT: 0.25      // ±25%
    }
};

// ============================================================
// 📦 ÉTAT GLOBAL
// ============================================================

let sock = null;
let isBotStarting = false;
let isReady = false;
let isFullyInitialized = false;  // ⭐ NOUVEAU : Track si l'init est COMPLÈTE
let retryCount = 0;
let reconnectTimeout = null;
let pairingCodeRequested = false;
let currentQR = null;
let qrGeneratedAt = null;
let connectionOpenCount = 0;
let initTimeoutHandle = null;     // ⭐ Pour forcer la reconnexion si init trop longue
let lastMessageSentTime = null;   // ⭐ Suivi des envois
let sendQueue = [];               // ⭐ File d'attente des messages pendant reconnexion

// ============================================================
// 🛠️ UTILITAIRES
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calcule le délai progressif avec backoff exponentiel + jitter
 */
function getProgressiveDelay() {
    const { BASE_DELAY_MS, MAX_DELAY_MS, BACKOFF_FACTOR, JITTER_PERCENT } = CONFIG.RETRY;
    
    let delay = BASE_DELAY_MS * Math.pow(BACKOFF_FACTOR, retryCount);
    delay = Math.min(delay, MAX_DELAY_MS);
    
    // Ajouter du jitter
    const jitter = delay * JITTER_PERCENT * (Math.random() * 2 - 1);
    delay = Math.round(delay + jitter);
    
    return Math.max(delay, BASE_DELAY_MS); // Minimum 15s
}

/**
 * Vérifie la santé complète du socket
 */
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
    
    if (!sock) {
        return health;
    }
    
    health.details.hasSocket = true;
    const wsState = sock.ws?.readyState;
    health.details.wsState = wsState;
    health.details.wsStateText = {0:'connecting',1:'open',2:'closing',3:'closed'}[wsState] || `unknown(${wsState})`;
    health.details.hasAuth = !!sock.authState?.creds?.registered;
    health.details.userId = sock.user?.id || null;
    health.details.userName = sock.user?.name || null;
    
    // Conditions pour être "healthy"
    health.ready = isReady && wsState === 1;
    health.fullyInitialized = isFullyInitialized;
    
    // Condition CRITIQUE pour pouvoir envoyer
    health.canSend = health.ready && health.fullyInitialized && health.details.hasAuth;
    health.healthy = health.canSend;
    
    return health;
}

/**
 * Enfile un message à envoyer plus tard (pendant reconnexion)
 */
function queueMessage(jid, message, options = {}) {
    const queuedItem = {
        jid,
        message,
        options,
        timestamp: Date.now(),
        attempts: 0
    };
    
    sendQueue.push(queuedItem);
    console.log(`📤 Message enfilé (${sendQueue.length} en attente) → ${jid}`);
}

/**
 * Traite la file d'attente des messages
 */
async function processSendQueue() {
    if (sendQueue.length === 0 || !canSendMessage()) {
        return;
    }
    
    console.log(`\n📬 Traitement file d'attente: ${sendQueue.length} message(s)`);
    
    const processed = [];
    const failed = [];
    
    while (sendQueue.length > 0) {
        const item = sendQueue.shift();
        item.attempts++;
        
        try {
            await sendMessageInternal(item.jid, item.message, item.options);
            processed.push(item);
            console.log(`✅ Message traité depuis file → ${item.jid}`);
            
            // Petit délai entre les messages pour éviter le rate limiting
            await sleep(500);
            
        } catch (err) {
            item.error = err.message;
            
            if (item.attempts < 3) {
                // Réessayer plus tard
                sendQueue.unshift(item);
                console.log(`⚠️ Échec envoi (tentative ${item.attempts}/3), réessaiera`);
                break; // Sortir pour éviter boucle infinie
            } else {
                failed.push(item);
                console.error(`❌ Abandon après ${item.attempts} tentatives:`, err.message);
            }
        }
    }
    
    if (processed.length > 0) {
        console.log(`📊 File traitée: ${processed.length} succès, ${failed.length} échecs\n`);
    }
}

/**
 * Vérifie si on peut envoyer des messages maintenant
 */
function canSendMessage() {
    const health = checkSocketHealth();
    return health.canSend;
}

// ============================================================
// 🚀 FONCTION D'ENVOI SÉCURISÉE
// ============================================================

/**
 * Envoie un message avec gestion complète d'erreurs et retries
 * @param {string} jid - Destination (ex: 'xxx@s.whatsapp.net')
 * @param {object} message - Contenu du message ({ text: '...' })
 * @param {object} options - Options optionnelles
 * @returns {Promise<object>} Résultat de l'envoi
 */
async function sendMessage(jid, message, options = {}) {
    // Validation des paramètres
    if (!jid || typeof jid !== 'string') {
        throw new Error('JID invalide ou manquant');
    }
    
    if (!message || typeof message !== 'object') {
        throw new Error('Message invalide ou manquant');
    }
    
    // Si pas prêt, enfiler pour plus tard
    if (!canSendMessage()) {
        console.log(`⏳ Socket pas prêt - Message enfilé pour ${jid}`);
        queueMessage(jid, message, options);
        return { queued: true, jid, timestamp: Date.now() };
    }
    
    return sendMessageInternal(jid, message, options);
}

/**
 * Implémentation interne de l'envoi (sans vérification de readiness)
 */
async function sendMessageInternal(jid, message, options = {}) {
    const { SEND_MESSAGE_TIMEOUT_MS } = CONFIG.TIMEOUTS;
    
    console.log(`\n📤 ENVOI MESSAGE → ${jid}`);
    console.log(`   Type: ${Object.keys(message)[0] || 'unknown'}`);
    console.log(`   Timeout: ${SEND_MESSAGE_TIMEOUT_MS / 1000}s`);
    
    try {
        // Race entre l'envoi et un timeout
        const result = await Promise.race([
            sock.sendMessage(jid, message, options),
            new Promise((_, reject) => 
                setTimeout(
                    () => reject(new Error(`Timeout envoi après ${SEND_MESSAGE_TIMEOUT_MS / 1000}s`)),
                    SEND_MESSAGE_TIMEOUT_MS
                )
            )
        ]);
        
        // Succès !
        lastMessageSentTime = new Date().toISOString();
        
        console.log(`✅ MESSAGE ENVOYÉ AVEC SUCCÈS !`);
        console.log(`   ID: ${result?.key?.id || 'N/A'}`);
        console.log(`   Timestamp: ${lastMessageSentTime}\n`);
        
        // Traiter la file d'attente après un envoi réussi
        setTimeout(processSendQueue, 1000);
        
        return result;
        
    } catch (err) {
        console.error(`\n❌ ÉCHEC ENVOI VERS ${jid}:`);
        console.error(`   Erreur: ${err.message}`);
        console.error(`   Type: ${err.constructor.name}`);
        
        // Analyse de l'erreur et action appropriée
        await handleSendError(err, jid, message, options);
        
        throw err; // Re-throw pour que l'appelant puisse gérer aussi
    }
}

/**
 * Gère les erreurs d'envoi avec logique de récupération
 */
async function handleSendError(err, jid, originalMessage, originalOptions) {
    const msg = err.message || '';
    const stack = err.stack || '';
    
    console.error('\n🔍 ANALYSE ERREUR ENVOI:');
    
    if (msg.includes('Timed Out') || msg.includes('timeout')) {
        console.error('   Type: TIMEOUT');
        console.error('   Cause: Socket probablement zombie ou réseau lent');
        console.error('   Action: Marquer socket comme non-initialisé et préparer reconnexion');
        
        // Forcer la réinitialisation
        isFullyInitialized = false;
        
        // Si ça arrive plusieurs fois, forcer reconnexion
        if (retryCount < 3) {
            console.error('   → Tentative de récupération (reset init state)');
        } else {
            console.error('   → Trop de timeouts, reconnexion forcée planifiée');
            scheduleReconnect();
        }
        
    } else if (msg.includes('Connection Closed') || msg.includes('closed') || msg.includes('not open')) {
        console.error('   Type: CONNEXION FERMÉE');
        console.error('   Action: Reconnexion immédiate nécessaire');
        isReady = false;
        isFullyInitialized = false;
        scheduleReconnect();
        
    } else if (msg.includes('403') || msg.includes('forbidden')) {
        console.error('   Type: ACCÈS REFUSÉ');
        console.error('   Cause: Numéro bloqué ou session invalide');
        console.error('   Action: Nécessite intervention manuelle');
        
    } else if (msg.includes('428') || msg.includes('precondition') || msg.includes('initialization')) {
        console.error('   Type: INITIALISATION INCOMPLÈTE');
        console.error('   Cause: Le timeout chats.js a laissé le socket dans état instable');
        console.error('   Action: Reconnexion forcée recommandée');
        
        isFullyInitialized = false;
        scheduleReconnect();
        
    } else {
        console.error('   Type: INCONNU');
        console.error(`   Stack: ${stack.substring(0, 300)}`);
    }
    
    console.error('');
}

/**
 * Planifie une reconnexion
 */
function scheduleReconnect() {
    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
    }
    
    const delay = getProgressiveDelay();
    console.log(`\n🔄 RECONNEXION PLANIFIÉE dans ${(delay / 1000).toFixed(1)}s`);
    
    reconnectTimeout = setTimeout(() => {
        connectWhatsApp();
    }, delay);
}

// ============================================================
// 🔌 CONNEXION PRINCIPALE
// ============================================================

async function connectWhatsApp() {
    // ----------------------------------------------------------
    // GUARDS
    // ----------------------------------------------------------
    
    if (isBotStarting) {
        console.log('⚠️ Connexion déjà en cours, skip...');
        return null;
    }
    
    if (retryCount > CONFIG.TIMEOUTS.MAX_RETRIES) {
        console.error(`💥 Max retries atteint (${CONFIG.TIMEOUTS.MAX_RETRIES})`);
        console.error('   → Reset automatique dans 2 minutes...');
        
        isBotStarting = false;
        
        reconnectTimeout = setTimeout(() => {
            retryCount = 0;
            console.log('🔄 Reset retry count - Nouvelle tentative autorisée');
            connectWhatsApp();
        }, 120000);
        
        return null;
    }
    
    // ----------------------------------------------------------
    // DÉBUT CONNEXION
    // ----------------------------------------------------------
    
    isBotStarting = true;
    isFullyInitialized = false; // Reset
    
    // Annuler tout timeout d'init précédent
    if (initTimeoutHandle) {
        clearTimeout(initTimeoutHandle);
        initTimeoutHandle = null;
    }
    
    try {
        console.log('\n' + '='.repeat(60));
        console.log(`🔐 CONNEXION WHATSAPP v3.0`);
        console.log(`📊 Tentative #${retryCount + 1}/${CONFIG.TIMEOUTS.MAX_RETRIES + 1}`);
        console.log(`🕐 Heure: ${new Date().toISOString()}`);
        console.log('='.repeat(60) + '\n');

        // ----------------------------------------------------------
        // MONGODB
        // ----------------------------------------------------------
        
        console.log('🗄️ Connexion MongoDB Atlas...');
        
        if (mongoose.connection.readyState !== 1) {
            await mongoose.connect(CONFIG.MONGO_URI, {
                serverSelectionTimeoutMS: 30000,
                socketTimeoutMS: 120000,
                maxPoolSize: 10,
                bufferCommands: false,
                heartbeatFrequencyMS: 10000
            });
        }
        console.log('✅ MongoDB connecté !\n');

        // ----------------------------------------------------------
        // AUTH STATE
        // ----------------------------------------------------------
        
        const { state, saveCreds } = await useMongoDBAuthState();

        // ----------------------------------------------------------
        // NETTOYAGE ANCIEN SOCKET
        // ----------------------------------------------------------
        
        if (sock) {
            console.log('🔄 Nettoyage ancienne connexion...');
            
            try {
                sock.ev.removeAllListeners('connection.update');
                sock.ev.removeAllListeners('creds.update');
                sock.ev.removeAllListeners('error');
                sock.ev.removeAllListeners('messages.upsert');
                
                if (sock.ws?.readyState === 1) {
                    sock.ws.close(1000, 'Reconnexion planifiée');
                }
            } catch (e) {
                console.log('   ⚠️ Erreur cleanup:', e.message);
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

        // ----------------------------------------------------------
        // CRÉATION SOCKET
        // ----------------------------------------------------------
        
        console.log('📱 Création socket WhatsApp...\n');
        
        sock = makeWASocket({
            auth: state,
            usePairingCode: true,
            
            // ⭐ ANTI-TIMEOUT CHATS.JS
            syncFullHistory: false,
            shouldSyncHistoryMessage: () => false,
            
            // ⭐ TIMEOUTS OPTIMISÉS POUR RENDER
            connectTimeoutMs: CONFIG.TIMEOUTS.CONNECT_MS,      // 5 min
            queryTimeoutMs: CONFIG.TIMEOUTS.QUERY_MS,          // 10 min ⭐⭐⭐
            keepAliveIntervalMs: CONFIG.TIMEOUTS.KEEPALIVE_MS, // 45s
            
            // ⭐ RETRIES
            retryRequestDelayMs: 10000,
            maxMsgRetryCount: 5,
            
            // ⭐ IDENTITÉ
            browser: ["Ubuntu", "Chrome", "20.0.04"],
            
            // ⭐ LOGGING
            logger: pino({ 
                level: 'warn',
                transport: {
                    target: 'pino-pretty',
                    options: { colorize: true, translateTime: 'SYS:h:MM:ss TT' }
                }
            }),
            
            // ⭐ OPTIONS
            markOnlineOnConnect: false,
            
            // ⭐ AGENT (optionnel, pour environnements restreints)
            // agent: require('https').Agent({ keepAlive: true })
        });

        // ----------------------------------------------------------
        // EVENT LISTENERS
        // ----------------------------------------------------------
        
        // 💾 Créds
        sock.ev.on('creds.update', async (creds) => {
            try {
                await saveCreds(creds);
            } catch (e) {
                console.error('❌ Erreur sauvegarde creds:', e.message);
            }
        });

        // 📡 Connection updates
        let connectionErrorHandled = false;
        
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            // QR Code
            if (qr) {
                currentQR = qr;
                qrGeneratedAt = Date.now();
                console.log('📷 QR Code généré');
            }

            // Pairing code (une seule fois)
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
                    console.log(`📱 NUMÉRO CIBLE : ${cleanNumber}`);
                    console.log(`🔑 CODE D'APPAIRAGE : ${code}`);
                    console.log('='.repeat(50) + '\n');
                    
                } catch (err) {
                    console.error('❌ Erreur pairing code:', err.message);
                    pairingCodeRequested = false;
                }
            }

            // Connexion fermée
            if (connection === 'close') {
                isReady = false;
                isFullyInitialized = false;
                isBotStarting = false;
                pairingCodeRequested = false;
                currentQR = null;
                qrGeneratedAt = null;
                connectionErrorHandled = false;
                
                // Annuler timeout d'init
                if (initTimeoutHandle) {
                    clearTimeout(initTimeoutHandle);
                    initTimeoutHandle = null;
                }
                
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const reason = lastDisconnect?.error?.output?.payload?.message || 'Inconnue';
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                
                console.log('\n' + '❌'.repeat(30));
                console.log(` CONNEXION FERMÉE`);
                console.log(` Status: ${statusCode || 'N/A'}`);
                console.log(` Raison: ${reason}`);
                console.log(` Reconnexion: ${shouldReconnect ? 'OUI ✅' : 'NON ❌'}`);
                console.log('❌'.repeat(30) + '\n');
                
                if (shouldReconnect) {
                    retryCount++;
                    const delay = getProgressiveDelay();
                    console.log(`🔄 Reconnexion dans ${(delay / 1000).toFixed(1)}s (retry #${retryCount})\n`);
                    
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), delay);
                } else {
                    console.log('⛔ Logged out - Supprimez auth/ et redémarrez\n');
                    retryCount = 0;
                }
            }

            // Connexion ouverte ✅
            if (connection === 'open') {
                isReady = true;
                isBotStarting = false;
                retryCount = 0;
                connectionErrorHandled = false;
                connectionOpenCount++;
                currentQR = null;
                qrGeneratedAt = null;
                
                // ⭐ IMPORTANT: Ne PAS marquer fullyInitialized ici encore
                // On attend de voir si l'init se passe bien
                
                const userJid = sock.user?.id || 'inconnu';
                const userName = sock.user?.name || 'Sans nom';
                
                console.log('\n' + '✅'.repeat(30));
                console.log(` CONNEXION WHATSAPP RÉUSSIE !`);
                console.log('✅'.repeat(30));
                console.log(`\n👤 Utilisateur: ${userName}`);
                console.log(`📱 JID: ${userJid}`);
                console.log(`🔢 Connexion #: ${connectionOpenCount}`);
                console.log(`⏰ Heure: ${new Date().toISOString()}`);
                console.log(`\n⏳ Attente finalisation initialisation...`);
                console.log(`   (Max ${CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS / 1000}s avant reconnexion si échec)\n`);
                
                // ⭐ Planifier un timeout pour l'initialisation
                // Si après ce délai on n'est pas fullyInitialized, on reconnecte
                initTimeoutHandle = setTimeout(async () => {
                    if (!isFullyInitialized && isReady) {
                        console.warn('\n' + '⚠️'.repeat(35));
                        console.warn(' ⚠️ INITIALISATION TROP LONGUE ⚠️');
                        console.warn(' Le socket est connecté mais l\'init semble bloquée');
                        console.warn(' Stratégie: Reconnexion pour forcer un init propre');
                        console.warn('⚠️'.repeat(35) + '\n');
                        
                        // Forcer reconnexion
                        isFullyInitialized = false;
                        await forceCleanReconnect();
                    }
                }, CONFIG.TIMEOUTS.INIT_MAX_WAIT_MS); // 90 secondes
            }
        });

        // 🛠️ Gestion erreurs socket
        sock.ev.on('error', async (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            const isErrorInitChats = 
                stack.includes('chats.js') || 
                stack.includes('fetchProps') || 
                stack.includes('executeInitQueries');
            
            // --- TIMEOUT LORS DE L'INIT DES CHATS ---
            if ((msg.includes('Timed Out') || stack.includes('Timed Out')) && isErrorInitChats) {
                if (!connectionErrorHandled) {
                    connectionErrorHandled = true;
                    
                    console.warn('\n' + '⚠️'.repeat(35));
                    console.warn(' ⚠️ TIMEOUT SYNCHRO CHATS DÉTECTÉ ⚠️');
                    console.warn('⚠️'.repeat(35));
                    console.warn('\n📋 Ce timeout rend le socket INSTABLE pour l\'envoi');
                    console.warn('📋 Action: Reconnexion forcée immédiate\n');
                    
                    // Annuler le timeout d'init car on sait déjà que ça a échoué
                    if (initTimeoutHandle) {
                        clearTimeout(initTimeoutHandle);
                        initTimeoutHandle = null;
                    }
                    
                    // ⭐ STRATÉGIE CLÉ: Reconnexion forcée au lieu d'ignorer
                    // Ignorer l'erreur menait à un socket zombie incapable d'envoyer
                    await forceCleanReconnect();
                }
                return;
            }
            
            // --- AUTRES TIMEOUTS ---
            if (msg.includes('Timed Out') || stack.includes('Timed Out')) {
                console.warn('⚠️ [TIMEOUT] Socket timeout (non-critique):');
                console.warn(`   ${msg.substring(0, 100)}\n`);
                return;
            }
            
            // --- STREAM ERRORS (normales) ---
            if (msg.includes('stream') || msg.includes('conflict') || msg.includes('Stream removed')) {
                console.warn('⚠️ [STREAM] Erreur stream (normale)\n');
                return;
            }
            
            // --- PRESENCE WARNING (inoffensif) ---
            if (msg.includes('no name present')) {
                // Silencieux
                return;
            }
            
            // --- AUTRES ERREURS ---
            console.error('❌ [ERROR] Erreur socket:', error.constructor.name);
            console.error('   Message:', msg.substring(0, 200));
            if (stack) console.error('   Stack:', stack.substring(0, 200), '\n');
        });

        // 📨 Messages entrants
        sock.ev.on('messages.upsert', async ({ messages, type }) => {
            // Marquer comme initialisé dès qu'on reçoit un message
            // (preuve que le socket fonctionne)
            if (!isFullyInitialized && isReady && sock?.ws?.readyState === 1) {
                console.log('📨 Premier message reçu → Initialisation confirmée fonctionnelle');
                markAsFullyInitialized();
            }
            
            if (!isReady || !messages) return;
            
            try {
                const realMessages = messages.filter(m => !m.notificationType && !m.key.fromMe);
                
                for (const msg of realMessages) {
                    // Votre logique de traitement ici
                    await handleIncomingMessage(sock, msg);
                }
                
            } catch (err) {
                console.error('❌ Erreur traitement message:', err.message);
            }
        });

        // ----------------------------------------------------------
        // FIN CRÉATION SOCKET
        // ----------------------------------------------------------
        
        console.log('✅ Socket créé - Attente événements...\n');

        return sock;

    } catch (err) {
        console.error('\n' + '💥'.repeat(35));
        console.error(` ERREUR CRITIQUE: ${err.message}`);
        console.error('💥'.repeat(35) + '\n');
        
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
// 🔧 FONCTIONS AUXILIAIRES CRITIQUES
// ============================================================

/**
 * Marque le socket comme pleinement initialisé et prêt pour l'envoi
 */
function markAsFullyInitialized() {
    if (!isFullyInitialized) {
        isFullyInitialized = true;
        
        // Annuler le timeout d'init si présent
        if (initTimeoutHandle) {
            clearTimeout(initTimeoutHandle);
            initTimeoutHandle = null;
        }
        
        console.log('\n' + '🎉'.repeat(25));
        console.log('  SOCKET PLEINEMENT INITIALISÉ !');
        console.log('  ✅ Prêt à envoyer/recevoir des messages');
        console.log('🎉'.repeat(25) + '\n');
        
        // Traiter la file d'attente des messages en retard
        setTimeout(processSendQueue, 2000);
    }
}

/**
 * Force une reconnexion propre (fermeture + recréation)
 */
async function forceCleanReconnect() {
    console.log('\n🔁 DÉBUT RECONNEXION FORCÉE PROPRE...');
    
    try {
        // 1. Nettoyage complet
        if (sock) {
            console.log('   1/4 Suppression listeners...');
            sock.ev.removeAllListeners();
            
            if (sock.ws?.readyState === 1) {
                console.log('   2/4 Fermeture WebSocket...');
                sock.ws.close(4001, 'Reconnexion forcée après timeout init');
            }
        }
        
        // 2. Reset état
        console.log('   3/4 Reset état global...');
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
        
        // 3. Attendre
        console.log('   4/4 Attente 5s...');
        await sleep(5000);
        
        // 4. Relancer (sans incrémenter retryCount car c'est une récupération proactive)
        console.log('🚀 Relancement connexion...\n');
        // retryCount = Math.max(0, retryCount - 1); // Optionnel: ne pas pénaliser
        
        await connectWhatsApp();
        
    } catch (err) {
        console.error('❌ Erreur pendant reconnexion forcée:', err.message);
        // Fallback: retry normal
        retryCount++;
        const delay = getProgressiveDelay();
        setTimeout(() => connectWhatsApp(), delay);
    }
}

/**
 * Handler pour les messages entrants (à implémenter selon vos besoins)
 */
async function handleIncomingMessage(socket, msg) {
    // TODO: Votre logique métier ici
    const from = msg.key.remoteJid;
    const body = msg.message?.conversation || msg.message?.extendedTextMessage?.text || '';
    
    console.log(`📩 Message de ${from}: ${body.substring(0, 50)}...`);
    
    // Exemple de réponse automatique (décommentez si souhaité):
    /*
    if (body.toLowerCase() === 'ping') {
        await sendMessage(from, { text: 'pong! 🏓' });
    }
    */
}

// ============================================================
// 🌐 API ROUTES (pour Express/Fastify)
// ============================================================

/**
 * Setup les routes API pour le bot
 * @param {object} app - Express/Fastify app
 */
function setupAPIRoutes(app) {
    // Health check
    app.get('/api/health', (req, res) => {
        const health = checkSocketHealth();
        res.json({
            service: 'whatsapp-bot-v3',
            status: health.healthy ? 'OK' : health.ready ? 'DEGRADED' : 'DOWN',
            ...health,
            config: {
                maxRetries: CONFIG.TIMEOUTS.MAX_RETRIES,
                currentRetry: retryCount,
                queuedMessages: sendQueue.length
            }
        });
    });
    
    // Envoyer un message
    app.post('/api/send', async (req, res) => {
        try {
            const { jid, message, options } = req.body;
            
            if (!jid || !message) {
                return res.status(400).json({ error: 'jid et message requis' });
            }
            
            const result = await sendMessage(jid, message, options);
            res.json({ success: true, result });
            
        } catch (err) {
            console.error('❌ API /send error:', err.message);
            res.status(500).json({ error: err.message });
        }
    });
    
    // QR Code
    app.get('/api/qr', (req, res) => {
        if (!currentQR || !qrGeneratedAt) {
            return res.json({ 
                qr: null, 
                status: 'waiting',
                message: 'En attente de génération du QR'
            });
        }
        
        const ageSeconds = (Date.now() - qrGeneratedAt) / 1000;
        res.json({
            qr: currentQR,
            generatedAt: qrGeneratedAt,
            ageSeconds: Math.round(ageSeconds),
            expired: ageSeconds > 20, // QR expire après ~20s
            status: 'active'
        });
    });
    
    // Statistiques
    app.get('/api/stats', (req, res) => {
        res.json({
            connections: connectionOpenCount,
            retries: retryCount,
            ready: isReady,
            fullyInitialized: isFullyInitialized,
            queuedMessages: sendQueue.length,
            lastMessageSent: lastMessageSentTime,
            uptime: process.uptime()
        });
    });
    
    console.log('✅ Routes API configurées: /health, /send, /qr, /stats');
}

// ============================================================
// 🚀 DÉMARRAGE AUTOMATIQUE
// ============================================================

/**
 * Démarre le bot (appeler au démarrage du serveur)
 */
async function startBot() {
    console.log('\n' + '🚀'.repeat(30));
    console.log(' DÉMARRAGE WHATSAPP BOT v3.0');
    console.log('🚀'.repeat(30));
    console.log(`\n⏰ Heure: ${new Date().toISOString()}`);
    console.log(`🎯 Version: Production Stable`);
    console.log(`📍 Environnement: ${process.env.NODE_ENV || 'development'}`);
    
    // Démarrer la connexion
    await connectWhatsApp();
    
    // Health check périodique (toutes les 5 minutes)
    setInterval(() => {
        const health = checkSocketHealth();
        console.log(`\n${new Date().toISOString()} | Health: ${health.healthy ? '✅ OK' : health.ready ? '⚠️ DEGRADED' : '❌ DOWN'} | Init: ${health.fullyInitialized} | Queue: ${sendQueue.length}`);
        
        // Tentative de traitement de la file si dégradé mais ready
        if (health.ready && !health.fullyInitialized && sendQueue.length > 0) {
            console.log('⚠️ File d\'attente non vide mais init incomplète - Messages en attente');
        }
    }, 300000); // 5 minutes
}

// ============================================================
// 📦 EXPORTS
// ============================================================

module.exports = {
    // Fonctions principales
    connectWhatsApp,
    startBot,
    setupAPIRoutes,
    
    // Fonctions d'envoi
    sendMessage,
    canSendMessage,
    
    // Utilitaires
    checkSocketHealth,
    getProgressiveDelay,
    
    // États (lecture seule)
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
