import { WebSocketServer, WebSocket } from 'ws';

// Utiliser le port attribué par Render ou 8080 par défaut en local
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const clients = new Map();
// Nouvelle Map pour stocker les messages en attente : userId -> Tableau de messages
const offlineMessages = new Map();

console.log(`Serveur de relais WebSocket démarré sur le port ${PORT}`);

wss.on('connection', (ws) => {
  let currentUserId = null;

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);

      // 1. Événement d'enregistrement de l'utilisateur avec son ID aléatoire
      if (message.type === 'register') {
        currentUserId = message.userId;
        if (currentUserId) {
          clients.set(currentUserId, ws);
          console.log(`[Connecté] Utilisateur enregistré : ${currentUserId}`);

          // Dès que l'utilisateur s'enregistre, on lui envoie ses messages en attente s'il y en a !
          if (offlineMessages.has(currentUserId)) {
            const pending = offlineMessages.get(currentUserId);
            console.log(`[File d'attente] Envoi de ${pending.length} message(s) en attente à ${currentUserId}`);
            
            pending.forEach((msg) => {
              ws.send(JSON.stringify(msg));
            });

            // On vide la file d'attente une fois envoyés
            offlineMessages.delete(currentUserId);
          }
        }
      }

      // 2. Événement d'envoi de message chiffré vers un autre utilisateur
      else if (message.type === 'message') {
        const { recipientId, encryptedPayload, senderPublicKey } = message;
        
        const recipientWs = clients.get(recipientId);
        const messagePayload = {
          type: 'message',
          senderId: currentUserId,
          encryptedPayload: encryptedPayload,
          senderPublicKey: senderPublicKey
        };

        if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          // Destinataire en ligne : Transférer immédiatement
          recipientWs.send(JSON.stringify(messagePayload));
          console.log(`[Message relayé] De ${currentUserId} vers ${recipientId}`);
        } else {
          // Destinataire HORS LIGNE : Stocker dans sa file d'attente personnelle
          if (!offlineMessages.has(recipientId)) {
            offlineMessages.set(recipientId, []);
          }
          offlineMessages.get(recipientId).push(messagePayload);
          console.log(`[Hors ligne] Message stocké pour ${recipientId} (expéditeur: ${currentUserId})`);

          // Optionnel : On peut notifier l'expéditeur que le message a bien été mis en attente 
          // (au lieu de lui renvoyer une erreur bloquante)
          ws.send(JSON.stringify({
            type: 'info',
            message: `Utilisateur hors ligne. Message mis en attente sur le serveur.`
          }));
        }
      }
    } catch (e) {
      console.error('Erreur lors du traitement du message JSON :', e);
    }
  });

  ws.on('close', () => {
    if (currentUserId) {
      clients.delete(currentUserId);
      console.log(`[Déconnecté] Utilisateur retiré : ${currentUserId}`);
    }
  });
});