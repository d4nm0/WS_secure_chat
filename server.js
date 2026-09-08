import { WebSocketServer, WebSocket } from 'ws';

// Utiliser le port attribué par Render ou 8080 par défaut en local
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const clients = new Map();

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
        }
      }

      // 2. Événement d'envoi de message chiffré vers un autre utilisateur
      else if (message.type === 'message') {
        const { recipientId, encryptedPayload, senderPublicKey } = message;
        
        const recipientWs = clients.get(recipientId);
        if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          // Transférer le message ET la clé publique de l'expéditeur au destinataire
          recipientWs.send(JSON.stringify({
            type: 'message',
            senderId: currentUserId,
            encryptedPayload: encryptedPayload,
            senderPublicKey: senderPublicKey
          }));
          console.log(`[Message relayé] De ${currentUserId} vers ${recipientId}`);
        } else {
          // Destinataire introuvable ou hors ligne
          ws.send(JSON.stringify({
            type: 'error',
            message: `L'utilisateur ${recipientId} est introuvable ou hors ligne.`
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