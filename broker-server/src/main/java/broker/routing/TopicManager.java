package broker.routing;

import broker.model.Message;
import broker.session.ClientSession;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * ══════════════════════════════════════════════════════════════════
 * TopicManager — Patrón 3: Publicador-Suscriptor (Pub/Sub) con Topics.
 * ══════════════════════════════════════════════════════════════════
 *
 * ¿QUÉ ES PUB/SUB?
 * ──────────────────────────────────────────────────────────────────
 * El publicador emite mensajes a un TOPIC sin saber quién los recibe.
 * TODOS los consumers suscritos a ese topic reciben el mensaje (fan-out).
 * El publisher y los suscriptores están completamente desacoplados.
 *
 * DIFERENCIA con Send & Forget:
 *   Send & Forget → mensaje va a UN solo consumer  (balanceo de carga)
 *   Pub/Sub       → mensaje va a TODOS los consumers (broadcast)
 *
 * CARACTERÍSTICAS:
 *   ✔ Fan-out: un mensaje puede llegar a 1, 10 o 1000 suscriptores.
 *   ✔ Sin estado intermedio: si no hay suscriptores al publicar, el mensaje
 *     se descarta (los suscriptores deben estar conectados para recibirlos).
 *   ✔ Múltiples topics: un consumer puede suscribirse a N topics distintos.
 *   ✔ Entrega sincrónica: la publicación itera suscriptores directamente
 *     (no hay cola intermedia, máxima velocidad de entrega).
 *
 * PROTOCOLO TCP:
 *   Producer → Broker:   TOPIC_PUBLISH|<topic>|<contenido>
 *   Consumer → Broker:   TOPIC_SUBSCRIBE|<topic>
 *   Broker   → Consumer: TOPIC_MSG|<topic>|<id>|<timestamp>|<contenido>
 *
 * CASOS DE USO REALES:
 *   - Topic "precios": todos los servicios de carrito reciben actualizaciones.
 *   - Topic "alertas": N dashboards muestran las mismas alarmas en tiempo real.
 *   - Topic "eventos": event sourcing — múltiples proyectores consumen el mismo stream.
 *   - Topic "chat-sala1": todos los usuarios en la sala reciben los mensajes.
 *
 * ESTRUCTURA DE DATOS:
 *   ConcurrentHashMap<String, Set<ClientSession>>:
 *   - Key: nombre del topic
 *   - Value: conjunto de suscriptores activos (ConcurrentHashSet = thread-safe)
 *   - computeIfAbsent garantiza que la creación del Set sea atómica.
 */
public class TopicManager {

    private static final Logger log = Logger.getLogger(TopicManager.class.getName());

    /**
     * Mapa de suscriptores por topic.
     * ConcurrentHashMap.newKeySet() devuelve un Set backed por ConcurrentHashMap,
     * que soporta operaciones concurrentes de add/remove/iterate sin ConcurrentModificationException.
     */
    private final ConcurrentHashMap<String, Set<ClientSession>> topicSubscribers =
        new ConcurrentHashMap<>();

    // ══════════════════════════════════════════════════════════════
    // API PÚBLICA
    // ══════════════════════════════════════════════════════════════

    /**
     * Suscribe un consumer a un topic nombrado.
     * Crea el topic automáticamente si no existe.
     * Un consumer puede llamar a este método múltiples veces con distintos topics.
     */
    public void subscribe(String topicName, ClientSession session) {
        topicSubscribers
            .computeIfAbsent(topicName, k -> ConcurrentHashMap.newKeySet())
            .add(session);
        session.subscribeToTopic(topicName);
        log.info("[TOPIC] " + session + " suscrito a topic '" + topicName + "'"
            + " | suscriptores: " + topicSubscribers.get(topicName).size());
    }

    /**
     * Elimina un consumer de todos sus topics al desconectarse.
     */
    public void unsubscribeAll(ClientSession session) {
        for (String topic : session.getTopicSubscriptions()) {
            Set<ClientSession> subs = topicSubscribers.get(topic);
            if (subs != null) subs.remove(session);
        }
    }

    /**
     * Publica un mensaje en un topic → entrega a TODOS los suscriptores (fan-out).
     *
     * La entrega es sincrónica: iterar y llamar session.send() para cada suscriptor.
     * Esto es eficiente porque session.send() → writer.println() es muy rápido.
     *
     * Nota: si un suscriptor está lento (socket congestionado), println() puede
     * bloquearse brevemente para ese suscriptor, pero no afecta a los demás
     * porque la iteración continúa con el siguiente.
     *
     * @param topicName el topic donde publicar
     * @param message   el mensaje a distribuir
     */
    public void publish(String topicName, Message message) {
        Set<ClientSession> subscribers = topicSubscribers.get(topicName);

        if (subscribers == null || subscribers.isEmpty()) {
            log.warning("[TOPIC] Topic '" + topicName
                + "' sin suscriptores. Mensaje descartado (normal en Pub/Sub).");
            return;
        }

        // PROTOCOLO: TOPIC_MSG|<topic>|<id>|<timestamp>|<contenido>
        String payload = "TOPIC_MSG|" + topicName + "|"
            + message.id() + "|" + message.timestamp() + "|" + message.content();

        int delivered = 0;
        // Iteración sobre ConcurrentHashSet: thread-safe, puede cambiar mientras iteramos
        for (ClientSession subscriber : subscribers) {
            subscriber.send(payload);
            if (!subscriber.hasError()) {
                delivered++;
            }
        }

        log.info("[TOPIC] PUBLICADO → topic='" + topicName + "'"
            + " | entregado a " + delivered + "/" + subscribers.size() + " suscriptor(es)"
            + " | msgId=" + message.id().substring(0, 8) + "...");
    }

    public int getSubscriberCount(String topicName) {
        Set<ClientSession> s = topicSubscribers.get(topicName);
        return s == null ? 0 : s.size();
    }
}
