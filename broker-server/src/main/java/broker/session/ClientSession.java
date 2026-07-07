package broker.session;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ══════════════════════════════════════════════════════════════════
 * ClientSession — Estado de la conexión de UN cliente con el broker.
 * ══════════════════════════════════════════════════════════════════
 *
 * Encapsula identidad, canal TCP y suscripciones activas de un cliente.
 *
 * POR QUÉ send() ES synchronized:
 *   Múltiples virtual threads pueden escribir al mismo cliente al mismo tiempo:
 *   - El dispatcher de la cola "orders" envía QUEUE_MSG
 *   - Un publisher del topic "news" envía TOPIC_MSG
 *   - El RR manager envía RESPONSE_MSG
 *   PrintWriter NO es thread-safe. synchronized garantiza que los bytes
 *   de cada mensaje lleguen completos y en orden al cliente.
 */
public class ClientSession {

    private final String sessionId;
    private final String role;        // "PRODUCER" o "CONSUMER"
    private final PrintWriter writer; // canal de salida TCP hacia este cliente

    // Suscripciones activas del consumer (thread-safe via ConcurrentHashSet)
    private final Set<String> queueSubscriptions = ConcurrentHashMap.newKeySet();
    private final Set<String> topicSubscriptions  = ConcurrentHashMap.newKeySet();

    /** true si este consumer maneja el patrón Request-Response */
    private volatile boolean rrHandler = false;

    public ClientSession(String sessionId, String role, PrintWriter writer) {
        this.sessionId = sessionId;
        this.role      = role;
        this.writer    = writer;
    }

    /**
     * Envía una línea al cliente por TCP.
     * synchronized porque múltiples VT pueden escribir concurrentemente.
     */
    public synchronized void send(String message) {
        writer.println(message);
    }

    /**
     * Devuelve true si el canal tuvo un error (socket cerrado).
     * Usado por dispatchers para detectar consumers desconectados.
     */
    public synchronized boolean hasError() {
        return writer.checkError();
    }

    // ── Suscripciones ─────────────────────────────────────────────
    public void subscribeToQueue(String q) { queueSubscriptions.add(q); }
    public void subscribeToTopic(String t) { topicSubscriptions.add(t); }
    public void unsubscribeQueue(String q)  { queueSubscriptions.remove(q); }
    public void unsubscribeTopic(String t)  { topicSubscriptions.remove(t); }
    public boolean isInQueue(String q)      { return queueSubscriptions.contains(q); }
    public boolean isInTopic(String t)      { return topicSubscriptions.contains(t); }
    public void setRrHandler(boolean v)     { this.rrHandler = v; }
    public boolean isRrHandler()            { return rrHandler; }

    // ── Getters ───────────────────────────────────────────────────
    public String     getSessionId()             { return sessionId; }
    public String     getRole()                  { return role; }
    public Set<String> getQueueSubscriptions()   { return queueSubscriptions; }
    public Set<String> getTopicSubscriptions()   { return topicSubscriptions; }

    @Override
    public String toString() {
        return role + "[" + sessionId.substring(0, 8) + "...]";
    }
}
