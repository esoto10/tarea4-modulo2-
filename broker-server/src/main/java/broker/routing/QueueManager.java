package broker.routing;

import broker.model.Message;
import broker.session.ClientSession;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * ══════════════════════════════════════════════════════════════════
 * QueueManager — Patrón 1: Send & Forget (Cola nombrada).
 * ══════════════════════════════════════════════════════════════════
 *
 * ¿QUÉ ES SEND & FORGET?
 * ──────────────────────────────────────────────────────────────────
 * El productor ENVÍA el mensaje y NO espera confirmación ni respuesta.
 * El broker garantiza entrega a exactamente UN consumer (trabajo compartido).
 *
 * DIFERENCIA con Pub/Sub:
 *   Send & Forget → mensaje va a UN solo consumer  (work queue / reparto de carga)
 *   Pub/Sub       → mensaje va a TODOS los consumers (broadcast / fan-out)
 *
 * CARACTERÍSTICAS:
 *   ✔ Desacoplamiento temporal: el consumer no necesita estar activo al enviar.
 *   ✔ Balanceo automático: con N workers en la misma cola, los mensajes se
 *     reparten equitativamente en round-robin.
 *   ✔ Backpressure: si la cola llega a 1000 mensajes, el productor espera
 *     (put() bloquea) en lugar de perder datos o causar OutOfMemoryError.
 *   ✔ Un dispatcher virtual thread por cola — sin busy-wait (take() duerme).
 *
 * PROTOCOLO TCP:
 *   Producer → Broker:   QUEUE_SEND|<cola>|<contenido>
 *   Consumer → Broker:   QUEUE_SUBSCRIBE|<cola>
 *   Broker   → Consumer: QUEUE_MSG|<cola>|<id>|<timestamp>|<contenido>
 *
 * CASOS DE USO REALES:
 *   - Cola "emails":  múltiples workers envían correos en paralelo.
 *   - Cola "orders":  microservicio de pagos procesa pedidos uno a uno.
 *   - Cola "resize":  workers redimensionan imágenes subidas por usuarios.
 *
 * ESTRUCTURAS DE DATOS:
 *   LinkedBlockingQueue  → cola de mensajes pendientes por cola nombrada.
 *   CopyOnWriteArrayList → lista de consumers suscritos (optimizada para
 *                          muchas lecturas en round-robin, pocas escrituras).
 *   AtomicInteger        → índice de round-robin por cola (sin synchronized).
 */
public class QueueManager {

    private static final Logger log = Logger.getLogger(QueueManager.class.getName());

    /** Mensajes pendientes por cola: queueName → BlockingQueue */
    private final ConcurrentHashMap<String, LinkedBlockingQueue<Message>> queues =
        new ConcurrentHashMap<>();

    /** Consumers suscritos por cola: queueName → lista de sessions */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ClientSession>> subscribers =
        new ConcurrentHashMap<>();

    /** Índice de round-robin por cola (atómico = thread-safe sin locks) */
    private final ConcurrentHashMap<String, AtomicInteger> rrIndex =
        new ConcurrentHashMap<>();

    /** Pool de Virtual Threads para los dispatchers (uno por cola) */
    private final ExecutorService dispatchers = Executors.newVirtualThreadPerTaskExecutor();

    // ══════════════════════════════════════════════════════════════
    // API PÚBLICA
    // ══════════════════════════════════════════════════════════════

    /**
     * Suscribe un consumer a una cola nombrada.
     * Si la cola no existe, la crea y lanza su dispatcher en un Virtual Thread.
     */
    public void subscribe(String queueName, ClientSession session) {
        subscribers.computeIfAbsent(queueName, k -> new CopyOnWriteArrayList<>()).add(session);
        queues.computeIfAbsent(queueName, k -> {
            LinkedBlockingQueue<Message> q = new LinkedBlockingQueue<>(1_000);
            rrIndex.put(queueName, new AtomicInteger(0));
            // Un virtual thread dispatcher dedicado a esta cola
            dispatchers.submit(() -> dispatchLoop(queueName, q));
            log.info("[QUEUE] Cola '" + queueName + "' creada con dispatcher VT");
            return q;
        });
        session.subscribeToQueue(queueName);
        log.info("[QUEUE] " + session + " suscrito a '" + queueName + "'"
            + " | workers en cola: " + subscribers.get(queueName).size());
    }

    /**
     * Elimina un consumer de todas sus colas al desconectarse.
     * Llamado por ConnectionHandler en el bloque finally.
     */
    public void unsubscribeAll(ClientSession session) {
        for (String q : session.getQueueSubscriptions()) {
            CopyOnWriteArrayList<ClientSession> subs = subscribers.get(q);
            if (subs != null) subs.remove(session);
        }
    }

    /**
     * Encola un mensaje en una cola nombrada.
     * Si la cola no existe aún (ningún consumer conectado), la crea y espera.
     * put() → backpressure natural: bloquea al productor si la cola está llena.
     */
    public void enqueue(String queueName, Message message) {
        LinkedBlockingQueue<Message> queue = queues.computeIfAbsent(queueName, k -> {
            LinkedBlockingQueue<Message> q = new LinkedBlockingQueue<>(1_000);
            rrIndex.put(queueName, new AtomicInteger(0));
            subscribers.computeIfAbsent(queueName, n -> new CopyOnWriteArrayList<>());
            dispatchers.submit(() -> dispatchLoop(queueName, q));
            log.info("[QUEUE] Cola '" + queueName + "' auto-creada al enviar");
            return q;
        });
        try {
            queue.put(message);
            log.info("[QUEUE] ENQUEUE → cola='" + queueName + "'"
                + " | id=" + message.id().substring(0, 8) + "..."
                + " | pendientes=" + queue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ══════════════════════════════════════════════════════════════
    // DISPATCHER INTERNO — un Virtual Thread por cola
    // ══════════════════════════════════════════════════════════════

    /**
     * Loop de despacho para UNA cola. Corre en su propio Virtual Thread.
     *
     * take() duerme eficientemente cuando la cola está vacía (sin busy-wait).
     * Round-robin asegura distribución equitativa entre workers.
     *
     * Ejemplo con 3 workers:
     *   Mensaje-1 → Worker-A
     *   Mensaje-2 → Worker-B
     *   Mensaje-3 → Worker-C
     *   Mensaje-4 → Worker-A  (vuelve al inicio)
     */
    private void dispatchLoop(String queueName, LinkedBlockingQueue<Message> queue) {
        log.info("[QUEUE] Dispatcher activo para cola '" + queueName + "'");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Bloquea sin gastar CPU hasta que llegue un mensaje
                Message msg = queue.take();

                // Si no hay consumers, esperar (el mensaje se conserva en la variable)
                CopyOnWriteArrayList<ClientSession> subs = subscribers.get(queueName);
                while (subs == null || subs.isEmpty()) {
                    log.warning("[QUEUE] '" + queueName + "' sin workers. Esperando...");
                    Thread.sleep(500);
                    subs = subscribers.get(queueName);
                }

                // Round-robin: índice atómico, módulo del tamaño actual
                boolean delivered = false;
                int attempts = 0;
                int size = subs.size();

                while (!delivered && attempts < size) {
                    int idx = Math.abs(rrIndex.get(queueName).getAndIncrement() % subs.size());
                    ClientSession consumer = subs.get(idx);

                    // PROTOCOLO: QUEUE_MSG|<cola>|<id>|<timestamp>|<contenido>
                    String payload = "QUEUE_MSG|" + queueName + "|"
                        + msg.id() + "|" + msg.timestamp() + "|" + msg.content();

                    consumer.send(payload);

                    if (consumer.hasError()) {
                        log.warning("[QUEUE] Consumer " + consumer + " con error, saltando...");
                        subs.remove(consumer);
                        attempts++;
                    } else {
                        log.info("[QUEUE] ENTREGADO → cola='" + queueName + "'"
                            + " | worker=" + consumer);
                        delivered = true;
                    }
                }

                if (!delivered) {
                    log.warning("[QUEUE] Fallo total. Reencolando: " + msg.id().substring(0, 8));
                    queue.put(msg);
                    Thread.sleep(300);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public int getSubscriberCount(String queueName) {
        CopyOnWriteArrayList<ClientSession> s = subscribers.get(queueName);
        return s == null ? 0 : s.size();
    }
}
