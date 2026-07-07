package broker.routing;

import broker.model.Message;
import broker.session.ClientSession;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * ══════════════════════════════════════════════════════════════════
 * RequestResponseManager — Patrón 2: Request-Response (RPC sobre mensajería).
 * ══════════════════════════════════════════════════════════════════
 *
 * ¿QUÉ ES REQUEST-RESPONSE?
 * ──────────────────────────────────────────────────────────────────
 * El productor envía una SOLICITUD y ESPERA una RESPUESTA concreta.
 * A diferencia de HTTP (conexión directa), aquí el broker actúa como
 * intermediario: el producer y el consumer nunca se conectan entre sí.
 *
 * El mecanismo de correlación es clave:
 *   - El producer genera un correlationId único (UUID)
 *   - Envía: REQUEST|<correlId>|<contenido>
 *   - El broker guarda: correlId → sesión del producer
 *   - El consumer recibe: REQUEST_MSG|<correlId>|...|<contenido>
 *   - El consumer procesa y responde: RESPONSE|<correlId>|<respuesta>
 *   - El broker busca: correlId → sesión del producer
 *   - Entrega: RESPONSE_MSG|<correlId>|<respuesta> al producer original
 *
 * CARACTERÍSTICAS:
 *   ✔ Correlación: el correlationId es la "clave" que une request con response.
 *   ✔ Balanceo: múltiples consumers RR se distribuyen en round-robin.
 *   ✔ Timeout: si nadie responde en 30s, el producer recibe un error.
 *   ✔ Desacoplamiento: producer y consumer no conocen sus direcciones IP.
 *
 * PROTOCOLO TCP:
 *   Producer → Broker:   REQUEST|<correlId>|<contenido>
 *   Consumer → Broker:   RR_READY  (registrarse como handler)
 *   Broker   → Consumer: REQUEST_MSG|<correlId>|<id>|<timestamp>|<contenido>
 *   Consumer → Broker:   RESPONSE|<correlId>|<respuesta>
 *   Broker   → Producer: RESPONSE_MSG|<correlId>|<respuesta>
 *
 * CASOS DE USO REALES:
 *   - Consulta de inventario: "¿Hay stock de producto X?" → "Sí, 15 unidades"
 *   - Validación: "¿Es válida esta tarjeta?" → "Sí/No"
 *   - Cálculo distribuido: "Calcula precio con descuento" → "$ 89.90"
 *
 * ESTRUCTURAS DE DATOS:
 *   ConcurrentHashMap<correlId, ClientSession>: mapea cada request pendiente
 *   al producer que la envió. Thread-safe para lectura/escritura concurrente.
 *
 *   CopyOnWriteArrayList<ClientSession>: lista de consumers RR registrados.
 *   Optimizada para iteración frecuente (round-robin) con pocas modificaciones.
 *
 *   AtomicInteger: índice de round-robin. getAndIncrement() es atómico,
 *   sin necesidad de synchronized para el turno de cada consumer.
 */
public class RequestResponseManager {

    private static final Logger log = Logger.getLogger(RequestResponseManager.class.getName());

    /** Timeout en milisegundos antes de notificar al producer que no hubo respuesta */
    private static final long TIMEOUT_MS = 30_000L;

    /**
     * Requests pendientes: correlationId → sesión del producer original.
     * Cuando llega la RESPONSE, buscamos aquí al producer al que responder.
     */
    private final ConcurrentHashMap<String, ClientSession> pendingRequests =
        new ConcurrentHashMap<>();

    /** Lista de consumers registrados como handlers RR */
    private final CopyOnWriteArrayList<ClientSession> rrConsumers =
        new CopyOnWriteArrayList<>();

    /** Índice de round-robin para distribución equitativa entre handlers */
    private final AtomicInteger rrIndex = new AtomicInteger(0);

    // ══════════════════════════════════════════════════════════════
    // API PÚBLICA
    // ══════════════════════════════════════════════════════════════

    /**
     * Registra un consumer como handler de Request-Response.
     * A partir de aquí recibirá REQUEST_MSG del broker.
     */
    public void registerConsumer(ClientSession session) {
        session.setRrHandler(true);
        rrConsumers.add(session);
        log.info("[RR] Consumer registrado como handler: " + session
            + " | handlers activos: " + rrConsumers.size());
    }

    /**
     * Desregistra un consumer al desconectarse.
     */
    public void unregisterConsumer(ClientSession session) {
        rrConsumers.remove(session);
        log.info("[RR] Consumer RR desregistrado: " + session);
    }

    /**
     * Procesa una REQUEST entrante del producer:
     *   1. Verifica que haya consumers RR disponibles.
     *   2. Selecciona uno en round-robin.
     *   3. Le envía la REQUEST_MSG.
     *   4. Guarda correlId → producer para cuando llegue la response.
     *   5. Lanza un virtual thread de timeout (30s).
     *
     * @param producer     la sesión del producer que envió la request
     * @param correlId     el ID de correlación generado por el producer
     * @param message      el mensaje con el contenido de la solicitud
     */
    public void handleRequest(ClientSession producer, String correlId, Message message) {
        if (rrConsumers.isEmpty()) {
            producer.send("ERROR|No hay handlers RR conectados. Conecta un consumer con RR_READY.");
            log.warning("[RR] Request rechazada — sin handlers: correlId=" + correlId);
            return;
        }

        // Round-robin: seleccionar consumer
        int idx = Math.abs(rrIndex.getAndIncrement() % rrConsumers.size());
        ClientSession consumer = rrConsumers.get(idx);

        // Guardar la asociación correlId → producer
        pendingRequests.put(correlId, producer);

        // PROTOCOLO: REQUEST_MSG|<correlId>|<id>|<timestamp>|<contenido>
        String payload = "REQUEST_MSG|" + correlId + "|"
            + message.id() + "|" + message.timestamp() + "|" + message.content();

        consumer.send(payload);
        log.info("[RR] Request enviada → correlId=" + correlId
            + " | handler=" + consumer);

        // Virtual thread de timeout: si en 30s no llega RESPONSE, notificar al producer
        String corrIdFinal = correlId;
        Thread.ofVirtual().name("rr-timeout-" + correlId.substring(0, 8)).start(() -> {
            try {
                Thread.sleep(TIMEOUT_MS);
                // remove() devuelve null si ya fue procesado (la response llegó)
                ClientSession timedOutProducer = pendingRequests.remove(corrIdFinal);
                if (timedOutProducer != null) {
                    timedOutProducer.send("RESPONSE_MSG|" + corrIdFinal
                        + "|TIMEOUT: El handler no respondió en "
                        + (TIMEOUT_MS / 1000) + " segundos");
                    log.warning("[RR] TIMEOUT → correlId=" + corrIdFinal);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Procesa una RESPONSE del consumer:
     *   1. Busca al producer original por correlId.
     *   2. Le entrega la respuesta.
     *   3. Limpia el estado pendiente (permite que el VT de timeout termine limpiamente).
     *
     * @param correlId        el ID que vincula esta response con su request
     * @param responseContent el contenido de la respuesta del consumer
     */
    public void handleResponse(String correlId, String responseContent) {
        ClientSession producer = pendingRequests.remove(correlId);

        if (producer == null) {
            log.warning("[RR] Response ignorada — correlId desconocido o expirado: " + correlId);
            return;
        }

        // PROTOCOLO: RESPONSE_MSG|<correlId>|<respuesta>
        producer.send("RESPONSE_MSG|" + correlId + "|" + responseContent);
        log.info("[RR] Response entregada al producer → correlId=" + correlId
            + " | respuesta='" + responseContent + "'");
    }

    public int getHandlerCount() { return rrConsumers.size(); }
}
