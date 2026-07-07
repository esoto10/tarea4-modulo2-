package broker.network;

import broker.model.Message;
import broker.server.BrokerServer;
import broker.session.ClientSession;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * ConnectionHandler — Gestiona la conexion de UN cliente con el broker.
 *
 * Corre en su propio Virtual Thread (uno por cliente conectado).
 * Lee la primera linea para identificar el rol y luego enruta
 * todos los comandos al manager correspondiente.
 *
 * PROTOCOLO DE IDENTIFICACION:
 *   Cliente → Broker:  "PRODUCER"  o  "CONSUMER"
 *
 * COMANDOS DEL PRODUCER:
 *   QUEUE_SEND|<cola>|<contenido>       → Patron 1: Send and Forget
 *   TOPIC_PUBLISH|<topic>|<contenido>   → Patron 3: Pub/Sub
 *   REQUEST|<correlId>|<contenido>      → Patron 2: Request-Response
 *
 * COMANDOS DEL CONSUMER:
 *   QUEUE_SUBSCRIBE|<cola>              → suscribirse a una cola (Patron 1)
 *   TOPIC_SUBSCRIBE|<topic>             → suscribirse a un topic (Patron 3)
 *   RR_READY                            → registrarse como handler RR (Patron 2)
 *   RESPONSE|<correlId>|<respuesta>     → enviar respuesta al producer (Patron 2)
 *
 * RESPUESTAS DEL BROKER AL PRODUCER:
 *   OK|<mensaje>                         → confirmacion
 *   ERROR|<mensaje>                      → error
 *   RESPONSE_MSG|<correlId>|<respuesta>  → respuesta del consumer (Patron 2)
 *
 * MENSAJES DEL BROKER AL CONSUMER:
 *   QUEUE_MSG|<cola>|<id>|<ts>|<contenido>      → Patron 1
 *   TOPIC_MSG|<topic>|<id>|<ts>|<contenido>     → Patron 3
 *   REQUEST_MSG|<correlId>|<id>|<ts>|<contenido>→ Patron 2
 */
public class ConnectionHandler implements Runnable {

    private static final Logger log = Logger.getLogger(ConnectionHandler.class.getName());

    private final Socket     socket;
    private final BrokerServer broker;

    public ConnectionHandler(Socket socket, BrokerServer broker) {
        this.socket = socket;
        this.broker = broker;
    }

    @Override
    public void run() {
        String addr = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        log.info("Nueva conexion TCP: " + addr + " | VirtualThread=" + Thread.currentThread().isVirtual());

        try (
            socket;
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"));
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")), true)
        ) {
            // Leer primera linea: identificacion del rol
            String firstLine = in.readLine();
            if (firstLine == null) { return; }

            // Limpiar caracteres de formato (BOM, etc.) y normalizar
            String role = firstLine.replaceAll("[\\p{Cc}\\p{Cf}]", "").trim().toUpperCase();
            log.info("Cliente " + addr + " identificado como: [" + role + "]");

            // Crear sesion unica para este cliente
            String sessionId = UUID.randomUUID().toString();
            ClientSession session = new ClientSession(sessionId, role, out);
            broker.addSession(session);

            try {
                switch (role) {
                    case "PRODUCER" -> handleProducer(in, session, addr);
                    case "CONSUMER" -> handleConsumer(in, session, addr);
                    default -> {
                        out.println("ERROR|Rol desconocido '" + role + "'. Usa PRODUCER o CONSUMER");
                        log.warning("Rol no reconocido de " + addr + ": [" + role + "]");
                    }
                }
            } finally {
                // SIEMPRE limpiar la sesion, incluso si hay excepcion
                broker.removeSession(session);
            }

        } catch (IOException e) {
            log.info("Conexion cerrada: " + addr + " (" + e.getMessage() + ")");
        }
    }

    // ══════════════════════════════════════════════════════════════
    // HANDLER PRODUCER
    // ══════════════════════════════════════════════════════════════

    /**
     * Lee y procesa comandos del producer hasta que se desconecte.
     * Cada comando se enruta al manager del patron correspondiente.
     */
    private void handleProducer(BufferedReader in, ClientSession session, String addr)
            throws IOException {

        session.send("OK|Conectado como PRODUCER. Comandos disponibles:");
        session.send("OK|  QUEUE_SEND|<cola>|<msg>        (Send and Forget)");
        session.send("OK|  REQUEST|<correlId>|<msg>        (Request-Response)");
        session.send("OK|  TOPIC_PUBLISH|<topic>|<msg>     (Pub/Sub)");

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isBlank()) continue;

            if (line.startsWith("QUEUE_SEND|")) {
                // Patron 1: Send & Forget
                // Formato: QUEUE_SEND|<cola>|<contenido>
                String[] parts = line.split("\\|", 3);
                if (parts.length < 3 || parts[1].isBlank() || parts[2].isBlank()) {
                    session.send("ERROR|Formato: QUEUE_SEND|<cola>|<mensaje>"); continue;
                }
                Message msg = Message.of(parts[2]);
                broker.getQueueManager().enqueue(parts[1], msg);
                session.send("OK|[QUEUE] Encolado en '" + parts[1] + "' | ID=" + msg.id().substring(0,8) + "...");

            } else if (line.startsWith("TOPIC_PUBLISH|")) {
                // Patron 3: Pub/Sub
                // Formato: TOPIC_PUBLISH|<topic>|<contenido>
                String[] parts = line.split("\\|", 3);
                if (parts.length < 3 || parts[1].isBlank() || parts[2].isBlank()) {
                    session.send("ERROR|Formato: TOPIC_PUBLISH|<topic>|<mensaje>"); continue;
                }
                Message msg = Message.of(parts[2]);
                broker.getTopicManager().publish(parts[1], msg);
                int count = broker.getTopicManager().getSubscriberCount(parts[1]);
                session.send("OK|[TOPIC] Publicado en '" + parts[1] + "' → " + count + " suscriptor(es)");

            } else if (line.startsWith("REQUEST|")) {
                // Patron 2: Request-Response
                // Formato: REQUEST|<correlationId>|<contenido>
                String[] parts = line.split("\\|", 3);
                if (parts.length < 3 || parts[1].isBlank() || parts[2].isBlank()) {
                    session.send("ERROR|Formato: REQUEST|<correlId>|<pregunta>"); continue;
                }
                Message msg = Message.of(parts[2]);
                broker.getRrManager().handleRequest(session, parts[1], msg);
                int handlers = broker.getRrManager().getHandlerCount();
                session.send("OK|[RR] Request enviada | correlId=" + parts[1].substring(0,8)
                    + "... | handlers=" + handlers);

            } else {
                session.send("ERROR|Comando desconocido: " + line);
                log.warning("[PRODUCER] Comando no reconocido de " + addr + ": " + line);
            }
        }
        log.info("PRODUCER desconectado: " + addr);
    }

    // ══════════════════════════════════════════════════════════════
    // HANDLER CONSUMER
    // ══════════════════════════════════════════════════════════════

    /**
     * Lee y procesa comandos del consumer.
     * El consumer puede suscribirse a colas, topics y registrarse como handler RR.
     * Para el patron Request-Response, tambien envia RESPONSE al broker.
     *
     * A diferencia del diseno original (consumer puramente pasivo),
     * ahora el consumer TAMBIEN envia mensajes al broker (RESPONSE).
     * El loop de lectura maneja ambos tipos de lineas.
     */
    private void handleConsumer(BufferedReader in, ClientSession session, String addr)
            throws IOException {

        session.send("OK|Conectado como CONSUMER. Comandos de suscripcion:");
        session.send("OK|  QUEUE_SUBSCRIBE|<cola>     (Send and Forget)");
        session.send("OK|  TOPIC_SUBSCRIBE|<topic>    (Pub/Sub)");
        session.send("OK|  RR_READY                   (Request-Response handler)");

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isBlank()) continue;

            if (line.startsWith("QUEUE_SUBSCRIBE|")) {
                // Suscribirse a una cola nombrada (Patron 1)
                String queueName = line.substring("QUEUE_SUBSCRIBE|".length()).trim();
                if (queueName.isBlank()) { session.send("ERROR|Nombre de cola no puede estar vacio"); continue; }
                broker.getQueueManager().subscribe(queueName, session);
                session.send("OK|[QUEUE] Suscrito a cola '" + queueName + "'. Esperando mensajes...");

            } else if (line.startsWith("TOPIC_SUBSCRIBE|")) {
                // Suscribirse a un topic (Patron 3)
                String topicName = line.substring("TOPIC_SUBSCRIBE|".length()).trim();
                if (topicName.isBlank()) { session.send("ERROR|Nombre de topic no puede estar vacio"); continue; }
                broker.getTopicManager().subscribe(topicName, session);
                session.send("OK|[TOPIC] Suscrito a topic '" + topicName + "'. Esperando publicaciones...");

            } else if ("RR_READY".equals(line)) {
                // Registrarse como handler de Request-Response (Patron 2)
                broker.getRrManager().registerConsumer(session);
                session.send("OK|[RR] Registrado como handler. Esperando REQUEST_MSG...");

            } else if (line.startsWith("RESPONSE|")) {
                // Enviar respuesta al producer que hizo un REQUEST (Patron 2)
                // Formato: RESPONSE|<correlId>|<contenido de la respuesta>
                String[] parts = line.split("\\|", 3);
                if (parts.length < 3) { session.send("ERROR|Formato: RESPONSE|<correlId>|<respuesta>"); continue; }
                broker.getRrManager().handleResponse(parts[1], parts[2]);
                // No enviamos ACK aqui; el producer ya recibio su RESPONSE_MSG

            } else {
                log.fine("[CONSUMER] Linea ignorada de " + addr + ": " + line);
            }
        }
        log.info("CONSUMER desconectado: " + addr);
    }
}
