package broker.network;

import broker.model.Message;
import broker.server.BrokerServer;

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
 * ══════════════════════════════════════════════════════════════════
 * ConnectionHandler — Gestiona la conexión de UN cliente con el broker.
 * ══════════════════════════════════════════════════════════════════
 *
 * Cada instancia de esta clase maneja el ciclo de vida completo de
 * una conexión TCP: desde que el cliente se conecta hasta que se desconecta.
 *
 * Implementa Runnable para ser ejecutado por el ExecutorService basado
 * en Virtual Threads del BrokerServer. Cada cliente tiene su propio
 * virtual thread corriendo esta lógica de forma independiente.
 *
 * ──────────────────────────────────────────────────────────────────
 * Por qué Virtual Threads aquí?
 * ──────────────────────────────────────────────────────────────────
 * Este handler hace operaciones BLOQUEANTES de I/O:
 *   - in.readLine()  → bloquea esperando datos del cliente
 *   - out.println()  → bloquea escribiendo datos al cliente
 *
 * Con threads del OS (threads clásicos):
 *   - Cada thread ocupa ~1MB de stack en el OS
 *   - 1000 conexiones = ~1GB de memoria solo en stacks
 *   - El OS scheduler tiene un límite de threads manejables
 *
 * Con Virtual Threads (Java 21 / Project Loom):
 *   - El JVM gestiona miles de virtual threads sobre un pool pequeño de OS threads
 *   - Cuando un virtual thread se bloquea en I/O, el JVM "desmonta" el virtual thread
 *     del OS thread y monta otro que tiene trabajo que hacer (continuations)
 *   - 10,000 conexiones concurrentes con memoria y CPU mínimos
 *   - El código se escribe de forma SÍNCRONA (simple de leer) pero se ejecuta
 *     de forma ASÍNCRONA internamente — lo mejor de ambos mundos
 *
 * ──────────────────────────────────────────────────────────────────
 * Protocolo de comunicación (línea 1):
 * ──────────────────────────────────────────────────────────────────
 *   Cliente → Broker:  "PRODUCER"  (el cliente enviará mensajes)
 *   Cliente → Broker:  "CONSUMER"  (el cliente recibirá mensajes)
 *
 * Protocolo para PRODUCER:
 *   Cliente → Broker:  "PUBLISH|<contenido del mensaje>"
 *   Broker  → Cola:    Crea Message.of(contenido) y lo encola
 *
 * Protocolo para CONSUMER:
 *   Broker → Cliente:  "MSG|<id>|<timestamp>|<contenido>"
 *   (el broker dispatcher escribe directamente en el PrintWriter del consumer)
 */
public class ConnectionHandler implements Runnable {

    private static final Logger log = Logger.getLogger(ConnectionHandler.class.getName());

    /** El socket TCP de este cliente en particular */
    private final Socket socket;

    /** Referencia al broker para encolar mensajes y registrar consumers */
    private final BrokerServer broker;

    /**
     * @param socket el socket aceptado por ServerSocket.accept()
     * @param broker el servidor broker con la cola y el registro de consumers
     */
    public ConnectionHandler(Socket socket, BrokerServer broker) {
        this.socket = socket;
        this.broker = broker;
    }

    /**
     * Método principal del handler. Se ejecuta en un Virtual Thread.
     *
     * Flujo:
     *   1. Abrir streams del socket (BufferedReader de entrada, PrintWriter de salida)
     *   2. Leer la primera línea → determinar rol (PRODUCER o CONSUMER)
     *   3. Delegar al handler específico del rol
     *   4. Al terminar (desconexión), el try-with-resources cierra el socket
     */
    @Override
    public void run() {
        // Dirección del cliente para incluir en logs
        String clientAddr = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();

        log.info("Nueva conexión TCP desde: " + clientAddr
            + " | Thread: " + Thread.currentThread().getName()
            + " | Virtual: " + Thread.currentThread().isVirtual());

        /*
         * try-with-resources garantiza que el socket y sus streams se cierren
         * al salir del bloque, incluso si ocurre una excepción.
         *
         * Nota: incluimos 'socket' directamente en el try — en Java 9+ los sockets
         * implementan AutoCloseable.
         *
         * BufferedReader/PrintWriter con buffer: reduce las llamadas al OS al
         * agrupar bytes en bloques, mucho más eficiente que escribir byte a byte.
         */
        try (
            socket; // cierra el socket al terminar
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"));
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")),
                true // autoFlush: cada println() envía los datos inmediatamente
            )
        ) {
            // ── Paso 1: Leer el rol del cliente (primera línea) ──────────────
            String role = in.readLine();

            if (role == null) {
                // El cliente se conectó y cerró sin enviar nada
                log.warning("Cliente desconectado antes de identificarse: " + clientAddr);
                return;
            }

            // Eliminar caracteres de control y de formato (ej: BOM U+FEFF de algunos clientes),
            // espacios y convertir a mayúsculas para comparación robusta.
            // \p{Cc} = control chars, \p{Cf} = format chars (incluye BOM)
            role = role.replaceAll("[\\p{Cc}\\p{Cf}]", "").trim().toUpperCase();
            log.info("Cliente " + clientAddr + " se identificó como: [" + role + "]");

            // ── Paso 2: Enrutar según el rol ─────────────────────────────────
            switch (role) {
                case "PRODUCER" -> handleProducer(in, out, clientAddr);
                case "CONSUMER" -> handleConsumer(in, out, clientAddr);
                default -> {
                    // Protocolo desconocido — informar al cliente y cerrar
                    out.println("ERROR|Rol desconocido '" + role + "'. Usa: PRODUCER o CONSUMER");
                    log.warning("Rol no reconocido de " + clientAddr + ": [" + role + "]");
                }
            }

        } catch (IOException e) {
            // IOException puede ocurrir si el cliente cierra la conexión abruptamente
            // Es normal en TCP y no se considera un error grave del broker
            log.info("Conexión cerrada con " + clientAddr + ": " + e.getMessage());
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // MANEJO DE PRODUCER
    // ══════════════════════════════════════════════════════════════════

    /**
     * Procesa los mensajes de un cliente PRODUCER.
     *
     * Lee continuamente del socket. Cada línea debe tener el formato:
     *   PUBLISH|<contenido del mensaje>
     *
     * El loop termina cuando el cliente se desconecta (readLine() devuelve null)
     * o cuando ocurre un error de I/O.
     *
     * @param in        stream de entrada del socket del productor
     * @param out       stream de salida (para enviar confirmaciones o errores)
     * @param clientAddr dirección del cliente (solo para logs)
     */
    private void handleProducer(BufferedReader in, PrintWriter out, String clientAddr)
            throws IOException {

        log.info("PRODUCER activo — esperando comandos PUBLISH de: " + clientAddr);

        // Confirmación al cliente de que el broker lo reconoció
        out.println("OK|Conectado como PRODUCER. Envía: PUBLISH|<mensaje>");

        String line;

        // Leer línea a línea hasta EOF (cliente desconectado)
        while ((line = in.readLine()) != null) {
            line = line.trim();

            if (line.isBlank()) {
                continue; // ignorar líneas vacías
            }

            // Verificar que el comando sigue el protocolo
            if (!line.startsWith("PUBLISH|")) {
                log.warning("Comando inválido de " + clientAddr + ": [" + line + "]");
                out.println("ERROR|Comando inválido. Formato esperado: PUBLISH|<contenido>");
                continue;
            }

            // Extraer el contenido real del mensaje
            // substring es safe aquí porque ya verificamos startsWith("PUBLISH|")
            String content = line.substring("PUBLISH|".length());

            if (content.isBlank()) {
                log.warning("Mensaje vacío ignorado de: " + clientAddr);
                out.println("ERROR|El contenido del mensaje no puede estar vacío");
                continue;
            }

            // ── Crear el mensaje y encolarlo en el broker ─────────────────
            Message message = Message.of(content);
            broker.enqueue(message);

            // Confirmar al productor que el mensaje fue recibido
            out.println("OK|Mensaje encolado con ID: " + message.id());

            log.info("PUBLISH recibido de " + clientAddr
                + " → ID=" + message.id()
                + " | Contenido='" + content + "'");
        }

        log.info("PRODUCER desconectado: " + clientAddr);
    }

    // ══════════════════════════════════════════════════════════════════
    // MANEJO DE CONSUMER
    // ══════════════════════════════════════════════════════════════════

    /**
     * Registra un cliente CONSUMER y mantiene su conexión activa.
     *
     * ¿Cómo funciona la entrega de mensajes al consumer?
     *
     *   El DISPATCHER del broker (un virtual thread separado) lee de la
     *   BlockingQueue y escribe directamente en el PrintWriter de cada consumer.
     *
     *   Este método simplemente:
     *     1. Registra el PrintWriter del consumer en el broker
     *     2. Bloquea leyendo del socket (para detectar desconexión)
     *     3. Al detectar EOF → desregistra al consumer
     *
     * El "bloqueo" aquí es eficiente porque estamos en un Virtual Thread —
     * el JVM cede el OS thread a otro virtual thread mientras esperamos datos.
     *
     * @param in        stream de entrada (para detectar desconexión)
     * @param out       stream de salida (el dispatcher escribe mensajes aquí)
     * @param clientAddr dirección del cliente (solo para logs)
     */
    private void handleConsumer(BufferedReader in, PrintWriter out, String clientAddr)
            throws IOException {

        // Generar un ID único para este consumer — usado en logs y en el ConcurrentHashMap
        String consumerId = UUID.randomUUID().toString();

        log.info("CONSUMER registrándose — ID: " + consumerId + " | Desde: " + clientAddr);

        // Confirmación al cliente
        out.println("OK|Conectado como CONSUMER. Esperando mensajes del broker...");

        // ── Registrar en el broker ─────────────────────────────────────────
        // A partir de este punto, el DISPATCHER enviará mensajes a través de 'out'
        broker.registerConsumer(consumerId, out);

        // ── Mantener la conexión viva ──────────────────────────────────────
        // Necesitamos detectar cuándo el consumer se desconecta.
        // La forma más limpia: leer del socket en un loop.
        // Cuando el consumer cierre la conexión → readLine() devuelve null.
        //
        // Los consumers no envían datos en este protocolo simple,
        // pero si en el futuro se agrega un comando UNSUBSCRIBE, iría aquí.
        try {
            String line;
            while ((line = in.readLine()) != null) {
                // En esta POC el consumer no envía comandos.
                // Ignoramos cualquier dato inesperado.
                log.fine("Dato inesperado del consumer " + consumerId + ": [" + line + "]");
            }
        } finally {
            // ── Limpiar al desconectarse ───────────────────────────────────
            // finally garantiza que removemos al consumer incluso si hay excepción
            broker.unregisterConsumer(consumerId);
            log.info("CONSUMER desconectado — ID: " + consumerId + " | Desde: " + clientAddr);
        }
    }
}
