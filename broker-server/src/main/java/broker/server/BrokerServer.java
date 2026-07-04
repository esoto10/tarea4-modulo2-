package broker.server;

import broker.model.Message;
import broker.network.ConnectionHandler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * ══════════════════════════════════════════════════════════════════
 * BrokerServer — El núcleo del Message Broker.
 * ══════════════════════════════════════════════════════════════════
 *
 * Responsabilidades:
 *   1. Escuchar conexiones TCP entrantes (productores y consumidores)
 *   2. Mantener la cola de mensajes en memoria (BlockingQueue)
 *   3. Mantener el registro de consumidores activos (ConcurrentHashMap)
 *   4. Despachar mensajes de la cola a todos los consumidores conectados
 *
 * ══════════════════════════════════════════════════════════════════
 * DECISIONES DE DISEÑO Y CONCURRENCIA
 * ══════════════════════════════════════════════════════════════════
 *
 * ── Virtual Threads (Java 21 / Project Loom) ──────────────────────
 *
 * PROBLEMA que resuelven:
 *   Un servidor TCP tradicional tiene dos opciones para manejar concurrencia:
 *
 *   a) Thread por conexión (un OS thread por cliente):
 *      - Simple de programar, código secuencial fácil de entender
 *      - PERO: cada OS thread ocupa ~1MB de stack + overhead del OS scheduler
 *      - Con 5000 conexiones → 5GB solo en stacks de threads
 *      - El OS tiene un límite de threads (típicamente ~10,000)
 *
 *   b) Modelo reactivo/NIO (callbacks, CompletableFuture, Project Reactor):
 *      - Muy eficiente en CPU y memoria
 *      - PERO: código complejo, difícil de razonar, stack traces incomprensibles
 *      - "Callback hell" y composición de operaciones asíncronas
 *
 *   Virtual Threads dan lo mejor de ambos mundos:
 *   - Código SÍNCRONO y simple (como opción a)
 *   - Eficiencia comparable a NIO (como opción b)
 *
 *   La JVM mantiene un pool pequeño de OS threads ("carrier threads").
 *   Cuando un virtual thread se bloquea en I/O, la JVM lo "desmonta" del
 *   carrier thread (guarda su estado en heap) y "monta" otro virtual thread
 *   que tenga trabajo disponible. Esto se llama "continuation".
 *
 *   Resultado: podemos tener 100,000 virtual threads con el mismo overhead
 *   que antes teníamos con 100 OS threads.
 *
 *   API en Java 21:
 *   - Thread.ofVirtual().start(runnable)  → crear un virtual thread
 *   - Executors.newVirtualThreadPerTaskExecutor() → ExecutorService que
 *     crea un nuevo virtual thread por cada tarea enviada
 *
 * ── LinkedBlockingQueue ───────────────────────────────────────────
 *
 * La cola central del broker. Implementa el patrón Producer-Consumer.
 *
 * Por qué no ArrayList o LinkedList con synchronized?
 *   - LinkedBlockingQueue es thread-safe por diseño: usa locks internos
 *     optimizados para acceso concurrente
 *   - put() BLOQUEA si la cola está llena (backpressure natural)
 *   - take() BLOQUEA si la cola está vacía (el dispatcher "duerme" sin
 *     gastar CPU — no hay busy-wait)
 *   - Garantiza orden FIFO (First-In, First-Out)
 *   - La capacidad máxima (QUEUE_CAPACITY) previene OutOfMemoryError
 *     si los productores son más rápidos que los consumidores
 *
 *   Flujo:
 *     Productor ──put()──→ [msg1, msg2, msg3] ──take()──→ Dispatcher
 *
 * ── ConcurrentHashMap ────────────────────────────────────────────
 *
 * Registro de consumidores conectados: consumerId → PrintWriter.
 *
 * Por qué no HashMap con synchronized(this)?
 *   - HashMap no es thread-safe; sin sincronización puede corromperse
 *   - HashMap con synchronized bloquea TODO el mapa para cada operación
 *   - ConcurrentHashMap usa "segment locking" (o CAS en Java 8+):
 *     divide el mapa en segmentos independientes y solo bloquea el
 *     segmento afectado → mucho mayor throughput concurrente
 *
 *   Operaciones concurrentes que manejamos:
 *   - Virtual thread A: registerConsumer() → put() en el mapa
 *   - Virtual thread B: unregisterConsumer() → remove() del mapa
 *   - Virtual thread C (dispatcher): iterar consumers → entrySet()
 *   Todas estas operaciones son seguras con ConcurrentHashMap.
 *
 * ── ExecutorService (newVirtualThreadPerTaskExecutor) ─────────────
 *
 * Por qué usarlo en lugar de llamar Thread.ofVirtual().start() directamente?
 *   - Gestiona el ciclo de vida de los threads (shutdown ordenado)
 *   - Permite monitoreo y métricas
 *   - Si en el futuro queremos limitar concurrencia, solo cambiamos el executor
 *   - Es la API recomendada por Java para gestionar trabajo concurrente
 */
public class BrokerServer {

    private static final Logger log = Logger.getLogger(BrokerServer.class.getName());

    /** Puerto TCP donde el broker escucha conexiones */
    static final int PORT = 9090;

    /**
     * Capacidad máxima de la cola de mensajes.
     *
     * Implementa BACKPRESSURE: si la cola está llena (1000 mensajes),
     * el método put() bloqueará al productor hasta que el dispatcher
     * consuma algún mensaje. Esto evita OutOfMemoryError bajo carga alta
     * y comunica presión al productor en lugar de perder datos silenciosamente.
     */
    private static final int QUEUE_CAPACITY = 1_000;

    // ══════════════════════════════════════════════════════════════════
    // ESTADO COMPARTIDO DEL BROKER
    // Estas estructuras son accedidas concurrentemente por múltiples
    // virtual threads — por eso son thread-safe por diseño.
    // ══════════════════════════════════════════════════════════════════

    /**
     * Cola central de mensajes pendientes de entrega.
     *
     * Productores escriben aquí (put), el dispatcher lee aquí (take).
     * Semántica FIFO garantizada. Thread-safe.
     */
    private final BlockingQueue<Message> messageQueue =
        new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /**
     * Registro de consumidores activos.
     *
     * Key:   consumerId (String UUID) — identifica la conexión
     * Value: PrintWriter del socket TCP del consumer — para enviar mensajes
     *
     * El dispatcher itera este mapa para entregar cada mensaje a todos
     * los consumers. Thread-safe para operaciones concurrentes de
     * registro/desregistro mientras el dispatcher itera.
     */
    private final Map<String, PrintWriter> consumers = new ConcurrentHashMap<>();

    /**
     * ExecutorService basado en Virtual Threads.
     *
     * Cada llamada a submit(Runnable) crea un nuevo Virtual Thread.
     * Adecuado para este caso: un virtual thread por conexión de cliente.
     *
     * newVirtualThreadPerTaskExecutor() es la factory de Java 21 para esto.
     */
    private final ExecutorService connectionExecutor =
        Executors.newVirtualThreadPerTaskExecutor();

    // ══════════════════════════════════════════════════════════════════
    // INICIO DEL SERVIDOR
    // ══════════════════════════════════════════════════════════════════

    /**
     * Inicia el broker: arranca el dispatcher y comienza a aceptar conexiones TCP.
     *
     * Este método no retorna (loop infinito de accept).
     * En producción se agregaría manejo de shutdown con ShutdownHook.
     */
    public void start() {
        printBanner();

        // ── Iniciar el Dispatcher en su propio Virtual Thread ─────────────
        // El dispatcher corre de forma independiente al accept loop.
        // Le damos un nombre descriptivo para identificarlo en logs/profilers.
        Thread.ofVirtual()
            .name("broker-dispatcher")
            .start(this::dispatchLoop);

        log.info("Dispatcher iniciado en virtual thread 'broker-dispatcher'");

        // ── Iniciar el servidor TCP ────────────────────────────────────────
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            // SO_REUSEADDR: permite reiniciar el broker rápidamente sin esperar
            // que el OS libere el puerto del proceso anterior
            serverSocket.setReuseAddress(true);

            log.info("Servidor TCP escuchando en puerto " + PORT);
            log.info("Listo para aceptar conexiones de productores y consumidores.\n");

            // ── Loop principal: aceptar conexiones indefinidamente ─────────
            while (true) {
                /*
                 * accept() BLOQUEA el thread hasta que llega una nueva conexión.
                 * Dado que estamos en el main thread (no en un virtual thread),
                 * este bloqueo es aceptable — es el loop principal del servidor.
                 *
                 * Cada vez que llega una conexión, creamos un ConnectionHandler
                 * y lo enviamos al ExecutorService → nuevo virtual thread.
                 */
                Socket clientSocket = serverSocket.accept();

                // Configurar timeout para detectar conexiones muertas (heartbeat implícito)
                // Si el cliente no envía nada en 5 minutos, se considera desconectado
                clientSocket.setSoTimeout(300_000); // 5 minutos en ms

                // Delegar el manejo de este cliente a un Virtual Thread
                connectionExecutor.submit(new ConnectionHandler(clientSocket, this));

                log.info("Conexión aceptada. Consumers activos: " + consumers.size());
            }

        } catch (IOException e) {
            log.severe("Error fatal iniciando el servidor en puerto " + PORT + ": " + e.getMessage());
            log.severe("¿Está el puerto " + PORT + " ocupado por otro proceso?");
            System.exit(1);
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // API PÚBLICA DEL BROKER (llamada por ConnectionHandler)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Encola un mensaje para entrega a los consumers.
     *
     * Llamado por ConnectionHandler cuando un PRODUCER envía PUBLISH.
     *
     * put() es BLOQUEANTE si la cola está llena (backpressure).
     * El productor espera hasta que el dispatcher libere espacio en la cola.
     * Esto previene que un productor muy rápido sature la memoria del broker.
     *
     * @param message el mensaje a encolar (inmutable, creado por Message.of())
     */
    public void enqueue(Message message) {
        try {
            messageQueue.put(message); // bloquea si cola llena

            log.info("ENQUEUE → ID=" + message.id()
                + " | Contenido='" + message.content() + "'"
                + " | Cola: " + messageQueue.size() + "/" + QUEUE_CAPACITY);

        } catch (InterruptedException e) {
            // InterruptedException significa que el thread fue interrumpido
            // mientras esperaba. Debemos restaurar el flag de interrupción.
            Thread.currentThread().interrupt();
            log.warning("enqueue() interrumpido para mensaje: " + message.id());
        }
    }

    /**
     * Registra un consumer recién conectado.
     *
     * Llamado por ConnectionHandler cuando identifica un cliente CONSUMER.
     * A partir de este momento, el dispatcher enviará mensajes a este consumer.
     *
     * @param consumerId ID único del consumer (UUID generado en ConnectionHandler)
     * @param writer     PrintWriter conectado al socket TCP del consumer
     */
    public void registerConsumer(String consumerId, PrintWriter writer) {
        consumers.put(consumerId, writer);
        log.info("CONSUMER registrado → ID=" + consumerId
            + " | Total consumers: " + consumers.size());
    }

    /**
     * Desregistra un consumer que se desconectó.
     *
     * Llamado por ConnectionHandler en el bloque finally cuando detecta
     * que el socket del consumer se cerró (readLine() devolvió null).
     *
     * @param consumerId el ID del consumer a remover
     */
    public void unregisterConsumer(String consumerId) {
        consumers.remove(consumerId);
        log.info("CONSUMER removido → ID=" + consumerId
            + " | Total consumers: " + consumers.size());
    }

    // ══════════════════════════════════════════════════════════════════
    // DISPATCHER: corazón de la distribución de mensajes
    // ══════════════════════════════════════════════════════════════════

    /**
     * Loop del Dispatcher — corre en su propio Virtual Thread de forma perpetua.
     *
     * El dispatcher es el componente que:
     *   1. Lee mensajes de la BlockingQueue (bloqueando si está vacía)
     *   2. Si no hay consumers, espera hasta que uno se conecte
     *   3. Serializa el mensaje una vez
     *   4. Envía el mensaje a TODOS los consumers conectados
     *
     * ── Por qué un único dispatcher? ──────────────────────────────────
     * Para esta POC, un único dispatcher simplifica el diseño.
     * La BlockingQueue.take() es thread-safe, pero si hubiera múltiples
     * dispatchers, cada mensaje solo llegaría a UNO de ellos (no a todos
     * los consumers). Para broadcast real con múltiples dispatchers se
     * necesitaría una arquitectura diferente (pub/sub por topic, etc.).
     *
     * ── Flujo de take() con Virtual Threads ───────────────────────────
     * take() bloquea cuando la cola está vacía.
     * Como el dispatcher corre en un Virtual Thread, el bloqueo no
     * desperdicia un OS thread — la JVM desmonta el virtual thread del
     * carrier thread hasta que haya un mensaje disponible (LockSupport.park).
     */
    private void dispatchLoop() {
        log.info("Dispatcher activo — esperando mensajes en la BlockingQueue...");

        while (true) {
            try {
                // ── take(): bloquea hasta que haya un mensaje en la cola ───
                // Este es el punto donde el dispatcher "duerme" eficientemente
                // cuando no hay mensajes (sin busy-wait, sin sleep())
                Message message = messageQueue.take();

                log.info("DISPATCH → Procesando mensaje ID=" + message.id()
                    + " | Consumers activos: " + consumers.size());

                // ── Esperar si no hay consumers conectados ─────────────────
                // Retenemos el mensaje (en la variable local 'message') hasta
                // que al menos un consumer esté disponible.
                // Esto garantiza que no se pierda el mensaje.
                while (consumers.isEmpty()) {
                    log.warning("DISPATCH → Sin consumers. Mensaje ID="
                        + message.id() + " esperando entrega...");
                    Thread.sleep(500); // virtual thread duerme sin bloquear OS thread
                }

                // ── Serializar una sola vez para todos los consumers ───────
                // Evitamos serializar N veces para N consumers
                String serialized = message.serialize();

                // ── Entregar a todos los consumers ─────────────────────────
                int delivered = 0;
                int failed = 0;

                for (Map.Entry<String, PrintWriter> entry : consumers.entrySet()) {
                    String consumerId = entry.getKey();
                    PrintWriter writer = entry.getValue();

                    try {
                        // println() envía la línea y hace flush (autoFlush=true en el writer)
                        writer.println(serialized);

                        /*
                         * checkError() devuelve true si el writer tuvo algún error
                         * (por ejemplo, el socket del consumer ya se cerró pero aún
                         * no fue desregistrado por su ConnectionHandler).
                         *
                         * En ese caso, el consumer se removerá cuando su
                         * ConnectionHandler detecte el EOF y llame a unregisterConsumer().
                         * No es necesario removerlo aquí.
                         */
                        if (writer.checkError()) {
                            log.warning("DISPATCH → Error al escribir a consumer: " + consumerId);
                            failed++;
                        } else {
                            log.fine("DISPATCH → Entregado a consumer: " + consumerId);
                            delivered++;
                        }

                    } catch (Exception e) {
                        log.warning("DISPATCH → Excepción enviando a consumer "
                            + consumerId + ": " + e.getMessage());
                        failed++;
                    }
                }

                log.info("DISPATCH → Mensaje ID=" + message.id()
                    + " entregado a " + delivered + " consumer(s)"
                    + (failed > 0 ? " | " + failed + " fallo(s)" : ""));

            } catch (InterruptedException e) {
                // El dispatcher fue interrumpido (shutdown del servidor)
                Thread.currentThread().interrupt();
                log.warning("Dispatcher interrumpido — deteniéndose...");
                break;
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // UTILIDADES
    // ══════════════════════════════════════════════════════════════════

    /** Imprime el banner de inicio con información de configuración */
    private void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║         MESSAGE BROKER — Java 21 POC             ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  Implementación desde cero sin frameworks        ║");
        System.out.println("║  Tecnologías:                                    ║");
        System.out.println("║    ✔ Virtual Threads (Project Loom)              ║");
        System.out.println("║    ✔ LinkedBlockingQueue (cola de mensajes)      ║");
        System.out.println("║    ✔ ConcurrentHashMap (registro consumers)      ║");
        System.out.println("║    ✔ ServerSocket / Socket (TCP puro)            ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  Puerto: " + PORT + "                                       ║");
        System.out.println("║  Cola:   " + QUEUE_CAPACITY + " mensajes máximo                   ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * Configura el sistema de logging de la JVM para salida más legible.
     *
     * Por defecto, java.util.logging muestra líneas muy largas con información
     * redundante. Este método configura un formato conciso con solo la información
     * relevante: hora, nivel y mensaje.
     */
    private static void configureLogging() {
        // Remover el handler por defecto (que imprime en stderr con formato largo)
        Logger rootLogger = Logger.getLogger("");
        java.util.logging.Handler[] handlers = rootLogger.getHandlers();
        for (java.util.logging.Handler handler : handlers) {
            rootLogger.removeHandler(handler);
        }

        // Agregar un ConsoleHandler con formato personalizado
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);

        // Formato: HH:mm:ss.SSS [NIVEL  ] [Clase método] Mensaje
        System.setProperty(
            "java.util.logging.SimpleFormatter.format",
            "%1$tH:%1$tM:%1$tS.%1$tL [%4$-7s] [%2$s] %5$s%n"
        );
        consoleHandler.setFormatter(new SimpleFormatter());

        rootLogger.addHandler(consoleHandler);
        rootLogger.setLevel(Level.INFO);
    }

    // ══════════════════════════════════════════════════════════════════
    // PUNTO DE ENTRADA
    // ══════════════════════════════════════════════════════════════════

    /**
     * Punto de entrada de la aplicación del broker.
     *
     * Configura el logging y arranca el servidor.
     * El servidor corre indefinidamente (Ctrl+C para detener).
     */
    public static void main(String[] args) {
        configureLogging();
        new BrokerServer().start();
    }
}
