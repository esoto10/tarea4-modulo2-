package broker.server;

import broker.network.ConnectionHandler;
import broker.routing.QueueManager;
import broker.routing.RequestResponseManager;
import broker.routing.TopicManager;
import broker.session.ClientSession;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 * BrokerServer â€” NÃºcleo del Message Broker multi-patrÃ³n.
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 *
 * Coordina los 3 patrones de mensajerÃ­a:
 *   1. Send & Forget   â†’ QueueManager
 *   2. Request-Response â†’ RequestResponseManager
 *   3. Pub/Sub          â†’ TopicManager
 *
 * Responsabilidades del servidor:
 *   - Aceptar conexiones TCP en el puerto 9090
 *   - Crear un Virtual Thread por cada cliente conectado
 *   - Delegar el protocolo a ConnectionHandler
 *   - Proveer acceso a los 3 managers de patrones
 */
public class BrokerServer {

    private static final Logger log = Logger.getLogger(BrokerServer.class.getName());
    static final int PORT = 9090;

    // â”€â”€ Los 3 managers de patrones de mensajerÃ­a â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    private final QueueManager            queueManager = new QueueManager();
    private final TopicManager            topicManager = new TopicManager();
    private final RequestResponseManager  rrManager    = new RequestResponseManager();

    /**
     * Registro de todas las sesiones activas.
     * Ãštil para monitoreo y limpieza global en caso de shutdown.
     */
    private final ConcurrentHashMap<String, ClientSession> activeSessions =
        new ConcurrentHashMap<>();

    /**
     * Un Virtual Thread por cada conexiÃ³n de cliente.
     * newVirtualThreadPerTaskExecutor() crea un nuevo VT en cada submit().
     */
    private final ExecutorService connectionPool =
        Executors.newVirtualThreadPerTaskExecutor();

    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    // INICIO DEL SERVIDOR
    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    public void start() {
        printBanner();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            log.info("Servidor TCP escuchando en puerto " + PORT);
            log.info("Patrones disponibles: Send&Forget | Request-Response | Pub/Sub\n");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(300_000); // 5 min timeout de inactividad
                connectionPool.submit(new ConnectionHandler(clientSocket, this));
                log.info("Conexion aceptada. Sesiones activas: " + activeSessions.size());
            }
        } catch (IOException e) {
            log.severe("Error fatal en el servidor: " + e.getMessage());
            System.exit(1);
        }
    }

    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    // GESTIÃ“N DE SESIONES
    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    public void addSession(ClientSession session) {
        activeSessions.put(session.getSessionId(), session);
    }

    public void removeSession(ClientSession session) {
        activeSessions.remove(session.getSessionId());
        // Limpieza en todos los managers
        queueManager.unsubscribeAll(session);
        topicManager.unsubscribeAll(session);
        rrManager.unregisterConsumer(session);
        log.info("Sesion cerrada: " + session
            + " | sesiones activas: " + activeSessions.size());
    }

    // â”€â”€ Getters de los managers (usados por ConnectionHandler) â”€â”€â”€â”€
    public QueueManager           getQueueManager() { return queueManager; }
    public TopicManager           getTopicManager() { return topicManager; }
    public RequestResponseManager getRrManager()    { return rrManager; }

    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    // UTILIDADES
    // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    private void printBanner() {
        System.out.println();
        System.out.println("â•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—");
        System.out.println("â•‘      MESSAGE BROKER MULTI-PATRON â€” Java 21           â•‘");
        System.out.println("â• â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•£");
        System.out.println("â•‘  Patron 1: Send & Forget  (QUEUE_SEND / QUEUE_MSG)   â•‘");
        System.out.println("â•‘  Patron 2: Request-Response (REQUEST / RESPONSE)     â•‘");
        System.out.println("â•‘  Patron 3: Pub/Sub Topic  (TOPIC_PUBLISH / TOPIC_MSG)â•‘");
        System.out.println("â• â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•£");
        System.out.println("â•‘  Puerto: 9090  |  Virtual Threads (Java 21)          â•‘");
        System.out.println("â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•");
        System.out.println();
    }

    private static void configureLogging() {
        Logger rootLogger = Logger.getLogger("");
        for (java.util.logging.Handler h : rootLogger.getHandlers()) {
            rootLogger.removeHandler(h);
        }
        System.setProperty(
            "java.util.logging.SimpleFormatter.format",
            "%1$tH:%1$tM:%1$tS.%1$tL [%4$-7s] %5$s%n"
        );
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        ch.setFormatter(new SimpleFormatter());
        rootLogger.addHandler(ch);
        rootLogger.setLevel(Level.INFO);
    }

    public static void main(String[] args) {
        configureLogging();
        new BrokerServer().start();
    }
}


/**
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 * BrokerServer â€” El nÃºcleo del Message Broker.
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 *
 * Responsabilidades:
 *   1. Escuchar conexiones TCP entrantes (productores y consumidores)
 *   2. Mantener la cola de mensajes en memoria (BlockingQueue)
 *   3. Mantener el registro de consumidores activos (ConcurrentHashMap)
 *   4. Despachar mensajes de la cola a todos los consumidores conectados
 *
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 * DECISIONES DE DISEÃ‘O Y CONCURRENCIA
 * â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
 *
 * â”€â”€ Virtual Threads (Java 21 / Project Loom) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
 *
 * PROBLEMA que resuelven:
 *   Un servidor TCP tradicional tiene dos opciones para manejar concurrencia:
 *
 *   a) Thread por conexiÃ³n (un OS thread por cliente):
 *      - Simple de programar, cÃ³digo secuencial fÃ¡cil de entender
 *      - PERO: cada OS thread ocupa ~1MB de stack + overhead del OS scheduler
 *      - Con 5000 conexiones â†’ 5GB solo en stacks de threads
 *      - El OS tiene un lÃ­mite de threads (tÃ­picamente ~10,000)
 *
 *   b) Modelo reactivo/NIO (callbacks, CompletableFuture, Project Reactor):
 *      - Muy eficiente en CPU y memoria
 *      - PERO: cÃ³digo complejo, difÃ­cil de razonar, stack traces incomprensibles
 *      - "Callback hell" y composiciÃ³n de operaciones asÃ­ncronas
 *
 *   Virtual Threads dan lo mejor de ambos mundos:
 *   - CÃ³digo SÃNCRONO y simple (como opciÃ³n a)
 *   - Eficiencia comparable a NIO (como opciÃ³n b)
 *
 *   La JVM mantiene un pool pequeÃ±o de OS threads ("carrier threads").
 *   Cuando un virtual thread se bloquea en I/O, la JVM lo "desmonta" del
 *   carrier thread (guarda su estado en heap) y "monta" otro virtual thread
 *   que tenga trabajo disponible. Esto se llama "continuation".
 *
 *   Resultado: podemos tener 100,000 virtual threads con el mismo overhead
 *   que antes tenÃ­amos con 100 OS threads.
 *
 *   API en Java 21:
 *   - Thread.ofVirtual().start(runnable)  â†’ crear un virtual thread
 *   - Executors.newVirtualThreadPerTaskExecutor() â†’ ExecutorService que
 *     crea un nuevo virtual thread por cada tarea enviada
 *
 * â”€â”€ LinkedBlockingQueue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
 *
 * La cola central del broker. Implementa el patrÃ³n Producer-Consumer.
 *
 * Por quÃ© no ArrayList o LinkedList con synchronized?
 *   - LinkedBlockingQueue es thread-safe por diseÃ±o: usa locks internos
 *     optimizados para acceso concurrente
 *   - put() BLOQUEA si la cola estÃ¡ llena (backpressure natural)
 *   - take() BLOQUEA si la cola estÃ¡ vacÃ­a (el dispatcher "duerme" sin
 *     gastar CPU â€” no hay busy-wait)
 *   - Garantiza orden FIFO (First-In, First-Out)
 *   - La capacidad mÃ¡xima (QUEUE_CAPACITY) previene OutOfMemoryError
 *     si los productores son mÃ¡s rÃ¡pidos que los consumidores
 *
 *   Flujo:
 *     Productor â”€â”€put()â”€â”€â†’ [msg1, msg2, msg3] â”€â”€take()â”€â”€â†’ Dispatcher
 *
 * â”€â”€ ConcurrentHashMap â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
 *
 * Registro de consumidores conectados: consumerId â†’ PrintWriter.
 *
 * Por quÃ© no HashMap con synchronized(this)?
 *   - HashMap no es thread-safe; sin sincronizaciÃ³n puede corromperse
 *   - HashMap con synchronized bloquea TODO el mapa para cada operaciÃ³n
 *   - ConcurrentHashMap usa "segment locking" (o CAS en Java 8+):
 *     divide el mapa en segmentos independientes y solo bloquea el
 *     segmento afectado â†’ mucho mayor throughput concurrente
 *
 *   Operaciones concurrentes que manejamos:
 *   - Virtual thread A: registerConsumer() â†’ put() en el mapa
 *   - Virtual thread B: unregisterConsumer() â†’ remove() del mapa
 *   - Virtual thread C (dispatcher): iterar consumers â†’ entrySet()
 *   Todas estas operaciones son seguras con ConcurrentHashMap.
 *
 * â”€â”€ ExecutorService (newVirtualThreadPerTaskExecutor) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
 *
 * Por quÃ© usarlo en lugar de llamar Thread.ofVirtual().start() directamente?
 *   - Gestiona el ciclo de vida de los threads (shutdown ordenado)
 *   - Permite monitoreo y mÃ©tricas
 *   - Si en el futuro queremos limitar concurrencia, solo cambiamos el executor
 *   - Es la API recomendada por Java para gestionar trabajo concurrente
 */
