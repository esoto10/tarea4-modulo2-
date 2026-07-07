package broker.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;
import java.util.UUID;

/**
 * ProducerClient — Cliente productor con soporte para 3 patrones.
 *
 * Al conectar, muestra un menu para elegir el patron de mensajeria:
 *
 *   Patron 1 — Send and Forget (Cola):
 *     Envia mensajes a una cola nombrada. El broker los entrega a
 *     UN consumer en round-robin. El producer no espera respuesta.
 *     Comando enviado: QUEUE_SEND|<cola>|<mensaje>
 *
 *   Patron 2 — Request-Response:
 *     Envia una solicitud y ESPERA la respuesta del consumer.
 *     El correlationId (UUID) vincula la solicitud con su respuesta.
 *     Comando enviado: REQUEST|<correlId>|<pregunta>
 *     Respuesta recibida: RESPONSE_MSG|<correlId>|<respuesta>
 *
 *   Patron 3 — Publicador/Suscriptor (Topic):
 *     Publica mensajes en un topic. TODOS los consumers suscritos
 *     al topic reciben el mensaje (fan-out / broadcast).
 *     Comando enviado: TOPIC_PUBLISH|<topic>|<mensaje>
 */
public class ProducerClient {

    private static final String HOST = "localhost";
    private static final int    PORT = 9090;

    // Ultimo correlationId enviado — el receiver thread lo usa para mostrar la respuesta
    private static volatile String lastCorrelId = null;

    public static void main(String[] args) {
        printBanner();
        System.out.println("Conectando a broker " + HOST + ":" + PORT + "...\n");

        try (
            Socket socket = new Socket(HOST, PORT);
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")), true);
            BufferedReader serverIn = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"));
            Scanner kb = new Scanner(System.in)
        ) {
            System.out.println("Conectado!\n");

            // Identificarse como PRODUCER
            out.println("PRODUCER");

            // Leer los mensajes OK de bienvenida del broker
            for (int i = 0; i < 4; i++) {
                String line = serverIn.readLine();
                if (line != null) System.out.println("[BROKER] " + (line.startsWith("OK|") ? line.substring(3) : line));
            }

            // Hilo receptor: muestra ACKs y RESPONSE_MSG en tiempo real
            Thread receiver = Thread.ofVirtual().name("producer-receiver").start(() -> {
                try {
                    String line;
                    while ((line = serverIn.readLine()) != null) {
                        if (line.startsWith("OK|")) {
                            System.out.println("\n[ACK] " + line.substring(3));
                        } else if (line.startsWith("RESPONSE_MSG|")) {
                            // RESPONSE_MSG|<correlId>|<respuesta>
                            String[] parts = line.split("\\|", 3);
                            System.out.println("\n[RESPUESTA RECIBIDA]");
                            System.out.println("  CorrelId : " + (parts.length > 1 ? parts[1].substring(0,8) + "..." : "?"));
                            System.out.println("  Respuesta: " + (parts.length > 2 ? parts[2] : ""));
                            System.out.println();
                        } else if (line.startsWith("ERROR|")) {
                            System.err.println("\n[ERROR BROKER] " + line.substring(6));
                        }
                    }
                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted())
                        System.err.println("\n[BROKER] Conexion cerrada.");
                }
            });

            // Menu de seleccion de patron
            System.out.println("\nSelecciona el patron de mensajeria:");
            System.out.println("  1 - Send and Forget  (cola nombrada)");
            System.out.println("  2 - Request-Response (espera respuesta)");
            System.out.println("  3 - Pub/Sub Topic    (broadcast a suscriptores)");
            System.out.print("\nPatron [1/2/3]: ");

            String choice = kb.hasNextLine() ? kb.nextLine().trim() : "1";

            switch (choice) {
                case "1" -> runSendAndForget(out, kb);
                case "2" -> runRequestResponse(out, kb);
                case "3" -> runPubSub(out, kb);
                default  -> { System.out.println("Opcion invalida. Usando patron 1."); runSendAndForget(out, kb); }
            }

            receiver.interrupt();
            System.out.println("\nProducer desconectado. Hasta luego!");

        } catch (IOException e) {
            System.err.println("[ERROR] No se pudo conectar: " + e.getMessage());
            System.err.println("Verifica que el broker este corriendo: cd broker-server && mvn exec:java");
        }
    }

    // ──────────────────────────────────────────────────────────────
    // PATRON 1: Send and Forget
    // ──────────────────────────────────────────────────────────────

    private static void runSendAndForget(PrintWriter out, Scanner kb) {
        System.out.print("\nNombre de la cola (ej: pedidos): ");
        String queue = kb.hasNextLine() ? kb.nextLine().trim() : "default";
        if (queue.isBlank()) queue = "default";

        System.out.println("\n[Send and Forget] Cola: '" + queue + "'");
        System.out.println("Escribe mensajes y presiona Enter. 'exit' para salir.\n");

        while (kb.hasNextLine()) {
            System.out.print("Mensaje: ");
            String input = kb.nextLine().trim();
            if ("exit".equalsIgnoreCase(input)) break;
            if (input.isBlank()) { System.out.println("[!] Mensaje vacio ignorado."); continue; }
            out.println("QUEUE_SEND|" + queue + "|" + input);
        }
    }

    // ──────────────────────────────────────────────────────────────
    // PATRON 2: Request-Response
    // ──────────────────────────────────────────────────────────────

    private static void runRequestResponse(PrintWriter out, Scanner kb) {
        System.out.println("\n[Request-Response] El broker enrutara tu solicitud al handler mas cercano.");
        System.out.println("Escribe tu solicitud y presiona Enter. 'exit' para salir.\n");

        while (kb.hasNextLine()) {
            System.out.print("Solicitud: ");
            String input = kb.nextLine().trim();
            if ("exit".equalsIgnoreCase(input)) break;
            if (input.isBlank()) { System.out.println("[!] Solicitud vacia ignorada."); continue; }

            // Generar correlationId unico para correlacionar esta request con su response
            String correlId = UUID.randomUUID().toString();
            lastCorrelId = correlId;

            out.println("REQUEST|" + correlId + "|" + input);
            System.out.println("→ Request enviada | correlId=" + correlId.substring(0,8) + "...");
            System.out.println("  Esperando respuesta del handler...");
        }
    }

    // ──────────────────────────────────────────────────────────────
    // PATRON 3: Pub/Sub
    // ──────────────────────────────────────────────────────────────

    private static void runPubSub(PrintWriter out, Scanner kb) {
        System.out.print("\nNombre del topic (ej: noticias): ");
        String topic = kb.hasNextLine() ? kb.nextLine().trim() : "general";
        if (topic.isBlank()) topic = "general";

        System.out.println("\n[Pub/Sub] Topic: '" + topic + "'");
        System.out.println("Cada mensaje se entregara a TODOS los suscriptores del topic.");
        System.out.println("Escribe mensajes y presiona Enter. 'exit' para salir.\n");

        while (kb.hasNextLine()) {
            System.out.print("Publicar: ");
            String input = kb.nextLine().trim();
            if ("exit".equalsIgnoreCase(input)) break;
            if (input.isBlank()) { System.out.println("[!] Mensaje vacio ignorado."); continue; }
            out.println("TOPIC_PUBLISH|" + topic + "|" + input);
        }
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║     MESSAGE BROKER — PRODUCER (3 patrones)      ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  1. Send and Forget  → QUEUE_SEND                ║");
        System.out.println("║  2. Request-Response → REQUEST / RESPONSE_MSG    ║");
        System.out.println("║  3. Pub/Sub Topic    → TOPIC_PUBLISH             ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }
}
