package broker.consumer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

/**
 * ConsumerClient — Cliente consumidor con soporte para 3 patrones.
 *
 * Al conectar muestra un menu para elegir:
 *   1 - Send and Forget  : suscribirse a una cola (un mensaje por consumer, round-robin)
 *   2 - Request-Response : handler que recibe solicitudes y responde automaticamente
 *   3 - Pub/Sub Topic    : suscribirse a topics, recibe broadcast de publishers
 */
public class ConsumerClient {

    private static final String HOST = "localhost";
    private static final int    PORT = 9090;

    private static final DateTimeFormatter FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static int msgCount = 0;

    public static void main(String[] args) {
        printBanner();
        System.out.println("Conectando a broker " + HOST + ":" + PORT + "...\n");

        try (
            Socket socket = new Socket(HOST, PORT);
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")), true);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"));
            Scanner kb = new Scanner(System.in)
        ) {
            System.out.println("Conectado!\n");
            out.println("CONSUMER");

            // Leer las 4 lineas de bienvenida del broker
            for (int i = 0; i < 4; i++) {
                String line = in.readLine();
                if (line != null) System.out.println("[BROKER] " + (line.startsWith("OK|") ? line.substring(3) : line));
            }

            System.out.println("\nSelecciona el patron de consumo:");
            System.out.println("  1 - Send and Forget  (recibir de una cola)");
            System.out.println("  2 - Request-Response (handler: recibe y responde)");
            System.out.println("  3 - Pub/Sub Topic    (suscriptor de topics)");
            System.out.print("\nPatron [1/2/3]: ");
            System.out.flush();

            String choice = kb.hasNextLine() ? kb.nextLine().trim() : "1";

            switch (choice) {
                case "1" -> subscribeQueue(out, in, kb);
                case "2" -> subscribeRR(out, in);
                case "3" -> subscribeTopic(out, in, kb);
                default  -> { System.out.println("Opcion invalida. Usando patron 1."); subscribeQueue(out, in, kb); }
            }

            System.out.println("\nTotal mensajes recibidos: " + msgCount);
            System.out.println("Consumer desconectado.");

        } catch (IOException e) {
            System.err.println("[ERROR] No se pudo conectar: " + e.getMessage());
            System.err.println("Verifica que el broker este corriendo: cd broker-server && mvn exec:java");
        }
    }

    // --- PATRON 1: Send and Forget ---

    private static void subscribeQueue(PrintWriter out, BufferedReader in, Scanner kb)
            throws IOException {
        System.out.print("\nNombre de la cola (ej: pedidos): ");
        System.out.flush();
        String queue = kb.hasNextLine() ? kb.nextLine().trim() : "default";
        if (queue.isBlank()) queue = "default";

        out.println("QUEUE_SUBSCRIBE|" + queue);
        String ack = in.readLine();
        System.out.println("[BROKER] " + (ack != null && ack.startsWith("OK|") ? ack.substring(3) : ack));
        System.out.println("\n[Send and Forget] Escuchando cola: " + queue);
        System.out.println("Presiona Ctrl+C para salir.\n");

        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("QUEUE_MSG|")) {
                String[] p = line.split("\\|", 5);
                printQueueMsg(p.length>1?p[1]:"?", p.length>2?p[2]:"?", p.length>3?p[3]:"?", p.length>4?p[4]:line);
            } else if (line.startsWith("OK|") || line.startsWith("ERROR|")) {
                System.out.println("[BROKER] " + (line.startsWith("OK|") ? line.substring(3) : line));
            }
        }
    }

    // --- PATRON 2: Request-Response (handler) ---

    private static void subscribeRR(PrintWriter out, BufferedReader in)
            throws IOException {
        out.println("RR_READY");
        String ack = in.readLine();
        System.out.println("[BROKER] " + (ack != null && ack.startsWith("OK|") ? ack.substring(3) : ack));
        System.out.println("\n[Request-Response] Handler activo. Respondiendo automaticamente.");
        System.out.println("Presiona Ctrl+C para salir.\n");

        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("REQUEST_MSG|")) {
                String[] p = line.split("\\|", 5);
                String correlId = p.length > 1 ? p[1] : "?";
                String content  = p.length > 4 ? p[4] : line;
                printRequestMsg(correlId, p.length>2?p[2]:"?", p.length>3?p[3]:"?", content);
                String response = "PROCESADO: " + content.toUpperCase();
                out.println("RESPONSE|" + correlId + "|" + response);
                System.out.println("-> Respuesta enviada: " + response + "\n");
            }
        }
    }

    // --- PATRON 3: Pub/Sub ---

    private static void subscribeTopic(PrintWriter out, BufferedReader in, Scanner kb)
            throws IOException {
        System.out.print("\nTopics (separados por coma, ej: noticias,economia): ");
        System.out.flush();
        String input = kb.hasNextLine() ? kb.nextLine().trim() : "general";
        if (input.isBlank()) input = "general";

        for (String t : input.split(",")) {
            String topic = t.trim();
            if (!topic.isBlank()) {
                out.println("TOPIC_SUBSCRIBE|" + topic);
                String ack = in.readLine();
                System.out.println("[BROKER] " + (ack != null && ack.startsWith("OK|") ? ack.substring(3) : ack));
            }
        }
        System.out.println("\n[Pub/Sub] Suscrito a: " + input);
        System.out.println("Presiona Ctrl+C para salir.\n");

        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("TOPIC_MSG|")) {
                String[] p = line.split("\\|", 5);
                printTopicMsg(p.length>1?p[1]:"?", p.length>2?p[2]:"?", p.length>3?p[3]:"?", p.length>4?p[4]:line);
            } else if (line.startsWith("OK|") || line.startsWith("ERROR|")) {
                System.out.println("[BROKER] " + (line.startsWith("OK|") ? line.substring(3) : line));
            }
        }
    }

    // --- DISPLAY ---

    private static void printQueueMsg(String queue, String id, String ts, String content) {
        msgCount++;
        System.out.println("+-----------------------------------------");
        System.out.printf("|  [COLA] MENSAJE #%d%n", msgCount);
        System.out.println("|  Cola      : " + queue);
        System.out.println("|  ID        : " + id);
        System.out.println("|  Timestamp : " + formatTs(ts));
        System.out.println("|  Contenido : " + content);
        System.out.println("+-----------------------------------------\n");
    }

    private static void printTopicMsg(String topic, String id, String ts, String content) {
        msgCount++;
        System.out.println("+-----------------------------------------");
        System.out.printf("|  [TOPIC] MENSAJE #%d%n", msgCount);
        System.out.println("|  Topic     : " + topic);
        System.out.println("|  ID        : " + id);
        System.out.println("|  Timestamp : " + formatTs(ts));
        System.out.println("|  Contenido : " + content);
        System.out.println("+-----------------------------------------\n");
    }

    private static void printRequestMsg(String correlId, String id, String ts, String content) {
        msgCount++;
        System.out.println("+-----------------------------------------");
        System.out.printf("|  [REQUEST] SOLICITUD #%d%n", msgCount);
        System.out.println("|  CorrelId  : " + correlId);
        System.out.println("|  ID        : " + id);
        System.out.println("|  Timestamp : " + formatTs(ts));
        System.out.println("|  Solicitud : " + content);
        System.out.println("+-----------------------------------------");
    }

    private static String formatTs(String iso) {
        try { return FMT.format(Instant.parse(iso)); }
        catch (Exception e) { return iso; }
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║     MESSAGE BROKER — CONSUMER (3 patrones)      ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  1. Send and Forget  -> recibe de una cola       ║");
        System.out.println("║  2. Request-Response -> handler: recibe/responde ║");
        System.out.println("║  3. Pub/Sub Topic    -> suscriptor de topics     ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }
}
