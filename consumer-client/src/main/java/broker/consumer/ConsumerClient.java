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

/**
 * ══════════════════════════════════════════════════════════════════
 * ConsumerClient — Cliente consumidor de mensajes.
 * ══════════════════════════════════════════════════════════════════
 *
 * Aplicación de consola que se conecta al Message Broker vía TCP
 * y recibe mensajes en tiempo real. Cada mensaje se muestra
 * formateado en consola inmediatamente al recibirlo.
 *
 * El consumer es completamente PASIVO: no envía datos al broker,
 * solo escucha. El broker (a través del dispatcher) le entrega
 * mensajes automáticamente cuando los hay en la cola.
 *
 * ── Protocolo de comunicación ─────────────────────────────────────
 *
 *   Paso 1 — Identificación:
 *     Cliente → Broker:  "CONSUMER\n"
 *     Broker  → Cliente: "OK|Conectado como CONSUMER...\n"
 *
 *   Paso 2 — Recepción de mensajes (indefinidamente):
 *     Broker  → Cliente: "MSG|<id>|<timestamp>|<contenido>\n"
 *
 *   El consumer queda bloqueado en readLine() esperando el siguiente
 *   mensaje. Con Virtual Threads (en el broker) esto escala muy bien
 *   sin consumir recursos del OS.
 *
 * ── Formato de mensaje recibido ───────────────────────────────────
 *
 *   MSG|3f4a9b12-...|2025-01-15T10:30:00.123456789Z|Hola Mundo
 *     ^    ^                ^                           ^
 *     |    |                |                           |
 *   tipo  UUID           timestamp ISO-8601           contenido
 *
 * ── Múltiples consumers ────────────────────────────────────────────
 *
 * Pueden ejecutarse MÚLTIPLES instancias del ConsumerClient simultáneamente.
 * El broker registrará cada una y el dispatcher enviará TODOS los mensajes
 * a TODOS los consumers conectados (broadcast).
 *
 * Esto es diferente a una cola de trabajo (work queue) donde cada mensaje
 * va a UN solo consumer. Aquí implementamos el patrón BROADCAST / FAN-OUT.
 */
public class ConsumerClient {

    private static final String BROKER_HOST = "localhost";
    private static final int BROKER_PORT = 9090;

    /**
     * Formateador de timestamp para mostrar fechas en formato local legible.
     *
     * El broker envía timestamps en UTC ISO-8601.
     * Los convertimos a la zona horaria local del sistema para
     * mejor legibilidad en la consola.
     *
     * Ejemplo de conversión:
     *   UTC:   2025-01-15T15:30:00Z
     *   Local: 2025-01-15 10:30:00 (si estamos en UTC-5)
     */
    private static final DateTimeFormatter DISPLAY_FORMATTER =
        DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    /** Contador de mensajes recibidos en esta sesión */
    private static int messageCount = 0;

    public static void main(String[] args) {
        printBanner();
        System.out.println("Conectando a broker " + BROKER_HOST + ":" + BROKER_PORT + "...\n");

        // try-with-resources: cierra socket y streams automáticamente al salir
        try (
            Socket socket = new Socket(BROKER_HOST, BROKER_PORT);
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")),
                true  // autoFlush
            );
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"))
        ) {
            System.out.println("Conectado al broker!");

            // ── Paso 1: Identificarse como CONSUMER ───────────────────────
            out.println("CONSUMER");

            // Leer confirmación del broker (primera respuesta)
            String confirmation = in.readLine();
            if (confirmation != null) {
                if (confirmation.startsWith("OK|")) {
                    System.out.println("[BROKER] " + confirmation.substring(3));
                } else {
                    System.out.println("[BROKER] " + confirmation);
                }
            }

            System.out.println("─────────────────────────────────────────");
            System.out.println(" Esperando mensajes del broker...");
            System.out.println(" Presiona Ctrl+C para desconectarse.");
            System.out.println("─────────────────────────────────────────\n");

            // ── Paso 2: Loop de recepción de mensajes ─────────────────────
            //
            // readLine() BLOQUEA hasta que el broker envíe una línea.
            // Cuando no hay mensajes, este thread duerme eficientemente
            // (el OS lo despierta solo cuando llegan bytes al socket).
            //
            // El loop termina cuando:
            //   a) El broker cierra la conexión (readLine() devuelve null)
            //   b) El usuario presiona Ctrl+C (IOException)
            String line;
            while ((line = in.readLine()) != null) {
                displayMessage(line);
            }

            System.out.println("\n[INFO] Conexión cerrada por el broker.");

        } catch (IOException e) {
            // Verificar si fue una desconexión normal (Ctrl+C) o un error real
            if (e.getMessage() != null && e.getMessage().contains("Connection reset")) {
                System.out.println("\n[INFO] Conexión cerrada.");
            } else {
                System.err.println("\n[ERROR] No se pudo conectar al broker:");
                System.err.println("  " + e.getMessage());
                System.err.println("\n¿Está corriendo el broker?");
                System.err.println("  Ejecuta primero: cd broker-server && mvn exec:java");
                System.exit(1);
            }
        }

        System.out.println("Total mensajes recibidos en esta sesión: " + messageCount);
        System.out.println("Consumer desconectado. ¡Hasta luego!");
    }

    /**
     * Parsea y muestra un mensaje recibido del broker.
     *
     * Formato esperado: MSG|<id>|<timestamp>|<content>
     *
     * Si la línea no tiene el formato esperado, la muestra como texto plano
     * (puede ser un mensaje de estado del broker como OK|...).
     *
     * @param raw la línea de texto recibida del socket
     */
    private static void displayMessage(String raw) {
        if (raw == null || raw.isBlank()) {
            return;
        }

        // Intentar parsear como mensaje (formato: MSG|id|timestamp|content)
        if (raw.startsWith("MSG|")) {
            // Dividir en máximo 4 partes para respetar '|' en el contenido
            String[] parts = raw.split("\\|", 4);

            if (parts.length == 4) {
                messageCount++;
                String id = parts[1];
                String formattedTimestamp = formatTimestamp(parts[2]);
                String content = parts[3];

                // Mostrar el mensaje con formato visual claro
                System.out.println("┌─────────────────────────────────────────");
                System.out.printf("│  MENSAJE #%-3d RECIBIDO%n", messageCount);
                System.out.println("├─────────────────────────────────────────");
                System.out.println("│  ID        : " + id);
                System.out.println("│  Timestamp : " + formattedTimestamp);
                System.out.println("│  Contenido : " + content);
                System.out.println("└─────────────────────────────────────────");
                System.out.println(); // línea en blanco para separar mensajes
                return;
            }
        }

        // Si no es un MSG, mostrar como respuesta genérica del broker
        if (raw.startsWith("OK|")) {
            System.out.println("[BROKER] " + raw.substring(3));
        } else if (raw.startsWith("ERROR|")) {
            System.err.println("[BROKER ERROR] " + raw.substring(6));
        } else {
            System.out.println("[BROKER] " + raw);
        }
    }

    /**
     * Convierte un timestamp ISO-8601 UTC al formato de fecha/hora local legible.
     *
     * @param isoTimestamp timestamp en formato "2025-01-15T10:30:00.123456789Z"
     * @return timestamp formateado como "2025-01-15 10:30:00.123" en zona local
     */
    private static String formatTimestamp(String isoTimestamp) {
        try {
            Instant instant = Instant.parse(isoTimestamp);
            return DISPLAY_FORMATTER.format(instant);
        } catch (Exception e) {
            // Si el formato no es parseable, devolver el original
            return isoTimestamp;
        }
    }

    /** Banner de presentación del consumer */
    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       MESSAGE BROKER — CONSUMER CLIENT           ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  Recibe mensajes del broker en tiempo real       ║");
        System.out.println("║  Protocolo: MSG|id|timestamp|contenido           ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }
}
