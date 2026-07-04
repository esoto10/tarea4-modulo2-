package broker.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * ══════════════════════════════════════════════════════════════════
 * ProducerClient — Cliente productor de mensajes.
 * ══════════════════════════════════════════════════════════════════
 *
 * Aplicación de consola que se conecta al Message Broker vía TCP
 * y permite enviar mensajes escritos por teclado.
 *
 * ── Protocolo de comunicación ─────────────────────────────────────
 *
 *   Paso 1 — Identificación:
 *     Cliente → Broker:  "PRODUCER\n"
 *     Broker  → Cliente: "OK|Conectado como PRODUCER...\n"
 *
 *   Paso 2 — Envío de mensajes (se repite N veces):
 *     Cliente → Broker:  "PUBLISH|<contenido del mensaje>\n"
 *     Broker  → Cliente: "OK|Mensaje encolado con ID: <uuid>\n"
 *
 *   Paso 3 — Desconexión:
 *     El usuario escribe "exit" → se cierra el socket
 *
 * ── Arquitectura de la conexión ──────────────────────────────────
 *
 *   [Teclado (stdin)]
 *       ↓  Scanner.nextLine()
 *   [ProducerClient]
 *       ↓  PrintWriter.println("PUBLISH|mensaje")
 *   [Socket TCP → puerto 9090]
 *       ↓
 *   [BrokerServer]
 *       ↓
 *   [BlockingQueue<Message>]
 *       ↓
 *   [Dispatcher → Consumers]
 *
 * ── Hilo receptor de confirmaciones ──────────────────────────────
 *
 * Para recibir las confirmaciones del broker (OK|...) mientras el usuario
 * escribe, usamos un Thread separado que lee del socket en background.
 * Esto evita que tengamos que alternar manualmente entre leer y escribir.
 *
 * Patrón: Thread dedicado solo a leer respuestas del servidor.
 */
public class ProducerClient {

    private static final String BROKER_HOST = "localhost";
    private static final int BROKER_PORT = 9090;

    public static void main(String[] args) {
        printBanner();
        System.out.println("Conectando a broker " + BROKER_HOST + ":" + BROKER_PORT + "...\n");

        // try-with-resources: cierra socket automáticamente al salir
        try (
            Socket socket = new Socket(BROKER_HOST, BROKER_PORT);
            PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")),
                true  // autoFlush: cada println() envía los datos inmediatamente
            );
            BufferedReader serverIn = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), "UTF-8"));
            Scanner keyboard = new Scanner(System.in)
        ) {
            System.out.println("Conectado al broker!");

            // ── Paso 1: Identificarse como PRODUCER ───────────────────────
            out.println("PRODUCER");

            // Leer confirmación del broker en un thread separado
            // para no bloquear la lectura del teclado
            Thread receiverThread = Thread.ofVirtual()
                .name("producer-receiver")
                .start(() -> readServerResponses(serverIn));

            System.out.println("─────────────────────────────────────────");
            System.out.println(" Comandos disponibles:");
            System.out.println("   <cualquier texto>  →  envía el mensaje");
            System.out.println("   exit               →  desconectarse");
            System.out.println("─────────────────────────────────────────\n");

            // ── Paso 2: Loop de envío de mensajes ─────────────────────────
            while (keyboard.hasNextLine()) {
                String input = keyboard.nextLine().trim();

                // Salir limpiamente
                if ("exit".equalsIgnoreCase(input)) {
                    System.out.println("\nCerrando conexión con el broker...");
                    break;
                }

                // Ignorar líneas vacías
                if (input.isBlank()) {
                    System.out.println("[!] Mensaje vacío ignorado. Escribe algo primero.");
                    continue;
                }

                // Enviar el mensaje con el formato del protocolo: PUBLISH|<contenido>
                out.println("PUBLISH|" + input);
                System.out.println("→ Enviando: \"" + input + "\"");
            }

            // Interrumpir el hilo receptor al salir
            receiverThread.interrupt();
            System.out.println("Producer desconectado. ¡Hasta luego!");

        } catch (IOException e) {
            System.err.println("\n[ERROR] No se pudo conectar al broker:");
            System.err.println("  " + e.getMessage());
            System.err.println("\n¿Está corriendo el broker?");
            System.err.println("  Ejecuta primero: cd broker-server && mvn exec:java");
            System.exit(1);
        }
    }

    /**
     * Método que corre en un virtual thread separado.
     * Lee continuamente las respuestas del broker y las muestra en consola.
     *
     * Esto permite que el usuario vea confirmaciones (OK|...) o errores
     * del broker sin interrumpir el flujo de escritura de mensajes.
     *
     * @param in stream de entrada del socket hacia el broker
     */
    private static void readServerResponses(BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("OK|")) {
                    // Mostrar confirmación del broker
                    System.out.println("[BROKER ACK] " + line.substring(3));
                } else if (line.startsWith("ERROR|")) {
                    // Mostrar error del broker
                    System.err.println("[BROKER ERR] " + line.substring(6));
                } else {
                    // Respuesta desconocida
                    System.out.println("[BROKER] " + line);
                }
            }
        } catch (IOException e) {
            // El socket se cerró — es el comportamiento esperado al salir
            if (!Thread.currentThread().isInterrupted()) {
                System.err.println("[BROKER] Conexión cerrada por el servidor.");
            }
        }
    }

    /** Banner de presentación del productor */
    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       MESSAGE BROKER — PRODUCER CLIENT           ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  Envía mensajes al broker vía TCP                ║");
        System.out.println("║  Protocolo: PUBLISH|<mensaje>                    ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }
}
