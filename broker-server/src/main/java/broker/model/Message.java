package broker.model;

import java.time.Instant;
import java.util.UUID;

/**
 * ══════════════════════════════════════════════════════════════════
 * Message — Modelo inmutable de un mensaje en el broker.
 * ══════════════════════════════════════════════════════════════════
 *
 * Usamos un Record de Java 16+ (estable en Java 21) en lugar de una clase
 * tradicional. Un Record es ideal aquí porque:
 *
 *   1. INMUTABILIDAD por diseño: los campos solo se asignan en el constructor.
 *      En un sistema concurrente, la inmutabilidad elimina condiciones de carrera
 *      en la lectura de mensajes sin necesidad de synchronized.
 *
 *   2. CÓDIGO CONCISO: el compilador genera automáticamente:
 *        - Constructor canónico con validación
 *        - Accessors: id(), timestamp(), content()
 *        - equals() y hashCode() basados en todos los campos
 *        - toString() legible para logs
 *
 *   3. SEMÁNTICA CLARA: un Record comunica "este objeto es solo datos", lo cual
 *      es exactamente lo que es un mensaje en un broker.
 *
 * Atributos (los tres requeridos por la POC):
 *   - id:        identificador único UUID del mensaje
 *   - timestamp: momento exacto de creación (UTC)
 *   - content:   texto del mensaje enviado por el productor
 *
 * Protocolo de serialización sobre TCP:
 *   Formato de red: MSG|<id>|<timestamp>|<content>
 *   Ejemplo:        MSG|a1b2c3d4-...|2025-01-15T10:30:00Z|Hola Mundo
 *
 * NOTA: el separador '|' es safe incluso si 'content' contiene '|',
 * porque usamos split("\\|", 4) — solo divide en las primeras 3 ocurrencias.
 */
public record Message(String id, Instant timestamp, String content) {

    /**
     * Constructor compacto con validación.
     * En un Record, el constructor compacto se ejecuta antes que el canónico.
     * Validamos en el límite del sistema para evitar mensajes corruptos.
     */
    public Message {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("El ID del mensaje no puede ser nulo ni vacío");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("El timestamp no puede ser nulo");
        }
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("El contenido del mensaje no puede ser nulo ni vacío");
        }
    }

    /**
     * Factory method: crea un mensaje nuevo con ID UUID aleatorio y
     * timestamp del instante actual en UTC.
     *
     * Patrón "static factory" en lugar de constructor público directo:
     * - Comunica intención ("crea un mensaje a partir de contenido")
     * - Oculta los detalles de generación de ID y timestamp
     *
     * @param content el texto del mensaje (no puede ser nulo ni vacío)
     * @return nueva instancia inmutable de Message
     */
    public static Message of(String content) {
        return new Message(
            UUID.randomUUID().toString(),  // ID globalmente único
            Instant.now(),                 // Timestamp en UTC
            content
        );
    }

    /**
     * Serializa este mensaje a una línea de texto para transmitir por TCP.
     *
     * Formato: MSG|<id>|<timestamp>|<content>
     *
     * El prefijo "MSG" permite al consumidor identificar el tipo de línea
     * si en el futuro el protocolo soporta otros tipos (ACK, PING, etc.).
     *
     * @return string serializado listo para enviar por PrintWriter.println()
     */
    public String serialize() {
        // Instant.toString() produce formato ISO-8601: 2025-01-15T10:30:00.123456789Z
        // Este formato es estándar y parseable con Instant.parse()
        return "MSG" + SEPARATOR + id + SEPARATOR + timestamp + SEPARATOR + content;
    }

    /**
     * Deserializa una línea recibida por TCP a un objeto Message.
     *
     * Espera formato: MSG|<id>|<timestamp>|<content>
     *
     * Usamos split("\\|", 4) para dividir en MÁXIMO 4 partes.
     * Esto garantiza que si el contenido tiene '|', solo se split
     * en las primeras 3 ocurrencias — el contenido queda intacto.
     *
     * @param raw la línea de texto tal como llegó del socket
     * @return instancia de Message con los datos parseados
     * @throws IllegalArgumentException si el formato es inválido
     */
    public static Message deserialize(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("No se puede deserializar una línea nula o vacía");
        }

        // Dividir respetando que el content puede tener separadores
        String[] parts = raw.split("\\" + SEPARATOR, 4);

        if (parts.length < 4) {
            throw new IllegalArgumentException(
                "Formato de mensaje inválido (se esperan 4 partes): " + raw
            );
        }
        if (!"MSG".equals(parts[0])) {
            throw new IllegalArgumentException(
                "Tipo de mensaje desconocido (se esperaba MSG): " + parts[0]
            );
        }

        try {
            return new Message(
                parts[1],                   // id
                Instant.parse(parts[2]),    // timestamp ISO-8601
                parts[3]                    // content (puede contener '|')
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Error al parsear mensaje: " + raw, e);
        }
    }

    /** Separador del protocolo TCP. Definido como constante para consistencia. */
    public static final String SEPARATOR = "|";
}
