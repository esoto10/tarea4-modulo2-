# Message Broker POC — Java 21 (Multi-Patron)

Implementacion desde cero de un **Message Broker** con soporte para 3 patrones de mensajeria,
usando unicamente el JDK de Java 21. Sin frameworks, sin dependencias externas.

---

## Indice

1. [Stack Tecnologico](#1-stack-tecnologico)
2. [Que es un Message Broker](#2-que-es-un-message-broker)
3. [Patron 1 — Send and Forget](#3-patron-1--send-and-forget)
4. [Patron 2 — Request-Response](#4-patron-2--request-response)
5. [Patron 3 — Publicador / Suscriptor (Pub/Sub)](#5-patron-3--publicador--suscriptor-pubsub)
6. [Comparativa de Patrones](#6-comparativa-de-patrones)
7. [Arquitectura del Sistema](#7-arquitectura-del-sistema)
8. [Estructura del Proyecto](#8-estructura-del-proyecto)
9. [Protocolo TCP](#9-protocolo-tcp)
10. [Concurrencia y Decisiones de Diseno](#10-concurrencia-y-decisiones-de-diseno)
11. [Como Ejecutar Localmente](#11-como-ejecutar-localmente)
12. [Ejemplos de Ejecucion](#12-ejemplos-de-ejecucion)

---

## 1. Stack Tecnologico

| Componente | Tecnologia | Version |
|---|---|---|
| Lenguaje | Java | 21 LTS |
| Concurrencia | Virtual Threads (Project Loom) | Java 21 |
| Cola Send and Forget | LinkedBlockingQueue por cola nombrada | JDK |
| Registro Pub/Sub | ConcurrentHashMap<topic, Set<session>> | JDK |
| Correlacion Request-Response | ConcurrentHashMap<correlId, session> | JDK |
| Transporte | ServerSocket / Socket TCP | JDK |
| Modelo de datos | Java record (Message) | Java 16+ |
| Build | Apache Maven | 3.6+ |

---

## 2. Que es un Message Broker

Un **Message Broker** es un componente de infraestructura que actua como intermediario entre
productores (quienes envian mensajes) y consumidores (quienes los reciben).

### Responsabilidades del broker

- **Desacoplamiento**: el productor no necesita saber quien consume sus mensajes.
- **Persistencia temporal**: los mensajes se almacenan en cola hasta ser entregados.
- **Enrutamiento**: decide a quien entregar cada mensaje segun el patron configurado.
- **Control de flujo (Backpressure)**: si la cola se llena, detiene al productor.

### Por que no conectar productor y consumidor directamente?

```
SIN BROKER (acoplado):
  Servicio-A --> Servicio-B   (si B cae, A falla tambien)
  Servicio-A --> Servicio-C   (A debe conocer la IP/puerto de C)
  Servicio-A --> Servicio-D   (cambio en D requiere cambiar A)

CON BROKER (desacoplado):
  Servicio-A --> [BROKER] --> Servicio-B
                         --> Servicio-C    (A solo conoce al broker)
                         --> Servicio-D    (B, C, D pueden caer sin afectar a A)
```

---

## 3. Patron 1 — Send and Forget

### Teoria

**Send and Forget** (tambien llamado *Fire and Forget* o *Work Queue*) es el patron mas simple:
el productor envia un mensaje al broker y **no espera respuesta**. El broker lo almacena en
una cola y lo entrega a exactamente **UN consumer**.

Si hay multiples consumers suscritos a la misma cola, el broker los atiende en **round-robin**,
distribuyendo el trabajo equitativamente.

#### Caracteristicas clave

| Caracteristica | Descripcion |
|---|---|
| Receptor | UN solo consumer (round-robin si hay varios) |
| Respuesta | No (el producer no sabe si fue procesado) |
| Persistencia | Si — los mensajes esperan en cola si no hay consumers |
| Balanceo de carga | Si — los mensajes se distribuyen entre workers |
| Garantia de entrega | Al menos una vez (si el broker no reinicia) |

#### Casos de uso reales

- **Procesamiento de pedidos**: un servicio publica pedidos; varios workers los procesan en paralelo.
- **Envio de emails**: una API publica emails a enviar; workers de correo los despachan.
- **Redimension de imagenes**: al subir una imagen, se encola una tarea; workers la procesan.
- **Logs asincronos**: microservicios publican logs; un worker los escribe en base de datos.

### Flujo de ejemplo — Pedidos de una tienda online

```
TIEMPO    PRODUCTOR (API)            BROKER                    CONSUMIDOR (Worker)

T=1       Llega pedido #1001
          QUEUE_SEND|pedidos|        Recibe mensaje             Worker-A conectado:
          "Laptop Dell 15"  ------>  Crea Message(id, ts, msg)  QUEUE_SUBSCRIBE|pedidos
                                     queue["pedidos"].put(msg)

T=2                                  Dispatcher toma mensaje:
                                     msg = queue.take()
                                     Round-Robin -> Worker-A
                                     Envia: QUEUE_MSG|pedidos    ------> Worker-A recibe:
                                            |uuid|2026-01-15...|        COLA MENSAJE #1
                                            |"Laptop Dell 15"           Cola     : pedidos
                                                                         Contenido: Laptop Dell 15

T=3       Llega pedido #1002         Mismo flujo
          QUEUE_SEND|pedidos|        Round-Robin -> Worker-B    ------> Worker-B recibe
          "Mouse Logitech" ------->  (si hay 2 workers)                 pedido #1002

T=4       [Producer no espera        
           respuesta — continua      
           procesando]               
```

### Protocolo TCP

```
Consumer --> Broker:  QUEUE_SUBSCRIBE|pedidos
Broker   --> Consumer: OK|Suscrito a cola 'pedidos'. Esperando mensajes...

Producer --> Broker:  QUEUE_SEND|pedidos|Laptop Dell 15
Broker   --> Producer: OK|Encolado en 'pedidos' | ID=a1b2c3d4...
Broker   --> Consumer: QUEUE_MSG|pedidos|a1b2c3d4-...|2026-01-15T10:30:00Z|Laptop Dell 15
```

---

## 4. Patron 2 — Request-Response

### Teoria

**Request-Response** implementa comunicacion sincronica sobre mensajeria asincronica.
El producer envia una **solicitud** y **espera activamente** una **respuesta** del consumer.

El mecanismo clave es el **correlationId**: un UUID unico que vincula cada solicitud con
su respuesta. El broker usa este ID para enrutar la respuesta al producer correcto, incluso
cuando hay multiples solicitudes en vuelo simultaneamente.

#### Como funciona la correlacion

```
Producer envia solicitud A (correlId: corr-001)
Producer envia solicitud B (correlId: corr-002)   <-- multiples en vuelo

Broker almacena:
  corr-001 --> sesion del Producer
  corr-002 --> sesion del Producer

Consumer responde a corr-002 primero (puede tardar diferente tiempo)
  Broker busca corr-002 --> Producer
  Entrega RESPONSE_MSG|corr-002|resultado al Producer

Consumer responde a corr-001
  Broker busca corr-001 --> Producer
  Entrega RESPONSE_MSG|corr-001|resultado al Producer
```

#### Caracteristicas clave

| Caracteristica | Descripcion |
|---|---|
| Receptor | UN handler (round-robin si hay varios) |
| Respuesta | Si — el producer recibe la respuesta del consumer |
| Persistencia | No — si no hay handlers, el broker rechaza la request |
| Timeout | 30 segundos (configurable en RequestResponseManager) |
| Correlacion | UUID generado por el producer |

#### Por que usar mensajeria para Request-Response en lugar de HTTP directo?

- **Desacoplamiento**: el producer no conoce la IP/puerto del consumer.
- **Balanceo automatico**: multiples handlers se distribuyen sin load balancer externo.
- **Reintentos y circuit breaking**: el broker puede implementarlos de forma centralizada.
- **Auditoria**: cada solicitud y respuesta queda registrada en el broker.

#### Casos de uso reales

- **Consulta de inventario**: "hay stock del producto X?" → "Si, 15 unidades"
- **Validacion de pago**: "es valida esta tarjeta?" → "Aprobada / Rechazada"
- **Calculo de precio**: "precio con descuento para cliente premium?" → "$ 89.90"
- **Geocodificacion**: "coordenadas de esta direccion?" → "lat: -12.04, lon: -77.05"

### Flujo de ejemplo — Consulta de stock

```
TIEMPO    PRODUCER (API Carrito)     BROKER                    CONSUMER (Servicio Stock)

T=0                                                             Consumer conectado:
                                                                RR_READY

T=1       Usuario agrega producto
          al carrito.
          correlId = UUID()
          "corr-abc123"

T=2       REQUEST|corr-abc123|       Recibe REQUEST
          "hay stock prod-42?" ---->  Crea Message(id, ts, msg)
                                     pendingRequests["corr-abc123"]
                                       = sesion del Producer
                                     Round-Robin: elige handler
                                     Envia a Consumer:           ----> Consumer recibe:
                                     REQUEST_MSG|corr-abc123|          REQUEST SOLICITUD #1
                                     uuid|ts|"hay stock prod-42?"      Solicitud: hay stock prod-42?

T=3       Producer queda             VT Timeout iniciado (30s)   Consumer procesa...
          leyendo del socket         si no hay respuesta         consulta base de datos...
          esperando RESPONSE_MSG     en 30s -> envia TIMEOUT     "Si, 23 unidades"
                                                                  out.println("RESPONSE|
                                                                  corr-abc123|Si, 23 unidades")

T=4                                  Recibe RESPONSE|corr-abc123
                                     Busca: corr-abc123 -> Producer
                                     Envia al Producer:
                                     RESPONSE_MSG|corr-abc123|  ----> Producer recibe:
                                     "Si, 23 unidades"                RESPUESTA RECIBIDA
                                                                       CorrelId : corr-abc123
                                                                       Respuesta: Si, 23 unidades

T=5       Continua el flujo
          del carrito con
          confirmacion de stock.
```

### Protocolo TCP

```
Consumer --> Broker:  RR_READY
Broker   --> Consumer: OK|Registrado como handler...

Producer --> Broker:  REQUEST|corr-abc123|hay stock prod-42?
Broker   --> Producer: OK|Request enviada | correlId=corr-abc1...
Broker   --> Consumer: REQUEST_MSG|corr-abc123|uuid|2026-01-15T...|hay stock prod-42?
Consumer --> Broker:  RESPONSE|corr-abc123|Si, 23 unidades
Broker   --> Producer: RESPONSE_MSG|corr-abc123|Si, 23 unidades
```

---

## 5. Patron 3 — Publicador / Suscriptor (Pub/Sub)

### Teoria

**Publish-Subscribe** es el patron de mayor desacoplamiento: el publisher emite mensajes
a un **topic** (canal nombrado) sin saber cuantos ni quienes son los suscriptores.
**TODOS** los consumers suscritos al topic reciben el mismo mensaje simultaneamente (fan-out).

Un topic es como una "frecuencia de radio": el emisor transmite y cualquiera con el receptor
sintonizado en esa frecuencia recibe la senal.

#### Diferencia entre Send and Forget y Pub/Sub

```
Send and Forget (Work Queue):
  Mensaje --> [Cola] --> UN consumer (trabajo compartido)
  Ejemplo: 3 workers procesan 3 pedidos diferentes

Pub/Sub (Broadcast):
  Mensaje --> [Topic] --> TODOS los suscriptores (notificacion)
  Ejemplo: 3 dashboards muestran la misma alerta de sistema
```

#### Caracteristicas clave

| Caracteristica | Descripcion |
|---|---|
| Receptor | TODOS los suscriptores del topic |
| Respuesta | No |
| Persistencia | No — si nadie esta suscrito, el mensaje se descarta |
| Fan-out | Si — 1 mensaje llega a N suscriptores simultaneamente |
| Multi-topic | Un consumer puede suscribirse a varios topics |

#### Casos de uso reales

- **Precios en tiempo real**: un servicio publica el precio del dolar; N apps lo muestran.
- **Notificaciones**: al crear un usuario se publica un evento; email-service, crm-service y audit-service lo reciben.
- **Sincronizacion de cache**: al actualizar un catalogo, todos los nodos invalidan su cache.
- **Chat en sala**: un mensaje en la sala llega a todos los participantes conectados.
- **Event Sourcing**: un evento de negocio llega a multiples proyectores que construyen vistas.

### Flujo de ejemplo — Sistema de alertas de monitoreo

```
TIEMPO    PUBLISHER (Monitor)        BROKER                    SUSCRIPTORES

T=0                                                             Dashboard-1 conectado:
                                                                TOPIC_SUBSCRIBE|alertas

                                                                Dashboard-2 conectado:
                                                                TOPIC_SUBSCRIBE|alertas

                                                                Notif-Email conectado:
                                                                TOPIC_SUBSCRIBE|alertas

T=1       CPU al 95%
          TOPIC_PUBLISH|alertas|    Recibe mensaje
          "CPU 95% en srv-01" ---->  Crea Message(id, ts, msg)
                                     subscribers["alertas"] =
                                     {Dashboard-1, Dashboard-2, Notif-Email}
                                     Fan-out: envia a los 3:
                                     TOPIC_MSG|alertas|uuid|ts| --> Dashboard-1 muestra alerta
                                     "CPU 95% en srv-01"        --> Dashboard-2 muestra alerta
                                                                 --> Notif-Email envia correo

T=2       Publisher recibe ACK:
          OK|Publicado en 'alertas'
            -> 3 suscriptores

T=3       Nuevo suscriptor conecta:
          Dashboard-3:
          TOPIC_SUBSCRIBE|alertas
          (desde este momento
           recibira los proximos
           mensajes, NO los anteriores)

T=4       CPU baja al 20%
          TOPIC_PUBLISH|alertas|    Fan-out a los 4 suscriptores:
          "CPU 20% en srv-01" ---->  --> Dashboard-1, 2, 3 actualizan
                                     --> Notif-Email envia "resuelto"
```

### Protocolo TCP

```
Consumer --> Broker:  TOPIC_SUBSCRIBE|alertas
Broker   --> Consumer: OK|Suscrito a topic 'alertas'. Esperando publicaciones...

Publisher --> Broker: TOPIC_PUBLISH|alertas|CPU 95% en srv-01
Broker   --> Publisher: OK|Publicado en 'alertas' -> 3 suscriptores
Broker   --> Consumer1: TOPIC_MSG|alertas|uuid|2026-01-15T10:30:00Z|CPU 95% en srv-01
Broker   --> Consumer2: TOPIC_MSG|alertas|uuid|2026-01-15T10:30:00Z|CPU 95% en srv-01
Broker   --> Consumer3: TOPIC_MSG|alertas|uuid|2026-01-15T10:30:00Z|CPU 95% en srv-01
```

---

## 6. Comparativa de Patrones

| Caracteristica | Send and Forget | Request-Response | Pub/Sub |
|---|---|---|---|
| Receptor del mensaje | 1 consumer | 1 handler | TODOS los suscriptores |
| El producer espera respuesta | No | Si (con timeout 30s) | No |
| Cola persistente | Si (BlockingQueue) | No | No |
| Balanceo de carga | Si (round-robin) | Si (round-robin) | No aplica |
| Correlacion de mensajes | No | Si (correlationId UUID) | No |
| Sin suscriptores activos | Mensaje espera en cola | Rechaza con ERROR | Mensaje descartado |
| Caso tipico | Tareas asincronas | Consultas sincronas | Notificaciones y eventos |
| Clase en el proyecto | QueueManager.java | RequestResponseManager.java | TopicManager.java |

---

## 7. Arquitectura del Sistema

```
+----------------------------------------------------------------------+
|                    BROKER SERVER :9090                               |
|                                                                      |
|  ServerSocket.accept()                                               |
|       |                                                              |
|       v  (un Virtual Thread por conexion)                            |
|  ConnectionHandler                                                   |
|    QUEUE_SEND     -----> QueueManager                                |
|    TOPIC_PUBLISH  -----> TopicManager                                |
|    REQUEST        -----> RequestResponseManager                      |
|    QUEUE_SUBSCRIBE----> QueueManager.subscribe()                     |
|    TOPIC_SUBSCRIBE----> TopicManager.subscribe()                     |
|    RR_READY       -----> RequestResponseManager.registerConsumer()   |
|    RESPONSE       -----> RequestResponseManager.handleResponse()     |
|                                                                      |
|  +----------------+  +------------------+  +---------------------+  |
|  | QueueManager   |  | TopicManager     |  | RequestResponse Mgr |  |
|  |                |  |                  |  |                     |  |
|  | "pedidos" ->   |  | "alertas" ->     |  | corr-001 -> prod-A  |  |
|  | [msg1,msg2]    |  | {sub-A, sub-B}   |  | corr-002 -> prod-B  |  |
|  | VT Dispatcher  |  | Fan-out directo  |  | VT Timeout 30s      |  |
|  | Round-Robin    |  |                  |  | RR Consumers []     |  |
|  +----------------+  +------------------+  +---------------------+  |
+----------------------------------------------------------------------+
```

---

## 8. Estructura del Proyecto

```
tarea04-poc/
    README.md
    test-patrones.ps1                           Script de prueba automatizada
    broker-server/
        pom.xml
        src/main/java/broker/
            model/
                Message.java                    Record: id, timestamp, content
            session/
                ClientSession.java              Sesion activa (send sincronizado)
            routing/
                QueueManager.java               Patron 1: Send and Forget
                RequestResponseManager.java     Patron 2: Request-Response
                TopicManager.java               Patron 3: Pub/Sub
            network/
                ConnectionHandler.java          Router de protocolo TCP
            server/
                BrokerServer.java               ServerSocket + VT pool + 3 managers
    producer-client/
        pom.xml
        src/main/java/broker/client/
            ProducerClient.java                 Menu: elige patron y envia
    consumer-client/
        pom.xml
        src/main/java/broker/consumer/
            ConsumerClient.java                 Menu: elige patron y recibe
```

---

## 9. Protocolo TCP

Todas las comunicaciones son texto plano sobre TCP. Cada linea termina con \n.

### Identificacion (primera linea del cliente)
```
PRODUCER
CONSUMER
```

### Comandos del Producer
```
QUEUE_SEND|<cola>|<contenido>        Patron 1: enviar a cola nombrada
REQUEST|<correlId>|<contenido>       Patron 2: enviar solicitud
TOPIC_PUBLISH|<topic>|<contenido>    Patron 3: publicar en topic
```

### Comandos del Consumer
```
QUEUE_SUBSCRIBE|<cola>               Patron 1: suscribirse a cola
RR_READY                             Patron 2: registrarse como handler
TOPIC_SUBSCRIBE|<topic>              Patron 3: suscribirse a topic
RESPONSE|<correlId>|<respuesta>      Patron 2: enviar respuesta al producer
```

### Mensajes del Broker al Consumer
```
QUEUE_MSG|<cola>|<id>|<timestamp>|<contenido>           Patron 1
REQUEST_MSG|<correlId>|<id>|<timestamp>|<contenido>     Patron 2
TOPIC_MSG|<topic>|<id>|<timestamp>|<contenido>          Patron 3
```

### Mensajes del Broker al Producer
```
OK|<confirmacion>
ERROR|<motivo>
RESPONSE_MSG|<correlId>|<respuesta>                     Patron 2
```

---

## 10. Concurrencia y Decisiones de Diseno

### Virtual Threads (Java 21 / Project Loom)

Los Virtual Threads son hilos ultra-ligeros gestionados por la JVM (no por el OS).

**Problema que resuelven:**
Un servidor TCP tradicional tiene que elegir entre:
- Threads del OS (1 por conexion): simple pero caro en memoria (~1MB/thread). Con 5000 conexiones = 5GB solo en stacks.
- Programacion reactiva (NIO, callbacks): eficiente pero codigo complejo y dificil de depurar.

**Solucion con Virtual Threads:**
- Codigo bloqueante y simple (como threads del OS).
- Escala como NIO: la JVM "desmonta" el VT cuando se bloquea en I/O y cede el OS thread a otro VT.
- Podemos tener 100,000 conexiones con el mismo overhead que antes con 100 threads.

### Decisiones de estructura de datos

| Estructura | Donde se usa | Razon |
|---|---|---|
| LinkedBlockingQueue | QueueManager por cola | take() duerme sin CPU; put() da backpressure |
| CopyOnWriteArrayList | Lista de consumers por cola | Muchas lecturas (round-robin), pocas escrituras |
| ConcurrentHashMap | Topics, sessions, RR pending | Operaciones concurrentes sin bloquear todo el mapa |
| AtomicInteger | Indices round-robin | getAndIncrement() atomico sin synchronized |
| synchronized send() | ClientSession | PrintWriter no es thread-safe; multiples VT escriben al mismo socket |
| Java record | Message | Inmutabilidad: cero condiciones de carrera al leer entre threads |

---

## 11. Como Ejecutar Localmente

### Pre-requisitos
- Java 21+ (java -version)
- Maven 3.6+ (mvn -version)
- 3 o 4 terminales abiertas

### Paso 1 — SIEMPRE iniciar el Broker primero

```bash
cd broker-server
mvn exec:java
```

Esperar: `Servidor TCP escuchando en puerto 9090`

---

## 12. Ejemplos de Ejecucion

### Ejemplo A — Patron 1: Send and Forget

Terminal 2 — Consumer (worker que recibe pedidos):
```bash
cd consumer-client
mvn exec:java
# Seleccionar: 1
# Cola: pedidos
```

Terminal 3 — Producer (API que envia pedidos):
```bash
cd producer-client
mvn exec:java
# Seleccionar: 1
# Cola: pedidos
# Mensaje: Pedido 1001 - Laptop Dell
```

Resultado en Consumer:
```
+-----------------------------------------
|  [COLA] MENSAJE #1
|  Cola      : pedidos
|  ID        : 62c596ec-...
|  Timestamp : 2026-07-06 20:30:00.123
|  Contenido : Pedido 1001 - Laptop Dell
+-----------------------------------------
```

---

### Ejemplo B — Patron 2: Request-Response (Consulta de Inventario)

Terminal 2 — Consumer (handler de inventario):
```bash
cd consumer-client
mvn exec:java
# Seleccionar: 2
```

Al arrancar el handler muestra el catalogo disponible:
```
[Request-Response] Handler de inventario activo.
Productos disponibles para consultar:
  prod-001 -> Laptop Dell 15"
  prod-002 -> Mouse Logitech
  prod-003 -> Teclado Mecanico
  prod-004 -> Monitor 4K 27"
  prod-005 -> Auriculares Sony
  prod-006 -> Webcam HD 1080p
  prod-007 -> SSD 1TB
  prod-008 -> Hub USB-C
```

Terminal 3 — Producer (cliente que consulta productos):
```bash
cd producer-client
mvn exec:java
# Seleccionar: 2
# Solicitud: prod-001
```

Resultado en Consumer (handler):
```
+-----------------------------------------
|  [REQUEST] SOLICITUD #1
|  CorrelId  : 7ad22946-a827-...
|  Timestamp : 2026-07-06 20:10:00.123
|  Solicitud : prod-001
+-----------------------------------------
-> Respuesta enviada: OK | Laptop Dell 15" | Stock: 8 unidades | Precio: $1,299.00
```

Resultado en Producer:
```
-> Request enviada | correlId=7ad22946...
   Esperando respuesta del handler...

[RESPUESTA RECIBIDA]
  CorrelId : 7ad22946...
  Respuesta: OK | Laptop Dell 15" | Stock: 8 unidades | Precio: $1,299.00
```

Consulta de producto agotado (`prod-005`):
```
[RESPUESTA RECIBIDA]
  Respuesta: OK | Auriculares Sony | Stock: 0 unidades | Precio: $199.00 (AGOTADO)
```

Consulta de producto inexistente (`prod-999`):
```
[RESPUESTA RECIBIDA]
  Respuesta: ERROR | Producto 'prod-999' no encontrado. Productos: prod-001, prod-002, ...
```

Productos del catalogo:

| Codigo | Producto | Stock | Precio |
|---|---|---|---|
| prod-001 | Laptop Dell 15" | 8 | $1,299.00 |
| prod-002 | Mouse Logitech | 42 | $29.90 |
| prod-003 | Teclado Mecanico | 15 | $89.50 |
| prod-004 | Monitor 4K 27" | 3 | $549.00 |
| prod-005 | Auriculares Sony | 0 | $199.00 (AGOTADO) |
| prod-006 | Webcam HD 1080p | 22 | $79.00 |
| prod-007 | SSD 1TB | 11 | $109.00 |
| prod-008 | Hub USB-C | 35 | $39.90 |

---

### Ejemplo C — Patron 3: Pub/Sub con 2 suscriptores

Terminal 2 — Consumer suscriptor 1:
```bash
cd consumer-client && mvn exec:java
# Seleccionar: 3 | Topic: noticias
```

Terminal 3 — Consumer suscriptor 2:
```bash
cd consumer-client && mvn exec:java
# Seleccionar: 3 | Topic: noticias
```

Terminal 4 — Producer (publisher de noticias):
```bash
cd producer-client && mvn exec:java
# Seleccionar: 3
# Topic: noticias
# Publicar: El dolar sube 0.5% hoy
```

Resultado en AMBOS consumers simultaneamente:
```
+-----------------------------------------
|  [TOPIC] MENSAJE #1
|  Topic     : noticias
|  ID        : b37ab9d5-...
|  Timestamp : 2026-07-06 20:35:00.456
|  Contenido : El dolar sube 0.5% hoy
+-----------------------------------------
```

> Nota: conectar siempre el consumer ANTES que el producer
> para que este suscrito cuando llegue el primer mensaje.