# test-patrones.ps1 — Prueba los 3 patrones del Message Broker
$enc = New-Object System.Text.UTF8Encoding($false)
$ok = $true

function NewClient($role) {
    $c = New-Object System.Net.Sockets.TcpClient; $c.Connect("localhost", 9090)
    $s = $c.GetStream(); $s.ReadTimeout = 3000
    $w = New-Object System.IO.StreamWriter($s, $enc); $w.AutoFlush = $true
    $r = New-Object System.IO.StreamReader($s, $enc)
    $w.WriteLine($role)
    for ($i=0; $i -lt 4; $i++) { $r.ReadLine() | Out-Null }  # welcome messages
    return @{Client=$c; Writer=$w; Reader=$r}
}

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════"
Write-Host "   TEST 3 PATRONES — Message Broker Java 21"
Write-Host "═══════════════════════════════════════════════════════"

# ─── PATRON 1: Send and Forget ───────────────────────────────
Write-Host ""
Write-Host "[ PATRON 1 ] Send and Forget (Cola nombrada)"

$consumer1 = NewClient("CONSUMER")
$consumer1.Writer.WriteLine("QUEUE_SUBSCRIBE|pedidos"); Start-Sleep -m 400
$ack1 = $consumer1.Reader.ReadLine()
Write-Host "  Consumer suscrito: $ack1"

$producer1 = NewClient("PRODUCER")
$producer1.Writer.WriteLine("QUEUE_SEND|pedidos|Pedido #1001 - Laptop Dell"); Start-Sleep -m 400
$prodAck1 = $producer1.Reader.ReadLine()
Write-Host "  Producer ACK: $prodAck1"

$received1 = $consumer1.Reader.ReadLine()
Write-Host "  Consumer recibio: $received1"
$producer1.Client.Close(); $consumer1.Client.Close()
if ($received1 -like "QUEUE_MSG|pedidos|*") { Write-Host "  RESULTADO: EXITOSO" } else { Write-Host "  RESULTADO: FALLO"; $ok = $false }

# ─── PATRON 2: Request-Response ──────────────────────────────
Write-Host ""
Write-Host "[ PATRON 2 ] Request-Response"

$handler = NewClient("CONSUMER")
$handler.Writer.WriteLine("RR_READY"); Start-Sleep -m 400
$rrAck = $handler.Reader.ReadLine()
Write-Host "  Handler registrado: $rrAck"

$requester = NewClient("PRODUCER")
$corrId = [Guid]::NewGuid().ToString()
$requester.Writer.WriteLine("REQUEST|$corrId|Cuanto es 10 por 10?"); Start-Sleep -m 400
$reqAck = $requester.Reader.ReadLine()
Write-Host "  Request enviada: $reqAck"

$requestReceived = $handler.Reader.ReadLine()
Write-Host "  Handler recibio: $requestReceived"

if ($requestReceived -like "REQUEST_MSG|*") {
    $parts = $requestReceived -split "\|", 5
    $corrBack = $parts[1]
    $handler.Writer.WriteLine("RESPONSE|$corrBack|El resultado es 100"); Start-Sleep -m 400
    $responseDelivered = $requester.Reader.ReadLine()
    Write-Host "  Producer recibio: $responseDelivered"
    if ($responseDelivered -like "RESPONSE_MSG|*") { Write-Host "  RESULTADO: EXITOSO" } else { Write-Host "  RESULTADO: FALLO"; $ok = $false }
} else { Write-Host "  RESULTADO: FALLO (request no llego al handler)"; $ok = $false }
$requester.Client.Close(); $handler.Client.Close()

# ─── PATRON 3: Pub/Sub ───────────────────────────────────────
Write-Host ""
Write-Host "[ PATRON 3 ] Publicador / Suscriptor (Topic)"

$sub1 = NewClient("CONSUMER"); $sub1.Writer.WriteLine("TOPIC_SUBSCRIBE|noticias"); Start-Sleep -m 200; $sub1.Reader.ReadLine() | Out-Null
$sub2 = NewClient("CONSUMER"); $sub2.Writer.WriteLine("TOPIC_SUBSCRIBE|noticias"); Start-Sleep -m 200; $sub2.Reader.ReadLine() | Out-Null
Write-Host "  2 suscriptores conectados al topic 'noticias'"

$pub = NewClient("PRODUCER")
$pub.Writer.WriteLine("TOPIC_PUBLISH|noticias|Evento: El broker multi-patron funciona!"); Start-Sleep -m 500
$pubAck = $pub.Reader.ReadLine()
Write-Host "  Publisher ACK: $pubAck"

$sub1msg = $sub1.Reader.ReadLine()
$sub2msg = $sub2.Reader.ReadLine()
Write-Host "  Suscriptor-1 recibio: $sub1msg"
Write-Host "  Suscriptor-2 recibio: $sub2msg"
$pub.Client.Close(); $sub1.Client.Close(); $sub2.Client.Close()

if (($sub1msg -like "TOPIC_MSG|noticias|*") -and ($sub2msg -like "TOPIC_MSG|noticias|*")) {
    Write-Host "  RESULTADO: EXITOSO (ambos suscriptores recibieron el mensaje)"
} else { Write-Host "  RESULTADO: FALLO"; $ok = $false }

# ─── RESUMEN ─────────────────────────────────────────────────
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════"
if ($ok) { Write-Host "   TODOS LOS PATRONES: EXITOSOS" }
else     { Write-Host "   ALGUNOS PATRONES FALLARON" }
Write-Host "═══════════════════════════════════════════════════════"
Write-Host ""