# test-e2e.ps1 — Prueba end-to-end del Message Broker
# Ejecutar con: .\test-e2e.ps1
# Requisito: el broker debe estar corriendo en localhost:9090

$enc = New-Object System.Text.UTF8Encoding($false)

Write-Host ""
Write-Host "=== TEST END-TO-END: Message Broker Java 21 ==="
Write-Host ""

# 1. Conectar Consumer
$consumer = New-Object System.Net.Sockets.TcpClient
$consumer.Connect("localhost", 9090)
$cs = $consumer.GetStream()
$cs.ReadTimeout = 3000
$cw = New-Object System.IO.StreamWriter($cs, $enc)
$cw.AutoFlush = $true
$cr = New-Object System.IO.StreamReader($cs, $enc)
$cw.WriteLine("CONSUMER")
Start-Sleep -Milliseconds 500
$cAck = $cr.ReadLine()
Write-Host "[1] CONSUMER registrado : $cAck"

# 2. Conectar Producer
$producer = New-Object System.Net.Sockets.TcpClient
$producer.Connect("localhost", 9090)
$ps = $producer.GetStream()
$ps.ReadTimeout = 3000
$pw = New-Object System.IO.StreamWriter($ps, $enc)
$pw.AutoFlush = $true
$pr = New-Object System.IO.StreamReader($ps, $enc)
$pw.WriteLine("PRODUCER")
Start-Sleep -Milliseconds 500
$pAck = $pr.ReadLine()
Write-Host "[2] PRODUCER registrado : $pAck"

# 3. Publicar mensaje
$testMsg = "Hola Mundo - Diplomado Arquitectura - Modulo 02 - POC Broker Java 21"
$pw.WriteLine("PUBLISH|$testMsg")
Start-Sleep -Milliseconds 500
$pResp = $pr.ReadLine()
Write-Host "[3] BROKER ACK publish  : $pResp"

# 4. Consumer lee el mensaje entregado por el dispatcher
$delivered = $cr.ReadLine()
Write-Host "[4] CONSUMER recibio    : $delivered"

# Cleanup
$producer.Close()
$consumer.Close()

Write-Host ""
if ($delivered -like "MSG|*") {
    $parts = $delivered -split "\|", 4
    Write-Host "--- Mensaje parseado ---"
    Write-Host "  Tipo      : $($parts[0])"
    Write-Host "  ID        : $($parts[1])"
    Write-Host "  Timestamp : $($parts[2])"
    Write-Host "  Contenido : $($parts[3])"
    Write-Host ""
    Write-Host "*** RESULTADO: EXITOSO - El mensaje viajo Producer -> Broker -> Consumer ***"
} else {
    Write-Host "*** RESULTADO: FALLO - El mensaje no llego al consumer ***"
}
Write-Host ""
