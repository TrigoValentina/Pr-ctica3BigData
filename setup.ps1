# Script de instalación automática para Windows (PowerShell)
# Este script prepara e inicializa todo el proyecto BigData

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  INSTALACIÓN AUTOMÁTICA DEL PROYECTO" -ForegroundColor Cyan
Write-Host "  BigData - Kafka + Spark + MySQL" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Función para verificar comandos
function Test-Command {
    param($Command)
    $null = Get-Command $Command -ErrorAction SilentlyContinue
    return $?
}

# 1. Verificar Docker
Write-Host "[1/8] Verificando Docker..." -ForegroundColor Yellow
if (-not (Test-Command "docker")) {
    Write-Host "❌ Docker no está instalado. Por favor instala Docker Desktop desde:" -ForegroundColor Red
    Write-Host "   https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}
Write-Host "✅ Docker encontrado" -ForegroundColor Green

# Verificar que Docker esté corriendo
try {
    docker ps | Out-Null
    Write-Host "✅ Docker está corriendo" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker no está corriendo. Por favor inicia Docker Desktop." -ForegroundColor Red
    exit 1
}

# 2. Verificar Docker Compose
Write-Host "[2/8] Verificando Docker Compose..." -ForegroundColor Yellow
if (-not (Test-Command "docker-compose")) {
    Write-Host "⚠️  docker-compose no encontrado, intentando con 'docker compose'..." -ForegroundColor Yellow
    $dockerComposeCmd = "docker compose"
} else {
    $dockerComposeCmd = "docker-compose"
}
Write-Host "✅ Docker Compose disponible" -ForegroundColor Green

# 3. Verificar archivos CSV
Write-Host "[3/8] Verificando archivos CSV en data/..." -ForegroundColor Yellow
$csvFiles = @(
    "data\EM310-UDL-915M soterrados nov 2024.csv",
    "data\EM500-CO2-915M nov 2024.csv",
    "data\WS302-915M SONIDO NOV 2024.csv"
)

$missingFiles = @()
foreach ($file in $csvFiles) {
    if (-not (Test-Path $file)) {
        $missingFiles += $file
        Write-Host "⚠️  No encontrado: $file" -ForegroundColor Yellow
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "⚠️  Algunos archivos CSV no se encontraron. El proyecto funcionará pero no habrá datos para procesar." -ForegroundColor Yellow
    Write-Host "   Asegúrate de colocar los archivos CSV en la carpeta data/ con los nombres exactos:" -ForegroundColor Yellow
    foreach ($file in $missingFiles) {
        Write-Host "   - $file" -ForegroundColor Yellow
    }
} else {
    Write-Host "✅ Todos los archivos CSV encontrados" -ForegroundColor Green
}

# 4. Detener contenedores existentes
Write-Host "[4/8] Limpiando contenedores existentes..." -ForegroundColor Yellow
Invoke-Expression "$dockerComposeCmd down --volumes --remove-orphans 2>&1 | Out-Null"
Write-Host "✅ Limpieza completada" -ForegroundColor Green

# 5. Construir imágenes
Write-Host "[5/8] Construyendo imágenes Docker..." -ForegroundColor Yellow
Write-Host "   Esto puede tardar varios minutos la primera vez..." -ForegroundColor Gray
$buildResult = Invoke-Expression "$dockerComposeCmd build 2>&1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error al construir las imágenes:" -ForegroundColor Red
    Write-Host $buildResult -ForegroundColor Red
    exit 1
}
Write-Host "✅ Imágenes construidas correctamente" -ForegroundColor Green

# 6. Levantar contenedores
Write-Host "[6/8] Levantando contenedores..." -ForegroundColor Yellow
$upResult = Invoke-Expression "$dockerComposeCmd up -d 2>&1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error al levantar los contenedores:" -ForegroundColor Red
    Write-Host $upResult -ForegroundColor Red
    exit 1
}
Write-Host "✅ Contenedores levantados" -ForegroundColor Green

# 7. Esperar a que MySQL esté listo
Write-Host "[7/8] Esperando a que MySQL esté listo..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$mysqlReady = $false

while ($attempt -lt $maxAttempts -and -not $mysqlReady) {
    Start-Sleep -Seconds 2
    $attempt++
    try {
        $result = docker exec mysql mysql -uroot -p"Os51t=Ag/3=B" -e "SELECT 1;" 2>&1
        if ($LASTEXITCODE -eq 0) {
            $mysqlReady = $true
            Write-Host "✅ MySQL está listo (intento $attempt/$maxAttempts)" -ForegroundColor Green
        } else {
            Write-Host "   Esperando MySQL... (intento $attempt/$maxAttempts)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "   Esperando MySQL... (intento $attempt/$maxAttempts)" -ForegroundColor Gray
    }
}

if (-not $mysqlReady) {
    Write-Host "⚠️  MySQL no respondió después de $maxAttempts intentos. Continuando de todas formas..." -ForegroundColor Yellow
} else {
    # Esperar un poco más para que el script init.sql termine
    Write-Host "   Esperando inicialización de tablas..." -ForegroundColor Gray
    Start-Sleep -Seconds 5
}

# 8. Verificar estado de contenedores
Write-Host "[8/8] Verificando estado de contenedores..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}"
$expectedContainers = @("zookeeper", "kafka", "spark-master", "spark-worker", "mysql", "mongodb", "spark-consumer-em310", "spark-consumer-em500", "spark-consumer-ws302")

$runningContainers = @()
foreach ($container in $containers) {
    $runningContainers += $container
}

$missingContainers = @()
foreach ($expected in $expectedContainers) {
    if ($runningContainers -notcontains $expected) {
        $missingContainers += $expected
    }
}

if ($missingContainers.Count -gt 0) {
    Write-Host "⚠️  Algunos contenedores no están corriendo:" -ForegroundColor Yellow
    foreach ($container in $missingContainers) {
        Write-Host "   - $container" -ForegroundColor Yellow
    }
    Write-Host "   Revisa los logs con: docker logs $($missingContainers[0])" -ForegroundColor Yellow
} else {
    Write-Host "✅ Todos los contenedores están corriendo" -ForegroundColor Green
}

# Mostrar estado
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ESTADO DE LOS SERVICIOS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
Write-Host ""

# Preguntar si ejecutar los producers
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ¿EJECUTAR LOS PRODUCERS?" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Los producers envían los datos de los CSV a Kafka." -ForegroundColor Gray
Write-Host ""

$response = Read-Host "¿Deseas ejecutar los producers ahora? (S/N)"
if ($response -eq "S" -or $response -eq "s" -or $response -eq "Y" -or $response -eq "y") {
    Write-Host ""
    Write-Host "Ejecutando producers..." -ForegroundColor Yellow
    
    Write-Host "  → Producer EM310 (soterrados)..." -ForegroundColor Gray
    docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em310.py
    
    Start-Sleep -Seconds 2
    
    Write-Host "  → Producer EM500 (calidad del aire)..." -ForegroundColor Gray
    docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em500.py
    
    Start-Sleep -Seconds 2
    
    Write-Host "  → Producer WS302 (sonido)..." -ForegroundColor Gray
    docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_ws302.py
    
    Write-Host ""
    Write-Host "✅ Producers ejecutados en segundo plano" -ForegroundColor Green
    Write-Host "   Puedes revisar los logs con: docker logs spark-master" -ForegroundColor Gray
    Write-Host "   Los consumers procesarán los datos automáticamente." -ForegroundColor Gray
}

# Verificar Python para el dashboard
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CONFIGURACIÓN DEL DASHBOARD" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if (Test-Command "python") {
    $pythonVersion = python --version 2>&1
    Write-Host "✅ Python encontrado: $pythonVersion" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "Para ejecutar el dashboard:" -ForegroundColor Yellow
    Write-Host "  1. cd dashboards" -ForegroundColor Gray
    Write-Host "  2. python -m venv .venv" -ForegroundColor Gray
    Write-Host "  3. .venv\Scripts\activate" -ForegroundColor Gray
    Write-Host "  4. pip install streamlit pandas mysql-connector-python plotly pymongo" -ForegroundColor Gray
    Write-Host "  5. streamlit run dashboard.py" -ForegroundColor Gray
    Write-Host ""
    Write-Host "O simplemente ejecuta: .\dashboards\run_dashboard.bat" -ForegroundColor Cyan
} else {
    Write-Host "⚠️  Python no encontrado. El dashboard requiere Python 3.10+" -ForegroundColor Yellow
    Write-Host "   Descarga desde: https://www.python.org/downloads/" -ForegroundColor Gray
}

# Resumen final
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✅ INSTALACIÓN COMPLETADA" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Servicios disponibles:" -ForegroundColor Cyan
Write-Host "  • Kafka: localhost:29092" -ForegroundColor Gray
Write-Host "  • MySQL: localhost:3307 (usuario: root, password: Os51t=Ag/3=B)" -ForegroundColor Gray
Write-Host "  • MongoDB: localhost:27017" -ForegroundColor Gray
Write-Host "  • Spark Master UI: http://localhost:18080" -ForegroundColor Gray
Write-Host ""
Write-Host "Comandos útiles:" -ForegroundColor Cyan
Write-Host "  • Ver logs: docker compose logs -f [nombre-servicio]" -ForegroundColor Gray
Write-Host "  • Detener todo: docker compose down" -ForegroundColor Gray
Write-Host "  • Reiniciar: docker compose restart" -ForegroundColor Gray
Write-Host ""
Write-Host "¡Listo para usar! 🚀" -ForegroundColor Green

