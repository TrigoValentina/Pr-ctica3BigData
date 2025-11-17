@echo off
REM Script de instalación automática para Windows (CMD)
REM Este script prepara e inicializa todo el proyecto BigData

echo ========================================
echo   INSTALACION AUTOMATICA DEL PROYECTO
echo   BigData - Kafka + Spark + MySQL
echo ========================================
echo.

REM Verificar Docker
echo [1/8] Verificando Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no esta instalado. Por favor instala Docker Desktop desde:
    echo    https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)
echo [OK] Docker encontrado

REM Verificar que Docker esté corriendo
docker ps >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker no esta corriendo. Por favor inicia Docker Desktop.
    pause
    exit /b 1
)
echo [OK] Docker esta corriendo

REM Verificar Docker Compose
echo [2/8] Verificando Docker Compose...
docker-compose --version >nul 2>&1
if errorlevel 1 (
    docker compose version >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Docker Compose no encontrado.
        pause
        exit /b 1
    )
    set DOCKER_COMPOSE_CMD=docker compose
) else (
    set DOCKER_COMPOSE_CMD=docker-compose
)
echo [OK] Docker Compose disponible

REM Verificar archivos CSV
echo [3/8] Verificando archivos CSV en data/...
if not exist "data\EM310-UDL-915M soterrados nov 2024.csv" (
    echo [ADVERTENCIA] No encontrado: EM310-UDL-915M soterrados nov 2024.csv
)
if not exist "data\EM500-CO2-915M nov 2024.csv" (
    echo [ADVERTENCIA] No encontrado: EM500-CO2-915M nov 2024.csv
)
if not exist "data\WS302-915M SONIDO NOV 2024.csv" (
    echo [ADVERTENCIA] No encontrado: WS302-915M SONIDO NOV 2024.csv
)
echo [OK] Verificacion de CSV completada

REM Limpiar contenedores
echo [4/8] Limpiando contenedores existentes...
%DOCKER_COMPOSE_CMD% down --volumes --remove-orphans >nul 2>&1
echo [OK] Limpieza completada

REM Construir imágenes
echo [5/8] Construyendo imagenes Docker...
echo    Esto puede tardar varios minutos la primera vez...
%DOCKER_COMPOSE_CMD% build
if errorlevel 1 (
    echo [ERROR] Error al construir las imagenes
    pause
    exit /b 1
)
echo [OK] Imagenes construidas correctamente

REM Levantar contenedores
echo [6/8] Levantando contenedores...
%DOCKER_COMPOSE_CMD% up -d
if errorlevel 1 (
    echo [ERROR] Error al levantar los contenedores
    pause
    exit /b 1
)
echo [OK] Contenedores levantados

REM Esperar a MySQL
echo [7/8] Esperando a que MySQL este listo...
set /a ATTEMPT=0
set /a MAX_ATTEMPTS=30
:MYSQL_LOOP
set /a ATTEMPT+=1
timeout /t 2 /nobreak >nul
docker exec mysql mysql -uroot -p"Os51t=Ag/3=B" -e "SELECT 1;" >nul 2>&1
if not errorlevel 1 (
    echo [OK] MySQL esta listo (intento %ATTEMPT%/%MAX_ATTEMPTS%)
    goto MYSQL_READY
)
if %ATTEMPT% geq %MAX_ATTEMPTS% (
    echo [ADVERTENCIA] MySQL no respondio despues de %MAX_ATTEMPTS% intentos. Continuando...
    goto MYSQL_READY
)
echo    Esperando MySQL... (intento %ATTEMPT%/%MAX_ATTEMPTS%)
goto MYSQL_LOOP
:MYSQL_READY
timeout /t 5 /nobreak >nul
echo    Esperando inicializacion de tablas...

REM Verificar contenedores
echo [8/8] Verificando estado de contenedores...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.

REM Preguntar por producers
echo ========================================
echo   EJECUTAR LOS PRODUCERS?
echo ========================================
echo Los producers envian los datos de los CSV a Kafka.
echo.
goto EjecutarProducers

:EjecutarProducers
echo.
echo Ejecutando producers...
echo   Producer EM310 (soterrados)...
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em310.py
timeout /t 2 /nobreak >nul

echo   Producer EM500 (calidad del aire)...
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em500.py
timeout /t 2 /nobreak >nul

echo   Producer WS302 (sonido)...
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_ws302.py
echo.
echo [OK] Producers ejecutados en segundo plano
echo   Puedes revisar los logs con: docker logs spark-master


REM Verificar Python
echo.
echo ========================================
echo   CONFIGURACION DEL DASHBOARD
echo ========================================
python --version >nul 2>&1
if not errorlevel 1 (
    echo [OK] Python encontrado
    echo.
    echo Para ejecutar el dashboard:
    echo   1. cd dashboards
    echo   2. python -m venv .venv
    echo   3. .venv\Scripts\activate
    echo   4. pip install streamlit pandas mysql-connector-python plotly pymongo
    echo   5. streamlit run dashboard.py
    echo.
    echo O simplemente ejecuta: dashboards\run_dashboard.bat
) else (
    echo [ADVERTENCIA] Python no encontrado. El dashboard requiere Python 3.10+
    echo    Descarga desde: https://www.python.org/downloads/
)

REM Resumen
echo.
echo ========================================
echo   INSTALACION COMPLETADA
echo ========================================
echo.
echo Servicios disponibles:
echo   Kafka: localhost:29092
echo   MySQL: localhost:3307 (usuario: root, password: Os51t=Ag/3=B)
echo   MongoDB: localhost:27017
echo   Spark Master UI: http://localhost:18080
echo.
echo Comandos utiles:
echo   Ver logs: docker compose logs -f [nombre-servicio]
echo   Detener todo: docker compose down
echo   Reiniciar: docker compose restart
echo.
echo Listo para usar!
echo.
pause

