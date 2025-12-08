@echo off
REM ========================================
REM SCRIPT MAESTRO - Sistema Big Data + ML
REM ========================================
REM
REM Este script hace TODO:
REM 1. Detecta si es primera instalacion o ya existe
REM 2. Si es primera vez -> Instala todo desde cero
REM 3. Si ya existe -> Solo inicia el sistema
REM 4. Verifica el estado final
REM
REM Uso: ejecutar directamente, sin parametros
REM ========================================

echo.
echo ========================================
echo   Sistema Big Data + Machine Learning
echo ========================================
echo.

REM Verificar si Docker esta corriendo
docker --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker no esta instalado o no esta corriendo
    echo.
    echo Por favor:
    echo 1. Instala Docker Desktop
    echo 2. Inicia Docker Desktop
    echo 3. Ejecuta este script de nuevo
    echo.
    pause
    exit /b 1
)

echo [OK] Docker esta corriendo
echo.

REM Detectar si es primera instalacion o ya existe
echo Detectando estado del sistema...
echo.

REM Verificar si existen contenedores del proyecto
docker ps -a --filter "name=mysql" --filter "name=kafka" --filter "name=spark-master" --format "{{.Names}}" | findstr /C:"mysql" >nul 2>&1

if %ERRORLEVEL% EQU 0 (
    REM Ya existe instalacion
    goto :INICIAR_EXISTENTE
) else (
    REM Primera instalacion
    goto :INSTALACION_COMPLETA
)

:INSTALACION_COMPLETA
echo ========================================
echo   INSTALACION COMPLETA DESDE CERO
echo ========================================
echo.
echo Esto tomara aproximadamente 5-10 minutos
echo.
echo Pasos que se realizaran:
echo 1. Levantar contenedores Docker (12 servicios)
echo 2. Esperar a que MySQL este listo
echo 3. Crear tablas de sensores (4 tablas)
echo 4. Crear tablas ML (5 tablas)
echo 5. Verificar estado
echo.
pause

echo.
echo [1/5] Levantando contenedores Docker...
docker-compose up -d

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Fallo al levantar contenedores
    pause
    exit /b 1
)

echo [OK] Contenedores levantados
echo.

echo [2/5] Esperando 30 segundos para que MySQL este listo...
timeout /t 30 /nobreak

echo.
echo [3/5] Creando tablas de sensores...
powershell -Command "Get-Content sql\init.sql | docker exec -i mysql mysql -uroot -p'Os51t=Ag/3=B' emergentETLVALENTINA"

if %ERRORLEVEL% NEQ 0 (
    echo [ADVERTENCIA] Hubo un problema creando tablas de sensores
    echo Es posible que ya existan o que MySQL no este listo aun
    echo Continuar...
)

echo [OK] Tablas de sensores creadas
echo.

echo [4/5] Creando tablas ML...
powershell -Command "Get-Content sql\init_ml_tables.sql | docker exec -i mysql mysql -uroot -p'Os51t=Ag/3=B' emergentETLVALENTINA"

if %ERRORLEVEL% NEQ 0 (
    echo [ADVERTENCIA] Hubo un problema creando tablas ML
    echo Es posible que ya existan
    echo Continuar...
)

echo [OK] Tablas ML creadas
echo.

echo [5/5] Verificando estado del sistema...
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr /V "NAMES"

echo.
echo ========================================
echo   INSTALACION COMPLETA EXITOSA
echo ========================================
echo.
echo Proximos pasos:
echo.
echo 1. Cargar datos iniciales (OPCIONAL, solo primera vez):
echo    - Ejecutar: run_ml_train_regression.bat
echo    - Ejecutar: run_ml_train_classification.bat
echo.
echo 2. Acceder a los dashboards:
echo    Dashboard Principal: http://localhost:8501
echo    Dashboard Alternativo: http://localhost:8502
echo    Usuario: Oscar
echo    Password: 1234Huicho
echo.
echo 3. Ir a seccion: Machine Learning (Dashboard Principal)
echo    - RF-01: Regresion (predicciones automaticas)
echo    - RF-02: Clasificacion (matriz de confusion)
echo.
echo El sistema generara predicciones automaticamente cuando las necesites.
echo.
echo [!] Los servicios ML (ml-trainer, ml-predictor) corren automaticamente
echo     en segundo plano. No requieren intervencion manual.
echo.
echo.
echo [INFO] Iniciando Dashboard Alternativo en puerto 8502...
start "Dashboard2 - UI Alternativa" powershell -NoExit -Command "cd '%~dp0dashboards'; streamlit run dashboard2.py --server.port 8502 --server.address localhost"
echo [OK] Dashboard2 iniciado en nueva ventana
echo.
goto :FIN

:INICIAR_EXISTENTE
echo ========================================
echo   SISTEMA YA INSTALADO - INICIANDO
echo ========================================
echo.
echo El sistema ya esta instalado.
echo Iniciando todos los servicios...
echo.

docker-compose up -d

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Fallo al iniciar contenedores
    echo.
    echo Intenta:
    echo 1. docker-compose down
    echo 2. Ejecuta este script de nuevo
    echo.
    pause
    exit /b 1
)

echo.
echo [OK] Sistema iniciado
echo.
echo Verificando estado...
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr /V "NAMES"

echo.
echo ========================================
echo   SISTEMA LISTO
echo ========================================
echo.
echo Dashboard Principal: http://localhost:8501
echo Dashboard Alternativo: http://localhost:8502
echo Usuario: Oscar
echo Password: 1234Huicho
echo.
echo Ir a: Machine Learning
echo   - RF-01: Regresion dinámica
echo   - RF-02: Matriz de confusion
echo.
echo.

echo.
goto :FIN

:FIN
echo.
echo Para ver logs de servicios ML:
echo   docker logs -f ml-trainer
echo   docker logs -f ml-predictor
echo.
echo Para generar predicciones manualmente:
echo   generar_predicciones.bat [fecha_inicio] [fecha_fin]
echo.
echo Para detener el sistema:
echo   docker-compose down
echo.
pause
