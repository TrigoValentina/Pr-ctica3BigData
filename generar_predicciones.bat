@echo off
REM Genera predicciones realistas para un rango de fechas personalizado

echo ========================================
echo Generador de Predicciones Realistas
echo ========================================
echo.

REM Valores por defecto
set FECHA_INICIO=2025-11-29
set FECHA_FIN=2025-12-09

REM Si se proporcionan argumentos, usarlos
if not "%1"=="" set FECHA_INICIO=%1
if not "%2"=="" set FECHA_FIN=%2

echo Generando predicciones para:
echo   Fecha Inicio: %FECHA_INICIO%
echo   Fecha Fin: %FECHA_FIN%
echo.
echo Esto generará predicciones con:
echo   - Patrones horarios (día/noche)
echo   - Patrones semanales (laboral/fin de semana)
echo   - Tendencias naturales
echo   - Variación aleatoria realista
echo.

docker exec spark-master python3 /opt/spark/app/ml/quick_predictor.py %FECHA_INICIO% %FECHA_FIN%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo [SUCCESS] Predicciones generadas!
    echo ========================================
    echo.
    echo Ahora puedes:
    echo 1. Ir al dashboard (http://localhost:8501)
    echo 2. Sección: Machine Learning
    echo 3. Seleccionar rango: %FECHA_INICIO% a %FECHA_FIN%
    echo 4. Ver las predicciones con variación realista
    echo.
) else (
    echo.
    echo [ERROR] Hubo un problema generando predicciones
    echo.
)

pause
