@echo off
REM Train classification model for EM310

echo ========================================
echo Training Classification Model (EM310)
echo ========================================
echo.
echo This will train the classification model for:
echo - EM310 Soterrados (Normal/Alerta/Critico)
echo.
echo This may take a few minutes...
echo.

docker exec spark-master python3 /opt/spark/app/ml/spark_ml_trainer_classification.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo [SUCCESS] Classification model trained!
    echo ========================================
    echo.
    echo Next steps:
    echo 1. Generate predictions: run_ml_predict.bat
    echo 2. View confusion matrix in dashboard
    echo.
) else (
    echo.
    echo [ERROR] Training failed. Check the logs above.
    echo.
)

pause
