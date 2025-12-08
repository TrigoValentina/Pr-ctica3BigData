@echo off
REM Train regression models for all sensor types

echo ========================================
echo Training Regression Models
echo ========================================
echo.
echo This will train models for:
echo - EM500 (CO2, temperature, humidity, pressure)
echo - WS302 (LAeq, LAI, LAImax)
echo - EM310 (distance)
echo.
echo This may take several minutes...
echo.

docker exec spark-master python3 /opt/spark/app/ml/spark_ml_trainer_regression.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo [SUCCESS] Regression models trained!
    echo ========================================
    echo.
    echo Next steps:
    echo 1. Train classification model: run_ml_train_classification.bat
    echo 2. Generate predictions: run_ml_predict.bat
    echo 3. View results in dashboard
    echo.
) else (
    echo.
    echo [ERROR] Training failed. Check the logs above.
    echo.
)

pause
