@echo off
REM Generate predictions for the next 7 days

echo ========================================
echo Generating ML Predictions
echo ========================================
echo.
echo This will generate predictions for:
echo - Next 7 days
echo - All sensor types
echo - Both regression and classification
echo.

docker exec spark-master python3 /opt/spark/app/ml/spark_ml_predictor.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo [SUCCESS] Predictions generated!
    echo ========================================
    echo.
    echo You can now view predictions in the dashboard:
    echo - Navigate to "Machine Learning" section
    echo - Select RF-01 (Regression) or RF-02 (Classification)
    echo - Choose your date range and sensor type
    echo.
) else (
    echo.
    echo [ERROR] Prediction generation failed. Check the logs above.
    echo.
)

pause
