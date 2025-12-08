#!/bin/bash
# ML Prediction Scheduler - Genera predicciones cada hora

echo "========================================="
echo "ML Prediction Scheduler iniciado"
echo "Predicciones se generarán cada 1 hora"
echo "========================================="

while true; do
    echo ""
    echo "[$(date)] Generando predicciones..."
    
    python3 /opt/spark/app/ml/spark_ml_predictor.py
    
    if [ $? -eq 0 ]; then
        echo "[$(date)] ✅ Predicciones generadas exitosamente"
    else
        echo "[$(date)] ❌ Error generando predicciones"
    fi
    
    echo "[$(date)] Predicciones completadas. Próxima ejecución en 1 hora."
    
    # Esperar 1 hora (3600 segundos)
    sleep 3600
done
