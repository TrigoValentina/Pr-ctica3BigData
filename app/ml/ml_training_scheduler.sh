#!/bin/bash
# ML Training Scheduler - Ejecuta entrenamiento diario

echo "========================================="
echo "ML Training Scheduler iniciado"
echo "Entrenamiento se ejecutará cada 24 horas"
echo "========================================="

while true; do
    echo ""
    echo "[$(date)] Iniciando entrenamiento de modelos..."
    
    # Entrenar modelos de regresión
    echo "[$(date)] Entrenando modelos de regresión..."
    python3 /opt/spark/app/ml/spark_ml_trainer_regression.py
    
    if [ $? -eq 0 ]; then
        echo "[$(date)] ✅ Modelos de regresión entrenados exitosamente"
    else
        echo "[$(date)] ❌ Error entrenando modelos de regresión"
    fi
    
    # Pequeña pausa
    sleep 5
    
    # Entrenar modelo de clasificación
    echo "[$(date)] Entrenando modelo de clasificación..."
    python3 /opt/spark/app/ml/spark_ml_trainer_classification.py
    
    if [ $? -eq 0 ]; then
        echo "[$(date)] ✅ Modelo de clasificación entrenado exitosamente"
    else
        echo "[$(date)] ❌ Error entrenando modelo de clasificación"
    fi
    
    echo "[$(date)] Entrenamiento completado. Próximo entrenamiento en 24 horas."
    
    # Esperar 24 horas (86400 segundos)
    sleep 86400
done
