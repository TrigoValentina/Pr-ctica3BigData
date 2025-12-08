#!/usr/bin/env python3
# app/ml/spark_ml_predictor.py
"""
Servicio de predicción continua usando modelos ML entrenados
Genera predicciones para fechas futuras
"""

import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hour, dayofweek, month
from pyspark.ml import PipelineModel
import mysql.connector

# Importar configuración
sys.path.append('/opt/spark/app')
from ml.ml_config import (
    REGRESSION_CONFIG, CLASSIFICATION_CONFIG, DB_CONFIG, STORAGE_CONFIG,
    get_jdbc_url, get_db_properties
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MLPredictor:
    def __init__(self):
        # Inicializar Spark
        self.spark = SparkSession.builder \
            .appName("ML_Predictor") \
            .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
            .getOrCreate()
        
        logger.info("🚀 Iniciando servicio de predicción ML")
    
    def get_active_model_info(self, sensor_type, model_type, target_metric=None):
        """Obtiene información del modelo activo desde MySQL"""
        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cursor = conn.cursor(dictionary=True)
            
            if target_metric:
                cursor.execute("""
                    SELECT * FROM ml_models_metadata 
                    WHERE sensor_type = %s 
                      AND model_type = %s 
                      AND target_metric = %s
                      AND is_active = TRUE
                    ORDER BY training_date DESC 
                    LIMIT 1
                """, (sensor_type, model_type, target_metric))
            else:
                cursor.execute("""
                    SELECT * FROM ml_models_metadata 
                    WHERE sensor_type = %s 
                      AND model_type = %s 
                      AND is_active = TRUE
                    ORDER BY training_date DESC 
                    LIMIT 1
                """, (sensor_type, model_type))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return result
        except Exception as e:
            logger.error(f"❌ Error obteniendo info del modelo: {e}")
            return None
    
    def generate_future_dates(self, days_ahead=7):
        """Genera fechas futuras para predicción"""
        logger.info(f"📅 Generando fechas futuras ({days_ahead} días)")
        
        future_dates = []
        start_date = datetime.now()
        
        for i in range(1, days_ahead + 1):
            future_date = start_date + timedelta(days=i)
            # Generar varias horas por día
            for hour_val in [0, 6, 12, 18]:
                dt = future_date.replace(hour=hour_val, minute=0, second=0, microsecond=0)
                future_dates.append(dt)
        
        return future_dates
    
    def predict_regression(self, sensor_type, target_metric):
        """Realiza predicciones de regresión"""
        logger.info(f"🔮 Prediciendo {target_metric} para {sensor_type}")
        
        # Obtener modelo activo
        model_info = self.get_active_model_info(sensor_type, 'regression', target_metric)
        
        if not model_info:
            logger.warning(f"⚠️ No hay modelo activo para {sensor_type}/{target_metric}")
            return
        
        model_path = model_info['model_path']
        model_version = model_info['model_version']
        
        logger.info(f"📂 Cargando modelo desde {model_path}")
        
        try:
            model = PipelineModel.load(model_path)
        except Exception as e:
            logger.error(f"❌ Error cargando modelo: {e}")
            return
        
        # Generar fechas futuras
        future_dates = self.generate_future_dates(STORAGE_CONFIG['predictions_days_ahead'])
        
        # Obtener último valor real para lag features
        config = REGRESSION_CONFIG[sensor_type]
        jdbc_url = get_jdbc_url()
        db_props = get_db_properties()
        
        # Leer últimos valores reales
        last_values_df = self.spark.read.jdbc(
            url=jdbc_url,
            table=config['table'],
            properties=db_props
        )
        
        last_values_df = last_values_df.select(
            col(config['device_column']).alias("device_name"),
            col(target_metric).cast("double")
        ).filter(col(target_metric).isNotNull())
        
        # Obtener dispositivos únicos
        devices = [row['device_name'] for row in last_values_df.select("device_name").distinct().collect()]
        
        if not devices:
            logger.warning(f"⚠️ No hay dispositivos con datos para {sensor_type}")
            return
        
        # Calcular último valor promedio por dispositivo (para lag features)
        device_last_values = {}
        for device in devices:
            last_val = last_values_df.filter(col("device_name") == device) \
                .agg({target_metric: "avg"}) \
                .collect()[0][0]
            device_last_values[device] = last_val if last_val else 0
        
        # Crear DataFrame de predicción para cada dispositivo
        predictions_data = []
        
        for device in devices[:3]:  # Limitar a 3 dispositivos para no sobrecargar
            last_val = device_last_values.get(device, 0)
            
            for dt in future_dates:
                predictions_data.append({
                    "device_name": device,
                    "time": dt,
                    "hour": dt.hour,
                    "day_of_week": dt.isoweekday(),
                    "month": dt.month,
                    "device_index": 0.0,  # Simplificado
                    f"{target_metric}_lag1": last_val,
                    f"{target_metric}_lag2": last_val,
                    f"{target_metric}_lag3": last_val
                })
        
        if not predictions_data:
            logger.warning("⚠️ No se generaron datos de predicción")
            return
        
        future_df = self.spark.createDataFrame(predictions_data)
        
        # Realizar predicción
        predictions = model.transform(future_df)
        
        # Preparar para guardar
        pred_df = predictions.select(
            lit(sensor_type).alias("sensor_type"),
            col("device_name"),
            col("time"),
            lit(target_metric).alias("metric_name"),
            lit(None).cast("double").alias("real_value"),  # Futuro = sin valor real
            col("prediction").alias("predicted_value"),
            lit(model_version).alias("model_version")
        )
        
        # Guardar predicciones
        try:
            pred_df.write.jdbc(
                url=jdbc_url,
                table="ml_predictions_regression",
                mode="append",
                properties=db_props
            )
            logger.info(f"✅ {pred_df.count()} predicciones guardadas para {target_metric}")
        except Exception as e:
            logger.error(f"❌ Error guardando predicciones: {e}")
    
    def predict_classification(self):
        """Realiza predicciones de clasificación"""
        sensor_type = "em310_soterrados"
        logger.info(f"🔮 Prediciendo clasificación para {sensor_type}")
        
        # Obtener modelo activo
        model_info = self.get_active_model_info(sensor_type, 'classification')
        
        if not model_info:
            logger.warning(f"⚠️ No hay modelo activo de clasificación")
            return
        
        model_path = model_info['model_path']
        model_version = model_info['model_version']
        
        logger.info(f"📂 Cargando modelo desde {model_path}")
        
        try:
            model = PipelineModel.load(model_path)
        except Exception as e:
            logger.error(f"❌ Error cargando modelo: {e}")
            return
        
        # Generar fechas futuras
        future_dates = self.generate_future_dates(STORAGE_CONFIG['predictions_days_ahead'])
        
        # Crear datos sintéticos para predicción
        # En producción, esto vendría de sensores reales
        config = CLASSIFICATION_CONFIG[sensor_type]
        
        predictions_data = []
        for i, dt in enumerate(future_dates):
            # Simular distancia basada en patrón (esto es simplificado)
            distance = 50.0 + (i % 10) * 5.0
            
            predictions_data.append({
                "device_name": "EM310-001",
                "time": dt,
                "distance": distance,
                "status_encoded": 1.0,  # OK
                "hour": dt.hour,
                "day_of_week": dt.isoweekday(),
                "month": dt.month,
                "device_index": 0.0
            })
        
        future_df = self.spark.createDataFrame(predictions_data)
        
        # Realizar predicción
        predictions = model.transform(future_df)
        
        # Mapeo de clases
        class_mapping = {0: "Normal", 1: "Alerta", 2: "Crítico"}
        
        # Preparar para guardar
        from pyspark.sql.types import StringType
        from pyspark.sql.functions import udf
        
        def map_class(pred):
            return class_mapping.get(int(pred), "Desconocido")
        
        map_class_udf = udf(map_class, StringType())
        
        pred_df = predictions.select(
            lit(sensor_type).alias("sensor_type"),
            col("device_name"),
            col("time"),
            lit(None).cast("string").alias("real_class"),
            map_class_udf(col("prediction")).alias("predicted_class"),
            lit(1.0).alias("confidence"),
            lit(model_version).alias("model_version")
        )
        
        # Guardar predicciones
        jdbc_url = get_jdbc_url()
        db_props = get_db_properties()
        
        try:
            pred_df.write.jdbc(
                url=jdbc_url,
                table="ml_predictions_classification",
                mode="append",
                properties=db_props
            )
            logger.info(f"✅ {pred_df.count()} predicciones de clasificación guardadas")
        except Exception as e:
            logger.error(f"❌ Error guardando predicciones: {e}")
    
    def run(self):
        """Ejecuta el flujo completo de predicción"""
        try:
            logger.info("\n" + "="*70)
            logger.info("GENERANDO PREDICCIONES")
            logger.info("="*70 + "\n")
            
            # Predicciones de regresión
            for sensor_type, config in REGRESSION_CONFIG.items():
                for target_metric in config['target_metrics']:
                    self.predict_regression(sensor_type, target_metric)
            
            # Predicciones de clasificación
            self.predict_classification()
            
            logger.info("\n✅ TODAS LAS PREDICCIONES COMPLETADAS")
            
        except Exception as e:
            logger.error(f"❌ Error en predicción: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.spark.stop()

def main():
    """Punto de entrada principal"""
    logger.info("🚀 INICIANDO SERVICIO DE PREDICCIÓN ML")
    
    predictor = MLPredictor()
    predictor.run()

if __name__ == "__main__":
    main()
