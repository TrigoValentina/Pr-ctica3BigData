#!/usr/bin/env python3
# app/ml/spark_ml_trainer_regression.py
"""
Entrenador de modelos de regresión usando Spark MLlib
Entrena modelos para predecir valores continuos (CO2, temperatura, LAeq, distancia, etc.)
"""

import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, 
    lag, avg as spark_avg, stddev, count, lit
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import mysql.connector

# Importar configuración
sys.path.append('/opt/spark/app')
from ml.ml_config import (
    REGRESSION_CONFIG, DB_CONFIG, EVALUATION_CONFIG,
    get_model_version, get_model_path, get_jdbc_url, get_db_properties
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RegressionTrainer:
    def __init__(self, sensor_type):
        self.sensor_type = sensor_type
        self.config = REGRESSION_CONFIG[sensor_type]
        self.model_version = get_model_version()
        
        # Inicializar Spark
        self.spark = SparkSession.builder \
            .appName(f"ML_Regression_Trainer_{sensor_type}") \
            .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
            .getOrCreate()
        
        logger.info(f"🚀 Iniciando entrenador de regresión para {sensor_type}")
    
    def load_data(self):
        """Carga datos desde MySQL"""
        logger.info(f"📊 Cargando datos de tabla {self.config['table']}")
        
        jdbc_url = get_jdbc_url()
        db_props = get_db_properties()
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=self.config['table'],
            properties=db_props
        )
        
        logger.info(f"✅ Datos cargados: {df.count()} registros")
        return df
    
    def engineer_features(self, df, target_metric):
        """Ingeniería de features temporales"""
        logger.info(f"🔧 Generando features para métrica: {target_metric}")
        
        # Convertir time a timestamp
        df = df.withColumn("time", col("time").cast("timestamp"))
        
        # Features temporales
        df = df.withColumn("hour", hour(col("time")))
        df = df.withColumn("day_of_week", dayofweek(col("time")))
        df = df.withColumn("month", month(col("time")))
        df = df.withColumn("year", year(col("time")))
        
        # Convertir target a numérico
        df = df.withColumn(target_metric, col(target_metric).cast("double"))
        
        # Filtrar valores nulos
        df = df.filter(col(target_metric).isNotNull())
        
        # Window para lag features (ordenado por tiempo)
        window_spec = Window.partitionBy(self.config['device_column']).orderBy("time")
        
        # Lag features (valores históricos)
        for lag_val in self.config['features']['lag_features']:
            df = df.withColumn(
                f"{target_metric}_lag{lag_val}",
                lag(col(target_metric), lag_val).over(window_spec)
            )
        
        # Eliminar filas con lags nulos (primeras filas de cada dispositivo)
        for lag_val in self.config['features']['lag_features']:
            df = df.filter(col(f"{target_metric}_lag{lag_val}").isNotNull())
        
        # Codificar device_name como índice numérico
        if self.config['device_column'] in df.columns:
            indexer = StringIndexer(
                inputCol=self.config['device_column'],
                outputCol="device_index"
            )
            df = indexer.fit(df).transform(df)
        else:
            df = df.withColumn("device_index", lit(0))
        
        logger.info(f"✅ Features generados. Total registros: {df.count()}")
        return df
    
    def train_model(self, df, target_metric):
        """Entrena modelo de regresión"""
        logger.info(f"🤖 Entrenando modelo para {target_metric}")
        
        # Definir features
        feature_cols = ["hour", "day_of_week", "month", "device_index"]
        for lag_val in self.config['features']['lag_features']:
            feature_cols.append(f"{target_metric}_lag{lag_val}")
        
        # Ensamblar features en un vector
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Configurar modelo
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=target_metric,
            predictionCol="prediction",
            numTrees=self.config['hyperparameters']['numTrees'],
            maxDepth=self.config['hyperparameters']['maxDepth'],
            minInstancesPerNode=self.config['hyperparameters']['minInstancesPerNode'],
            seed=self.config['hyperparameters']['seed']
        )
        
        # Crear pipeline
        pipeline = Pipeline(stages=[assembler, rf])
        
        # Split train/test
        train_df, test_df = df.randomSplit(
            [EVALUATION_CONFIG['train_test_split'], 1 - EVALUATION_CONFIG['train_test_split']],
            seed=42
        )
        
        logger.info(f"📊 Train: {train_df.count()} | Test: {test_df.count()}")
        
        # Entrenar
        logger.info("⏳ Entrenando modelo...")
        model = pipeline.fit(train_df)
        logger.info("✅ Modelo entrenado")
        
        # Predecir en test set
        predictions = model.transform(test_df)
        
        # Evaluar
        metrics = self.evaluate_model(predictions, target_metric)
        
        # Guardar modelo
        model_path = get_model_path(self.sensor_type, "regression", self.model_version)
        model_path_metric = f"{model_path}/{target_metric}"
        
        logger.info(f"💾 Guardando modelo en {model_path_metric}")
        model.write().overwrite().save(model_path_metric)
        
        # Guardar metadata
        self.save_model_metadata(target_metric, train_df.count(), model_path_metric)
        
        # Guardar métricas
        self.save_metrics(target_metric, metrics, test_df.count())
        
        # Guardar predicciones
        self.save_predictions(predictions, target_metric)
        
        return model, metrics
    
    def evaluate_model(self, predictions, target_metric):
        """Evalúa el modelo y calcula métricas"""
        logger.info(f"📈 Evaluando modelo para {target_metric}")
        
        # R²
        r2_evaluator = RegressionEvaluator(
            labelCol=target_metric,
            predictionCol="prediction",
            metricName="r2"
        )
        r2 = r2_evaluator.evaluate(predictions)
        
        # RMSE
        rmse_evaluator = RegressionEvaluator(
            labelCol=target_metric,
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = rmse_evaluator.evaluate(predictions)
        
        # MAE
        mae_evaluator = RegressionEvaluator(
            labelCol=target_metric,
            predictionCol="prediction",
            metricName="mae"
        )
        mae = mae_evaluator.evaluate(predictions)
        
        metrics = {
            "r2": r2,
            "rmse": rmse,
            "mae": mae
        }
        
        logger.info(f"✅ Métricas - R²: {r2:.4f} | RMSE: {rmse:.4f} | MAE: {mae:.4f}")
        
        return metrics
    
    def save_model_metadata(self, target_metric, train_samples, model_path):
        """Guarda metadata del modelo en MySQL"""
        logger.info("💾 Guardando metadata del modelo")
        
        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cursor = conn.cursor()
            
            # Desactivar modelos anteriores
            cursor.execute("""
                UPDATE ml_models_metadata 
                SET is_active = FALSE 
                WHERE sensor_type = %s AND model_type = 'regression' AND target_metric = %s
            """, (self.sensor_type, target_metric))
            
            # Insertar nuevo modelo
            cursor.execute("""
                INSERT INTO ml_models_metadata 
                (sensor_type, model_type, model_version, target_metric, training_date, 
                 training_samples, model_path, hyperparameters_json, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.sensor_type,
                'regression',
                self.model_version,
                target_metric,
                datetime.now(),
                train_samples,
                model_path,
                str(self.config['hyperparameters']),
                True
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Metadata guardada")
        except Exception as e:
            logger.error(f"❌ Error guardando metadata: {e}")
    
    def save_metrics(self, target_metric, metrics, sample_count):
        """Guarda métricas de evaluación en MySQL"""
        logger.info("💾 Guardando métricas de evaluación")
        
        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cursor = conn.cursor()
            
            today = datetime.now().date()
            
            cursor.execute("""
                INSERT INTO ml_metrics_regression 
                (sensor_type, metric_name, model_version, date_from, date_to, 
                 r2_score, rmse, mae, sample_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.sensor_type,
                target_metric,
                self.model_version,
                today - timedelta(days=30),  # Últimos 30 días
                today,
                metrics['r2'],
                metrics['rmse'],
                metrics['mae'],
                sample_count
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Métricas guardadas")
        except Exception as e:
            logger.error(f"❌ Error guardando métricas: {e}")
    
    def save_predictions(self, predictions, target_metric):
        """Guarda predicciones en MySQL"""
        logger.info("💾 Guardando predicciones")
        
        try:
            # Seleccionar columnas relevantes
            device_col = self.config['device_column']
            
            pred_df = predictions.select(
                lit(self.sensor_type).alias("sensor_type"),
                col(device_col).alias("device_name"),
                col("time"),
                lit(target_metric).alias("metric_name"),
                col(target_metric).alias("real_value"),
                col("prediction").alias("predicted_value"),
                lit(self.model_version).alias("model_version")
            )
            
            # Guardar en MySQL
            jdbc_url = get_jdbc_url()
            db_props = get_db_properties()
            
            pred_df.write.jdbc(
                url=jdbc_url,
                table="ml_predictions_regression",
                mode="append",
                properties=db_props
            )
            
            logger.info(f"✅ {pred_df.count()} predicciones guardadas")
        except Exception as e:
            logger.error(f"❌ Error guardando predicciones: {e}")
    
    def run(self):
        """Ejecuta el flujo completo de entrenamiento"""
        try:
            # Cargar datos
            df = self.load_data()
            
            if df.count() < EVALUATION_CONFIG['min_samples_required']:
                logger.warning(f"⚠️ Insuficientes datos ({df.count()} < {EVALUATION_CONFIG['min_samples_required']})")
                return
            
            # Entrenar para cada métrica objetivo
            for target_metric in self.config['target_metrics']:
                logger.info(f"\n{'='*60}")
                logger.info(f"🎯 Procesando métrica: {target_metric}")
                logger.info(f"{'='*60}\n")
                
                # Feature engineering
                df_features = self.engineer_features(df, target_metric)
                
                if df_features.count() < EVALUATION_CONFIG['min_samples_required']:
                    logger.warning(f"⚠️ Insuficientes datos después de feature engineering")
                    continue
                
                # Entrenar modelo
                model, metrics = self.train_model(df_features, target_metric)
                
                logger.info(f"✅ Modelo para {target_metric} completado\n")
            
            logger.info(f"🎉 Entrenamiento completado para {self.sensor_type}")
            
        except Exception as e:
            logger.error(f"❌ Error en entrenamiento: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.spark.stop()

def main():
    """Punto de entrada principal"""
    logger.info("🚀 INICIANDO ENTRENAMIENTO DE MODELOS DE REGRESIÓN")
    
    # Entrenar para cada tipo de sensor
    for sensor_type in REGRESSION_CONFIG.keys():
        logger.info(f"\n{'#'*70}")
        logger.info(f"# SENSOR: {sensor_type}")
        logger.info(f"{'#'*70}\n")
        
        trainer = RegressionTrainer(sensor_type)
        trainer.run()
    
    logger.info("\n✅ TODOS LOS MODELOS DE REGRESIÓN ENTRENADOS")

if __name__ == "__main__":
    main()
