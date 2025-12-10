#!/usr/bin/env python3
# app/ml/spark_ml_trainer_classification.py
"""
Entrenador de modelos de clasificación usando Spark MLlib
Clasifica el nivel de contaminación/alerta basado en sensores soterrados (EM310)
"""

import sys
import logging
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, when, 
    lit, udf
)
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
import mysql.connector
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
# Importar configuración
sys.path.append('/opt/spark/app')
from ml.ml_config import (
    CLASSIFICATION_CONFIG, DB_CONFIG, EVALUATION_CONFIG,
    get_model_version, get_model_path, get_jdbc_url, get_db_properties
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
get_confidence_udf = udf(lambda probs: float(max(probs)), DoubleType())

class ClassificationTrainer:
    def __init__(self):
        self.sensor_type = "em310_soterrados"
        self.config = CLASSIFICATION_CONFIG[self.sensor_type]
        self.model_version = get_model_version()
        
        # Inicializar Spark
        self.spark = SparkSession.builder \
            .appName(f"ML_Classification_Trainer_{self.sensor_type}") \
            .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
            .getOrCreate()
        
        logger.info(f"🚀 Iniciando entrenador de clasificación para {self.sensor_type}")
    
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
    
    def create_labels(self, df):
        """Crea etiquetas de clasificación basadas en reglas de negocio"""
        logger.info("🏷️ Generando etiquetas de clasificación")
        
        # Convertir distance a numérico
        df = df.withColumn("distance", col("distance").cast("double"))
        
        # Filtrar valores nulos
        df = df.filter(col("distance").isNotNull())
        
        # Reglas de clasificación:
        # - Crítico (2): distance <= 30 cm O status == "ERROR"
        # - Alerta (1): 30 < distance <= 100 cm
        # - Normal (0): distance > 100 cm Y status != "ERROR"
        
        thresholds = self.config['thresholds']
        
        df = df.withColumn(
            "class_label",
            when(
                (col("distance") <= thresholds['distance_low']) | (col("status") == "ERROR"),
                lit(2)  # Crítico
            ).when(
                (col("distance") > thresholds['distance_low']) & 
                (col("distance") <= thresholds['distance_high']),
                lit(1)  # Alerta
            ).otherwise(
                lit(0)  # Normal
            )
        )
        
        # Crear columna de nombre de clase para referencia
        df = df.withColumn(
            "class_name",
            when(col("class_label") == 0, lit("Normal"))
            .when(col("class_label") == 1, lit("Alerta"))
            .when(col("class_label") == 2, lit("Crítico"))
            .otherwise(lit("Desconocido"))
        )
        
        # Mostrar distribución de clases
        class_dist = df.groupBy("class_name").count().collect()
        logger.info("📊 Distribución de clases:")
        for row in class_dist:
            logger.info(f"   {row['class_name']}: {row['count']}")
        
        return df
    
    def engineer_features(self, df):
        """Ingeniería de features"""
        logger.info("🔧 Generando features")
        
        # Convertir time a timestamp
        df = df.withColumn("time", col("time").cast("timestamp"))
        
        # Features temporales
        df = df.withColumn("hour", hour(col("time")))
        df = df.withColumn("day_of_week", dayofweek(col("time")))
        df = df.withColumn("month", month(col("time")))
        
        # Codificar status (OK=1, ERROR=0, otros=0.5)
        df = df.withColumn(
            "status_encoded",
            when(col("status") == "OK", lit(1.0))
            .when(col("status") == "ERROR", lit(0.0))
            .otherwise(lit(0.5))
        )
        
        # Codificar device_name
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
    
    def train_model(self, df):
        """Entrena modelo de clasificación"""
        logger.info("🤖 Entrenando modelo de clasificación")
        
        # Definir features
        feature_cols = [
            "distance", 
            "status_encoded", 
            "hour", 
            "day_of_week", 
            "month", 
            "device_index"
        ]
        
        # Ensamblar features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Configurar clasificador
        rf_classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="class_label",
            predictionCol="prediction",
            numTrees=self.config['hyperparameters']['numTrees'],
            maxDepth=self.config['hyperparameters']['maxDepth'],
            minInstancesPerNode=self.config['hyperparameters']['minInstancesPerNode'],
            seed=self.config['hyperparameters']['seed']
        )
        
        # Crear pipeline
        pipeline = Pipeline(stages=[assembler, rf_classifier])
        
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
        metrics, confusion_matrix = self.evaluate_model(predictions)
        
        # Guardar modelo
        model_path = get_model_path(self.sensor_type, "classification", self.model_version)
        
        logger.info(f"💾 Guardando modelo en {model_path}")
        model.write().overwrite().save(model_path)
        
        # Guardar metadata
        self.save_model_metadata(train_df.count(), model_path)
        
        # Guardar métricas
        self.save_metrics(metrics, confusion_matrix, test_df.count())
        
        # Guardar predicciones
        self.save_predictions(predictions)
        
        return model, metrics
    
    def evaluate_model(self, predictions):
        """Evalúa el modelo y calcula métricas + matriz de confusión"""
        logger.info("📈 Evaluando modelo")
        
        # Accuracy
        accuracy_evaluator = MulticlassClassificationEvaluator(
            labelCol="class_label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = accuracy_evaluator.evaluate(predictions)
        
        # Precision (weighted)
        precision_evaluator = MulticlassClassificationEvaluator(
            labelCol="class_label",
            predictionCol="prediction",
            metricName="weightedPrecision"
        )
        precision = precision_evaluator.evaluate(predictions)
        
        # Recall (weighted)
        recall_evaluator = MulticlassClassificationEvaluator(
            labelCol="class_label",
            predictionCol="prediction",
            metricName="weightedRecall"
        )
        recall = recall_evaluator.evaluate(predictions)
        
        # F1 (weighted)
        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol="class_label",
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = f1_evaluator.evaluate(predictions)
        
        metrics = {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1
        }
        
        logger.info(f"✅ Métricas:")
        logger.info(f"   Accuracy:  {accuracy:.4f}")
        logger.info(f"   Precision: {precision:.4f}")
        logger.info(f"   Recall:    {recall:.4f}")
        logger.info(f"   F1-Score:  {f1:.4f}")
        
        # Matriz de confusión
        pred_and_labels = predictions.select("prediction", "class_label").rdd
        multiclass_metrics = MulticlassMetrics(pred_and_labels.map(lambda x: (float(x[0]), float(x[1]))))
        
        confusion_matrix = multiclass_metrics.confusionMatrix().toArray()
        
        logger.info(f"\n📊 Matriz de Confusión:")
        logger.info(f"   Predicho →")
        logger.info(f"   Real ↓")
        for i, row in enumerate(confusion_matrix):
            class_name = self.config['classes'][i]
            logger.info(f"   {class_name:8s}: {row}")
        
        # Convertir a formato JSON para guardar
        confusion_matrix_json = {
            "matrix": confusion_matrix.tolist(),
            "labels": list(self.config['classes'].values())
        }
        
        return metrics, confusion_matrix_json
    
    def save_model_metadata(self, train_samples, model_path):
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
                WHERE sensor_type = %s AND model_type = 'classification'
            """, (self.sensor_type,))
            
            # Insertar nuevo modelo
            cursor.execute("""
                INSERT INTO ml_models_metadata 
                (sensor_type, model_type, model_version, target_metric, training_date, 
                 training_samples, model_path, hyperparameters_json, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.sensor_type,
                'classification',
                self.model_version,
                'class_label',
                datetime.now(),
                train_samples,
                model_path,
                json.dumps(self.config['hyperparameters']),
                True
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Metadata guardada")
        except Exception as e:
            logger.error(f"❌ Error guardando metadata: {e}")
    
    def save_metrics(self, metrics, confusion_matrix_json, sample_count):
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
                INSERT INTO ml_metrics_classification 
                (sensor_type, model_version, date_from, date_to, 
                 accuracy, precision_score, recall_score, f1_score, 
                 confusion_matrix_json, sample_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.sensor_type,
                self.model_version,
                today - timedelta(days=30),
                today,
                metrics['accuracy'],
                metrics['precision'],
                metrics['recall'],
                metrics['f1'],
                json.dumps(confusion_matrix_json),
                sample_count
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ Métricas y matriz de confusión guardadas")
        except Exception as e:
            logger.error(f"❌ Error guardando métricas: {e}")
    
    def save_predictions(self, predictions):
        """Guarda predicciones en MySQL"""
        logger.info("💾 Guardando predicciones")
        
        try:
            device_col = self.config['device_column']

            # UDF local para mapear predicción (0/1/2) → nombre de clase
            def get_class_name(pred_val):
                pred_int = int(pred_val)
                return self.config['classes'].get(pred_int, "Desconocido")

            get_class_name_udf = udf(get_class_name, StringType())

            pred_df = predictions.select(
                lit(self.sensor_type).alias("sensor_type"),
                col(device_col).alias("device_name"),
                col("time"),
                col("class_name").alias("real_class"),
                get_class_name_udf(col("prediction")).alias("predicted_class"),
                get_confidence_udf(col("probability")).alias("confidence"),
                lit(self.model_version).alias("model_version")
            )

            # Guardar en MySQL
            jdbc_url = get_jdbc_url()
            db_props = get_db_properties()

            pred_df.write.jdbc(
                url=jdbc_url,
                table="ml_predictions_classification",
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
            
            # Crear etiquetas
            df_labeled = self.create_labels(df)
            
            # Feature engineering
            df_features = self.engineer_features(df_labeled)
            
            # Entrenar modelo
            model, metrics = self.train_model(df_features)
            
            logger.info(f"🎉 Entrenamiento de clasificación completado para {self.sensor_type}")
            
        except Exception as e:
            logger.error(f"❌ Error en entrenamiento: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.spark.stop()

def main():
    """Punto de entrada principal"""
    logger.info("🚀 INICIANDO ENTRENAMIENTO DE MODELO DE CLASIFICACIÓN")
    
    trainer = ClassificationTrainer()
    trainer.run()
    
    logger.info("\n✅ MODELO DE CLASIFICACIÓN ENTRENADO")

if __name__ == "__main__":
    main()
