from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType
import mysql.connector
from pymongo import MongoClient
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("🚀 CONSUMER SPARK - EM310 SOTERRADOS")
logger.info("=" * 80)

DB_HOST = "mysql"
DB_PORT = 3306
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "datos_sensores"
TABLA = "em310_soterrados"

spark = SparkSession.builder \
    .appName("SparkConsumer_EM310") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_em310") \
    .getOrCreate()

logger.info("✅ Spark Session creada")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

logger.info("✅ Lectura de Kafka configurada")

def procesar_lote(batch_df, batch_id):
    try:
        rows = batch_df.collect()
        if not rows:
            return
        
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True
        )
        cursor = conn.cursor()
        
        mongo_client = MongoClient(MONGO_ATLAS_URI)
        db_name = MONGO_ATLAS_URI.split("/")[-1].split("?")[0]
        mongo_db = mongo_client[db_name]
        mongo_collection = mongo_db["sensores"]
        
        registros_guardados = 0
        
        for row in rows:
            try:
                value_str = row.value.decode('utf-8')
                data = json.loads(value_str)
                
                # FILTRAR SOLO EM310 (debe tener "distance")
                if "object" not in data or "distance" not in data.get("object", {}):
                    continue
                
                # Extraer datos
                time_val = data.get("time", datetime.now().isoformat())
                device_name = data.get("deviceInfo", {}).get("deviceName", "")
                address = data.get("deviceInfo", {}).get("tags", {}).get("Address", "")
                location = data.get("deviceInfo", {}).get("tags", {}).get("Location", "")
                distance = data.get("object", {}).get("distance", "")
                status = data.get("object", {}).get("status", "")
                
                # Insertar en MySQL
                insert_sql = f"""
                    INSERT INTO `{TABLA}` (time, device_name, Address, Location, distance, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (time_val, device_name, address, location, distance, status))
                registros_guardados += 1
                
                # Insertar en MongoDB
                mongo_doc = {
                    "tipo": "EM310",
                    "time": time_val,
                    "device_name": device_name,
                    "Address": address,
                    "Location": location,
                    "distance": distance,
                    "status": status,
                    "batch_id": batch_id
                }
                mongo_collection.insert_one(mongo_doc)
                
            except Exception as e:
                logger.error(f"❌ Error procesando registro: {e}")
                continue
        
        cursor.close()
        conn.close()
        mongo_client.close()
        
        if registros_guardados > 0:
            logger.info(f"✅ Batch {batch_id}: {registros_guardados} registros EM310 guardados")
    
    except Exception as e:
        logger.error(f"❌ Error en procesar_lote: {e}")

query = df.writeStream \
    .foreachBatch(procesar_lote) \
    .start()

logger.info("✅ Streaming iniciado - Esperando datos EM310...")
query.awaitTermination()

