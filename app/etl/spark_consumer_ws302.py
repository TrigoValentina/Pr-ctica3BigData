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
logger.info("🚀 CONSUMER SPARK - WS302 SONIDO")
logger.info("=" * 80)

DB_HOST = "mysql"
DB_PORT = 3306
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "datos_sensores"
TABLA = "ws302_sonido"

spark = SparkSession.builder \
    .appName("SparkConsumer_WS302") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_ws302") \
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
                
                # FILTRAR SOLO WS302 (debe tener LAeq, LAI o LAImax)
                object_data = data.get("object", {})
                has_LAeq = "LAeq" in object_data
                has_LAI = "LAI" in object_data
                has_LAImax = "LAImax" in object_data
                
                if not (has_LAeq or has_LAI or has_LAImax):
                    continue
                
                # Extraer datos
                time_val = data.get("time", datetime.now().isoformat())
                tenant_name = data.get("deviceInfo", {}).get("tenantName", "")
                address = data.get("deviceInfo", {}).get("tags", {}).get("Address", "")
                location = data.get("deviceInfo", {}).get("tags", {}).get("Location", "")
                LAeq = object_data.get("LAeq", "")
                LAI = object_data.get("LAI", "")
                LAImax = object_data.get("LAImax", "")
                status = object_data.get("status", "")
                
                # Insertar en MySQL
                insert_sql = f"""
                    INSERT INTO `{TABLA}` (time, tenant_name, Address, Location, LAeq, LAI, LAImax, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (time_val, tenant_name, address, location, LAeq, LAI, LAImax, status))
                registros_guardados += 1
                
                # Insertar en MongoDB
                mongo_doc = {
                    "tipo": "WS302",
                    "time": time_val,
                    "tenant_name": tenant_name,
                    "Address": address,
                    "Location": location,
                    "LAeq": LAeq,
                    "LAI": LAI,
                    "LAImax": LAImax,
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
            logger.info(f"✅ Batch {batch_id}: {registros_guardados} registros WS302 guardados")
    
    except Exception as e:
        logger.error(f"❌ Error en procesar_lote: {e}")

query = df.writeStream \
    .foreachBatch(procesar_lote) \
    .start()

logger.info("✅ Streaming iniciado - Esperando datos WS302...")
query.awaitTermination()

