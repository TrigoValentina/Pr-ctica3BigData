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
logger.info("🚀 CONSUMER SPARK - EM500 CO2")
logger.info("=" * 80)

DB_HOST = "mysql"
DB_PORT = 3306
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "datos_sensores"
TABLA = "em500_co2"

spark = SparkSession.builder \
    .appName("SparkConsumer_EM500") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_em500") \
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
                
                # FILTRAR SOLO EM500 (debe tener co2, temperature, humidity o pressure)
                object_data = data.get("object", {})
                has_co2 = "co2" in object_data
                has_temp = "temperature" in object_data
                has_hum = "humidity" in object_data
                has_press = "pressure" in object_data
                
                # También verificar por nombre de dispositivo
                device_name = data.get("deviceInfo", {}).get("deviceName", "").upper()
                is_ems = device_name.startswith("EMS-")
                
                if not (has_co2 or has_temp or has_hum or has_press or is_ems):
                    continue
                
                # Extraer datos
                time_val = data.get("time", datetime.now().isoformat())
                address = data.get("deviceInfo", {}).get("tags", {}).get("Address", "")
                location = data.get("deviceInfo", {}).get("tags", {}).get("Location", "")
                
                co2 = object_data.get("co2")
                co2_status = object_data.get("co2_status", "")
                co2_message = object_data.get("co2_message", "")
                temperature = object_data.get("temperature")
                temperature_status = object_data.get("temperature_status", "")
                temperature_message = object_data.get("temperature_message", "")
                humidity = object_data.get("humidity")
                humidity_status = object_data.get("humidity_status", "")
                humidity_message = object_data.get("humidity_message", "")
                pressure = object_data.get("pressure")
                pressure_status = object_data.get("pressure_status", "")
                pressure_message = object_data.get("pressure_message", "")
                
                # Validar que al menos uno de los campos principales tenga valor (no null)
                has_valid_co2 = co2 is not None and co2 != "" and str(co2).lower() != "null"
                has_valid_temp = temperature is not None and temperature != "" and str(temperature).lower() != "null"
                has_valid_hum = humidity is not None and humidity != "" and str(humidity).lower() != "null"
                has_valid_press = pressure is not None and pressure != "" and str(pressure).lower() != "null"
                
                if not (has_valid_co2 or has_valid_temp or has_valid_hum or has_valid_press):
                    continue
                
                # Insertar en MySQL
                insert_sql = f"""
                    INSERT INTO `{TABLA}` (
                        time, device_name, Address, Location,
                        co2, co2_status, co2_message,
                        temperature, temperature_status, temperature_message,
                        humidity, humidity_status, humidity_message,
                        pressure, pressure_status, pressure_message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (
                    time_val, device_name, address, location,
                    co2, co2_status, co2_message,
                    temperature, temperature_status, temperature_message,
                    humidity, humidity_status, humidity_message,
                    pressure, pressure_status, pressure_message
                ))
                registros_guardados += 1
                
                # Insertar en MongoDB (solo campos con valores, sin null)
                mongo_doc = {
                    "tipo": "EM500",
                    "time": time_val,
                    "batch_id": batch_id
                }
                if device_name and device_name != "":
                    mongo_doc["device_name"] = device_name
                if address and address != "":
                    mongo_doc["Address"] = address
                if location and location != "":
                    mongo_doc["Location"] = location
                if has_valid_co2:
                    mongo_doc["co2"] = co2
                if has_valid_temp:
                    mongo_doc["temperature"] = temperature
                if has_valid_hum:
                    mongo_doc["humidity"] = humidity
                if has_valid_press:
                    mongo_doc["pressure"] = pressure
                mongo_collection.insert_one(mongo_doc)
                
            except Exception as e:
                logger.error(f"❌ Error procesando registro: {e}")
                continue
        
        cursor.close()
        conn.close()
        mongo_client.close()
        
        if registros_guardados > 0:
            logger.info(f"✅ Batch {batch_id}: {registros_guardados} registros EM500 guardados")
    
    except Exception as e:
        logger.error(f"❌ Error en procesar_lote: {e}")

query = df.writeStream \
    .foreachBatch(procesar_lote) \
    .start()

logger.info("✅ Streaming iniciado - Esperando datos EM500...")
query.awaitTermination()

