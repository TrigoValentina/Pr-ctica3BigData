from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient

# === CONFIGURACIÓN DE MONGO ATLAS ===
MONGO_URI = "mongodb+srv://emergentes118_db_user:Womita14@cluster0.xcvehjm.mongodb.net/?retryWrites=true&w=majority"
MONGO_DB = "gamc_db"
MONGO_COLLECTION = "sensores"

# === Esquema del JSON que llega por Kafka ===
schema = StructType([
    StructField("time", StringType()),
    StructField("deviceInfo", StructType([
        StructField("deviceName", StringType()),
        StructField("tags", StructType([
            StructField("Address", StringType()),
            StructField("Location", StringType())
        ])),
        StructField("tenantName", StringType())
    ])),
    StructField("object", StructType([
        StructField("co2", StringType()),
        StructField("temperature", StringType()),
        StructField("humidity", StringType()),
        StructField("distance", StringType()),
        StructField("status", StringType()),
        StructField("co2_status", StringType()),
        StructField("temperature_status", StringType()),
        StructField("pressure_status", StringType()),
        StructField("humidity_status", StringType()),
        StructField("pressure", StringType()),
        StructField("pressure_message", StringType()),
        StructField("temperature_message", StringType()),
        StructField("humidity_message", StringType()),
        StructField("co2_message", StringType()),
        StructField("LAeq", StringType()),
        StructField("LAI", StringType()),
        StructField("LAImax", StringType())
    ]))
])

# === Función para guardar documentos en Mongo Atlas ===
def guardar_mongo(doc):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        collection.insert_one(doc)
        client.close()
        print(f"✅ Insertado en Mongo Atlas: {doc.get('device_name', 'sin_nombre')}")
    except Exception as e:
        print(f"⚠️ Error MongoDB Atlas: {e}")

# === Crear sesión de Spark ===
spark = SparkSession.builder.appName("Kafka_ETL_Streaming_MongoAtlas").getOrCreate()

# === Leer stream desde Kafka (nombre del servicio dentro de docker-compose) ===
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "datos_sensores") \
    .option("startingOffsets", "earliest") \
    .load()

# === Parsear el JSON recibido ===
df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Convertir campo de tiempo
df_json = df_json.withColumn("time", to_timestamp(col("time")))

# === Procesar cada fila y enviar a Mongo Atlas ===
def procesar_fila(row):
    row_dict = row.asDict(recursive=True)
    flat = {}

    # Aplanar 'object'
    obj = row_dict.get("object") or {}
    flat.update({k: obj.get(k) for k in obj.keys()})

    # Aplanar 'deviceInfo'
    dev = row_dict.get("deviceInfo") or {}
    flat["device_name"] = dev.get("deviceName")
    flat["tenant_name"] = dev.get("tenantName")
    tags = dev.get("tags") or {}
    flat["Address"] = tags.get("Address")
    flat["Location"] = tags.get("Location")

    # Agregar timestamp
    flat["time"] = row_dict.get("time")

    # Guardar en Mongo Atlas
    guardar_mongo(flat)

# === Iniciar el stream de Spark ===
query = df_json.writeStream.foreach(procesar_fila).outputMode("append").start()
query.awaitTermination()
