from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
from pymongo import MongoClient
from pyspark.sql.streaming import StreamingQuery
import socket

# ===============================
# Configuraciones de Bases de Datos
# ===============================
POSTGRES_HOST = "db.yynzqzlnknngffbsixmh.supabase.co"  # o el host de tu contenedor local
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "Womita1425."
POSTGRES_PORT = 5432

MONGO_URI = "mongodb://mongodb:27017/"  # nombre del contenedor Mongo
MONGO_DB = "gamc_db"
MONGO_COLLECTION = "sensores"

# ===============================
# Schema de JSON
# ===============================
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

# ===============================
# Funciones de guardado
# ===============================
def guardar_postgres_tablas(df_row):
    try:
        # --- Solución para el problema de IPv6 en Docker ---
        # Resolvemos el hostname a su dirección IPv4 justo antes de conectar.
        try:
            host_ip = socket.gethostbyname(POSTGRES_HOST)
        except socket.gaierror:
            host_ip = POSTGRES_HOST # Si falla, usamos el original

        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=host_ip, # Usamos la IP resuelta
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        # Elegir tabla según datos
        if "distance" in df_row:
            tabla = "em310_soterrados"
        elif "co2" in df_row:
            tabla = "em500_co2"
        elif "LAeq" in df_row:
            tabla = "ws302_sonido"
        else:
            tabla = "otros"

        # Crear tabla dinámica
        cols_defs = ", ".join([f"{col} TEXT" for col in df_row.keys()])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {tabla} (id SERIAL PRIMARY KEY, {cols_defs});")

        # Insertar fila
        cols_str = ", ".join(df_row.keys())
        vals_str = ", ".join(["%s"] * len(df_row))
        vals = [str(v) for v in df_row.values()]
        cursor.execute(f"INSERT INTO {tabla} ({cols_str}) VALUES ({vals_str})", vals)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Error Postgres: {e}")

def guardar_mongo(df_row):
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        collection.insert_one(df_row)
    except Exception as e:
        print(f"⚠️ Error MongoDB: {e}")

# ===============================
# Spark Session
# ===============================
spark = SparkSession.builder.appName("Kafka_ETL_Streaming").getOrCreate()

# ===============================
# Leer de Kafka
# ===============================
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "datos_sensores") \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_json = df_json.withColumn("time", to_timestamp(col("time")))

# ===============================
# Procesamiento de filas
# ===============================
class ProcesarFila:
    def open(self, partition_id, epoch_id):
        return True
    def process(self, row: Row):
        row_dict = row.asDict(recursive=True)
        flat_dict = {}

        # Aplanar 'object'
        if "object" in row_dict:
            flat_dict.update(row_dict["object"])
        # Aplanar 'deviceInfo'
        if "deviceInfo" in row_dict:
            flat_dict["device_name"] = row_dict["deviceInfo"].get("deviceName")
            flat_dict["tenant_name"] = row_dict["deviceInfo"].get("tenantName")
            if "tags" in row_dict["deviceInfo"]:
                flat_dict["Address"] = row_dict["deviceInfo"]["tags"].get("Address")
                flat_dict["Location"] = row_dict["deviceInfo"]["tags"].get("Location")

        flat_dict["time"] = row_dict.get("time")

        # Guardar en bases de datos
        guardar_postgres_tablas(flat_dict)
        guardar_mongo(flat_dict)

    def close(self, error):
        pass

# ===============================
# Streaming
# ===============================
query = df_json.writeStream.foreach(ProcesarFila()).outputMode("append").start()
query.awaitTermination()
