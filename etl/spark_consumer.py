from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
from pymongo import MongoClient

POSTGRES_HOST = "db.yynzqzlnknngffbsixmh.supabase.co"
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "Womita1425."
POSTGRES_PORT = 5432

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "gamc_db"
MONGO_COLLECTION = "sensores"

# Schema de JSON
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

# Funciones de guardado
def guardar_postgres_tablas(df_row):
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
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

# Spark session
spark = SparkSession.builder.appName("Kafka_ETL_Streaming").getOrCreate()

# Leer de Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "datos_sensores") \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_json = df_json.withColumn("time", to_timestamp(col("time")))

def procesar_fila(row):
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

    guardar_postgres_tablas(flat_dict)
    guardar_mongo(flat_dict)

# Streaming
query = df_json.writeStream.foreach(procesar_fila).outputMode("append").start()
query.awaitTermination()
