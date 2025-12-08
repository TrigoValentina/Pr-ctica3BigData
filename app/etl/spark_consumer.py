from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import mysql.connector
from pymongo import MongoClient
from pyspark.sql.streaming import StreamingQuery
import logging
import traceback
import json
from datetime import datetime

# ===============================
# CONFIGURACIÓN DE LOGGING
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("🚀 INICIANDO CONSUMER SPARK - PROCESAMIENTO DE DATOS DE KAFKA")
logger.info("=" * 80)

# ===============================
# Configuraciones de Bases de Datos
# ===============================
# Configuración de MySQL
# Nota: Si se ejecuta dentro de Docker, usar "mysql" como host
# Si se ejecuta localmente, usar "localhost"
DB_HOST = "mysql"  # Nombre del servicio en docker-compose
DB_PORT = 3306
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"

# Configuración de MongoDB Atlas
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
MONGO_COLLECTION = "sensores"

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "datos_sensores"

logger.info("-" * 80)
logger.info("📊 CONFIGURACIÓN DE CONEXIONES")
logger.info("-" * 80)
logger.info(f"📡 Kafka Broker: {KAFKA_BROKER}")
logger.info(f"📬 Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"🐬 MySQL Host: {DB_HOST}")
logger.info(f"🐬 MySQL Port: {DB_PORT}")
logger.info(f"🐬 MySQL Database: {DB_NAME}")
logger.info(f"🐬 MySQL User: {DB_USER}")
logger.info(f"🍃 MongoDB Atlas URI: {MONGO_ATLAS_URI[:50]}...")  # Mostrar solo primeros caracteres por seguridad
logger.info(f"🍃 MongoDB Collection: {MONGO_COLLECTION}")

# ===============================
# Schema de JSON
# ===============================
logger.info("-" * 80)
logger.info("📋 CONFIGURANDO SCHEMA DE JSON")
logger.info("-" * 80)

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

logger.info("✅ Schema configurado con los siguientes campos:")
logger.info("   - time")
logger.info("   - deviceInfo (deviceName, tags, tenantName)")
logger.info("   - object (co2, temperature, humidity, distance, status, etc.)")

# ===============================
# Funciones de guardado
# ===============================
logger.info("-" * 80)
logger.info("💾 CONFIGURANDO FUNCIONES DE ALMACENAMIENTO")
logger.info("-" * 80)

def guardar_mysql_tablas(df_row):
    try:
        logger.debug(f"   🐬 Intentando conectar a MySQL: {DB_HOST}:{DB_PORT}")
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.debug("   ✅ Conexión a MySQL establecida")
        
        cursor = conn.cursor()
        
        # Definir columnas permitidas por tabla (según el schema SQL)
        tablas_columnas = {
            "em310_soterrados": ["time", "device_name", "Address", "Location", "distance", "status"],
            "em500_co2": ["time", "device_name", "Address", "Location", "co2", "co2_status", "co2_message",
                         "temperature", "temperature_status", "temperature_message",
                         "humidity", "humidity_status", "humidity_message",
                         "pressure", "pressure_status", "pressure_message"],
            "ws302_sonido": ["time", "tenant_name", "Address", "Location", "LAeq", "LAI", "LAImax", "status"],
            "otros": ["time", "device_name", "tenant_name", "Address", "Location"]
        }
        
        # Elegir tabla según datos
        # Detectar EM500 por presencia de co2, temperature, humidity, pressure O por nombre de dispositivo (EMS-*)
        device_name = df_row.get("device_name", "").upper() if df_row.get("device_name") else ""
        
        if "distance" in df_row:
            tabla = "em310_soterrados"
            logger.debug("   📊 Tipo de sensor detectado: EM310 (soterrados)")
        elif "co2" in df_row or "temperature" in df_row or "humidity" in df_row or "pressure" in df_row or (device_name and device_name.startswith("EMS-")):
            tabla = "em500_co2"
            logger.debug(f"   📊 Tipo de sensor detectado: EM500 (CO2/Temperatura/Humedad/Presión o dispositivo {device_name})")
        elif "LAeq" in df_row or "LAI" in df_row or "LAImax" in df_row:
            tabla = "ws302_sonido"
            logger.debug("   📊 Tipo de sensor detectado: WS302 (sonido)")
        else:
            tabla = "otros"
            logger.warning("   ⚠️ Tipo de sensor no reconocido, usando tabla 'otros'")

        logger.debug(f"   📋 Tabla destino: {tabla}")
        
        # Filtrar solo las columnas que existen en la tabla
        columnas_permitidas = tablas_columnas.get(tabla, [])
        datos_filtrados = {k: v for k, v in df_row.items() if k in columnas_permitidas}
        
        if not datos_filtrados:
            logger.warning(f"   ⚠️ No hay datos válidos para insertar en {tabla}")
            cursor.close()
            conn.close()
            return

        # Insertar fila (las tablas ya están creadas por el script SQL)
        cols_str = ", ".join([f"`{col}`" for col in datos_filtrados.keys()])
        vals_str = ", ".join(["%s"] * len(datos_filtrados))
        vals = []
        for col in datos_filtrados.keys():
            val = datos_filtrados[col]
            # Convertir datetime a string si es necesario
            if isinstance(val, datetime):
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            vals.append(str(val) if val is not None else None)
        
        insert_sql = f"INSERT INTO `{tabla}` ({cols_str}) VALUES ({vals_str})"
        
        logger.debug(f"   💾 Insertando registro en {tabla}")
        logger.debug(f"      Columnas: {cols_str}")
        logger.debug(f"      Valores: {len(vals)} valores")
        cursor.execute(insert_sql, vals)

        conn.commit()
        logger.info(f"   ✅ Registro guardado en MySQL ({tabla})")
        
        cursor.close()
        conn.close()
        logger.debug("   🔌 Conexión MySQL cerrada")
        
    except Exception as e:
        logger.error(f"   ❌ Error al guardar en MySQL: {e}")
        logger.error(traceback.format_exc())

def guardar_mongo(df_row):
    try:
        logger.debug(f"   🍃 Intentando conectar a MongoDB Atlas...")
        client = MongoClient(MONGO_ATLAS_URI)
        
        # Extraer el nombre de la base de datos de la URI
        # La URI es: mongodb+srv://user:pass@cluster/dbname?params
        db_name = MONGO_ATLAS_URI.split('/')[-1].split('?')[0]
        db = client[db_name]
        collection = db[MONGO_COLLECTION]
        
        logger.debug(f"   📋 Base de datos: {db_name}")
        logger.debug(f"   📋 Colección: {MONGO_COLLECTION}")
        logger.debug(f"   💾 Insertando documento en MongoDB Atlas")
        
        result = collection.insert_one(df_row)
        logger.info(f"   ✅ Registro guardado en MongoDB Atlas (ID: {result.inserted_id})")
        
        client.close()
        logger.debug("   🔌 Conexión MongoDB Atlas cerrada")
        
    except Exception as e:
        logger.error(f"   ❌ Error al guardar en MongoDB Atlas: {e}")
        logger.error(traceback.format_exc())

logger.info("✅ Funciones de almacenamiento configuradas")

# ===============================
# Spark Session
# ===============================
logger.info("-" * 80)
logger.info("⚡ INICIANDO SPARK SESSION")
logger.info("-" * 80)

try:
    spark = SparkSession.builder \
        .appName("Kafka_ETL_Streaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    logger.info("✅ Spark Session creada exitosamente")
    logger.info(f"   - App Name: Kafka_ETL_Streaming")
    logger.info(f"   - Spark Version: {spark.version}")
except Exception as e:
    logger.error(f"❌ Error al crear Spark Session: {e}")
    logger.error(traceback.format_exc())
    raise

# ===============================
# Leer de Kafka
# ===============================
logger.info("-" * 80)
logger.info("📡 CONFIGURANDO LECTURA DE KAFKA")
logger.info("-" * 80)
logger.info(f"   Broker: {KAFKA_BROKER}")
logger.info(f"   Topic: {KAFKA_TOPIC}")
logger.info(f"   Starting Offsets: earliest")

try:
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✅ Stream de Kafka configurado exitosamente")
    
    logger.info("🔄 Procesando mensajes JSON...")
    df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")
    
    logger.info("✅ Mensajes JSON parseados")
    
    logger.info("🔄 Convirtiendo columna 'time' a timestamp...")
    df_json = df_json.withColumn("time", to_timestamp(col("time")))
    logger.info("✅ Conversión de timestamp completada")
    
except Exception as e:
    logger.error(f"❌ Error al configurar lectura de Kafka: {e}")
    logger.error(traceback.format_exc())
    raise

# ===============================
# Procesamiento de filas
# ===============================
logger.info("-" * 80)
logger.info("🔄 CONFIGURANDO PROCESADOR DE FILAS")
logger.info("-" * 80)

class ProcesarFila:
    def __init__(self):
        self.contador_registros = 0
        logger.info("   ✅ Instancia de ProcesarFila creada")
    
    def open(self, partition_id, epoch_id):
        logger.info(f"   🔓 Abriendo partición {partition_id}, epoch {epoch_id}")
        return True
    
    def process(self, row: Row):
        try:
            self.contador_registros += 1
            logger.info(f"   📥 Procesando registro #{self.contador_registros}")
            
            row_dict = row.asDict(recursive=True)
            logger.debug(f"      Datos recibidos: {json.dumps(row_dict, default=str, ensure_ascii=False)}")
            
            flat_dict = {}

            # Aplanar 'object' - incluir incluso si tiene valores None/null
            if "object" in row_dict and row_dict["object"] is not None:
                # Incluir todos los campos, incluso si son None
                obj_dict = row_dict["object"]
                if isinstance(obj_dict, dict):
                    for key, value in obj_dict.items():
                        flat_dict[key] = value  # Incluir incluso si es None
                    logger.debug(f"      ✅ Campos de 'object' agregados: {list(obj_dict.keys())}")
                else:
                    logger.debug(f"      ⚠️ 'object' no es un diccionario: {type(obj_dict)}")
            else:
                logger.debug("      ⚠️ No hay campo 'object' o está vacío")
            
            # Aplanar 'deviceInfo'
            if "deviceInfo" in row_dict and row_dict["deviceInfo"]:
                device_info = row_dict["deviceInfo"]
                flat_dict["device_name"] = device_info.get("deviceName")
                flat_dict["tenant_name"] = device_info.get("tenantName")
                
                if "tags" in device_info and device_info["tags"]:
                    flat_dict["Address"] = device_info["tags"].get("Address")
                    flat_dict["Location"] = device_info["tags"].get("Location")
                    logger.debug(f"      ✅ DeviceInfo procesado: {device_info.get('deviceName')}")
                else:
                    logger.debug("      ⚠️ No hay tags en deviceInfo")
            else:
                logger.debug("      ⚠️ No hay campo 'deviceInfo' o está vacío")

            flat_dict["time"] = row_dict.get("time")
            logger.debug(f"      ⏰ Timestamp: {flat_dict.get('time')}")

            logger.info(f"   📊 Registro procesado - Campos: {len(flat_dict)}")
            logger.debug(f"      Estructura plana: {json.dumps(flat_dict, default=str, ensure_ascii=False)}")

            # Guardar en bases de datos
            logger.info(f"   💾 Guardando registro #{self.contador_registros} en bases de datos...")
            guardar_mysql_tablas(flat_dict)
            guardar_mongo(flat_dict)
            logger.info(f"   ✅ Registro #{self.contador_registros} procesado y guardado exitosamente")
            
        except Exception as e:
            logger.error(f"   ❌ Error procesando fila: {e}")
            logger.error(traceback.format_exc())

    def close(self, error):
        if error:
            logger.error(f"   ❌ Error al cerrar procesador: {error}")
        else:
            logger.info(f"   🔒 Cerrando procesador - Total registros procesados: {self.contador_registros}")

logger.info("✅ Procesador de filas configurado")

# ===============================
# Streaming
# ===============================
logger.info("=" * 80)
logger.info("🚀 INICIANDO STREAMING DE DATOS")
logger.info("=" * 80)
logger.info("   ⚠️ El proceso quedará ejecutándose esperando mensajes de Kafka...")
logger.info("   ⚠️ Presiona Ctrl+C para detener")

try:
    query = df_json.writeStream \
        .foreach(ProcesarFila()) \
        .outputMode("append") \
        .start()
    
    logger.info("✅ Streaming iniciado exitosamente")
    logger.info(f"   Query ID: {query.id}")
    logger.info("=" * 80)
    logger.info("📡 ESCUCHANDO MENSAJES DE KAFKA...")
    logger.info("=" * 80)
    
    query.awaitTermination()
    
except KeyboardInterrupt:
    logger.info("=" * 80)
    logger.info("🛑 DETENIENDO STREAMING (Interrupción del usuario)")
    logger.info("=" * 80)
    query.stop()
    logger.info("✅ Streaming detenido")
except Exception as e:
    logger.error(f"❌ Error en streaming: {e}")
    logger.error(traceback.format_exc())
    if 'query' in locals():
        query.stop()
