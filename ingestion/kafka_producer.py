import os
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import traceback

# ========================
# CONFIGURACIÓN DE LOGS DETALLADA
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("🚀 INICIANDO PRODUCTOR KAFKA - INGESTA DE DATOS DE SENSORES")
logger.info("=" * 80)

# ========================
# CREAR PRODUCTOR KAFKA
# ========================
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'datos_sensores'

logger.info(f"📡 Intentando conectar a Kafka en: {KAFKA_BROKER}")
logger.info(f"📬 Tópico destino: {KAFKA_TOPIC}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    logger.info("✅ Productor Kafka creado exitosamente")
    logger.info(f"   - Broker: {KAFKA_BROKER}")
    logger.info(f"   - Tópico: {KAFKA_TOPIC}")
except Exception as e:
    logger.error(f"❌ Error al crear productor Kafka: {e}")
    logger.error(traceback.format_exc())
    raise

# ========================
# BUSCAR ARCHIVOS CSV
# ========================
logger.info("-" * 80)
logger.info("📂 BUSCANDO ARCHIVOS CSV EN LA CARPETA 'data'")
logger.info("-" * 80)

base_path = Path(__file__).resolve().parent
data_folder = base_path.parent / "data"

logger.info(f"📁 Ruta base del script: {base_path}")
logger.info(f"📁 Carpeta de datos: {data_folder}")
logger.info(f"📁 ¿Existe la carpeta?: {data_folder.exists()}")

archivos_esperados = [
    "WS302-915M SONIDO NOV 2024.csv",
    "EM500-CO2-915M nov 2024.csv",
    "EM310-UDL-915M soterrados nov 2024.csv"
]

logger.info(f"📋 Archivos CSV esperados ({len(archivos_esperados)}):")
for archivo in archivos_esperados:
    logger.info(f"   - {archivo}")

csv_files = []
if data_folder.exists():
    logger.info(f"🔍 Buscando archivos en: {data_folder}")
    for root, dirs, files in os.walk(data_folder):
        logger.info(f"   Directorio actual: {root}")
        logger.info(f"   Archivos encontrados: {files}")
        for file in files:
            if file in archivos_esperados:
                full_path = os.path.join(root, file)
                csv_files.append(full_path)
                logger.info(f"   ✅ Archivo encontrado: {file} -> {full_path}")
else:
    logger.error(f"❌ La carpeta 'data' no existe en: {data_folder}")

if not csv_files:
    logger.error("❌ No se encontraron los CSV esperados dentro de la carpeta 'data'.")
    logger.error(f"   Carpeta buscada: {data_folder}")
    logger.error(f"   Archivos esperados: {archivos_esperados}")
    exit(1)

logger.info(f"✅ Total de archivos CSV encontrados: {len(csv_files)}")
for idx, csv_file in enumerate(csv_files, 1):
    logger.info(f"   {idx}. {csv_file}")

# ========================
# COLUMNAS A FILTRAR POR CSV
# ========================
logger.info("-" * 80)
logger.info("📊 CONFIGURACIÓN DE COLUMNAS POR ARCHIVO CSV")
logger.info("-" * 80)

columnas_por_csv = {
    "EM310-UDL-915M soterrados nov 2024.csv": [
        "time",
        "deviceInfo.deviceName",
        "deviceInfo.tags.Address",
        "deviceInfo.tags.Location",
        "object.distance",
        "object.status"
    ],
    "EM500-CO2-915M nov 2024.csv": [
        "time",
        "deviceInfo.deviceName",
        "deviceInfo.tags.Address",
        "deviceInfo.tags.Location",
        "object.co2_status",
        "object.co2",
        "object.temperature_message",
        "object.pressure_message",
        "object.pressure",
        "object.co2_message",
        "object.pressure_status",
        "object.humidity_status",
        "object.temperature",
        "object.humidity",
        "object.humidity_message",
        "object.temperature_status"
    ],
    "WS302-915M SONIDO NOV 2024.csv": [
        "time",
        "deviceInfo.tenantName",
        "deviceInfo.tags.Address",
        "deviceInfo.tags.Location",
        "object.LAeq",
        "object.LAI",
        "object.LAImax",
        "object.status"
    ]
}

for archivo, columnas in columnas_por_csv.items():
    logger.info(f"📄 {archivo}:")
    logger.info(f"   Total columnas: {len(columnas)}")
    for col in columnas:
        logger.info(f"      - {col}")

# ========================
# PROCESAR Y ENVIAR DATOS DE CADA CSV
# ========================
logger.info("=" * 80)
logger.info("🔄 INICIANDO PROCESAMIENTO Y ENVÍO DE DATOS")
logger.info("=" * 80)

total_registros_enviados = 0
total_archivos_procesados = 0

for csv_path in csv_files:
    try:
        nombre_csv = Path(csv_path).name
        logger.info("-" * 80)
        logger.info(f"📄 PROCESANDO ARCHIVO: {nombre_csv}")
        logger.info(f"   Ruta completa: {csv_path}")
        logger.info("-" * 80)
        
        columnas_filtradas = columnas_por_csv.get(nombre_csv, [])
        
        if not columnas_filtradas:
            logger.warning(f"⚠️ No hay configuración de columnas para: {nombre_csv}")
            logger.warning(f"   Saltando este archivo...")
            continue
        
        logger.info(f"📊 Leyendo CSV: {csv_path}")
        logger.info(f"   Columnas configuradas: {len(columnas_filtradas)}")
        
        # Leer CSV
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"✅ CSV leído exitosamente")
        logger.info(f"   Filas originales: {len(df)}")
        logger.info(f"   Columnas en CSV: {len(df.columns)}")
        logger.info(f"   Primeras columnas: {list(df.columns[:5])}...")
        
        # Limpiar filas completamente vacías
        df = df.dropna(how="all")
        logger.info(f"   Filas después de limpiar vacías: {len(df)}")
        
        # Verificar columnas esperadas
        columnas_faltantes = [col for col in columnas_filtradas if col not in df.columns]
        if columnas_faltantes:
            logger.warning(f"⚠️ Columnas faltantes en CSV ({len(columnas_faltantes)}):")
            for col in columnas_faltantes:
                logger.warning(f"      - {col}")
        else:
            logger.info(f"✅ Todas las columnas esperadas están presentes")
        
        columnas_encontradas = [col for col in columnas_filtradas if col in df.columns]
        logger.info(f"📋 Columnas que se procesarán: {len(columnas_encontradas)}/{len(columnas_filtradas)}")

        logger.info(f"📤 Iniciando envío de {len(df)} registros a Kafka...")
        logger.info(f"⚡ MODO RÁPIDO: Limitando a 100 registros por archivo para testing")
        df = df.head(100)  # Limitar a 100 registros para testing rápido
        registros_enviados_archivo = 0
        
        for index, row in df.iterrows():
            try:
                data = {}
                
                logger.debug(f"   Procesando fila {index+1}/{len(df)}")

                # Filtrar solo las columnas requeridas
                for col in columnas_filtradas:
                    if col in df.columns:
                        valor = row[col]
                        
                        # Verificar si el valor es NaN
                        if pd.isna(valor):
                            logger.debug(f"      ⚠️ Columna '{col}' tiene valor NaN, omitiendo")
                            continue
                        
                        # Crear estructura anidada (deviceInfo, object, etc.)
                        partes = col.split(".")
                        ref = data
                        for p in partes[:-1]:
                            if p not in ref:
                                ref[p] = {}
                            ref = ref[p]
                        ref[partes[-1]] = valor
                        logger.debug(f"      ✅ Agregado: {col} = {valor}")
                    else:
                        logger.debug(f"      ⚠️ Columna '{col}' no existe en CSV")

                # Asegurar que 'time' esté presente
                if "time" not in data:
                    data["time"] = datetime.now(timezone.utc).isoformat()
                    logger.debug(f"      ⏰ Timestamp generado: {data['time']}")
                elif isinstance(data.get("time"), pd.Timestamp):
                    data["time"] = data["time"].isoformat()
                    logger.debug(f"      ⏰ Timestamp convertido: {data['time']}")

                # Validar que data no esté vacío
                if not data:
                    logger.warning(f"   ⚠️ Fila {index+1} resultó vacía después del procesamiento, saltando...")
                    continue

                # Enviar al tópico Kafka
                logger.debug(f"   📨 Enviando registro {index+1} a Kafka...")
                future = producer.send(KAFKA_TOPIC, value=data)
                
                # Obtener metadata del mensaje (opcional, para confirmación)
                try:
                    record_metadata = future.get(timeout=10)
                    logger.info(f"📨 [{nombre_csv}] Registro {index+1}/{len(df)} enviado exitosamente")
                    logger.info(f"      Tópico: {record_metadata.topic}")
                    logger.info(f"      Partición: {record_metadata.partition}")
                    logger.info(f"      Offset: {record_metadata.offset}")
                    logger.debug(f"      Datos: {json.dumps(data, ensure_ascii=False, default=str)}")
                    registros_enviados_archivo += 1
                    total_registros_enviados += 1
                except Exception as e:
                    logger.error(f"   ❌ Error al enviar registro {index+1}: {e}")
                    logger.error(f"      Datos: {json.dumps(data, ensure_ascii=False, default=str)}")

                time.sleep(0.1)  # Flujo más rápido (0.1 segundos entre filas)

            except Exception as e:
                logger.error(f"   ❌ Error procesando fila {index+1}: {e}")
                logger.error(traceback.format_exc())
                continue

        logger.info(f"✅ Envío completado para {nombre_csv}")
        logger.info(f"   Registros enviados: {registros_enviados_archivo}/{len(df)}")
        total_archivos_procesados += 1

    except Exception as e:
        logger.error(f"❌ Error procesando archivo {csv_path}: {e}")
        logger.error(traceback.format_exc())
        continue

# ========================
# FINALIZAR ENVÍO
# ========================
logger.info("=" * 80)
logger.info("🏁 FINALIZANDO ENVÍO DE DATOS")
logger.info("=" * 80)

logger.info("🔄 Flusheando mensajes pendientes en Kafka...")
producer.flush()
logger.info("✅ Flush completado")

logger.info("=" * 80)
logger.info("📊 RESUMEN FINAL")
logger.info("=" * 80)
logger.info(f"✅ Archivos procesados: {total_archivos_procesados}/{len(csv_files)}")
logger.info(f"✅ Total de registros enviados: {total_registros_enviados}")
logger.info(f"✅ Tópico Kafka: {KAFKA_TOPIC}")
logger.info(f"✅ Broker: {KAFKA_BROKER}")
logger.info("=" * 80)
logger.info("🎯 Proceso completado exitosamente")
logger.info("=" * 80)
