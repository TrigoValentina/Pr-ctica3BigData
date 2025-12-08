import os
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import traceback

# ---------------- CONFIGURACIÓN DE LOG DETALLADA ---------------- 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("🚀 INICIANDO PRODUCTOR KAFKA - INGESTA DE DATOS DE SENSORES (DOCKER)")
logger.info("=" * 80)

# ---------------- PRODUCTOR KAFKA ---------------- 
KAFKA_BROKER = 'kafka:9092'  # Para uso dentro de Docker
KAFKA_TOPIC = 'datos_sensores'

logger.info(f"📡 Intentando conectar a Kafka en: {KAFKA_BROKER}")
logger.info(f"📬 Tópico destino: {KAFKA_TOPIC}")
logger.info(f"🐳 Modo: Docker (contenedor)")

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

# ---------------- CARPETA DE DATOS ---------------- 
logger.info("-" * 80)
logger.info("📂 CONFIGURANDO CARPETA DE DATOS")
logger.info("-" * 80)

# En Docker, los datos están montados en /opt/spark/data
data_folder = Path("/opt/spark/data")
logger.info(f"📁 Carpeta de datos (Docker): {data_folder}")
logger.info(f"📁 ¿Existe la carpeta?: {data_folder.exists()}")

archivos_esperados = [
    "EM310-UDL-915M soterrados nov 2024.csv",
    "EM500-CO2-915M nov 2024.csv",
    "WS302-915M SONIDO NOV 2024.csv"
]

logger.info(f"📋 Archivos CSV esperados ({len(archivos_esperados)}):")
for archivo in archivos_esperados:
    logger.info(f"   - {archivo}")

# ---------------- COLUMNAS POR CSV ---------------- 
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

# ---------------- PROCESAMIENTO Y ENVÍO ---------------- 
logger.info("=" * 80)
logger.info("🔄 INICIANDO PROCESAMIENTO Y ENVÍO DE DATOS")
logger.info("=" * 80)

total_registros_enviados = 0
total_archivos_procesados = 0

for archivo in archivos_esperados:
    try:
        csv_path = data_folder / archivo
        logger.info("-" * 80)
        logger.info(f"📄 PROCESANDO ARCHIVO: {archivo}")
        logger.info(f"   Ruta completa: {csv_path}")
        logger.info("-" * 80)
        
        if not csv_path.exists():
            logger.warning(f"⚠️ Archivo no encontrado: {archivo}")
            logger.warning(f"   Ruta buscada: {csv_path}")
            continue

        columnas_filtradas = columnas_por_csv.get(archivo, [])
        if not columnas_filtradas:
            logger.warning(f"⚠️ No hay configuración de columnas para: {archivo}")
            continue

        logger.info(f"📊 Leyendo CSV: {csv_path}")
        logger.info(f"   Columnas configuradas: {len(columnas_filtradas)}")
        
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"✅ CSV leído exitosamente")
        logger.info(f"   Filas originales: {len(df)}")
        logger.info(f"   Columnas en CSV: {len(df.columns)}")
        
        df = df.dropna(how="all")
        total_filas = len(df)
        logger.info(f"   Filas después de limpiar vacías: {total_filas}")
        
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
        logger.info(f"📤 Iniciando envío de {total_filas} registros a Kafka...")
        
        registros_enviados_archivo = 0

        for i, row in df.iterrows():
            try:
                data = {}
                logger.debug(f"   Procesando fila {i+1}/{total_filas}")

                for col in columnas_por_csv[archivo]:
                    if col in df.columns:
                        valor = row[col]
                        
                        # Para EM500, incluir campos incluso si están vacíos para que el consumer los detecte
                        # Solo omitir si es completamente NaN y no es un campo crítico de identificación
                        if pd.isna(valor):
                            # Si es un campo de object (co2, temperature, etc.), incluir como None
                            if col.startswith("object."):
                                valor = None  # Incluir como None en lugar de omitir
                                logger.debug(f"      ⚠️ Columna '{col}' tiene valor NaN, incluyendo como None")
                            else:
                                logger.debug(f"      ⚠️ Columna '{col}' tiene valor NaN, omitiendo")
                                continue
                        
                        parts = col.split(".")
                        ref = data
                        for p in parts[:-1]:
                            if p not in ref:
                                ref[p] = {}
                            ref = ref[p]
                        ref[parts[-1]] = valor
                        logger.debug(f"      ✅ Agregado: {col} = {valor}")

                # Si no hay "time", usar timestamp actual
                if "time" not in data or pd.isna(data.get("time")):
                    data["time"] = datetime.now(timezone.utc).isoformat()
                    logger.debug(f"      ⏰ Timestamp generado: {data['time']}")
                elif isinstance(data.get("time"), pd.Timestamp):
                    data["time"] = data["time"].isoformat()
                    logger.debug(f"      ⏰ Timestamp convertido: {data['time']}")

                # Para EM500, asegurar que siempre haya un objeto "object" aunque esté vacío
                # Esto permite que el consumer detecte el tipo de sensor por el nombre del dispositivo
                if archivo == "EM500-CO2-915M nov 2024.csv" and "object" not in data:
                    data["object"] = {}
                    logger.debug(f"      ✅ Objeto 'object' creado vacío para EM500")

                if not data or ("time" not in data and "deviceInfo" not in data):
                    logger.warning(f"   ⚠️ Fila {i+1} resultó vacía, saltando...")
                    continue

                # Enviar a Kafka
                logger.debug(f"   📨 Enviando registro {i+1} a Kafka...")
                future = producer.send(KAFKA_TOPIC, value=data)
                
                try:
                    record_metadata = future.get(timeout=10)
                    logger.info(f"📨 [{archivo}] Registro {i+1}/{total_filas} enviado exitosamente")
                    logger.info(f"      Tópico: {record_metadata.topic}")
                    logger.info(f"      Partición: {record_metadata.partition}")
                    logger.info(f"      Offset: {record_metadata.offset}")
                    logger.debug(f"      Datos: {json.dumps(data, ensure_ascii=False, default=str)}")
                    registros_enviados_archivo += 1
                    total_registros_enviados += 1
                except Exception as e:
                    logger.error(f"   ❌ Error al enviar registro {i+1}: {e}")
                    logger.error(f"      Datos: {json.dumps(data, ensure_ascii=False, default=str)}")

                # Reducir sleep para acelerar el procesamiento
                if archivo == "EM500-CO2-915M nov 2024.csv":
                    time.sleep(0.01)  # 0.01 segundos para EM500 (muy rápido)
                elif archivo == "WS302-915M SONIDO NOV 2024.csv":
                    time.sleep(0.01)  # 0.01 segundos para WS302 (muy rápido)
                else:
                    time.sleep(0.1)  # 0.1 segundos para EM310

            except Exception as e:
                logger.error(f"   ❌ Error procesando fila {i+1}: {e}")
                logger.error(traceback.format_exc())
                continue

        logger.info(f"✅ Envío completado para {archivo}")
        logger.info(f"   Registros enviados: {registros_enviados_archivo}/{total_filas}")
        total_archivos_procesados += 1

    except Exception as e:
        logger.error(f"❌ Error procesando archivo {archivo}: {e}")
        logger.error(traceback.format_exc())
        continue

# ---------------- FINALIZAR ---------------- 
logger.info("=" * 80)
logger.info("🏁 FINALIZANDO ENVÍO DE DATOS")
logger.info("=" * 80)

logger.info("🔄 Flusheando mensajes pendientes en Kafka...")
producer.flush()
logger.info("✅ Flush completado")

logger.info("=" * 80)
logger.info("📊 RESUMEN FINAL")
logger.info("=" * 80)
logger.info(f"✅ Archivos procesados: {total_archivos_procesados}/{len(archivos_esperados)}")
logger.info(f"✅ Total de registros enviados: {total_registros_enviados}")
logger.info(f"✅ Tópico Kafka: {KAFKA_TOPIC}")
logger.info(f"✅ Broker: {KAFKA_BROKER}")
logger.info("=" * 80)
logger.info("🎯 Proceso completado exitosamente")
logger.info("=" * 80)
