import os
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("🚀 PRODUCTOR KAFKA - EM500 CO2")
logger.info("=" * 80)

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'datos_sensores'
data_folder = Path("/opt/spark/data")
archivo = "EM500-CO2-915M nov 2024.csv"

columnas_por_csv = {
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
    ]
}

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    logger.info("✅ Productor Kafka creado")
except Exception as e:
    logger.error(f"❌ Error al crear productor: {e}")
    raise

csv_path = data_folder / archivo
logger.info(f"📄 Procesando: {archivo}")
logger.info(f"📁 Ruta: {csv_path}")

if not csv_path.exists():
    logger.error(f"❌ Archivo no encontrado: {csv_path}")
    exit(1)

df = pd.read_csv(csv_path, low_memory=False)
df = df.dropna(how="all")
total_filas = len(df)
logger.info(f"📊 Total filas: {total_filas}")

columnas_filtradas = columnas_por_csv.get(archivo, [])
registros_enviados = 0

for i, row in df.iterrows():
    try:
        data = {}
        for col in columnas_filtradas:
            if col in df.columns:
                valor = row[col]
                # Para EM500, incluir campos de object incluso si están vacíos
                if pd.isna(valor):
                    if col.startswith("object."):
                        valor = None  # Incluir como None
                    else:
                        continue
                parts = col.split(".")
                ref = data
                for p in parts[:-1]:
                    if p not in ref:
                        ref[p] = {}
                    ref = ref[p]
                ref[parts[-1]] = valor

        # Asegurar que siempre haya un objeto "object" para EM500
        if "object" not in data:
            data["object"] = {}

        if "time" not in data or pd.isna(data.get("time")):
            data["time"] = datetime.now(timezone.utc).isoformat()
        elif isinstance(data.get("time"), pd.Timestamp):
            data["time"] = data["time"].isoformat()

        if not data or ("time" not in data and "deviceInfo" not in data):
            continue

        future = producer.send(KAFKA_TOPIC, value=data)
        record_metadata = future.get(timeout=10)
        registros_enviados += 1
        
        if registros_enviados % 1000 == 0:
            logger.info(f"📨 {registros_enviados}/{total_filas} registros enviados")

    except Exception as e:
        logger.error(f"❌ Error en fila {i+1}: {e}")
        continue

producer.flush()
logger.info(f"✅ Completado: {registros_enviados}/{total_filas} registros enviados")

