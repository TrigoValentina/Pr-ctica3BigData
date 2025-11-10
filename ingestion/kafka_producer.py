import os
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ========================
# CONFIGURACIÓN DE LOGS
# ========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================
# CREAR PRODUCTOR KAFKA
# ========================
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
logger.info("✅ Productor Kafka creado.")

# ========================
# BUSCAR ARCHIVOS CSV
# ========================
base_path = Path(__file__).resolve().parent
data_folder = base_path.parent / "data"

archivos_esperados = [
    "WS302-915M SONIDO NOV 2024.csv",
    "EM500-CO2-915M nov 2024.csv",
    "EM310-UDL-915M soterrados nov 2024.csv"
]

csv_files = []
for root, dirs, files in os.walk(data_folder):
    for file in files:
        if file in archivos_esperados:
            csv_files.append(os.path.join(root, file))

if not csv_files:
    logger.error("❌ No se encontraron los CSV esperados dentro de la carpeta 'data'.")
    exit(1)

logger.info(f"📁 Archivos encontrados: {csv_files}")

# ========================
# COLUMNAS A FILTRAR POR CSV
# ========================
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

# ========================
# PROCESAR CADA CSV
# ========================
for csv_path in csv_files:
    try:
        df = pd.read_csv(csv_path, low_memory=False).dropna(how="all")
        device_name = Path(csv_path).stem

        columnas_filtradas = columnas_por_csv.get(Path(csv_path).name, [])
        logger.info(f"📤 Enviando datos desde: {device_name} ({len(df)} registros)")

        for index, row in df.iterrows():
            data = {}
            for col in columnas_filtradas:
                if col in df.columns:
                    parts = col.split(".")
                    ref = data
                    for p in parts[:-1]:
                        if p not in ref:
                            ref[p] = {}
                        ref = ref[p]
                    ref[parts[-1]] = row[col]

            # Asegurar que 'time' siempre exista
            if 'time' not in data:
                data['time'] = datetime.now(timezone.utc).isoformat()
            elif isinstance(data['time'], pd.Timestamp):
                data['time'] = data['time'].isoformat()

            producer.send("datos_sensores", value=data)

            # Mostrar fila completa enviada
            logger.info(f"📨 [{device_name}] Enviado ({index+1}/{len(df)}): {json.dumps(data, ensure_ascii=False)}")

            time.sleep(1)  # Simular flujo real

        logger.info(f"✅ Envío completado para {device_name}")

    except Exception as e:
        logger.error(f"⚠️ Error procesando {csv_path}: {e}")

# Asegurarse que todo se envíe
producer.flush()
logger.info("🎯 Todos los datos fueron enviados correctamente a Kafka.")
