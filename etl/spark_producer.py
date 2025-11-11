import os
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ---------------- CONFIGURACIÓN DE LOG ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- PRODUCTOR KAFKA ----------------
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  # default=str para NaN
)
logger.info("✅ Productor Kafka creado.")

# ---------------- CARPETA DE DATOS ----------------
data_folder = Path(__file__).resolve().parent.parent / "data"
archivos_esperados = [
    "WS302-915M SONIDO NOV 2024.csv",
    "EM500-CO2-915M nov 2024.csv",
    "EM310-UDL-915M soterrados nov 2024.csv"
]

# ---------------- COLUMNAS POR CSV ----------------
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

# ---------------- PROCESAMIENTO Y ENVÍO ----------------
for archivo in archivos_esperados:
    csv_path = data_folder / archivo
    if not csv_path.exists():
        logger.warning(f"⚠️ Archivo no encontrado: {archivo}")
        continue

    df = pd.read_csv(csv_path, low_memory=False).dropna(how="all")
    total_filas = len(df)
    logger.info(f"📤 Enviando datos desde: {archivo} ({total_filas} filas)")

    for i, row in df.iterrows():
        data = {}
        for col in columnas_por_csv[archivo]:
            if col in df.columns:
                parts = col.split(".")
                ref = data
                for p in parts[:-1]:
                    if p not in ref:
                        ref[p] = {}
                    ref = ref[p]
                ref[parts[-1]] = row[col]

        # Si no hay "time", usar timestamp actual
        if "time" not in data or pd.isna(data["time"]):
            data["time"] = datetime.now(timezone.utc).isoformat()

        # Enviar a Kafka
        producer.send("datos_sensores", value=data)

        # Mostrar información en consola
        logger.info(
            f"📨 [{archivo}] Enviado ({i+1}/{total_filas}): {json.dumps(data, default=str)}"
        )

        time.sleep(1)  # simular envío gradual

# ---------------- FINALIZAR ----------------
producer.flush()
logger.info("🎯 Todos los datos fueron enviados correctamente a Kafka.")
