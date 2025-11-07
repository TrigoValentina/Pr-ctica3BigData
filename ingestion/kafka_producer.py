import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime, timezone

# Configurar logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear el productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logger.info("Productor Kafka creado.")

# 📄 Ruta de tu archivo CSV real
csv_path = r"C:\Users\Valentina\Videos\Pr-ctica3BigData2\data\EM310-UDL-915M soterrados nov 2024.csv"

# Leer el CSV
df = pd.read_csv(csv_path)

# Convertir columna de tiempo (si existe)
if 'time' in df.columns:
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
else:
    # Si no hay columna de tiempo, la agregamos
    df['time'] = datetime.now(timezone.utc)

# Eliminar filas vacías
df = df.dropna(how="all")

logger.info(f"{len(df)} registros cargados desde el CSV.")

# Enviar cada fila como mensaje Kafka
for index, row in df.iterrows():
    data = {
        "time": row.get("time", datetime.now(timezone.utc)).isoformat(),
        "deviceInfo": {"deviceName": "EM310_UDL_CSV"},
        "object": {}
    }

    # Agregar todas las columnas numéricas como sensores
    for col in df.columns:
        if col not in ["time"] and pd.notnull(row[col]):
            data["object"][col] = row[col]

    # Enviar al topic
    producer.send("datos_sensores", value=data)
    logger.info(f"Enviado ({index+1}/{len(df)}): {data}")

    # Espera pequeña entre envíos para simular flujo real
    time.sleep(1)

producer.flush()
logger.info("✅ Todos los datos del CSV fueron enviados a Kafka.")
