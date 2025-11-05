# ingestion/data_generator.py
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# =============================
# CONFIGURACIÓN
# =============================
KAFKA_BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'datos_ambientales'  # Topic donde se enviarán los datos
DELAY_SECONDS = 2  # Tiempo entre cada mensaje

# =============================
# CREAR PRODUCTOR DE KAFKA
# =============================
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# =============================
# FUNCIONES PARA GENERAR DATOS
# =============================
def generar_dato_em500():
    return {
        "time": datetime.now(timezone.utc).isoformat(),
        "deviceInfo": {"deviceName": "EM500"},
        "object": {
            "temperature": round(random.uniform(18, 32), 2),
            "humidity": round(random.uniform(40, 75), 2),
            "co2": random.randint(400, 1500),
            "pressure": round(random.uniform(1010, 1025), 2)
        }
    }

def generar_dato_ws302():
    return {
        "time": datetime.now(timezone.utc).isoformat(),
        "deviceInfo": {"deviceName": "WS302"},
        "object": {
            "laeq": round(random.uniform(30, 100), 1),
            "latmax": round(random.uniform(40, 120), 1),
            "battery": round(random.uniform(20, 100), 1)
        }
    }

def generar_dato_em310():
    return {
        "time": datetime.now(timezone.utc).isoformat(),
        "deviceInfo": {"deviceName": "EM310"},
        "object": {
            "distance": round(random.uniform(10, 200), 1),
            "battery": round(random.uniform(2.5, 4.2), 2),
            "status": random.choice(["activo", "inactivo"])
        }
    }

# =============================
# BUCLE PRINCIPAL
# =============================
def main():
    while True:
        for sensor_func in [generar_dato_em500, generar_dato_ws302, generar_dato_em310]:
            dato = sensor_func()
            producer.send(TOPIC_NAME, value=dato)
            producer.flush()
            print(f"Enviado: {dato}")
        time.sleep(DELAY_SECONDS)

if __name__ == "__main__":
    main()
