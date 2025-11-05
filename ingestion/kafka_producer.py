import json, time, random
from kafka import KafkaProducer
from datetime import datetime, timezone

KAFKA_BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'datos_ambientales'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generar_dato(sensor_type: str):
    timestamp = datetime.now(timezone.utc).isoformat()
    if sensor_type == "EM500":
        return {
            "time": timestamp,
            "deviceInfo": {"deviceName": "EM500"},
            "object": {
                "temperature": round(random.uniform(18, 32), 2),
                "humidity": round(random.uniform(40, 75), 2),
                "co2": random.randint(400, 1500),
                "pressure": round(random.uniform(1010, 1025), 2)
            }
        }

def main():
    while True:
        for sensor in ["EM500", "WS302", "EM310"]:
            mensaje = generar_dato(sensor)
            producer.send(TOPIC_NAME, value=mensaje)
            producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    main()
