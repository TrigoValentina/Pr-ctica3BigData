import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "datos_sensores"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',  # <-- leer desde el inicio
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=1000
)


print("Consumidor Kafka iniciado. Esperando mensajes...")

for mensaje in consumer:
    registro = mensaje.value
    print(f"Recibido: {registro}")
