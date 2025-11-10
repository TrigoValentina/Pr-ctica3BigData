import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "datos_sensores"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',  # leer desde el inicio si no hay offsets guardados
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    # NO se pone consumer_timeout_ms
)

print("Consumidor Kafka iniciado. Esperando mensajes en tiempo real...")

# Bucle infinito para consumir datos a medida que llegan
try:
    for mensaje in consumer:
        registro = mensaje.value
        print(f"Recibido: {registro}")
except KeyboardInterrupt:
    print("🛑 Consumidor detenido manualmente")
finally:
    consumer.close()
