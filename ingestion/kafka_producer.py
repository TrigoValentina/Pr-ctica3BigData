import json
import time
import logging
from kafka import KafkaProducer
from data_generator import generar_dato_aleatorio

KAFKA_BROKER_URL = "localhost:29092"

TOPIC_NAME = "datos_sensores"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def crear_productor():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='all'
        )
        logger.info("Productor Kafka creado.")
        return producer
    except Exception as e:
        logger.error(f"Error creando productor Kafka: {e}")
        return None

def enviar_datos():
    producer = crear_productor()
    if not producer:
        return
    try:
        while True:
            mensaje = generar_dato_aleatorio()
            producer.send(TOPIC_NAME, value=mensaje)
            producer.flush()
            logger.info(f"Enviado: {mensaje}")
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Detención manual.")
    finally:
        producer.close()

if __name__ == "__main__":
    enviar_datos()
