# from kafka import KafkaConsumer
# import json
# from etl.etl_processor import procesar_y_guardar

# KAFKA_BROKER = "localhost:29092"
# TOPIC_NAME = "datos_sensores"

# consumer = KafkaConsumer(
#     TOPIC_NAME,
#     bootstrap_servers=[KAFKA_BROKER],
#     auto_offset_reset='earliest',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# print("📡 Consumidor Kafka escuchando datos en tiempo real...")

# try:
#     for mensaje in consumer:
#         data = mensaje.value
#         procesar_y_guardar(data)
# except KeyboardInterrupt:
#     print("🛑 Consumer detenido manualmente.")
# finally:
#     consumer.close()
