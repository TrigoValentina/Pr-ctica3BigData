import json, random, time
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🎲 Enviando datos aleatorios... Ctrl+C para detener")

try:
    while True:
        data = {
            "time": datetime.now(timezone.utc).isoformat(),
            "deviceInfo.deviceName": random.choice(["EM310", "WS302", "EM500"]),
            "object.co2": random.uniform(350, 1000),
            "object.temperature": random.uniform(15, 35),
            "object.humidity": random.uniform(30, 90),
        }
        producer.send("datos_sensores", value=data)
        print(f"📨 Enviado: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Simulación detenida")
