import random
from datetime import datetime, timezone

def generar_dato_aleatorio():
    timestamp_actual = datetime.now(timezone.utc).isoformat()
    return {
        "time": timestamp_actual,
        "deviceInfo": {"deviceName": f"Sensor_Simulado_{random.randint(1,3)}"},
        "object": {
            "temperature": round(random.uniform(18, 32), 2),
            "humidity": round(random.uniform(40, 75), 2),
            "co2": random.randint(400, 1500),
            "pressure": round(random.uniform(1010, 1025), 2),
            "distance": round(random.uniform(50, 200), 1),
            "battery": round(random.uniform(3, 5), 2)
        }
    }
