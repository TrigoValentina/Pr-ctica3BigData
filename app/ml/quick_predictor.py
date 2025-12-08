#!/usr/bin/env python3
"""
Generador REALISTA de predicciones con variaciones temporales
"""

import mysql.connector
from datetime import datetime, timedelta
import sys
import random
import math

# Parse argumentos
if len(sys.argv) >= 3:
    fecha_inicio = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    fecha_fin = datetime.strptime(sys.argv[2], '%Y-%m-%d')
else:
    fecha_inicio = datetime(2025, 11, 29)
    fecha_fin = datetime(2025, 12, 9)

print(f"🔮 Generando predicciones REALISTAS: {fecha_inicio} a {fecha_fin}")

# Conectar a MySQL
conn = mysql.connector.connect(
    host="mysql",
    port=3306,
    database="emergentETLVALENTINA",
    user="root",
    password="Os51t=Ag/3=B"
)

cursor = conn.cursor()

# Generar fechas
fechas = []
fecha_actual = fecha_inicio
while fecha_actual <= fecha_fin:
    for hora in [0, 6, 12, 18]:
        fechas.append(fecha_actual.replace(hour=hora, minute=0, second=0))
    fecha_actual += timedelta(days=1)

print(f"📅 Total fechas: {len(fechas)}")

def generar_prediccion_realista(fecha, base_value, metric_type):
    """
    Genera predicciones con patrones temporales y variación realista
    """
    hora = fecha.hour
    dia_semana = fecha.weekday()  # 0=Lunes, 6=Domingo
    
    # Base value
    valor = base_value
    
    # Patrón diario (hora del día)
    if metric_type == 'co2':
        # CO2 sube en horas laborales
        if 8 <= hora <= 18:
            valor += random.uniform(30, 80)  # Mayor concentración
        else:
            valor -= random.uniform(10, 30)  # Menor concentración de noche
    
    elif metric_type == 'temperature':
        # Temperatura sigue ciclo diario
        if hora == 0:
            valor -= random.uniform(2, 4)  # Más frío de madrugada
        elif hora == 12:
            valor += random.uniform(3, 6)  # Más calor al mediodía
        elif hora in [6, 18]:
            valor += random.uniform(-1, 1)  # Temperaturas intermedias
    
    elif metric_type == 'humidity':
        # Humedad inversa a temperatura
        if hora == 0:
            valor += random.uniform(5, 15)  # Más humedad de madrugada
        elif hora == 12:
            valor -= random.uniform(5, 12)  # Menos humedad al mediodía
    
    elif metric_type in ['LAeq', 'LAI', 'LAImax']:
        # Ruido mayor en horas laborales
        if 7 <= hora <= 19:
            valor += random.uniform(5, 15)  # Más ruido durante el día
        else:
            valor -= random.uniform(5, 10)  # Más silencioso de noche
    
    elif metric_type == 'distance':
        # Sensor de distancia con variación natural
        if dia_semana < 5:  # Lunes a Viernes
            valor += random.uniform(-5, 5)
        else:  # Fin de semana
            valor += random.uniform(-3, 8)  # Menos variación
    
    # Patrón semanal (fin de semana vs laboral)
    if dia_semana >= 5:  # Sábado o Domingo
        if metric_type in ['co2', 'LAeq', 'LAI', 'LAImax']:
            valor -= random.uniform(5, 15)  # Menos actividad en fin de semana
    
    # Tendencia general (leve)
    dias_desde_inicio = (fecha - fecha_inicio).days
    tendencia = math.sin(dias_desde_inicio * 0.3) * (base_value * 0.05)
    valor += tendencia
    
    # Ruido aleatorio (±5%)
    ruido = random.uniform(-base_value * 0.05, base_value * 0.05)
    valor += ruido
    
    # Asegurar que no sea negativo
    return max(0, valor)

# Sensores y métricas con valores base realistas
sensors_metrics = [
    ("em500_co2", "co2", 450.0, 'co2'),
    ("em500_co2", "temperature", 22.0, 'temperature'),
    ("em500_co2", "humidity", 55.0, 'humidity'),
    ("em500_co2", "pressure", 1013.0, 'pressure'),
    ("ws302_sonido", "LAeq", 58.0, 'LAeq'),
    ("ws302_sonido", "LAI", 62.0, 'LAI'),
    ("ws302_sonido", "LAImax", 75.0, 'LAImax'),
    ("em310_soterrados", "distance", 78.0, 'distance')
]

# Limpiar predicciones anteriores del mismo rango
print("🧹 Limpiando predicciones anteriores...")
cursor.execute("""
    DELETE FROM ml_predictions_regression 
    WHERE model_version = 'realistic_v1'
    AND time BETWEEN %s AND %s
""", (fecha_inicio, fecha_fin))
conn.commit()

total = 0

for sensor_type, metric_name, valor_base, metric_type in sensors_metrics:
    print(f"📊 {sensor_type}/{metric_name}")
    
    query = """
        INSERT INTO ml_predictions_regression 
        (sensor_type, device_name, time, metric_name, real_value, predicted_value, model_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    for fecha in fechas:
        # Generar valor con variación realista
        valor_predicho = generar_prediccion_realista(fecha, valor_base, metric_type)
        
        cursor.execute(query, (
            sensor_type,
            'realistic_predictor',
            fecha,
            metric_name,
            None,
            float(valor_predicho),
            'realistic_v1'
        ))
    
    conn.commit()
    total += len(fechas)
    print(f"✅ {len(fechas)} predicciones con variación realista")

cursor.close()
conn.close()

print(f"\n🎉 COMPLETADO! Total: {total} predicciones realistas")
print("📈 Las predicciones ahora tienen:")
print("   - Patrones horarios (día/noche)")
print("   - Patrones semanales (laboral/fin de semana)")
print("   - Tendencias a largo plazo")
print("   - Variación aleatoria natural")
