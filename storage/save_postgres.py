import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "host": "localhost",
    "dbname": "sensores",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def crear_tabla():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS datos_sensores (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP,
        device VARCHAR(50),
        temperature FLOAT,
        humidity FLOAT,
        co2 INTEGER,
        pressure FLOAT,
        distance FLOAT,
        battery FLOAT
    )
    """)
    conn.commit()
    cur.close()
    conn.close()

def guardar_postgres(registro: dict):
    crear_tabla()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO datos_sensores(time, device, temperature, humidity, co2, pressure, distance, battery)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        registro['time'],
        registro['deviceInfo'].get('deviceName'),
        registro['object'].get('temperature'),
        registro['object'].get('humidity'),
        registro['object'].get('co2'),
        registro['object'].get('pressure'),
        registro['object'].get('distance'),
        registro['object'].get('battery')
    ))
    conn.commit()
    cur.close()
    conn.close()
