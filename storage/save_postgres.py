import psycopg2
import pandas as pd

def guardar_postgres(df):
    conn = psycopg2.connect(
        dbname="ambiental",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensores (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP,
            device_name TEXT,
            variable TEXT,
            valor TEXT
        );
    """)
    for _, row in df.iterrows():
        for col, val in row.items():
            cursor.execute(
                "INSERT INTO sensores (time, device_name, variable, valor) VALUES (%s,%s,%s,%s)",
                (row.get("time"), row.get("deviceInfo.deviceName", "N/A"), col, str(val))
            )
    conn.commit()
    cursor.close()
    conn.close()
