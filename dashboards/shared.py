# dashboards/shared.py

import pandas as pd
import mysql.connector
import logging
from datetime import datetime

DB_HOST = "localhost"
DB_PORT = 3307
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"

logger = logging.getLogger(__name__)

def get_mysql_connection():
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True
    )
    return conn

def leer_datos_mysql(tabla):
    conn = get_mysql_connection()
    df = pd.read_sql(f"SELECT * FROM `{tabla}` ORDER BY time DESC LIMIT 10000", conn)
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
    return df
