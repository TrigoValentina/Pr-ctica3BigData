import psycopg2
import pandas as pd

conn = psycopg2.connect(
    dbname="gamc_db",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

def save_data(df, table_name):
    df.to_sql(table_name, con=conn, if_exists='append', index=False)

def get_latest_data(sensor_type):
    query = f"SELECT * FROM {sensor_type.lower()} ORDER BY time DESC LIMIT 500"
    return pd.read_sql(query, conn)
