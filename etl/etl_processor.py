import pandas as pd

def transform_data(df):
    # Ejemplo: limpiar datos nulos
    df = df.fillna(method='ffill')
    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()
    return df
