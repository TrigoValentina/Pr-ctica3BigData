import pandas as pd

def procesar_csv(path_csv: str) -> pd.DataFrame:
    df = pd.read_csv(path_csv)
    df.columns = df.columns.str.strip().str.lower()
    # Aquí puedes añadir transformaciones adicionales
    return df
