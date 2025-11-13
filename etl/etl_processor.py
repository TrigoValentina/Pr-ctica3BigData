
#No se usa
# import pandas as pd
# from storage.save_postgres import guardar_postgres
# from storage.save_mongodb import guardar_mongodb

# def procesar_y_guardar(data):
#     """
#     Limpieza mínima y envío a almacenamiento SQL y NoSQL.
#     """
#     try:
#         df = pd.DataFrame([data])

#         # Limpieza y normalización básica
#         if "time" in df.columns:
#             df["time"] = pd.to_datetime(df["time"], errors="coerce")

#         # Eliminar duplicados
#         df = df.drop_duplicates(subset=["time"], keep="last")

#         # Envío a bases de datos
#         guardar_postgres(df)
#         guardar_mongodb(df)

#         print(f"✅ Procesado y guardado: {data.get('source_file', 'desconocido')}")
#     except Exception as e:
#         print(f"⚠️ Error en ETL: {e}")
