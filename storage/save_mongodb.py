from pymongo import MongoClient

def guardar_mongodb(df):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ambiental_db"]
    coleccion = db["sensores"]
    data_json = df.to_dict(orient="records")
    if data_json:
        coleccion.insert_many(data_json)
    client.close()
