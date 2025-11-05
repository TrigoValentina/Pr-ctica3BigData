from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "sensores"
COLLECTION_NAME = "datos"

def guardar_mongodb(registro: dict):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    collection.insert_one(registro)
