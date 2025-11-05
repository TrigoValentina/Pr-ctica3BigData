from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["gamc_db"]

def save_data_mongo(collection_name, data):
    db[collection_name].insert_many(data)
