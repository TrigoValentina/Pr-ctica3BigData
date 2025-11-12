from pymongo import MongoClient

MONGO_URI = "mongodb+srv://emergentes118_db_user:Womita14@cluster0.xcvehjm.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)
print(client.list_database_names())