from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017/")

db = client["retail_db"]

print("Conexión exitosa")
print(db.list_collection_names())