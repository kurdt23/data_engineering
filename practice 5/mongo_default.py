from pymongo import MongoClient

# подключение к клиенту Mongo
def connect_mongo():
    client = MongoClient()
    db = client['test-db']
    return db.person
connect_mongo()
# вставка в БД сразу несколько данных
def insert_many(collection, data):
    collection.insert_many(data)