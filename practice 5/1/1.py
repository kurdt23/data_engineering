"""Дан файл с некоторыми данными. Формат файла – произвольный. Считайте данные из файла и запишите их в Mongo.
Реализуйте и выполните следующие запросы:
	вывод* первых 10 записей, отсортированных по убыванию по полю salary;
	вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
	вывод первых 10 записей, отфильтрованных по сложному предикату: (записи только из произвольного города,
    записи только из трех произвольно взятых профессий), отсортировать по возрастанию по полю age
	вывод количества записей, получаемых в результате следующей фильтрации (age в произвольном диапазоне,
    year в [2019,2022], 50 000 < salary <= 75 000 || 125 000 < salary < 150 000).
* – здесь и везде предполагаем вывод в JSON.
"""
from pymongo import MongoClient
import json

# подключение к клиенту Mongo
def connect_mongo():
    client = MongoClient()
    db = client['test-db']
    return db.person

# вставка в БД сразу несколько данных
def insert_many(collection, data):
    collection.insert_many(data)

# вывод* первых 10 записей, отсортированных по убыванию по полю salary
def get_sort_by_salary(collection):
    data = []

    for person in collection.find(limit=10).sort({'salary': -1}):
        del person['_id']
        data.append(person)
    return data

# вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
def get_filter_by_age(collection):
    data = []

    for person in (collection
            .find({"age": {"$lt": 30}}, limit=15)
            .sort({"salary": -1})):
        del person['_id']
        data.append(person)
    return data

"""вывод первых 10 записей, отфильтрованных по сложному предикату: (записи только из произвольного города,
    записи только из трех произвольно взятых профессий), отсортировать по возрастанию по полю age"""
def get_filter_by_city_and_job(collection):
    data = []

    for person in (collection
            .find({"city": "Минск",
                  "job": {"$in": ["Продавец", "Инженер", "Учитель"]}
                  }, limit=5)
            .sort({"age": -1})):
        del person['_id']
        data.append(person)

    return data

"""ывод количества записей, получаемых в результате следующей фильтрации (age в произвольном диапазоне,
    year в [2019,2022], 50 000 < salary <= 75 000 || 125 000 < salary < 150 000)"""
def get_count_obj(collection):
    result = collection.count_documents({
        "age": {"$gt": 20, "$lt": 40},  # age in 20 - 40
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lt": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })

    return result

# загрузка входных данных
def get_from_json(f):
    items = []
    with open(f, "r", encoding='utf-8') as f:
        items = json.load(f)
    return items

# print(get_from_json("task_1_item.json"))


data = get_from_json('task_1_item.json')


# вставка в БД сразу несколько данных
# insert_many(connect_mongo(), data)

# сохранение в json
def save_in_json(dictionary, name):
    with open(f"{name}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(dictionary, indent=4))

sort_by_salary = get_sort_by_salary(connect_mongo())
save_in_json(sort_by_salary, 'sort_by_salary')

filter_by_age = get_filter_by_age(connect_mongo())
save_in_json(filter_by_age, 'filter_by_age')

filter_by_city_and_job = get_filter_by_city_and_job(connect_mongo())
save_in_json(filter_by_city_and_job, 'filter_by_city_and_job')

# print(get_count_obj(connect_mongo()))
