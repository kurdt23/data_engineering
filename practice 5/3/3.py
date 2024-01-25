"""Дан файл с некоторыми данными. Формат файла – произвольный, не совпадает с тем, что был в первом/втором заданиях.
Необходимо считать данные и добавить их к той коллекции, куда были записаны данные в первом и втором заданиях.
Выполните следующие запросы:
	удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
	увеличить возраст (age) всех документов на 1
	поднять заработную плату на 5% для произвольно выбранных профессий
	поднять заработную плату на 7% для произвольно выбранных городов
	поднять заработную плату на 10% для выборки по сложному предикату (произвольный город,
    произвольный набор профессий, произвольный диапазон возраста)
	удалить из коллекции записи по произвольному предикату
"""
from pymongo import MongoClient
# import json
import msgpack

# подключение к клиенту Mongo
def connect_mongo():
    client = MongoClient()
    db = client['test-db']
    return db.person

# вставка в БД сразу несколько данных
def insert_many(collection, data):
    collection.insert_many(data)

# # сохранение в json
# def save_in_json(dictionary, name):
#     with open(f"{name}.json", "w", encoding="utf-8") as f:
#         f.write(json.dumps(dictionary, indent=4))

# Загрузка входных данных
def load_data(filename):
    with open(filename, 'rb') as file:
        new_data = msgpack.unpack(file, raw=False)
    return new_data

# удалить из коллекции документы по предикату: salary < 25 000 || salary > 175000
def delete_by_salary(collection):
    result = collection.delete_many(
        {
            "$or": [
                {"salary": {"$lt": 25000}},
                {"salary": {"$gt": 175000}}

            ]
        }
    )
    print('delete_by_salary -', result)

# увеличить возраст (age) всех документов на 1
def update_age(collection):
    result = collection.update_many({}, {
            "$inc": {"age": 1}
        }
    )
    print('update_age -', result)

# поднять заработную плату на 5% для произвольно выбранных профессий
# поднять заработную плату на 7% для произвольно выбранных городов
def increase_salary_by_column(collection, column_name, percent, values):
    job_filter = {
        column_name: {"$in": values}
    }

    update = {
        "$mul": {
            "salary": (1 + percent / 100)
        }
    }

    print('increase_salary_by_column -', collection.update_many(job_filter, update))

# поднять заработную плату на 10% для выборки по сложному предикату (произвольный город,
#  произвольный набор профессий, произвольный диапазон возраста 18-45)
def increase_salary_by_multipredicate(collection):
    job_filter = {
        "$and": [
            {"city": {"$in": ["Москва", "Лондон", "Берлин"]}},
            {"job": {"$in": ["Водитель", "Продавец", "Учитель"]}},
            {"age": {"$gt": 18, "$lt": 45}}
        ]
    }

    update = {
        "$mul": {
            "salary": 1.1
        }
    }

    print('increase_salary_by_multipredicate -', collection.update_many(job_filter, update))

# удалить из коллекции записи по произвольному предикату
def delete_by_salary_ang_age(collection):
    result = collection.delete_many(
        {
            "$and": [
                {"salary": {"$gte": 50000, "$lte": 150000}},
                {"age": {"$gt": 18, "$lt": 45}}
            ]
        }
    )
    print('delete_by_salary_and_age -', result)


data = load_data('task_3_item.msgpack')
insert_many(connect_mongo(), data)

delete_by_salary(connect_mongo())

update_age(connect_mongo())

increase_salary_by_column(connect_mongo(), 'job', 5, ["Повар", "Водитель", "Учитель"])

increase_salary_by_column(connect_mongo(), 'city', 7, ["Москва", "Лондон", "Берлин"])

increase_salary_by_multipredicate(connect_mongo())

delete_by_salary_ang_age(connect_mongo())