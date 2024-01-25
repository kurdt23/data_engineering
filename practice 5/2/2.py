"""Дан файл с некоторыми данными. Формат файла – произвольный, не совпадает с тем, что был в первом задании.
Необходимо считать данные и добавить их к той коллекции, куда были записаны данные в первом задании.
Реализовать следующие запросы:
	вывод минимальной, средней, максимальной salary
	вывод количества данных по представленным профессиям
	вывод минимальной, средней, максимальной salary по городу
	вывод минимальной, средней, максимальной salary по профессии
	вывод минимального, среднего, максимального возраста по городу
	вывод минимального, среднего, максимального возраста по профессии
	вывод максимальной заработной платы при минимальном возрасте
	вывод минимальной заработной платы при максимальной возрасте
	вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000,
    отсортировать вывод по любому полю.
	вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах по городу, профессии, и возрасту:
    18<age<25 & 50<age<65
	произвольный запрос с $match, $group, $sort
"""
import pickle
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

# вывод минимальной, средней, максимальной salary
def get_stats_by_salary(collection):
    data = []

    q = [
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

# вывод количества данных по представленным профессиям
def get_freq_by_job(collection):
    data = []

    q = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]

    for freq in collection.aggregate(q):
        data.append(freq)

    return data

"""вывод минимальной, средней, максимальной salary по городу
	вывод минимальной, средней, максимальной salary по профессии
	вывод минимального, среднего, максимального возраста по городу
	вывод минимального, среднего, максимального возраста по профессии"""
def get_value_stats_by_column(collection, value_name, column_name):
    data = []

    q = [
        {
            "$group": {
                "_id": f"${column_name}",
                "max": {"$max": f"${value_name}"},
                "min": {"$min": f"${value_name}"},
                "avg": {"$avg": f"${value_name}"}
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data


# вывод максимальной заработной платы при минимальном возрасте
def get_max_salary_by_min_age(collection):
    data = []

    q = [
        {
            "$match": {
                "age": 18
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {"$min": "$age"},
                "max_salary": {"$max": "$salary"}
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

# вывод минимальной заработной платы при максимальной возрасте
def get_min_salary_by_max_age(collection):
    data = []

    q = [
        {
            "$group": {
                "_id": "$age",
                "min_salary": {"$min": "$salary"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "max_age": {"$max": "$_id"},
                "min_salary": {"$min": "$min_salary"}
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

# вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000,
# сортировка вывод по городу
def get_filter_age_stats_by_column(collection, column_name):
    data = []

    q = [
        {
            "$match": {
                "salary": {"$gt": 50000}
            }
        },
        {
            "$group": {
                "_id": f"${column_name}",
                "max": {"$max": "$age"},
                "min": {"$min": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

"""вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах 
по городу, профессии, и возрасту:
18<age<25 & 50<age<65"""
def get_filter_salary_stats_by_city_job_age(collection):
    data = []

    q = [
        {
            "$match": {
                "city": {"$in": ['Москва', 'Минск', 'Краков']},
                "job": {"$in": ['Продавец', 'Водитель', 'Учитель']},
                "$or": [
                    {"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}}
                ]
            }
        },
        {
            "$group": {
                "_id": "result",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

# произвольный запрос
"""вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах 
по году, профессии, и возрасту:
18<age<25 & 50<age<65"""
def get_filter_salary_stats_by_job_age_year(collection):
    data = []

    q = [
        {
            "$match": {
                "job": {"$in": ['Продавец', 'Водитель', 'Учитель']},
                "$or": [
                    {"age": {"$gt": 18, "$lt": 25}},
                    {"age": {"$gt": 50, "$lt": 65}},
                    {"year": {"$gte": 2010, "$lte": 2013}},
                    {"year": {"$gte": 2019, "$lte": 2022}}
                ],

            }
        },
        {
            "$group": {
                "_id": "$job",
                "min": {"$min": "$salary"},
                "max": {"$max": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    for stat in collection.aggregate(q):
        data.append(stat)

    return data

# Загрузка входных данных
def load_data(filename):
    with open(filename, 'rb') as file:
        data = pickle.load(file)
    return data


data = load_data('task_2_item.pkl')

# вставка в БД
# insert_many(connect_mongo(), data)

# сохранение в json
def save_in_json(dictionary, name):
    with open(f"{name}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(dictionary, indent=4))

save_in_json(get_stats_by_salary(connect_mongo()), 'stats_by_salary')
save_in_json(get_freq_by_job(connect_mongo()), 'freq_by_job')
save_in_json(get_value_stats_by_column(connect_mongo(), 'salary', 'city'), 'salary_stats_by_city')
save_in_json(get_value_stats_by_column(connect_mongo(), 'salary', 'job'), 'salary_stats_by_job')
save_in_json(get_value_stats_by_column(connect_mongo(), 'age', 'city'), 'age_stats_by_by_city')
save_in_json(get_value_stats_by_column(connect_mongo(), 'age', 'job'), 'age_stats_by_by_job')
save_in_json(get_max_salary_by_min_age(connect_mongo()), 'max_salary_by_min_age')
save_in_json(get_min_salary_by_max_age(connect_mongo()), 'min_salary_by_max_age')
save_in_json(get_filter_age_stats_by_column(connect_mongo(), 'city'), 'filter_age_stats_by_city')
save_in_json(get_filter_salary_stats_by_city_job_age(connect_mongo()), 'filter_salary_stats_by_city_job_age')
save_in_json(get_filter_salary_stats_by_job_age_year(connect_mongo()), 'filter_salary_stats_by_job_age_year')