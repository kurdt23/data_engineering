import msgpack
import sqlite3
import json


# данные из msgpack файла, информация о некоторых товарах
def load_products(filename):
    with open(filename, mode='br') as file:
        data = msgpack.load(file)
        for product in data:
            if 'category' in product:
                continue
            else:
                product['category'] = None
        return data


# данные из text файла, информация об изменениях
def load_updates(filename):
    with open(filename, mode='r', encoding='utf-8') as file:
        items = []
        item = {}
        for line in file:
            if '=====' in line:
                items.append(item)
                item = {}
            else:
                split = line.strip().split('::')
                if split[0] == 'param':
                    if split[1].lstrip('-').isnumeric():
                        item['param'] = float(split[1])
                    elif split[1] == 'False' or split[1] == 'True':
                        item['param'] = split[1] == 'True'
                    else:
                        item['param'] = ''
                else:
                    item[split[0]] = split[1]
        return items


# вставка полученных данных о товарах из msgpack файла в таблицу базы данных
def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""INSERT INTO products(name, price, quantity, category, fromCity, isAvailable, views) 
        VALUES(:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)""", data)
    db.commit()

    cursor.close()


# подключаемся к базе данных
def connect_to_db(filename):
    connection = sqlite3.connect(filename)
    connection.row_factory = sqlite3.Row
    return connection


# общая функция для выполнения запросов query после записи всех данных и обновлении
def execute_query(db, query, params=[], name=None):
    cursor = db.cursor()
    result = cursor.execute(query, params)
    if name is not None:
        cursor.execute('update products set version = version + 1 where name = ?', [name])
    data = result.fetchall()
    db.commit()
    cursor.close()
    return data


# удаление товара по имени, method::remove
def delete_by_name(db, name):
    query = "delete from products where name = ?"
    params = [name]
    execute_query(db, query, params)


# обновление цены в процентах, method::price_percent
def update_price_percent(db, name, percent):
    query = "update products set price = price * (1 + ?) where name = ?"
    params = [percent, name]
    execute_query(db, query, params, name)


# обновление цены по абсолютной величине, method::price_abs
def update_price_abs(db, name, sum):
    query = "update products set price = price + ? where name = ? and (price + ?) >= 0"
    params = [sum, name, sum]
    execute_query(db, query, params, name)


# обновление количества товаров, method::quantity_sub method::quantity_add
def update_quantity(db, name, quantity):
    query = "update products set quantity = quantity + ? where name = ? and (quantity + ?) >= 0"
    params = [query, name, quantity]
    execute_query(db, query, params, name)


# обновление доступности товара, method::available
def update_available(db, name, isAvailable):
    query = "update products set isAvailable = ? where name = ?"
    params = [isAvailable, name]
    execute_query(db, query, params, name)


# обновление всех товаров в соответсвтии с методами из text файла
def update_products(db, updates):
    for update in updates:
        name = update['name']
        param = update['param']
        method = update['method']

        if method == 'remove':
            delete_by_name(db, name)
        elif method == 'price_percent':
            update_price_percent(db, name, param)
        elif method == 'price_abs':
            update_price_abs(db, name, param)
        elif method == 'quantity_add' or method == 'quantity_sub':
            update_quantity(db, name, param)
        elif method == 'available':
            update_available(db, name, param)


# общая функция для записи json
def write_data(filename, data):
    with open(filename, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


# вывод топ-10 самых обновляемых товаров
def query_1(db, filename, limit=10):
    query = 'select * from products order by version desc limit ?'
    params = [limit]
    ordered_data = execute_query(db, query, params)
    result = [dict(row) for row in ordered_data]

    write_data(filename, result)


# проанализировать цены товаров, найдя (сумму, мин, макс, среднее) для каждой группы, а также количество товаров в группе
def query_2(db, filename):
    query = """select category, sum(price) as sum, min(price) as min, max(price) as max, avg(price), count() as count
        from products group by category"""
    stats = execute_query(db, query)
    result = [dict(row) for row in stats]

    write_data(filename, result)

# проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров
def query_3(db, filename):
    query = """select category, sum(quantity) as sum, min(quantity) as min, max(quantity) as max, avg(quantity), 
    count() as count from products group by category"""
    groups = execute_query(db, query)
    result = [dict(row) for row in groups]

    write_data(filename, result)

# произвольный запрос quantity < 3500 и упорядочить по просмотрам от (большиего к меньшего) и вывести первые 5
def query_4(db, filename, quantity, limit=76):
    query = 'select * from products where quantity < ? order by views desc limit ?'
    params = [quantity, limit]
    filtered_data = execute_query(db, query, params)
    result = [dict(row) for row in filtered_data]

    write_data(filename, result)


connection = connect_to_db('db_4.db')
data = load_products('task_4_var_23_product_data.msgpack')
# insert_data(connection, data)
updates = load_updates('task_4_var_23_update_data.text')
# update_products(connection, updates)
query_1(connection, 'top_by_version.json')
query_2(connection, 'price_stats.json')
query_3(connection, 'quantity_stats.json')
query_4(connection, 'filtered_by_quantity.json', 3500, 5)
