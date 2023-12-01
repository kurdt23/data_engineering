import json
import sqlite3


def parse_data(file_name):
    items = []
    with open(file_name, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = dict()
            else:
                line = line.strip()
                splitted = line.split('::')

                if splitted[0] == 'price':
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]
    return items

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO books2 (books_id, title, price, place, date)
        VALUES(
            (SELECT id FROM books WHERE title = :title), :title, :price, :place, :date
            )
        """, data)
    db.commit()

def first_query(db, name):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT *
        FROM books2
        WHERE books_id = (SELECT id FROM books WHERE author = ?)
        """, [name])
    print(dict(result.fetchone()))
    cursor.close()
    return []

def second_query(db, name):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
            AVG(price) as avg_price
        FROM books2
        WHERE books_id = (SELECT id FROM books WHERE author = ?)
        """, [name])
    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)
    cursor.close()
    print(items)


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
            author,
            (SELECT COUNT(*) FROM book2 WHERE id = books_id) as title
        FROM books
        ORDER BY title
        LIMIT 10
        """)
    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)
    cursor.close()
    print(items)
items = parse_data('task_2_var_23_subitem.text')
db = connect_to_db('db_2.db')
insert_data(db, items)
first_query(db, 'Майн Рид')
second_query(db, 'Майн Рид')
third_query(db)