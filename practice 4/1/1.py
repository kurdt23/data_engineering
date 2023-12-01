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

                if splitted[0] in ['pages', 'published_year', 'views']:
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == 'rating':
                    item[splitted[0]] = float(splitted[1])
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
        INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views)
        VALUES(
            :title, :author, :genre, :pages, :published_year, :isbn, :rating, :views
        )
    """, data)
    db.commit()


def get_top_by_published_year(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM books ORDER BY published_year DESC LIMIT ?", [limit])
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)
    cursor.close()
    return items

    with open(f'result_4_1_order_published_year.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))


def min_max_pages(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(pages) as sum,
            AVG(pages) as avg,
            MIN(pages) as min,
            MAX(pages) as max
        FROM books""")
    print(dict(res.fetchone()))
    cursor.close()
    return []


def get_freq_genre(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as count,
            genre as genre
            FROM books
            GROUP BY genre""")
    print(dict(res.fetchall()))
    cursor.close()
    return []


def filter_views(db, min_views, limit):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * 
        FROM books 
        WHERE views > ?
        ORDER BY views DESC 
        LIMIT ?
        """, [min_views, limit])
    items = []
    for row in res.fetchall():
        items.append(dict(row))
    cursor.close()
    return items
    with open(f'result_4_1_filter_views.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))


items = parse_data('task_1_var_23_item.text')
db = connect_to_db('db_1.db')
insert_data(db, items)
get_top_by_published_year(db, 33)
min_max_pages(db)
get_freq_genre(db)
filter_views(db, 90000, 33)
# limit = VAR + 10 = 23 + 10 = 33