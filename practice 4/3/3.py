import sqlite3
import json
import pickle

# данные из text файла
def parce_data(data_1):
    items = []
    with open(data_1, 'r', encoding='utf-8') as file:
        data = file.readlines()
        item = dict()
        for i in data:
            if i == '=====\n':
                items.append(item)
                item = dict()
            else:
                i = i.strip()
                splitted = i.split('::')
                if splitted[0] == 'duration_ms' or splitted[0] == 'year':
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == 'tempo':
                    item[splitted[0]] = float(splitted[1])
                elif  splitted[0] == 'instrumentalness'  or splitted[0] == 'explicit' or splitted[0] == 'loudness':
                    continue
                else:
                    item[splitted[0]] = splitted[1]
    print(items)
    return items

# данные из pkl файла
data = list()
def parce_data_pk(data_2):
    with open(data_2, 'rb') as pk_file:
        data_mp = pickle.load(pk_file)
        for item in data_mp:
            song_data = {
                "artist": item["artist"],
                "song": item["song"],
                "duration_ms": int(item["duration_ms"]),
                "year": int(item["year"]),
                "tempo": float(item["tempo"]),
                "genre": item["genre"],
                # "acousticness": float(item["acousticness"]),
                # "energy": float(item["energy"]),
                # "popularity": int(item["popularity"]),
            }
            data.append(song_data)
    print(data_mp)
    return data

# подключаемся к базе данных
def connect_to_db(elem):
    connection = sqlite3.connect(elem)
    connection.row_factory = sqlite3.Row
    return connection

# записываем выделенные общие хранимые данные в бд
def insert_price(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO music (artist, song, duration_ms, year, tempo, genre)
        VALUES(
        :artist, :song, :duration_ms, :year, :tempo, :genre
        )
    """, data)
    db.commit()

# вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json
# по темпу tempo
def get_top_by_tempo(db, limit):
    cursor = db.cursor()
    result = cursor.execute("SELECT * FROM music ORDER BY tempo LIMIT ?", [limit])
    items = [dict(row) for row in result.fetchall()]
    with open(f'result_4_3_order_tempo.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, indent=2, ensure_ascii=False))
    cursor.close()
    return items

# вывод (сумму, мин, макс, среднее) по произвольному числовому полю
# по длительность в мс duration_ms
def min_max(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
            SUM(duration_ms) as sum,
            AVG(duration_ms) as avg,
            MIN(duration_ms) as min,
            MAX(duration_ms) as max
        FROM music
        """)
    items = dict(result.fetchone())
    cursor.close()
    with open(f'result_4_3_max_duration_ms.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, indent=2, ensure_ascii=False))
    return []

# вывод частоты встречаемости для категориального поля
# по исполнителям artist
def get_occuerrence(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
        COUNT(*) AS frequency,
        artist as artist
        FROM music
        GROUP BY artist
        """)
    # items = dict(result.fetchall())
    items = [dict(row) for row in result.fetchall()]
    cursor.close()
    with open(f'result_4_3_occuerrence_artist.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, indent=2, ensure_ascii=False))
    return []


# вывод первых (VAR+15) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю
# строк из таблицы в файл формате json

# отфильтрован по жанру рок, отсортирован по году
def get_sort_year(db, min_rating, limit):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT * 
        FROM music 
        WHERE genre = ?
        ORDER BY year
        LIMIT ?
        """, [min_rating, limit])
    items = [dict(row) for row in result.fetchall()]
    cursor.close()
    with open(f'result_4_3_filter_genre.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, indent=2, ensure_ascii=False))
    return items

# вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.
item_1 = parce_data('task_3_var_23_part_2.text')
item_2 = parce_data_pk('task_3_var_23_part_1.pkl')
db = connect_to_db('db_3.db')
items = item_1 + item_2
# insert_price(db, items)

# limit = VAR + 10 = 23 + 10 = 33
get_top_by_tempo(db, 33)
min_max(db)
get_occuerrence(db)

# limit = VAR + 15 = 23 + 15 = 38
get_sort_year(db, 'rock', 38)