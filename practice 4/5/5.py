# комментарии в youtube_half.py
import csv
import json
import sqlite3


# преобразование данных
def parse_dict(row):
    new_row = {}

    for key, val in row.items():
        if not key:
            continue
        n_key = key.strip()\
            .replace('Gross tertiary education enrollment (%)', 'Gross_tertiary_education_enrollment')\
            .replace('Unemployment rate', 'Unemployment_rate')
        # if key in row:
        #     new_row[n_key] = val
        #     if row[key] == 'nan':
        #         new_row[n_key] = ''
        new_row[n_key] = val
        if row[key] == '':
            new_row[n_key] = None
        elif key in ['rank', 'subscribers', 'uploads', 'video_views_rank', 'country_rank',
                   'channel_type_rank', 'video_views_for_the_last_30_days', 'subscribers_for_last_30_days',
                   'created_year', 'created_date', 'Population', 'Urban_population']:
            # if row[key] == '':
            #     new_row[n_key] = None
            if row[key]:
                # print(key, new_row[n_key])
                new_row[n_key] = int(val)
            # else:
            #     continue
        elif key in ['Gross_tertiary_education_enrollment', 'Unemployment_rate']:
            # if row[key] == '':
            #     new_row[n_key] = None
            if row[key]:
                new_row[n_key] = float(val)
            # else:
            #     continue
        elif key in ['video views', 'lowest_monthly_earnings', 'highest_monthly_earnings', 'lowest_yearly_earnings',
                     'highest_yearly_earnings', 'Latitude', 'Longitude']:
            continue
        else:
            new_row[n_key] = val
    return new_row

# данные из csv файла
def load_data_csv(filename):
    data = []
    with open(filename, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            new_row = parse_dict(row)
            data.append(new_row)


    return data

# данные из json файла
def load_data_json(file_name):
    data = []
    with open(file_name, "r", encoding='utf-8') as f:
        json_reader = json.load(f)

        for row in json_reader:
            new_row = parse_dict(row)
            data.append(new_row)

    return data

# подключаемся к базе данных
def connect_to_db(filename):
    connection = sqlite3.connect(filename)
    # connection.set_trace_callback(print)
    connection.row_factory = sqlite3.Row
    return connection

# 1. вставка в таблицу channel базы данных краткой информации о каналах
def insert_data_channel(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO channel (
            rank, Youtuber, subscribers, category, Title, uploads,
            channel_type, created_year, created_month, created_date
        )
        VALUES(
            :rank, :Youtuber, :subscribers, :category, :Title, :uploads,
            :channel_type, :created_year, :created_month, :created_date
        )
        """, data)

    db.commit()

# 2. вставка в таблицу channel_parameter базы данных информации о рейтингах и статистике за 30 дней каналов
def insert_data_channel_parameters(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO channel_parameters (
            rank, Title, category, video_views_rank, channel_type_rank, video_views_for_the_last_30_days,
            subscribers_for_last_30_days
        )
        VALUES (
            :rank, :Title, :category, :video_views_rank, :channel_type_rank, :video_views_for_the_last_30_days,
            :subscribers_for_last_30_days
        )
        """, data)

    db.commit()

# 3. вставка в таблицу country базы данных информации о странах каналов
def insert_data_country(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO country (
            rank, Title, Country, Abbreviation,
            country_rank, Population, Gross_tertiary_education_enrollment,
            Unemployment_rate, Urban_population
        )
        VALUES (
            :rank, :Title, :Country, :Abbreviation,
            :country_rank, :Population, :Gross_tertiary_education_enrollment,
            :Unemployment_rate, :Urban_population
        )
        """, data)

    db.commit()

# 1. вывод топ 30 каналов с одним загруженным видео, отсортированных по названию канала
def get_top_channel_by_min_uploads(db, min_uploads, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM channel WHERE uploads = ? ORDER BY Title LIMIT ?",
                         [min_uploads,limit])
    items = [dict(row) for row in res]
    cursor.close()
    return items

# 2. вывод первых 5-ти каналов сгруппирированных по category = Gaming, channel_type = Games, created_year = 2012,
# rank <= 666 и отсортированных по наибольшему кол-ву видео
def get_groups_category_channel_type(db, limit):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * FROM channel 
        WHERE category = ? AND channel_type = ? AND created_year = ? AND rank <= ? ORDER BY uploads DESC LIMIT ?""",
        ("Gaming", "Games", 2012, 666, limit))
    items = [dict(row) for row in res]
    cursor.close()
    return items

# 3. статистика просмотров за последнии 30 дней, группировка по указанной категории канала, сортировка по уменьшению AVG
def get_stat_views_last_30_days(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT category,
            SUM(video_views_for_the_last_30_days) as sum,
            MIN(video_views_for_the_last_30_days) as min,
            MAX(video_views_for_the_last_30_days) as max,
            AVG(video_views_for_the_last_30_days) as avg
        FROM channel_parameters
        WHERE category != ''
        GROUP BY category
        ORDER BY AVG(video_views_for_the_last_30_days) DESC
    """)

    items = [dict(row) for row in res]
    cursor.close()
    return items

# 4. статистика подписчиков за последнии 30 дней, группировка по указанной категории канала, сортировка по уменьшению AVG
def get_stat_subscribers_last_30_days(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            category,
            SUM(subscribers_for_last_30_days) as sum,
            MIN(subscribers_for_last_30_days) as min,
            MAX(subscribers_for_last_30_days) as max,
            AVG(subscribers_for_last_30_days) as avg
        FROM channel_parameters
        WHERE category != ''
        GROUP BY category
        ORDER BY AVG(subscribers_for_last_30_days) DESC
    """)

    items = [dict(row) for row in res]
    cursor.close()
    return items

# 5. обновление статуса страны для канала на 625 месте
def update_country_status(db):
    cursor = db.cursor()
    cursor.execute("UPDATE country SET Country = ?, Abbreviation = ? WHERE rank = ?", ['Russia', 'RU', 625])
    db.commit()
    cursor.close()

# 6. Частота встречаемости страны, сортировка по уменьшению населения
def get_freq_country_by_population(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) AS frequency,
            Country as Country,
            Abbreviation as Abbreviation,
            Urban_population as Urban_population,
            Gross_tertiary_education_enrollment as Gross_tertiary_education_enrollment,
            Unemployment_rate as Unemployment_rate
            FROM country
            WHERE Country != ''
            GROUP BY Country
            ORDER BY Population DESC     
    """)

    items = [dict(row) for row in res]
    cursor.close()
    return items

# 7. Частота встречаемости категории канала, сортировка по уменьшению общего кол-ва отдельной категории
def get_freq_category(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            CAST(count(*) as REAL) / (SELECT COUNT(*) FROM channel_parameters) as frequency,
            COUNT(category) as count,
            category
        FROM channel_parameters
        WHERE category != ''
        GROUP BY category
        ORDER BY count DESC
    """)

    items = [dict(row) for row in res]
    cursor.close()
    return items

# общая функция для записи выводов json
def save_in_json(dictionary, name):
    with open(f"{name}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(dictionary, indent=4, ensure_ascii=False))

data_json = load_data_json('half2.json')  # Данные о ютубканалах 2019 в json
data_csv = load_data_csv('half1.csv')  # Данные о ютубканалах 2019 в csv
data = data_csv + data_json  # Объединение данных отчетов
# print(data_csv[5])
# # print(data_json[1])
data_base = connect_to_db('youtube.db')  # База данных sqlite

# insert_data_channel(data_base, data)  # вставка данных в таблицу channel
# insert_data_channel_parameters(data_base, data)  # вставка данных в таблицу channel_parameters
# insert_data_country(data_base, data)  # вставка данных в таблицу country

save_in_json(get_top_channel_by_min_uploads(data_base, 1, 30), 'top_channel_by_min_uploads')

save_in_json(get_groups_category_channel_type(data_base, 5), 'groups_category_channel_type')

save_in_json(get_stat_views_last_30_days(data_base), 'stat_views_last_30_days')

save_in_json(get_stat_subscribers_last_30_days(data_base), 'stat_subscribers_last_30_days')

save_in_json(get_freq_country_by_population(data_base), 'freq_country_by_population')

save_in_json(get_freq_category(data_base), 'freq_category')

update_country_status(data_base)