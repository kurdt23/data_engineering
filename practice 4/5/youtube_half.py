""" Самостоятельно выбрать предметную область. Подобрать пару наборов данных разных форматов. Создать
базу данных минимум на три таблицы. Заполнение данных осуществляем из файлов. Реализовать
выполнение 6-7 запросов к базе данных с выводом результатов в json. Среди них могут быть:
	выборка с простым условием + сортировка + ограничение количество
	подсчет объектов по условию, а также другие функции агрегации
	группировка
	обновление данных
В решении необходимо указать:
	название и описание предметной области (осмысленное)
	SQL для создания таблиц
	файлы исходных данных (можно обрезать до такого размера, чтобы влезли в GitHub)
	скрипт для инициализации базы данных (создание таблиц)
	скрипт для загрузки данных из файлов в базу данных
	файл базы данных
	скрипт с выполнением запросов к базе данных """

"""Global YouTube Statistics 2023
A collection of YouTube giants, this dataset offers a perfect avenue to analyze and gain valuable 
insights from the luminaries of the platform.
https://www.kaggle.com/datasets/nelgiriyewithana/global-youtube-statistics-2023?resource=download

Сначала разделила базу данных Global YouTube Statistics.csv на две половины и записала в csv и json: youtube_half.py
Далее работала с half1.csv half2.json

rank: уникальной номер для канала

Были использованы столбцы:
rank: Position of the YouTube channel based on the number of subscribers
# Youtuber: Name of the YouTube channel
# subscribers: Number of subscribers to the channel
# category: Category or niche of the channel
# Title: Title of the YouTube channel
# uploads: Total number of videos uploaded on the channel

# # # Country: Country where the YouTube channel originates
# # # Abbreviation: Abbreviation of the country

# channel_type: Type of the YouTube channel (e.g., individual, brand)

# # video_views_rank: Ranking of the channel based on total video views

# # # country_rank: Ranking of the channel based on the number of subscribers within its country

# # channel_type_rank: Ranking of the channel based on its type (individual or brand)
# # video_views_for_the_last_30_days: Total video views in the last 30 days
# # subscribers_for_last_30_days: Number of new subscribers gained in the last 30 days

# created_year: Year when the YouTube channel was created
# created_month: Month when the YouTube channel was created
# created_date: Exact date of the YouTube channel's creation

# # # Gross tertiary education enrollment (%): Percentage of the population enrolled in tertiary education in the country
# # # Population: Total population of the country
# # # Unemployment rate: Unemployment rate in the country
# # # Urban_population: Percentage of the population living in urban areas


Пропущены столбцы:
video views: Total views across all videos on the 
lowest_monthly_earnings: Lowest estimated monthly earnings from the channel
highest_monthly_earnings: Highest estimated monthly earnings from the channel
lowest_yearly_earnings: Lowest estimated yearly earnings from the channel
highest_yearly_earnings: Highest estimated yearly earnings from the channel
Latitude: Latitude coordinate of the country's location
Longitude: Longitude coordinate of the country's 

# использовались в таблице channel
# # использовались в таблице channel_parameters
# # # использовались в таблице country"""

import csv
import json

# Чтение CSV файла
with open('Global YouTube Statistics.csv', 'r', newline='', encoding='windows-1251') as file:
    reader = csv.DictReader(file)
    data = list(reader)

# Замена значений nan на пустые строки
for row in data:
    for key, value in row.items():
        if value.lower() == 'nan':
            row[key] = ''

# Разделение на две половины
half1 = data[:497]
half2 = data[497:]

# Запись первой половины в CSV файл
with open('half1.csv', 'w', newline='', encoding='utf-8') as file:
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(half1)

# Запись второй половины в JSON файл
with open('half2.json', 'w', encoding='utf-8') as file:
    json.dump(half2, file, ensure_ascii=False)