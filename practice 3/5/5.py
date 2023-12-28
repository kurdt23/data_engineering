import zipfile
import json
import sys
from bs4 import BeautifulSoup

data = []
freq = {}

f_max = 0
f_min = sys.maxsize
f_non_empty = 0
f_sum = 0


def handle_int_value(value: float, max: float, min: float, non_empty: float, sum: float):
    if value >= 0:
        non_empty += 1
        sum += value
        if value > max:
            max = value
        if value < min:
            min = value
    return max, min, non_empty, sum


with zipfile.ZipFile("site1.zip", "r") as zip_ref:
    file_list = zip_ref.namelist()

    for filename in file_list:
        with zip_ref.open(filename) as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, "html.parser")

        articul = (soup.find("div", class_="product__info product__serial").get_text(strip=True).split(":")[1].strip())
        quanity = soup.find("div", class_="product__info product__quanity").get_text(strip=True)
        status = soup.find("div", {"class": "text"})
        if status is not None:
            status = status.get_text()
        elif status is None:
            status = "нет статуса"
        price = int(soup.find("span", class_="product-price-data").get_text(strip=True).replace(" ", "").strip())
        name = soup.find("title").get_text(strip=True)

        if quanity == "":
            quanity = "В наличии"

        if status != "":
            freq[status] = freq.get(status, 0) + 1

        f_max, f_min, f_non_empty, f_sum = handle_int_value(price, f_max, f_min, f_non_empty, f_sum)

        book_data = {"Название": name, "Артикул": articul, "Наличие": quanity, "Статус": status, "Цена": price}

        data.append(book_data)

sorted_f = sorted(data, key=lambda x: x["Артикул"])

filtered_f = list(filter(lambda x: x["Статус"] == "скоро в наличии", sorted_f))

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_f, indent=2, ensure_ascii=False))

avr = f_sum / f_non_empty
s = 0

for d in data:
    if d["Цена"] >= 0:
        s += (d["Цена"] - avr) ** 2

dev = (s / f_non_empty) ** 0.5

with open("stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps({"sum": f_sum, "min": f_min, "max": f_max, "average": avr, "deviation": dev}, indent=2))

freq_sorted = dict(sorted(freq.items(), key=lambda x: x[1], reverse=True))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq_sorted, ensure_ascii=False, indent=2))
