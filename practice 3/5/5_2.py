import json
import zipfile
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


with zipfile.ZipFile("site2.zip", "r") as zip_ref:
    file_list = zip_ref.namelist()

    for filename in file_list:
        with zip_ref.open(filename) as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, "html.parser")

        for p_div in soup.find_all("div", class_="catalog__product"):
            p_data = {}
            p_data["id"] = int(p_div.find_all("div")[0]["id"].replace("item", ""))
            p_data["link"] = p_div.find_all("a")[1]["href"]
            p_data["name"] = p_div.find("div", class_="product-item__link").get_text(strip=True)
            p_data["price"] = int(
                p_div.find("div", class_="product-item-price").get_text(strip=True).replace(" ", "").replace("₽", ""))
            p_data["status"] = p_div.find("div", {"class": "text"})

            if p_data["status"] is not None:
                p_data["status"] = p_data["status"].get_text()
            elif p_data["status"] is None:
                p_data["status"] = "нет статуса"

            # подсчет вхождений status

            if p_data["status"] != "":
                freq[p_data["status"]] = freq.get(p_data["status"], 0) + 1

            # числовые расчеты для цены
            f_max, f_min, f_non_empty, f_sum = handle_int_value(p_data["price"], f_max, f_min, f_non_empty, f_sum)

            data.append(p_data)

# сортировка по алфавиту имени

sorted_f = sorted(data, key=lambda x: x["name"])

# фильтрация по  54430000 < id < 54567166

filtered_f = list(filter(lambda x: x["id"] > 54430000 and x["id"] < 54567166, sorted_f))

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_f, indent=2, ensure_ascii=False))

# числовые расчеты для цены

avr = f_sum / f_non_empty
s = 0

for d in data:
    if d["price"] >= 0:
        s += (d["price"] - avr) ** 2

dev = (s / f_non_empty) ** 0.5

with open("stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps({"sum": f_sum, "min": f_min, "max": f_max, "average": avr, "deviation": dev}, indent=2))

freq_sorted = dict(sorted(freq.items(), key=lambda x: x[1], reverse=True))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq_sorted, ensure_ascii=False, indent=2))
