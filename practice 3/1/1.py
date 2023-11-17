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


with zipfile.ZipFile("zip_var_23.zip", "r") as zip_ref:
    file_list = zip_ref.namelist()

    for filename in file_list:
        with zip_ref.open(filename) as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, "html.parser")

        category = (soup.find("div", class_="book-wrapper").find_all_next("div")[0].find_next("span")
            .get_text(strip=True).split(":")[1].strip())

        if category != "":
            freq[category] = freq.get(category, 0) + 1

        book_title = soup.find("h1", class_="book-title").get_text(strip=True)
        author = soup.find("p", class_="author-p").get_text(strip=True)
        pages = int(soup.find("span", class_="pages").get_text(strip=True).split()[1])
        year = int(soup.find("span", class_="year").get_text(strip=True).split()[-1])
        isbn = (soup.find("div", class_="book-wrapper").find_all_next("div")[2].find_all_next("span")[2]
            .get_text(strip=True).split(":")[1].strip())
        description = (soup.find("div", class_="book-wrapper").find_all_next("div")[2].find_next("p")
            .get_text(strip=True).split("Описание", 1)[1].strip())
        img_src = soup.find("img")["src"]
        rating = float(soup.find("div", class_="book-wrapper").find_all_next("div")[4].find_all_next("span")[0]
            .get_text(strip=True).split(":")[1].strip())
        f_max, f_min, f_non_empty, f_sum = handle_int_value(rating, f_max, f_min, f_non_empty, f_sum)

        views = int(soup.find("div", class_="book-wrapper").find_all_next("div")[4].find_all_next("span")[1]
            .get_text(strip=True).split(":")[1].strip())

        book_data = {"category": category, "title": book_title, "author": author, "pages": pages, "year": year,
            "isbn": isbn, "description": description, "img_src": img_src, "rating": rating, "views": views,}

        data.append(book_data)


sorted_f = sorted(data, key=lambda x: x["pages"])

filtered_f = list(filter(lambda x: x["year"] > 1999, sorted_f))

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False, ))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_f, indent=2, ensure_ascii=False, ))

avr = f_sum / f_non_empty
s = 0

for d in data:
    if d["views"] >= 0:
        s += (d["views"] - avr) ** 2

dev = (s / f_non_empty) ** 0.5

with open("stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps({"sum": f_sum, "min": f_min, "max": f_max, "average": avr, "deviation": dev, }, indent=2,))

freq_sorted = dict(sorted(freq.items(), key=lambda x: x[1], reverse=True))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq_sorted, ensure_ascii=False, indent=2))
