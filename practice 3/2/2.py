import json
import zipfile
import sys
from bs4 import BeautifulSoup
import re

data = []
data_freq = {}

ram_max = 0
ram_min = sys.maxsize
ram_non_empty = 0
ram_sum = 0

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

        for p_div in soup.find_all("div", class_="pad"):
            p_data = {}

            p_data["data_id"] = p_div.find("a", class_="add-to-favorite")["data-id"]
            p_data["link"] = p_div.find_all("a")[1]["href"]
            p_data["img_url"] = p_div.find_all("img")[0]["src"]
            p_data["name"] = p_div.find("span").get_text(strip=True)
            p_data["price"] = int(p_div.find("price").get_text(strip=True).replace(" ", "")[:-1])
            bonus_strong = p_div.find("strong")
            p_data["bonus"] = int(bonus_strong.get_text(strip=True).split()[2])

            ul_data = {}
            ul = p_div.find("ul")
            if ul:
                for li in ul.find_all("li"):
                    type_name = li["type"]
                    text = li.get_text(strip=True)
                    if type_name == "sim" or type_name == "ram" or type_name == "camera" or type_name == "acc":
                        match = re.match(r"\d+", text)
                        if match:
                            ex_number = int(match.group())
                            ul_data[type_name] = ex_number
                            if type_name == "ram":
                                (ram_max, ram_min, ram_non_empty, ram_sum) = handle_int_value(ex_number, ram_max,
                                                                                              ram_min, ram_non_empty, ram_sum)
                    elif type_name == "resolution" and text != "":
                        data_freq[text] = data_freq.get(text, 0) + 1
                    else:
                        ul_data[type_name] = li.get_text(strip=True)

            p_data.update(ul_data)

            data.append(p_data)

sorted_f = sorted(data, key=lambda x: x["bonus"])

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False))

filtered_f = list(filter(lambda x: x["price"] > 499900, sorted_f))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(
        json.dumps(filtered_f, indent=2, ensure_ascii=False))

avr = ram_sum / ram_non_empty
s = 0

for d in data:
    if "ram" in d:
        s += (d["ram"] - avr) ** 2

dev = (s / ram_non_empty) ** 0.5

with open("stats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps({"sum": ram_sum, "min": ram_min, "max": ram_max, "average": avr, "deviation": dev}, indent=2))

freq_sorted = dict(sorted(data_freq.items(), key=lambda x: x[1], reverse=True))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq_sorted, ensure_ascii=False, indent=2))
