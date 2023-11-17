import xml.etree.ElementTree as ET
import json
from zipfile import ZipFile
from statistics import mean, stdev

all_data = []
freq = {}

with ZipFile("zip_var_23.zip", "r") as zip_ref:
    file_list = zip_ref.namelist()

    for filename in file_list:
        with zip_ref.open(filename) as file:
            html_content = file.read()

        root = ET.fromstring(html_content)

        clothing_data = []

        for clothing_elem in root.findall(".//clothing"):
            clothing_id = clothing_elem.findtext("id")
            name = clothing_elem.findtext("name")
            category = clothing_elem.findtext("category")
            size = clothing_elem.findtext("size")
            color = clothing_elem.findtext("color")
            material = clothing_elem.findtext("material")
            price_str = clothing_elem.findtext("price")
            rating_str = clothing_elem.findtext("rating")
            reviews_str = clothing_elem.findtext("reviews")
            sporty = clothing_elem.findtext("sporty")

            clothing_id = clothing_id.strip() if clothing_id is not None else None
            name = name.strip() if name is not None else None
            category = category.strip() if category is not None else None
            size = size.strip() if size is not None else None
            color = color.strip() if color is not None else None
            material = material.strip() if material is not None else None
            price = int(price_str) if price_str is not None else None
            rating = float(rating_str) if rating_str is not None else None
            reviews = int(reviews_str) if reviews_str is not None else None
            sporty = sporty.strip() if sporty is not None else None

            if material:
                freq[material] = freq.get(material, 0) + 1

            data = {"id": clothing_id, "name": name, "category": category, "size": size, "color": color,
                "material": material, "price": price, "rating": rating, "reviews": reviews, "sporty": sporty,}

            all_data.append(data)

sorted_f = sorted(all_data, key=lambda x: x["category"])

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False))

filtered_f = list(filter(lambda x: x["rating"] > 4.95, sorted_f))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_f, indent=2, ensure_ascii=False))

values = [item.get("reviews", 0) for item in all_data]
f_sum = sum(values)
f_min = min(values)
f_max = max(values)
f_avg = mean(values)
f_stdev = stdev(values)

sats = {"sum": f_sum, "min": f_min, "max": f_max, "avg": f_avg, "stdev": f_stdev}

with open("sats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sats, indent=2, ensure_ascii=False))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq, indent=2, ensure_ascii=False))
