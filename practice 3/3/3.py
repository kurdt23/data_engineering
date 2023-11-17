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

        name = root.find("name").text.strip()
        constellation = root.find("constellation").text.strip()
        spectral_class = root.find("spectral-class").text.strip()
        radius = int(root.find("radius").text.strip())
        rotation = float(root.find("rotation").text.split()[0])
        age = float(root.find("age").text.split()[0])
        distance = float(root.find("distance").text.split()[0])
        absolute_magnitude = float(root.find("absolute-magnitude").text.split()[0])

        data = {"name": name, "constellation": constellation, "spectral_class": spectral_class, "radius": radius,
            "rotation": rotation, "age": age, "distance": distance, "absolute_magnitude": absolute_magnitude}

        if constellation != "":
            freq[constellation] = freq.get(constellation, 0) + 1

        all_data.append(data)

sorted_f = sorted(all_data, key=lambda x: x["absolute_magnitude"])

with open("sorted.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(sorted_f, indent=2, ensure_ascii=False))

filtered_f = list(filter(lambda x: x["age"] < 3.0, sorted_f))

with open("filtered.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(filtered_f, indent=2, ensure_ascii=False))

values = [item["rotation"] for item in all_data]
f_sum = sum(values)
f_min = min(values)
f_max = max(values)
f_avg = mean(values)
f_stdev = stdev(values)

stats_data = {"sum": f_sum, "min": f_min, "max": f_max, "avg": f_avg, "stdev": f_stdev}

with open("sats.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(stats_data, indent=2, ensure_ascii=False))

with open("freq.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(freq, indent=2, ensure_ascii=False))
