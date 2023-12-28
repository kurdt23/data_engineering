import collections
import json
import os
import zipfile

import pandas as pd
from bs4 import BeautifulSoup

with zipfile.ZipFile('zip_var_23.zip', 'r') as zip_ref:
	zip_ref.extractall()
items = []
for filename in os.listdir():
	if filename.endswith(".xml"):
		with open(filename, "r", encoding="utf-8") as file:
			text = ""
			for row in file.readlines():
				text += row
			root = BeautifulSoup(text, 'xml')


			for clothing in root.find_all("clothing"):
				item = dict()
				for el in clothing.contents:
					if el.name is None:
						continue
					elif el.name == "price" or el.name == "reviews":
						item[el.name] = int(el.get_text().strip())
					elif el.name == "price" or el.name == "rating":
						item[el.name] = float(el.get_text().strip())
					elif el.name == "new":
						item[el.name] = el.get_text().strip() == "+"
					elif el.name == "exclusive" or el.name == "sporty":
						item[el.name] = el.get_text().strip() == "yes"
					else:
						item[el.name] = el.get_text().strip()


				items.append(item)

items = sorted(items, key=lambda x: x['category'])

with open("sorted1.json", "w", encoding="utf-8") as f:
	f.write(json.dumps(items, indent=2, ensure_ascii=False))

filtered_items = []
for color in items:
	if color["rating"] > 4.95:
		filtered_items.append(color)

result = []

df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

stats = df['reviews'].agg(['sum', 'min', 'max', 'mean', 'std']).to_dict()
result.append(stats)

result2 = []

material = [item['material'] for item in items]
f1 = collections.Counter(material)
result2.append(f1)

with open("filtered1.json", "w", encoding="utf-8") as f:
	f.write(json.dumps(filtered_items, indent=4, ensure_ascii=False))

with open("stats1.json", "w", encoding="utf-8") as f:
	f.write(json.dumps(result, indent=4,  ensure_ascii=False))

with open("freq1.json", "w", encoding="utf-8") as f:
	f.write(json.dumps(result2, indent=4,  ensure_ascii=False))

for filename in os.listdir():
	if filename.endswith(f'.xml'):
		os.remove(filename)
