import pandas as pd
import os
import json
import msgpack

# Исходник https://catalog.data.gov/dataset/crime-data-from-2020-to-present/resource/5eb6507e-fa82-4595-a604-023f8a326099?inner_span=True
data = pd.read_csv("Crime_Data_from_2020_to_Present.csv", delimiter=',')

# большинство значений текстовые/обозначают кодировки
selected_fields = data[['TIME OCC', 'AREA NAME', 'Vict Age', 'Vict Sex', 'Vict Descent', 'Weapon Used Cd', 'Status']]

numerical = ['TIME OCC', 'Vict Age']
stats = {}

for values in numerical:
    stats[values] = {
        'min': int(data[values].min()),
        'max': int(data[values].max()),
        'mean': float(data[values].mean()),
        'sum': float(data[values].sum()),
        'std': float(data[values].std())
    }

text_values = ['AREA NAME', 'Vict Sex', 'Vict Descent', 'Weapon Used Cd', 'Status']
frequency = {}
for values in text_values:
    frequency[values] = data[values].value_counts(normalize=True).to_dict()

with open('stats.json', 'w') as json_file:
    json.dump(stats, json_file)

with open('frequency.json', 'w') as json_file:
    json.dump(frequency, json_file)

datat = pd.read_csv("Crime_Data_from_2020_to_Present.csv",
                    usecols=['TIME OCC', 'AREA NAME', 'Vict Age', 'Vict Sex', 'Vict Descent', 'Weapon Used Cd',
                             'Status'])
data.to_csv('data.csv', index=False)
data.to_pickle('data.pkl')
data.to_json('data.json')

''' т.к. поддержка msgpack в pandas удалена, то весь DataFrame исходных данных, лежащий под именем 'data',
сохранить в .msgpack формате не получится, но возможно, к примеру, только часть испольхзуемых 7-ми столбцов 'datat' (usecols)
p.s. честно, так и не поняла, почему через указание заданных каких-либо столбцов, можно и через pandas сохранить в .msgpack'''


with open('data.msgpack', "wb") as f:
    f.write(msgpack.dumps(datat.to_dict()))

files = ['stats.json', 'frequency.json', 'data.csv', 'data.json', 'data.pkl', 'data.msgpack']

for i in files:
    print('Size of %s:' % i, os.path.getsize(i))
