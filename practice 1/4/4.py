import csv

variant = 23
avg_salary = 0
items = []

with open('text_4_var_23', newline='\n', encoding='utf-8') as file:
    reader = csv.reader( file)
    for row in reader:
        item = {'id': int(row[0]),
            'name': row[2] + " " + row[1],
            'age': int(row[3]),
            'salary': int(row[4][:-1])}
        avg_salary += item['salary']
        items.append(item)

avg_salary /= len(items)

for i in items:
    if i['salary'] < avg_salary or i['age'] <= (25 + variant % 10):
        items.remove(i)

output_n = sorted(items, key=lambda x: x['id'])

with open('text_4_output.csv', 'w', newline='', encoding='utf-8') as output:
    writer = csv.writer(output)
    writer.writerow(['id', 'name', 'age', 'salary'])
    for value in output_n:
        writer.writerow([value['id'], value['name'], value['age'], value['salary']])