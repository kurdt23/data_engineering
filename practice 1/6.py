from bs4 import BeautifulSoup
import json

str_json = ''

with open('text_6_var_23', encoding='utf-8') as file:
    lines = file.readlines()
    for line in lines:
        str_json += line

data = json.loads(str_json)
data = data['items']

soup = BeautifulSoup("""<table>
    <tr>
        <th>project</th>
        <th>article</th>
        <th>status</th>
        <th>time</th>
        <th>access</th>
    </tr>
 </table>""", "html.parser")

table = soup.contents[0]
for tick in data:
    tr = soup.new_tag("tr")
    for key, value in tick.items():
        td = soup.new_tag("td")
        td.string = value
        tr.append(td)
    table.append(tr)

with open('text_6_output.html', 'w') as result:
    result.write(soup.prettify())
    result.write('\n')