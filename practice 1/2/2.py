with open('text_2_var_23', 'r') as file:
    average_list = []
    for line in file.readlines():
        numbers_in_line = line.split(' ')
        sum = 0
        for number in numbers_in_line:
            sum += int(number)
        average_list.append(sum / len(numbers_in_line))

with open('text_2_output', 'w') as output:
    for average in average_list:
        output.write(f'{average:.3f}\n')