variant = 23
output_n=[]
with open('text_3_var_23', 'r') as file:
    for line in file.readlines():
        numbers = line.strip().split(',')
        for i in range(len(numbers)):
            if numbers[i] == 'NA':
                numbers[i] = str(int(numbers[i - 1]) + int(numbers[i + 1]) / 2)
        for n in numbers:
            if float(n)**0.5 < 50 + variant:
                numbers.remove(n)
        output_n.append(','.join(numbers))

with open('text_3_output', 'w') as output:
    for line in output_n:
        output.write(line + '\n')