import re
with open('text_1_var_23', 'r') as file:
    replacement = re.compile("[^a-zA-Z\d]")
    text = replacement.sub(" ", str(file.read())).replace("  "," ").strip().split(" ")
    word_statistics = {}

    for word in text:
        if word in word_statistics:
            word_statistics[word] += 1
        else:
            word_statistics[word] = 1
    word_statistics = dict(sorted(word_statistics.items(), reverse=True, key=lambda item: item[1]))

with open('text_1_output', 'w') as output:
    for word, freq in word_statistics.items():
       output.write(f'{word}:{freq}\n')
