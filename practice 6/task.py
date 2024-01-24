import pandas as pd
import os
from data_json import save_in_json, create_dir


pd.set_option("display.max_rows", 20, "display.max_columns", 60)


# считывание входного файла
def read_file(filename):
    return pd.read_csv(filename)


# используемая память входным файлом
# Объем памяти, который занимает файл на диске и при загрузке в память
def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024**2
    return "{:03.2f} MB".format(usage_mb)


# используемая память по колонкам
# Доля от общего объема, тип данных для каждой колонки до оптимизации
def get_memory_stat_by_column(df):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = memory_usage_stat.sum()
    file_size = f"{os.path.getsize(file_name) // 1024} КБ"
    file_in_memory_size = f"{total_memory_usage // 1024} КБ"
    column_stats = []
    for key in df.dtypes.keys():
        column_stats.append({
            'column_name': key,
            'memory_abs': f'{memory_usage_stat[key] // 1024} КБ',
            'memory_per': f'{round(memory_usage_stat[key] / total_memory_usage * 100, 4)} %',
            'dtype': df.dtypes[key]
        })
    column_stats.sort(key=lambda x: x['memory_abs'], reverse=True)
    column_stats.insert(0, {'file_in_memory_size': file_in_memory_size})
    column_stats.insert(0, {'file_size': file_size})
    return column_stats


# Оптимизация объектов
def opt_obj(df):
    converted_obj = pd.DataFrame()
    dataset_obj = df.select_dtypes(include=['object']).copy()

    for col in dataset_obj.columns:
        num_unique_values = len(dataset_obj[col].unique())
        num_total_values = len(dataset_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:, col] = dataset_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = dataset_obj[col]
    # print('_obj', mem_usage(dataset_obj))
    # print(mem_usage(converted_obj))

    return converted_obj


# Оптимизация целых чисел
def opt_int(df):
    dataset_int = df.select_dtypes(include=['int'])
    converted_int = dataset_int.apply(pd.to_numeric, downcast='unsigned')
    # print('_int', mem_usage(dataset_int))
    # print(mem_usage(converted_int))
    return converted_int


# Оптимизация чисел с плавающей точкой
def opt_float(df):
    dataset_float = df.select_dtypes(include=['float'])
    converted_float = dataset_float.apply(pd.to_numeric, downcast='float')
    # print('_float', mem_usage(dataset_float))
    # print(mem_usage(converted_float))
    return converted_float


############################################################################
############################################################################

"""№1 Входной датасет [1]game_logs.csv"""

file_name = "data/[1]game_logs.csv"


# Чтение файла
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_1', 'stats_optimization_1')


# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float

# print(mem_usage(dataset))
# print(mem_usage(optimized_dataset))

# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_1', 'stats_optimization_1')


# Выбор данных для чтения и преобразования
column_dtype = {
    'date': pd.Int64Dtype(),
    'day_of_week': pd.CategoricalDtype(),
    'v_name': pd.CategoricalDtype(),
    'v_league': pd.CategoricalDtype(),
    'v_game_number': pd.Int64Dtype(),
    'day_night': pd.CategoricalDtype(),
    'attendance': pd.Int64Dtype(),
    'length_minutes': pd.Int64Dtype(),
    'h_player_1_id': pd.StringDtype(),
    'h_player_2_id': pd.StringDtype()
}

# Обновление датафрейма после оптимизации и выбора данных
new_dataset = pd.DataFrame()

for column, dtype in column_dtype.items():
    new_dataset[column] = optimized_dataset[column].astype(dtype)


# Сохранение преобразованных данных
new_dataset.dropna().to_csv(os.path.join('stats_optimization_1', "df_1.csv"), index=False, mode="a", header=True)

file_name = "stats_optimization_1/df_1.csv"
dataset = read_file(file_name)


############################################################################
############################################################################


"""№2 Входной датасет [2]automotive.csv.zip"""

file_name = "data/[2]automotive.csv.zip"

# Выбор данных для чтения и преобразования
column_dtype = {
    'firstSeen': pd.StringDtype,
    'brandName': pd.CategoricalDtype,
    'modelName': pd.CategoricalDtype,
    'askPrice': pd.Int64Dtype(),
    'isNew': pd.CategoricalDtype,
    'vf_Wheels': pd.Int64Dtype(),
    'vf_Seats': pd.Int64Dtype(),
    'vf_Windows': pd.StringDtype,
    'vf_WheelSizeRear': pd.StringDtype,
    'vf_WheelBaseShort': pd.StringDtype,
}


# Обновление датафрейма после оптимизации и выбора данных
# Сохранение преобразованных данных
create_dir('stats_optimization_2') # создание подпапки для результатов 2-го файла

has_header = True

for part in pd.read_csv(file_name,
                        usecols=lambda x: x in column_dtype.keys(),
                        dtype=column_dtype,
                        chunksize=500_000,
                        compression='zip'):
    part.dropna().to_csv(os.path.join('stats_optimization_2', "df_2.csv"), mode="a", index=False, header=has_header)
    has_header = False


file_name = "stats_optimization_2/df_2.csv"
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_2', 'stats_optimization_2')

# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float

# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_2', 'stats_optimization_2')


############################################################################
############################################################################


"""№3 Входной датасет [3]flights.csv"""


file_name = "data/[3]flights.csv"

# Чтение файла
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_3', 'stats_optimization_3')


# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_3', 'stats_optimization_3')


# Выбор данных для чтения и преобразования
column_dtype = {
    'YEAR': pd.Int64Dtype(),
    'MONTH': pd.Int64Dtype(),
    'DAY': pd.Int64Dtype(),
    'DAY_OF_WEEK': pd.Int64Dtype(),
    'AIRLINE': pd.CategoricalDtype(),
    'TAIL_NUMBER': pd.StringDtype(),
    'ORIGIN_AIRPORT': pd.StringDtype(),
    'DESTINATION_AIRPORT': pd.StringDtype(),
    'DISTANCE': pd.Int64Dtype(),
    'ARRIVAL_DELAY': pd.Int64Dtype()
}


# Обновление датафрейма после оптимизации и выбора данных
# Сохранение преобразованных данных
new_dataset = pd.DataFrame()

for column, dtype in column_dtype.items():
    new_dataset[column] = optimized_dataset[column].astype(dtype)

new_dataset.dropna().to_csv(os.path.join('stats_optimization_3', "df_3.csv"), index=False, mode="a", header=True)

file_name = "stats_optimization_3/df_3.csv"
dataset = read_file(file_name)


############################################################################
############################################################################


"""№4 Входной датасет [4]vacancies.csv.gz"""

file_name = "data/[4]vacancies.csv.gz"


# Выбор данных для чтения и преобразования
column_dtype = {
    'id': pd.Int64Dtype(),
    'key_skills': pd.StringDtype,
    'schedule_id': pd.CategoricalDtype,
    'schedule_name': pd.CategoricalDtype,
    'accept_handicapped': pd.CategoricalDtype,
    'accept_kids': pd.CategoricalDtype,
    'specializations': pd.CategoricalDtype,
    'allow_messages': pd.CategoricalDtype,
    'employer_id': pd.Int64Dtype(),
    'prof_classes_found': pd.CategoricalDtype
}


# Обновление датафрейма после оптимизации и выбора данных
# Сохранение преобразованных данных
create_dir('stats_optimization_4')

has_header = True

for part in pd.read_csv(file_name,
                        usecols=lambda x: x in column_dtype.keys(),
                        dtype=column_dtype,
                        chunksize=500_000,
                        compression='gzip'):
    part.dropna().to_csv(os.path.join('stats_optimization_4', "df_4.csv"), mode="a", index=False, header=has_header)
    has_header = False

file_name = "stats_optimization_4/df_4.csv"
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_4', 'stats_optimization_4')


# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float

# print(mem_usage(dataset))
# print(mem_usage(optimized_dataset))

# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_4', 'stats_optimization_4')


############################################################################
############################################################################


"""№5 Входной датасет [5]asteroid.zip"""


file_name = "data/[5]asteroid.zip"


# Выбор данных для чтения и преобразования
column_dtype = {
    'id': pd.StringDtype,
    'spkid': pd.Int64Dtype(),
    'full_name': pd.StringDtype,
    'H': pd.Float64Dtype(),
    'diameter': pd.Float64Dtype(),
    'diameter_sigma': pd.Float64Dtype(),
    'orbit_id': pd.CategoricalDtype,
    'epoch_mjd': pd.Int64Dtype(),
    'epoch_cal': pd.Float64Dtype(),
    'equinox': pd.CategoricalDtype
}


# Обновление датафрейма после оптимизации и выбора данных
# Сохранение преобразованных данных
create_dir('stats_optimization_5')

has_header = True

for part in pd.read_csv(file_name,
                        usecols=lambda x: x in column_dtype.keys(),
                        dtype=column_dtype,
                        chunksize=500_000,
                        compression='zip'):
    part.dropna().to_csv(os.path.join('stats_optimization_5', "df_5.csv"), mode="a", index=False, header=has_header)
    has_header = False

file_name = "stats_optimization_5/df_5.csv"
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_5', 'stats_optimization_5')


# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float

# print(mem_usage(dataset))
# print(mem_usage(optimized_dataset))

# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_5', 'stats_optimization_5')


###########################################################################
###########################################################################


"""№6 Входной датасет brewery_data_complete_extended.zip"""

# Произвольный датасет 'Brewery Operations and Market Analysis Dataset'
# https://www.kaggle.com/datasets/ankurnapa/brewery-operations-and-market-analysis-dataset
"""
Комментарии к выбранным слобцам
'Batch_ID'                      # unique identifier assigned to each batch of beer produced
'Beer_Style'                    # The style or type of beer, such as IPA, Stout, Lager, Ale, etc.
'SKU'                           # The packaging type in which the beer is sold, like Kegs, Bottles, Cans, or Pints
'Fermentation_Time'             # The duration of the fermentation process, measured in days
'Temperature'                   # The average temperature (in Celsius) maintained during the brewing process
'pH_Level'                      # The pH level of the beer, indicating its acidity or alkalinity
'Alcohol_Content'               # The percentage of alcohol by volume in the beer
'Quality_Score'                 # An overall quality score assigned to the beer batch, rated out of 10
'Brewhouse_Efficiency'          # The efficiency of the brewing process, expressed as a percentage
'Loss_During_Bottling_Kegging'  # The percentage of volume loss during the bottling or kegging process"""

file_name = "data/brewery_data_complete_extended.csv.zip"

# Выбор данных для чтения и преобразования
column_dtype = {
    'Batch_ID': pd.Int64Dtype(),
    'Beer_Style': pd.CategoricalDtype,
    'SKU': pd.CategoricalDtype,
    'Fermentation_Time': pd.Int64Dtype(),
    'Temperature': pd.Float64Dtype(),
    'pH_Level': pd.Float64Dtype(),
    'Alcohol_Content': pd.Float64Dtype(),
    'Quality_Score': pd.Float64Dtype(),
    'Brewhouse_Efficiency': pd.Float64Dtype(),
    'Loss_During_Bottling_Kegging': pd.Float64Dtype()
}


# Обновление датафрейма после оптимизации и выбора данных
# Сохранение преобразованных данных
create_dir('stats_optimization_6')
has_header = True

for part in pd.read_csv(file_name,
                        usecols=lambda x: x in column_dtype.keys(),
                        dtype=column_dtype,
                        chunksize=500_000,
                        compression='zip'):
    part.dropna().to_csv(os.path.join('stats_optimization_6', "df_6.csv"), mode="a", index=False, header=has_header)
    has_header = False

file_name = "stats_optimization_6/df_6.csv"
dataset = read_file(file_name)


# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки до оптимизации
column_stat = get_memory_stat_by_column(dataset)
save_in_json(column_stat, 'stats_before_optimization_6', 'stats_optimization_6')


# Оптимизация датафрейма
optimized_dataset = dataset.copy()

optimal_object = opt_obj(optimized_dataset)
optimal_integer = opt_int(optimized_dataset)
optimal_float = opt_float(optimized_dataset)

optimized_dataset[optimal_object.columns] = optimal_object
optimized_dataset[optimal_integer.columns] = optimal_integer
optimized_dataset[optimal_float.columns] = optimal_float

# print(mem_usage(dataset))
# print(mem_usage(optimized_dataset))

# Объем памяти, который занимает файл на диске и при загрузке в память,
# доля от общего объема, тип данных для каждой колонки после оптимизации
column_stat = get_memory_stat_by_column(optimized_dataset)
save_in_json(column_stat, 'stats_after_optimization_6', 'stats_optimization_6')