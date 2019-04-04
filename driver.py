import csv
import os

# from config import settings
from config_ import settings
from create_database_from_csv import Creator
from data_check import data_check

'''
def data_check(csv_file_name): #пока не работает. #TODO: перенести в АПИ
    TODO: проверка csv на ";;" и "/r;" или "/n;"
    """
    Функция приведения в адекватный формат файлов данных БДИП ФССП с целью подготовки к записи в БД.
    """

    new_csv_file_name = csv_file_name + '_tmp'

    with open(csv_file_name, 'r') as csv_file_read, open(new_csv_file_name, 'w') as csv_file_write:
        reader = csv.reader(csv_file_read, delimiter='\n')
        writer = csv.writer(csv_file_write) #delimiter='\n'
        for row in reader:
            r = row[0].split(';')
            print(r[0])
            if r[0] == '':
                continue
            else:
                writer.writerow(row)
            # print(row)
    
    os.rename(csv_file_name, csv_file_name + '_')
    os.rename(new_csv_file_name, csv_file_name)
'''


def csv_to_database():
    '''
    Main-функция, копирующая наборы данных БДИП ФССП, находящихся на локальном хосте 
    по указанным в конфигурационном файле путям, в базу данных под управление PostgreSQL.
    Файлы данных должны быть скачаны на жёсткий диск локального хоста в формате csv (каждый файл в отдельном каталоге).
    Также файлы с описанием формата файлов данных должны быть скачаны на жёсткий диск локального хоста в формате csv и храниться в одном каталоге с соответствующим файлом данных.
    Все настройки указываются в файле config.py.
    '''

    psql_host = settings['psql_host']
    username = settings['username']
    password = settings['password']
    connection_port = settings['connection_port']
    database_name = settings['database_name']

    keys_list = [i.replace(' ', '_') for i in settings['primary_keys_for_tables']]
    keys_str = ', '.join(keys_list)
    # table_name = settings['table_name']

    # Инициализация объекта, осуществляющего копирование
    c = Creator(psql_host, username, password, database_name, connection_port)
    # c.create_database()  # Создание БД

    for folder in [settings['ip_path'], settings['complete_path']]:
        c.create_connection()

        file_list = os.listdir(folder)
        for file_name in file_list:
            if file_name.startswith('data'):
                data_file = folder + file_name  # Файл данных
                if not os.path.isfile(folder + file_name):
                    raise OSError('"{}" не является файлом.'.format(file_name))
                break
            # elif file_name.startswith('structure'):
            #     format_description_file = folder + file_name  # Файл с описанием формата файла данных #ВАЖНО: формат в этом файле не соответствует данным
            else:
                continue
        if not data_file: #or format_description_file
            raise OSError('Файл "{}" не найден.'.format(file_name))

        table_name_end = "_ip" if folder == settings['ip_path'] else "_complete"
        table_name = settings['table_name'] + table_name_end

        # c.delete_pkey(table_name) # удаляет PRIMARY KEY
        c.delete_index(table_name) # удаляет индекс
        c.create_table(table_name, data_file, keys_str)  # Создание и разметка таблицы для отчётности за определённый год
        data_check(data_file) # Проверка csv-файла на валидность данных
        # c.copy_from_csv(table_name, data_file) # Заполнение созданной таблицы данными
        # c.create_pkey(table_name, keys_str) # добавляет PRIMARY KEY
        c.create_index(table_name, keys_str) # добавляет индекс

        if c.conn:
            c.conn.close()
        print('{} - OK!\n\n'.format(table_name_end))

if __name__ == '__main__':
    csv_to_database()