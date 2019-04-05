import csv
import os

# from config import settings
from config_ import settings
from create_database_from_csv import Creator
from data_check import data_check


def csv_to_database():
    """
    Main-функция, копирующая наборы данных БДИП ФССП, находящихся на локальном хосте 
    по указанным в конфигурационном файле путям, в базу данных под управление PostgreSQL.
    Файлы данных должны быть скачаны на жёсткий диск локального хоста в формате csv (каждый файл в отдельном каталоге).
    Также файлы с описанием формата файлов данных должны быть скачаны на жёсткий диск локального хоста в формате csv и храниться в одном каталоге с соответствующим файлом данных.
    Все настройки указываются в файле config.py.

    Функция парсит конфиг и для каждого из двух файлов наборов данных:
    - создаёт коннекцию к БД;
    - ищет файл по указанному пути (проверяет на его наличие и "файловость");
    - выбирает первый найденный в каталоге файл, начинающийся с "data";
    - удаляет созданный ранее этой же программой индекс из таблицы (если он имеет имя "<названиетаблицы>_index")
    {остальные индексы надо удалить самим для улучшения производительности программы, а по завершению работы программы создать заново};
    - проверяет файл на валидность данных (удаляет строки с повторяющимися значениями полей, указанных в качестве primary key в конфиге);
    - создаёт и размечает таблицу в БД, а также заполняет её данными из csv-файла;
    - создаёт unique-индекс по primary ключам созданной таблицы;
    - закрывает коннекцию к БД, если она не была закрыта ранее.

    Raises:
        OSError -- ошибка при поиске файла по указанному в config.py имени (файл не найден, либо это не файл)
    """

    psql_host = settings['psql_host']
    username = settings['username']
    password = settings['password']
    connection_port = settings['connection_port']
    database_name = settings['database_name']

    keys_list = [i.replace(' ', '_')
                 for i in settings['primary_keys_for_tables']]
    keys_str = ', '.join(keys_list)

    # Инициализация объекта, осуществляющего копирование
    c = Creator(psql_host, username, password, database_name, connection_port)
    # c.create_database()  # Создание БД (без пересоздания при наличии) - нужны определённые права пользователя БД

    for folder in [settings['ip_path'], settings['complete_path']]:
        c.create_connection()  # создание коннекции к БД

        file_list = os.listdir(folder)
        for file_name in file_list:
            if file_name.startswith('data'):
                data_file = folder + file_name  # Файл данных
                if not os.path.isfile(folder + file_name):
                    raise OSError('"{}" не является файлом.'.format(file_name))
                break  # используется первый найденный файл, начинающийся на "data"
            # elif file_name.startswith('structure'): # Файл с описанием формата файла данных
            #     format_description_file = folder + file_name #ВАЖНО: формат в этом файле не соответствует данным
            else:
                continue
        if not data_file:  # or format_description_file
            raise OSError('Файл "{}" не найден.'.format(file_name))

        # Генерация имени таблицы на основе введённой в конфиге
        table_name_end = "_ip" if folder == settings['ip_path'] else "_complete"
        table_name = settings['table_name'] + table_name_end

        # c.delete_pkey(table_name) # удаляет PRIMARY KEY - не влияет на производительность
        # Удаление индекса (если есть) - ускоряет производительность
        c.delete_index(table_name)
        # Проверка csv-файла на валидность данных (актуально для завершенных производств)
        data_check(data_file)
        # Создание и разметка таблицы для отчётности за определённый год. Заполнение созданной таблицы данными
        c.create_table(table_name, data_file, keys_str)
        # c.copy_from_csv(table_name, data_file) # legacy - в этой версии copy_from_csv встроена в create_table
        # c.create_pkey(table_name, keys_str) # добавляет PRIMARY KEY
        # Добавление индекса
        c.create_index(table_name, keys_str)

        if c.conn:
            c.conn.close()  # закрытие коннекции к БД
        print('{} - OK!\n\n'.format(table_name_end))


if __name__ == '__main__':
    csv_to_database()
