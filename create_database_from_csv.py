import csv
import re
import sys
from datetime import datetime

import psycopg2


class Creator():
    '''
    Класс для копирования набора данных БДИП ФССП в формате .csv в БД PostgreSQL.
    '''

    def __init__(self, host, user_name, user_pwd, db_name, port):
        self.host = host
        self.user_name = user_name
        self.user_pwd = user_pwd
        self.db_name = db_name
        self.port = port
        self.conn = ''
        self.cursor = ''

    def create_database(self):
        '''
        Функция для создания БД.
        '''
        try:
            connect_str = "dbname='postgres' user={} host={} password={}".format(self.user_name, self.host, self.user_pwd)
            con = psycopg2.connect(connect_str)
            con.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = con.cursor()
            cur.execute('''CREATE DATABASE {}'''.format(self.db_name))
            con.commit()
        except psycopg2.ProgrammingError as err:
            if con:
                con.rollback()
            print('Database creation error: ' + str(err))
        except Exception as e:
            if con:
                con.rollback()
            sys.exit(str(e))
        finally:
            if cur:
                cur.close()
            if con:
                con.close()
            del cur, con

    def create_connection(self):
        '''
        Создаёт коннекцию к БД, которую в дальнейшем используют другие методы.
        Коннекция создаётся как атрибут класса!
        '''

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                user=self.user_name,
                port=self.port,
                dbname=self.db_name,
                password=self.user_pwd
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.rollback()
                self.conn.close()
            sys.exit(str(e))

    def create_table(self, table_name, data_file, primary_keys):
        '''
        Функция для создания и разметки таблицы в БД для набора данных БДИП ФССП.
        При разметке используется файл формата данных в формате .csv .
        А также функция для записи данных из .csv в созданную и размеченную таблицу.

        Arguments:
            table_name {str} -- Имя создаваемой таблицы.
            data_file {str} -- Имя файла набора данных БДИП ФССП для разметки и заполнения таблицы.
            primary_keys {str} -- Primary ключи данной таблицы.
        '''
        # Проверка наличия таблицы в базе
        try:
            self.cursor.execute(
                '''SELECT EXISTS(SELECT 1 AS result FROM pg_tables WHERE tablename = %s);''', (table_name,))  # Проверка на существование таблицы в базе
        except Exception as e:
            if self.cursor:
                self.cursor.close()
            self.conn.rollback()
            self.conn.close()
            sys.exit(str(e))

        # Если таблица отсутствует в базе, то она создаётся.
        if not self.cursor.fetchone()[0]:
            created = True
            statement = '''CREATE TABLE IF NOT EXISTS "{}" ('''.format(table_name)  # IF NOT EXISTS не нужен, но мало ли
            try:
                with open(data_file, 'r', encoding='utf-8') as csv_file:
                    reader = csv.reader(csv_file, delimiter=',')
                    columns = next(reader)  # парсинг первой строки data-файла

                    # for row in reader: # парсинг structure-файла
                    #     column_name = row["english field name"].replace(' ', '_').replace(',', '')
                    #     formating = row["english description"] if column_name != 'Date_complete_IP_reason' else row["russian description"]
                    #     if formating != "Текстовое":
                    #         print(f'В файле с описанием формата данных: "{columns_file}" указан неизвестный формат поля (не "Текстовое"). По умолчанию заменён на "Текстовое".')
                    #     column_type = "text"
                    #     statement += '{} {}, '.format(column_name, column_type)

            except Exception as e:
                if self.cursor:
                    self.cursor.close()
                self.conn.rollback()
                self.conn.close()
                sys.exit(str(e))

            column_type = "text"
            for column_name in columns:
                statement += '{} {}, '.format(column_name.replace(' ', '_').replace(',', ''), column_type)
            statement += '''PRIMARY KEY({}));'''.format(primary_keys)

            # TODO: написать регулярку для ассерта  # test_dev
            # assert statement == '''CREATE TABLE IF NOT EXISTS "{}" (
            #                         %s %s(%i),
            #                         %s %s(%i),
            #                         PRIMARY KEY({})
            #                         );'''.format(table_name, primary_keys)

            try:
                # Здесь выполняется один большой запрос (statement) на создание таблицы с 14 полями
                self.cursor.execute(statement)
                # self.conn.commit()
            except Exception as e:
                if self.cursor:
                    self.cursor.close()
                self.conn.rollback()
                self.conn.close()
                sys.exit(str(e))

        else:  # Если таблица присутствует в базе, то она не пересоздается, а выводится следующее сообщение:
            created = False
            print('Table creation error: table "{}" already exists'.format(table_name))

    # def copy_from_csv(self, table_name, data_file):
        print('Copying started at {}'.format(datetime.now()))
        while True:
            if created:
                # для первого добавления
                stm = '''COPY {} FROM %s FORCE NOT NULL {} HEADER FREEZE DELIMITER ',' CSV ENCODING 'utf-8';'''.format(table_name, primary_keys)
            else:
                # для последующих добавлений
                stm = '''COPY {} FROM %s FORCE NOT NULL {} HEADER DELIMITER ',' CSV ENCODING 'utf-8';'''.format(table_name, primary_keys)
            try:
                self.cursor.execute(stm, (data_file,))
                self.conn.commit()
            except psycopg2.IntegrityError as e:  # в случае нахождения дублирующего ключа - запись из базы стирается (т.е. заменяется на запись из csv)
                self.conn.rollback()
                regex = r"\(.*\)=\(.*\)"
                founded = re.search(regex, str(e))
                if sys.version_info >= (3, 6):  # в разных версиях питона по-разному выглядят match-объекты
                    key_and_value = founded[0].strip("()").split(")=(")
                else:
                    key_and_value = str(founded.group(0)).strip("()").split(")=(")
                key = key_and_value[0]
                value = key_and_value[1]
                statement = '''DELETE FROM {} WHERE {} = '{}';'''.format(table_name, key, value)
                self.cursor.execute(statement)
                self.conn.commit()
                continue
            except Exception as e:
                if self.cursor:
                    self.cursor.close()
                self.conn.rollback()
                self.conn.close()
                sys.exit(str(e))
            else:
                break

        print('Copying ended at {}'.format(datetime.now()))

    def create_index(self, table_name, primary_keys):
        """
        Функция добавляет в созданную при помощи create_table() таблицу индекс с названием "<названиетаблицы>_index".
        Индекс - UNIQUE, по primary ключам таблицы.

        Arguments:
            table_name {str} -- Имя создаваемой в дальнейшем при помощи self.create_table() таблицы.
            primary_keys {str} -- Primary ключи данной таблицы.
        """

        statement = '''CREATE UNIQUE INDEX {}_index \
                       ON public.{} USING btree \
                       ({} COLLATE pg_catalog."default") \
                       TABLESPACE pg_default;'''.format(table_name, table_name, primary_keys)
        try:
            self.cursor.execute(statement)
            self.conn.commit()
        except Exception as e:
            if self.cursor:
                self.cursor.close()
            self.conn.rollback()
            self.conn.close()
            sys.exit(str(e))
            # print(e.__class__.__name__)

    def delete_index(self, table_name):
        """
        Функция удаляет созданный ранее этой же программой (или вручную) индекс из таблицы (если он имеет имя "<названиетаблицы>_index").

        Arguments:
            table_name {str} -- Имя созданной при помощи self.create_table() таблицы.
        """

        statement = '''DROP INDEX {}_index;'''.format(table_name)
        try:
            self.cursor.execute(statement)
            self.conn.commit()
        except psycopg2.ProgrammingError as e:  # Если index отсутствует в таблице, то выводится следующее сообщение:
            print('Index deletion error: index "{}_index" does not exists'.format(table_name))
            self.conn.rollback()
        except Exception as e:
            if self.cursor:
                self.cursor.close()
            self.conn.rollback()
            self.conn.close()
            sys.exit(str(e))

    # def create_pkey(self, table_name, primary_keys):
    #     statement = '''ALTER TABLE {} ADD PRIMARY KEY ({});'''.format(table_name, primary_keys)
    #     try:
    #         self.cursor.execute(statement)
    #         self.conn.commit()
    #     except Exception as e:
    #         if self.cursor:
    #             self.cursor.close()
    #         self.conn.rollback()
    #         self.conn.close()
    #         sys.exit(str(e))

    # def delete_pkey(self, table_name):
    #     statement = '''ALTER TABLE {} DROP CONSTRAINT {}_pkey;'''.format(table_name, table_name)
    #     try:
    #         self.cursor.execute(statement)
    #         self.conn.commit()
    #     except psycopg2.ProgrammingError as e: # Если primary_key отсутствует в таблице, то выводится следующее сообщение:
    #         print('Primary_key deletion error: primary_key "{}_pkey" does not exist'.format(table_name))
    #         self.conn.rollback()
    #     except Exception as e:
    #         if self.cursor:
    #             self.cursor.close()
    #         self.conn.rollback()
    #         self.conn.close()
    #         sys.exit(str(e))
