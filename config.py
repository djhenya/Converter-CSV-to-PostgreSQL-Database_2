'''
Настройки для подключения к БД:
    'psql_host': 'str: имя хоста с базой данных',
    'username': 'str: имя пользователя базы данных',
    'password': 'str: пароль пользователя',
    'connection_port': 'str: порт для коннекций при выполнении скрипта',
    'database_name': 'str: название базы данных',
    'table_name': 'str: название создаваемых таблиц (к ним автоматически добавляется "_ip" - для текущих исполнительных производств или "_complete" - для оконченных)',
    'primary_keys_for_tables': 'list: перечисление желаемых первичных ключей в виде списка строк. Пример: ["The number of enforcement proceeding", "The number of executive document", "Date of the institution of proceeding"]',
    'index_fields': 'list: перечисление желаемых индексов в виде списка строк. Пример: ["The number of enforcement proceeding", "The number of executive document", "Date of the institution of proceeding"]',
    'ip_path': 'str: путь до директории с данными БДИП ФССП по текущим исполнительным производствам (под названием "structure-<дата1>.csv" - с описанием формата данных, под названием "data-<дата2>-structure-<дата1>.csv") в виде строки.\
        Пример для Windows: "C:\\CSVtoDatabase\\ipFiles\\"',
    'complete_path': 'str: путь до директории с данными БДИП ФССП по оконченным исполнительным производствам (под названием "structure-<дата1>.csv" - с описанием формата данных, под названием "data-<дата2>-structure-<дата1>.csv") в виде строки.\
        Пример для Windows: "C:\\CSVtoDatabase\\completeFiles\\"',
'''

settings = {
    'psql_host': '',
    'username': '',
    'password': '',
    'connection_port': '',
    'database_name': '',
    'table_name': '',
    'primary_keys_for_tables': [],
    'index_fields': [],
    'ip_path': '',
    'complete_path': '',
}