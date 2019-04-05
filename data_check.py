import csv
import shutil


def data_check(csvfile_name):
    """
    Функция удаляет дубликаты из набора данных БДИП ФССП (в формате .csv) путём добавления записей в множество и перезаписи в новых файл.

    Arguments:
        csvfile_name {str} -- Имя файла набора данных БДИП ФССП.
    """

    seen = set()
    invalid_data_file_name = f'{csvfile_name}_invalid'
    valid_data_file_name = f'{csvfile_name}_'
    with open(csvfile_name, 'r', encoding='utf-8') as data_file, open(invalid_data_file_name, 'w', encoding='utf-8', newline='') as invalid_data_file, open(valid_data_file_name, 'w', encoding='utf-8', newline='') as valid_data_file:
        reader = csv.DictReader(data_file, delimiter=',')
        writer_invalid = csv.DictWriter(invalid_data_file, fieldnames=reader.fieldnames)
        writer_valid = csv.DictWriter(valid_data_file, fieldnames=reader.fieldnames)
        writer_invalid.writeheader()
        writer_valid.writeheader()

        for row in reader:
            tuplee = (row['The number of enforcement proceeding'],
                      row['The number of executive document'],
                      row['Date of the institution of proceeding'],)
            if tuplee in seen:
                writer_invalid.writerow(row)
            else:
                seen.add(tuplee)
                writer_valid.writerow(row)
    shutil.move(valid_data_file_name, csvfile_name)
