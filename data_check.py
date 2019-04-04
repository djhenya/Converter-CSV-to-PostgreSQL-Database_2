import csv
import shutil

def data_check(csvfile_name):
    seen = set()
    invalid_data_file = f'{csvfile_name}_invalid'
    valid_data_file = f'{csvfile_name}_'
    with open(csvfile_name, 'r', encoding='utf-8') as csv_file, open(invalid_data_file, 'w', encoding='utf-8', newline='') as csv_file1, open(valid_data_file, 'w', encoding='utf-8', newline='') as csv_file2:
        reader = csv.DictReader(csv_file, delimiter=',')
        writer = csv.DictWriter(csv_file1, fieldnames=reader.fieldnames)
        writer2 = csv.DictWriter(csv_file2, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer2.writeheader()

        for row in reader:
            tuplee = (row['The number of enforcement proceeding'], row['The number of executive document'], row['Date of the institution of proceeding'],)
            if tuplee in seen:
                writer.writerow(row)
            else:
                seen.add(tuplee)
                writer2.writerow(row)
    shutil.move(valid_data_file, csvfile_name)