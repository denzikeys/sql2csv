import csv
import chardet
import re
import os
import glob

# Использование функции
filename = 's_users.sql'  # Ваш большой текстовый файл
chunksize = 100000  # Количество строк в одном файле
output_prefix = 'output'  # Префикс для выходных файлов
csv_merge_quantity = 5  # Количество CSV-файлов, объединяемых в один

def split_and_convert_file(filename, chunksize, output_prefix, csv_merge_quantity):
    with open(filename, 'r', encoding='utf-8') as f:
        chunk = []
        count = 0
        for line in f:
            if line.strip().startswith('INSERT INTO') and line.strip().endswith(');'):
                chunk.append(line)
                if len(chunk) == chunksize:
                    convert_chunk_to_csv(chunk, f'{output_prefix}_{count}.csv')
                    chunk = []
                    count += 1
        if chunk:
            convert_chunk_to_csv(chunk, f'{output_prefix}_{count}.csv')

    merge_csv_files(output_prefix, csv_merge_quantity)

def convert_chunk_to_csv(chunk, output_file):
    # Создание CSV-файла и запись заголовков
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';')

        # Извлечение заголовков столбцов из первой строки
        header_pattern = r'INSERT INTO .*\((.*)\) VALUES'
        header_match = re.search(header_pattern, chunk[0])
        header_raw = header_match.group(1)
        headers = header_raw.split(', ')
        writer.writerow(headers)

        # Извлечение значений из каждой строки и запись в CSV-файл
        for line in chunk:
            value_pattern = r'VALUES \((.*)\);'
            value_match = re.search(value_pattern, line)
            values_raw = value_match.group(1)
            values = [re.sub(r"^'(.*)'$", r"\1", value) for value in values_raw.split(', ')]
            writer.writerow(values)

def merge_csv_files(output_prefix, csv_merge_quantity):
    csv_files = sorted(glob.glob(f'{output_prefix}_*.csv'))
    merged_file_count = 0

    for i in range(0, len(csv_files), csv_merge_quantity):
        with open(f'merged_{output_prefix}_{merged_file_count}.csv', 'w', encoding='utf-8-sig', newline='') as merged_file:
            writer = csv.writer(merged_file, delimiter=';')

            # Write headers only once
            if i == 0:
                with open(csv_files[i], 'r', encoding='utf-8-sig') as csv_file:
                    reader = csv.reader(csv_file, delimiter=';')
                    headers = next(reader)
                    writer.writerow(headers)

            for j in range(i, min(i + csv_merge_quantity, len(csv_files))):
                with open(csv_files[j], 'r', encoding='utf-8-sig') as csv_file:
                    reader = csv.reader(csv_file, delimiter=';')
                    next(reader)  # Skip headers
                    for row in reader:
                        writer.writerow(row)

            merged_file_count += 1

        # Remove merged CSV files
        for j in range(i, min(i + csv_merge_quantity, len(csv_files))):
            os.remove(csv_files[j])

split_and_convert_file(filename, chunksize, output_prefix, csv_merge_quantity)
