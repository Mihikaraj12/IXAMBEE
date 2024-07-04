import pandas as pd
import csv
import json
import datetime
import json
from pymongo import MongoClient

def read_lines_from_file(file_path):
    with open(file_path, encoding="utf-8-sig") as file:
        lines = file.read().strip().split('\n')
    return lines

def process_lines(lines):
    time = []
    user_host = []
    query_time = []
    lock_time = []
    rows_sent = []
    rows_examined = []
    SET_timestamp = []
    CALL_proc = []
    unique_calls = set()  # Keep track of unique "CALL proc_" values
    for line in lines:
        if line.startswith('2023'):
            data = line.split(':')[1].strip()
            if data.startswith('2') and data.endswith('Z'):
                time.append(data)
        elif line.startswith('# Query_time:'):
            parts = line.split(':')[1:]
            query_time.append(float(parts[0].split()[0]))
            lock_time.append(float(parts[1].split()[0]))
            rows_sent.append(int(parts[2].split()[0]))
            rows_examined.append(int(parts[3].split()[0]))
        elif line.startswith('SET timestamp='):
            SET_timestamp.append(line.split('=')[1].strip())
        elif line.startswith('CALL proc_'):
            call_proc = line.split(':')[0].strip()[10:]
            if '(' in call_proc:
                call_proc = call_proc[:call_proc.index('(')].strip()
            if call_proc not in unique_calls:
                unique_calls.add(call_proc)
                time.append('')
                user_host.append('')
                query_time.append('')
                lock_time.append('')
                rows_sent.append('')
                rows_examined.append('')
                SET_timestamp.append('')
                CALL_proc.append(call_proc)
        for line in lines:
            if line.startswith('# User@Host:'):
                user_encountered = True
                user_host.append(line.split(':')[1].strip())
            elif line.startswith('#'):
                user_encountered = False
            elif user_encountered:
                print(line)

    min_length = len(unique_calls)
    data = {
        'Time': time[:min_length],
        'User@Host': user_host[:min_length],
        'Query_time': query_time[:min_length],
        'Lock_time': lock_time[:min_length],
        'Rows_sent': rows_sent[:min_length],
        'Rows_examined': rows_examined[:min_length],
        'SET timestamp': SET_timestamp[:min_length],
        'CALL proc': CALL_proc[:min_length],
        'Type': ['mysql'] * min_length,
        'DATE': ['NONE'] * min_length,
        'TIME': ['NONE'] * min_length,
        'ERROR': ['NONE'] * min_length,
        'ID': ['NONE'] * min_length,
        'SPECIAL_ID': ['NONE'] * min_length,
        'OPEN': ['NONE'] * min_length,
        'Type_of_Error': ['NONE'] * min_length,
        'FAILED': ['NONE'] * min_length,
        'ERROR_MESSAGE': ['NONE'] * min_length,
        'CLIENT': ['NONE'] * min_length,
        'SERVER': ['NONE'] * min_length,
        'REQUEST': ['NONE'] * min_length,
        'HOST': ['NONE'] * min_length,
        'REFERRER': ['NONE'] * min_length
    }

    unique_calls_list = list(unique_calls)
    unique_calls_list.sort()
    print("Unique CALL proc_ values:", unique_calls_list)

    return data
def save_as_csv(data, csv_file_path):
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, index=False)
    print("Data saved as get.csv")

def convert_csv_to_json(csv_file_path, json_file_path):
    with open(csv_file_path, 'r') as csv_file:
        csv_data = csv.DictReader(csv_file)
        data = [row for row in csv_data]
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for row in data:
        row['Current_time'] = current_time
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print("Data converted to JSON and saved as index.json")

file_path = 'full.txt'
lines = read_lines_from_file(file_path)
data = process_lines(lines)
csv_file_path = 'get.csv'
save_as_csv(data, csv_file_path)
json_file_path = 'index.json'
convert_csv_to_json(csv_file_path, json_file_path)

client = MongoClient('mongodb://localhost:27017/')
database = client['mydata']
collection = database['finaldata']

json_file_path = 'index.json'
with open(json_file_path, 'r') as file:
    json_data = json.load(file)
for doc in json_data:
    doc['type'] = 'mysql'

collection.insert_many(json_data)
print("Inserted document IDs:", collection.distinct("_id"))