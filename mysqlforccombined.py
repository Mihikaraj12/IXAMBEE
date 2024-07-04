import pandas as pd
import csv
import json
import datetime
from pymongo import MongoClient

def read_lines_from_file(file_path):
    with open(file_path, encoding="utf-8-sig") as file:
        lines = file.read().strip().split('\n')
    return lines

def process_lines(lines):
    data = []
    unique_error_messages = set()

    for line in lines:
        values = line.split()
        if len(values) >= 6:
            time_value = values[0][2:-1]  # Extract the desired substring for time
            note_start_index = line.find("[")
            note_end_index = line.find("user:")
            if note_start_index != -1 and note_end_index != -1:
                note_value = line[note_start_index+1:note_end_index]
            else:
                note_value = ''
            if "user:" in values:
                user_index = values.index("user:")
                current_user = values[user_index+1]
            host_index = values.index("host:") if "host:" in values else -1
            if host_index != -1:
                host_value = values[host_index+1]
            else:
                host_value = ''
            error_start_index = line.find("(")
            error_end_index = line.find(")")
            if error_start_index != -1 and error_end_index != -1:
                error_value = line[error_start_index+1:error_end_index]
            else:
                error_value = ''
            if error_value not in unique_error_messages:
                unique_error_messages.add(error_value)
                data.append({
                    'TIME': time_value,
                    'NOTE': note_value.strip(),
                    'USER': current_user,
                    'HOST': host_value,
                    'ERROR_MESSAGE': error_value
                })

    df = pd.DataFrame(data)
    sorted_df = df.sort_values(by='TIME')
    return sorted_df

def save_as_csv(data, csv_file_path):
    if data.shape[0] == 0:  # Check the number of rows in the DataFrame
        print("No data to save.")
    else:
        data.to_csv(csv_file_path, index=False)
        print("Data saved as fourth.csv")

# Rest of the code remains the same...


def convert_csv_to_json(csv_file_path, json_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['Current_time'] = current_time
        df.to_json(json_file_path, orient='records', indent=4)
        print("Data converted to JSON and saved as index.json")
    except pd.errors.EmptyDataError:
        print("CSV file is empty.")

def insert_json_data_to_mongodb(json_file_path, collection):
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    if json_data and isinstance(json_data, list):
        result = collection.insert_many(json_data)
        inserted_ids = result.inserted_ids
        print("Data inserted successfully.")
    else:
        print("No JSON data found or the data is not a valid list.")

def modify_data(data, columns):
    max_length = max(len(column) for column in columns)
    modified_data = {}
    for column in columns:
        modified_data[column] = data.get(column, [None] * max_length)
    return modified_data

file_path = 'fourthworktext.txt'  # Update the file path accordingly
lines = read_lines_from_file(file_path)
data = process_lines(lines)
csv_file_path = 'fourth.csv'
save_as_csv(data, csv_file_path)
json_file_path = 'fourth.json'
convert_csv_to_json(csv_file_path, json_file_path)

client = MongoClient('mongodb://localhost:27017/')
database = client['mydata']
collection = database['data']

json_file_path = 'fourth.json'
insert_json_data_to_mongodb(json_file_path, collection)

file_path = 'full.txt'
lines = read_lines_from_file(file_path)
data = process_lines(lines)
columns = [
    'Time', 'User@Host', 'Query_time', 'Lock_time', 'Rows_sent', 'Rows_examined',
    'SET timestamp', 'CALL proc', 'Type', 'DATE', 'TIME', 'ERROR', 'ID',
    'SPECIAL_ID', 'OPEN', 'Type_of_Error', 'FAILED', 'ERROR_MESSAGE', 'CLIENT',
    'SERVER', 'REQUEST', 'HOST', 'REFERRER'
]
modified_data = modify_data(data, columns)
csv_file_path = 'get.csv'
save_as_csv(modified_data, csv_file_path)
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
