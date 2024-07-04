import pandas as pd
import datetime
import csv
import json
from pymongo import MongoClient

def read_lines_from_file(file_path):
    with open(file_path, encoding="utf-8-sig") as file:
        lines = file.read().strip().split('\n')
    return lines

def process_lines(lines):
    data = {
        'DATE': [],
        'TIME': [],
        'ERROR': [],
        'ID': [],
        'SPECIAL_ID': [],
        'OPEN': [],
        'Type_of_Error': [],
        'FAILED': [],
        'ERROR_MESSAGE': [],
        'CLIENT': [],
        'SERVER': [],
        'REQUEST': [],
        'HOST': [],
        'REFERRER': [],
        'time': [],
        'user_host': [],
        'query_time': [],
        'lock_time': [],
        'rows_sent': [],
        'rows_examined': [],
        'SET_timestamp': [],
        'CALL_proc': [],
        'type': [],
        'Count': []  # New column for counting errors
    }

    unique_type_of_errors = set() 
    error_counts = {}

    for line in lines:
        parts = line.strip().split()
        if len(parts) < 14:
            continue

        data['DATE'].append(parts[0])
        data['TIME'].append(parts[1])
        data['ERROR'].append(parts[2].strip('[]'))

        if parts[3].startswith("1"):
            data['ID'].append(parts[3])
        else:
            data['ID'].append('')

        if parts[4].startswith("*"):
            data["SPECIAL_ID"].append(parts[4])
        else:
            data["SPECIAL_ID"].append('')

        open_index = line.find('open()')
        if open_index != -1:
            data['OPEN'].append(parts[parts.index('open()') + 1])
        else:
            data['OPEN'].append('')

        if 'failed' in line:
            failed_index = line.find('failed')
            type_of_error = parts[parts.index('failed') - 1]
            if type_of_error not in unique_type_of_errors:
                data['Type_of_Error'].append(type_of_error)
                unique_type_of_errors.add(type_of_error)
                data['FAILED'].append('failed')
                data['ERROR_MESSAGE'].append(line[failed_index + 7:].split(',')[0])
                data['Count'].append(1)  # Initialize count to 1
                error_counts[type_of_error] = 1
            else:
                error_counts[type_of_error] += 1
                index = list(unique_type_of_errors).index(type_of_error)
                data['Count'].append(error_counts[type_of_error])
        else:
            data['Type_of_Error'].append('')
            data['FAILED'].append('')
            data['ERROR_MESSAGE'].append('')
            data['Count'].append('')

        if 'client:' in line:
            data['CLIENT'].append(parts[parts.index('client:') + 1].rstrip(','))
        else:
            data['CLIENT'].append('')

        if 'server:' in line:
            data['SERVER'].append(parts[parts.index('server:') + 1].rstrip(','))
        else:
            data['SERVER'].append('')

        request_index = parts.index('request:') if 'request:' in parts else -1
        if request_index != -1:
            data['REQUEST'].append(parts[request_index + 1].strip('",'))
        else:
            data['REQUEST'].append('')

        if 'host:' in line:
            data['HOST'].append(parts[parts.index('host:') + 1].strip('",'))
        else:
            data['HOST'].append('')

        if 'referrer:' in line:
            data['REFERRER'].append(parts[parts.index('referrer:') + 1].strip('",'))
        else:
            data['REFERRER'].append('')
        data['time'].append('none')
        data['user_host'].append('none')
        data['query_time'].append('none')
        data['lock_time'].append('none')
        data['rows_sent'].append('none')
        data['rows_examined'].append('none')
        data['SET_timestamp'].append('none')
        data['CALL_proc'].append('none')
        data['type'].append('php')

    unique_rows = []
    for i in range(len(data['Type_of_Error'])):
        if data['Type_of_Error'][i] in unique_type_of_errors:
            unique_rows.append(i)

    unique_data = {}
    for key, values in data.items():
        unique_data[key] = [values[i] for i in unique_rows]

    return pd.DataFrame(unique_data)

def save_as_csv(df, csv_file_path):
    try:
        df.to_csv(csv_file_path, index=False)
        print(f"Data saved as {csv_file_path}")
    except Exception as e:
        print(f"Error saving CSV file: {str(e)}")

def convert_csv_to_json(csv_file_path, json_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        df['Current_time'] = datetime.datetime.utcnow().isoformat()

        df.to_json(json_file_path, orient='records', indent=4)
        
        print(f"Data converted to JSON and saved as {json_file_path}")
    except Exception as e:
        print(f"Error converting CSV to JSON: {str(e)}")

def insert_json_data_to_mongodb(json_file_path, collection):
    try:
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        if json_data:
            result = collection.insert_many(json_data)
            inserted_ids = result.inserted_ids
            print("Data inserted successfully.")
        else:
            print("No JSON data found or the data is empty.")
    except Exception as e:
        print(f"Error inserting JSON data to MongoDB: {str(e)}")

def main():
    file_path = 'fifth.txt'
    lines = read_lines_from_file(file_path)
    df = process_lines(lines)
    csv_file_path = 'fifth.csv'
    save_as_csv(df, csv_file_path)
    json_file_path = 'fifth.json'
    convert_csv_to_json(csv_file_path, json_file_path)
    client = MongoClient('mongodb://localhost:27017/')
    database = client['mydata']
    collection = database['counteddata']

    collection.create_index('_id')

    insert_json_data_to_mongodb(json_file_path, collection)

if __name__ == '__main__':
    main()