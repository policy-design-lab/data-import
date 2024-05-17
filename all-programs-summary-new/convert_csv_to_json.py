# Convert CSV file to JSON file

import csv
import json
import sys


def convert_csv_to_json(csv_file, json_file):
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as file:
            csv_reader = csv.DictReader(file)
            data = list(csv_reader)
            for data_row in data:
                for key, value in data_row.items():
                    try:
                        data_row[key] = int(value)
                    except ValueError:
                        try:
                            data_row[key] = float(value)
                        except ValueError:
                            pass
            with open(json_file, 'w') as json_file:
                json.dump(data, json_file, indent=2)
    except Exception as e:
        print(f"Error converting CSV to JSON: {e}")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python convert_csv_to_json.py input.csv output.json")
        sys.exit(1)
    csv_file = sys.argv[1]
    json_file = sys.argv[2]
    convert_csv_to_json(csv_file, json_file)
