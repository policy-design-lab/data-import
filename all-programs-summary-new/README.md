# Update All Programs and Summary JSON files

## consolidated_data.xlsx

The raw and processed data is stored in this XLSX file. The sheets with `(Main)` in the name contains processed data for
individual titles. There are hidden sheets with `Raw` in the name that contains raw data for individual titles and
programs. `All Programs - Final` and `Summary - Final` sheets contain final data used to generate the JSON files.

The outline of the approach was to copy all the raw data into she sheets with `Raw` in the name. Then, the data was
processed using pivot tables where necessary and copied to sheets with `(Main)` in the name. Finally, the data was
copied to `All Programs - Final` and `Summary - Final` using formulae referring to the `Main` sheets and in the format
needed for the JSON documents.

## Run Convert CSV to JSON

1. Copy the contents of `All Programs - Final` and `Summary - Final` sheets to CSV files.
2. Run the following commands to convert the CSV files to JSON files.

```bash
python3 convert_csv_to_json.py all_programs.csv allprograms.json
python3 convert_csv_to_json.py summary.csv summary.json
```