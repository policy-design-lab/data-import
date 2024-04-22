# Update All Programs and Summary JSON files

## consolidated_data.xlsx

The raw and processed data is stored in this XLSX file. There are four groups of sheets.

1. Hidden sheets with `(Raw)` in the name contains raw data for individual titles and programs - raw sheets.
2. Hidden sheets with `(Pivot Table)` in the name contains pivot tables for some titles and programs - pivot sheets.
3. The sheets with `(Main)` in the name contains processed data for individual titles - main sheets.
4. `All Programs (Final)` and `Summary (Final)` sheets contain final data used to generate the JSON files - final
   sheets.

The outline of the approach was to copy all the raw data into raw sheets. Then, the data was processed using pivot
tables where necessary and copied to pivot sheets. Only relevant columns and calculations are used at this level. Later
the data from the pivot sheets and copied and combined to the main sheets. Finally, the data from the main sheets were
copied to `All Programs (Final)` and `Summary (Final)` using formulae referring to the `Main`
sheets and in the format needed for the JSON documents.

## Run Convert CSV to JSON

1. Copy the contents of `All Programs (Final)` and `Summary (Final)` sheets to CSV files.
2. Run the following commands to convert the CSV files to JSON files.

```bash
python3 convert_csv_to_json.py all_programs.csv allprograms.json
python3 convert_csv_to_json.py summary.csv summary.json
```