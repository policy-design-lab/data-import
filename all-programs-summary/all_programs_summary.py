import pandas as pd


class AllProgramsParser:
    def __int__(self, start_year, end_year, all_programs_csv_filepath, summary_csv_filepath):
        self.all_programs_csv_filepath = all_programs_csv_filepath
        self.summary_csv_filepath = summary_csv_filepath

    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        all_programs_data = pd.read_csv(self.all_programs_csv_filepath)


if __name__ == '__main__':
    all_programs_parser = AllProgramsParser()
