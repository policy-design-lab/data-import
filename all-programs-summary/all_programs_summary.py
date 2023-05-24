import pandas as pd
import json

from datetime import datetime


class AllProgramsParser:
    def __init__(self, start_year, end_year, topline_csv_filepath, all_programs_json_filepath, summary_json_filepath):
        self.start_year = start_year
        self.end_year = end_year
        self.topline_csv_filepath = topline_csv_filepath
        self.all_programs_json_filepath = all_programs_json_filepath
        self.summary_json_filepath = summary_json_filepath

        self.all_programs_data = None
        self.all_programs_dict = None
        self.summary_data = None

    def parse_and_process(self):
        # Import JSON files into a Pandas DataFrame
        self.all_programs_data = pd.read_json(self.all_programs_json_filepath)
        self.summary_data = pd.read_json(self.summary_json_filepath)
        topline_data = pd.read_csv(self.topline_csv_filepath)
        crop_insurance_total = topline_data["ci_net_benefit"].sum()

        for index, row in self.all_programs_data.iterrows():
            total_amount = 0.0
            for year in range(self.start_year, self.end_year + 1):

                if row["State"] == "Total":
                    topline_data_year = topline_data[(topline_data["year"] == year)]
                    crop_insurance_total_for_year = topline_data_year["ci_net_benefit"].sum()
                    self.all_programs_data.at[index, "Crop Insurance " + str(year)] = round(
                        crop_insurance_total_for_year, 2)
                else:
                    topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                                                           (topline_data["year"] == year)]
                    if topline_data_state_year.size != 0:
                        amount = topline_data_state_year["ci_net_benefit"].item()
                        total_amount += amount
                        self.all_programs_data.at[index, "Crop Insurance " + str(year)] = round(amount, 2)

            if row["State"] == "Total":
                self.all_programs_data.at[index, "Crop Insurance Total"] = round(crop_insurance_total, 2)
            else:
                self.all_programs_data.at[index, "Crop Insurance Total"] = round(total_amount, 2)

        # Programs list
        programs_list = ["Crop Insurance", "SNAP", "Title I", "Title II"]

        start_year_obj = datetime(self.start_year, 1, 1)
        end_year_obj = datetime(self.end_year, 1, 1)

        self.all_programs_dict = json.loads(
            self.all_programs_data.to_json(indent=2, orient="records", double_precision=2))
        # Update totals
        for item in self.all_programs_dict:
            year_range_all_programs_total = 0
            for year in range(self.start_year, self.end_year + 1):
                year_all_programs_total = 0
                for program in programs_list:
                    year_all_programs_total += item[program + " " + str(year)]
                item[str(year) + " All Programs Total"] = round(year_all_programs_total, 2)
                year_range_all_programs_total += year_all_programs_total

            key = start_year_obj.strftime("%y") + "-" + end_year_obj.strftime("%y") + " All Programs Total"
            item[key] = round(year_range_all_programs_total, 2)

        for index, row in self.summary_data.iterrows():
            for year in range(self.start_year, self.end_year + 1):
                if row["Title"] == "Crop Insurance" and row["Fiscal Year"] == year:
                    topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                                                           (topline_data["year"] == year)]
                    if topline_data_state_year.size != 0:
                        amount = topline_data_state_year["ci_net_benefit"].item()
                        self.summary_data.at[index, "Amount"] = round(amount, 2)

    def write_updated_json_files(self):
        with open(self.summary_json_filepath + ".updated.json", "w") as summary_file_new:
            summary_data_dict = self.summary_data.to_json(indent=2, orient="records", double_precision=2)
            summary_file_new.write(summary_data_dict)

        with open(self.all_programs_json_filepath + ".updated.json", "w") as all_programs_file_new:
            all_programs_file_new.write(json.dumps(self.all_programs_dict, indent=2))


if __name__ == '__main__':
    all_programs_parser = AllProgramsParser(2018, 2022, "topline.csv", "allprograms.json", "summary.json")
    all_programs_parser.parse_and_process()
    all_programs_parser.write_updated_json_files()
