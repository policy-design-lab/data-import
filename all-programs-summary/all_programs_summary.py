import json
from datetime import datetime

import pandas as pd


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
        self.summary_data["Average Monthly Participation"] = self.summary_data["Average Monthly Participation"].astype(
            "Int64")

        topline_data = pd.read_csv(self.topline_csv_filepath)

        # Additional check to filter data for only required years
        topline_data = topline_data[topline_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        title_i_grand_total = topline_data["titlei"].sum()
        title_ii_grand_total = topline_data["title_ii"].sum()
        crop_insurance_grand_total = topline_data["ci_net_benefit"].sum()
        snap_grand_total = topline_data["snap_cost"].sum()

        for index, row in self.all_programs_data.iterrows():
            crop_insurance_total_amount = 0.0
            snap_total_amount = 0.0
            title_i_total_amount = 0.0
            title_ii_total_amount = 0.0
            for year in range(self.start_year, self.end_year + 1):

                if row["State"] == "Total":
                    topline_data_year = topline_data[(topline_data["year"] == year)]
                    title_i_total_for_year = topline_data_year["titlei"].sum()
                    title_ii_total_for_year = topline_data_year["title_ii"].sum()
                    crop_insurance_total_for_year = topline_data_year["ci_net_benefit"].sum()
                    snap_total_for_year = topline_data_year["snap_cost"].sum()

                    self.all_programs_data.at[index, "Title I " + str(year)] = round(
                        title_i_total_for_year, 2)
                    # TODO: Uncomment below code for different programs as accurate raw data becomes available.
                    # self.all_programs_data.at[index, "Title II " + str(year)] = round(
                    #     title_ii_total_for_year, 2)
                    # self.all_programs_data.at[index, "Crop Insurance " + str(year)] = round(
                    #     crop_insurance_total_for_year, 2)
                    # self.all_programs_data.at[index, "SNAP " + str(year)] = round(
                    #     snap_total_for_year, 2)
                else:
                    topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                                                           (topline_data["year"] == year)]
                    if topline_data_state_year.size != 0:
                        title_i_amount = topline_data_state_year["titlei"].item()
                        title_i_total_amount += title_i_amount
                        self.all_programs_data.at[index, "Title I " + str(year)] = round(title_i_amount, 2)

                        # TODO: Uncomment below code for different programs as accurate raw data becomes available.
                        # title_ii_amount = topline_data_state_year["title_ii"].item()
                        # title_ii_total_amount += title_ii_amount
                        # self.all_programs_data.at[index, "Title II " + str(year)] = round(title_ii_amount, 2)
                        #
                        # crop_insurance_amount = topline_data_state_year["ci_net_benefit"].item()
                        # crop_insurance_total_amount += crop_insurance_amount
                        # self.all_programs_data.at[index, "Crop Insurance " + str(year)] = round(crop_insurance_amount,
                        #                                                                         2)
                        #
                        # snap_amount = topline_data_state_year["snap_cost"].item()
                        # snap_total_amount += snap_amount
                        # self.all_programs_data.at[index, "SNAP " + str(year)] = round(snap_amount, 2)

            if row["State"] == "Total":
                self.all_programs_data.at[index, "Title I Total"] = round(title_i_grand_total, 2)
                # TODO: Uncomment below code for different programs as accurate raw data becomes available.
                # self.all_programs_data.at[index, "Title II Total"] = round(title_ii_grand_total, 2)
                # self.all_programs_data.at[index, "Crop Insurance Total"] = round(crop_insurance_grand_total, 2)
                # self.all_programs_data.at[index, "SNAP Total"] = round(snap_grand_total, 2)
            else:
                self.all_programs_data.at[index, "Title I Total"] = round(title_i_total_amount, 2)
                # TODO: Uncomment below code for different programs as accurate raw data becomes available.
                # self.all_programs_data.at[index, "Title II Total"] = round(title_ii_total_amount, 2)
                # self.all_programs_data.at[index, "Crop Insurance Total"] = round(crop_insurance_total_amount, 2)
                # self.all_programs_data.at[index, "SNAP Total"] = round(snap_total_amount, 2)

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
                    if item[program + " " + str(year)]:
                        year_all_programs_total += item[program + " " + str(year)]
                item[str(year) + " All Programs Total"] = round(year_all_programs_total, 2)
                year_range_all_programs_total += year_all_programs_total

            key = start_year_obj.strftime("%y") + "-" + end_year_obj.strftime("%y") + " All Programs Total"
            item[key] = round(year_range_all_programs_total, 2)

        for index, row in self.summary_data.iterrows():
            for year in range(self.start_year, self.end_year + 1):
                if row["Title"] == "Title I: Commodities" and row["Fiscal Year"] == year:
                    topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                                                           (topline_data["year"] == year)]
                    if topline_data_state_year.size != 0:
                        title_i_amount = topline_data_state_year["titlei"].item()
                        self.summary_data.at[index, "Amount"] = round(title_i_amount, 2)
                # TODO: Uncomment below code for different programs as accurate raw data becomes available.
                # elif row["Title"] == "Title II: Commodities" and row["Fiscal Year"] == year:
                #     topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                #                                            (topline_data["year"] == year)]
                #     if topline_data_state_year.size != 0:
                #         title_ii_amount = topline_data_state_year["snap_cost"].item()
                #         self.summary_data.at[index, "Amount"] = round(title_ii_amount, 2)
                # elif row["Title"] == "Crop Insurance" and row["Fiscal Year"] == year:
                #     topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                #                                            (topline_data["year"] == year)]
                #     if topline_data_state_year.size != 0:
                #         crop_insurance_amount = topline_data_state_year["ci_net_benefit"].item()
                #         self.summary_data.at[index, "Amount"] = round(crop_insurance_amount, 2)
                # elif row["Title"] == "Supplemental Nutrition Assistance Program (SNAP)" and row["Fiscal Year"] == year:
                #     topline_data_state_year = topline_data[(topline_data["abbreviation"] == row["State"]) &
                #                                            (topline_data["year"] == year)]
                #     if topline_data_state_year.size != 0:
                #         snap_amount = topline_data_state_year["snap_cost"].item()
                #         self.summary_data.at[index, "Amount"] = round(snap_amount, 2)

    def write_updated_json_files(self):
        with open(self.summary_json_filepath + ".updated.json", "w") as summary_file_new:
            summary_file_new.write(
                json.dumps([row.dropna().to_dict() for index, row in self.summary_data.iterrows()], indent=2))

        with open(self.all_programs_json_filepath + ".updated.json", "w") as all_programs_file_new:
            all_programs_file_new.write(json.dumps(self.all_programs_dict, indent=2))


if __name__ == '__main__':
    all_programs_parser = AllProgramsParser(2018, 2022, "topline.csv", "allprograms.json", "summary.json")
    all_programs_parser.parse_and_process()
    all_programs_parser.write_updated_json_files()
