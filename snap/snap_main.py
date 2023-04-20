import pandas as pd
import json
import csv
from datetime import datetime


class SnapDataParser:
    def __init__(self, start_year, end_year, summary_filepath, all_programs_filepath, monthly_participation_filepath,
                 total_costs_filepath):
        self.summary_filepath = summary_filepath
        self.all_programs_filepath = all_programs_filepath
        self.monthly_participation_filepath = monthly_participation_filepath
        self.total_costs_filepath = total_costs_filepath
        self.start_year = start_year
        self.end_year = end_year
        self.summary_file_dict = dict()
        self.all_programs__dict = dict()
        self.state_distribution_data_dict = dict()

        self.us_state_abbreviation = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming',
            'DC': 'District of Columbia',
            'MP': 'Northern Mariana Islands',
            'PW': 'Palau',
            'PR': 'Puerto Rico',
            'VI': 'Virgin Islands',
            'AA': 'Armed Forces Americas (Except Canada)',
            'AE': 'Armed Forces Africa/Canada/Europe/Middle East',
            'AP': 'Armed Forces Pacific'
        }

        # Load JSON files
        with open(self.summary_filepath) as summary_file:
            self.summary_file_dict = json.load(summary_file)

        with open(self.all_programs_filepath) as all_programs_file:
            self.all_programs__dict = json.load(all_programs_file)

    def parse_data(self):
        snap_monthly_participation_data = pd.read_csv(self.monthly_participation_filepath)
        snap_costs_data = pd.read_csv(self.total_costs_filepath)
        # TODO: Change summary file processing to use Pandas as well.

        # Iterate through summary file dict
        for item in self.summary_file_dict:
            if item["Title"] == "Supplemental Nutrition Assistance Program (SNAP)":
                average_monthly_participation = \
                    snap_monthly_participation_data[snap_monthly_participation_data["State"] == item["State"]][
                        str(item["Fiscal Year"])]
                item["Average Monthly Participation"] = int(average_monthly_participation)
                state_cost = snap_costs_data[snap_costs_data["State"] == item["State"]][str(item["Fiscal Year"])]
                item["Amount"] = int(state_cost)

        # Iterate through all programs file dict
        for item in self.all_programs__dict:
            state_total = 0
            for year in range(self.start_year, self.end_year + 1):
                if "SNAP " + str(year) in item:
                    state_cost = snap_costs_data[snap_costs_data["State"] == item["State"]][str(year)]
                    rounded_state_cost = int(state_cost)
                    item["SNAP " + str(year)] = rounded_state_cost
                    state_total += rounded_state_cost
            if "SNAP Total" in item:
                item["SNAP Total"] = state_total

        # Programs list
        programs_list = ["Crop Insurance", "SNAP", "Title I", "Title II"]

        start_year_obj = datetime(self.start_year, 1, 1)
        end_year_obj = datetime(self.end_year, 1, 1)

        # Update totals
        for item in self.all_programs__dict:
            year_range_all_programs_total = 0
            for year in range(self.start_year, self.end_year + 1):
                year_all_programs_total = 0
                for program in programs_list:
                    year_all_programs_total += item[program + " " + str(year)]
                item[str(year) + " All Programs Total"] = round(year_all_programs_total, 2)
                year_range_all_programs_total += year_all_programs_total

            key = start_year_obj.strftime("%y") + "-" + end_year_obj.strftime("%y") + " All Programs Total"
            item[key] = round(year_range_all_programs_total, 2)

        # Tabular data JSON
        snap_costs_data_total = snap_costs_data[snap_costs_data["State"] == "Total"]

        # TODO: The following code snippet is not used and could be deleted later.
        # for index, row in snap_costs_data.iterrows():
        #     if row["State"] != "Total" and row["State"] != "Total (w/o DC)":
        #         data_entry_list = []
        #         state_name = self.us_state_abbreviation[row["State"]]
        #         state_snap_participation_data = \
        #             snap_monthly_participation_data[snap_monthly_participation_data["State"] == row["State"]]
        #         for year in range(self.start_year, self.end_year + 1):
        #             str_year = str(year)
        #             total_monthly_average_participation_for_year = round(
        #                 snap_monthly_participation_data[str_year].sum(), 2)
        #             monthly_average_participation = state_snap_participation_data[str_year].item()
        #
        #             data_entry_list.append({
        #                 "years": str_year,
        #                 "totalPaymentInDollars": row[str_year],
        #                 "totalPaymentInPercentageNationwide": round(
        #                     row[str_year] / snap_costs_data_total[str_year].item() * 100, 2),
        #                 "averageMonthlyParticipation": monthly_average_participation,
        #                 "averageMonthlyParticipationInPercentageNationwide": round(
        #                     monthly_average_participation / total_monthly_average_participation_for_year * 100, 2)
        #             })
        #
        #         # Total for start to end years
        #         monthly_average_participation_all_years = state_snap_participation_data["Avg."].item()
        #         total_monthly_average_participation_all_years = round(snap_monthly_participation_data["Avg."].sum(), 2)
        #         data_entry_list.append({
        #             "years": str(self.start_year) + "-" + str(self.end_year),
        #             "totalPaymentInDollars": row["Total"],
        #             "totalPaymentInPercentageNationwide": round(
        #                 row["Total"] / snap_costs_data_total["Total"].item() * 100, 2),
        #             "averageMonthlyParticipation": monthly_average_participation_all_years,
        #             "averageMonthlyParticipationInPercentageNationwide": round(
        #                 monthly_average_participation_all_years / total_monthly_average_participation_all_years * 100,
        #                 2)
        #         })
        #         self.state_distribution_data_dict[state_name] = data_entry_list

        for state in self.us_state_abbreviation:
            state_snap_participation_data = snap_monthly_participation_data[
                snap_monthly_participation_data["State"] == state]
            state_snap_costs_data = snap_costs_data[snap_costs_data["State"] == state]

            for year in range(self.start_year, self.end_year + 1):
                str_year = str(year)
                total_monthly_average_participation_for_year = round(
                    snap_monthly_participation_data[str_year].sum(), 2)

                if state_snap_participation_data[str_year].size > 0:
                    monthly_average_participation = state_snap_participation_data[str_year].item()

                    if str_year not in self.state_distribution_data_dict:
                        self.state_distribution_data_dict[str_year] = []
                    self.state_distribution_data_dict[str_year].append({
                        "state": state,
                        "totalPaymentInDollars": state_snap_costs_data[str_year].item(),
                        "totalPaymentInPercentageNationwide": round(
                            state_snap_costs_data[str_year].item() / snap_costs_data_total[str_year].item() * 100, 2),
                        "averageMonthlyParticipation": monthly_average_participation,
                        "averageMonthlyParticipationInPercentageNationwide": round(
                            monthly_average_participation / total_monthly_average_participation_for_year * 100, 2)
                    })

            # Total for start to end years
            if state_snap_participation_data["Avg."].size > 0:
                monthly_average_participation_all_years = state_snap_participation_data["Avg."].item()
                total_monthly_average_participation_all_years = round(snap_monthly_participation_data["Avg."].sum(), 2)

                str_year = str(self.start_year) + "-" + str(self.end_year)
                if str_year not in self.state_distribution_data_dict:
                    self.state_distribution_data_dict[str_year] = []
                self.state_distribution_data_dict[str_year].append({
                    "state": state,
                    "totalPaymentInDollars": state_snap_costs_data["Total"].item(),
                    "totalPaymentInPercentageNationwide": round(
                        state_snap_costs_data["Total"].item() / snap_costs_data_total["Total"].item() * 100, 2),
                    "averageMonthlyParticipation": monthly_average_participation_all_years,
                    "averageMonthlyParticipationInPercentageNationwide": round(
                        monthly_average_participation_all_years / total_monthly_average_participation_all_years * 100,
                        2)
                })

        for year in self.state_distribution_data_dict:
            # For each year, sort states by decreasing order of percentages (totalPaymentInPercentageNationwide)
            self.state_distribution_data_dict[year] = sorted(self.state_distribution_data_dict[year],
                                                             key=lambda x: x["totalPaymentInPercentageNationwide"],
                                                             reverse=True)

        # Write processed_data_dict as JSON data
        with open("snap_state_distribution_data.json", "w") as output_json_file:
            output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=4))

    def update_json_files(self):
        with open(self.summary_filepath + ".updated.json", "w") as summary_file_new:
            json.dump(self.summary_file_dict, summary_file_new, indent=2)

        with open(self.all_programs_filepath + ".updated.json", "w") as all_programs_file_new:
            json.dump(self.all_programs__dict, all_programs_file_new, indent=2)

    def update_csv_files(self):
        with open(self.summary_filepath + ".updated.csv", "w") as summary_file_new:
            writer = csv.DictWriter(summary_file_new, fieldnames=["Title", "State", "Fiscal Year", "Amount",
                                                                  "Average Monthly Participation"])
            writer.writeheader()
            writer.writerows(self.summary_file_dict)

        with open(self.all_programs_filepath + ".updated.csv", "w") as all_programs_file_new:
            # TODO: Continue from here - generate field names
            writer = csv.DictWriter(all_programs_file_new, fieldnames=["State", ""])
            writer.writeheader()
            writer.writerows(self.summary_file_dict)


if __name__ == '__main__':
    snap_data_parser = SnapDataParser(2018, 2022, "summary.json", "allPrograms.json", "snap_monthly_participation.csv",
                                      "snap_costs.csv")
    snap_data_parser.parse_data()
    snap_data_parser.update_json_files()
