import json
import os
import re
import sys

import pandas as pd


class HouseOutlayParser:
    def __init__(self, fiscal_year, start_year, end_year, data_folder, practice_code_data, house_outlay_max_data):
        self.practice_code_data = practice_code_data
        self.house_outlay_max_data = house_outlay_max_data
        self.fiscal_year = int(fiscal_year)
        self.start_year = start_year
        self.end_year = end_year
        self.unique_practices = []
        self.data_folder = data_folder

        self.us_state_abbreviation = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AS': 'American Samoa', 'AZ': 'Arizona',
            'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut',
            'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
            'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine',
            'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
            'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska',
            'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
            'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
            'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
            'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
            'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
            'MP': 'Northern Mariana Islands', 'PW': 'Palau', 'PR': 'Puerto Rico',
            'AA': 'Armed Forces Americas (Except Canada)',
            'AE': 'Armed Forces Africa/Canada/Europe/Middle East', 'AP': 'Armed Forces Pacific',
            'VI': 'U.S. Virgin Islands'
        }

        self.us_50_states = [
            'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
            'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
            'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
            'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
            'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
            'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
            'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
            'Wisconsin', 'Wyoming'
        ]

        self.state_name_to_abbreviation = {v: k for k, v in self.us_state_abbreviation.items()}

    def extract_practice_number(self, practice_name):
        match = re.search(r'\((?:[^\d]*)(\d+)(?:[^\d]*)\)', practice_name)
        return int(match.group(1)) if match else None

    def extract_practice_number_clean(self, practice_name):
        match = re.search(r'\((\d+)\)', practice_name)
        return int(match.group(1)) if match else None

    def replace_state_name_with_abbreviation(self, state_name):
        return self.state_name_to_abbreviation.get(state_name, state_name)

    def create_house_outlay_max_by_year(self, df1, df2):
        states = df2['state'].unique()

        if len(states) > 50:
            print("There are more than 50 states in the payment data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        df2 = df2[df2['state'].isin(states)]

        if len(df2['state'].unique()) != 50:
            print("The state for the house outlay max table is not 50")
            print("Exit state distribution function")
            sys.exit()

        ###########################
        # create future year data #
        ###########################

        output = {str(year): [] for year in range(self.fiscal_year, self.end_year + 1)}

        for year in range(self.start_year, self.end_year + 1):
            for state in states:
                state_abbr = self.replace_state_name_with_abbreviation(state)

                year_data = {
                    "state": state_abbr,
                    "predictedMinimumTotalPaymentInDollars": 0,
                    "predictedMaximumTotalPaymentInDollars": 0,
                    "predictedMinimumTotalPaymentPercentageNationwide": 0,
                    "predictedMaximumTotalPaymentPercentageNationwide": 0,
                    "practices": []
                }

                for column in df2.columns:
                    if column.startswith("p_"):
                        # Extract practice_code from the column name
                        practice_code = column[2:]

                        if practice_code in df1['practice_code'].values:
                            practice_name = df1.loc[df1['practice_code'] == practice_code, 'practice_name'].values[0]
                            # Add number to the practice name
                            practice_name = f"{practice_name} ({practice_code})"

                            practice_data = {
                                "practiceName": practice_name,
                                "predictedMinimumTotalPaymentInDollars": 0,
                                "predictedMaximumTotalPaymentInDollars": 0,
                            }

                            max_values = df2[(df2['state'] == state) & (df2['year'] == year)][column]

                            if not max_values.empty:
                                practice_data["predictedMaximumTotalPaymentInDollars"] += float(max_values.values[0])
                                year_data["predictedMaximumTotalPaymentInDollars"] += practice_data[
                                    "predictedMaximumTotalPaymentInDollars"]
                                # round practice data to 2 decimal places
                                practice_data["predictedMaximumTotalPaymentInDollars"] = round(
                                    practice_data["predictedMaximumTotalPaymentInDollars"], 2)

                            year_data["practices"].append(practice_data)

                # round each state's total payment to 2 decimal places
                year_data["predictedMaximumTotalPaymentInDollars"] = \
                    round(year_data["predictedMaximumTotalPaymentInDollars"], 2)

                # sort year_data by practice name's number
                year_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

                output[str(year)].append(year_data)

                # calculate total payment percentage nationwide for payment for each year
                total_payment = sum([output[str(year)][i]["predictedMaximumTotalPaymentInDollars"]
                                     for i in range(len(output[str(year)]))])

                # add total payment in percentage nationwide values
                for state_data in output[str(year)]:
                    # avoid division by zero
                    if total_payment != 0:
                        state_data["predictedMaximumTotalPaymentPercentageNationwide"] = \
                            round((state_data["predictedMaximumTotalPaymentInDollars"] / total_payment) * 100, 2)
                    else:
                        state_data["predictedMaximumTotalPaymentPercentageNationwide"] = 0

        return json.dumps(output, indent=4)

    def create_house_outlay_max(self, df1, df2):
        future_year = str(self.start_year) + "-" + str(self.end_year)
        states = df2['state'].unique()

        if len(states) > 50:
            print("There are more than 50 states in the payment data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        df2 = df2[df2['state'].isin(states)]

        if len(df2['state'].unique()) != 50:
            print("The state for the house outlay max table is not 50")
            print("Exit state distribution function")
            sys.exit()

        output = {future_year: []}

        ###########################
        # create future year data #
        ###########################

        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)
            state_data = {
                "state": state_abbr,
                "predictedMaximumTotalPaymentInDollars": 0,
                "predictedMaximumTotalPaymentPercentageNationwide": 0,
                "practices": []
            }

            for column in df2.columns:
                if column.startswith("p_"):
                    # Extract practice_code from the column name
                    practice_code = column[2:]

                    if practice_code in df1['practice_code'].values:
                        practice_name = df1.loc[df1['practice_code'] == practice_code, 'practice_name'].values[0]
                        # Add number to the practice name
                        practice_name = f"{practice_name} ({practice_code})"

                        practice_data = {
                            "practiceName": practice_name,
                            "predictedMaximumTotalPaymentInDollars": 0,
                        }

                        # Sum up max values for all future years
                        for year in range(self.start_year, self.end_year + 1):
                            max_values = df2[(df2['state'] == state) & (df2['year'] == year)][column]
                            if not max_values.empty:
                                max_value = float(max_values.values[0])
                                practice_data["predictedMaximumTotalPaymentInDollars"] += max_value
                                state_data["predictedMaximumTotalPaymentInDollars"] += max_value

                        # round practice data to 2 decimal places
                        practice_data["predictedMaximumTotalPaymentInDollars"] = round(
                            practice_data["predictedMaximumTotalPaymentInDollars"], 2)
                        state_data["practices"].append(practice_data)

            # round each state's total payment to 2 decimal places
            state_data["predictedMaximumTotalPaymentInDollars"] = \
                round(state_data["predictedMaximumTotalPaymentInDollars"], 2)

            # sort year_data by practice name's number
            state_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

            output[future_year].append(state_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[future_year][i]["predictedMaximumTotalPaymentInDollars"]
                                 for i in range(len(output[future_year]))])

            # add total payment in percentage nationwide values
            for state_data in output[future_year]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedMaximumTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedMaximumTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedMaximumTotalPaymentPercentageNationwide"] = 0

        return json.dumps(output, indent=4)

    def create_unique_practices(self, df1, df2):
        future_year = str(self.start_year) + "-" + str(self.end_year)
        output = {future_year: []}

        # create unique practices data for the future year
        states = df2['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the future data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df2 = df2[df2['state'].isin(states)]

        # check the unique states in df1
        if len(df2['state'].unique()) != 50:
            print("The state for the future data table is not 50")
            print("Exit state distribution function")
            sys.exit()

        future_data = []
        for column in df2.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df1['practice_code'].values:
                    practice_name = df1.loc[df1['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"
                    future_data.append(practice_name)
                else:
                    print("Practice code missing in the merged practice standards CSV file: " + practice_code)

        # sort year_data by practice name's number
        future_data.sort(key=lambda x: self.extract_practice_number(x))

        output[future_year] = future_data

        return json.dumps(output, indent=4)

    def parse_and_process(self):
        df1 = pd.read_csv(self.practice_code_data)
        df2 = pd.read_csv(self.house_outlay_max_data)

        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df2.fillna(0, inplace=True)

        # dump unique practices to a json file
        unique_practices_data = self.create_unique_practices(df1, df2)
        with open(os.path.join(self.data_folder, "house_outlay_practices.json"), "w") as json_file:
            json_file.write(unique_practices_data)

        # create house outlay max json
        house_outlay_max_data = self.create_house_outlay_max(df1, df2)
        with open(os.path.join(self.data_folder, "house_outlay_max.json"), "w") as json_file:
            json_file.write(house_outlay_max_data)


if __name__ == '__main__':
    fiscal_year = "2023"
    start_year = 2024
    end_year = 2033
    practice_code_data = "../title-2-conservation/common/merged_practice_standards.csv"
    house_outlay_max_data = "../title-2-conservation/house_outlay/20241013_max_house_minus_baseline_ira_outlay.xlsx"
    house_outlay_parser = HouseOutlayParser(
        fiscal_year, start_year, end_year, "../title-2-conservation/house_outlay",
        practice_code_data, house_outlay_max_data)
    house_outlay_parser.parse_and_process()
