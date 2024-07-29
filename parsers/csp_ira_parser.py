import json
import os
import re
import sys

import numpy as np
import pandas as pd


class CspIraParser:
    def __init__(self, fiscal_year, start_year, end_year, data_folder,
                 practice_count_data, actual_pay_data, practice_code_data):
        self.practice_count_data = practice_count_data
        self.actual_pay_data = actual_pay_data
        self.practice_code_data = practice_code_data
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

    def create_state_distribution(self, df1, df2):
        year = str(self.fiscal_year)
        states = df1['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df1 = df1[df1['state'].isin(states)]

        # check the unique states in both df1 and df2
        if len(df1['state'].unique()) != 50:
            print("The state for the table is not 50")
            print("Exit state distribution function")
            sys.exit()

        output = {year: []}

        # create state distribution data
        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)

            year_data = {
                "state": state_abbr,
                "totalPaymentInDollars": 0,
                "totalPaymentPercentageNationwide": 0,
                "practices": []
            }

            # create practice number list from df2
            practice_codes = df2['practice_code'].tolist()

            for column in df1.columns:
                if column.startswith("p_"):
                    # Extract practice_code from the column name
                    practice_code = column[2:]

                    if practice_code in df2['practice_code'].values:
                        practice_name = df2.loc[df2['practice_code'] == practice_code, 'practice_name'].values[0]
                        # Add number to the practice name
                        practice_name = f"{practice_name} ({practice_code})"

                        practice_data = {
                            "practiceName": practice_name,
                            "totalPaymentInDollars": 0
                        }
                        dollar_values = df1[df1['state'] == state][column]
                        if not dollar_values.empty:
                            practice_data["totalPaymentInDollars"] = float(
                                dollar_values.sum())  # Assuming you want the sum of values
                            year_data["totalPaymentInDollars"] += practice_data["totalPaymentInDollars"]

                        year_data["practices"].append(practice_data)

            # round each state's total payment to 2 decimal places
            year_data["totalPaymentInDollars"] = \
                round(year_data["totalPaymentInDollars"], 2)

            # sort year_data by practice name's number
            year_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

            output[str(year)].append(year_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[str(year)][i]["totalPaymentInDollars"]
                                 for i in range(len(output[str(year)]))])

            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["totalPaymentPercentageNationwide"] = \
                        round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["totalPaymentPercentageNationwide"] = 0

        # sort the predicted year by total payment in dollars
        output[year].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)

        for state in output[year]:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def create_summary(self, df1, df2):
        states = df1['state'].unique()

        if len(states) > 50:
            print("There are more than 50 states in the data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]
        df1 = df1[df1['state'].isin(states)]

        if len(df1['state'].unique()) != 50:
            print("The state for the first table is not 50")
            print("Exit state distribution function")
            sys.exit()

        year = self.fiscal_year
        df1 = df1[df1['year'] == year]

        # create output template
        output = {year: []}

        # create summary data
        # sum all the values by columns and transpose the sum
        df1 = df1.sum()
        df1 = df1.to_frame().T

        year_data = {
            "totalPaymentInDollars": 0,
            "practices": []
        }

        for column in df1.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df2['practice_code'].values:
                    practice_name = df2.loc[df2['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"

                    practice_data = {
                        "practiceName": practice_name,
                        "totalPaymentInDollars": 0,
                        "totalPaymentInPercentageNationwide": 0
                    }
                    dollar_values = df1[column]
                    if not dollar_values.empty:
                        practice_data["totalPaymentInDollars"] = float(dollar_values.sum())
                        year_data["totalPaymentInDollars"] += practice_data["totalPaymentInDollars"]

                    year_data["practices"].append(practice_data)

        # round each state's total payment to 2 decimal places
        year_data["totalPaymentInDollars"] = \
            round(year_data["totalPaymentInDollars"], 2)

        # sort year_data by practice name's number
        year_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

        output[year].append(year_data)

        # calculate total payment percentage nationwide for payment for each year
        total_payment = sum([output[year][i]["totalPaymentInDollars"]
                             for i in range(len(output[year]))])

        # add total payment in percentage nationwide values
        for practice_data in output[year][0]["practices"]:
            # avoid division by zero
            if total_payment != 0:
                practice_data["totalPaymentInPercentageNationwide"] = \
                    round((practice_data["totalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                practice_data["totalPaymentInPercentageNationwide"] = 0


        # for state_data in output[year]:
        #     # avoid division by zero
        #     if total_payment != 0:
        #         state_data["totalPaymentPercentageNationwide"] = \
        #             round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
        #     else:
        #         state_data["totalPaymentPercentageNationwide"] = 0
        #
        # # sort the predicted year by total payment in dollars
        # output[year].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)
        #
        # for state in output[year]:
        #     for practice in state["practices"]:
        #         for key, value in practice.items():
        #             if isinstance(value, float) and not value.is_integer():
        #                 practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def parse_and_process(self):
        df1 = pd.read_excel(self.practice_count_data)
        df2 = pd.read_excel(self.actual_pay_data)
        df3 = pd.read_csv(self.practice_code_data)
        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)

        # remove rows with "Total" in the "Practice Name" column
        df1 = df1[~df1['PRACTICE NAME'].str.match("Total")]

        # select only 2023 from FISCAL YEAR column
        df1 = df1[df1['FISCAL YEAR'] == self.fiscal_year]

        # create unique practices list
        self.unique_practices = df1['PRACTICE NAME'].unique()

        # convert the unique practices to a list and put under the fiscal year json
        unique_practices = self.unique_practices.tolist()

        # sort the unique practices by value in the parenthesis
        # unique_practices.sort(key=lambda x: int(re.search(r'\((\d+)\)', x).group(1)))
        unique_practices.sort()

        unique_practices_dict = {self.fiscal_year: unique_practices}

        # dump unique practices to a json file
        with open(os.path.join(self.data_folder, "csp_ira_practices.json"), "w") as json_file:
            json.dump(unique_practices_dict, indent=4, fp=json_file)

        # create state distribution json
        # state_distribution_data = self.create_state_distribution(df2, df3)
        # with open(os.path.join(self.data_folder, "csp_ira_state_distribution.json"), "w") as json_file:
        #     json_file.write(state_distribution_data)

        # create summary json
        summary_data = self.create_summary(df2, df3)
        with open(os.path.join(self.data_folder, "csp_ira_summary.json"), "w") as json_file:
            json_file.write(summary_data)


if __name__ == '__main__':
    fiscal_year = "2023"
    start_year = 2024
    end_year = 2031
    practice_count_data = \
        "../title-2-conservation/csp_ira/" \
        "Practice FIPS Download - All programs and practice counts and obligation (May 6 2024).xlsx"
    actual_pay_data = \
        "../title-2-conservation/csp_ira/20240724_CSP-IRA_combined_practice_actualpay.xlsx"
    practice_code_data = "../title-2-conservation/common/merged_practice_standards.csv"
    csp_data_parser = CspIraParser(
        fiscal_year, start_year, end_year, "../title-2-conservation/csp_ira/",
        practice_count_data, actual_pay_data, practice_code_data)
    csp_data_parser.parse_and_process()
