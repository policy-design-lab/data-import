import json
import os
import re
import sys

import numpy as np
import pandas as pd


class HouseOutlayParser:
    def __init__(self, fiscal_year, start_year, end_year, data_folder, total_table_filepath, future_max_filepath):
        self.total_table_filepath = total_table_filepath
        self.future_max_filepath = future_max_filepath
        self.fiscal_year = int(fiscal_year)
        self.start_year = start_year
        self.end_year = end_year
        self.unique_practices = []
        self.data_folder = data_folder
        # this variables if for the future year
        # if this is true, it will only contain the practices that are in the state in the year
        self.practices_in_state = False

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
        match = re.search(r'\((\d+)\)', practice_name)
        return int(match.group(1)) if match else None

    def convert_dollars(self, in_val):
        if isinstance(in_val, str):
            return int(in_val.replace("$", "").replace(",", ""))
        return in_val

    def replace_state_name_with_abbreviation(self, state_name):
        return self.state_name_to_abbreviation.get(state_name, state_name)

    def create_house_outlay_max(self, df1, df2):
        states = df1['STATE'].unique()
        state2 = df2['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50 or len(state2) > 50:
            print("There are more than 50 states in the data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df1 = df1[df1['STATE'].isin(states)]
        df2 = df2[df2['state'].isin(states)]

        # check the unique states in both df1 and df2
        if len(df1['STATE'].unique()) != 50:
            print("The state for the first table is not 50")
            print("Exit state distribution function")
            sys.exit()

        if len(df2['state'].unique()) != 50:
            print("The state for the second table is not 50")
            print("Exit state distribution function")
            sys.exit()

        # only select the 2023 from fiscal year for df1
        df1 = df1[df1['FISCAL YEAR'] == str(self.fiscal_year)]

        # remove the total row for practice name in df1
        df1 = df1[df1['PRACTICE NAME'].str.lower() != "total"]

        output = {str(year): [] for year in range(self.fiscal_year, self.end_year + 1)}

        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)
            practices = df1[df1['STATE'] == state]['PRACTICE NAME'].unique()

            year_data_2023 = {
                "state": state_abbr,
                "totalPaymentInDollars": 0,
                "totalPracticeInstanceCount": 0,
                "totalPaymentPercentageNationwide": 0,
                "totalPracticeInstancePercentageNationwide": 0,
                "practices": []
            }

            for practice in practices:
                practice_number = df1[(df1['STATE'] == state) &
                                      (df1['PRACTICE NAME'] == practice)]['practice_number'].values[0]
                practice_data = {
                    "practiceName": practice,
                    "practiceInstanceCount": 0,
                    "totalPaymentInDollars": 0
                    # "2023PaymentInDollars": 0
                }

                total_payment = df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (
                        df1['FISCAL YEAR'] == str(self.fiscal_year))]['DOLLARS OBLIGATED']
                if not total_payment.empty:
                    practice_data['totalPaymentInDollars'] = float(total_payment.values[0])
                    year_data_2023['totalPaymentInDollars'] += practice_data['totalPaymentInDollars']

                practice_instance_count = df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (
                        df1['FISCAL YEAR'] == str(self.fiscal_year))]['PRACTICE INSTANCE COUNT']
                if not practice_instance_count.empty:
                    practice_data['practiceInstanceCount'] = int(practice_instance_count.values[0])
                    year_data_2023['totalPracticeInstanceCount'] += practice_data['practiceInstanceCount']

                year_payment = df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (
                        df1['FISCAL YEAR'] == str(self.fiscal_year))]['DOLLARS OBLIGATED']
                # if not year_payment.empty:
                # practice_data['2023PaymentInDollars'] = float(year_payment.values[0])

                year_data_2023["practices"].append(practice_data)

            # sort year_data_2023's practices by practice name's practice number
            year_data_2023["practices"].sort(key=lambda x: self.extract_practice_number(x["practiceName"]))

            output[str(self.fiscal_year)].append(year_data_2023)

        # calculate total payment percentage nationwide
        total_payment = df1[
            (df1['FISCAL YEAR'] == str(self.fiscal_year)) &
            (df1['PRACTICE NAME'].str.lower() != "total")
            ]['DOLLARS OBLIGATED'].sum()

        for state_data in output[str(self.fiscal_year)]:
            # avoid division by zero
            if total_payment != 0:
                state_data["totalPaymentPercentageNationwide"] = \
                    round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                state_data["totalPaymentPercentageNationwide"] = 0

        # calculate total practice instance percentage nationwide
        total_instance_count = df1[
            (df1['FISCAL YEAR'] == str(self.fiscal_year)) &
            (df1['PRACTICE NAME'].str.lower() != "total")
            ]['PRACTICE INSTANCE COUNT'].sum()

        for state_data in output[str(self.fiscal_year)]:
            # avoid division by zero
            if total_instance_count != 0:
                state_data["totalPracticeInstancePercentageNationwide"] = \
                    round((state_data["totalPracticeInstanceCount"] / total_instance_count) * 100, 2)
            else:
                state_data["totalPracticeInstancePercentageNationwide"] = 0

        # create future year data
        for year in range(self.start_year, self.end_year + 1):
            for state in states:
                state_abbr = self.replace_state_name_with_abbreviation(state)
                # if you only want to contain the practices that are in the state in the year
                if self.practices_in_state:
                    practices = df1[df1['STATE'] == state]['PRACTICE NAME'].unique()
                else:
                    practices = self.unique_practices

                year_data = {
                    "state": state_abbr,
                    "predictedMinimumTotalPaymentInDollars": 0,
                    "predictedMaximumTotalPaymentInDollars": 0,
                    "predictedMinimumTotalPaymentPercentageNationwide": 0,
                    "predictedMaximumTotalPaymentPercentageNationwide": 0,
                    "practices": []
                }

                for practice in practices:
                    if self.practices_in_state:
                        practice_number = df1[(df1['STATE'] == state) &
                                              (df1['PRACTICE NAME'] == practice)]['practice_number'].values[0]
                    else:
                        practice_number = re.search(r'\((\d+)\)', practice).group(1)
                    practice_data = {
                        "practiceName": practice,
                        "predictedMinimumTotalPaymentInDollars": 0,
                        "predictedMaximumTotalPaymentInDollars": 0
                    }

                    if f"p_{practice_number}" in df2.columns:
                        min_values = df2[(df2['state'] == state) & (df2['year'] == year)][f"p_{practice_number}"]
                        if not min_values.empty:
                            practice_data["predictedMaximumTotalPaymentInDollars"] = float(min_values.values[0])
                            year_data["predictedMaximumTotalPaymentInDollars"] \
                                += practice_data["predictedMaximumTotalPaymentInDollars"]

                    year_data["practices"].append(practice_data)

                # round each state's total payment to 2 decimal places
                year_data["predictedMaximumTotalPaymentInDollars"] = \
                    round(year_data["predictedMaximumTotalPaymentInDollars"], 2)

                # sort year_data's practices by practice name's practice number
                year_data["practices"].sort(key=lambda x: self.extract_practice_number(x["practiceName"]))

                output[str(year)].append(year_data)

            # calculate total payment percentage nationwide for maximum payment for each year
            total_payment = sum([output[str(year)][i]["predictedMaximumTotalPaymentInDollars"]
                                 for i in range(len(output[str(year)]))])
            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedMaximumTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedMaximumTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedMaximumTotalPaymentPercentageNationwide"] = 0

            # calculate total payment percentage nationwide for minimum payment for each year
            total_payment = sum([output[str(year)][i]["predictedMinimumTotalPaymentInDollars"]
                                 for i in range(len(output[str(year)]))])
            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedMinimumTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedMinimumTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedMinimumTotalPaymentPercentageNationwide"] = 0

        # sort the fiscal year data by the total payment in dollars
        output[str(self.fiscal_year)].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)

        # sort the predicted year by maximum total payment in dollars
        for year in range(self.start_year, self.end_year + 1):
            output[str(year)].sort(key=lambda x: x['predictedMaximumTotalPaymentInDollars'], reverse=True)

        for year in output:
            for state in output[year]:
                for practice in state["practices"]:
                    for key, value in practice.items():
                        if isinstance(value, float) and not value.is_integer():
                            practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def parse_and_process(self):
        df1 = pd.read_csv(self.total_table_filepath)
        df2 = pd.read_excel(self.future_max_filepath)

        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)

        # remove state name "Total" rows
        df1 = df1[df1['STATE'] != 'Total']

        # remove rows with "Total" in the "Practice Name" column
        df1 = df1[~df1['PRACTICE NAME'].str.match("Total")]

        # create unique practices list
        self.unique_practices = df1['PRACTICE NAME'].unique()

        df1['practice_number'] = df1['PRACTICE NAME'].apply(self.extract_practice_number)
        df1['practice_number'] = df1['practice_number'].fillna(0).astype(int)

        df1['PRACTICE INSTANCE COUNT'] = pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.
                                                       replace(",", ""), errors='coerce').fillna(0).astype(int)

        df1['DOLLARS OBLIGATED'] = pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.
                                                 replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        house_outlay_max_data = self.create_house_outlay_max(df1, df2)
        with open(os.path.join(self.data_folder, "house_outlay_max.json"), "w") as json_file:
            json_file.write(house_outlay_max_data)



if __name__ == '__main__':
    fiscal_year = "2023"
    start_year = 2024
    end_year = 2031
    total_table_filepath = \
        "../title-2-conservation/house_outlay/Practice FIPS Download_modified.csv"
    future_max_filepath = \
        "../title-2-conservation/house_outlay/20231106-2024_2031-EQIPextrafund-project-by-practice-MAX-clean.xls"
    house_outlay_parser = HouseOutlayParser(
        fiscal_year, start_year, end_year, "../title-2-conservation/house_outlay/", total_table_filepath,
        future_max_filepath)
    house_outlay_parser.parse_and_process()
