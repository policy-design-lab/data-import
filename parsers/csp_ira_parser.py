import json
import os
import re
import sys

import numpy as np
import pandas as pd


class CspIraParser:
    def __init__(self, fiscal_year, start_year, end_year, data_folder, original_raw_data, actual_pay_data,
                 instance_count_data, practice_code_data, future_authority_data):
        self.original_raw_data = original_raw_data
        self.actual_pay_data = actual_pay_data
        self.instance_count_data = instance_count_data
        self.practice_code_data = practice_code_data
        self.future_authority_data = future_authority_data
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

    def create_unique_practices(self, df1, df2, df3):
        year = str(self.fiscal_year)
        future_year = str(self.start_year) + "-" + str(self.end_year)
        output = {year: [], future_year: []}

        states = df1['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the payment data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df1 = df1[df1['state'].isin(states)]

        # check the unique states in df1
        if len(df1['state'].unique()) != 50:
            print("The state for the payment data table is not 50")
            print("Exit state distribution function")
            sys.exit()

        # create unique practices data for the fiscal year
        year_data = []
        for column in df1.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df2['practice_code'].values:
                    practice_name = df2.loc[df2['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"
                    year_data.append(practice_name)

        # sort year_data by practice name's number
        year_data.sort(key=lambda x: self.extract_practice_number(x))

        output[year] = year_data

        # create unique practices data for the future year
        states = df3['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the future data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df3 = df3[df3['state'].isin(states)]

        # check the unique states in df1
        if len(df3['state'].unique()) != 50:
            print("The state for the future data table is not 50")
            print("Exit state distribution function")
            sys.exit()

        future_data = []
        for column in df3.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df2['practice_code'].values:
                    practice_name = df2.loc[df2['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"
                    future_data.append(practice_name)

        # sort year_data by practice name's number
        future_data.sort(key=lambda x: self.extract_practice_number(x))

        output[future_year] = future_data

        return json.dumps(output, indent=4)

    def create_state_distribution(self, df1, df2, df3, df4):
        year = str(self.fiscal_year)
        future_year = str(self.start_year) + "-" + str(self.end_year)
        states = df1['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the payment data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df1 = df1[df1['state'].isin(states)]

        # check the unique states in both df1
        if len(df1['state'].unique()) != 50:
            print("The state for the payment table is not 50")
            print("Exit state distribution function")
            sys.exit()

        states = df2['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the instance count data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df2 = df2[df2['state'].isin(states)]

        # check the unique states in both df1
        if len(df2['state'].unique()) != 50:
            print("The state for the instance count table is not 50")
            print("Exit state distribution function")
            sys.exit()

        output = {year: [], future_year: []}

        # create state distribution data
        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)

            year_data = {
                "state": state_abbr,
                "totalPaymentInDollars": 0,
                "totalPracticeInstanceCount": 0,
                "totalPaymentPercentageNationwide": 0,
                "totalPracticeInstancePercentageNationwide": 0,
                "practices": []
            }

            # create practice number list from df3
            practice_codes = df3['practice_code'].tolist()

            for column in df1.columns:
                if column.startswith("p_"):
                    # Extract practice_code from the column name
                    practice_code = column[2:]

                    if practice_code in df3['practice_code'].values:
                        practice_name = df3.loc[df3['practice_code'] == practice_code, 'practice_name'].values[0]
                        # Add number to the practice name
                        practice_name = f"{practice_name} ({practice_code})"

                        practice_data = {
                            "practiceName": practice_name,
                            "totalPaymentInDollars": 0,
                            "practiceInstanceCount": 0,
                        }
                        dollar_values = df1[df1['state'] == state][column]
                        count_values = df2[df2['state'] == state][column]

                        if not dollar_values.empty:
                            practice_data["totalPaymentInDollars"] = float(
                                dollar_values.sum())
                            year_data["totalPaymentInDollars"] += practice_data["totalPaymentInDollars"]

                        if not count_values.empty:
                            practice_data["practiceInstanceCount"] = int(
                                count_values.sum())
                            year_data["totalPracticeInstanceCount"] += practice_data["practiceInstanceCount"]

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

            # calculate total practice instance percentage nationwide for practice instance count for each year
            total_practice_instance = sum([output[str(year)][i]["totalPracticeInstanceCount"]
                                           for i in range(len(output[str(year)]))])

            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["totalPaymentPercentageNationwide"] = \
                        round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
                    state_data["totalPracticeInstancePercentageNationwide"] = \
                        round((state_data["totalPracticeInstanceCount"] / total_practice_instance) * 100, 2)
                else:
                    state_data["totalPaymentPercentageNationwide"] = 0
                    state_data["totalPracticeInstancePercentageNationwide"] = 0

        # sort the predicted year by total payment in dollars
        output[year].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)

        for state in output[year]:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        ########################
        # generate future data #
        ########################
        states = df4['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the future data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df4 = df4[df4['state'].isin(states)]

        # check the unique states in df4
        if len(df4['state'].unique()) != 50:
            print("The state for the future data table is not 50")
            print("Exit state distribution function")
            sys.exit()

        # create state distribution data
        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)

            future_data = {
                "state": state_abbr,
                "totalPaymentInDollars": 0,
                "totalPaymentPercentageNationwide": 0,
                "practices": []
            }

            # create practice number list from df3
            practice_codes = df3['practice_code'].tolist()

            for column in df4.columns:
                if column.startswith("p_"):
                    # Extract practice_code from the column name
                    practice_code = column[2:]

                    if practice_code in df3['practice_code'].values:
                        practice_name = df3.loc[df3['practice_code'] == practice_code, 'practice_name'].values[0]
                        # Add number to the practice name
                        practice_name = f"{practice_name} ({practice_code})"

                        practice_data = {
                            "practiceName": practice_name,
                            "totalPaymentInDollars": 0
                        }
                        dollar_values = df4[df4['state'] == state][column]

                        if not dollar_values.empty:
                            practice_data["totalPaymentInDollars"] = float(
                                dollar_values.sum())
                            future_data["totalPaymentInDollars"] += practice_data["totalPaymentInDollars"]

                        future_data["practices"].append(practice_data)

            # round each state's total payment to 2 decimal places
            future_data["totalPaymentInDollars"] = \
                round(future_data["totalPaymentInDollars"], 2)

            # sort year_data by practice name's number
            future_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

            output[future_year].append(future_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[future_year][i]["totalPaymentInDollars"]
                                 for i in range(len(output[future_year]))])

            for state_data in output[future_year]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["totalPaymentPercentageNationwide"] = \
                        round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["totalPaymentPercentageNationwide"] = 0

        # sort the predicted year by total payment in dollars
        output[future_year].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)

        for state in output[future_year]:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def create_summary(self, df1, df2, df3, df4):
        states = df1['state'].unique()

        if len(states) > 50:
            print("There are more than 50 states in the payment data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]
        df1 = df1[df1['state'].isin(states)]

        if len(df1['state'].unique()) != 50:
            print("The state for the payment table is not 50")
            print("Exit state distribution function")
            sys.exit()

        states = df2['state'].unique()

        if len(states) > 50:
            print("There are more than 50 states in the instance count data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]
        df2 = df2[df2['state'].isin(states)]

        if len(df2['state'].unique()) != 50:
            print("The state for the instance count table is not 50")
            print("Exit state distribution function")
            sys.exit()

        year = self.fiscal_year
        future_year = str(self.start_year) + "-" + str(self.end_year)
        df1 = df1[df1['year'] == year]
        df2 = df2[df2['year'] == year]

        # create output template
        output = {year: [], future_year: []}

        # create summary data
        # sum all the values by columns and transpose the sum
        df1 = df1.sum()
        df1 = df1.to_frame().T

        df2 = df2.sum()
        df2 = df2.to_frame().T

        year_data = {
            "totalPaymentInDollars": 0,
            "totalPracticeInstanceCount": 0,
            "practices": []
        }

        for column in df1.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df3['practice_code'].values:
                    practice_name = df3.loc[df3['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"

                    practice_data = {
                        "practiceName": practice_name,
                        "totalPaymentInDollars": 0,
                        "totalPracticeInstanceCount": 0,
                        "totalPaymentInPercentageNationwide": 0,
                        "totalPracticeInstanceNationwide": 0
                    }
                    dollar_values = df1[column]
                    count_values = df2[column]
                    if not dollar_values.empty:
                        practice_data["totalPaymentInDollars"] = float(dollar_values.sum())
                        year_data["totalPaymentInDollars"] += practice_data["totalPaymentInDollars"]

                    if not count_values.empty:
                        practice_data["totalPracticeInstanceCount"] = int(count_values.sum())
                        year_data["totalPracticeInstanceCount"] += practice_data["totalPracticeInstanceCount"]

                    year_data["practices"].append(practice_data)

        # round each state's total payment to 2 decimal places
        year_data["totalPaymentInDollars"] = \
            round(year_data["totalPaymentInDollars"], 2)

        year_data["totalPracticeInstanceCount"] = \
            int(year_data["totalPracticeInstanceCount"])

        # sort year_data by practice name's number
        year_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

        output[year].append(year_data)

        # calculate total payment percentage nationwide for payment for each year
        total_payment = sum([output[year][i]["totalPaymentInDollars"]
                             for i in range(len(output[year]))])

        total_instance = sum([output[year][i]["totalPracticeInstanceCount"]
                             for i in range(len(output[year]))])

        # add total payment in percentage nationwide values
        for practice_data in output[year][0]["practices"]:
            # avoid division by zero
            if total_payment != 0:
                practice_data["totalPaymentInPercentageNationwide"] = \
                    round((practice_data["totalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                practice_data["totalPaymentInPercentageNationwide"] = 0

            if total_instance != 0:
                practice_data["totalPracticeInstanceNationwide"] = \
                    round((practice_data["totalPracticeInstanceCount"] / total_instance) * 100, 2)
            else:
                practice_data["totalPracticeInstanceNationwide"] = 0

        output[year] = output[year][0]

        ###########################
        # create future year data #
        ###########################
        future_data = {
            "predictedTotalPaymentInDollars": 0,
            "practices": []
        }

        for column in df4.columns:
            if column.startswith("p_"):
                # Extract practice_code from the column name
                practice_code = column[2:]

                if practice_code in df3['practice_code'].values:
                    practice_name = df3.loc[df3['practice_code'] == practice_code, 'practice_name'].values[0]
                    # Add number to the practice name
                    practice_name = f"{practice_name} ({practice_code})"

                    practice_data = {
                        "practiceName": practice_name,
                        "predictedTotalPaymentInDollars": 0,
                        "predictedTotalPaymentInPercentageNationwide": 0,
                    }
                    dollar_values = df4[column]

                    if not dollar_values.empty:
                        practice_data["predictedTotalPaymentInDollars"] = float(dollar_values.sum())
                        future_data["predictedTotalPaymentInDollars"] += practice_data["predictedTotalPaymentInDollars"]
                        # round practice data to 2 decimal places
                        practice_data["predictedTotalPaymentInDollars"] = round(practice_data["predictedTotalPaymentInDollars"], 2)

                    future_data["practices"].append(practice_data)

        # round each state's total payment to 2 decimal places
        future_data["predictedTotalPaymentInDollars"] = \
            round(future_data["predictedTotalPaymentInDollars"], 2)

        # sort year_data by practice name's number
        future_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

        output[future_year].append(future_data)

        # calculate total payment percentage nationwide for payment for each year
        total_payment = sum([output[future_year][i]["predictedTotalPaymentInDollars"]
                             for i in range(len(output[future_year]))])

        # add total payment in percentage nationwide values
        for practice_data in output[future_year][0]["practices"]:
            # avoid division by zero
            if total_payment != 0:
                practice_data["predictedTotalPaymentInPercentageNationwide"] = \
                    round((practice_data["predictedTotalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                practice_data["predictedTotalPaymentInPercentageNationwide"] = 0

        output[future_year] = output[future_year][0]

        return json.dumps(output, indent=4)

    def create_aggregated_prediction(self, df1, df2):
        future_year = str(self.start_year) + "-" + str(self.end_year)
        output = {future_year: []}

        states = df1['state'].unique()

        # check if the state is 50 us states
        if len(states) > 50:
            print("There are more than 50 states in the future data, Only 50 states will be processed")
            states = [state for state in states if state in self.us_50_states]

        # remove the state that is not in the 50 states
        df1 = df1[df1['state'].isin(states)]

        # check the unique states in df1
        if len(df1['state'].unique()) != 50:
            print("The state for the future data table is not 50")
            print("Exit state distribution function")
            sys.exit()

        # create state distribution data
        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)

            future_data = {
                "state": state_abbr,
                "predictedTotalPaymentInDollars": 0,
                "predictedTotalPaymentPercentageNationwide": 0,
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
                            "predictedTotalPaymentInDollars": 0
                        }
                        dollar_values = df1[df1['state'] == state][column]

                        if not dollar_values.empty:
                            practice_data["predictedTotalPaymentInDollars"] = float(
                                dollar_values.sum())
                            future_data["predictedTotalPaymentInDollars"] += \
                                practice_data["predictedTotalPaymentInDollars"]

                        future_data["practices"].append(practice_data)

            # round each state's total payment to 2 decimal places
            future_data["predictedTotalPaymentInDollars"] = \
                round(future_data["predictedTotalPaymentInDollars"], 2)

            # sort year_data by practice name's number
            future_data["practices"].sort(key=lambda x: self.extract_practice_number_clean(x["practiceName"]))

            output[future_year].append(future_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[future_year][i]["predictedTotalPaymentInDollars"]
                                 for i in range(len(output[future_year]))])

            for state_data in output[future_year]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedTotalPaymentPercentageNationwide"] = 0

        # sort the predicted year by total payment in dollars
        output[future_year].sort(key=lambda x: x['predictedTotalPaymentInDollars'], reverse=True)

        for state in output[future_year]:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def parse_and_process(self):
        df1 = pd.read_excel(self.original_raw_data)
        df2 = pd.read_excel(self.actual_pay_data)
        df3 = pd.read_excel(self.instance_count_data)
        df4 = pd.read_csv(self.practice_code_data)
        df5 = pd.read_excel(self.future_authority_data)

        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)
            df3.fillna(0, inplace=True)
            df5.fillna(0, inplace=True)

        # the following process only needed when the data is the raw one
        # remove rows with "Total" in the "Practice Name" column
        df1 = df1[~df1['PRACTICE NAME'].str.match("Total")]

        # select only 2023 from FISCAL YEAR column
        df1 = df1[df1['FISCAL YEAR'] == self.fiscal_year]

        # dump unique practices to a json file
        unique_practices_data = self.create_unique_practices(df2, df4, df5)
        with open(os.path.join(self.data_folder, "csp_ira_practices.json"), "w") as json_file:
            json_file.write(unique_practices_data)

        # create state distribution json
        state_distribution_data = self.create_state_distribution(df2, df3, df4, df5)
        with open(os.path.join(self.data_folder, "csp_ira_state_distribution.json"), "w") as json_file:
            json_file.write(state_distribution_data)

        # create summary json
        summary_data = self.create_summary(df2, df3, df4, df5)
        with open(os.path.join(self.data_folder, "csp_ira_summary.json"), "w") as json_file:
            json_file.write(summary_data)

        # create aggregated prediction json
        aggregated_prediction_data = self.create_aggregated_prediction(df5, df4)
        with open(os.path.join(self.data_folder, "csp_ira_aggregated_prediction.json"), "w") as json_file:
            json_file.write(aggregated_prediction_data)


if __name__ == '__main__':
    fiscal_year = "2023"
    start_year = 2024
    end_year = 2031
    original_raw_data = \
        "../title-2-conservation/csp_ira/" \
        "Practice FIPS Download - All programs and practice counts and obligation (May 6 2024).xlsx"
    actual_pay_data = \
        "../title-2-conservation/csp_ira/20240724_CSP-IRA_combined_practice_actualpay.xlsx"
    instance_count_data = \
        "../title-2-conservation/csp_ira/20240804_CSP-IRA_combined_practice_actual_instance_count.xlsx"
    practice_code_data = "../title-2-conservation/common/merged_practice_standards.csv"
    future_authority_data = "../title-2-conservation/csp_ira/20240805_BA_CSP_IRA_base2023_value.xlsx"
    csp_data_parser = CspIraParser(
        fiscal_year, start_year, end_year, "../title-2-conservation/csp_ira/",
        original_raw_data, actual_pay_data, instance_count_data, practice_code_data, future_authority_data)
    csp_data_parser.parse_and_process()
