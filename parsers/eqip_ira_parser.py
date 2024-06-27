import pandas as pd
import numpy as np
import json
import re

class EqipIraParser:
    def __init__(self, fiscal_year, start_year, end_year, total_table_filepath, future_filepath,
                 future_min_filepath, future_max_filepath, future_aggregated_filepath):
        self.total_table_filepath = total_table_filepath
        self.future_filepath = future_filepath
        self.future_min_filepath = future_min_filepath
        self.future_max_filepath = future_max_filepath
        self.future_aggregated_filepath = future_aggregated_filepath
        self.fiscal_year = int(fiscal_year)
        self.start_year = start_year
        self.end_year = end_year
        self.unique_practices = []
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
            'VI': 'Virgin Islands of the U.S.', 'AA': 'Armed Forces Americas (Except Canada)',
            'AE': 'Armed Forces Africa/Canada/Europe/Middle East', 'AP': 'Armed Forces Pacific',
            'VI': 'U.S. Virgin Islands'
        }

        self.state_name_to_abbreviation = {v: k for k, v in self.us_state_abbreviation.items()}

    def handle_min_max_p_table(self, df):
        df_copy = df.copy()
        rename_mapping = {col: col[:-6] for col in df_copy.columns if col.endswith('_total')}
        df_copy = df_copy.rename(columns=rename_mapping)
        merged_df = pd.concat([df, df_copy], ignore_index=True)
        merged_df = merged_df.groupby(['state', 'year']).first().reset_index()
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_total')]]
        merged_df = merged_df.sort_values(by=['state', 'year'])
        return merged_df

    def extract_practice_number(self, practice_name):
        match = re.search(r'\((\d+)\)', practice_name)
        return int(match.group(1)) if match else None

    def convert_dollars(self, in_val):
        if isinstance(in_val, str):
            return int(in_val.replace("$", "").replace(",", ""))
        return in_val

    def replace_state_name_with_abbreviation(self, state_name):
        return self.state_name_to_abbreviation.get(state_name, state_name)

    def create_state_distribution_min_max(self, df1, df2, df3):
        states = df1['STATE'].unique()
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
                    df1['FISCAL YEAR'].str.lower() == "total")]['DOLLARS OBLIGATED']
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

            output[str(self.fiscal_year)].append(year_data_2023)

        # calculate total payment percentage nationwide
        total_payment = sum([state_data["totalPaymentInDollars"] for state_data in output[str(self.fiscal_year)]])
        for state_data in output[str(self.fiscal_year)]:
            # avoid division by zero
            if total_payment != 0:
                state_data["totalPaymentPercentageNationwide"] = \
                    round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                state_data["totalPaymentPercentageNationwide"] = 0

        # calculate total practice instance percentage nationwide
        total_instance_count = sum([state_data["totalPracticeInstanceCount"]
                                    for state_data in output[str(self.fiscal_year)]])
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

                    if f"min_p_{practice_number}" in df2.columns:
                        min_values = df2[(df2['state'] == state) & (df2['year'] == year)][f"min_p_{practice_number}"]
                        if not min_values.empty:
                            practice_data["predictedMinimumTotalPaymentInDollars"] = float(min_values.values[0])
                            year_data["predictedMinimumTotalPaymentInDollars"] \
                                += practice_data["predictedMinimumTotalPaymentInDollars"]

                    if f"max_p_{practice_number}" in df3.columns:
                        max_values = df3[(df3['state'] == state) & (df3['year'] == year)][f"max_p_{practice_number}"]
                        if not max_values.empty:
                            practice_data["predictedMaximumTotalPaymentInDollars"] = float(max_values.values[0])
                            year_data["predictedMaximumTotalPaymentInDollars"] \
                                += practice_data["predictedMaximumTotalPaymentInDollars"]

                    year_data["practices"].append(practice_data)

                # round each state's total payment to 2 decimal places
                year_data["predictedMinimumTotalPaymentInDollars"] = \
                    round(year_data["predictedMinimumTotalPaymentInDollars"], 2)
                year_data["predictedMaximumTotalPaymentInDollars"] = \
                    round(year_data["predictedMaximumTotalPaymentInDollars"], 2)

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

    def create_state_distribution(self, df1, df2):
        states = df1['STATE'].unique()
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
                        df1['FISCAL YEAR'].str.lower() == "total")]['DOLLARS OBLIGATED']
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

            output[str(self.fiscal_year)].append(year_data_2023)

        # calculate total payment percentage nationwide
        total_payment = sum([state_data["totalPaymentInDollars"] for state_data in output[str(self.fiscal_year)]])
        for state_data in output[str(self.fiscal_year)]:
            # avoid division by zero
            if total_payment != 0:
                state_data["totalPaymentPercentageNationwide"] = \
                    round((state_data["totalPaymentInDollars"] / total_payment) * 100, 2)
            else:
                state_data["totalPaymentPercentageNationwide"] = 0

        # calculate total practice instance percentage nationwide
        total_instance_count = sum([state_data["totalPracticeInstanceCount"]
                                    for state_data in output[str(self.fiscal_year)]])
        for state_data in output[str(self.fiscal_year)]:
            # avoid division by zero
            if total_instance_count != 0:
                state_data["totalPracticeInstancePercentageNationwide"] = \
                    round((state_data["totalPracticeInstanceCount"] / total_instance_count) * 100, 2)
            else:
                state_data["totalPracticeInstancePercentageNationwide"] = 0

        ################################
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
                    "predictedTotalPaymentInDollars": 0,
                    "predictedTotalPaymentPercentageNationwide": 0,
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
                        "predictedTotalPaymentInDollars": 0
                    }

                    if f"p_{practice_number}" in df2.columns:
                        min_values = df2[(df2['state'] == state) & (df2['year'] == year)][f"p_{practice_number}"]
                        if not min_values.empty:
                            practice_data["predictedTotalPaymentInDollars"] = float(min_values.values[0])
                            year_data["predictedTotalPaymentInDollars"] \
                                += practice_data["predictedTotalPaymentInDollars"]

                    year_data["practices"].append(practice_data)

                # round each state's total payment to 2 decimal places
                year_data["predictedTotalPaymentInDollars"] = \
                    round(year_data["predictedTotalPaymentInDollars"], 2)

                output[str(year)].append(year_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[str(year)][i]["predictedTotalPaymentInDollars"]
                                 for i in range(len(output[str(year)]))])
            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedTotalPaymentPercentageNationwide"] = 0

        # sort the fiscal year data by the total payment in dollars
        output[str(self.fiscal_year)].sort(key=lambda x: x['totalPaymentInDollars'], reverse=True)

        # sort the predicted year by total payment in dollars
        for year in range(self.start_year, self.end_year + 1):
            output[str(year)].sort(key=lambda x: x['predictedTotalPaymentInDollars'], reverse=True)

        for year in output:
            for state in output[year]:
                for practice in state["practices"]:
                    for key, value in practice.items():
                        if isinstance(value, float) and not value.is_integer():
                            practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def create_aggregated_prediction(self, df1, df2):
        year = str(self.start_year) + "-" + str(self.end_year)
        states = df1['STATE'].unique()
        output = {year: []}

        # create aggregated prediction data
        for state in states:
            state_abbr = self.replace_state_name_with_abbreviation(state)

            practices = self.unique_practices

            year_data = {
                "state": state_abbr,
                "predictedTotalPaymentInDollars": 0,
                "predictedTotalPaymentPercentageNationwide": 0,
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
                    "predictedTotalPaymentInDollars": 0
                }

                if f"p_{practice_number}" in df2.columns:
                    min_values = df2[(df2['state'] == state)][f"p_{practice_number}"]
                    if not min_values.empty:
                        practice_data["predictedTotalPaymentInDollars"] = float(min_values.values[0])
                        year_data["predictedTotalPaymentInDollars"] \
                            += practice_data["predictedTotalPaymentInDollars"]

                year_data["practices"].append(practice_data)

            # round each state's total payment to 2 decimal places
            year_data["predictedTotalPaymentInDollars"] = \
                round(year_data["predictedTotalPaymentInDollars"], 2)

            output[str(year)].append(year_data)

            # calculate total payment percentage nationwide for payment for each year
            total_payment = sum([output[str(year)][i]["predictedTotalPaymentInDollars"]
                                 for i in range(len(output[str(year)]))])
            for state_data in output[str(year)]:
                # avoid division by zero
                if total_payment != 0:
                    state_data["predictedTotalPaymentPercentageNationwide"] = \
                        round((state_data["predictedTotalPaymentInDollars"] / total_payment) * 100, 2)
                else:
                    state_data["predictedTotalPaymentPercentageNationwide"] = 0

        # sort the predicted year by total payment in dollars
        output[year].sort(key=lambda x: x['predictedTotalPaymentInDollars'], reverse=True)

        for state in output[year]:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def create_summary(self, df1, df4):
        states = df1['STATE'].unique()
        df1['PRACTICE INSTANCE COUNT'] = \
            pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.
                          replace(",", ""), errors='coerce').fillna(0).astype(int)
        df1['DOLLARS OBLIGATED'] = \
            pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.
                          replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        output = {str(year): {} for year in range(self.fiscal_year, self.end_year + 1)}

        practice_data_2023 = []
        practices = df1['PRACTICE NAME'].unique()

        # calculate total instance count and total payment for the entire dataset where the fiscal year is "Total"
        nationwide_total_instance_count = \
            df1[df1['FISCAL YEAR'].str.lower() == "total"]['PRACTICE INSTANCE COUNT'].sum()
        nationwide_total_payment = df1[df1['FISCAL YEAR'].str.lower() == "total"]['DOLLARS OBLIGATED'].sum()

        for practice in practices:
            # calculate total instance count and total payment for each practice where the fiscal year is "Total"
            total_instance_count = df1[(df1['PRACTICE NAME'] == practice) &
                                       (df1['FISCAL YEAR'].str.lower() == "total")]['PRACTICE INSTANCE COUNT'].sum()
            total_payment = df1[(df1['PRACTICE NAME'] == practice) &
                                (df1['FISCAL YEAR'].str.lower() == "total")]['DOLLARS OBLIGATED'].sum()

            practice_data_2023.append({
                "practiceName": practice,
                "totalPracticeInstanceCount": int(total_instance_count),
                "totalPaymentInDollars": round(total_payment, 2),
                "totalPaymentInPercentageNationwide": round((total_payment / nationwide_total_payment) * 100, 2),
                "totalPracticeInstanceNationwide":
                    round((total_instance_count / nationwide_total_instance_count) * 100, 2)
            })

        for data in practice_data_2023:
            for key in data:
                if isinstance(data[key], (np.generic)):
                    data[key] = data[key].item()

        # sort the practices by the practice name's number
        practice_data_2023 = sorted(practice_data_2023, key=lambda x: int(re.search(r'\((\d+)\)', x['practiceName']).group(1)))

        summary_2023 = {
            "totalPracticeInstanceCount": int(nationwide_total_instance_count),
            "totalPaymentInDollars": round(nationwide_total_payment, 2),
            "practices": practice_data_2023
        }

        # add the 2023 data to the output
        output[str(self.fiscal_year)] = summary_2023

        ###############################################################
        # create future year data
        # make two separate ways to calculate the future year data
        # first one is aggreating the whole future year data
        # and the other is to create the future year data for each year

        # following parameter is used to set the whole future year or year by year
        year_by_year = False
        # if year_by_year is true, it will create the future year data for each year

        if year_by_year:
            for year in range(self.start_year, self.end_year + 1):
                summary_data = {}

                # select the rows for the iteration year
                tmp_df = df4[df4['year'] == year]

                # make the sum of all the rows by columns
                tmp_df = tmp_df.sum()

                # transpose the sum
                tmp_df = tmp_df.to_frame().T

                # select the columns that are started with 'p_'
                tmp_df = tmp_df.filter(regex='^p_')

                # remove the 'p_' prefix from the column names
                tmp_df.columns = [col[2:] for col in tmp_df.columns]

                # sort the columns by the column name
                tmp_df = tmp_df[sorted(tmp_df.columns)]

                # sum all the columns
                total_payment = tmp_df.sum(axis=1).values[0]

                # add total payment to summary_data
                summary_data["totalPaymentInDollars"] = round(total_payment, 2)

                practice_data = []
                # create a dictionary mapping the numbers to their corresponding values in list2
                mapping_dict = {}
                for item in self.unique_practices:
                    match = re.search(r'\((\d+)\)', item)
                    if match:
                        number = match.group(1)
                        mapping_dict[number] = item

                        # add the payment for the practice to the summary data only if the column exists
                        if number in tmp_df.columns:
                            total_payment = tmp_df[number].values[0]
                            practice_data.append({
                                "practiceName": item,
                                "totalPaymentInDollars": round(total_payment, 2)
                            })

                # sort the practices by the practice name's number
                practice_data = sorted(practice_data, key=lambda x: int(re.search(r'\((\d+)\)', x['practiceName']).group(1)))

                summary_data["practices"] = practice_data

                # add the summary data to the output
                output[str(year)] = summary_data
        else:  # this will create whole future year aggregated data
            year = str(self.start_year) + "-" + str(self.end_year)

            # remove entries from output from start_year to end_year
            for i in range(self.start_year, self.end_year + 1):
                if str(i) in output:
                    del output[str(i)]

            summary_data = {}

            # sum all the columns
            tmp_df = df4.sum()

            # transpose the sum
            tmp_df = tmp_df.to_frame().T

            # select the columns that are started with 'p_'
            tmp_df = tmp_df.filter(regex='^p_')

            # remove the 'p_' prefix from the column names
            tmp_df.columns = [col[2:] for col in tmp_df.columns]

            # sort the columns by the column name
            tmp_df = tmp_df[sorted(tmp_df.columns)]

            # sum all the columns
            total_payment = tmp_df.sum(axis=1).values[0]

            # add total payment to summary_data
            summary_data["totalPaymentInDollars"] = round(total_payment, 2)

            practice_data = []
            # create a dictionary mapping the numbers to their corresponding values in list2
            mapping_dict = {}
            for item in self.unique_practices:
                match = re.search(r'\((\d+)\)', item)
                if match:
                    number = match.group(1)
                    mapping_dict[number] = item

                    # add the payment for the practice to the summary data only if the column exists
                    if number in tmp_df.columns:
                        total_payment = tmp_df[number].values[0]
                        practice_data.append({
                            "practiceName": item,
                            "totalPaymentInDollars": round(total_payment, 2)
                        })

            # sort the practices by the practice name's number
            practice_data = sorted(practice_data, key=lambda x: int(re.search(r'\((\d+)\)', x['practiceName']).group(1)))

            summary_data["practices"] = practice_data

            # add the summary data to the output
            output[year] = summary_data

        return json.dumps(output, indent=4)

    def create_unique_practices(self, df1, df2):
        # initialize dictionary to store practice names for fiscal year
        practice_names_for_each_year = {}

        # get unique practice names for fiscal year
        for index, row in df1.iterrows():
            year = str(row['FISCAL YEAR'])
            practice_name = row['PRACTICE NAME']

            # Skip rows where practice name is empty or fiscal year is not present
            if pd.isnull(year) or pd.isnull(practice_name):
                continue

            # If year not in dictionary, create a new entry
            if year not in practice_names_for_each_year:
                practice_names_for_each_year[year] = []

            # Add practice name to the list for the corresponding yea
            if practice_name not in practice_names_for_each_year[year]:
                practice_names_for_each_year[year].append(practice_name)

        # remove the 'Total' practice name from the list
        if 'Total' in practice_names_for_each_year:
            del practice_names_for_each_year['Total']

        # get the unique practice names for the future year

        # get all the column names that start with 'p_'
        practice_columns = [col for col in df2.columns if col.startswith('p_')]

        # remove the 'p_' prefix from the column names
        practice_columns = [col[2:] for col in practice_columns]

        # create a dictionary mapping the numbers to their corresponding values in list2
        mapping_dict = {}
        for item in self.unique_practices:
            match = re.search(r'\((\d+)\)', item)
            if match:
                number = match.group(1)
                mapping_dict[number] = item

        # remap practice columns for future year
        future_practices = [mapping_dict[num] for num in practice_columns if num in mapping_dict]

        # add future practices to practice_names_for_future_year
        for i in range(self.end_year - self.start_year + 1):
            year = str(self.start_year + i)
            practice_names_for_each_year[year] = future_practices

        # convert practice names to json
        practice_names_for_each_year = json.dumps(practice_names_for_each_year, indent=4)

        return practice_names_for_each_year

    def parse_and_process(self):
        df1 = pd.read_csv(self.total_table_filepath)
        df2 = pd.read_excel(self.future_min_filepath)
        df3 = pd.read_excel(self.future_max_filepath)
        df4 = pd.read_excel(self.future_filepath)
        df5 = pd.read_excel(self.future_aggregated_filepath)

        df2 = self.handle_min_max_p_table(df2)
        df3 = self.handle_min_max_p_table(df3)

        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)
            df3.fillna(0, inplace=True)
            df4.fillna(0, inplace=True)

        # remove state name "Total" rows
        df1 = df1[df1['STATE'] != 'Total']

        # remove rows with "Total" in the "Practice Name" column
        df1 = df1[~df1['PRACTICE NAME'].str.match("Total")]

        # create unique practices list
        self.unique_practices = df1['PRACTICE NAME'].unique()

        # create a unique list of practices for each year json
        practices_list_for_each_year = self.create_unique_practices(df1, df4)

        # save practice names for each year to a json file
        with open("../title-2-conservation/eqip_ira/eqip_ira_practices.json", "w") as json_file:
            json_file.write(practices_list_for_each_year)

        df1['practice_number'] = df1['PRACTICE NAME'].apply(self.extract_practice_number)
        df1['practice_number'] = df1['practice_number'].fillna(0).astype(int)

        df1['PRACTICE INSTANCE COUNT'] = pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.
                                                       replace(",", ""), errors='coerce').fillna(0).astype(int)

        df1['DOLLARS OBLIGATED'] = pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.
                                                 replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        # create summary json
        summary_data = self.create_summary(df1, df4)
        summary_json_file = "../title-2-conservation/eqip_ira/eqip_ira_summary.json"
        with open(summary_json_file, "w") as json_file:
            json_file.write(summary_data)

        # create state distribution json
        state_distribution_data = self.create_state_distribution(df1, df4)
        out_json_file = "../title-2-conservation/eqip_ira/eqip_ira_state_distribution.json"
        with open(out_json_file, "w") as json_file:
            json_file.write(state_distribution_data)

        # the following code is for creating the state distribution json for min and max values
        # create state distribution json for min and max values
        state_distribution_data = self.create_state_distribution_min_max(df1, df2, df3)
        out_json_file = "../title-2-conservation/eqip_ira/eqip_ira_state_distribution_min_max.json"
        with open(out_json_file, "w") as json_file:
            json_file.write(state_distribution_data)

        # create aggregated prediction json
        aggregated_prediction_data = self.create_aggregated_prediction(df1, df5)
        out_json_file = "../title-2-conservation/eqip_ira/eqip_ira_aggregated_prediction.json"
        with open(out_json_file, "w") as json_file:
            json_file.write(aggregated_prediction_data)

    def remap_state_name_to_abbreviation(self, input_dict):
        state_names = list(input_dict.keys())

        for state_name in state_names:
            if state_name in self.us_state_abbreviation.values():
                state_abbr = [abbr for abbr, name in self.us_state_abbreviation.items() if name == state_name][0]
                input_dict[state_abbr] = input_dict.pop(state_name)

        return input_dict


if __name__ == '__main__':
    fiscal_year = "2023"
    start_year = 2024
    end_year = 2031
    total_table_filepath = \
        "../title-2-conservation/eqip_ira/20240215_EQIP_IRA.csv"
    future_min_filepath = \
        "../title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MIN-clean.xls"
    future_max_filepath = \
        "../title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MAX-clean.xls"
    future_filepath = \
        "../title-2-conservation/eqip_ira/2024_2031_EQIP_IRA_base2023_value.xls"
    future_aggregated_filepath = \
        "../title-2-conservation/eqip_ira/BudgetAuthority_EQIP_IRA_projection_base_on_2023_ratio.xls"
    eqip_data_parser = EqipIraParser(
        fiscal_year, start_year, end_year, total_table_filepath, future_filepath, future_min_filepath,
        future_max_filepath, future_aggregated_filepath)
    eqip_data_parser.parse_and_process()
