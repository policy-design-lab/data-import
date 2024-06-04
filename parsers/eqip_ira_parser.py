import pandas as pd
import numpy as np
import json
import re

class EqipIraParser:
    def __init__(self, fiscal_year, start_year, end_year, total_table_filepath,
                 future_min_filepath, future_max_filepath):
        self.total_table_filepath = total_table_filepath
        self.future_min_filepath = future_min_filepath
        self.future_max_filepath = future_max_filepath
        self.fiscal_year = int(fiscal_year)
        self.start_year = start_year
        self.end_year = end_year

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
            'AE': 'Armed Forces Africa/Canada/Europe/Middle East', 'AP': 'Armed Forces Pacific'
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

    def create_state_distribution(self, df1, df2, df3):
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

        for year in range(self.start_year, self.end_year + 1):
            for state in states:
                state_abbr = self.replace_state_name_with_abbreviation(state)
                practices = df1[df1['STATE'] == state]['PRACTICE NAME'].unique()

                year_data = {
                    "state": state_abbr,
                    "predictedMinimumTotalPaymentInDollars": 0,
                    "predictedMaximumTotalPaymentInDollars": 0,
                    "predictedMinimumTotalPaymentPercentageNationwide": 0,
                    "predictedMaximumTotalPaymentPercentageNationwide": 0,
                    "practices": []
                }

                for practice in practices:
                    practice_number = df1[(df1['STATE'] == state) &
                                          (df1['PRACTICE NAME'] == practice)]['practice_number'].values[0]
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

    def create_summary(self, df1):
        df1['PRACTICE INSTANCE COUNT'] = \
            pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.
                          replace(",", ""), errors='coerce').fillna(0).astype(int)
        df1['DOLLARS OBLIGATED'] = \
            pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.
                          replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        practice_summary_data = []
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

            practice_summary_data.append({
                "practiceName": practice,
                "totalPracticeInstanceCount": int(total_instance_count),
                "totalPaymentInDollars": round(total_payment, 2),
                "totalPaymentInPercentageNationwide": round((total_payment / nationwide_total_payment) * 100, 2),
                "totalPracticeInstanceNationwide":
                    round((total_instance_count / nationwide_total_instance_count) * 100, 2)
            })

        for data in practice_summary_data:
            for key in data:
                if isinstance(data[key], (np.generic)):
                    data[key] = data[key].item()

        # sort the practices by the practice name
        practice_summary_data = sorted(practice_summary_data, key=lambda x: x['practiceName'])

        summary_data = {
            "totalPracticeInstanceCount": int(nationwide_total_instance_count),
            "totalPaymentInDollars": round(nationwide_total_payment, 2),
            "practices": practice_summary_data
        }

        return json.dumps(summary_data, indent=4)

    def parse_and_process(self):
        df1 = pd.read_csv(self.total_table_filepath)
        df2 = pd.read_excel(self.future_min_filepath)
        df3 = pd.read_excel(self.future_max_filepath)

        df2 = self.handle_min_max_p_table(df2)
        df3 = self.handle_min_max_p_table(df3)

        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)
            df3.fillna(0, inplace=True)

        # remove state name "Total" rows
        df1 = df1[df1['STATE'] != 'Total']

        # remove rows with "Total" in the "Practice Name" column
        df1 = df1[~df1['PRACTICE NAME'].str.match("Total")]

        df1['practice_number'] = df1['PRACTICE NAME'].apply(self.extract_practice_number)
        df1['practice_number'] = df1['practice_number'].fillna(0).astype(int)

        df1['PRACTICE INSTANCE COUNT'] = pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.
                                                       replace(",", ""), errors='coerce').fillna(0).astype(int)

        df1['DOLLARS OBLIGATED'] = pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.
                                                 replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        # create summary json
        summary_data = self.create_summary(df1)
        summary_json_file = "../title-2-conservation/eqip_ira/eqip_ira_summary.json"
        with open(summary_json_file, "w") as json_file:
            json_file.write(summary_data)

        # create state distribution json
        state_distribution_data = self.create_state_distribution(df1, df2, df3)
        out_json_file = "../title-2-conservation/eqip_ira/eqip_ira_state_distribution.json"
        with open(out_json_file, "w") as json_file:
            json_file.write(state_distribution_data)

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
    total_table_filepath = "../title-2-conservation/eqip_ira/20240215_EQIP_IRA.csv"
    future_min_filepath = "../title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MIN-clean.xls"
    future_max_filepath = "../title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MAX-clean.xls"
    eqip_data_parser = EqipIraParser(
        fiscal_year, start_year, end_year, total_table_filepath, future_min_filepath, future_max_filepath)
    eqip_data_parser.parse_and_process()
