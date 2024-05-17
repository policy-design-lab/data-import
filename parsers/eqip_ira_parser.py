import pandas as pd
import numpy as np  # Add this line to import numpy
import json
import re

class EqipIraParser:
    def __init__(self, fiscal_year, start_year, end_year, total_table_filepath, future_min_filepath, future_max_filepath):
        self.total_table_filepath = total_table_filepath
        self.future_min_filepath = future_min_filepath
        self.future_max_filepath = future_max_filepath
        self.fiscal_year = fiscal_year
        self.start_year = start_year
        self.end_year = end_year

        self.us_state_abbreviation = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AS': 'American Samoa',
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
            'VI': 'Virgin Islands of the U.S.',
            'AA': 'Armed Forces Americas (Except Canada)',
            'AE': 'Armed Forces Africa/Canada/Europe/Middle East',
            'AP': 'Armed Forces Pacific'
        }

        # Create a reverse dictionary for state name to abbreviation
        self.state_name_to_abbreviation = {v: k for k, v in self.us_state_abbreviation.items()}

    def handle_min_max_p_table(self, df):
        df_copy = df.copy()

        # Rename the columns ending with '_total' to remove '_total' from the copy table
        rename_mapping = {col: col[:-6] for col in df_copy.columns if col.endswith('_total')}
        df_copy = df_copy.rename(columns=rename_mapping)

        # Merge the two tables by concatenating the copy table to the original table
        merged_df = pd.concat([df, df_copy], ignore_index=True)

        # Group by 'state' and 'year', taking the first non-null value for each column within each group
        merged_df = merged_df.groupby(['state', 'year']).first().reset_index()

        # Select only the renamed columns and the columns without '_total' suffix
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_total')]]

        # Sort the merged dataframe by 'state' and 'year'
        merged_df = merged_df.sort_values(by=['state', 'year'])

        return merged_df

    # Extract practice number from practice name
    def extract_practice_number(self, practice_name):
        match = re.search(r'\((\d+)\)', practice_name)
        return int(match.group(1)) if match else None

    # Convert dollar values to integers
    def convert_dollars(self, in_val):
        if isinstance(in_val, str):
            return int(in_val.replace("$", "").replace(",", ""))
        return in_val

    # Replace state name with abbreviation
    def replace_state_name_with_abbreviation(self, state_name):
        return self.state_name_to_abbreviation.get(state_name, state_name)

    def create_state_distribution(self, df1, df2, df3):
        states = df1['STATE'].unique()
        state_payment_dict = {}

        # Collect totalPaymentInDollars for each state for the practice named "Total"
        for state in states:
            total_payment = df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'].str.lower() == "total")]['DOLLARS OBLIGATED']
            if not total_payment.empty:
                state_payment_dict[state] = self.convert_dollars(total_payment.values[0])
            else:
                state_payment_dict[state] = 0

        # Sort states by totalPaymentInDollars
        sorted_states = sorted(state_payment_dict.keys(), key=lambda x: state_payment_dict[x], reverse=True)

        output = []
        for state in sorted_states:
            state_abbr = self.replace_state_name_with_abbreviation(state)
            state_data = {"state": state_abbr, "practices": []}
            practices = df1[df1['STATE'] == state]['PRACTICE NAME'].unique()
            total_practice = None
            other_practices = []

            for practice in practices:
                practice_number = df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice)]['practice_number'].values[0]
                practice_data = {
                    "practiceName": practice,
                    "practiceInstanceCount": int(df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (df1['FISCAL YEAR'] == self.fiscal_year)]['PRACTICE INSTANCE COUNT'].values[0]),
                    "totalPaymentInDollars": float(
                        df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (
                            df1['FISCAL YEAR'].str.lower() == "total")]['DOLLARS OBLIGATED'].values[0]),
                    "2023PaymentInDollars": float(
                        df1[(df1['STATE'] == state) & (df1['PRACTICE NAME'] == practice) & (
                            df1['FISCAL YEAR'] == self.fiscal_year)]['DOLLARS OBLIGATED'].values[0])
                }

                for year in range(self.start_year, self.end_year + 1):
                    min_col_name = f"min_p_{practice_number}"
                    max_col_name = f"max_p_{practice_number}"
                    if min_col_name in df2.columns and state in df2['state'].values:
                        practice_data[f"{year}MinimumDollars"] = float(
                            df2[(df2['state'] == state) & (df2['year'] == year)][min_col_name].values[0])
                    else:
                        print("There is no column named: ", min_col_name,
                              " in the min table of " + state + " for year " + str(year))
                    if max_col_name in df3.columns and state in df3['state'].values:
                        practice_data[f"{year}MaximumDollars"] = float(
                            df3[(df3['state'] == state) & (df3['year'] == year)][max_col_name].values[0])
                    else:
                        print("There is no column named: ", max_col_name,
                              " in the max table of " + state + " for year " + str(year))

                if practice.lower() == "total":
                    total_practice = practice_data
                else:
                    other_practices.append(practice_data)

            # Add the "Total" practice first, if it exists
            if total_practice:
                state_data["practices"].append(total_practice)
            state_data["practices"].extend(other_practices)

            output.append(state_data)

        # Format numerical values to two decimal places if they are not integers
        for state in output:
            for practice in state["practices"]:
                for key, value in practice.items():
                    if isinstance(value, float) and not value.is_integer():
                        practice[key] = round(value, 2)

        return json.dumps(output, indent=4)

    def create_summary_output(self, df1):
        # Convert 'PRACTICE INSTANCE COUNT' and 'DOLLARS OBLIGATED' to numeric types
        df1['PRACTICE INSTANCE COUNT'] = pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.replace(",", ""), errors='coerce').fillna(0).astype(int)
        df1['DOLLARS OBLIGATED'] = pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        # Calculate the summary for each practice
        summary_data = []
        practices = df1['PRACTICE NAME'].unique()

        nationwide_total_instance_count = df1['PRACTICE INSTANCE COUNT'].sum()
        nationwide_total_payment = df1['DOLLARS OBLIGATED'].sum()

        for practice in practices:
            total_instance_count = df1[df1['PRACTICE NAME'] == practice]['PRACTICE INSTANCE COUNT'].sum()
            total_payment = df1[df1['PRACTICE NAME'] == practice]['DOLLARS OBLIGATED'].sum()

            summary_data.append({
                "practiceName": practice,
                "totalPracticeInstanceCount": int(total_instance_count),
                "totalPaymentInDollars": round(total_payment, 2),
                "totalPaymentInPercentageNationwide": round((total_payment / nationwide_total_payment) * 100, 2),
                "totalPracticeInstanceNationwide": round((total_instance_count / nationwide_total_instance_count) * 100, 2)
            })

        # Ensure all values are converted to native Python types
        for data in summary_data:
            for key in data:
                if isinstance(data[key], (np.generic)):
                    data[key] = data[key].item()

        return json.dumps({"practices": summary_data}, indent=4)

    def parse_and_process(self):
        # Process state distribution data
        df1 = pd.read_csv(self.total_table_filepath)
        df2 = pd.read_excel(self.future_min_filepath)
        df3 = pd.read_excel(self.future_max_filepath)

        df2 = self.handle_min_max_p_table(df2)
        df3 = self.handle_min_max_p_table(df3)

        # Fill NaN and Null values to zero if convert_nan_to_zero is True
        convert_nan_to_zero = True

        if convert_nan_to_zero:
            df1.fillna(0, inplace=True)
            df2.fillna(0, inplace=True)
            df3.fillna(0, inplace=True)

        # Extract the practice number from 'PRACTICE NAME'
        df1['practice_number'] = df1['PRACTICE NAME'].apply(self.extract_practice_number)
        df1['practice_number'] = df1['practice_number'].fillna(0).astype(int)

        # Convert 'PRACTICE INSTANCE COUNT' to numeric after removing commas
        df1['PRACTICE INSTANCE COUNT'] = pd.to_numeric(df1['PRACTICE INSTANCE COUNT'].astype(str).str.replace(",", ""), errors='coerce').fillna(0).astype(int)

        # Convert 'DOLLARS OBLIGATED' column to float after removing commas and dollar signs
        df1['DOLLARS OBLIGATED'] = pd.to_numeric(df1['DOLLARS OBLIGATED'].astype(str).str.replace(",", "").str.replace("$", "", regex=False), errors='coerce').fillna(0).astype(float)

        # Create state distribution JSON output
        json_output = self.create_state_distribution(df1, df2, df3)
        out_json_file = "../title-2-conservation/eqip_ira/eqip_ira_state_distribution.json"
        with open(out_json_file, "w") as json_file:
            json_file.write(json_output)

        # Create summary JSON output
        summary_output = self.create_summary_output(df1)
        summary_json_file = "../title-2-conservation/eqip_ira/eqip_ira_summary.json"
        with open(summary_json_file, "w") as json_file:
            json_file.write(summary_output)

    def remap_state_name_to_abbreviation(self, input_dict):
        # Remap state names to abbreviations
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
