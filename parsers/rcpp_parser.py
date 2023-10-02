import json
import os

import pandas as pd
from deepmerge import always_merger


class RcppParser:
    def __init__(self, start_year, end_year, program_main_category_name, data_folder, program_csv_filename, **kwargs):
        self.start_year = start_year
        self.end_year = end_year
        self.program_main_category_name = program_main_category_name
        self.data_folder = data_folder
        self.program_csv_filepath = os.path.join(data_folder, program_csv_filename)
        self.program_main_category_name = program_main_category_name
        self.program_data = None

        # Output data dictionaries
        self.processed_data_dict = dict()
        self.state_distribution_data_dict = dict()
        self.program_data_dict = dict()

        self.metadata = {
            "column_names_map": {
                "Number of Contracts": "contracts",
                "Number of Acres": "acres",
                "Total Financial Assistance Payments ($1000)": "assistance payments",
                "Total Reimbursable Payments": "reimburse payments",
                "Total Techinical Assistance Payments": "tech payments",
                "Total Payments": "total payments"
            }
        }

        self.us_state_abbreviations = {
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
            'HI': 'Hawaii/Pacific',
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
            'WY': 'Wyoming'
        }


    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        program_data = pd.read_csv(self.program_csv_filepath)

        # Rename column names to make it more uniform
        program_data.rename(columns=self.metadata["column_names_map"], inplace=True)

        # some columns have empty values and this makes the rows type as object
        # this makes the process of SUM errors since those are object not number
        # so the columns should be numeric all the time
        # make sure every number columns be number
        program_data[["contracts",
                      "acres",
                      "assistance payments",
                      "reimburse payments",
                      "tech payments",
                      "total payments"]] = \
            program_data[["contracts",
                          "acres",
                          "assistance payments",
                          "reimburse payments",
                          "tech payments",
                          "total payments"]].apply(pd.to_numeric)

        # Filter only relevant years' data
        program_data = program_data[program_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # 1. Generate State Distribution JSON Data
        self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

        # calculate national level values
        total_contract_at_national_level = int(
            program_data["contracts"].sum())
        total_acre_at_national_level = int(
            program_data["acres"].sum())
        total_assistance_payments_at_national_level = round(
            program_data["assistance payments"].sum(), 1)
        total_reimburse_payments_at_national_level = round(
            program_data["reimburse payments"].sum(), 1)
        total_tech_payments_at_national_level = round(
            program_data["tech payments"].sum(), 1)
        total_payments_at_national_level = round(
            program_data["total payments"].sum(), 1)

        # group total data by state, then sum
        sum_by_contract_by_state = \
            program_data[
                ["year", "state", "contracts"]
            ].groupby(
                ["state"]
            )["contracts"].sum()

        sum_by_acre_by_state = \
            program_data[
                ["year", "state", "acres"]
            ].groupby(
                ["state"]
            )["acres"].sum()

        sum_by_assistance_payments_by_state = \
            program_data[
                ["year", "state", "assistance payments"]
            ].groupby(
                ["state"]
            )["assistance payments"].sum()

        sum_by_reimburse_payments_by_state = \
            program_data[
                ["year", "state", "reimburse payments"]
            ].groupby(
                ["state"]
            )["reimburse payments"].sum()

        sum_by_tech_payments_by_state = \
            program_data[
                ["year", "state", "tech payments"]
            ].groupby(
                ["state"]
            )["tech payments"].sum()

        sum_by_total_payments_by_state = \
            program_data[
                ["year", "state", "total payments"]
            ].groupby(
                ["state"]
            )["total payments"].sum()

        for state_abbr in self.us_state_abbreviations:
            # there was an error in the line
            # because the original csv file contains the space in alaska, and hawaii/pacific
            # so if it makes an error, needs to check the state name if it has any extra space
            state = self.us_state_abbreviations[state_abbr]
            # if there is a zero division problem in with state percentage
            within_state_assistance_payments = 0
            within_state_reimburse_payments = 0
            within_state_tech_payments = 0

            if int(sum_by_total_payments_by_state[state].item()) != 0:
                within_state_assistance_payments = \
                    round((sum_by_assistance_payments_by_state[state].item() /
                           sum_by_total_payments_by_state[state].item()) * 100, 2)
                within_state_reimburse_payments = \
                    round((sum_by_reimburse_payments_by_state[state].item() /
                           sum_by_total_payments_by_state[state].item()) * 100, 2)
                within_state_tech_payments = \
                    round((sum_by_tech_payments_by_state[state].item() /
                           sum_by_total_payments_by_state[state].item()) * 100, 2)

                contract_percentage_nation = 0.00
                acres_percentage_nation = 0.00
                assistant_percentage_nation = 0.00
                reimburse_percentage_nation = 0.00
                tech_percentage_nation = 0.00
                total_payment_percentage_nation = 0.00

                if total_contract_at_national_level > 0:
                    contract_percentage_nation = \
                        round((sum_by_contract_by_state[state].item() /
                               total_contract_at_national_level) * 100, 2)
                if total_assistance_payments_at_national_level > 0:
                    acres_percentage_nation = \
                        round((sum_by_acre_by_state[state].item() /
                               total_acre_at_national_level) * 100, 2)
                if total_assistance_payments_at_national_level > 0:
                    assistant_percentage_nation = \
                        round((sum_by_assistance_payments_by_state[state].item() /
                               total_assistance_payments_at_national_level) * 100, 2)
                if total_reimburse_payments_at_national_level > 0:
                    reimburse_percentage_nation = \
                        round((sum_by_reimburse_payments_by_state[state].item() /
                               total_reimburse_payments_at_national_level) * 100, 2)
                if total_tech_payments_at_national_level:
                    tech_percentage_nation = \
                        round((sum_by_tech_payments_by_state[state].item() /
                               total_tech_payments_at_national_level) * 100, 2)
                if total_payment_percentage_nation > 0:
                    total_payment_percentage_nation = \
                        round((sum_by_total_payments_by_state[state].item() /
                               total_payments_at_national_level) * 100, 2)

            new_data_entry = {
                "state": state,
                "programs": [
                    {
                        "programName": "RCPP",
                        "totalContracts": int(sum_by_contract_by_state[state].item()),
                        "totalAcres": int(sum_by_acre_by_state[state].item()),
                        "assistancePaymentInDollars": int(sum_by_assistance_payments_by_state[state].item() * 1000),
                        "reimbursePaymentInDollars": int(sum_by_reimburse_payments_by_state[state].item() * 1000),
                        "techPaymentInDollars": int(sum_by_tech_payments_by_state[state].item() * 1000),
                        "paymentInDollars": int(sum_by_total_payments_by_state[state].item() * 1000),
                        "contractsInPercentageNationwide": contract_percentage_nation,
                        "acresInPercentageNationwide": acres_percentage_nation,
                        "assistancePaymentInPercentageNationwide": assistant_percentage_nation,
                        "reimbursePaymentInPercentageNationwide": reimburse_percentage_nation,
                        "techPaymentInPercentageNationwide": tech_percentage_nation,
                        "totalPaymentInPercentageNationwide": total_payment_percentage_nation,
                        "assistancePaymentInPercentageWithinState": within_state_assistance_payments,
                        "reimbursePaymentInPercentageWithinState": within_state_reimburse_payments,
                        "techPaymentInPercentageWithinState": within_state_tech_payments,
                        "subPrograms": []
                    },
                ]
            }

            self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)].append(
                new_data_entry)

        # Sort states by decreasing order of total indemnities
        for year in self.state_distribution_data_dict:
            self.state_distribution_data_dict[year] = sorted(self.state_distribution_data_dict[year],
                                                             key=lambda x: x["programs"][0][
                                                                 "paymentInDollars"],
                                                             reverse=True)

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "rcpp_state_distribution_data.json"),
                  "w") as output_json_file:
            output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=2))

        # 2. Generate Sub Programs Data
        # Group total
        total_by_contract = \
            program_data["contracts"].sum()

        total_by_acre = \
            program_data["acres"].sum()

        total_by_assistance_payments = \
            program_data["assistance payments"].sum()

        total_by_reimburse_payments = \
            program_data["reimburse payments"].sum()

        total_by_tech_payments = \
            program_data["tech payments"].sum()

        total_by_total_payments = \
            program_data["total payments"].sum()

        self.program_data_dict = {
            "programs": [
                {
                    "programName": "RCPP",
                    "totalContracts": int(total_by_contract.item()),
                    "totalAcre": int(total_by_acre.item()),
                    "assistancePaymentInDollars": int(total_by_assistance_payments.item() * 1000),
                    "reimbursePaymentInDollars": int(total_by_reimburse_payments.item() * 1000),
                    "techPaymentInDollars": int(total_by_tech_payments.item() * 1000),
                    "paymentInDollars": int(total_by_total_payments * 1000),
                    "subPrograms": []
                },
            ]
        }

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "rcpp_subprograms_data.json"), "w") as output_json_file:
            output_json_file.write(json.dumps(self.program_data_dict, indent=2))
