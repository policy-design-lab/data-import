import json
import os

import pandas as pd
from deepmerge import always_merger


class DataParser:
    def __init__(self, start_year, end_year, program_main_category_name, data_folder, program_csv_filename, **kwargs):
        self.start_year = start_year
        self.end_year = end_year
        self.program_main_category_name = program_main_category_name
        self.data_folder = data_folder
        self.program_csv_filepath = os.path.join(data_folder, program_csv_filename)
        self.program_main_category_name = program_main_category_name
        self.program_data = None

        # Main program category specific file paths
        if self.program_main_category_name == "Title 1: Commodities":
            self.base_acres_data = None
            self.farm_payee_count_data = None

            self.base_acres_csv_filepath_arc_co = os.path.join(data_folder, kwargs["base_acres_csv_filename_arc_co"])
            self.base_acres_csv_filepath_plc = os.path.join(data_folder, kwargs["base_acres_csv_filename_plc"])

            self.farm_payee_count_csv_filepath_arc_co = os.path.join(data_folder,
                                                                     kwargs["farm_payee_count_csv_filename_arc_co"])
            self.farm_payee_count_csv_filepath_arc_ic = os.path.join(data_folder,
                                                                     kwargs["farm_payee_count_csv_filename_arc_ic"])
            self.farm_payee_count_csv_filepath_plc = os.path.join(data_folder,
                                                                  kwargs["farm_payee_count_csv_filename_plc"])
            self.total_payment_csv_filepath_arc_co = os.path.join(data_folder,
                                                                  kwargs["total_payment_csv_filename_arc_co"])
            self.total_payment_csv_filepath_arc_ic = os.path.join(data_folder,
                                                                  kwargs["total_payment_csv_filename_arc_ic"])
            self.total_payment_csv_filepath_plc = os.path.join(data_folder,
                                                               kwargs["total_payment_csv_filename_plc"])

        # Output data dictionaries
        self.processed_data_dict = dict()
        self.state_distribution_data_dict = dict()
        self.program_data_dict = dict()

        self.metadata = {
            "Title 1: Commodities": {
                "programs_subprograms_map": {
                    "Agriculture Risk Coverage (ARC)": ["Agriculture Risk Coverage County Option (ARC-CO)",
                                                        "Agriculture Risk Coverage Individual Coverage (ARC-IC)"],
                    "Price Loss Coverage (PLC)": [],
                    # "Dairy": ["Dairy Margin Coverage Program (DMC)", "Dairy Indemnity Payment Program (DIPP)"],
                    # "Disaster Assistance": ["Tree Assistance Program (TAP)",
                    #                         "Noninsured Crop Disaster Assistance Program (NAP)",
                    #                         "Livestock Forage Disaster Program (LFP)",
                    #                         "Livestock Indemnity Program (LIP)",
                    #                         "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)"]
                },
                "value_names_map": {
                    "ARC-Ind": "Agriculture Risk Coverage Individual Coverage (ARC-IC)",
                    "ARC-CO": "Agriculture Risk Coverage County Option (ARC-CO)",
                    "PLC": "Price Loss Coverage (PLC)",
                    "DMC": "Dairy Margin Coverage Program (DMC)",
                    "TAP": "Tree Assistance Program (TAP)",
                    "NAP": "Noninsured Crop Disaster Assistance Program (NAP)",
                    "LFP": "Livestock Forage Disaster Program (LFP)",
                    "LIP": "Livestock Indemnity Program (LIP)",
                    "ELAP": "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)",
                    "Ad Hoc": "Ad hoc or Supplemental",
                    "MFP": "Market Facilitation Program (MFP)",
                    "CFAP": "Coronavirus Food Assistance Program (CFAP)",
                    "Dairy Indemnity": "Dairy Indemnity Payment Program (DIPP)"
                },
                "column_names_map": {
                    "Year": "year",
                    "Program": "program_description",
                    "amount": "payments",
                    "State Name": "state"
                },
                "zero_subprograms_map": {
                    "subProgramName": None,
                    "paymentInDollars": 0.0,
                    "paymentInPercentageWithinState": 0.00,
                    "averageAreaInAcres": 0.0,
                    "averageRecipientCount": 0
                }
            },
            "Crop Insurance": {
                "programs_subprograms_map": {
                    "Policies Earning Premium": [],
                    "Total Liabilities": [],
                    "Total Indemnities": [],
                    "Total Premium": [],
                    "Subsidy": [],
                    "Farmer Paid Premium": [],
                    "Loss Ratio": []
                },
                "value_names_map": {
                },
                "column_names_map": {
                },
                "zero_subprograms_map": {
                }
            },
            "Title 2: Conservation: CRP": {
                "programs_subprograms_map": {
                    "General Sign-Up": ["NUMBER OF CONTRACTS", "NUMBER OF FARMS", "ACRES",
                                        "ANNUAL RENTAL PAYMENTS ($1000)", "ANNUAL RENTAL PAYMENTS ($/ACRE)"],
                    "Total Continuous Sign-up": {
                        "CREP Only": ["NUMBER OF CONTRACTS", "NUMBER OF FARMS", "ACRES",
                                      "ANNUAL RENTAL PAYMENTS ($1000)", "ANNUAL RENTAL PAYMENTS ($/ACRE)"],
                        "Continuous Non-CREP)": ["NUMBER OF CONTRACTS", "NUMBER OF FARMS", "ACRES",
                                                 "ANNUAL RENTAL PAYMENTS ($1000)",
                                                 "ANNUAL RENTAL PAYMENTS ($/ACRE)"],
                        "Farmable Wetland": ["NUMBER OF CONTRACTS", "NUMBER OF FARMS", "ACRES",
                                             "ANNUAL RENTAL PAYMENTS ($1000)", "ANNUAL RENTAL PAYMENTS ($/ACRE)"]
                    },
                    "Grassland": ["NUMBER OF CONTRACTS", "NUMBER OF FARMS", "ACRES",
                                  "ANNUAL RENTAL PAYMENTS ($1000)", "ANNUAL RENTAL PAYMENTS ($/ACRE)"]
                },
                "value_names_map": {
                    'ALABAMA': 'AL',
                    'ALASKA': 'AK',
                    'ARIZONA': 'AZ',
                    'ARKANSAS': 'AR',
                    'CALIFORNIA': 'CA',
                    'COLORADO': 'CO',
                    'CONNECTICUT': 'CT',
                    'DELAWARE': 'DE',
                    'FLORIDA': 'FL',
                    'GEORGIA': 'GA',
                    'HAWAII': 'HI',
                    'IDAHO': 'ID',
                    'ILLINOIS': 'IL',
                    'INDIANA': 'IN',
                    'IOWA': 'IA',
                    'KANSAS': 'KS',
                    'KENTUCKY': 'KY',
                    'LOUISIANA': 'LA',
                    'MAINE': 'ME',
                    'MARYLAND': 'MD',
                    'MASSACHUSETTS': 'MA',
                    'MICHIGAN': 'MI',
                    'MINNESOTA': 'MN',
                    'MISSISSIPPI': 'MS',
                    'MISSOURI': 'MO',
                    'MONTANA': 'MT',
                    'NEBRASKA': 'NE',
                    'NEVADA': 'NV',
                    'NEW HAMPSHIRE': 'NH',
                    'NEW JERSEY': 'NJ',
                    'NEW MEXICO': 'NM',
                    'NEW YORK': 'NY',
                    'NORTH CAROLINA': 'NC',
                    'NORTH DAKOTA': 'ND',
                    'OHIO': 'OH',
                    'OKLAHOMA': 'OK',
                    'OREGON': 'OR',
                    'PENNSYLVANIA': 'PA',
                    'RHODE ISLAND': 'RI',
                    'SOUTH CAROLINA': 'SC',
                    'SOUTH DAKOTA': 'SD',
                    'TENNESSEE': 'TN',
                    'TEXAS': 'TX',
                    'UTAH': 'UT',
                    'VERMONT': 'VT',
                    'VIRGINIA': 'VA',
                    'WASHINGTON': 'WA',
                    'WEST VIRGINIA': 'WV',
                    'WISCONSIN': 'WI',
                    'WYOMING': 'WY'
                },
                "column_names_map": {
                    "Total CRP - NUMBER OF CONTRACTS": "CRP-Contract",
                    "Total CRP - NUMBER OF FARMS": "CRP-Farm",
                    "Total CRP - ACRES": "CRP-Acre",
                    "Total CRP - ANNUAL RENTAL PAYMENTS ($1000)": "CRP-Rent-1K",
                    "Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)": "CRP-Ren-Acre",
                    "Total General Sign-Up - NUMBER OF CONTRACTS": "Gen-Contract",
                    "Total General Sign-Up - NUMBER OF FARMS": "Gen-Farm",
                    "Total General Sign-Up - ACRES": "Gen-Acre",
                    "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)": "Gen-Rent-1K",
                    "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)": "Gen-Rent-Acre",
                    "Total Continuous - NUMBER OF CONTRACTS": "Cont-Contract",
                    "Total Continuous - NUMBER OF FARMS": "Cont-Farm",
                    "Total Continuous - ACRES": "Cont-Acre",
                    "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)": "Cont-Rent-1K",
                    "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)": "Cont-Rent-Acre",
                    "CREP Only - NUMBER OF CONTRACTS": "CREP-Contract",
                    "CREP Only - NUMBER OF FARMS": "CREP-Farm",
                    "CREP Only - ACRES": "CREP-Acre",
                    "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)": "CREP-Rent-1K",
                    "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)": "CREP-Rent-Acre",
                    "Continuous Non-CREP - NUMBER OF CONTRACTS": "No-CREP-Contract",
                    "Continuous Non-CREP - NUMBER OF FARMS": "No-CREP-Farm",
                    "Continuous Non-CREP - ACRES": "No-CREP-Acre",
                    "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)": "No-CREP-Rent-1K",
                    "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)": "No-CREP-Rent-Acre",
                    "Farmable Wetland - NUMBER OF CONTRACTS": "Wet-Contract",
                    "Farmable Wetland - NUMBER OF FARMS": "Wet-Farm",
                    "Farmable Wetland - ACRES": "Wet-Acre",
                    "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)": "Wet-Rent-1K",
                    "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)": "Wet-Rent-Acre",
                    "Grassland - NUMBER OF CONTRACTS": "Grass-Contract",
                    "Grassland - NUMBER OF FARMS": "Grass-Farm",
                    "Grassland - ACRES": "Grass-Acre",
                    "Grassland - ANNUAL RENTAL PAYMENTS ($1000)": "Grass-Rent-1K",
                    "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)": "Grass-Rent-Acre"
                },
                "zero_subprograms_map": {
                }
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
            'WY': 'Wyoming'
        }

    def find_program_by_subprogram(self, program_description):
        for program_name in self.metadata[self.program_main_category_name]["programs_subprograms_map"]:
            if program_description == program_name and len(
                    self.metadata[self.program_main_category_name]["programs_subprograms_map"][program_name]) == 0:
                return program_name
            if program_description in self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                program_name]:
                return program_name

    def find_and_get_zero_subprogram_entries(self, program_name, subprograms_list,
                                             for_percentage_json=False):
        diff_list = list(
            set(self.metadata[self.program_main_category_name]["programs_subprograms_map"][program_name]) - set(
                subprograms_list))
        zero_subprogram_entries = []
        for entry in diff_list:
            entry_dict = self.metadata[self.program_main_category_name]["zero_subprograms_map"].copy()
            entry_dict["subProgramName"] = entry
            if not for_percentage_json:
                del entry_dict["paymentInPercentageWithinState"]
                zero_subprogram_entries.append(entry_dict)
            else:
                zero_subprogram_entries.append(entry_dict)
        return zero_subprogram_entries

    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        if self.program_data is None:
            self.program_data = pd.read_csv(self.program_csv_filepath)

        self.program_data = self.program_data.replace(self.metadata[self.program_main_category_name]["value_names_map"])

        # Rename column names to make it more uniform
        self.program_data.rename(columns=self.metadata[self.program_main_category_name]["column_names_map"],
                                 inplace=True)

        # Filter only relevant years' data
        self.program_data = self.program_data[self.program_data["year"].between(self.start_year, self.end_year,
                                                                                inclusive="both")]

        # Exclude programs that are not included at present
        self.program_data = self.program_data[
            (self.program_data["program_description"] != "Ad hoc or Supplemental") &
            (self.program_data["program_description"] != "Market Facilitation Program (MFP)") &
            (self.program_data["program_description"] != "Coronavirus Food Assistance Program (CFAP)")
            ]

        # Group data by state, program description, and payment
        payments_by_program_by_state_for_year = \
            self.program_data[
                ["year", "state", "program_description", "payments"]
            ].groupby(
                ["year", "state", "program_description"]
            )["payments"].sum()

        # Import base acres data
        self.base_acres_data = self.base_acres_data.replace(
            self.metadata[self.program_main_category_name]["value_names_map"])

        # Rename column names to make it more uniform
        self.base_acres_data.rename(columns={"State Name": "state",
                                             "Year": "year",
                                             "Program": "program_description",
                                             "Enrolled Base": "base_acres"}, inplace=True)

        # Filter only relevant years' data
        self.base_acres_data = self.base_acres_data[
            self.base_acres_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # Import farmer count data
        self.farm_payee_count_data = self.farm_payee_count_data.replace(
            self.metadata[self.program_main_category_name]["value_names_map"])

        # Rename column names to make it more uniform
        self.farm_payee_count_data.rename(columns={"State Name": "state",
                                                   "Year": "year",
                                                   "Program": "program_description",
                                                   "Payee Count": "recipient_count"}, inplace=True)

        # Filter only relevant years' data
        self.farm_payee_count_data = self.farm_payee_count_data[
            self.farm_payee_count_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # 1. Generate map data
        if True:
            # Iterate through all tuples
            for data_tuple, payment in payments_by_program_by_state_for_year.items():
                year, state_name, program_description = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(year),
                    "programs": [
                        {
                            "programName": "Agriculture Risk Coverage (ARC)",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0

                        },
                        {
                            "programName": "Price Loss Coverage (PLC)",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        },
                        # {
                        #     "programName": "Dairy",
                        #     "subPrograms": [
                        #     ],
                        #     "programPaymentInDollars": 0.0
                        # },
                        # {
                        #     "programName": "Disaster Assistance",
                        #     "subPrograms": [
                        #     ],
                        #     "programPaymentInDollars": 0.0
                        # }
                    ]
                }

                # Get program name
                program_subprogram_name = self.find_program_by_subprogram(program_description)

                for program in new_data_entry["programs"]:
                    if program["programName"] == program_subprogram_name:
                        if len(self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                                   program_subprogram_name]) == 0:
                            program["programPaymentInDollars"] = rounded_payment
                        else:
                            program["subPrograms"].append({
                                "subProgramName": program_description,
                                "paymentInDollars": rounded_payment
                            })
                        break

                # Update self.processed_data_dict
                if state_name not in self.processed_data_dict:
                    self.processed_data_dict[state_name] = [new_data_entry]
                elif self.processed_data_dict[state_name] is not None:
                    found = False
                    for entry in self.processed_data_dict[state_name]:
                        if entry["years"] == new_data_entry["years"]:
                            for entry_program in entry["programs"]:
                                for new_data_entry_program in new_data_entry["programs"]:
                                    if entry_program["programName"] == new_data_entry_program["programName"]:
                                        # Merge existing entries and new data entries
                                        entry_program["subPrograms"] = always_merger.merge(
                                            entry_program["subPrograms"],
                                            new_data_entry_program["subPrograms"])
                            found = True
                            break
                    if not found:
                        self.processed_data_dict[state_name].append(new_data_entry)

            # Get total payment data
            total_payments_by_program_by_state = self.program_data[
                ["state", "program_description", "payments"]].groupby(
                ["state", "program_description"]
            )["payments"].sum()

            # Iterate through all tuples
            for data_tuple, payment in total_payments_by_program_by_state.items():
                state_name, program_description = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "programs": [
                        {
                            "programName": "Agriculture Risk Coverage (ARC)",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        },
                        {
                            "programName": "Price Loss Coverage (PLC)",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        },
                        # {
                        #     "programName": "Dairy",
                        #     "subPrograms": [
                        #     ],
                        #     "programPaymentInDollars": 0.0
                        # },
                        # {
                        #     "programName": "Disaster Assistance",
                        #     "subPrograms": [
                        #     ],
                        #     "programPaymentInDollars": 0.0
                        # }
                    ]
                }

                # Get program name
                program_subprogram_name = self.find_program_by_subprogram(program_description)

                for program in new_data_entry["programs"]:
                    if program["programName"] == program_subprogram_name:
                        if len(self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                                   program_subprogram_name]) == 0:
                            program["programPaymentInDollars"] = rounded_payment
                        else:
                            program["subPrograms"].append({
                                "subProgramName": program_description,
                                "paymentInDollars": rounded_payment
                            })

                found = False
                # Update self.processed_data_dict
                for entry in self.processed_data_dict[state_name]:
                    if entry["years"] == new_data_entry["years"]:
                        for entry_program in entry["programs"]:
                            for new_data_entry_program in new_data_entry["programs"]:
                                if entry_program["programName"] == new_data_entry_program["programName"]:
                                    entry_program["subPrograms"] = always_merger.merge(
                                        entry_program["subPrograms"],
                                        new_data_entry_program["subPrograms"])
                        found = True
                        break
                if not found:
                    self.processed_data_dict[state_name].append(new_data_entry)

            # Calculate total for each state and update data dictionary
            for state_name in self.processed_data_dict:
                for year_data in self.processed_data_dict[state_name]:
                    total_payment_in_dollars = 0
                    for program in year_data["programs"]:
                        total_payment_in_dollars_program = 0
                        for subprogram in program["subPrograms"]:
                            total_payment_in_dollars += subprogram["paymentInDollars"]
                            total_payment_in_dollars_program += subprogram["paymentInDollars"]
                        program["programPaymentInDollars"] = round(total_payment_in_dollars_program, 2)
                    year_data["totalPaymentInDollars"] = round(total_payment_in_dollars, 2)

            # Add zero entries
            for state_name in self.processed_data_dict:
                for year_data in self.processed_data_dict[state_name]:
                    for program in year_data["programs"]:
                        subprograms_list = []
                        for subprogram in program["subPrograms"]:
                            subprograms_list.append(subprogram["subProgramName"])
                        zero_entries = self.find_and_get_zero_subprogram_entries(program["programName"],
                                                                                 subprograms_list)
                        program["subPrograms"] = always_merger.merge(program["subPrograms"], zero_entries)

                        # Sort categories by name
                        program["subPrograms"].sort(key=lambda x: x["subProgramName"])

            # Write processed_data_dict as JSON data
            with open(os.path.join(self.data_folder, "commodities_map_data.json"), "w") as output_json_file:
                output_json_file.write(json.dumps(self.processed_data_dict, indent=2))

        # 2. Generate state distribution data
        if True:
            total_payments_by_state = round(self.program_data[
                                                ["state", "payments"]].groupby(["state"])["payments"].sum(), 2)

            total_payments_at_national_level = round(self.program_data["payments"].sum(), 2)

            total_payments_by_program_by_state = self.program_data[
                ["state", "program_description", "payments"]].groupby(
                ["state", "program_description"]
            )["payments"].sum()

            total_payments_by_program_at_national_level = round(
                self.program_data[["program_description", "payments"]].groupby(["program_description"]).sum(), 2)

            average_base_acres_by_program_by_state = self.base_acres_data[
                ["state", "program_description", "base_acres", "year"]].groupby(
                ["state", "program_description", "year"]
            )["base_acres"].sum().groupby(["state", "program_description"]).mean()

            average_payee_count_by_program_by_state = self.farm_payee_count_data[
                ["state", "program_description", "recipient_count", "year"]].groupby(
                ["state", "program_description", "year"]
            )["recipient_count"].sum().groupby(["state", "program_description"]).mean()

            self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

            for state in self.us_state_abbreviations:
                state_name = self.us_state_abbreviations[state]
                yearly_state_payment = total_payments_by_state[state_name]

                new_data_entry = {
                    "state": state,
                    "programs": [
                        {
                            "programName": "Agriculture Risk Coverage (ARC)",
                            "programPaymentInDollars": 0.0,
                            "averageAreaInAcres": 0.0,
                            "averageRecipientCount": 0,
                            "subPrograms": [
                            ],
                        },
                        {
                            "programName": "Price Loss Coverage (PLC)",
                            "programPaymentInDollars": 0.0,
                            "averageAreaInAcres": 0.0,
                            "averageRecipientCount": 0,
                            "subPrograms": [
                            ]
                        },
                        # {
                        #     "programName": "Dairy",
                        #     "programPaymentInDollars": 0.0,
                        #     "averageAreaInAcres": 0.0,
                        #     "averageRecipientCount": 0,
                        #     "subPrograms": [
                        #     ]
                        # },
                        # {
                        #     "programName": "Disaster Assistance",
                        #     "programPaymentInDollars": 0.0,
                        #     "averageAreaInAcres": 0.0,
                        #     "averageRecipientCount": 0,
                        #     "subPrograms": [
                        #     ]
                        # }
                    ],
                    "totalPaymentInPercentageNationwide": round(
                        (yearly_state_payment / total_payments_at_national_level) * 100, 2),
                    "totalPaymentInDollars": round(yearly_state_payment, 2)
                }

                program_payments_series = total_payments_by_program_by_state[state_name]
                if state_name in average_base_acres_by_program_by_state:
                    average_base_acres_series = average_base_acres_by_program_by_state[state_name]
                else:
                    average_base_acres_series = pd.Series(object)

                if state_name in average_payee_count_by_program_by_state:
                    average_payee_count_series = average_payee_count_by_program_by_state[state_name]
                else:
                    average_payee_count_series = pd.Series(object)

                for program_description, program_payment in program_payments_series.items():
                    program_subprogram_name = self.find_program_by_subprogram(program_description)
                    rounded_program_payment = round(program_payment, 2)
                    program_percentage_nationwide = round(
                        (rounded_program_payment / total_payments_by_program_at_national_level["payments"][
                            program_description]) * 100, 2)
                    if total_payments_by_state[state_name] == 0.0:
                        program_percentage_within_state = 0.0
                    else:
                        program_percentage_within_state = round(
                            (rounded_program_payment / total_payments_by_state[state_name]) * 100, 2)

                    if program_description in average_base_acres_series:
                        average_base_acres = round(average_base_acres_series[program_description], 2)
                    else:
                        average_base_acres = 0.0

                    if program_description in average_payee_count_series:
                        average_recipient_count = round(average_payee_count_series[program_description])
                    else:
                        average_recipient_count = 0

                    for program in new_data_entry["programs"]:
                        if program["programName"] == program_subprogram_name:

                            if len(self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                                       program_subprogram_name]) == 0:
                                pass
                            else:
                                program["subPrograms"].append({
                                    "subProgramName": program_description,
                                    "paymentInDollars": rounded_program_payment,
                                    "paymentInPercentageNationwide": program_percentage_nationwide,
                                    "paymentInPercentageWithinState": program_percentage_within_state,
                                    "averageAreaInAcres": average_base_acres,
                                    "averageRecipientCount": average_recipient_count
                                })
                            program["programPaymentInDollars"] += rounded_program_payment
                            program["averageAreaInAcres"] += average_base_acres
                            program["averageRecipientCount"] += average_recipient_count

                self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)].append(
                    new_data_entry)

            # Add zero entries
            for state_name in self.state_distribution_data_dict:
                for year_data in self.state_distribution_data_dict[state_name]:
                    for program in year_data["programs"]:
                        program["programPaymentInDollars"] = round(program["programPaymentInDollars"], 2)
                        program["averageAreaInAcres"] = round(program["averageAreaInAcres"], 2)

                        subprograms_list = []
                        for subprogram in program["subPrograms"]:
                            subprograms_list.append(subprogram["subProgramName"])
                        zero_entries = self.find_and_get_zero_subprogram_entries(program["programName"],
                                                                                 subprograms_list, True)
                        program["subPrograms"] = always_merger.merge(
                            program["subPrograms"], zero_entries)

                        # Sort categories by percentages
                        program["subPrograms"].sort(reverse=True,
                                                    key=lambda x: x["paymentInPercentageWithinState"])

            # Sort states by decreasing order of totalPaymentInPercentageNationwide
            for year in self.state_distribution_data_dict:
                self.state_distribution_data_dict[year] = sorted(self.state_distribution_data_dict[year],
                                                                 key=lambda x: x["totalPaymentInPercentageNationwide"],
                                                                 reverse=True)

            # Write processed_data_dict as JSON data
            with open(os.path.join(self.data_folder, "commodities_state_distribution_data.json"),
                      "w") as output_json_file:
                output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=2))

        # 3. Generate practice categories data for the donut chart
        if True:
            self.program_data_dict = {
                "programs": [
                    {
                        "programName": "Agriculture Risk Coverage (ARC)",
                        "subPrograms": [
                        ]
                    },
                    {
                        "programName": "Price Loss Coverage (PLC)",
                        "subPrograms": [
                        ]
                    },
                    # {
                    #     "programName": "Dairy",
                    #     "subPrograms": [
                    #     ]
                    # },
                    # {
                    #     "programName": "Disaster Assistance",
                    #     "subPrograms": [
                    #     ]
                    # }
                ]
            }
            total_for_program = {
                "Agriculture Risk Coverage (ARC)": 0.0,
                "Price Loss Coverage (PLC)": 0.0,
                # "Dairy": 0.0,
                # "Disaster Assistance": 0.0
            }

            for program in self.program_data_dict["programs"]:
                if len(self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                           program["programName"]]) > 0:
                    for program_subprogram_name in \
                            self.metadata[self.program_main_category_name]["programs_subprograms_map"][
                                program["programName"]]:
                        if program_subprogram_name in total_payments_by_program_at_national_level["payments"]:
                            subprogram_payment = round(
                                total_payments_by_program_at_national_level["payments"][program_subprogram_name], 2)
                            entry_dict = {
                                "subProgramName": program_subprogram_name,
                                "totalPaymentInDollars": subprogram_payment,
                            }
                            total_for_program[program["programName"]] += subprogram_payment
                        # When subprogram is not existing in the actual data
                        else:
                            entry_dict = {
                                "subProgramName": program_subprogram_name,
                                "totalPaymentInDollars": 0.0,
                            }
                        program["subPrograms"].append(entry_dict)
                else:
                    program_subprogram_name = program["programName"]
                    if program_subprogram_name in total_payments_by_program_at_national_level["payments"]:
                        subprogram_payment = round(
                            total_payments_by_program_at_national_level["payments"][program_subprogram_name], 2)
                        total_for_program[program["programName"]] += subprogram_payment

            for program in self.program_data_dict["programs"]:
                for subprogram in program["subPrograms"]:
                    subprogram["totalPaymentInPercentage"] = round(
                        subprogram["totalPaymentInDollars"] / total_for_program[
                            program["programName"]] * 100, 2)
                program["totalPaymentInDollars"] = round(total_for_program[program["programName"]], 2)
                program["totalPaymentInPercentage"] = round(
                    total_for_program[program["programName"]] / total_payments_at_national_level * 100, 2)
                program["subPrograms"].sort(key=lambda x: x["totalPaymentInPercentage"], reverse=True)

            # Write processed_data_dict as JSON data
            with open(os.path.join(self.data_folder, "commodities_subprograms_data.json"), "w") as output_json_file:
                output_json_file.write(json.dumps(self.program_data_dict, indent=2))

    def __convert_to_new_data_frame(self, data_frame, program_name, data_type):
        row_list = []
        for state in self.us_state_abbreviations:
            state_data = data_frame[
                data_frame["State Name"] == self.us_state_abbreviations[state]]
            for year in range(self.start_year, self.end_year + 1):
                row_dict = dict()
                if state_data['State Name'].size == 1:
                    row_dict["State Name"] = state_data['State Name'].item()
                    row_dict["Year"] = year
                    row_dict["Program"] = program_name

                    if data_type == "Base Acres":
                        row_dict["Enrolled Base"] = state_data[str(year)].item()
                    elif data_type == "Payee Count":
                        row_dict["Payee Count"] = state_data[str(year)].item()
                    elif data_type == "Total Payment":
                        row_dict["amount"] = round(state_data[str(year)].item(), 2)

                    row_list.append(row_dict)
        output_data_frame = pd.DataFrame(data=row_list)
        return output_data_frame

    def format_title_commodities_data(self):

        # Import base acres CSV files and convert to existing format
        base_acres_data_arc_co = pd.read_csv(self.base_acres_csv_filepath_arc_co)
        base_acres_data_plc = pd.read_csv(self.base_acres_csv_filepath_plc)
        base_acres_data_arc_co_output = self.__convert_to_new_data_frame(base_acres_data_arc_co, "ARC-CO", "Base Acres")
        base_acres_data_plc_output = self.__convert_to_new_data_frame(base_acres_data_plc, "PLC", "Base Acres")
        self.base_acres_data = pd.concat([base_acres_data_arc_co_output, base_acres_data_plc_output], ignore_index=True)

        # Import farm payee count CSV files and convert to existing format
        farm_payee_count_data_arc_co = pd.read_csv(self.farm_payee_count_csv_filepath_arc_co)
        farm_payee_count_data_arc_ic = pd.read_csv(self.farm_payee_count_csv_filepath_arc_ic)
        farm_payee_count_data_plc = pd.read_csv(self.farm_payee_count_csv_filepath_plc)

        farm_payee_count_data_arc_co_output = self.__convert_to_new_data_frame(farm_payee_count_data_arc_co, "ARC-CO",
                                                                               "Payee Count")
        farm_payee_count_data_arc_ic_output = self.__convert_to_new_data_frame(farm_payee_count_data_arc_ic, "ARC-Ind",
                                                                               "Payee Count")
        farm_payee_count_data_plc_output = self.__convert_to_new_data_frame(farm_payee_count_data_plc, "PLC",
                                                                            "Payee Count")
        self.farm_payee_count_data = pd.concat(
            [farm_payee_count_data_arc_co_output, farm_payee_count_data_arc_ic_output,
             farm_payee_count_data_plc_output], ignore_index=True)

        # Import total payment count CSV files and convert to existing format
        total_payment_data_arc_co = pd.read_csv(self.total_payment_csv_filepath_arc_co)
        total_payment_data_arc_ic = pd.read_csv(self.total_payment_csv_filepath_arc_ic)
        total_payment_data_plc = pd.read_csv(self.total_payment_csv_filepath_plc)

        total_payment_data_arc_co_output = self.__convert_to_new_data_frame(total_payment_data_arc_co, "ARC-CO",
                                                                            "Total Payment")
        total_payment_data_arc_ic_output = self.__convert_to_new_data_frame(total_payment_data_arc_ic, "ARC-Ind",
                                                                            "Total Payment")
        total_payment_data_plc_output = self.__convert_to_new_data_frame(total_payment_data_plc, "PLC", "Total Payment")
        self.program_data = pd.concat([total_payment_data_arc_co_output, total_payment_data_arc_ic_output,
                                       total_payment_data_plc_output], ignore_index=True)

    def parse_and_process_crop_insurance(self):
        # Import CSV file into a Pandas DataFrame
        program_data = pd.read_csv(self.program_csv_filepath)
        program_data = program_data.replace(self.metadata[self.program_main_category_name]["value_names_map"])

        # Rename column names to make it more uniform
        program_data.rename(columns=self.metadata[self.program_main_category_name]["column_names_map"], inplace=True)

        # Filter only relevant years' data
        program_data = program_data[program_data["year"].between(self.start_year, self.end_year,
                                                                 inclusive="both")]

        # 1. Generate State Distribution JSON Data
        self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

        # Total premium by state
        total_premium_by_state = \
            program_data[
                ["state", "premium"]
            ].groupby(
                ["state"]
            )["premium"].sum()

        # Total indemnities by state
        total_indemnities_by_state = \
            program_data[
                ["state", "indemnity"]
            ].groupby(
                ["state"]
            )["indemnity"].sum()

        # Total premium subsidies by state
        total_premium_subsidies_by_state = \
            program_data[
                ["state", "subsidy"]
            ].groupby(
                ["state"]
            )["subsidy"].sum()

        # Total farmer paid premium by state
        total_farmer_premium_by_state = \
            program_data[
                ["state", "farmer_premium"]
            ].groupby(
                ["state"]
            )["farmer_premium"].sum()

        # Total net farmer benefit by state
        total_net_farmer_benefit_by_state = \
            program_data[
                ["state", "net_benefit"]
            ].groupby(
                ["state"]
            )["net_benefit"].sum()

        # Total policies earning premium by state
        total_policies_earning_premium_by_state = \
            program_data[
                ["state", "policies_prem"]
            ].groupby(
                ["state"]
            )["policies_prem"].sum()

        # Total liabilities by state
        total_liabilities_by_state = \
            program_data[
                ["state", "liabilities"]
            ].groupby(
                ["state"]
            )["liabilities"].sum()

        # Loss ratio by state
        loss_ratio_by_state = total_indemnities_by_state / total_premium_by_state

        for state in self.us_state_abbreviations:
            new_data_entry = {
                "state": state,
                "programs": [
                    {
                        "programName": "Crop Insurance",
                        "totalIndemnitiesInDollars": total_indemnities_by_state[state].item(),
                        "totalPremiumInDollars": total_premium_by_state[state].item(),
                        "totalPremiumSubsidyInDollars": total_premium_subsidies_by_state[state].item(),
                        "totalFarmerPaidPremiumInDollars": total_farmer_premium_by_state[state].item(),
                        "totalNetFarmerBenefitInDollars": total_net_farmer_benefit_by_state[state].item(),
                        "totalPoliciesEarningPremium": total_policies_earning_premium_by_state[state].item(),
                        "totalLiabilitiesInDollars": total_liabilities_by_state[state].item(),
                        "lossRatio": round(loss_ratio_by_state[state].item(), 3),
                        "subPrograms": []
                    }

                ]
            }

            self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)].append(
                new_data_entry)

        # Sort states by decreasing order of total indemnities
        for year in self.state_distribution_data_dict:
            self.state_distribution_data_dict[year] = sorted(self.state_distribution_data_dict[year],
                                                             key=lambda x: x["programs"][0][
                                                                 "totalIndemnitiesInDollars"],
                                                             reverse=True)

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "crop_insurance_state_distribution_data.json"),
                  "w") as output_json_file:
            output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=2))

        # 2. Generate Sub Programs Data

        # Total premium
        total_premium = \
            program_data["premium"].sum()

        # Total indemnities
        total_indemnities = \
            program_data["indemnity"].sum()

        # Total premium subsidies
        total_premium_subsidies = \
            program_data["subsidy"].sum()

        # Total farmer paid premium
        total_farmer_premium = \
            program_data["farmer_premium"].sum()

        # Total net farmer benefit by state
        total_net_farmer_benefit = \
            program_data["net_benefit"].sum()

        # Total policies earning premium
        total_policies_earning_premium = \
            program_data["policies_prem"].sum()

        # Total liabilities
        total_liabilities = \
            program_data["liabilities"].sum()

        # Overall loss ratio
        overall_loss_ratio = total_indemnities / total_premium

        self.program_data_dict = {
            "programs": [
                {
                    "programName": "Crop Insurance",
                    "subPrograms": [
                    ],
                    "totalIndemnitiesInDollars": total_indemnities.item(),
                    "totalPremiumInDollars": total_premium.item(),
                    "totalPremiumSubsidyInDollars": total_premium_subsidies.item(),
                    "totalFarmerPaidPremiumInDollars": total_farmer_premium.item(),
                    "totalNetFarmerBenefitInDollars": total_net_farmer_benefit.item(),
                    "totalLiabilitiesInDollars": total_liabilities.item(),
                    "totalPoliciesEarningPremium": total_policies_earning_premium.item(),
                    "lossRatio": round(overall_loss_ratio.item(), 3)
                }
            ]
        }

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "crop_insurance_subprograms_data.json"), "w") as output_json_file:
            output_json_file.write(json.dumps(self.program_data_dict, indent=2))

    def parse_and_process_crp(self):
        # Import CSV file into a Pandas DataFrame
        program_data = pd.read_csv(self.program_csv_filepath)

        # Change state name to state abbreviation
        program_data = program_data.replace(self.metadata[self.program_main_category_name]["value_names_map"])

        # some columns have empty values and this makes the rows type as object
        # this makes the process of SUM errors since those are object not number
        # so the columns should be numeric all the time
        # make sure every number columns be number
        program_data[["Total CRP - NUMBER OF CONTRACTS",
                      "Total CRP - NUMBER OF FARMS",
                      "Total CRP - ACRES",
                      "Total CRP - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "Total General Sign-Up - NUMBER OF CONTRACTS",
                      "Total General Sign-Up - NUMBER OF FARMS",
                      "Total General Sign-Up - ACRES",
                      "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "Total Continuous - NUMBER OF CONTRACTS",
                      "Total Continuous - NUMBER OF FARMS",
                      "Total Continuous - ACRES",
                      "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "CREP Only - NUMBER OF CONTRACTS",
                      "CREP Only - NUMBER OF FARMS",
                      "CREP Only - ACRES",
                      "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)",
                      "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "Continuous Non-CREP - NUMBER OF CONTRACTS",
                      "Continuous Non-CREP - NUMBER OF FARMS",
                      "Continuous Non-CREP - ACRES",
                      "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "Farmable Wetland - NUMBER OF CONTRACTS",
                      "Farmable Wetland - NUMBER OF FARMS",
                      "Farmable Wetland - ACRES",
                      "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                      "Grassland - NUMBER OF CONTRACTS",
                      "Grassland - NUMBER OF FARMS",
                      "Grassland - ACRES",
                      "Grassland - ANNUAL RENTAL PAYMENTS ($1000)",
                      "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]] = \
            program_data[["Total CRP - NUMBER OF CONTRACTS",
                          "Total CRP - NUMBER OF FARMS",
                          "Total CRP - ACRES",
                          "Total CRP - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "Total General Sign-Up - NUMBER OF CONTRACTS",
                          "Total General Sign-Up - NUMBER OF FARMS",
                          "Total General Sign-Up - ACRES",
                          "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "Total Continuous - NUMBER OF CONTRACTS",
                          "Total Continuous - NUMBER OF FARMS",
                          "Total Continuous - ACRES",
                          "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "CREP Only - NUMBER OF CONTRACTS",
                          "CREP Only - NUMBER OF FARMS",
                          "CREP Only - ACRES",
                          "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)",
                          "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "Continuous Non-CREP - NUMBER OF CONTRACTS",
                          "Continuous Non-CREP - NUMBER OF FARMS",
                          "Continuous Non-CREP - ACRES",
                          "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "Farmable Wetland - NUMBER OF CONTRACTS",
                          "Farmable Wetland - NUMBER OF FARMS",
                          "Farmable Wetland - ACRES",
                          "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)",
                          "Grassland - NUMBER OF CONTRACTS",
                          "Grassland - NUMBER OF FARMS",
                          "Grassland - ACRES",
                          "Grassland - ANNUAL RENTAL PAYMENTS ($1000)",
                          "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].apply(pd.to_numeric)

        # Rename column names to make it more uniform
        # program_data.rename(columns=self.metadata[self.program_main_category_name]["column_names_map"], inplace=True)

        # there are rows for U.S. and Puerto Rico this should not be used to calculate national level
        # the weird thing is that the value in U.S. doesn't match to the sum of all the states
        # get national level values location from the table since it contains it
        us_row_loc = []
        for index, state in enumerate(program_data['state']):
            if state == 'U.S.':
                us_row_loc.append(index)

        # remove U.S. rows
        program_data = program_data.drop(program_data.index[us_row_loc])

        # remove puerto rico
        rico_row_loc = []
        for index, state in enumerate(program_data['state']):
            if state == 'PUERTO RICO':
                rico_row_loc.append(index)
        program_data = program_data.drop(program_data.index[rico_row_loc])

        # Filter only relevant years' data
        program_data = program_data[program_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # 1. Generate State Distribution JSON Data
        self.state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

        # calculate total
        total_contract_at_national_level = int(
            program_data["Total CRP - NUMBER OF CONTRACTS"].sum())
        total_farm_at_national_level = int(
            program_data["Total CRP - NUMBER OF FARMS"].sum())
        total_acre_at_national_level = int(
            program_data["Total CRP - ACRES"].sum())
        total_rental_1k_at_national_level = int(
            program_data["Total CRP - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        total_rental_acre_at_national_level = round(
            program_data["Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # general sign up
        total_general_signup_contract_at_national_level = int(
            program_data["Total General Sign-Up - NUMBER OF CONTRACTS"].sum())
        total_general_signup_farm_at_national_level = int(
            program_data["Total General Sign-Up - NUMBER OF FARMS"].sum())
        total_general_signup_acre_at_national_level = int(
            program_data["Total General Sign-Up - ACRES"].sum())
        total_general_signup_rental_1k_at_national_level = int(
            program_data["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        total_general_signup_rental_acre_at_national_level = round(
            program_data["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # continuous data
        total_continuous_contract_at_national_level = int(
            program_data["Total Continuous - NUMBER OF CONTRACTS"].sum())
        total_continuous_farm_at_national_level = int(
            program_data["Total Continuous - NUMBER OF FARMS"].sum())
        total_continuous_acre_at_national_level = int(
            program_data["Total Continuous - ACRES"].sum())
        total_continuous_rental_1k_at_national_level = int(
            program_data["Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        total_continuous_rental_acre_at_national_level = round(
            program_data["Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # crep only data
        crep_only_contract_at_national_level = int(
            program_data["CREP Only - NUMBER OF CONTRACTS"].sum())
        crep_only_farm_at_national_level = int(
            program_data["CREP Only - NUMBER OF FARMS"].sum())
        crep_only_acre_at_national_level = int(
            program_data["CREP Only - ACRES"].sum())
        crep_only_rental_1k_at_national_level = int(
            program_data["CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        crep_only_rental_acre_at_national_level = round(
            program_data["CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # non-crep data
        non_crep_contract_at_national_level = int(
            program_data["Continuous Non-CREP - NUMBER OF CONTRACTS"].sum())
        non_crep_farm_at_national_level = int(
            program_data["Continuous Non-CREP - NUMBER OF FARMS"].sum())
        non_crep_acre_at_national_level = int(
            program_data["Continuous Non-CREP - ACRES"].sum())
        non_crep_rental_1k_at_national_level = int(
            program_data["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        non_crep_rental_acre_at_national_level = round(
            program_data["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # farmable wetland data
        wetland_contract_at_national_level = int(
            program_data["Farmable Wetland - NUMBER OF CONTRACTS"].sum())
        wetland_farm_at_national_level = int(
            program_data["Farmable Wetland - NUMBER OF FARMS"].sum())
        wetland_acre_at_national_level = int(
            program_data["Farmable Wetland - ACRES"].sum())
        wetland_rental_1k_at_national_level = int(
            program_data["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        wetland_rental_acre_at_national_level = round(
            program_data["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # grassland data
        grassland_contract_at_national_level = int(
            program_data["Grassland - NUMBER OF CONTRACTS"].sum())
        grassland_farm_at_national_level = int(
            program_data["Grassland - NUMBER OF FARMS"].sum())
        grassland_acre_at_national_level = int(
            program_data["Grassland - ACRES"].sum())
        grassland_rental_1k_at_national_level = int(
            program_data["Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum())
        grassland_rental_acre_at_national_level = round(
            program_data["Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # Group total data by state, then sum
        total_by_contract_by_state = \
            program_data[
                ["year", "state", "Total CRP - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Total CRP - NUMBER OF CONTRACTS"].sum()

        total_by_farm_by_state = \
            program_data[
                ["year", "state", "Total CRP - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Total CRP - NUMBER OF FARMS"].sum()

        total_by_acre_by_state = \
            program_data[
                ["year", "state", "Total CRP - ACRES"]
            ].groupby(
                ["state"]
            )["Total CRP - ACRES"].sum()

        total_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Total CRP - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Total CRP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        total_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group General Sign-up data by state, then sum
        general_signup_by_contract_by_state = \
            program_data[
                ["year", "state", "Total General Sign-Up - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Total General Sign-Up - NUMBER OF CONTRACTS"].sum()

        general_signup_by_farm_by_state = \
            program_data[
                ["year", "state", "Total General Sign-Up - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Total General Sign-Up - NUMBER OF FARMS"].sum()

        general_signup_by_acre_by_state = \
            program_data[
                ["year", "state", "Total General Sign-Up - ACRES"]
            ].groupby(
                ["state"]
            )["Total General Sign-Up - ACRES"].sum()

        general_signup_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        general_signup_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous data by state and year, then sum
        continuous_by_contract_by_state = \
            program_data[
                ["year", "state", "Total Continuous - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Total Continuous - NUMBER OF CONTRACTS"].sum()

        continuous_by_farm_by_state = \
            program_data[
                ["year", "state", "Total Continuous - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Total Continuous - NUMBER OF FARMS"].sum()

        continuous_by_acre_by_state = \
            program_data[
                ["year", "state", "Total Continuous - ACRES"]
            ].groupby(
                ["state"]
            )["Total Continuous - ACRES"].sum()

        continuous_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        continuous_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group crep only data by state and year, then sum
        crep_only_by_contract_by_state = \
            program_data[
                ["year", "state", "CREP Only - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["CREP Only - NUMBER OF CONTRACTS"].sum()

        crep_only_by_farm_by_state = \
            program_data[
                ["year", "state", "CREP Only - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["CREP Only - NUMBER OF FARMS"].sum()

        crep_only_by_acre_by_state = \
            program_data[
                ["year", "state", "CREP Only - ACRES"]
            ].groupby(
                ["state"]
            )["CREP Only - ACRES"].sum()

        crep_only_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        crep_only_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous non-crep data by state and year, then sum
        non_crep_by_contract_by_state = \
            program_data[
                ["year", "state", "Continuous Non-CREP - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Continuous Non-CREP - NUMBER OF CONTRACTS"].sum()

        non_crep_by_farm_by_state = \
            program_data[
                ["year", "state", "Continuous Non-CREP - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Continuous Non-CREP - NUMBER OF FARMS"].sum()

        non_crep_by_acre_by_state = \
            program_data[
                ["year", "state", "Continuous Non-CREP - ACRES"]
            ].groupby(
                ["state"]
            )["Continuous Non-CREP - ACRES"].sum()

        non_crep_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        non_crep_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group farmable wetland data by state and year, then sum
        wetland_by_contract_by_state = \
            program_data[
                ["year", "state", "Farmable Wetland - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Farmable Wetland - NUMBER OF CONTRACTS"].sum()

        wetland_by_farm_by_state = \
            program_data[
                ["year", "state", "Farmable Wetland - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Farmable Wetland - NUMBER OF FARMS"].sum()

        wetland_by_acre_by_state = \
            program_data[
                ["year", "state", "Farmable Wetland - ACRES"]
            ].groupby(
                ["state"]
            )["Farmable Wetland - ACRES"].sum()

        wetland_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        wetland_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group grassland data by state and year, then sum
        grassland_by_contract_by_state = \
            program_data[
                ["year", "state", "Grassland - NUMBER OF CONTRACTS"]
            ].groupby(
                ["state"]
            )["Grassland - NUMBER OF CONTRACTS"].sum()

        grassland_by_farm_by_state = \
            program_data[
                ["year", "state", "Grassland - NUMBER OF FARMS"]
            ].groupby(
                ["state"]
            )["Grassland - NUMBER OF FARMS"].sum()

        grassland_by_acre_by_state = \
            program_data[
                ["year", "state", "Grassland - ACRES"]
            ].groupby(
                ["state"]
            )["Grassland - ACRES"].sum()

        grassland_by_rental_1k_by_state = \
            program_data[
                ["year", "state", "Grassland - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["state"]
            )["Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        grassland_by_rental_acre_by_state = \
            program_data[
                ["year", "state", "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["state"]
            )["Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        for state in self.us_state_abbreviations:
            new_data_entry = {
                "state": state,
                "programs": [
                    {
                        "programName": "Total CRP",
                        "totalContracts": int(total_by_contract_by_state[state].item()),
                        "totalFarms": int(total_by_farm_by_state[state].item()),
                        "totalAcre": int(total_by_acre_by_state[state].item()),
                        "paymentInDollars": int(total_by_rental_1k_by_state[state].item()) * 1000,
                        "paymentInAcre": round(total_by_rental_acre_by_state[state].item(), 2),
                        "contractInPercentageNationwide": round(
                            (total_by_contract_by_state[state].item() /
                             total_contract_at_national_level) * 100, 2),
                        "farmInPercentageNationwide": round(
                            (total_by_farm_by_state[state].item() /
                             total_farm_at_national_level) * 100, 2),
                        "acreInPercentageNationwide": round(
                            (total_by_acre_by_state[state].item() /
                             total_acre_at_national_level) * 100, 2),
                        "paymentInPercentageNationwide": round(
                            (total_by_rental_1k_by_state[state].item() /
                             total_rental_1k_at_national_level) * 100, 2),
                        "paymentInAcreInPercentageNationwide": round(
                            (total_by_rental_acre_by_state[state].item() /
                             total_rental_acre_at_national_level) * 100, 2),
                        "subPrograms": []
                    },
                    {
                        "programName": "Total General Sign-Up",
                        "totalContracts": int(general_signup_by_contract_by_state[state].item()),
                        "totalFarms": int(general_signup_by_farm_by_state[state].item()),
                        "totalAcre": int(general_signup_by_acre_by_state[state].item()),
                        "paymentInDollars": int(general_signup_by_rental_1k_by_state[state].item()) * 1000,
                        "paymentInAcre": round(general_signup_by_rental_acre_by_state[state].item(), 2),
                        "contractInPercentageNationwide": round(
                            (general_signup_by_contract_by_state[state].item() /
                             total_general_signup_contract_at_national_level) * 100, 2),
                        "farmInPercentageNationwide": round(
                            (general_signup_by_farm_by_state[state].item() /
                             total_general_signup_farm_at_national_level) * 100, 2),
                        "acreInPercentageNationwide": round(
                            (general_signup_by_acre_by_state[state].item() /
                             total_general_signup_acre_at_national_level) * 100, 2),
                        "paymentInPercentageNationwide": round(
                            (general_signup_by_rental_1k_by_state[state].item() /
                             total_general_signup_rental_1k_at_national_level) * 100, 2),
                        "paymentInAcreInPercentageNationwide": round(
                            (general_signup_by_rental_acre_by_state[state].item() /
                             total_general_signup_rental_acre_at_national_level) * 100, 2),
                        "subPrograms": []
                    },
                    {
                        "programName": "Total Continuous Sign-Up",
                        "totalContracts": int(continuous_by_contract_by_state[state].item()),
                        "totalFarms": int(continuous_by_farm_by_state[state].item()),
                        "totalAcre": int(continuous_by_acre_by_state[state].item()),
                        "paymentInDollars": int(continuous_by_rental_1k_by_state[state].item()) * 1000,
                        "paymentInAcre": round(continuous_by_rental_acre_by_state[state].item(), 2),
                        "contractInPercentageNationwide": round(
                            (continuous_by_contract_by_state[state].item() /
                             total_continuous_contract_at_national_level) * 100, 2),
                        "farmInPercentageNationwide": round(
                            (continuous_by_farm_by_state[state].item() /
                             total_continuous_farm_at_national_level) * 100, 2),
                        "acreInPercentageNationwide": round(
                            (continuous_by_acre_by_state[state].item() /
                             total_continuous_acre_at_national_level) * 100, 2),
                        "paymentInPercentageNationwide": round(
                            (continuous_by_rental_1k_by_state[state].item() /
                             total_continuous_rental_1k_at_national_level) * 100, 2),
                        "paymentInAcreInPercentageNationwide": round(
                            (continuous_by_rental_acre_by_state[state].item() /
                             total_continuous_rental_acre_at_national_level) * 100, 2),
                        "subPrograms": [
                            {
                                "programName": "CREP Only",
                                "totalContracts": int(crep_only_by_contract_by_state[state].item()),
                                "totalFarms": int(crep_only_by_farm_by_state[state].item()),
                                "totalAcre": int(crep_only_by_acre_by_state[state].item()),
                                "paymentInDollars": int(crep_only_by_rental_1k_by_state[state].item()) * 1000,
                                "paymentInAcre": round(crep_only_by_rental_acre_by_state[state].item(), 2),
                                "contractInPercentageNationwide": round(
                                    (crep_only_by_contract_by_state[state].item() /
                                     crep_only_contract_at_national_level) * 100, 2),
                                "farmInPercentageNationwide": round(
                                    (crep_only_by_farm_by_state[state].item() /
                                     crep_only_farm_at_national_level) * 100, 2),
                                "acreInPercentageNationwide": round(
                                    (crep_only_by_acre_by_state[state].item() /
                                     crep_only_acre_at_national_level) * 100, 2),
                                "paymentInPercentageNationwide": round(
                                    (crep_only_by_rental_1k_by_state[state].item() /
                                     crep_only_rental_1k_at_national_level) * 100, 2),
                                "paymentInAcreInPercentageNationwide": round(
                                    (crep_only_by_rental_acre_by_state[state].item() /
                                     crep_only_rental_acre_at_national_level) * 100, 2),
                            },
                            {
                                "programName": "Continuous Non-CREP",
                                "totalContracts": int(non_crep_by_contract_by_state[state].item()),
                                "totalFarms": int(non_crep_by_farm_by_state[state].item()),
                                "totalAcre": int(non_crep_by_acre_by_state[state].item()),
                                "paymentInDollars": int(non_crep_by_rental_1k_by_state[state].item()) * 1000,
                                "paymentInAcre": round(non_crep_by_rental_acre_by_state[state].item(), 2),
                                "contractInPercentageNationwide": round(
                                    (non_crep_by_contract_by_state[state].item() /
                                     non_crep_contract_at_national_level) * 100, 2),
                                "farmInPercentageNationwide": round(
                                    (non_crep_by_farm_by_state[state].item() /
                                     non_crep_farm_at_national_level) * 100, 2),
                                "acreInPercentageNationwide": round(
                                    (non_crep_by_acre_by_state[state].item() /
                                     non_crep_acre_at_national_level) * 100, 2),
                                "paymentInPercentageNationwide": round(
                                    (non_crep_by_rental_1k_by_state[state].item() /
                                     non_crep_rental_1k_at_national_level) * 100, 2),
                                "paymentInAcreInPercentageNationwide": round(
                                    (non_crep_by_rental_acre_by_state[state].item() /
                                     non_crep_rental_acre_at_national_level) * 100, 2),
                            },
                            {
                                "programName": "Farmable Wetland",
                                "totalContracts": int(wetland_by_contract_by_state[state].item()),
                                "totalFarms": int(wetland_by_farm_by_state[state].item()),
                                "totalAcre": int(wetland_by_acre_by_state[state].item()),
                                "paymentInDollars": int(wetland_by_rental_1k_by_state[state].item()) * 1000,
                                "paymentInAcre": round(wetland_by_rental_acre_by_state[state].item(), 2),
                                "contractInPercentageNationwide": round(
                                    (wetland_by_contract_by_state[state].item() /
                                     wetland_contract_at_national_level) * 100, 2),
                                "farmInPercentageNationwide": round(
                                    (wetland_by_farm_by_state[state].item() /
                                     wetland_farm_at_national_level) * 100, 2),
                                "acreInPercentageNationwide": round(
                                    (wetland_by_acre_by_state[state].item() /
                                     wetland_acre_at_national_level) * 100, 2),
                                "paymentInPercentageNationwide": round(
                                    (wetland_by_rental_1k_by_state[state].item() /
                                     wetland_rental_1k_at_national_level) * 100, 2),
                                "paymentInAcreInPercentageNationwide": round(
                                    (wetland_by_rental_acre_by_state[state].item() /
                                     wetland_rental_acre_at_national_level) * 100, 2),
                            }
                        ]
                    },
                    {
                        "programName": "Grassland",
                        "totalContracts": int(grassland_by_contract_by_state[state].item()),
                        "totalFarms": int(grassland_by_farm_by_state[state].item()),
                        "totalAcre": int(grassland_by_acre_by_state[state].item()),
                        "paymentInDollars": int(grassland_by_rental_1k_by_state[state].item()) * 1000,
                        "paymentInAcre": round(grassland_by_rental_acre_by_state[state].item(), 2),
                        "contractInPercentageNationwide": round(
                            (grassland_by_contract_by_state[state].item() /
                             grassland_contract_at_national_level) * 100, 2),
                        "farmInPercentageNationwide": round(
                            (grassland_by_farm_by_state[state].item() /
                             grassland_farm_at_national_level) * 100, 2),
                        "acreInPercentageNationwide": round(
                            (grassland_by_acre_by_state[state].item() /
                             grassland_acre_at_national_level) * 100, 2),
                        "paymentInPercentageNationwide": round(
                            (grassland_by_rental_1k_by_state[state].item() /
                             grassland_rental_1k_at_national_level) * 100, 2),
                        "paymentInAcreInPercentageNationwide": round(
                            (grassland_by_rental_acre_by_state[state].item() /
                             grassland_rental_acre_at_national_level) * 100, 2),
                        "subPrograms": []
                    }
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
        with open(os.path.join(self.data_folder, "crp_state_distribution_data.json"),
                  "w") as output_json_file:
            output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=2))

        # 2. Generate Sub Programs Data

        # Group total
        total_by_contract = \
            program_data["Total CRP - NUMBER OF CONTRACTS"].sum()

        total_by_farm = \
            program_data["Total CRP - NUMBER OF FARMS"].sum()

        total_by_acre = \
            program_data["Total CRP - ACRES"].sum()

        total_by_rental_1k = \
            program_data["Total CRP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        total_by_rental_acre = \
            program_data["Total CRP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group general sign up
        general_signup_by_contract = \
            program_data["Total General Sign-Up - NUMBER OF CONTRACTS"].sum()

        general_signup_by_farm = \
            program_data["Total General Sign-Up - NUMBER OF FARMS"].sum()

        general_signup_by_acre = \
            program_data["Total General Sign-Up - ACRES"].sum()

        general_signup_by_rental_1k = \
            program_data["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        general_signup_by_rental_acre = \
            program_data["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous data
        continuous_by_contract = \
            program_data["Total Continuous - NUMBER OF CONTRACTS"].sum()

        continuous_by_farm = \
            program_data["Total Continuous - NUMBER OF FARMS"].sum()

        continuous_by_acre = \
            program_data["Total Continuous - ACRES"].sum()

        continuous_by_rental_1k = \
            program_data["Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        continuous_by_rental_acre = \
            program_data["Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group crep only data
        crep_only_by_contract = \
            program_data["CREP Only - NUMBER OF CONTRACTS"].sum()

        crep_only_by_farm = \
            program_data["CREP Only - NUMBER OF FARMS"].sum()

        crep_only_by_acre = \
            program_data["CREP Only - ACRES"].sum()

        crep_only_by_rental_1k = \
            program_data["CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        crep_only_by_rental_acre = \
            program_data["CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous non-crep data
        non_crep_by_contract = \
            program_data["Continuous Non-CREP - NUMBER OF CONTRACTS"].sum()

        non_crep_by_farm = \
            program_data["Continuous Non-CREP - NUMBER OF FARMS"].sum()

        non_crep_by_acre = \
            program_data["Continuous Non-CREP - ACRES"].sum()

        non_crep_by_rental_1k = \
            program_data["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        non_crep_by_rental_acre = \
            program_data["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group farmable wetland data
        wetland_by_contract = \
            program_data["Farmable Wetland - NUMBER OF CONTRACTS"].sum()

        wetland_by_farm = \
            program_data["Farmable Wetland - NUMBER OF FARMS"].sum()

        wetland_by_acre = \
            program_data["Farmable Wetland - ACRES"].sum()

        wetland_by_rental_1k = \
            program_data["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        wetland_by_rental_acre = \
            program_data["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group grassland data
        grassland_by_contract = \
            program_data["Grassland - NUMBER OF CONTRACTS"].sum()

        grassland_by_farm = \
            program_data["Grassland - NUMBER OF FARMS"].sum()

        grassland_by_acre = \
            program_data["Grassland - ACRES"].sum()

        grassland_by_rental_1k = \
            program_data["Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        grassland_by_rental_acre= \
            program_data["Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        self.program_data_dict = {
            "programs": [
                {
                    "programName": "Total CRP",
                    "totalContracts": int(total_by_contract.item()),
                    "totalFarms": int(total_by_farm.item()),
                    "totalAcre": int(total_by_acre.item()),
                    "paymentInDollars": int(total_by_rental_1k.item()) * 1000,
                    "paymentInAcre": round(total_by_rental_acre.item(), 2),
                    "subPrograms": []
                },
                {
                    "programName": "Total General Sign-Up",
                    "totalContracts": int(general_signup_by_contract.item()),
                    "totalFarms": int(general_signup_by_farm.item()),
                    "totalAcre": int(general_signup_by_acre.item()),
                    "paymentInDollars": int(general_signup_by_rental_1k.item()) * 1000,
                    "paymentInAcre": round(general_signup_by_rental_acre.item(), 2),
                    "subPrograms": []
                },
                {
                    "programName": "Total Continuous",
                    "totalContracts": int(continuous_by_contract.item()),
                    "totalFarms": int(continuous_by_farm.item()),
                    "totalAcre": int(continuous_by_acre.item()),
                    "paymentInDollars": int(continuous_by_rental_1k.item()) * 1000,
                    "paymentInAcre": round(continuous_by_rental_acre.item(), 2),
                    "subPrograms": [
                        {
                            "programName": "CREP Only",
                            "totalContracts": int(crep_only_by_contract.item()),
                            "totalFarms": int(crep_only_by_farm.item()),
                            "totalAcre": int(crep_only_by_acre.item()),
                            "paymentInDollars": int(crep_only_by_rental_1k.item()) * 1000,
                            "paymentInAcre": round(crep_only_by_rental_acre.item(), 2)
                        },
                        {
                            "programName": "Continuous Non-CREP",
                            "totalContracts": int(non_crep_by_contract.item()),
                            "totalFarms": int(non_crep_by_farm.item()),
                            "totalAcre": int(non_crep_by_acre.item()),
                            "paymentInDollars": int(non_crep_by_rental_1k.item()) * 1000,
                            "paymentInAcre": round(non_crep_by_rental_acre.item(), 2)
                        },
                        {
                            "programName": "Farmable Wetland",
                            "totalContracts": int(wetland_by_contract.item()),
                            "totalFarms": int(wetland_by_farm.item()),
                            "totalAcre": int(wetland_by_acre.item()),
                            "paymentInDollars": int(wetland_by_rental_1k.item()) * 1000,
                            "paymentInAcre": round(wetland_by_rental_acre.item(), 2)
                        }
                    ]
                },
                {
                    "programName": "Grassland",
                    "totalContracts": int(grassland_by_contract.item()),
                    "totalFarms": int(grassland_by_farm.item()),
                    "totalAcre": int(grassland_by_acre.item()),
                    "paymentInDollars": int(grassland_by_rental_1k.item()) * 1000,
                    "paymentInAcre": round(grassland_by_rental_acre.item(), 2),
                    "subPrograms": []
                },
            ]
        }

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "crp_subprograms_data.json"), "w") as output_json_file:
            output_json_file.write(json.dumps(self.program_data_dict, indent=2))
