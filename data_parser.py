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

        # Main program category specific file paths
        if self.program_main_category_name == "Title 1: Commodities":
            self.base_acres_csv_filepath = os.path.join(data_folder, kwargs["base_acres_csv_filename"])
            self.farm_payee_count_csv_filepath = os.path.join(data_folder, kwargs["farm_payee_count_csv_filename"])

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
                    "Dairy": ["Dairy Margin Coverage Program (DMC)", "Dairy Indemnity Payment Program (DIPP)"],
                    "Disaster Assistance": ["Tree Assistance Program (TAP)",
                                            "Noninsured Crop Disaster Assistance Program (NAP)",
                                            "Livestock Forage Disaster Program (LFP)",
                                            "Livestock Indemnity Program (LIP)",
                                            "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)"]
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
                    "fiscal_year": "year",
                    "category": "program_description",
                    "amount": "payments"
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
        program_data = pd.read_csv(self.program_csv_filepath)
        program_data = program_data.replace(self.metadata[self.program_main_category_name]["value_names_map"])

        # Rename column names to make it more uniform
        program_data.rename(columns=self.metadata[self.program_main_category_name]["column_names_map"], inplace=True)

        # Filter only relevant years' data
        program_data = program_data[program_data["year"].between(self.start_year, self.end_year,
                                                                 inclusive="both")]

        # Exclude programs that are not included at present
        program_data = program_data[
            (program_data["program_description"] != "Ad hoc or Supplemental") &
            (program_data["program_description"] != "Market Facilitation Program (MFP)") &
            (program_data["program_description"] != "Coronavirus Food Assistance Program (CFAP)")
            ]

        # Group data by state, program description, and payment
        payments_by_program_by_state_for_year = \
            program_data[
                ["year", "state", "program_description", "payments"]
            ].groupby(
                ["year", "state", "program_description"]
            )["payments"].sum()

        # Import base acres CSV file
        base_acres_data = pd.read_csv(self.base_acres_csv_filepath)
        base_acres_data = base_acres_data.replace({
            "ARC-CO": "Agriculture Risk Coverage County Option (ARC-CO)",
            "ARCCO": "Agriculture Risk Coverage County Option (ARC-CO)",
            "PLC": "Price Loss Coverage (PLC)",
        })

        # Rename column names to make it more uniform
        base_acres_data.rename(columns={"State Name": "state",
                                        "Year": "year",
                                        "Program": "program_description",
                                        "Enrolled Base": "base_acres"}, inplace=True)

        # Filter only relevant years' data
        # TODO: Excluded 2018 data for now. This needs to be revisited later.
        #  https://github.com/policy-design-lab/data-import/issues/35
        base_acres_data = base_acres_data[
            base_acres_data["year"].between(self.start_year + 1, self.end_year, inclusive="both")]

        # Import farmer count CSV file
        farm_payee_count_data = pd.read_csv(self.farm_payee_count_csv_filepath)
        farm_payee_count_data = farm_payee_count_data.replace({
            "AGRICULTURAL RISK COVERAGE - INDIVIDUAL": "Agriculture Risk Coverage Individual Coverage (ARC-IC)",
            "AGRICULTURAL RISK COVERAGE PROG - COUNTY": "Agriculture Risk Coverage County Option (ARC-CO)",
            "AGRICULTURAL RISK COVERAGE -COUNTY PILOT": "Agriculture Risk Coverage County Option (ARC-CO)",

            "PRICE LOSS COVERAGE PROGRAM": "Price Loss Coverage (PLC)",

            "DAIRY MARGIN COVERAGE PROGRAM": "Dairy Margin Coverage Program (DMC)",
            "DAIRY MARGIN COVERAGE": "Dairy Margin Coverage Program (DMC)",
            "MARGIN PROTECTION PROGRAM - DAIRY": "Dairy Margin Coverage Program (DMC)",
            "MARGIN PROTECTION  - DAIRY": "Dairy Margin Coverage Program (DMC)",

            "TREE ASSISTANCE PROGRAM": "Tree Assistance Program (TAP)",
            "TREE ASSISTANCE PROGRAM - PECAN": "Tree Assistance Program (TAP)",

            "LIVESTOCK FORAGE DISASTER  PROGRAM": "Livestock Forage Disaster Program (LFP)",
            "LIVESTOCK FORAGE DISASTER PROGRAM": "Livestock Forage Disaster Program (LFP)",
            "LIVESTOCK FORAGE PROGRAM": "Livestock Forage Disaster Program (LFP)",

            "LIVESTOCK INDEMNITY PROGRAM": "Livestock Indemnity Program (LIP)",

            "EMERGENCY ASSISTANCE LIVESTOCK, HONEYBEE, FISH":
                "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)",
            "EMERG ASSIST LIVESTOCK BEES FISH (ELAP)":
                "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)",
            "\"EMERGENCY ASSISTANCE LIVESTOCK, HONEYBEE, FISH\"":
                "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)",
        })

        # Rename column names to make it more uniform
        farm_payee_count_data.rename(columns={"State": "state",
                                              "Year": "year",
                                              "Program": "program_description",
                                              "Payee Count": "recipient_count"}, inplace=True)

        # Filter only relevant years' data
        # TODO: Excluded 2018 data for now. This needs to be revisited later.
        #  https://github.com/policy-design-lab/data-import/issues/35
        farm_payee_count_data = farm_payee_count_data[
            farm_payee_count_data["year"].between(self.start_year + 1, self.end_year, inclusive="both")]

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
                        {
                            "programName": "Dairy",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        },
                        {
                            "programName": "Disaster Assistance",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        }
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
            total_payments_by_program_by_state = program_data[
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
                        {
                            "programName": "Dairy",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        },
                        {
                            "programName": "Disaster Assistance",
                            "subPrograms": [
                            ],
                            "programPaymentInDollars": 0.0
                        }
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
            total_payments_by_state = program_data[
                ["state", "payments"]].groupby(["state"])["payments"].sum()

            total_payments_at_national_level = round(program_data["payments"].sum(), 2)

            total_payments_by_program_by_state = program_data[
                ["state", "program_description", "payments"]].groupby(
                ["state", "program_description"]
            )["payments"].sum()

            total_payments_by_program_at_national_level = round(
                program_data[["program_description", "payments"]].groupby(["program_description"]).sum(), 2)

            average_base_acres_by_program_by_state = base_acres_data[
                ["state", "program_description", "base_acres", "year"]].groupby(
                ["state", "program_description", "year"]
            )["base_acres"].sum().groupby(["state", "program_description"]).mean()

            average_payee_count_by_program_by_state = farm_payee_count_data[
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
                        {
                            "programName": "Dairy",
                            "programPaymentInDollars": 0.0,
                            "averageAreaInAcres": 0.0,
                            "averageRecipientCount": 0,
                            "subPrograms": [
                            ]
                        },
                        {
                            "programName": "Disaster Assistance",
                            "programPaymentInDollars": 0.0,
                            "averageAreaInAcres": 0.0,
                            "averageRecipientCount": 0,
                            "subPrograms": [
                            ]
                        }
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
                    program_percentage_within_state = round(
                        (rounded_program_payment / total_payments_by_state[state_name]) * 100, 2)

                    if program_description in average_base_acres_series:
                        average_base_acres = round(average_base_acres_series[program_description], 2)
                    else:
                        average_base_acres = 0.0

                    if program_description in average_payee_count_series:
                        average_recipient_count = int(average_payee_count_series[program_description])
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
                    {
                        "programName": "Dairy",
                        "subPrograms": [
                        ]
                    },
                    {
                        "programName": "Disaster Assistance",
                        "subPrograms": [
                        ]
                    }
                ]
            }
            total_for_program = {
                "Agriculture Risk Coverage (ARC)": 0.0,
                "Price Loss Coverage (PLC)": 0.0,
                "Dairy": 0.0,
                "Disaster Assistance": 0.0
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
