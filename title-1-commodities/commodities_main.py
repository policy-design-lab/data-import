import pandas as pd
import json

from operator import itemgetter, attrgetter
from deepmerge import always_merger


class CommoditiesDataParser:
    def __init__(self, start_year, end_year, csv_filepath):
        self.start_year = start_year
        self.end_year = end_year
        self.csv_filepath = csv_filepath

        self.programs_subprograms_mapping = {
            "Agriculture Risk Coverage (ARC)": ["Agriculture Risk Coverage County Option (ARC-CO)",
                                                "Agriculture Risk Coverage Individual Coverage (ARC-IC)"],
            "Price Loss Coverage (PLC)": [],
            "Dairy Margin Coverage Program (DMC)": [],
            "Disaster Assistance": ["Tree Assistance Program (TAP)",
                                    "Noninsured Crop Disaster Assistance Program (NAP)",
                                    "Livestock Forage Disaster Program (LFP)", "Livestock Indemnity Program (LIP)",
                                    "Emergency Assistance for Livestock, Honeybees, and Farm-Raised Fish (ELAP)"]
        }

        self.processed_data_dict = dict()
        self.state_distribution_data_dict = dict()
        self.programs_data_dict = dict()

    def find_program_by_subprogram(self, program_description):
        for program_name in self.programs_subprograms_mapping:
            if program_description == program_name and len(self.programs_subprograms_mapping[program_name]) == 0:
                return program_name
            if program_description in self.programs_subprograms_mapping[program_name]:
                return program_name

    def find_and_get_zero_subprogram_entries(self, program_name, practice_categories_list,
                                             for_percentage_json=False):
        diff_list = list(set(self.programs_subprograms_mapping[program_name]) - set(practice_categories_list))
        zero_subprogram_entries = []
        for entry in diff_list:
            if not for_percentage_json:
                zero_subprogram_entries.append({
                    "subProgramName": entry,
                    "paymentInDollars": 0
                })
            else:
                zero_subprogram_entries.append({
                    "subProgramName": entry,
                    "paymentInDollars": 0.0,
                    "paymentInPercentageWithinState": 0.00
                })
        return zero_subprogram_entries

    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        commodities_data = pd.read_csv(self.csv_filepath)
        commodities_data = commodities_data.replace({
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
                "Emergency Assistance for Livestock,Honeybees, and Farm-Raised Fish (ELAP)",
            "EMERG ASSIST LIVESTOCK BEES FISH (ELAP)":
                "Emergency Assistance for Livestock,Honeybees, and Farm-Raised Fish (ELAP)",
            "\"EMERGENCY ASSISTANCE LIVESTOCK, HONEYBEE, FISH\"":
                "Emergency Assistance for Livestock,Honeybees, and Farm-Raised Fish (ELAP)",
        })

        # Rename column names to make it more uniform
        commodities_data.rename(columns={"fiscal_year": "pay_year",
                                         "accounting_program_description": "program_description",
                                         "amount": "payments"}, inplace=True)

        # Filter only relevant years' data
        commodities_data = commodities_data[commodities_data["pay_year"].between(self.start_year, self.end_year,
                                                                                 inclusive="both")]

        # Group data by state, program description, and payment
        payments_by_program_by_state_for_year = \
            commodities_data[
                ["pay_year", "state", "program_description", "payments"]
            ].groupby(
                ["pay_year", "state", "program_description"]
            )["payments"].sum()

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
                            "programName": "Dairy Margin Coverage Program (DMC)",
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
                        if len(self.programs_subprograms_mapping[program_subprogram_name]) == 0:
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
            total_payments_by_program_by_state = commodities_data[
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
                            "programName": "Dairy Margin Coverage Program (DMC)",
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
                        if len(self.programs_subprograms_mapping[program_subprogram_name]) == 0:
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
            with open("commodities_map_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(self.processed_data_dict, indent=2))

        # 2. Generate state distribution data
        if True:
            total_payments_by_state = commodities_data[
                ["state", "payments"]].groupby(["state"])["payments"].sum()
            total_payments_at_national_level = round(commodities_data["payments"].sum(), 2)

            total_payments_by_program_by_state = commodities_data[
                ["state", "program_description", "payments"]].groupby(
                ["state", "program_description"]
            )["payments"].sum()

            total_payments_by_program_at_national_level = round(
                commodities_data[["program_description", "payments"]].groupby(["program_description"]).sum(), 2)

            # Iterate through all tuples
            for state_name, payment in total_payments_by_state.items():
                yearly_state_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "programs": [
                        {
                            "programName": "Agriculture Risk Coverage (ARC)",
                            "programPaymentInDollars": 0.0,
                            "subPrograms": [
                            ],
                        },
                        {
                            "programName": "Price Loss Coverage (PLC)",
                            "programPaymentInDollars": 0.0,
                            "subPrograms": [
                            ]
                        },
                        {
                            "programName": "Dairy Margin Coverage Program (DMC)",
                            "programPaymentInDollars": 0.0,
                            "subPrograms": [
                            ]
                        },
                        {
                            "programName": "Disaster Assistance",
                            "programPaymentInDollars": 0.0,
                            "subPrograms": [
                            ]
                        }
                    ],
                    "totalPaymentInPercentageNationwide": round(
                        (yearly_state_payment / total_payments_at_national_level) * 100, 2),
                    "totalPaymentInDollars": yearly_state_payment
                }

                for data_tuple, program_payment in total_payments_by_program_by_state.items():
                    data_tuple_state_name, program_description = data_tuple

                    if data_tuple_state_name == state_name:
                        program_payment = round(program_payment, 2)
                        program_percentage_nationwide = round(
                            (program_payment / total_payments_by_program_at_national_level["payments"][
                                program_description]) * 100, 2)
                        program_percentage_within_state = round(
                            (program_payment / total_payments_by_state[data_tuple_state_name]) * 100, 2)
                        program_subprogram_name = self.find_program_by_subprogram(program_description)

                        for program in new_data_entry["programs"]:
                            if program["programName"] == program_subprogram_name:
                                if len(self.programs_subprograms_mapping[program_subprogram_name]) == 0:
                                    pass
                                else:
                                    program["subPrograms"].append({
                                        "subProgramName": program_description,
                                        "paymentInDollars": program_payment,
                                        "paymentInPercentageNationwide": program_percentage_nationwide,
                                        "paymentInPercentageWithinState": program_percentage_within_state
                                    })
                                program["programPaymentInDollars"] += program_payment

                self.state_distribution_data_dict[state_name] = [new_data_entry]

            # Add zero entries
            for state_name in self.state_distribution_data_dict:
                for year_data in self.state_distribution_data_dict[state_name]:
                    for program in year_data["programs"]:
                        # Round programPaymentInDollars
                        program["programPaymentInDollars"] = round(program["programPaymentInDollars"], 2)
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

            # Sort states by decreasing order of percentages
            self.state_distribution_data_dict = dict(sorted(self.state_distribution_data_dict.items(),
                                                            key=lambda x: x[1][0]["totalPaymentInPercentageNationwide"],
                                                            reverse=True))

            # Write processed_data_dict as JSON data
            with open("commodities_state_distribution_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(self.state_distribution_data_dict, indent=2))

        # 3. Generate practice categories data for the donut chart
        if True:
            programs_data = {
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
                        "programName": "Dairy Margin Coverage Program (DMC)",
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
                "Dairy Margin Coverage Program (DMC)": 0.0,
                "Disaster Assistance": 0.0
            }

            for program in programs_data["programs"]:
                if len(self.programs_subprograms_mapping[program["programName"]]) > 0:
                    for program_subprogram_name in self.programs_subprograms_mapping[program["programName"]]:
                        if program_subprogram_name in total_payments_by_program_at_national_level["payments"]:
                            subprogram_payment = round(
                                total_payments_by_program_at_national_level["payments"][program_subprogram_name], 2)
                            entry_dict = {
                                "subProgramName": program_subprogram_name,
                                "totalPaymentInDollars": subprogram_payment,
                            }
                            total_for_program[program["programName"]] += subprogram_payment
                            program["subPrograms"].append(entry_dict)
                else:
                    program_subprogram_name = program["programName"]
                    if program_subprogram_name in total_payments_by_program_at_national_level["payments"]:
                        subprogram_payment = round(
                            total_payments_by_program_at_national_level["payments"][program_subprogram_name], 2)
                        total_for_program[program["programName"]] += subprogram_payment

            for program in programs_data["programs"]:
                for subprogram in program["subPrograms"]:
                    subprogram["totalPaymentInPercentage"] = round(
                        subprogram["totalPaymentInDollars"] / total_for_program[
                            program["programName"]] * 100, 2)
                program["totalPaymentInDollars"] = round(total_for_program[program["programName"]], 2)
                program["totalPaymentInPercentage"] = round(
                    total_for_program[program["programName"]] / total_payments_at_national_level * 100, 2)
                program["subPrograms"].sort(key=lambda x: x["totalPaymentInPercentage"], reverse=True)

            # Write processed_data_dict as JSON data
            with open("commodities_subprograms_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(programs_data, indent=2))

        print(payments_by_program_by_state_for_year)


if __name__ == '__main__':
    commodities_data_parser = CommoditiesDataParser(2018, 2022, "filtered_commodities_by_program.csv")
    commodities_data_parser.parse_and_process()
