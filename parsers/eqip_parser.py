import pandas as pd
import json

from operator import itemgetter, attrgetter
from deepmerge import always_merger
from datetime import datetime


class EqipParser:
    def __init__(self, start_year, end_year, summary_filepath, all_programs_filepath, csv_filepath):

        self.summary_filepath = summary_filepath
        self.all_programs_filepath = all_programs_filepath
        self.start_year = start_year
        self.end_year = end_year
        self.csv_filepath = csv_filepath

        self.practices_category_dict = {
            "(6)(A) Practices": ["Structural", "Land management", "Vegetative", "Forest management",
                                 "Soil testing", "Soil remediation", "Other improvement"],
            "(6)(B) Practices": ["Comprehensive Nutrient Mgt.", "Resource-conserving crop rotation",
                                 "Soil health", "Conservation planning assessment", "Other planning"]
        }

        self.processed_data_dict = dict()
        self.percentages_data_dict = dict()
        self.statute_performance_data_dict = dict()

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

        # Load JSON files
        with open(self.summary_filepath) as summary_file:
            self.summary_file_dict = json.load(summary_file)

        with open(self.all_programs_filepath) as all_programs_file:
            self.all_programs__dict = json.load(all_programs_file)

    def find_statute_by_category(self, category_name):
        for statute_name in self.practices_category_dict:
            if category_name in self.practices_category_dict[statute_name]:
                return statute_name

    def find_and_get_zero_practice_category_entries(self, statue_name, practice_categories_list,
                                                    for_percentage_json=False):
        diff_list = list(set(self.practices_category_dict[statue_name]) - set(practice_categories_list))
        zero_practice_category_entries = []
        for entry in diff_list:
            if not for_percentage_json:
                zero_practice_category_entries.append({
                    "practiceCategoryName": entry,
                    "paymentInDollars": 0
                })
            else:
                zero_practice_category_entries.append({
                    "practiceCategoryName": entry,
                    "paymentInDollars": 0.0,
                    "paymentInPercentageWithinState": 0.00

                })
        return zero_practice_category_entries

    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        eqip_data = pd.read_csv(self.csv_filepath)
        eqip_data = eqip_data.replace({
            "Other 1 - planning": "Other planning",
            "Other 2 - improvement": "Other improvement",
            "Conservating planning assessment": "Conservation planning assessment",
            "Resource-conserving crop rotatation": "Resource-conserving crop rotation",
            "Land Management": "Land management",
            "Forest Management": "Land management",
            "Soil Remediation": "Soil remediation"
        })

        # Filter only relevant years' data
        eqip_data = eqip_data[eqip_data["Pay_year"].between(self.start_year, self.end_year, inclusive="both")]

        # Group data by state, practice category name, and payment
        payments_by_category_by_state_for_year = \
            eqip_data[
                ["Pay_year", "State", "category_name", "payments"]
            ].groupby(
                ["Pay_year", "State", "category_name"]
            )["payments"].sum()

        # 1. Get data for the map
        if True:
            # Iterate through all tuples
            for data_tuple, payment in payments_by_category_by_state_for_year.items():
                year, state_name, category_name = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(year),
                    "statutes": [
                        {
                            "statuteName": "(6)(A) Practices",
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "(6)(B) Practices",
                            "practiceCategories": [
                            ]
                        }
                    ]
                }

                # Get statute name and updated category name
                statute_name = self.find_statute_by_category(category_name)

                for statute in new_data_entry["statutes"]:
                    if statute["statuteName"] == statute_name:
                        statute["practiceCategories"].append({
                            "practiceCategoryName": category_name,
                            "paymentInDollars": rounded_payment
                        })

                # Update self.processed_data_dict
                if state_name not in self.processed_data_dict:
                    self.processed_data_dict[state_name] = [new_data_entry]
                elif self.processed_data_dict[state_name] is not None:
                    found = False
                    for entry in self.processed_data_dict[state_name]:
                        if entry["years"] == new_data_entry["years"]:
                            for entry_statute in entry["statutes"]:
                                for new_data_entry_statute in new_data_entry["statutes"]:
                                    if entry_statute["statuteName"] == new_data_entry_statute["statuteName"]:
                                        # Merge existing entries and new data entries
                                        entry_statute["practiceCategories"] = always_merger.merge(
                                            entry_statute["practiceCategories"],
                                            new_data_entry_statute["practiceCategories"])
                            found = True
                            break
                    if not found:
                        self.processed_data_dict[state_name].append(new_data_entry)

            # Get total payment data
            total_payments_by_category_by_state = eqip_data[
                ["State", "category_name", "payments"]].groupby(
                ["State", "category_name"]
            )["payments"].sum()

            # Iterate through all tuples
            for data_tuple, payment in total_payments_by_category_by_state.items():
                state_name, category_name = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "statutes": [
                        {
                            "statuteName": "(6)(A) Practices",
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "(6)(B) Practices",
                            "practiceCategories": [
                            ]
                        }
                    ]
                }

                # Get statute name and updated category name
                statute_name = self.find_statute_by_category(category_name)

                for statute in new_data_entry["statutes"]:
                    if statute["statuteName"] == statute_name:
                        statute["practiceCategories"].append({
                            "practiceCategoryName": category_name,
                            "paymentInDollars": rounded_payment
                        })

                found = False
                # Update self.processed_data_dict
                for entry in self.processed_data_dict[state_name]:
                    if entry["years"] == new_data_entry["years"]:
                        for entry_statute in entry["statutes"]:
                            for new_data_entry_statute in new_data_entry["statutes"]:
                                if entry_statute["statuteName"] == new_data_entry_statute["statuteName"]:
                                    entry_statute["practiceCategories"] = always_merger.merge(
                                        entry_statute["practiceCategories"],
                                        new_data_entry_statute["practiceCategories"])
                        found = True
                        break
                if not found:
                    self.processed_data_dict[state_name].append(new_data_entry)

            # Calculate total for each state and update data dictionary
            for state_name in self.processed_data_dict:
                for year_data in self.processed_data_dict[state_name]:
                    total_payment_in_dollars = 0
                    for statute in year_data["statutes"]:
                        for statute_category in statute["practiceCategories"]:
                            total_payment_in_dollars += statute_category["paymentInDollars"]
                    year_data["totalPaymentInDollars"] = round(total_payment_in_dollars, 2)

            # Add zero entries
            for state_name in self.processed_data_dict:
                for year_data in self.processed_data_dict[state_name]:
                    for statute in year_data["statutes"]:
                        statue_categories_list = []
                        for statute_category in statute["practiceCategories"]:
                            statue_categories_list.append(statute_category["practiceCategoryName"])
                        zero_entries = self.find_and_get_zero_practice_category_entries(statute["statuteName"],
                                                                                        statue_categories_list)
                        statute["practiceCategories"] = always_merger.merge(
                            statute["practiceCategories"], zero_entries)

                        # Sort categories by name
                        statute["practiceCategories"].sort(key=lambda x: x["practiceCategoryName"])

            # remap state names to abbreviations
            self.processed_data_dict = self.remap_state_name_to_abbreviation(self.processed_data_dict)

            # add year to the data
            tmp_output = dict()
            tmp_output[str(self.start_year) + "-" + str(self.end_year)] = []

            # add year to the tmp_output
            tmp_output[str(self.start_year) + "-" + str(self.end_year)].append(self.processed_data_dict)

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/eqip/eqip_map_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(tmp_output, indent=2))

        # 2. Get data for the table
        if True:
            total_payments_by_state = eqip_data[
                ["State", "payments"]].groupby(["State"])["payments"].sum()
            total_payments_at_national_level = round(eqip_data["payments"].sum(), 2)

            total_payments_by_category_by_state = eqip_data[
                ["State", "category_name", "payments"]].groupby(
                ["State", "category_name"]
            )["payments"].sum()

            total_payments_by_category_at_national_level = round(
                eqip_data[["category_name", "payments"]].groupby(["category_name"]).sum(), 2)

            # Iterate through all tuples
            for state_name, payment in total_payments_by_state.items():
                yearly_state_payment = round(payment, 2)

                new_data_entry = {
                    "state": state_name,
                    "statutes": [
                        {
                            "statuteName": "(6)(A) Practices",
                            "statutePaymentInDollars": 0.0,
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "(6)(B) Practices",
                            "statutePaymentInDollars": 0.0,
                            "practiceCategories": [
                            ]
                        }
                    ],
                    "totalPaymentInPercentageNationwide": round(
                        (yearly_state_payment / total_payments_at_national_level) * 100, 2),
                    "totalPaymentInDollars": yearly_state_payment
                }

                for data_tuple, category_payment in total_payments_by_category_by_state.items():
                    data_tuple_state_name, category_name = data_tuple

                    if data_tuple_state_name == state_name:
                        category_payment = round(category_payment, 2)
                        category_percentage_nationwide = round(
                            (category_payment / total_payments_by_category_at_national_level["payments"][
                                category_name]) * 100, 2)
                        category_percentage_within_state = round(
                            (category_payment / total_payments_by_state[data_tuple_state_name]) * 100, 2)
                        statute_name = self.find_statute_by_category(category_name)

                        for statute in new_data_entry["statutes"]:
                            if statute["statuteName"] == statute_name:
                                statute["practiceCategories"].append({
                                    "practiceCategoryName": category_name,
                                    "paymentInDollars": category_payment,
                                    "paymentInPercentageNationwide": category_percentage_nationwide,
                                    "paymentInPercentageWithinState": category_percentage_within_state
                                })
                                statute["statutePaymentInDollars"] += category_payment

                self.percentages_data_dict[state_name] = [new_data_entry]

            # Add zero entries
            for state_name in self.percentages_data_dict:
                for year_data in self.percentages_data_dict[state_name]:
                    for statute in year_data["statutes"]:
                        # Round statutePaymentInDollars
                        statute["statutePaymentInDollars"] = round(statute["statutePaymentInDollars"], 2)
                        statue_categories_list = []
                        for statute_category in statute["practiceCategories"]:
                            statue_categories_list.append(statute_category["practiceCategoryName"])
                        zero_entries = self.find_and_get_zero_practice_category_entries(statute["statuteName"],
                                                                                        statue_categories_list, True)
                        statute["practiceCategories"] = always_merger.merge(
                            statute["practiceCategories"], zero_entries)

                        # Sort categories by percentages
                        statute["practiceCategories"].sort(reverse=True,
                                                           key=lambda x: x["paymentInPercentageWithinState"])

            # Sort states by decreasing order of percentages
            self.percentages_data_dict = dict(sorted(self.percentages_data_dict.items(),
                                                     key=lambda x: x[1][0]["totalPaymentInPercentageNationwide"],
                                                     reverse=True))

            # restructure json to equivalent to acep or rcpp
            restructured_list = []

            for state, state_data in self.percentages_data_dict.items():
                state_abbr = \
                    [abbr for abbr, name in self.us_state_abbreviation.items() if name == state][0]
                for entry in state_data:
                    restructured_list.append({
                        'state': state_abbr,
                        'statutes': entry['statutes']
                    })

            # add year to the data
            tmp_output = dict()
            tmp_output[str(self.start_year) + "-" + str(self.end_year)] = []

            # add year to the tmp_output
            tmp_output[str(self.start_year) + "-" + str(self.end_year)] = restructured_list

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/eqip/eqip_state_distribution_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(tmp_output, indent=2))

        # 3. Get data for the Semi-donut chart
        if True:
            statutes_data = {
                "statutes": [
                    {
                        "statuteName": "(6)(A) Practices",
                        "practiceCategories": [

                        ]
                    },
                    {
                        "statuteName": "(6)(B) Practices",
                        "practiceCategories": [

                        ]

                    }
                ]
            }
            total_for_statute = {
                "(6)(A) Practices": 0.0,
                "(6)(B) Practices": 0.0
            }

            for statute in statutes_data["statutes"]:
                for category_name in self.practices_category_dict[statute["statuteName"]]:
                    if category_name in total_payments_by_category_at_national_level["payments"]:
                        category_payment = round(total_payments_by_category_at_national_level["payments"][category_name], 2)
                        entry_dict = {
                            "practiceCategoryName": category_name,
                            "totalPaymentInDollars": category_payment,
                        }
                        total_for_statute[statute["statuteName"]] += category_payment
                    # When category is not existing in the actual data
                    else:
                        entry_dict = {
                            "practiceCategoryName": category_name,
                            "totalPaymentInDollars": 0.0,
                        }
                    statute["practiceCategories"].append(entry_dict)

            for statute in statutes_data["statutes"]:
                for practice_category in statute["practiceCategories"]:
                    practice_category["totalPaymentInPercentage"] = round(
                        practice_category["totalPaymentInDollars"] / total_for_statute[statute["statuteName"]] * 100, 2)
                statute["totalPaymentInDollars"] = total_for_statute[statute["statuteName"]]
                statute["totalPaymentInPercentage"] = round(
                    total_for_statute[statute["statuteName"]] / total_payments_at_national_level * 100, 2)
                statute["practiceCategories"].sort(key=lambda x: x["totalPaymentInPercentage"], reverse=True)

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/eqip/eqip_practice_categories_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(statutes_data, indent=4))

        # TODO: Remove the below block soon.
        # 4. Update summary JSON, all programs JSON and totals
        # if True:
        #     # Iterate through summary file dict
        #     for item in self.summary_file_dict:
        #         if item["Title"] == "Title II: Conservation":
        #             state_name = self.us_state_abbreviation[item["State"]]
        #             eqip_data_for_year_and_state = eqip_data[(eqip_data["State"] == state_name) & (eqip_data["Pay_year"] == item["Fiscal Year"])]
        #             item["Amount"] = int(eqip_data_for_year_and_state["payments"].sum())
        #
        #     # Iterate through all programs file dict
        #     for item in self.all_programs__dict:
        #         state_total = 0
        #         for year in range(self.start_year, self.end_year + 1):
        #             if "Title II " + str(year) in item:
        #                 if item["State"] != "Total":
        #                     state_name = self.us_state_abbreviation[item["State"]]
        #                     eqip_data_for_year_and_state = eqip_data[
        #                         (eqip_data["State"] == state_name) & (eqip_data["Pay_year"] == year)]
        #                     state_amount = eqip_data_for_year_and_state["payments"].sum()
        #                 else:
        #                     eqip_data_for_year = eqip_data[eqip_data["Pay_year"] == year]
        #                     state_amount = eqip_data_for_year["payments"].sum()
        #                 rounded_state_amount = int(state_amount)
        #                 item["Title II " + str(year)] = rounded_state_amount
        #                 state_total += rounded_state_amount
        #         if "Title II Total" in item:
        #             item["Title II Total"] = state_total
        #
        #     # Programs list
        #     programs_list = ["Crop Insurance", "SNAP", "Title I", "Title II"]
        #
        #     start_year_obj = datetime(self.start_year, 1, 1)
        #     end_year_obj = datetime(self.end_year, 1, 1)
        #
        #     # Update totals
        #     for item in self.all_programs__dict:
        #         year_range_all_programs_total = 0
        #         for year in range(self.start_year, self.end_year + 1):
        #             year_all_programs_total = 0
        #             for program in programs_list:
        #                 year_all_programs_total += item[program + " " + str(year)]
        #             item[str(year) + " All Programs Total"] = round(year_all_programs_total, 2)
        #             year_range_all_programs_total += year_all_programs_total
        #
        #         key = start_year_obj.strftime("%y") + "-" + end_year_obj.strftime("%y") + " All Programs Total"
        #         item[key] = round(year_range_all_programs_total, 2)

    def update_json_files(self):
        with open(self.summary_filepath + ".updated.json", "w") as summary_file_new:
            json.dump(self.summary_file_dict, summary_file_new, indent=2)

        with open(self.all_programs_filepath + ".updated.json", "w") as all_programs_file_new:
            json.dump(self.all_programs__dict, all_programs_file_new, indent=2)

    def remap_state_name_to_abbreviation(self, input_dict):
        # remap state names to abbreviations
        # Create a copy of the keys to avoid the "dictionary keys changed during iteration" error
        state_names = list(input_dict.keys())

        # Iterate over the state names
        for state_name in state_names:
            # Check if the value of state_name is in the values of the us_state_abbreviation dictionary
            if state_name in self.us_state_abbreviation.values():
                # Replace the state_name with the corresponding abbreviation
                state_abbr = \
                    [abbr for abbr, name in self.us_state_abbreviation.items() if name == state_name][0]
                # Create a new entry with the updated key
                input_dict[state_abbr] = input_dict.pop(state_name)

        return input_dict


if __name__ == '__main__':
    summary_filepath = "../title-2-conservation/eqip/summary.json"
    all_programs_filepath = "../title-2-conservation/eqip/allPrograms.json"
    category_filepath = "../title-2-conservation/eqip/eqip-category-update.csv"
    eqip_data_parser = EqipParser(2018, 2022, summary_filepath, all_programs_filepath, category_filepath)
    eqip_data_parser.parse_and_process()
    eqip_data_parser.update_json_files()
