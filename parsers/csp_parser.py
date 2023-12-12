import pandas as pd
import json

from deepmerge import always_merger


class CSPDataParser:
    def __init__(self, start_year, end_year, csv_filepath):
        self.start_year = start_year
        self.end_year = end_year
        self.csv_filepath = csv_filepath

        self.statute_and_practice_categories_mapping = {
            "2018 Practices": ["Structural", "Land management", "Vegetative", "Forest management", "Soil testing",
                               "Soil remediation", "Other improvement", "Existing activity payments", "Bundles"],
            "2014 Eligible Land": ["Pastureland", "Cropland", "Pastured cropland", "Rangeland",
                                   "Non-industrial private forestland", "Other: supplemental, adjustment & other"],
        }

        self.processed_data_dict = dict()
        self.state_distribution_data_dict = dict()
        self.practice_categories_data_dict = dict()

    def find_statute_by_category(self, category_name):
        for statute_name in self.statute_and_practice_categories_mapping:
            if category_name in self.statute_and_practice_categories_mapping[statute_name]:
                return statute_name

    def find_and_get_zero_practice_category_entries(self, statue_name, practice_categories_list,
                                                    for_percentage_json=False):
        diff_list = list(set(self.statute_and_practice_categories_mapping[statue_name]) - set(practice_categories_list))
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
        csp_data = pd.read_csv(self.csv_filepath)

        # Replace category values for standardization
        csp_data = csp_data.replace({
            "Structural (6(A)(i))": "Structural",
            "Land Management (6(A)(ii))": "Land management",
            "Forest management (6(A)(iv))": "Forest management",
            "Vegetative (6(A)(iii))": "Vegetative",
            "Soil remediation (6(A)(vi))": "Soil remediation",
            "Other (6(A)(vii))": "Other improvement",
            "NIPF": "Non-industrial private forestland",
            "2014 Other Practices": "Other: supplemental, adjustment & other",
            "Existing Activity Payments": "Existing activity payments",
            "Pastured Cropland": "Pastured cropland"
        })

        # Rename column names to make it more uniform
        csp_data.rename(columns={"Pay_year": "pay_year", "State": "state", "StatutoryCategory": "category_name"},
                        inplace=True)

        statute_map = dict()
        for statute_name in self.statute_and_practice_categories_mapping:
            for value in self.statute_and_practice_categories_mapping[statute_name]:
                statute_map[value] = statute_name
        csp_data["statute_name"] = csp_data["category_name"].map(statute_map)

        # Filter data for only required years
        csp_data = csp_data[csp_data["pay_year"].between(self.start_year, self.end_year, inclusive="both")]

        # Group data by state, practice category name, and payment
        payments_by_category_by_state_for_year = \
            csp_data[
                ["pay_year", "state", "payments", "category_name"]
            ].groupby(
                ["pay_year", "state", "category_name"]
            )["payments"].sum()

        # 1. Generate map data
        if True:
            # Iterate through all tuples
            for data_tuple, payment in payments_by_category_by_state_for_year.items():
                year, state_name, category_name = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(year),
                    "statutes": [
                        {
                            "statuteName": "2018 Practices",
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "2014 Eligible Land",
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
            total_payments_by_category_by_state = csp_data[
                ["state", "category_name", "payments"]].groupby(
                ["state", "category_name"]
            )["payments"].sum()

            # Iterate through all tuples
            for data_tuple, payment in total_payments_by_category_by_state.items():
                state_name, category_name = data_tuple
                rounded_payment = round(payment, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "statutes": [
                        {
                            "statuteName": "2018 Practices",
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "2014 Eligible Land",
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

            # add year to the data
            tmp_output = dict()
            tmp_output[str(self.start_year) + "-" + str(self.end_year)] = []

            # add year to the tmp_output
            tmp_output[str(self.start_year) + "-" + str(self.end_year)].append(self.processed_data_dict)

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/csp/csp_map_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(tmp_output, indent=4))

        # 2. Generate state distribution data
        if True:
            total_payments_by_state = csp_data[
                ["state", "payments"]].groupby(["state"])["payments"].sum()
            total_payments_at_national_level = round(csp_data["payments"].sum(), 2)

            total_payments_by_category_by_state = csp_data[
                ["state", "category_name", "payments"]].groupby(
                ["state", "category_name"]
            )["payments"].sum()

            total_payments_by_category_at_national_level = round(
                csp_data[["category_name", "payments"]].groupby(["category_name"]).sum(), 2)

            total_payments_by_statute = csp_data[
                ["statute_name", "payments"]].groupby(
                ["statute_name"]
            )["payments"].sum()

            # Iterate through all tuples
            for state_name, payment in total_payments_by_state.items():
                yearly_state_payment = round(payment, 2)

                new_data_entry = {
                    "statutes": [
                        {
                            "statuteName": "2018 Practices",
                            "statutePaymentInDollars": 0.0,
                            "practiceCategories": [
                            ]
                        },
                        {
                            "statuteName": "2014 Eligible Land",
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

                self.state_distribution_data_dict[state_name] = [new_data_entry]

            # Add zero entries and additional percentages
            for state_name in self.state_distribution_data_dict:
                for year_data in self.state_distribution_data_dict[state_name]:
                    for statute in year_data["statutes"]:
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

                        statute["statutePaymentInPercentageWithinState"] = round(
                            statute["statutePaymentInDollars"] / year_data["totalPaymentInDollars"] * 100, 2)

                        statute["statutePaymentInPercentageNationwide"] = round(
                            statute["statutePaymentInDollars"] / total_payments_by_statute[
                                statute["statuteName"]] * 100, 2)

            # Sort states by decreasing order of percentages
            self.state_distribution_data_dict = dict(sorted(self.state_distribution_data_dict.items(),
                                                            key=lambda x: x[1][0]["totalPaymentInPercentageNationwide"],
                                                            reverse=True))

            # add year to the data
            tmp_output = dict()
            tmp_output[str(self.start_year) + "-" + str(self.end_year)] = []

            # add year to the tmp_output
            tmp_output[str(self.start_year) + "-" + str(self.end_year)].append(self.state_distribution_data_dict)

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/csp/csp_state_distribution_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(tmp_output, indent=4))

        # 3. Generate practice categories data for the donut chart
        if True:
            statutes_data = {
                "statutes": [
                    {
                        "statuteName": "2018 Practices",
                        "practiceCategories": [

                        ]
                    },
                    {
                        "statuteName": "2014 Eligible Land",
                        "practiceCategories": [

                        ]
                    }
                ]
            }
            total_for_statute = {
                "2018 Practices": 0.0,
                "2014 Eligible Land": 0.0
            }

            for statute in statutes_data["statutes"]:
                for category_name in self.statute_and_practice_categories_mapping[statute["statuteName"]]:

                    if category_name in total_payments_by_category_at_national_level["payments"]:
                        category_payment = float(round(
                            total_payments_by_category_at_national_level["payments"][category_name], 2))
                        entry_dict = {
                            "practiceCategoryName": category_name,
                            "totalPaymentInDollars": category_payment,
                        }
                        total_for_statute[statute["statuteName"]] += category_payment
                    # Add zero entry when practice category is not existing in the actual data
                    else:
                        entry_dict = {
                            "practiceCategoryName": category_name,
                            "totalPaymentInDollars": 0.0,
                        }
                    statute["practiceCategories"].append(entry_dict)

            for statute in statutes_data["statutes"]:
                for practice_category in statute["practiceCategories"]:
                    practice_category["totalPaymentInPercentage"] = round(
                        practice_category["totalPaymentInDollars"] / total_for_statute[
                            statute["statuteName"]] * 100, 2)
                statute["totalPaymentInDollars"] = float(round(total_for_statute[statute["statuteName"]], 2))
                statute["totalPaymentInPercentage"] = float(round(
                    total_for_statute[statute["statuteName"]] / total_payments_at_national_level * 100, 2))
                statute["practiceCategories"].sort(key=lambda x: x["totalPaymentInPercentage"], reverse=True)

            # Write processed_data_dict as JSON data
            with open("../title-2-conservation/csp/csp_practice_categories_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(statutes_data, indent=4))


if __name__ == '__main__':
    commodities_data_parser = CSPDataParser(2018, 2022, "../title-2-conservation/csp/CSPcategoriesUPDATE.csv")
    commodities_data_parser.parse_and_process()
