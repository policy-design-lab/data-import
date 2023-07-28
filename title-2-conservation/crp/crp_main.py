import pandas as pd
import json


class CRPDataParser:
    def __init__(self, start_year, end_year, csv_filepath):
        self.start_year = start_year
        self.end_year = end_year
        self.csv_filepath = csv_filepath

        self.total_data = dict()
        self.state_distribution_general_singup_data_dict = dict()
        self.state_distribution_continuous_data_dict = dict()
        self.state_distribution_crep_data_dict = dict()
        self.state_distribution_non_crep_data_dict = dict()
        self.state_distribution_wetland_data_dict = dict()
        self.state_distribution_grassland_data_dict = dict()

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
        crp_data = pd.read_csv(self.csv_filepath)

        # some columns have empty values and this makes the rows type as object
        # this makes the process of SUM errors since those are object not number
        # so the columns should be numeric all the time
        # make sure every number columns be number
        crp_data[["Total CRP - NUMBER OF CONTRACTS",
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
            crp_data[["Total CRP - NUMBER OF CONTRACTS",
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

        # Filter data for only required years
        crp_data = crp_data[crp_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # Group General Sign-up data by state and year, then sum
        general_signup_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "Total General Sign-Up - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["Total General Sign-Up - NUMBER OF CONTRACTS"].sum()

        general_signup_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "Total General Sign-Up - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["Total General Sign-Up - NUMBER OF FARMS"].sum()

        general_signup_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Total General Sign-Up - ACRES"]
            ].groupby(
                ["year", "state"]
            )["Total General Sign-Up - ACRES"].sum()

        general_signup_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        general_signup_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous data by state and year, then sum
        continuous_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "Total Continuous - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["Total Continuous - NUMBER OF CONTRACTS"].sum()

        continuous_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "Total Continuous - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["Total Continuous - NUMBER OF FARMS"].sum()

        continuous_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Total Continuous - ACRES"]
            ].groupby(
                ["year", "state"]
            )["Total Continuous - ACRES"].sum()

        continuous_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        continuous_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group crep only data by state and year, then sum
        crep_only_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "CREP Only - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["CREP Only - NUMBER OF CONTRACTS"].sum()

        crep_only_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "CREP Only - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["CREP Only - NUMBER OF FARMS"].sum()

        crep_only_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "CREP Only - ACRES"]
            ].groupby(
                ["year", "state"]
            )["CREP Only - ACRES"].sum()

        crep_only_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        crep_only_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group continuous non-crep data by state and year, then sum
        non_crep_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "Continuous Non-CREP - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["Continuous Non-CREP - NUMBER OF CONTRACTS"].sum()

        non_crep_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "Continuous Non-CREP - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["Continuous Non-CREP - NUMBER OF FARMS"].sum()

        non_crep_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Continuous Non-CREP - ACRES"]
            ].groupby(
                ["year", "state"]
            )["Continuous Non-CREP - ACRES"].sum()

        non_crep_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        non_crep_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group farmable wetland data by state and year, then sum
        wetland_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "Farmable Wetland - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["Farmable Wetland - NUMBER OF CONTRACTS"].sum()

        wetland_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "Farmable Wetland - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["Farmable Wetland - NUMBER OF FARMS"].sum()

        wetland_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Farmable Wetland - ACRES"]
            ].groupby(
                ["year", "state"]
            )["Farmable Wetland - ACRES"].sum()

        wetland_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        wetland_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # Group grassland data by state and year, then sum
        grassland_by_contract_by_state_for_year = \
            crp_data[
                ["year", "state", "Grassland - NUMBER OF CONTRACTS"]
            ].groupby(
                ["year", "state"]
            )["Grassland - NUMBER OF CONTRACTS"].sum()

        grassland_by_farm_by_state_for_year = \
            crp_data[
                ["year", "state", "Grassland - NUMBER OF FARMS"]
            ].groupby(
                ["year", "state"]
            )["Grassland - NUMBER OF FARMS"].sum()

        grassland_by_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Grassland - ACRES"]
            ].groupby(
                ["year", "state"]
            )["Grassland - ACRES"].sum()

        grassland_by_rental_1k_by_state_for_year = \
            crp_data[
                ["year", "state", "Grassland - ANNUAL RENTAL PAYMENTS ($1000)"]
            ].groupby(
                ["year", "state"]
            )["Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()

        grassland_by_rental_acre_by_state_for_year = \
            crp_data[
                ["year", "state", "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]
            ].groupby(
                ["year", "state"]
            )["Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()

        # there is a rows for U.S. and this should not be used to calculate national level
        # the weird thing is that the value in U.S. doesn't match to the sum of all the states
        # get national level values location from the table since it contains it
        us_row_loc = []
        for index, state in enumerate(crp_data['state']):
            if state == 'U.S.':
                us_row_loc.append(index)

        # remove U.S. rows
        crp_data_no_us = crp_data.drop(crp_data.index[us_row_loc])

        # Generate state distribution data
        if True:
            # general signup
            total_general_signup_contract_by_state = crp_data[
                ["state", "Total General Sign-Up - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "Total General Sign-Up - NUMBER OF CONTRACTS"].sum()
            total_general_signup_contract_at_national_level = round(
                crp_data_no_us["Total General Sign-Up - NUMBER OF CONTRACTS"].sum(), 2)

            total_general_signup_farm_by_state = crp_data[
                ["state", "Total General Sign-Up - NUMBER OF FARMS"]].groupby(["state"])[
                "Total General Sign-Up - NUMBER OF FARMS"].sum()
            total_general_signup_farm_at_national_level = round(
                crp_data_no_us["Total General Sign-Up - NUMBER OF FARMS"].sum(), 2)

            total_general_signup_acre_by_state = crp_data[
                ["state", "Total General Sign-Up - ACRES"]].groupby(["state"])[
                "Total General Sign-Up - ACRES"].sum()
            total_general_signup_acre_at_national_level = round(
                crp_data_no_us["Total General Sign-Up - ACRES"].sum(), 2)

            total_general_signup_rental_1k_by_state = crp_data[
                ["state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            total_general_signup_rental_1k_at_national_level = round(
                crp_data_no_us["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            total_general_signup_rental_acre_by_state = crp_data[
                ["state", "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            total_general_signup_rental_acre_at_national_level = round(
                crp_data_no_us["Total General Sign-Up - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # continuous data
            total_continuous_contract_by_state = crp_data[
                ["state", "Total Continuous - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "Total Continuous - NUMBER OF CONTRACTS"].sum()
            total_continuous_contract_at_national_level = round(
                crp_data_no_us["Total Continuous - NUMBER OF CONTRACTS"].sum(), 2)

            total_continuous_farm_by_state = crp_data[
                ["state", "Total Continuous - NUMBER OF FARMS"]].groupby(["state"])[
                "Total Continuous - NUMBER OF FARMS"].sum()
            total_continuous_farm_at_national_level = round(
                crp_data_no_us["Total Continuous - NUMBER OF FARMS"].sum(), 2)

            total_continuous_acre_by_state = crp_data[
                ["state", "Total Continuous - ACRES"]].groupby(["state"])[
                "Total Continuous - ACRES"].sum()
            total_continuous_acre_at_national_level = round(
                crp_data_no_us["Total Continuous - ACRES"].sum(), 2)

            total_continuous_rental_1k_by_state = crp_data[
                ["state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            total_continuous_rental_1k_at_national_level = round(
                crp_data_no_us["Total Continuous - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            total_continuous_rental_acre_by_state = crp_data[
                ["state", "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            total_continuous_rental_acre_at_national_level = round(
                crp_data_no_us["Total Continuous - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # crep only data
            crep_only_contract_by_state = crp_data[
                ["state", "CREP Only - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "CREP Only - NUMBER OF CONTRACTS"].sum()
            crep_only_contract_at_national_level = round(
                crp_data_no_us["CREP Only - NUMBER OF CONTRACTS"].sum(), 2)

            crep_only_farm_by_state = crp_data[
                ["state", "CREP Only - NUMBER OF FARMS"]].groupby(["state"])[
                "CREP Only - NUMBER OF FARMS"].sum()
            crep_only_farm_at_national_level = round(
                crp_data_no_us["CREP Only - NUMBER OF FARMS"].sum(), 2)

            crep_only_acre_by_state = crp_data[
                ["state", "CREP Only - ACRES"]].groupby(["state"])[
                "CREP Only - ACRES"].sum()
            crep_only_acre_at_national_level = round(
                crp_data_no_us["CREP Only - ACRES"].sum(), 2)

            crep_only_rental_1k_by_state = crp_data[
                ["state", "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            crep_only_rental_1k_at_national_level = round(
                crp_data_no_us["CREP Only - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            crep_only_rental_acre_by_state = crp_data[
                ["state", "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            crep_only_rental_acre_at_national_level = round(
                crp_data_no_us["CREP Only - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # non-crep data
            non_crep_contract_by_state = crp_data[
                ["state", "Continuous Non-CREP - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "Continuous Non-CREP - NUMBER OF CONTRACTS"].sum()
            non_crep_contract_at_national_level = round(
                crp_data_no_us["Continuous Non-CREP - NUMBER OF CONTRACTS"].sum(), 2)

            non_crep_farm_by_state = crp_data[
                ["state", "Continuous Non-CREP - NUMBER OF FARMS"]].groupby(["state"])[
                "Continuous Non-CREP - NUMBER OF FARMS"].sum()
            non_crep_farm_at_national_level = round(
                crp_data_no_us["Continuous Non-CREP - NUMBER OF FARMS"].sum(), 2)

            non_crep_acre_by_state = crp_data[
                ["state", "Continuous Non-CREP - ACRES"]].groupby(["state"])[
                "Continuous Non-CREP - ACRES"].sum()
            non_crep_acre_at_national_level = round(
                crp_data_no_us["Continuous Non-CREP - ACRES"].sum(), 2)

            non_crep_rental_1k_by_state = crp_data[
                ["state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            non_crep_rental_1k_at_national_level = round(
                crp_data_no_us["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            non_crep_rental_acre_by_state = crp_data[
                ["state", "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            non_crep_rental_acre_at_national_level = round(
                crp_data_no_us["Continuous Non-CREP - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # farmable wetland data
            wetland_contract_by_state = crp_data[
                ["state", "Farmable Wetland - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "Farmable Wetland - NUMBER OF CONTRACTS"].sum()
            wetland_contract_at_national_level = round(
                crp_data_no_us["Farmable Wetland - NUMBER OF CONTRACTS"].sum(), 2)

            wetland_farm_by_state = crp_data[
                ["state", "Farmable Wetland - NUMBER OF FARMS"]].groupby(["state"])[
                "Farmable Wetland - NUMBER OF FARMS"].sum()
            wetland_farm_at_national_level = round(
                crp_data_no_us["Farmable Wetland - NUMBER OF FARMS"].sum(), 2)

            wetland_acre_by_state = crp_data[
                ["state", "Farmable Wetland - ACRES"]].groupby(["state"])[
                "Farmable Wetland - ACRES"].sum()
            wetland_acre_at_national_level = round(
                crp_data_no_us["Farmable Wetland - ACRES"].sum(), 2)

            wetland_rental_1k_by_state = crp_data[
                ["state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            wetland_rental_1k_at_national_level = round(
                crp_data_no_us["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            wetland_rental_acre_by_state = crp_data[
                ["state", "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            wetland_rental_acre_at_national_level = round(
                crp_data_no_us["Farmable Wetland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # grassland data
            grassland_contract_by_state = crp_data[
                ["state", "Grassland - NUMBER OF CONTRACTS"]].groupby(["state"])[
                "Grassland - NUMBER OF CONTRACTS"].sum()
            grassland_contract_at_national_level = round(
                crp_data_no_us["Grassland - NUMBER OF CONTRACTS"].sum(), 2)

            grassland_farm_by_state = crp_data[
                ["state", "Grassland - NUMBER OF FARMS"]].groupby(["state"])[
                "Grassland - NUMBER OF FARMS"].sum()
            grassland_farm_at_national_level = round(
                crp_data_no_us["Grassland - NUMBER OF FARMS"].sum(), 2)

            grassland_acre_by_state = crp_data[
                ["state", "Grassland - ACRES"]].groupby(["state"])[
                "Grassland - ACRES"].sum()
            grassland_acre_at_national_level = round(
                crp_data_no_us["Grassland - ACRES"].sum(), 2)

            grassland_rental_1k_by_state = crp_data[
                ["state", "Grassland - ANNUAL RENTAL PAYMENTS ($1000)"]].groupby(["state"])[
                "Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum()
            grassland_rental_1k_at_national_level = round(
                crp_data_no_us["Grassland - ANNUAL RENTAL PAYMENTS ($1000)"].sum(), 2)

            grassland_rental_acre_by_state = crp_data[
                ["state", "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"]].groupby(["state"])[
                "Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum()
            grassland_rental_acre_at_national_level = round(
                crp_data_no_us["Grassland - ANNUAL RENTAL PAYMENTS ($/ACRE)"].sum(), 2)

        # Iterate through general signup
            for state_name, signup in total_general_signup_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGeneralSignupsByContractInPercentageNationwide": round(
                        (yearly_state_signup / total_general_signup_contract_at_national_level) * 100, 2),
                    "totalGeneralSignupsByContract": yearly_state_signup
                }

                self.state_distribution_general_singup_data_dict[state_name] = [new_data_entry]

            for state_name, signup in total_general_signup_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGeneralSignupsByFarmInPercentageNationwide": round(
                        (yearly_state_signup / total_general_signup_farm_at_national_level) * 100, 2),
                    "totalGeneralSignupsByFarm": yearly_state_signup
                }

                self.state_distribution_general_singup_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_general_signup_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGeneralSignupsByAcreInPercentageNationwide": round(
                        (yearly_state_signup / total_general_signup_acre_at_national_level) * 100, 2),
                    "totalGeneralSignupsByCare": yearly_state_signup
                }

                self.state_distribution_general_singup_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_general_signup_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGeneralSignupsByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / total_general_signup_rental_1k_at_national_level) * 100, 2),
                    "totalGeneralSignupsByRental1K": yearly_state_signup
                }

                self.state_distribution_general_singup_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_general_signup_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGeneralSignupsByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / total_general_signup_rental_acre_at_national_level) * 100, 2),
                    "totalGeneralSignupsByRentalAcre": yearly_state_signup
                }

                self.state_distribution_general_singup_data_dict[state_name].append(new_data_entry)

        # Iterate through continuous
            for state_name, signup in total_continuous_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalContinuousByContractInPercentageNationwide": round(
                        (yearly_state_signup / total_continuous_contract_at_national_level) * 100, 2),
                    "totalContinuousByContract": yearly_state_signup
                }

                self.state_distribution_continuous_data_dict[state_name] = [new_data_entry]

            for state_name, signup in total_continuous_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalContinuousByFarmInPercentageNationwide": round(
                        (yearly_state_signup / total_continuous_farm_at_national_level) * 100, 2),
                    "totalContinuousByFarm": yearly_state_signup
                }

                self.state_distribution_continuous_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_continuous_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalContinuousByAcreInPercentageNationwide": round(
                        (yearly_state_signup / total_continuous_acre_at_national_level) * 100, 2),
                    "totalContinuousByCare": yearly_state_signup
                }

                self.state_distribution_continuous_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_continuous_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalContinuousByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / total_continuous_rental_1k_at_national_level) * 100, 2),
                    "totalContinuousByRental1K": yearly_state_signup
                }

                self.state_distribution_continuous_data_dict[state_name].append(new_data_entry)

            for state_name, signup in total_continuous_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalContinuousByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / total_continuous_rental_acre_at_national_level) * 100, 2),
                    "totalContinuousByRentalAcre": yearly_state_signup
                }

                self.state_distribution_continuous_data_dict[state_name].append(new_data_entry)

        # Iterate through crep only
            for state_name, signup in crep_only_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalCREPOnlyByContractInPercentageNationwide": round(
                        (yearly_state_signup / crep_only_contract_at_national_level) * 100, 2),
                    "totalCREPOnlyByContract": yearly_state_signup
                }

                self.state_distribution_crep_data_dict[state_name] = [new_data_entry]

            for state_name, signup in crep_only_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalCREPOnlyByFarmInPercentageNationwide": round(
                        (yearly_state_signup / crep_only_farm_at_national_level) * 100, 2),
                    "totalCREPOnlyByFarm": yearly_state_signup
                }

                self.state_distribution_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in crep_only_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalCREPOnlyByAcreInPercentageNationwide": round(
                        (yearly_state_signup / crep_only_acre_at_national_level) * 100, 2),
                    "totalCREPOnlyByCare": yearly_state_signup
                }

                self.state_distribution_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in crep_only_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalCREPOnlyByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / crep_only_rental_1k_at_national_level) * 100, 2),
                    "totalCREPOnlyByRental1K": yearly_state_signup
                }

                self.state_distribution_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in crep_only_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalCREPOnlyByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / crep_only_rental_acre_at_national_level) * 100, 2),
                    "totalCREPOnlyByRentalAcre": yearly_state_signup
                }

                self.state_distribution_crep_data_dict[state_name].append(new_data_entry)

            # Iterate through non crep
            for state_name, signup in non_crep_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalNonCREPByContractInPercentageNationwide": round(
                        (yearly_state_signup / non_crep_contract_at_national_level) * 100, 2),
                    "totalNonCREPByContract": yearly_state_signup
                }

                self.state_distribution_non_crep_data_dict[state_name] = [new_data_entry]

            for state_name, signup in non_crep_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalNonCREPByFarmInPercentageNationwide": round(
                        (yearly_state_signup / non_crep_farm_at_national_level) * 100, 2),
                    "totalNonCREPByFarm": yearly_state_signup
                }

                self.state_distribution_non_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in non_crep_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalNonCREPByAcreInPercentageNationwide": round(
                        (yearly_state_signup / non_crep_acre_at_national_level) * 100, 2),
                    "totalNonCREPByCare": yearly_state_signup
                }

                self.state_distribution_non_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in non_crep_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalNonCREPByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / non_crep_rental_1k_at_national_level) * 100, 2),
                    "totalNonCREPByRental1K": yearly_state_signup
                }

                self.state_distribution_non_crep_data_dict[state_name].append(new_data_entry)

            for state_name, signup in non_crep_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalNonCREPByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / non_crep_rental_acre_at_national_level) * 100, 2),
                    "totalNonCREPByRentalAcre": yearly_state_signup
                }

                self.state_distribution_non_crep_data_dict[state_name].append(new_data_entry)

        # Iterate through farmable wetland
            for state_name, signup in wetland_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalWetlandByContractInPercentageNationwide": round(
                        (yearly_state_signup / wetland_contract_at_national_level) * 100, 2),
                    "totalWetlandByContract": yearly_state_signup
                }

                self.state_distribution_wetland_data_dict[state_name] = [new_data_entry]

            for state_name, signup in wetland_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalWetlandByFarmInPercentageNationwide": round(
                        (yearly_state_signup / wetland_farm_at_national_level) * 100, 2),
                    "totalWetlandByFarm": yearly_state_signup
                }

                self.state_distribution_wetland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in wetland_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalWetlandByAcreInPercentageNationwide": round(
                        (yearly_state_signup / wetland_acre_at_national_level) * 100, 2),
                    "totalWetlandByCare": yearly_state_signup
                }

                self.state_distribution_wetland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in wetland_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalWetlandByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / wetland_rental_1k_at_national_level) * 100, 2),
                    "totalWetlandByRental1K": yearly_state_signup
                }

                self.state_distribution_wetland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in wetland_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalWetlandByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / wetland_rental_acre_at_national_level) * 100, 2),
                    "totalWetlandByRentalAcre": yearly_state_signup
                }

                self.state_distribution_wetland_data_dict[state_name].append(new_data_entry)

        # Iterate through grassland
            for state_name, signup in grassland_contract_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGrasslandByContractInPercentageNationwide": round(
                        (yearly_state_signup / grassland_contract_at_national_level) * 100, 2),
                    "totalGrasslandByContract": yearly_state_signup
                }

                self.state_distribution_grassland_data_dict[state_name] = [new_data_entry]

            for state_name, signup in grassland_farm_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGrasslandByFarmInPercentageNationwide": round(
                        (yearly_state_signup / grassland_farm_at_national_level) * 100, 2),
                    "totalGrasslandByFarm": yearly_state_signup
                }

                self.state_distribution_grassland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in grassland_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGrasslandByAcreInPercentageNationwide": round(
                        (yearly_state_signup / grassland_acre_at_national_level) * 100, 2),
                    "totalGrasslandByCare": yearly_state_signup
                }

                self.state_distribution_grassland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in grassland_rental_1k_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGrasslandByRental1kInPercentageNationwide": round(
                        (yearly_state_signup / grassland_rental_1k_at_national_level) * 100, 2),
                    "totalGrasslandByRental1K": yearly_state_signup
                }

                self.state_distribution_grassland_data_dict[state_name].append(new_data_entry)

            for state_name, signup in grassland_rental_acre_by_state.items():
                yearly_state_signup = round(signup, 2)

                new_data_entry = {
                    "years": str(self.start_year) + "-" + str(self.end_year),
                    "totalGrasslandByRentalAcreInPercentageNationwide": round(
                        (yearly_state_signup / grassland_rental_acre_at_national_level) * 100, 2),
                    "totalGrasslandByRentalAcre": yearly_state_signup
                }

                self.state_distribution_grassland_data_dict[state_name].append(new_data_entry)

            # Sort general signup states by decreasing order of percentages
            self.state_distribution_general_singup_data_dict = dict(
                sorted(self.state_distribution_general_singup_data_dict.items(),
                       key=lambda x: x[1][0]["totalGeneralSignupsByContractInPercentageNationwide"],
                       reverse=True))

            # Sort continuous states by decreasing order of percentages
            self.state_distribution_continuous_data_dict = dict(
                sorted(self.state_distribution_continuous_data_dict.items(),
                       key=lambda x: x[1][0]["totalContinuousByContractInPercentageNationwide"],
                       reverse=True))

            # Sort crep only states by decreasing order of percentages
            self.state_distribution_crep_data_dict = dict(
                sorted(self.state_distribution_crep_data_dict.items(),
                       key=lambda x: x[1][0]["totalCREPOnlyByContractInPercentageNationwide"],
                       reverse=True))

            # Sort non crep states by decreasing order of percentages
            self.state_distribution_non_crep_data_dict = dict(
                sorted(self.state_distribution_non_crep_data_dict.items(),
                       key=lambda x: x[1][0]["totalNonCREPByContractInPercentageNationwide"],
                       reverse=True))

            # Sort farmable wetland states by decreasing order of percentages
            self.state_distribution_wetland_data_dict = dict(
                sorted(self.state_distribution_wetland_data_dict.items(),
                       key=lambda x: x[1][0]["totalWetlandByContractInPercentageNationwide"],
                       reverse=True))

            # Sort grassland states by decreasing order of percentages
            self.state_distribution_grassland_data_dict = dict(
                sorted(self.state_distribution_grassland_data_dict.items(),
                       key=lambda x: x[1][0]["totalGrasslandByContractInPercentageNationwide"],
                       reverse=True))

            # construct output json
            self.total_data["General Sign-up"] = self.state_distribution_general_singup_data_dict
            self.total_data["Total Continuous Sign-up"] = self.state_distribution_continuous_data_dict
            self.total_data["Total Continuous Sign-up"]["Continuous (Non-CREP)"] = \
                self.state_distribution_non_crep_data_dict
            self.total_data["Total Continuous Sign-up"]["Conservation Reserve Enhancement Program (CREP)"] = \
                self.state_distribution_crep_data_dict
            self.total_data["Total Continuous Sign-up"]["Farmable Wetland"] = \
                self.state_distribution_wetland_data_dict
            self.total_data["Grassland"] = self.state_distribution_grassland_data_dict
            print("test")

            # Write processed_data_dict as JSON data
            with open("crp_state_distribution_data.json", "w") as output_json_file:
                output_json_file.write(json.dumps(self.total_data, indent=4))


if __name__ == '__main__':
    conservation_data_parser = CRPDataParser(2018, 2022, "CRP_total_compiled_June_22_2023.csv")
    conservation_data_parser.parse_and_process()
