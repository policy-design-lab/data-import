import json
import os

import pandas as pd
from deepmerge import always_merger


class DairyDisasterParser:
    def __init__(self, start_year, end_year, program_main_category_name, data_folder, program_csv_filename, **kwargs):
        self.start_year = start_year
        self.end_year = end_year
        self.program_main_category_name = program_main_category_name
        self.data_folder = data_folder
        self.program_csv_filepath = os.path.join(data_folder, program_csv_filename)
        self.program_data = None
        self.dairy_data = None
        self.disaster_data = None

        # Output data dictionaries
        self.dairy_state_distribution_data_dict = dict()
        self.dairy_program_data_dict = dict()
        self.disaster_state_distribution_data_dict = dict()
        self.disaster_program_data_dict = dict()

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


    def parse_and_process(self):
        # Import CSV file into a Pandas DataFrame
        program_data = pd.read_csv(self.program_csv_filepath)

        # some columns have empty values and this makes the rows type as object
        # this makes the process of SUM errors since those are object not number
        # so the columns should be numeric all the time
        # make sure every number columns be number
        program_data[["payments",
                      "count"]] = \
            program_data[["payments",
                          "count"]].apply(pd.to_numeric)

        # Filter only relevant years' data
        program_data = program_data[program_data["year"].between(self.start_year, self.end_year, inclusive="both")]

        # Filter only dairy data
        dairy_data = program_data[program_data["program"] == "Dairy"]

        # Filter only disaster data
        disaster_data = program_data[program_data["program"] != "Dairy"]

        ###############################################################
        # dairy data process
        ###############################################################
        # 1. Generate State Distribution JSON Data for Dairy
        self.dairy_state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

        # calculate national level values
        total_dairy_payments_at_national_level = dairy_data["payments"].sum()
        total_dairy_count_at_national_level = int(dairy_data["count"].sum())

        # group total data by state, then sum
        sum_by_dairy_payments_by_state = \
            dairy_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()

        sum_by_dairy_count_by_state = \
            dairy_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()

        for state_abbr in self.us_state_abbreviations:
            state = self.us_state_abbreviations[state_abbr]
            # if there is a zero division problem in with state percentage
            within_state_payments = 0
            within_state_count = 0

            dairy_payments_percentage_nation = 0.00
            dairy_count_percentage_nation = 0.00

            if total_dairy_payments_at_national_level > 0:
                dairy_payments_percentage_nation = \
                    round((sum_by_dairy_payments_by_state[state].item() /
                           total_dairy_payments_at_national_level) * 100, 2)
            if total_dairy_count_at_national_level > 0:
                dairy_count_percentage_nation = \
                    round((sum_by_dairy_count_by_state[state].item() /
                           total_dairy_count_at_national_level) * 100, 2)

            new_data_entry = {
                "state": state,
                "programs": [
                    {
                        "programName": "Dairy",
                        "totalCounts": int(sum_by_dairy_count_by_state[state].item()),
                        "paymentInDollars": round(sum_by_dairy_payments_by_state[state].item(), 2),
                        "paymentInPercentageNationwide": dairy_payments_percentage_nation,
                        "countInPercentageNationwide": dairy_count_percentage_nation,
                        "subPrograms": []
                    },
                ]
            }

            self.dairy_state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)].append(
                new_data_entry)

        # Sort states by decreasing order of financial assistance payments
        for year in self.dairy_state_distribution_data_dict:
            self.dairy_state_distribution_data_dict[year] = \
                sorted(self.dairy_state_distribution_data_dict[year],
                       key=lambda x: x["programs"][0]["paymentInDollars"], reverse=True)

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "dairy_state_distribution_data.json"),
                  "w") as output_json_file:
            output_json_file.write(json.dumps(self.dairy_state_distribution_data_dict, indent=2))

        # 2. Generate Sub Programs Data
        # Group total
        dairy_total_by_payments = \
            dairy_data["payments"].sum()

        dairy_total_by_count = \
            program_data["count"].sum()

        self.dairy_program_data_dict = {
            "programs": [
                {
                    "programName": "Dairy",
                    "paymentInDollars": round(dairy_total_by_payments.item(), 2),
                    "totalContacts": int(dairy_total_by_count.item()),
                    "subPrograms": []
                },
            ]
        }

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "dairy_subprograms_data.json"), "w") as output_json_file:
            output_json_file.write(json.dumps(self.dairy_program_data_dict, indent=2))

        ###############################################################
        # disaster data process
        ###############################################################
        # 1. Generate State Distribution JSON Data for Dairy
        elap_data = disaster_data[disaster_data["program"] == "ELAP"]
        lfp_data = disaster_data[disaster_data["program"] == "LFP"]
        lip_data = disaster_data[disaster_data["program"] == "LIP"]
        tap_data = disaster_data[disaster_data["program"] == "TAP"]

        self.disaster_state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)] = []

        # calculate national level values
        total_disaster_payments_at_national_level = disaster_data["payments"].sum()
        total_disaster_count_at_national_level = int(
            disaster_data["count"].sum())
        total_elap_payments_at_national_level = elap_data["payments"].sum()
        total_elap_count_at_national_level = int(elap_data["count"].sum())
        total_lfp_payments_at_national_level = lfp_data["payments"].sum()
        total_lfp_count_at_national_level = int(lfp_data["count"].sum())
        total_lip_payments_at_national_level = lip_data["payments"].sum()
        total_lip_count_at_national_level = int(lip_data["count"].sum())
        total_tap_payments_at_national_level = tap_data["payments"].sum()
        total_tap_count_at_national_level = int(tap_data["count"].sum())


        # group total data by state, then sum
        sum_by_disaster_payments_by_state = \
            disaster_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()

        sum_by_disaster_count_by_state = \
            disaster_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()

        sum_by_elap_payments_by_state = \
            elap_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()
        sum_by_elap_count_by_state = \
            elap_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()

        sum_by_lfp_payments_by_state = \
            lfp_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()
        sum_by_lfp_count_by_state = \
            lfp_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()

        sum_by_lip_payments_by_state = \
            lip_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()
        sum_by_lip_count_by_state = \
            lip_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()

        sum_by_tap_payments_by_state = \
            tap_data[
                ["year", "state", "payments"]
            ].groupby(
                ["state"]
            )["payments"].sum()
        sum_by_tap_count_by_state = \
            tap_data[
                ["year", "state", "count"]
            ].groupby(
                ["state"]
            )["count"].sum()


        for state_abbr in self.us_state_abbreviations:
            state = self.us_state_abbreviations[state_abbr]

            disaster_payments_percentage_nation = 0.00
            disaster_count_percentage_nation = 0.00
            elap_payments_percentage_nation = 0.00
            elap_count_percentage_nation = 0.00
            lfp_payments_percentage_nation = 0.00
            lfp_count_percentage_nation = 0.00
            lip_payments_percentage_nation = 0.00
            lip_count_percentage_nation = 0.00
            tap_payments_percentage_nation = 0.00
            tap_count_percentage_nation = 0.00

            if total_disaster_payments_at_national_level > 0:
                disaster_payments_percentage_nation = \
                    round((sum_by_disaster_payments_by_state[state].item() /
                           total_disaster_payments_at_national_level) * 100, 2)
            if total_disaster_count_at_national_level > 0:
                disaster_count_percentage_nation = \
                    round((sum_by_disaster_count_by_state[state].item() /
                           total_disaster_count_at_national_level) * 100, 2)
            if total_elap_payments_at_national_level > 0:
                elap_payments_percentage_nation = \
                    round((sum_by_elap_payments_by_state[state].item() /
                           total_elap_payments_at_national_level) * 100, 2)
            if total_elap_count_at_national_level > 0:
                elap_count_percentage_nation = \
                    round((sum_by_elap_count_by_state[state].item() /
                           total_elap_count_at_national_level) * 100, 2)
            if total_lfp_payments_at_national_level > 0:
                lfp_payments_percentage_nation = \
                    round((sum_by_lfp_payments_by_state[state].item() /
                           total_lfp_payments_at_national_level) * 100, 2)
            if total_lfp_count_at_national_level > 0:
                lfp_count_percentage_nation = \
                    round((sum_by_lfp_count_by_state[state].item() /
                           total_lfp_count_at_national_level) * 100, 2)
            if total_lip_payments_at_national_level > 0:
                lip_payments_percentage_nation = \
                    round((sum_by_lip_payments_by_state[state].item() /
                           total_lip_payments_at_national_level) * 100, 2)
            if total_lip_count_at_national_level > 0:
                lip_count_percentage_nation = \
                    round((sum_by_lip_count_by_state[state].item() /
                           total_lip_count_at_national_level) * 100, 2)
            if total_tap_payments_at_national_level > 0:
                tap_payments_percentage_nation = \
                    round((sum_by_tap_payments_by_state[state].item() /
                           total_tap_payments_at_national_level) * 100, 2)
            if total_tap_count_at_national_level > 0:
                tap_count_percentage_nation = \
                    round((sum_by_tap_count_by_state[state].item() /
                           total_tap_count_at_national_level) * 100, 2)

            within_state_elap_percentage_payments = 0.0
            within_state_elap_percentage_count = 0.0
            within_state_lfp_percentage_payments = 0.0
            within_state_lfp_percentage_count = 0.0
            within_state_lip_percentage_payments = 0.0
            within_state_lip_percentage_count = 0.0
            within_state_tap_percentage_payments = 0.0
            within_state_tap_percentage_count = 0.0

            if int(sum_by_elap_payments_by_state[state].item()) != 0:
                within_state_elap_percentage_payments = \
                    round((sum_by_elap_payments_by_state[state].item() /
                           sum_by_elap_payments_by_state[state].item()) * 100, 2)
            if int(sum_by_elap_count_by_state[state].item()) != 0:
                within_state_elap_percentage_count = \
                    round((sum_by_elap_count_by_state[state].item() /
                           sum_by_elap_count_by_state[state].item()) * 100, 2)
            if int(sum_by_lfp_payments_by_state[state].item()) != 0:
                within_state_lfp_percentage_payments = \
                    round((sum_by_lfp_payments_by_state[state].item() /
                           sum_by_lfp_payments_by_state[state].item()) * 100, 2)
            if int(sum_by_lfp_count_by_state[state].item()) != 0:
                within_state_lfp_percentage_count = \
                    round((sum_by_lfp_count_by_state[state].item() /
                           sum_by_lfp_count_by_state[state].item()) * 100, 2)
            if int(sum_by_lip_payments_by_state[state].item()) != 0:
                within_state_lip_percentage_payments = \
                    round((sum_by_lip_payments_by_state[state].item() /
                           sum_by_lip_payments_by_state[state].item()) * 100, 2)
            if int(sum_by_lip_count_by_state[state].item()) != 0:
                within_state_lip_percentage_count = \
                    round((sum_by_lip_count_by_state[state].item() /
                           sum_by_lip_count_by_state[state].item()) * 100, 2)
            if int(sum_by_tap_payments_by_state[state].item()) != 0:
                within_state_tap_percentage_payments = \
                    round((sum_by_tap_payments_by_state[state].item() /
                           sum_by_tap_payments_by_state[state].item()) * 100, 2)
            if int(sum_by_tap_count_by_state[state].item()) != 0:
                within_state_tap_percentage_count = \
                    round((sum_by_tap_count_by_state[state].item() /
                           sum_by_tap_count_by_state[state].item()) * 100, 2)

            new_data_entry = {
                "state": state,
                "programs": [
                    {
                        "programName": "Disaster",
                        "totalCounts": int(sum_by_disaster_count_by_state[state].item()),
                        "paymentInDollars": round(sum_by_disaster_payments_by_state[state].item(),2),
                        "paymentInPercentageNationwide": disaster_payments_percentage_nation,
                        "countInPercentageNationwide": disaster_count_percentage_nation,
                        "subPrograms": [
                            {
                                "programName": "ELAP",
                                "totalCounts": int(sum_by_elap_count_by_state[state].item()),
                                "paymentInDollars": round(sum_by_elap_payments_by_state[state].item(), 2),
                                "paymentInPercentageNationwide": elap_payments_percentage_nation,
                                "countInPercentageNationwide": elap_count_percentage_nation,
                                "paymentInPercentageWithinState": within_state_elap_percentage_payments,
                                "countInPercentageWithinState": within_state_elap_percentage_count
                            },
                            {
                                "programName": "LFP",
                                "totalCounts": int(sum_by_lfp_count_by_state[state].item()),
                                "paymentInDollars": round(sum_by_lfp_payments_by_state[state].item(), 2),
                                "paymentInPercentageNationwide": lfp_payments_percentage_nation,
                                "countInPercentageNationwide": lfp_count_percentage_nation,
                                "paymentInPercentageWithinState": within_state_lfp_percentage_payments,
                                "countInPercentageWithinState": within_state_lfp_percentage_count
                            },
                            {
                                "programName": "LIP",
                                "totalCounts": int(sum_by_lip_count_by_state[state].item()),
                                "paymentInDollars": round(sum_by_lip_payments_by_state[state].item() ,2),
                                "paymentInPercentageNationwide": lip_payments_percentage_nation,
                                "countInPercentageNationwide": lip_count_percentage_nation,
                                "paymentInPercentageWithinState": within_state_lip_percentage_payments,
                                "countInPercentageWithinState": within_state_lip_percentage_count
                            },
                            {
                                "programName": "TAP",
                                "totalCounts": int(sum_by_tap_count_by_state[state].item()),
                                "paymentInDollars": round(sum_by_tap_payments_by_state[state].item(), 2),
                                "paymentInPercentageNationwide": tap_payments_percentage_nation,
                                "countInPercentageNationwide": tap_count_percentage_nation,
                                "paymentInPercentageWithinState": within_state_tap_percentage_payments,
                                "countInPercentageWithinState": within_state_tap_percentage_count
                            }
                        ]
                    },
                ]
            }

            self.disaster_state_distribution_data_dict[str(self.start_year) + "-" + str(self.end_year)].append(
                new_data_entry)

        # Sort states by decreasing order of financial assistance payments
        for year in self.disaster_state_distribution_data_dict:
            self.disaster_state_distribution_data_dict[year] = \
                sorted(self.disaster_state_distribution_data_dict[year],
                       key=lambda x: x["programs"][0]["paymentInDollars"], reverse=True)

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "disaster_state_distribution_data.json"),
                  "w") as output_json_file:
            output_json_file.write(json.dumps(self.disaster_state_distribution_data_dict, indent=2))

        # 2. Generate Sub Programs Data
        # Group total
        disaster_total_by_payments = \
            disaster_data["payments"].sum()
        disaster_total_by_count = \
            disaster_data["count"].sum()
        elap_total_by_payments = \
            elap_data["payments"].sum()
        elap_total_by_count = \
            elap_data["count"].sum()
        lfp_total_by_payments = \
            lfp_data["payments"].sum()
        lfp_total_by_count = \
            lfp_data["count"].sum()
        lip_total_by_payments = \
            lip_data["payments"].sum()
        lip_total_by_count = \
            lip_data["count"].sum()
        tap_total_by_payments = \
            tap_data["payments"].sum()
        tap_total_by_count = \
            tap_data["count"].sum()

        self.disaster_program_data_dict = {
            "programs": [
                {
                    "programName": "Disaster",
                    "paymentInDollars": round(disaster_total_by_payments.item(), 2),
                    "totalCounts": int(disaster_total_by_count.item()),
                    "subPrograms": [
                        {
                            "programName": "ELAP",
                            "paymentInDollars": round(elap_total_by_payments.item(), 2),
                            "totalCounts": int(elap_total_by_count.item()),
                        },
                        {
                            "programName": "LFP",
                            "paymentInDollars": round(lfp_total_by_payments.item(), 2),
                            "totalCounts": int(lfp_total_by_count.item()),
                        },
                        {
                            "programName": "LIP",
                            "paymentInDollars": round(lip_total_by_payments.item(), 2),
                            "totalCounts": int(lip_total_by_count.item()),
                        },
                        {
                            "programName": "TAP",
                            "paymentInDollars": round(tap_total_by_payments.item(), 2),
                            "totalCounts": int(tap_total_by_count.item()),
                        }
                    ]
                },
            ]
        }

        # Write processed_data_dict as JSON data
        with open(os.path.join(self.data_folder, "disaster_subprograms_data.json"), "w") as output_json_file:
            output_json_file.write(json.dumps(self.disaster_program_data_dict, indent=2))
