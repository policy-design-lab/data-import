import gzip
import json
import os

import numpy as np
import pandas as pd

class ArcPlcParser:
    def __init__(self, data_folder, current_farmbill_data, proposed_farmbill_data, baseacres_commodity_data):
        print("init ArcPlcParser")
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
        self.baseacres_commodity_data = baseacres_commodity_data
        self.data_folder = data_folder

    def parse_and_process(self):
        # Generate Current Json file
        current_df = self.process_df(self.current_farmbill_data)
        #current_df.to_csv(os.path.join(self.data_folder, "current_df.csv"), index=False)
        self.generate_output(current_df, "current")

        # Generate Proposed Json file
        proposed_df = self.process_df(self.proposed_farmbill_data)
        #proposed_df.to_csv(os.path.join(self.data_folder, "proposed_df.csv"), index=False)
        self.generate_output(proposed_df, "proposed")

    def process_df(self, data_path):
        # Load CVS files
        df = pd.read_csv(data_path)
        baseacres_df = pd.read_csv(self.baseacres_commodity_data)

        # Filter out so we have only the mean and median for PmtPerAc, other attributes aren't needed
        new_df = df[(df['attribute'].isin(['mean', 'median'])) & (df['element'] == 'PmtPerAc')]

        # Merge state info
        new_df["countyfips"] = new_df["countyfips"].astype(str)
        new_df["program"] = new_df["program"].str.replace('-', '')
        baseacres_df["ST_CTY"] = baseacres_df["ST_CTY"].astype(str)

        new_df = new_df.merge(
            baseacres_df,
            how="left",
            left_on=["countyfips", "commodity", "program"],
            right_on=["ST_CTY", "Crop Name", "Program"]
        )

        # Split PLC values
        split_plc = {
            'Corn': np.array([0.564, 0.641, 0.564, 0.538, 0.564, 0.538, 0.513, 0.538, 0.538, 0.487]),
            'Peanuts': np.array([0.99] * 10),
            'Rice': np.array([0.99] * 10),
            'Cotton': np.array([0.995, 0.99, 0.99, 0.99, 0.995, 0.995, 0.99, 0.99, 0.99, 0.99]),
            'Soybeans': np.array([0.205, 0.205, 0.179, 0.205, 0.282, 0.282, 0.256, 0.256, 0.308, 0.282]),
            'Wheat': np.array([0.205, 0.308, 0.615, 0.718, 0.615, 0.538, 0.615, 0.615, 0.667, 0.692])
        }

        # Total Base Acres
        cbo_total_baseacres = {
            'Corn': np.array([94, 94.5, 94.5, 94.5, 94.5, 94.5, 94.5, 94.5, 94.5, 94.5]) * 1e6,
            'Peanuts': np.array([2.448] * 10) * 1e6,
            'Rice': np.array([4.646] * 10) * 1e6,
            'Cotton': np.array([10, 11.2, 12.2, 12.8, 12.8, 12.8, 12.8, 12.8, 12.8, 12.8]) * 1e6,
            'Soybeans': np.array([53.5] * 10) * 1e6,
            'Wheat': np.array([61.8] * 10) * 1e6
        }

        # Apply Enrolled Base
        new_df['Enrolled Base'] = new_df.apply(lambda row: self.assign_enrolled_base(row, split_plc, cbo_total_baseacres), axis=1)

        return new_df

    def generate_output(self, df, scenario):
        # Convert state names to abbreviations
        state_abbreviation_mapping = {
            "Alabama": "AL",
            "Alaska": "AK",
            "Arizona": "AZ",
            "Arkansas": "AR",
            "California": "CA",
            "Colorado": "CO",
            "Connecticut": "CT",
            "Delaware": "DE",
            "District of Columbia": "DC",
            "Florida": "FL",
            "Georgia": "GA",
            "Hawaii": "HI",
            "Idaho": "ID",
            "Illinois": "IL",
            "Indiana": "IN",
            "Iowa": "IA",
            "Kansas": "KS",
            "Kentucky": "KY",
            "Louisiana": "LA",
            "Maine": "ME",
            "Maryland": "MD",
            "Massachusetts": "MA",
            "Michigan": "MI",
            "Minnesota": "MN",
            "Mississippi": "MS",
            "Missouri": "MO",
            "Montana": "MT",
            "Nebraska": "NE",
            "Nevada": "NV",
            "New Hampshire": "NH",
            "New Jersey": "NJ",
            "New Mexico": "NM",
            "New York": "NY",
            "North Carolina": "NC",
            "North Dakota": "ND",
            "Ohio": "OH",
            "Oklahoma": "OK",
            "Oregon": "OR",
            "Pennsylvania": "PA",
            "Rhode Island": "RI",
            "South Carolina": "SC",
            "South Dakota": "SD",
            "Tennessee": "TN",
            "Texas": "TX",
            "Utah": "UT",
            "Vermont": "VT",
            "Virginia": "VA",
            "Washington": "WA",
            "West Virginia": "WV",
            "Wisconsin": "WI",
            "Wyoming": "WY"
        }
        df["state"] = df["State Name"].map(state_abbreviation_mapping)

        # Dictionary to store results by year
        result = {}

        for year, year_df in df.groupby("my"):
            # Convert year to an integer
            year_int = int(year)
            if year_int not in result:
                result[year_int] = []

            for state, state_df in year_df.groupby("state"):
                state_total_payment = 0
                state_counties = []

                for county, county_df in state_df.groupby("countyfips"):
                    county_scenarios = {}

                    for scenario, scenario_df in county_df.groupby("fbill"):
                        commodities = {}
                        county_total_payment = 0

                        for commodity, commodity_df in scenario_df.groupby("commodity"):
                            programs = []
                            commodity_total_baseacres = 0

                            for program, program_df in commodity_df.groupby("program"):
                                # Use mean enrolled base for calculation
                                baseacres = program_df.loc[program_df["attribute"] == "mean", "Enrolled Base"].iloc[0]
                                commodity_total_baseacres += baseacres

                                meanPaymentRateInDollarsPerAcre = program_df.loc[program_df["attribute"] == "mean", "value"].iloc[0]
                                medianPaymentRateInDollarsPerAcre = program_df.loc[program_df["attribute"] == "median", "value"].iloc[0]

                                program_payment = round(baseacres * meanPaymentRateInDollarsPerAcre, 2)
                                county_total_payment += program_payment

                                programs.append({
                                    "programName": program,
                                    "baseAcres": round(baseacres, 1),
                                    "meanPaymentRateInDollarsPerAcre": round(meanPaymentRateInDollarsPerAcre, 2),
                                    "medianPaymentRateInDollarsPerAcre": round(medianPaymentRateInDollarsPerAcre, 2),
                                    "totalPaymentInDollars": round(program_payment, 2)
                                })

                            commodities[commodity] = {
                                "commodityName": commodity,
                                "baseAcres": round(commodity_total_baseacres, 1),
                                "programs": programs
                            }

                        county_scenarios[scenario] = {
                            "scenarioName": scenario,
                            "totalPaymentInDollars": round(county_total_payment, 2),
                            "commodities": list(commodities.values())
                        }

                    state_total_payment += county_total_payment

                    state_counties.append({
                        "countyFIPS": county,
                        "scenarios": list(county_scenarios.values())
                    })

                result[year_int].append({
                        "state": state,
                        "totalPaymentInDollars": round(state_total_payment, 2),
                        "counties": state_counties
                    })

        # Convert to JSON format
        json_output = json.dumps(result, indent=2, sort_keys=True).encode('utf-8')  # Ensure it's encoded as bytes

        # Compress and save using GZIP
        with gzip.open(os.path.join(self.data_folder, f"arc_plc_payments_{scenario}.json.gz"), "wb") as gzip_file:
            gzip_file.write(json_output)

    # Assign 'Enrolled Base'
    def assign_enrolled_base(self, row, split_plc, cbo_total_baseacres):
        if pd.isna(row['my']):  # Handle missing years
            return np.nan

        try:
            year_index = int(row['my']) - 2024  # Convert to index
            if row['commodity'] in split_plc and 0 <= year_index < len(split_plc[row['commodity']]):
                if row['program'] == 'PLC':
                    return cbo_total_baseacres[row['commodity']][year_index] * split_plc[row['commodity']][year_index]
                elif row['program'] == 'ARCCO':
                    return cbo_total_baseacres[row['commodity']][year_index] * (1 - split_plc[row['commodity']][year_index])
        except (ValueError, KeyError, IndexError):
            return np.nan  # Handle unexpected errors safely

        return np.nan  # Default for unmatched cases


if __name__ == '__main__':
    # Get this data from the box folder, I left it out because these are very large files
    # TODO consider making these files input parameters so this can later be used as part of a workflow
    # NOTE: Since these are big files, please find them in Box
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB.csv"
    baseacres_commodity_data = "../title-1-commodities/arcplc_model/2024_enrolled_base_county_crop_program.csv"

    arcplc_parser = ArcPlcParser("../title-1-commodities/arcplc_model", current_farmbill_data, proposed_farmbill_data, baseacres_commodity_data)
    arcplc_parser.parse_and_process()
