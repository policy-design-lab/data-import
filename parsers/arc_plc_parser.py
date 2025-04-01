import gzip
import json
import os

import numpy as np
import pandas as pd

import us

class ArcPlcParser:
    def __init__(self, data_folder, current_farmbill_data, proposed_farmbill_data):
        print("init ArcPlcParser")
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
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

        # Filter out so we have only the mean and median for PmtPerAc and TotalPmt, other attributes aren't needed
        new_df = df[(df['attribute'].isin(['mean', 'median'])) & (df['element'].isin(['PmtPerAc', 'TotalPmt']))]

        # Merge state info
        new_df["countyfips"] = new_df["countyfips"].astype(str).str.zfill(5)
        new_df["program"] = new_df["program"].str.replace('-', '')

        # Apply the function to get state abbreviation
        new_df['state'] = new_df['countyfips'].apply(self.fips_to_state_abbr).astype("string")

        return new_df

    def fips_to_state_abbr(self, fips):
        state = us.states.lookup(fips[:2])  # First two digits represent the state
        if state:
            state_abbr = state.abbr  # Get state abbreviation (e.g., 'IL' for Illinois)
            return state_abbr
        return None

    def generate_output(self, df, scenario):
        # sanity check
        df.to_csv(os.path.join(self.data_folder, f"sanity_check_{scenario}.csv"), index=False)

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

                            # First, gather all data and calculate baseacres
                            baseacres_list = []
                            for program, program_df in commodity_df.groupby("program"):
                                program_name = "ARC-CO" if program == "ARCCO" else program

                                meanPaymentRateInDollarsPerAcre = program_df.loc[
                                    (program_df["attribute"] == "mean") & (program_df["element"] == "PmtPerAc"),
                                    "value"
                                ].iloc[0]

                                medianPaymentRateInDollarsPerAcre = program_df.loc[
                                    (program_df["attribute"] == "median") & (program_df["element"] == "PmtPerAc"),
                                    "value"
                                ].iloc[0]

                                meanTotalPaymentInDollars = program_df.loc[
                                    (program_df["attribute"] == "mean") & (program_df["element"] == "TotalPmt"),
                                    "value"
                                ].iloc[0]

                                baseacres = meanTotalPaymentInDollars / meanPaymentRateInDollarsPerAcre if meanPaymentRateInDollarsPerAcre else 0
                                baseacres = 0 if np.isnan(baseacres) else baseacres

                                baseacres_list.append({
                                    "programName": program_name,
                                    "baseAcres": baseacres,
                                    "meanPaymentRateInDollarsPerAcre": meanPaymentRateInDollarsPerAcre,
                                    "medianPaymentRateInDollarsPerAcre": medianPaymentRateInDollarsPerAcre,
                                    "meanTotalPaymentInDollars": meanTotalPaymentInDollars
                                })

                                commodity_total_baseacres += baseacres

                            # Now create the final program data with percentages
                            for item in baseacres_list:
                                percentage = (item[
                                                  "baseAcres"] / commodity_total_baseacres * 100) if commodity_total_baseacres else 0
                                program_payment = round(item["meanTotalPaymentInDollars"], 2)
                                county_total_payment += program_payment

                                programs.append({
                                    "programName": item["programName"],
                                    "baseAcres": round(item["baseAcres"], 1),
                                    "percentageOfCommodityBaseAcres": round(percentage, 2),
                                    "meanPaymentRateInDollarsPerAcre": round(item["meanPaymentRateInDollarsPerAcre"],
                                                                             2),
                                    "medianPaymentRateInDollarsPerAcre": round(
                                        item["medianPaymentRateInDollarsPerAcre"], 2),
                                    "totalPaymentInDollars": program_payment
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


if __name__ == '__main__':
    # Get this data from the box folder, I left it out because these are very large files
    # TODO consider making these files input parameters so this can later be used as part of a workflow
    # NOTE: Since these are big files, please find them in Box
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB0204.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB0204.csv"

    arcplc_parser = ArcPlcParser("../title-1-commodities/arcplc_model", current_farmbill_data, proposed_farmbill_data)
    arcplc_parser.parse_and_process()
