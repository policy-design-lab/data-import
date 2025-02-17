import json
import os
import pandas as pd

class ArcPlcParser:
    def __init__(self, data_folder, current_farmbill_data, proposed_farmbill_data, baseacres_commodity_data):
        print("init ArcPlcParser")
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
        self.baseacres_commodity_data = baseacres_commodity_data
        self.data_folder = data_folder

    def parse_and_process(self):
        # Load CVS files
        df1 = pd.read_csv(self.current_farmbill_data)
        df2 = pd.read_csv(self.proposed_farmbill_data)
        df3 = pd.read_csv(self.baseacres_commodity_data)

        # Combine the two scenarios into a single data frame
        df = pd.concat([df1, df2], ignore_index=True)

        # Filter out so we have only the mean for PmtPerAc, other attributes aren't needed
        new_df = df[(df['attribute']=='mean') & (df['element']=='PmtPerAc')]

        # For sanity check, print what's remaining to a CSV
        combined_data = os.path.join(self.data_folder, "combined_data.csv")
        new_df.to_csv(combined_data, index=False)

        # Merge base acres and state info
        new_df["countyfips"] = new_df["countyfips"].astype(str)
        df3["ST_CTY"] = df3["ST_CTY"].astype(str)

        new_df = new_df.merge(
            df3,
            how="left",
            left_on=["countyfips", "commodity", "program"],
            right_on=["ST_CTY", "Crop Name", "Program"]
        )

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
        new_df["state"] = new_df["State Name"].map(state_abbreviation_mapping)

        # Dictionary to store results by year
        result = {}

        for year, year_df in new_df.groupby("my"):
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

                        # Compute mean and median PaymentRatePerAcre per commodity-program pair
                        payment_stats = scenario_df.groupby(["commodity", "program"])["value"].agg(
                            meanPaymentRateInDollarsPerAcre="mean",
                            medianPaymentRateInDollarsPerAcre="median"
                        ).reset_index()

                        # Merge computed mean and median rates back into scenario_df
                        scenario_df = scenario_df.merge(payment_stats, on=["commodity", "program"], how="left")

                        for commodity, commodity_df in scenario_df.groupby("commodity"):
                            programs = []
                            for program, program_df in commodity_df.groupby("program"):
                                program_payment = (program_df["Enrolled Base"] * program_df["value"]).sum()
                                county_total_payment += program_payment

                                programs.append({
                                    "programName": program,
                                    "baseAcres": program_df["Enrolled Base"].sum(),
                                    "meanPaymentRateInDollarsPerAcre": program_df["meanPaymentRateInDollarsPerAcre"].iloc[0],
                                    "medianPaymentRateInDollarsPerAcre":
                                        program_df["medianPaymentRateInDollarsPerAcre"].iloc[0],
                                    "totalPaymentInDollars": program_payment
                                })

                            commodities[commodity] = {
                                "commodityName": commodity,
                                "baseAcres": commodity_df["Enrolled Base"].sum(),
                                "programs": programs
                            }

                        county_scenarios[scenario] = {
                            "scenarioName": scenario,
                            "totalPaymentInDollars": county_total_payment,
                            "commodities": list(commodities.values())
                        }

                    state_total_payment += county_total_payment

                    state_counties.append({
                        "countyFIPS": county,
                        "scenarios": list(county_scenarios.values())
                    })

                result[year_int].append({
                    "state": state,
                    "totalPaymentInDollars": state_total_payment,
                    "counties": state_counties
                })

        # Convert to JSON format
        json_output = json.dumps(result, indent=2, sort_keys=True)

        with open(os.path.join(self.data_folder, "arc_pls_payments.json"), "w") as json_file:
            json_file.write(json_output)

if __name__ == '__main__':
    # Get this data from the box folder, I left it out because these are very large files
    # TODO consider making these files input parameters so this can later be used as part of a workflow
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB.csv"
    baseacres_commodity_data = "../title-1-commodities/baseacres_commodity_county_program.csv"

    arcplc_parser = ArcPlcParser("../title-1-commodities/arcplc_model", current_farmbill_data, proposed_farmbill_data, baseacres_commodity_data)
    arcplc_parser.parse_and_process()
