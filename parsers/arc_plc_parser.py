import gzip
import json
import os
import numpy as np
import pandas as pd
import us

class ArcPlcParser:
    def __init__(self, fiscal_year, start_year, end_year,
                 data_folder, current_farmbill_data, proposed_farmbill_data, current_farmbill_obbba_data, proposed_farmbill_obbba_data):
        print("init ArcPlcParser")
        self.fiscal_year = fiscal_year
        self.start_year = start_year
        self.end_year = end_year
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
        self.current_farmbill_obbba_data = current_farmbill_obbba_data
        self.proposed_farmbill_obbba_data = proposed_farmbill_obbba_data
        self.data_folder = data_folder

    def parse_and_process(self):
        # Generate Current Json file
        current_df = self.process_df(self.current_farmbill_data)
        self.generate_output(current_df, "current")

        # Generate Proposed Json file
        proposed_df = self.process_df(self.proposed_farmbill_data)
        self.generate_output(proposed_df, "proposed")

        # Generate Current OBBBA Json file
        current_obbba_df = self.process_df(self.current_farmbill_obbba_data)
        self.generate_output(current_obbba_df, "current_obbba")

        # Generate Proposed OBBBA Json file
        current_obbba_df = self.process_df(self.current_farmbill_obbba_data)
        self.generate_output(current_obbba_df, "proposed_obbba")

        # Sanity Check
        # current_df.to_csv(os.path.join(self.data_folder, "sanity_check_current_df.csv"), index=False)
        # proposed_df.to_csv(os.path.join(self.data_folder, "sanity_check_proposed_df.csv"), index=False)

    def process_df(self, data_path):
        # Load & filter
        df = pd.read_csv(data_path)

        # Clean keys and add state
        df['fbill'] = df['fbill'].astype(str).str.strip()
        df['countyfips'] = df['countyfips'].astype(str).str.zfill(5)
        df['commodity'] = df['commodity'].astype(str).str.strip()
        df['commodity'] = df['commodity'].replace({'Cotton': 'Seed Cotton'})
        df['program'] = df['program'].str.strip()
        df['state'] = df['countyfips'].apply(self.fips_to_state_abbr).astype("string")

        # extract base acres (attribute="total", element="BaseAcres")
        base = (
            df.query("attribute=='total' and element=='BaseAcres'")
            .rename(columns={'value': 'Program_Base'})
                # keep fbill and state here
            [['my', 'fbill', 'state', 'countyfips', 'commodity', 'program', 'Program_Base']]
        )

        # total by county/commodity/fbill
        total = (
            base
            .groupby(['my', 'fbill', 'state', 'countyfips', 'commodity'], as_index=False)
            ['Program_Base']
            .sum()
            .rename(columns={'Program_Base': 'Total_Base'})
        )

        # payments rows
        pay = (
            df.query("attribute in ['mean','median'] and element in ['PmtPerAc','TotalPmt']")
            .loc[:, ['my', 'fbill', 'state', 'countyfips', 'commodity', 'program', 'attribute', 'element', 'value']]
        )

        # merge base acres and totals (preserves fbill & state)
        merged = (
            pay
            .merge(base, on=['my', 'fbill', 'state', 'countyfips', 'commodity', 'program'], how='left')
            .merge(total, on=['my', 'fbill', 'state', 'countyfips', 'commodity'], how='left')
        )

        # compute program share
        merged['Program_Share'] = np.where(
            merged['Total_Base'] > 0,
            merged['Program_Base'] / merged['Total_Base'],
            0
        )

        return merged

    def fips_to_state_abbr(self, fips):
        state = us.states.lookup(fips[:2])  # First two digits represent the state
        return state.abbr if state else None

    def generate_output(self, processed_df: pd.DataFrame, scenario_label: str):
        # Write a CSV sanity check
        sanity_path = os.path.join(self.data_folder, f"sanity_check_{scenario_label}.csv")
        processed_df.to_csv(sanity_path, index=False)

        # Container for JSON structure
        output_by_year: dict[int, list] = {}

        for year_str, year_group in processed_df.groupby("my"):
            model_year = int(year_str)
            if not (self.start_year <= model_year <= self.end_year):
                continue

            output_by_year.setdefault(model_year, [])

            for state_abbr, state_group in year_group.groupby("state"):
                state_county_records = []

                for county_fips, county_group in state_group.groupby("countyfips"):
                    scenario_records: dict[str, dict] = {}

                    for scenario_name, scenario_group in county_group.groupby("fbill"):
                        commodity_records: dict[str, dict] = {}

                        for commodity_name, commodity_group in scenario_group.groupby("commodity"):
                            program_entries: list[dict] = []
                            county_total_base_acres = commodity_group["Total_Base"].iloc[0]

                            for prog_key, prog_group in commodity_group.groupby("program"):
                                program_name = "ARC-CO" if prog_key == "ARCCO" else prog_key
                                base_acres = prog_group["Program_Base"].iloc[0]
                                pct_of_commodity = (
                                        base_acres / county_total_base_acres * 100) if county_total_base_acres else 0

                                mean_rate = prog_group.loc[
                                    (prog_group.attribute == "mean") & (prog_group.element == "PmtPerAc"),
                                    "value"
                                ].iloc[0]
                                median_rate = prog_group.loc[
                                    (prog_group.attribute == "median") & (prog_group.element == "PmtPerAc"),
                                    "value"
                                ].iloc[0]

                                total_pay = prog_group.loc[
                                    (prog_group.attribute == "mean") & (prog_group.element == "TotalPmt"),
                                    "value"
                                ].iloc[0]

                                program_entries.append({
                                    "programName": program_name,
                                    "baseAcres": round(base_acres, 2),
                                    "percentageOfCommodityBaseAcres": round(pct_of_commodity, 2),
                                    "meanPaymentRateInDollarsPerAcre": round(mean_rate, 2),
                                    "medianPaymentRateInDollarsPerAcre": round(median_rate, 2),
                                    "totalPaymentInDollars": round(total_pay, 2),
                                })

                            # only include this commodity if at least one program exists
                            if program_entries:
                                commodity_records[commodity_name] = {
                                    "commodityName": commodity_name,
                                    "baseAcres": round(county_total_base_acres, 2),
                                    "programs": program_entries
                                }

                        # only include this scenario if at least one commodity exists
                        if commodity_records:
                            total_payments_for_county = sum(
                                prog["totalPaymentInDollars"]
                                for com in commodity_records.values()
                                for prog in com["programs"]
                            )
                            scenario_records[scenario_name] = {
                                "scenarioName": scenario_name,
                                "totalPaymentInDollars": round(total_payments_for_county, 2),
                                "commodities": list(commodity_records.values())
                            }

                    # only include this county if at least one scenario exists
                    if scenario_records:
                        state_county_records.append({
                            "countyFIPS": county_fips,
                            "scenarios": list(scenario_records.values())
                        })

                # only include this state if at least one county exists
                if state_county_records:
                    total_state_payments = sum(
                        scen["totalPaymentInDollars"]
                        for c in state_county_records
                        for scen in c["scenarios"]
                    )
                    output_by_year[model_year].append({
                        "state": state_abbr,
                        "totalPaymentInDollars": round(total_state_payments, 2),
                        "counties": state_county_records
                    })

        # Write gzipped JSON
        json_bytes = json.dumps(output_by_year, indent=2, sort_keys=True).encode("utf-8")
        output_path = os.path.join(self.data_folder, f"arc_plc_payments_{scenario_label}.json.gz")
        with gzip.open(output_path, "wb") as gzip_file:
            gzip_file.write(json_bytes)


if __name__ == '__main__':
    # Get this data from the box folder, I left it out because these are very large files
    # TODO consider making these files input parameters so this can later be used as part of a workflow
    # NOTE: Since these are big files, please find them in Box
    fiscal_year = 2025
    start_year = 2025
    end_year = 2035
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB06-27-2025.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB06-27-2025.csv"
    current_farmbill_obbba_data = "../title-1-commodities/arcplc_model/CSVResultsOBBBAFB08-04-2025.csv"
    proposed_farmbill_obbba_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB08-04-2025.csv"

    arcplc_parser = ArcPlcParser(fiscal_year, start_year, end_year,
                                 "../title-1-commodities/arcplc_model",
                                 current_farmbill_data, proposed_farmbill_data, current_farmbill_obbba_data, proposed_farmbill_obbba_data)
    arcplc_parser.parse_and_process()
