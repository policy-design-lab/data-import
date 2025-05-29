import gzip
import json
import os
import numpy as np
import pandas as pd
import us

class ArcPlcParser:
    def __init__(self, fiscal_year, start_year, end_year,
                 data_folder, current_farmbill_data, proposed_farmbill_data, enrolled_base_county_crop_program):
        print("init ArcPlcParser")
        self.fiscal_year = fiscal_year
        self.start_year = start_year
        self.end_year = end_year
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
        self.enrolled_base_county_crop = enrolled_base_county_crop_program
        self.data_folder = data_folder

    def parse_and_process(self):
        # Reads th024 enrolled base file, combines ARCCO and PLC enrollment for each county & crop,
        # and returns a DataFrame with allocation shares.
        county_crop_base_share = self.calculate_allocation_shares(self.enrolled_base_county_crop)

        # Generate Current Json file
        current_df = self.process_df(self.current_farmbill_data, county_crop_base_share)
        self.generate_output(current_df, "current")

        # Generate Proposed Json file
        proposed_df = self.process_df(self.proposed_farmbill_data, county_crop_base_share)
        self.generate_output(proposed_df, "proposed")

        # Sanity Check
        # current_df.to_csv(os.path.join(self.data_folder, "sanity_check_current_df.csv"), index=False)
        # proposed_df.to_csv(os.path.join(self.data_folder, "sanity_check_proposed_df.csv"), index=False)

    def process_df(self, data_path, county_crop_base_share):
        # Load & filter
        df = pd.read_csv(data_path)
        new_df = (
            df.loc[
                df['attribute'].isin(['mean', 'median']) &
                df['element'].isin(['PmtPerAc', 'TotalPmt'])
                ]
            .copy()
        )

        # Clean keys
        new_df.loc[:, 'countyfips'] = new_df['countyfips'].astype(str).str.zfill(5)
        new_df.loc[:, 'program'] = new_df['program'].str.replace('-', '', regex=False)
        new_df.loc[:, 'state'] = new_df['countyfips'].apply(self.fips_to_state_abbr).astype("string")

        # Prep the enrollment‐base share frame
        ccbs = county_crop_base_share.copy()
        ccbs.loc[:, 'ST_CTY'] = ccbs['ST_CTY'].astype(str).str.zfill(5)
        ccbs.loc[:, 'Crop'] = ccbs['Crop'].replace({
            'Grain Sorghum': 'Sorghum',
            'Seed Cotton': 'Cotton',
            'Rice_Long Grain': 'Rice',
            'Rice_Med/Short Grain': 'Rice',
            'Rice_Temperate Japonica': 'Rice'
        })
        # ensure the program column matches exactly (e.g. 'PLC' / 'ARCCO')
        ccbs.loc[:, 'Program'] = ccbs['Program']  # no change, but call out that this is needed

        # Merge by FIPS + crop + program
        merged = new_df.merge(
            ccbs[[
                'ST_CTY', 'Crop', 'Program',
                'Total_Base', 'Share',
                'Program_Base', 'Program_Share'
            ]],
            how='left',
            left_on=['countyfips', 'commodity', 'program'],
            right_on=['ST_CTY', 'Crop', 'Program']
        )

        # Fill null base acre values with 0
        for col in ['Total_Base', 'Share', 'Program_Base', 'Program_Share']:
            merged[col] = merged[col].fillna(0)

        # Drop the helper columns from the join
        merged = merged.drop(columns=['ST_CTY', 'Crop', 'Program'])

        return merged

    def fips_to_state_abbr(self, fips):
        state = us.states.lookup(fips[:2])  # First two digits represent the state
        return state.abbr if state else None

    def calculate_allocation_shares(self, path, sheet_name=0):
        # Load data
        df = pd.read_excel(
            path,
            sheet_name=sheet_name,
            skiprows=2,
            header=None,
            names=['ST_CTY', 'State', 'County', 'Crop', 'Program', 'Enrolled_Base']
        )

        # Drop blank rows & coerce types
        df = df.dropna(subset=['ST_CTY'])
        df['Enrolled_Base'] = pd.to_numeric(df['Enrolled_Base'], errors='coerce').fillna(0)

        # Zero‐pad ST_CTY to 5 digits (if it isn't already)
        df['ST_CTY'] = df['ST_CTY'].astype(str).str.zfill(5)

        # Program level base acres
        prog = (
            df.groupby(
                ['ST_CTY', 'State', 'County', 'Crop', 'Program'],
                as_index=False
            )['Enrolled_Base']
            .sum()
            .rename(columns={'Enrolled_Base': 'Program_Base'})
        )

        # Total base acres: Sum ARCCO + PLC per county‐crop (and preserve ST_CTY)
        total = (
            prog.groupby(
                ['ST_CTY', 'State', 'County', 'Crop'],
                as_index=False
            )['Program_Base']
            .sum()
            .rename(columns={'Program_Base': 'Total_Base'})
        )

        # merge & compute shares
        out = prog.merge(total, on=['ST_CTY', 'State', 'County', 'Crop'])
        out['Share'] = np.where(out['Total_Base'] > 0,
                                out['Total_Base'] / out.groupby('Crop')['Total_Base'].transform('sum'),
                                0)
        out['Program_Share'] = np.where(out['Total_Base'] > 0,
                                        out['Program_Base'] / out['Total_Base'],
                                        0)

        return out

    def generate_output(self, processed_df: pd.DataFrame, scenario_label: str):
        # Write a CSV sanity check
        sanity_path = os.path.join(self.data_folder, f"sanity_check_{scenario_label}.csv")
        processed_df.to_csv(sanity_path, index=False)

        # ARC-PLC split fractions and CBO national totals for years 2026–2036
        split_plc = {
            'Corn': np.array([0.667, 0.641, 0.615, 0.59, 0.513, 0.538, 0.564, 0.59, 0.513, 0.564, 0.128]),
            'Soybeans': np.array([0.359, 0.308, 0.333, 0.462, 0.359, 0.256, 0.282, 0.256, 0.282, 0.256, 0.231]),
            'Wheat': np.array([0.487, 0.615, 0.692, 0.59, 0.641, 0.692, 0.692, 0.692, 0.718, 0.718, 0.718]),
            'Cotton': np.array([0.995] * 9 + [0.99, 0.99]),
            'Peanuts': np.array([0.99] * 11),
            'Rice': np.array([0.99] * 11),
            'Sorghum': np.array([.744, .769, .795, .821, .744, .692, .718, .744, .795, .795, .795]),
        }
        cbo_totals = {
            'Corn': np.full(11, 94.5e6),
            'Soybeans': np.full(11, 53.5e6),
            'Wheat': np.full(11, 61.8e6),
            'Peanuts': np.full(11, 2.451e6),
            'Rice': np.full(11, 4.646e6),
            'Sorghum': np.full(11, 8.5e6),
            'Cotton': np.array([11.2, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12]) * 1e6,
        }

        START_YEAR = self.fiscal_year
        PROJ_FIRST_YEAR = self.start_year
        PROJECTION_YEARS = self.end_year - self.start_year + 1

        # Container for JSON structure
        output_by_year: dict[int, list] = {}

        for year_str, year_group in processed_df.groupby("my"):
            model_year = int(year_str)
            output_by_year.setdefault(model_year, [])

            if model_year == START_YEAR:
                use_enrolled_base = True
            else:
                proj_index = model_year - PROJ_FIRST_YEAR
                if proj_index < 0 or proj_index >= PROJECTION_YEARS:
                    # skip years outside 2026–2036
                    continue
                use_enrolled_base = False

            for state_abbr, state_group in year_group.groupby("state"):
                state_county_records = []

                for county_fips, county_group in state_group.groupby("countyfips"):
                    scenario_records: dict[str, dict] = {}

                    for scenario_name, scenario_group in county_group.groupby("fbill"):
                        commodity_records: dict[str, dict] = {}

                        for commodity_name, commodity_group in scenario_group.groupby("commodity"):
                            program_entries: list[dict] = []

                            if use_enrolled_base:
                                # 2025: take actual enrolled values
                                county_total_base_acres = commodity_group["Total_Base"].iloc[0]
                                for prog_key, prog_group in commodity_group.groupby("program"):
                                    program_name = "ARC-CO" if prog_key == "ARCCO" else prog_key
                                    base_acres = prog_group["Program_Base"].iloc[0]
                                    pct_of_commodity = prog_group["Program_Share"].iloc[0] * 100

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

                            else:
                                # 2026+: project from CBO totals + split fractions
                                share_fraction = commodity_group["Share"].iloc[0]
                                county_total_base_acres = share_fraction * cbo_totals[commodity_name][proj_index]

                                # detect how many programs were present in 2024
                                programs_present = commodity_group["program"].unique().tolist()
                                single_prog = (len(programs_present) == 1)

                                for prog_key, prog_group in commodity_group.groupby("program"):
                                    program_name = "ARC-CO" if prog_key == "ARCCO" else prog_key

                                    if single_prog:
                                        base_acres = county_total_base_acres
                                        pct_of_commodity = 100.0
                                    else:
                                        plc_frac = split_plc[commodity_name][proj_index]
                                        frac = plc_frac if program_name == "PLC" else (1 - plc_frac)
                                        base_acres = county_total_base_acres * frac
                                        pct_of_commodity = (base_acres / county_total_base_acres * 100) if county_total_base_acres else 0

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
    start_year = 2026
    end_year = 2036
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB05-28-2025.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB05-28-2025.csv"
    enrolled_base_county_crop_program = "../title-1-commodities/arcplc_model/2024_enrolled_base_county_crop_program.xlsx"

    arcplc_parser = ArcPlcParser(fiscal_year, start_year, end_year,
                                 "../title-1-commodities/arcplc_model",
                                 current_farmbill_data, proposed_farmbill_data, enrolled_base_county_crop_program)
    arcplc_parser.parse_and_process()
