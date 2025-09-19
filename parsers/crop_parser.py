# -*- coding: utf-8 -*-
"""
Crop Insurance Coverage Data Parser
- Parses ALL years (e.g., 1989–2024) and saves yearly .pkl for every year.
- Optionally exports yearly Excel only for a chosen set (e.g., 2014–2024).
- Always writes a combined pickle (sobcov.pkl) for all years.
- CSV export is configurable:
    * aggregate_to_ci=True  -> write a CI-style collapsed CSV
    * aggregate_to_ci=False -> write the raw combined SOB rows (filtered by csv_years)
- CI-style CSV computes loss_ratio AFTER aggregation: indemnity / premium.
"""

import os
import numpy as np
import pandas as pd
from typing import Optional, Tuple, Iterable


class CropParser:
    def __init__(
        self,
        start_year: int,
        end_year: int,
        data_folder: str,
        export_years: Optional[Iterable[int]] = None,  # years to also export as Excel
        output_filename: str = "sob-new.csv",              # CSV written under data_folder
        csv_years: Tuple[int, int] = (2014, 2024),     # year window for CSV export
        aggregate_to_ci: bool = True,                  # True => CI-style collapsed CSV
        state_name_from_abbr: bool = True,             # True => use full state names in CI output
        drop_all_other_counties: bool = True,          # drop "ALL OTHER COUNTIES" from CI output
    ):
        self.start_year = start_year
        self.end_year = end_year
        self.data_folder = data_folder
        self.export_years = set(export_years) if export_years else set()
        self.output_filename = output_filename
        self.csv_years = csv_years
        self.aggregate_to_ci = aggregate_to_ci
        self.state_name_from_abbr = state_name_from_abbr
        self.drop_all_other_counties = drop_all_other_counties

        # Column definitions (SOB raw, consistent across years)
        self.columns = [
            'year', 'state', 'stateAbr', 'county', 'countyNm', 'crop', 'cropNm',
            'plan', 'planNm', 'insType', 'del', 'cov',
            'polSold', 'polEarn', 'polIndem', 'unitEarn', 'unitIndem', 'acreTy',
            'acre', 'acreComp', 'liab', 'tot', 'sub', 'subPri', 'subAdd', 'subEFA',
            'pay', 'lossRatio'
        ]

        # Store processed yearly DataFrames
        self.yearly_data = {}
        self.combined_data = None

        # Abbreviation -> full state name (used if state_name_from_abbr=True)
        self.us_state_abbreviations = {
            'AL': 'Alabama','AK': 'Alaska','AZ': 'Arizona','AR': 'Arkansas','CA': 'California','CO': 'Colorado',
            'CT': 'Connecticut','DE': 'Delaware','FL': 'Florida','GA': 'Georgia','HI': 'Hawaii','ID': 'Idaho',
            'IL': 'Illinois','IN': 'Indiana','IA': 'Iowa','KS': 'Kansas','KY': 'Kentucky','LA': 'Louisiana',
            'ME': 'Maine','MD': 'Maryland','MA': 'Massachusetts','MI': 'Michigan','MN': 'Minnesota',
            'MS': 'Mississippi','MO': 'Missouri','MT': 'Montana','NE': 'Nebraska','NV': 'Nevada',
            'NH': 'New Hampshire','NJ': 'New Jersey','NM': 'New Mexico','NY': 'New York','NC': 'North Carolina',
            'ND': 'North Dakota','OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania',
            'RI': 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota','TN': 'Tennessee','TX': 'Texas',
            'UT': 'Utah','VT': 'Vermont','VA': 'Virginia','WA': 'Washington','WV': 'West Virginia',
            'WI': 'Wisconsin','WY': 'Wyoming','DC': 'District of Columbia','PR': 'Puerto Rico'
        }

    def _get_filename(self, year: int) -> str:
        """Determine filename pattern by year."""
        if year >= 2016:
            return f"sobcov_{year}.txt"
        else:
            # 2015 -> sobcov15.txt, etc.
            return f"sobcov{str(year)[-2:]}.txt"

    def parse_year(self, year: int) -> Optional[pd.DataFrame]:
        """Load and process a single year's file, save .pkl always, .xlsx optionally."""
        filename = self._get_filename(year)
        filepath = os.path.join(self.data_folder, filename)

        if not os.path.exists(filepath):
            print(f"⚠️ File missing: {filepath}")
            return None

        df = pd.read_csv(filepath, sep='|', header=None, names=self.columns)

        # Ensure numerics before computing fips
        for col in ["state", "county"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df['fips'] = (df['state'] * 1000 + df['county']).astype("Int64")

        # Always save yearly pickle
        pkl_name = os.path.join(self.data_folder, f"sobcov{year}.pkl")
        pd.to_pickle(df, pkl_name)

        # Save Excel only for export_years (e.g., 2014–2024)
        if year in self.export_years:
            xlsx_name = os.path.join(self.data_folder, f"sob{year}.xlsx")
            with pd.ExcelWriter(xlsx_name) as writer:
                df.to_excel(writer, sheet_name="sheet1", index=False)

        print(f"📂 Saved {year}: {df.shape[0]} rows")
        self.yearly_data[year] = df
        return df

    def _write_raw_csv_filtered(self, df_all: pd.DataFrame) -> str:
        """Write raw combined SOB filtered by csv_years (with headers)."""
        y0, y1 = self.csv_years
        filtered = df_all[df_all['year'].between(y0, y1, inclusive="both")].copy()

        outpath = os.path.join(self.data_folder, self.output_filename)
        filtered.to_csv(outpath, header=True, index=False)
        return outpath

    def _collapse_to_ci_and_write(self, df_all: pd.DataFrame) -> str:
        """
        Collapse to (year, state, county) and compute CI metrics.
        IMPORTANT:
          - Compute additive fields per-row first (farmer_premium, net_benefit), then sum.
          - Compute ratios AFTER aggregation (loss_ratio, benefit_by_pol, benefit_by_acre).
        """
        y0, y1 = self.csv_years
        sob = df_all[df_all['year'].between(y0, y1, inclusive="both")].copy()

        # Light text cleanup
        for c in ["stateAbr", "countyNm"]:
            if c in sob.columns:
                sob[c] = sob[c].astype(str).str.strip()

        # Optionally drop "ALL OTHER COUNTIES"
        if self.drop_all_other_counties and "countyNm" in sob.columns:
            sob = sob[sob["countyNm"].str.upper() != "ALL OTHER COUNTIES"].copy()

        # Prepare state/county text keys
        if self.state_name_from_abbr:
            sob["state_out"] = sob["stateAbr"].map(self.us_state_abbreviations).fillna(sob["stateAbr"])
        else:
            sob["state_out"] = sob["stateAbr"]
        sob["county_out"] = sob["countyNm"]

        # Coerce numeric columns used in aggregation/derivations
        for c in ["polEarn", "acre", "liab", "tot", "sub", "pay"]:
            sob[c] = pd.to_numeric(sob[c], errors="coerce").fillna(0)

        # ---- Row-wise additive fields first
        sob["farmer_premium_row"] = sob["tot"] - sob["sub"]
        sob["net_benefit_row"]   = sob["pay"] - sob["farmer_premium_row"]

        # ---- Aggregate (sum) to county-year
        agg = (
            sob.groupby(["year", "state_out", "county_out"], as_index=False)
               .agg(
                   policies_prem=("polEarn", "sum"),
                   acres_insured=("acre", "sum"),
                   liabilities=("liab", "sum"),
                   premium=("tot", "sum"),
                   subsidy=("sub", "sum"),
                   indemnity=("pay", "sum"),
                   farmer_premium=("farmer_premium_row", "sum"),
                   net_benefit=("net_benefit_row", "sum"),
               )
        ).rename(columns={"state_out": "state", "county_out": "county"})

        # Ratios AFTER aggregation
        agg["loss_ratio"] = np.where(agg["premium"].eq(0), np.nan, agg["indemnity"] / agg["premium"])
        agg["benefit_by_pol"] = np.where(agg["policies_prem"].eq(0), np.nan,
                                         agg["net_benefit"] / agg["policies_prem"])
        agg["benefit_by_acre"] = np.where(agg["acres_insured"].eq(0), np.nan,
                                          agg["net_benefit"] / agg["acres_insured"])

        # Column order to match CI files
        cols = [
            "year", "state", "county",
            "policies_prem", "acres_insured", "liabilities", "premium", "subsidy", "indemnity",
            "loss_ratio", "net_benefit", "farmer_premium", "benefit_by_pol", "benefit_by_acre"
        ]
        agg = agg.reindex(columns=cols)

        outpath = os.path.join(self.data_folder, self.output_filename)
        agg.to_csv(outpath, index=False)
        return outpath

    def parse_all_years(self) -> Optional[pd.DataFrame]:
        """Parse all years, write sobcov.pkl, and write CSV per configuration."""
        all_dfs = []
        for yr in range(self.start_year, self.end_year + 1):
            df = self.parse_year(yr)
            if df is not None:
                all_dfs.append(df)

        if not all_dfs:
            print(" No data parsed.")
            return None

        # Combined (all years)
        self.combined_data = pd.concat(all_dfs, ignore_index=True)

        # Save full pickle
        pd.to_pickle(self.combined_data, os.path.join(self.data_folder, "sobcov.pkl"))

        # CSV export
        if self.aggregate_to_ci:
            outpath = self._collapse_to_ci_and_write(self.combined_data)
            print(f"\n CI-style CSV written to {outpath} (years {self.csv_years[0]}–{self.csv_years[1]})")
        else:
            outpath = self._write_raw_csv_filtered(self.combined_data)
            print(f"\n Raw SOB CSV written to {outpath} (years {self.csv_years[0]}–{self.csv_years[1]})")

        return self.combined_data


if __name__ == "__main__":
    parser = CropParser(
        start_year=1989,
        end_year=2024,
        data_folder=os.path.join("..", "crop-insurance", "summary_of_business"),
        export_years=set(range(2014, 2015)),  # example: only export 2014 Excel
        output_filename="sob-new.csv",
        csv_years=(2014, 2024),
        aggregate_to_ci=True,                # compute CI-style output (county collapsed)
        state_name_from_abbr=True,           # True => full state names; False => abbreviations
        drop_all_other_counties=True,        # drop AOC to mirror Ryan's aggregation
    )
    parser.parse_all_years()
