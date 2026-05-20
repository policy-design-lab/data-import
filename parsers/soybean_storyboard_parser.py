from pathlib import Path
import pandas as pd
from functools import reduce
import json

class SoybeanStoryboardParser:

    def __init__(self, pork_poultry_filepath, us_planted_acres_filepaths, br_planted_acres_filepath,
                 soybean_production_filepath, soybean_exports_filepath, soybean_imports_filepath, soybean_consumption_filepath):
        self.pork_poultry_filepath = pork_poultry_filepath
        self.us_planted_acres_filepaths = us_planted_acres_filepaths
        self.br_planted_acres_filepath = br_planted_acres_filepath
        self.soybean_production_filepath = soybean_production_filepath
        self.soybean_exports_filepath = soybean_exports_filepath
        self.soybean_imports_filepath = soybean_imports_filepath
        self.soybean_consumption_filepath = soybean_consumption_filepath

    def parse_china_demand(self, filepath: str) -> dict[str, pd.DataFrame]:
        df = pd.read_csv(filepath)

        china = df[df["Country_Name"] == "China"]

        # --- Pork ---
        pork = (
            china[
                (china["Commodity_Description"] == "Meat, Swine")
                & (china["Attribute_Description"] == "Domestic Consumption")
                & (china["Unit_Description"] == "(1000 MT CWE)")
                ][["Market_Year", "Value"]]
            .sort_values("Market_Year")
            .rename(columns={"Value": "Domestic_Consumption_1000MT_CWE"})
            .reset_index(drop=True)
        )

        # --- Poultry: prefer "Meat, Chicken" (1999-2026); fill earlier years from
        #     "Poultry, Meat, Broiler" (1987-2016) where Chicken data is absent ---
        chicken = (
            china[
                (china["Commodity_Description"] == "Meat, Chicken")
                & (china["Attribute_Description"] == "Domestic Consumption")
                & (china["Unit_Description"] == "(1000 MT)")
                ][["Market_Year", "Value"]]
            .sort_values("Market_Year")
            .rename(columns={"Value": "Meat_Chicken"})
            .set_index("Market_Year")
        )

        broiler = (
            china[
                (china["Commodity_Description"] == "Poultry, Meat, Broiler")
                & (china["Attribute_Description"] == "Domestic Consumption")
                & (china["Unit_Description"] == "(1000 MT)")
                ][["Market_Year", "Value"]]
            .sort_values("Market_Year")
            .rename(columns={"Value": "Poultry_Broiler"})
            .set_index("Market_Year")
        )

        poultry = (
            chicken.join(broiler, how="outer")
            .assign(
                Domestic_Consumption_1000MT=lambda x: x["Meat_Chicken"].combine_first(
                    x["Poultry_Broiler"]
                ),
                Source=lambda x: x["Meat_Chicken"]
                .notna()
                .map({True: "Meat, Chicken", False: "Poultry, Meat, Broiler"}),
            )[["Domestic_Consumption_1000MT", "Source"]]
            .reset_index()
            .rename(columns={"Market_Year": "Market_Year"})
        )

        return {"pork": pork, "poultry": poultry}

    def parse_us_planted_acres(self, us_planted_acres_filepaths):
        parent_path = "../storyboard/planted_acres/US"
        data_dir = Path(parent_path)

        planted_acres_dfs = []
        for file in us_planted_acres_filepaths:
            full_path = data_dir / file
            df = pd.read_csv(full_path)
            planted_acres_dfs.append(df)

        year = 2012
        soybeans_dfs = []
        for i, allcrops_df in enumerate(planted_acres_dfs):
            soybeans_df = allcrops_df[allcrops_df['Crop'] == 'SOYBEANS'].copy()
            soybeans_df['Year'] = year
            grouped_df = soybeans_df.groupby(['State', 'County', 'State County Code', 'Year'], as_index=False)[
                'Planted Acres'].sum()
            grouped_df['State'] = grouped_df['State'].str.strip().str.title()

            state_map = {
                'Concticut': 'Connecticut',
                'Masacuset': 'Massachusetts',
                'Misisippi': 'Mississippi',
                'No Caroln': 'North Carolina',  # Typo
                'No Dakota': 'North Dakota',  # Abbreviation
                'New Hamp': 'New Hampshire',  # City to State
                'Newjersey': 'New Jersey',
                'Penslvana': 'Pennsylvania',
                'Rhode Isl': 'Rhode Island',
                'S Carolin': 'South Carolina',
                'S Dakota': 'South Dakota',
                'Washngton': 'Washington',
                'West Va': 'West Virginia'
            }

            grouped_df['State'] = grouped_df['State'].replace(state_map)
            grouped_df.rename(columns={'Planted Acres': 'Total Planted Acres'}, inplace=True)
            grouped_df["State County Code"] = grouped_df["State County Code"].astype(int)

            soybeans_dfs.append(grouped_df)
            year = year + 1

        soybeans_combined = pd.concat(soybeans_dfs, ignore_index=True)

        return soybeans_combined

    def parse_br_planted_acres(self, br_planted_acres_filepath):

        # Brazil has data back to 1974 - this filter could be removed to return more years of data in the CSV
        target_years = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020,
                        2021, 2022, 2023, 2024]
        #soybeans_df = pd.read_csv(br_planted_acres_filepath, dtype={'Year': int})
        soybeans_df = pd.read_csv(br_planted_acres_filepath)
        soybeans_df = soybeans_df[
            (soybeans_df["Year"].isin(target_years)) & (soybeans_df["Attribute"] == "Harvested Area")]

        return soybeans_df

    def parse_market_balance(self, soybean_production_filepath, soybean_exports_filepath, soybean_imports_filepath,
                             soybean_consumption_filepath):
        # Load the data
        soy_production_df = pd.read_csv(soybean_production_filepath)
        soy_exports_df = pd.read_csv(soybean_exports_filepath)
        soy_imports_df = pd.read_csv(soybean_imports_filepath)
        soy_consumption_df = pd.read_csv(soybean_consumption_filepath)

        # Reshape form wide to long format so we can compute total imports worldwide
        soy_imports_df = soy_imports_df.melt(
            id_vars='Country',
            var_name='Year',
            value_name='importsMT'
        )

        # Compute percentage worldwide for the imports
        soy_imports_df['importPercentageWorldwide'] = (soy_imports_df['importsMT'] / soy_imports_df.groupby('Year')[
            'importsMT'].transform('sum') * 100
        )

        # Filter for the three countries
        countries = ['United States of America', 'Brazil', 'China', 'Argentina']
        production_filtered_df = soy_production_df[soy_production_df['Country'].isin(countries)]
        exports_filtered_df = soy_exports_df[soy_exports_df['Country'].isin(countries)]
        imports_filtered_df = soy_imports_df[soy_imports_df['Country'].isin(countries)]
        consumption_filtered_df = soy_consumption_df[soy_consumption_df['Country'].isin(countries)]

        # Reshape from wide to long format
        production_filtered_df = production_filtered_df.melt(
            id_vars='Country',
            var_name='Year',
            value_name='productionMT'
        )
        exports_filtered_df = exports_filtered_df.melt(
            id_vars='Country',
            var_name='Year',
            value_name='exportsMT'
        )

        consumption_filtered_df = consumption_filtered_df.melt(
            id_vars='Country',
            var_name='Year',
            value_name='consumptionMT'
        )
        
        soybean_market_balance_dfs = [production_filtered_df, exports_filtered_df, imports_filtered_df, consumption_filtered_df]

        soybean_market_balance_df = reduce(
            lambda left, right: left.merge(right, on=['Country', 'Year'], how='left'),
            soybean_market_balance_dfs
        )

        # Convert Year to integer
        soybean_market_balance_df['Year'] = soybean_market_balance_df['Year'].astype(int)

        soybean_market_balance_df = soybean_market_balance_df.sort_values(['Country', 'Year']).reset_index(drop=True)

        # Define beginning stocks for 1999
        beginning_stocks = {
            'United States of America': 9484000,
            'Brazil': 8086000,
            'China': 1904000,
            'Argentina': 7145000,
        }

        country_codes = {
            'United States of America': 'US',
            'Brazil': 'BR',
            'China': 'CN',
            'Argentina': 'AR',
        }
        soybean_market_balance_df['code'] = soybean_market_balance_df['Country'].map(country_codes)

        soybean_market_balance_df['Beginning_Stock'] = soybean_market_balance_df['Country'].map(beginning_stocks)

        # Compute ending stock row by row, carrying forward the previous year's ending stock
        def compute_ending_stocks(group):
            for i, idx in enumerate(group.index):
                if i == 0:
                    # First year: use the static beginning stock
                    beg = group.loc[idx, 'Beginning_Stock']
                else:
                    # Subsequent years: beginning stock = prior year's ending stock
                    beg = group.loc[prev_idx, 'endingStockMT']

                group.loc[idx, 'endingStockMT'] = (
                        beg
                        + group.loc[idx, 'productionMT']
                        + group.loc[idx, 'importsMT']
                        - group.loc[idx, 'exportsMT']
                        - group.loc[idx, 'consumptionMT']
                )
                prev_idx = idx
            return group

        soybean_market_balance_df = soybean_market_balance_df.groupby('Country', group_keys=False).apply(
            compute_ending_stocks)

        soybean_market_balance_df = soybean_market_balance_df.drop(columns=['Beginning_Stock'])

        # Add Production, Import, Exports and Consumption column in bushels
        TONS_TO_BUSHELS = 36.7437
        cols_to_convert = ['productionMT', 'importsMT', 'exportsMT', 'consumptionMT', 'endingStockMT']

        for col in cols_to_convert:
            soybean_market_balance_df[f'{col}Bushels'] = soybean_market_balance_df[col] * TONS_TO_BUSHELS

        # Final cleanup for column matching
        soybean_market_balance_df.rename(columns={'Country' : 'country', 'productionMTBushels': 'productionBushels',
                                                  'exportsMTBushels': 'exportsBushels', 'importsMTBushels':
                                                      'importsBushels', 'consumptionMTBushels': 'consumptionBushels',
                                                  'endingStockMTBushels': 'endingStockBushels'}, inplace=True)
        soybean_market_balance_df['commodityName'] = 'soybeans'

        soybean_market_balance_df = soybean_market_balance_df[['Year', 'country', 'code', 'commodityName',
                                                               'productionMT', 'productionBushels', 'exportsMT',
                                                               'exportsBushels', 'importsMT', 'importsBushels',
                                                               'consumptionMT', 'consumptionBushels',
                                                               'importPercentageWorldwide', 'endingStockMT',
                                                               'endingStockBushels']]

        return soybean_market_balance_df

    def parse_and_process(self):
        pork_poultry_demand = self.parse_china_demand(self.pork_poultry_filepath)
        pork_df = pork_poultry_demand["pork"]
        poultry_df = pork_poultry_demand["poultry"]

        # Update Column names
        pork_df.rename(columns={"Domestic_Consumption_1000MT_CWE": "pork_demand", "Market_Year": "Year"}, inplace=True)
        poultry_df.rename(columns={"Domestic_Consumption_1000MT": "poultry_demand", "Market_Year": "Year"},
                          inplace=True)

        # Merge into a single dataset
        pork_poultry_df = pd.merge(pork_df, poultry_df, how="outer", on="Year")
        pork_poultry_df = pork_poultry_df.drop(columns=["Source"])

        pork_poultry_df.to_csv("china_pork_poultry_demand.csv", index=False)

        us_soybeans_plantedacres_df = self.parse_us_planted_acres(self.us_planted_acres_filepaths)
        us_soybeans_plantedacres_df.to_csv("us_plantedacres_soybeans_2012_2025.csv", index=False)

        # Generate JSON response
        final_output = {}
        for year, group in us_soybeans_plantedacres_df.groupby('Year'):
            year_key = str(year)
            commodity_name = "Soybeans"
            planted_acres = []

            for _, row in group.iterrows():
                raw_name = str(row['County']).title()
                display_name = raw_name if "County" in raw_name else f"{raw_name} County"
                fips_code = str(row['State County Code']).zfill(5)

                planted_acres.append({
                    "id": fips_code,
                    "idType": "fips",
                    "level": "county",
                    "name": display_name,
                    "totalAcres": float(row['Total Planted Acres'])
                })

            final_output[year_key] = {
                "country": "United States",
                "code": "US",
                "commodity": commodity_name,
                "plantedAcres": planted_acres
            }

        with open('us_plantedacres_soybeans_2012_2025.json', 'w') as f:
            json.dump(final_output, f, indent=3)

        br_soybeans_plantedacres_df = self.parse_br_planted_acres(self.br_planted_acres_filepath)
        br_soybeans_plantedacres_df.rename(columns={'Value': 'Total Planted Acres'}, inplace=True)
        br_soybeans_plantedacres_df.to_csv("brazil_plantedacres_soybeans_2005_2024.csv", index=False)

        final_output = {}
        for year, group in br_soybeans_plantedacres_df.groupby('Year'):
            year_key = str(year)
            commodity_name = "Soybeans"
            planted_acres = []

            for _, row in group.iterrows():
                raw_name = str(row['Municipio&UF']).title()
                display_name = raw_name
                fips_code = str(row['MunicipioID'])

                planted_acres.append({
                    "id": fips_code,
                    "idType": "ibge",
                    "level": "municipality",
                    "name": display_name,
                    "totalAcres": float(row['Total Planted Acres']) * 2.47105
                })

            final_output[year_key] = {
                "country": "Brazil",
                "code": "BR",
                "commodity": commodity_name,
                "plantedAcres": planted_acres
            }

        # Latin character encoding - set utf-8
        with open('brazil_plantedacres_soybeans_2005_2024.json', 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=3)

        market_balance = self.parse_market_balance(self.soybean_production_filepath, self.soybean_exports_filepath,
                                                   self.soybean_imports_filepath, self.soybean_consumption_filepath)

        market_balance.to_csv("soybeans_marketbalance_data.csv", index=False)



if __name__ == '__main__':
    # Pork and Poultry Demand
    pork_poultry_data = "../storyboard/psd_livestock.csv"
    us_planted_acres_data = ['2012_fsa_acres_jan_2013.csv', '2013_fsa_acres_jan_2014.csv',
                             '2014_fsa_acres_jan2014.csv', '2015_fsa_acres_01052016.csv',
                             '2016_fsa_acres_010417.csv', '2017_fsa_acres_010418.csv', '2018_fsa_acres_012819.csv',
                             '2019_fsa_acres_web_010220.csv', '2020_fsa_acres_web_010521.csv',
                             '2021_fsa_acres_web_010322.csv', '2022_fsa_acres_web_010323.csv',
                             '2023_fsa_acres_web_010224.csv', '2024_fsa_acres_010225.csv', '2025_fsa_acres_web_010526.csv']

    br_planted_acres_data = "../storyboard/planted_acres/BR/Brazil_soy_data_city.csv"

    soybean_production = "../storyboard/market_balance/soybean_production_1999_2025.csv"
    soybean_exports = "../storyboard/market_balance/soybean_exports_1999_2025.csv"
    soybean_imports = "../storyboard/market_balance/soybean_imports_1999_2025.csv"
    soybean_consumption = "../storyboard/market_balance/soybean_domestic_consumption_1999_2025.csv"

    storyboard_parser = SoybeanStoryboardParser(pork_poultry_data, us_planted_acres_data, br_planted_acres_data,
                                                soybean_production, soybean_exports, soybean_imports, soybean_consumption)
    storyboard_parser.parse_and_process()


