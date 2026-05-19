import pandas as pd

class SoybeanStoryboardParser:

    def __init__(self, pork_poultry_filepath):
        self.pork_poultry_filepath = pork_poultry_filepath

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


if __name__ == '__main__':
    # Pork and Poultry Demand
    pork_poultry_data = "../storyboard/psd_livestock.csv"

    storyboard_parser = SoybeanStoryboardParser(pork_poultry_data)
    storyboard_parser.parse_and_process()


