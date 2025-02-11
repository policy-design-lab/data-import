import os
import pandas as pd

class ArcPlcParser:
    def __init__(self, data_folder, current_farmbill_data, proposed_farmbill_data):
        print("init ArcPlcParser")
        self.current_farmbill_data = current_farmbill_data
        self.proposed_farmbill_data = proposed_farmbill_data
        self.data_folder = data_folder

    def parse_and_process(self):

        # Read the output based on the current farm bill
        df1 = pd.read_csv(self.current_farmbill_data)
        # Read the output based on what's proposed
        df2 = pd.read_csv(self.proposed_farmbill_data)

        # Combine the two scenarios into a single data frame
        df = pd.concat([df1, df2], ignore_index=True)

        # Filter out so we have only the mean for PmtPerAc, other attributes aren't needed
        new_df = df[(df['attribute']=='mean') & (df['element']=='PmtPerAc')]
        # print(len(new_df))

        # For sanity check, print what's remaining to a CSV
        combined_data = os.path.join(self.data_folder, "combined_data.csv")
        new_df.to_csv(combined_data, index=False)

        # TODO - see the proposed API response format https://github.com/policy-design-lab/pdl-api/issues/305
        # TODO Combined.csv has both proposed and current farmbill (two scenarios), this needs to be converted into
        #  the proposed response
        # TODO Use base acres enrolled in each county/crop/program to compute total payments at the county level
        # TODO At the state level, combine the total payments to get the total payments for a state from ARC and PLC

if __name__ == '__main__':
    # Get this data from the box folder, I left it out because these are very large files
    # TODO consider making these files input parameters so this can later be used as part of a workflow
    current_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsCurrentFB.csv"
    proposed_farmbill_data = "../title-1-commodities/arcplc_model/CSVResultsProposedFB.csv"

    arcplc_parser = ArcPlcParser("../title-1-commodities/arcplc_model", current_farmbill_data, proposed_farmbill_data)
    arcplc_parser.parse_and_process()
