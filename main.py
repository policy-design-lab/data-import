import os

from data_parser import DataParser

if __name__ == '__main__':
    # commodities_data_parser = DataParser(2018, 2022, "Title 1: Commodities",
    #                                      "title-1-commodities", "title_1_version_1.csv",
    #                                      base_acres_csv_filename="baseacres_commodity_county_program.csv",
    #                                      farm_payee_count_csv_filename="commodity_payments_counts.csv")
    # commodities_data_parser.parse_and_process()
    #
    # crop_insurance_data_parser = DataParser(2018, 2022, "Crop Insurance",
    #                                         "crop-insurance", "ci_state_year_benefits.csv")
    # crop_insurance_data_parser.parse_and_process_crop_insurance()

    crp_data_parser = DataParser(2018, 2022, "Title 2: Conservation: CRP",
                                 os.path.join("title-2-conservation", "crp"),
                                 "CRP_total_compiled_June_22_2023.csv")
    crp_data_parser.parse_and_process_crp()
