import os

from data_parser import DataParser
from parsers.acep_parser import AcepParser
from parsers.dairy_disaster_parser import DairyDisasterParser
from parsers.eqip_ira_parser import EqipIraParser
from parsers.house_outlay_parser import HouseOutlayParser
from parsers.rcpp_parser import RcppParser

if __name__ == '__main__':
    commodities_data_parser = DataParser(2014, 2021, "Title 1: Commodities",
                                         "title-1-commodities", "title_1_version_1.csv",
                                         base_acres_csv_filename_arc_co="ARC-CO Base Acres by Program.csv",
                                         base_acres_csv_filename_plc="PLC Base Acres by Program.csv",
                                         farm_payee_count_csv_filename_arc_co="ARC-CO Recipients by Program.csv",
                                         farm_payee_count_csv_filename_arc_ic="ARC-IC Recipients by Program.csv",
                                         farm_payee_count_csv_filename_plc="PLC Recipients by Program.csv",
                                         total_payment_csv_filename_arc_co="ARC-CO.csv",
                                         total_payment_csv_filename_arc_ic="ARC-IC.csv",
                                         total_payment_csv_filename_plc="PLC.csv"
                                         )
    commodities_data_parser.format_title_commodities_data()
    commodities_data_parser.parse_and_process()

    crp_data_parser = DataParser(2018, 2022, "Title 2: Conservation: CRP",
                                 os.path.join("title-2-conservation", "crp"),
                                 "CRP_total_compiled_August_24_2023.csv")
    crp_data_parser.parse_and_process_crp()

    crop_insurance_data_parser = DataParser(2018, 2022, "Crop Insurance",
                                            "crop-insurance", "ci_state_year_benefits 8-28-23.csv")
    crop_insurance_data_parser.parse_and_process_crop_insurance()

    acep_data_parser = AcepParser(2018, 2022, "Title 2: Conservation: ACEP",
                                  os.path.join("title-2-conservation", "acep"),
                                  "ACEP.csv")

    acep_data_parser.parse_and_process()

    rcpp_data_parser = RcppParser(2018, 2022, "Title 2: Conservation: ACEP",
                                  os.path.join("title-2-conservation", "rcpp"),
                                  "RCPP.csv")

    rcpp_data_parser.parse_and_process()

    dairy_disaster_parser = DairyDisasterParser(2014, 2021, "Title 1: Commodities: Dairy and Disaster",
                                                "title-1-commodities", "Dairy-Disaster.csv")

    dairy_disaster_parser.parse_and_process()

    eqip_ira_parser = EqipIraParser("2023", 2024, 2031,
                                    "title-2-conservation/eqip_ira",
                                    "title-2-conservation/eqip_ira/Practice FIPS Download_modified.csv",
                                    "title-2-conservation/eqip_ira/2024_2031_EQIP_IRA_base2023_value.xls",
                                    "title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MIN-clean.xls",
                                    "title-2-conservation/eqip_ira/20231106-2024_2031-EQIPextrafund-project-by-practice-MAX-clean.xls",
                                    "title-2-conservation/eqip_ira/20240702_BA_EQIP_IRA_base2023_value.xlsx")
    eqip_ira_parser.parse_and_process()

    house_outlay_parser = HouseOutlayParser("2023", 2024, 2033,
                                    "title-2-conservation/house_outlay",
                                    "title-2-conservation/house_outlay/20241013_max_house_minus_baseline_ira_outlay.xlsx",
                                    "title-2-conservation/common/merged_practice_standards.csv")
    house_outlay_parser.parse_and_process()
