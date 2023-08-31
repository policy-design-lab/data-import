from data_parser import DataParser

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

    crop_insurance_data_parser = DataParser(2018, 2022, "Crop Insurance",
                                            "crop-insurance", "ci_state_year_benefits 8-28-23.csv")
    crop_insurance_data_parser.parse_and_process_crop_insurance()
