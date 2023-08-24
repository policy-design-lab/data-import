import os

from data_parser import DataParser

if __name__ == '__main__':
    # commodities_data_parser = DataParser(2014, 2021, "Title 1: Commodities",
    #                                      "title-1-commodities", "title_1_version_1.csv",
    #                                      base_acres_csv_filename_arc_co="ARC-CO Base Acres by Program.csv",
    #                                      base_acres_csv_filename_plc="PLC Base Acres by Program.csv",
    #                                      farm_payee_count_csv_filename_arc_co="ARC-CO Recipients by Program.csv",
    #                                      farm_payee_count_csv_filename_arc_ic="ARC-IC Recipients by Program.csv",
    #                                      farm_payee_count_csv_filename_plc="PLC Recipients by Program.csv",
    #                                      total_payment_csv_filename_arc_co="ARC-CO.csv",
    #                                      total_payment_csv_filename_arc_ic="ARC-IC.csv",
    #                                      total_payment_csv_filename_plc="PLC.csv"
    #                                      )
    # commodities_data_parser.format_title_commodities_data()
    # commodities_data_parser.parse_and_process()

    crp_data_parser = DataParser(2018, 2022, "Title 2: Conservation: CRP",
                                 os.path.join("title-2-conservation", "crp"),
                                 "CRP_total_compiled_August_24_2023.csv")
    crp_data_parser.parse_and_process_crp()
