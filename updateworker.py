import scrapper
from scrapper import TableKey
from url import Url
from DBHandler import DBHandler


# class UpdateWorker:
#     db = DBHandler()
#     krx = scrapper.read_krx_code()
#     db.update_comp_info(krx)
#     for ticker in db.codes:
#         finance_dataframes = scrapper.get_page(Url.KR_FINANCE_SUMMARY, ticker)
#         parsed_finance_dict = scrapper.parse_main(ticker, finance_dataframes)
#         db.update_finance_summary(ticker, parsed_finance_dict[TableKey.FINANCE_SUMMARY])


def test():
    db = DBHandler()
    krx = scrapper.read_krx_code()
    db.update_comp_info(krx)
    print(krx)
    for index, company in krx.iterrows():
        finance_dataframes = scrapper.get_page(Url.KR_FINANCE_SUMMARY, company)
        parsed_finance_dict = scrapper.parse_main(company, finance_dataframes)
        if TableKey.FINANCE_SUMMARY in parsed_finance_dict.keys():
            db.update_finance_summary(company, parsed_finance_dict[TableKey.FINANCE_SUMMARY])
            # Todo : Insert Financial Index to Database


test()
