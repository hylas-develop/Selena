import requests
from bs4 import BeautifulSoup
from url import Url
import pandas
from enum import Enum

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/39.0.2171.95 Safari/537.36'}  # Fake Browser Header


class TableKey(Enum):
    FINANCE_SUMMARY = 11


def read_krx_code():
    """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=' \
          'download&searchType=13'
    result = requests.get(url, headers=headers)
    krx = pandas.read_html(result.text, header=0)[0]
    krx = krx[['종목코드', '회사명']]
    krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
    krx.code = krx.code.map('{:06d}'.format)
    return krx


def get_url(url, ticker):
    """
    return target page's url
    - url : target page's url [url class constant]
    - ticker = target stock's ticker code
    """
    if Url.KR_FINANCE_STATEMENT == url:
        return f'{url}{ticker}'
    elif Url.KR_FINANCE_SUMMARY == url:
        return f'{url}{ticker}'


def get_page(url, company_info):
    """return pandas dataframes """
    print(f"Scrapping {company_info.company}'s Main Page.", end='\n')
    target_url = get_url(url, company_info.code)
    html = requests.get(target_url, headers=headers).text
    main_page_tables = pandas.read_html(html)
    return main_page_tables


def parse_main(company_info, main_dataframes):
    """
    Parsing Main Page
    11. IFRS(연결) Annual
    return dataframe dict
    :return dict
    """
    # TODO : 시가총액, 보통주, 베타, 거래량, 내부자 거래, 외인 및 기관 투자
    print(f"Scrapping {company_info.company}'s Main Page.", end='\n')
    main_dict = dict()
    for index, df in enumerate(main_dataframes):
        if index == TableKey.FINANCE_SUMMARY.value:
            fs_dataframe = parse_finance_summary(df)
            main_dict[TableKey.FINANCE_SUMMARY] = fs_dataframe
    return main_dict


def parse_finance_summary(finance_summary_dataframe):
    """
    :type finance_summary_dataframe: pandas.DataFrame
    """
    # Todo : Handling GAAP Tables. reference => 휴벡셀
    # link : http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A212310&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701
    try:
        finance_summary_dataframe.index = finance_summary_dataframe['IFRS(연결)']['IFRS(연결)'].tolist()
        finance_summary_dataframe.pop('IFRS(연결)')
        finance_summary_dataframe = finance_summary_dataframe.transpose()
        finance_summary_dataframe.index = map(lambda x: x[1].split(
            '/')[0], finance_summary_dataframe.index.tolist())
        finance_summary_dataframe = \
            finance_summary_dataframe.rename(
                columns={'매출액': 'sales', '영업이익(발표기준)': 'business_profit', '당기순이익': 'current_net_profit',
                         '지배주주순이익': 'control_net_profit', '비지배주주순이익': 'non_control_net_profit',
                         '자산총계': 'total_assets', '부채총계': 'total_debt', '자본총계': 'total_negative_debt', 'EPS(원)': 'EPS',
                         'BPS(원)': 'BPS', 'DPS(원)': 'DPS'})
        finance_summary_dataframe = finance_summary_dataframe[
            ['sales', 'business_profit', 'current_net_profit', 'control_net_profit', 'non_control_net_profit',
             'total_assets', 'total_debt', 'total_negative_debt', 'ROA', 'ROE', 'EPS', 'BPS', 'DPS', 'PER', 'PBR']]
        return finance_summary_dataframe
    except KeyError:
        return None


def parse_finance_statement(finance_dataframes):
    for index, df in enumerate(finance_dataframes):
        df.index = df['IFRS(연결)'].tolist()
        df.pop('IFRS(연결)')
        df = df.transpose()
        df = df.rename(columns={'매출액': 'sales', '매출원가': 'sales_cost', '매출총이익': 'sales_profit',
                                '판매비와관리비계산에 참여한 계정 펼치기': 'sell_manage_cost', '영업이익(발표기준)': 'business_profit',
                                '금융수익계산에 참여한 계정 펼치기': 'finance_profit', '금융원가계산에 참여한 계정 펼치기': 'finance_origin_cost',
                                '기타수익계산에 참여한 계정 펼치기': 'other_profit', '기타비용계산에 참여한 계정 펼치기': 'other_cost',
                                '세전계속사업이익': 'pre-tax_profit', '당기순이익': 'current_net_profit',
                                '지배주주순이익': 'control_net_profit'})
        df = df[['sales', 'sales_cost', 'sales_profit', 'sell_manage_cost', 'business_profit',
                 'finance_profit', 'other_cost', 'pre-tax_profit', 'current_net_profit', 'control_net_profit']]
        print(df)
        if index == 0 or index == 1:
            writer = pandas.ExcelWriter('output.xlsx')
            df.to_excel(writer, encoding='utf8')
            writer.save()
            return
            # df = df[["매출액", "매출원가", "판매비와관리비",
            #          "영업이익(발표기준)", "금융수익", "기타비용", "세전계속사업이익", "당기순이익", "지배주주순이익"]]
        elif index == 1:
            pass
        elif index == 1:
            pass
