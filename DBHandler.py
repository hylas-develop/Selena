import pymysql
import pandas
from datetime import datetime
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/39.0.2171.95 Safari/537.36'}  # Fake Browser Header


def create_db_value(db_value):
    if db_value != "null":
        return f"'{db_value}'"
    return db_value


class DBHandler:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root',
                                    password='0000', db='SIEVE', charset='utf8')
        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS finance_summary (
                code VARCHAR(20),
                date DATE,
                sales BIGINT(20),
                business_profit BIGINT(20),
                current_net_profit BIGINT(20),
                control_net_profit BIGINT(20),
                non_control_net_profit BIGINT(20),
                total_assets BIGINT(20),
                total_debt BIGINT(20),
                total_negative_debt BIGINT(20),
                last_update DATE,
                PRIMARY KEY (code,date))
            """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS finance_index (
                code VARCHAR(20),
                date DATE,
                ROA BIGINT(20),
                ROE BIGINT(20),
                EPS BIGINT(20),
                BPS BIGINT(20),
                DPS BIGINT(20),
                PBR BIGINT(20),
                PCR BIGINT(20),
                PER BIGINT(20),
                PSR BIGINT(20),
                PRGR BIGINT(20),
                EV_EBITA BIGINT(20),
                last_update DATE,
                PRIMARY KEY (code, date))
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()

    def update_finance_summary(self, company_info, finance_dataframe):
        """재무 요약 정보를 finance_summary 테이블에 업데이트 """
        if finance_dataframe is None:
            print(f"Received {company_info.company}'s Empty Rows.", end='\n')
            return
        print(f"Updating {company_info.company}'s Rows.", end='\n')
        finance_dataframe = finance_dataframe.fillna("null")
        with self.conn.cursor() as curs:
            sql = f"SELECT max(last_update) FROM finance_summary WHERE code = {company_info.code}"
            curs.execute(sql)
            rs = curs.fetchone()
            today = create_db_value(datetime.today().strftime('%Y-%m-%d'))
            ticker = create_db_value(company_info.code)
            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                for idx, row in finance_dataframe.iterrows():
                    date = create_db_value(f"{idx}-12-31")
                    sales = create_db_value(row['sales'])
                    business_profit = create_db_value(row['business_profit'])
                    current_net_profit = create_db_value(row['current_net_profit'])
                    control_net_profit = create_db_value(row['control_net_profit'])
                    non_control_net_profit = create_db_value(row['non_control_net_profit'])
                    total_assets = create_db_value(row['total_assets'])
                    total_debt = create_db_value(row['total_debt'])
                    total_negative_debt = create_db_value(row['total_negative_debt'])
                    sql = "REPLACE INTO finance_summary (code, date, sales, business_profit,"\
                        f"current_net_profit, control_net_profit, non_control_net_profit, total_assets, "\
                        f"total_debt, total_negative_debt, "\
                        f"last_update) VALUES ({ticker},{date}, {sales}, {business_profit}, {current_net_profit}"\
                        f",{control_net_profit}, {non_control_net_profit}, {total_assets}, {total_debt}"\
                        f",{total_negative_debt}, {today})"
                    curs.execute(sql)
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{int(idx)+1:04d} REPLACE INTO finance_summary "
                          f"VALUES ({date}, {sales}, {business_profit}, {current_net_profit}, {control_net_profit}, "
                          f"{non_control_net_profit}, {total_assets}, {total_debt}, {total_negative_debt} "
                          f",{today})", end='\r')
                os.system('cls')
                self.conn.commit()
                print('')

    def update_comp_info(self, krx):
        """종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pandas.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.now().strftime('%Y-%m-%d')
            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                # krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last"\
                        f"_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info "
                          f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('')
