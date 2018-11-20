# -*- coding: utf-8 -*-
# Leo70kg

"""从聚宽数据源获取期货日线数据并保存"""

from __future__ import division
from __future__ import print_function

from jqdatasdk import *
import pymysql
import re
import numpy as np
import datetime
from Config import JqConfig, MysqlConfig
from DataStoreBase import DataStoreBase
import Util


class FutureDailyData(DataStoreBase):

    def connect2db(self):
        host = MysqlConfig.host.value
        user = MysqlConfig.user.value
        password = MysqlConfig.password.value
        db = MysqlConfig.db1.value

        con = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8')
        return con

    def create_new_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """CREATE TABLE IF NOT EXISTS {:s}(
            id int primary key auto_increment,
            trade_date date not null,
            open float(12,4),
            close float(12,4),
            high float(12,4),
            low float(12,4),
            vwap float(12,4),
            volume float(12,4),
            amount float(12,6),
            pct_chg float(12,6),
            log_return float(12,6)
            )
            ENGINE=myisam
            """.format(table_name)

            cursor.execute(sql)

        else:
            pass

    def drop_existed_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """drop table if EXISTS {:s}""".format(table_name)
            cursor.execute(sql)

        else:
            pass

    def find_start_date(self, db, symbol, table_name):

        cursor = db.cursor()

        sql1 = """SELECT trade_date from {:s}
                where id=(select max(id) from {:s})""".format(table_name, table_name)

        row_num = cursor.execute(sql1)

        if row_num == 0:

            start_date = get_security_info(symbol).start_date
        else:
            last_date = Util.datetime2date(cursor.fetchone()[0])
            sql2 = """select trade_date from trade_date where id= 
                    (select id from trade_date where trade_date = '{:%Y-%m-%d}')+1""".format(last_date)

            cursor.execute(sql2)
            start_date = cursor.fetchone()[0]

        return start_date

    @Util.deco1
    def dominant_contract(self, end_date, create_condition):
        db = self.connect2db()
        cursor = db.cursor()

        all_contracts = get_all_securities(types=['futures'], date=datetime.date.today())
        code_lis = all_contracts.index
        lis = []
        for i in code_lis:
            code = re.search('\D+', i).group()
            lis.append(code)
        dominant_code = set(lis) - set(['ER', 'FB', 'GN', 'JR', 'LR', 'PM', 'RI', 'RS', 'WS', 'WT', 'RO', 'TC', 'ME'])
        dominant_code = list(dominant_code)

        if create_condition:
            sql = Util.repeat_str(dominant_code)

            cursor.execute(sql)

            date_lis = get_trade_days(start_date=datetime.date(2005, 1, 1), end_date=end_date)
        else:

            sql1 = """SELECT trade_date from hiscode
                    where id=(select max(id) from hiscode)"""

            cursor.execute(sql1)
            last_date = Util.datetime2date(cursor.fetchone()[0])

            sql2 = """select trade_date from trade_date where id= 
                    (select id from trade_date where trade_date = '{:%Y-%m-%d}')+1""".format(last_date)

            cursor.execute(sql2)
            start_date = cursor.fetchone()[0]

            date_lis = get_trade_days(start_date=start_date, end_date=end_date)

        date_lis_split = [date_lis[i:i+100] for i in range(0, len(date_lis), 100)]

        for date_lis in date_lis_split:
            all_insert_data = []
            for date in date_lis:

                contract_code_lis = [date]
                print(date)
                for code in dominant_code:
                    if code == 'MA':
                        if date < datetime.date(2014, 6, 17):
                            contract_code = get_dominant_future('ME', date).replace('ME', 'MA')
                        else:
                            contract_code = get_dominant_future(code, date)

                    elif code == 'ZC':
                        if date < datetime.date(2015, 5, 18):
                            contract_code = get_dominant_future('TC', date).replace('TC', 'ZC')
                        else:
                            contract_code = get_dominant_future(code, date)
                    else:
                        contract_code = get_dominant_future(code, date)

                    contract_code_lis.append(contract_code)
                    line_insert_data = tuple(contract_code_lis)

                all_insert_data.append(line_insert_data)

            sql = """INSERT INTO hiscode(trade_date, """

            for i in xrange(len(dominant_code)):

                if i == len(dominant_code) - 1:
                    st = """{:s}9999) """.format(dominant_code[i])

                else:
                    st = """{:s}9999, """.format(dominant_code[i])
                sql = sql + st

            sql = sql + 'VALUES (%s, '

            for i in xrange(len(dominant_code)):

                if i == len(dominant_code) - 1:
                    st = "%s)"

                else:
                    st = "%s, "
                sql = sql + st

            cursor.executemany(sql, all_insert_data)

        db.commit()
        cursor.close()

    @Util.deco1
    def data_handle(self, symbol, contract_multiplier, end_date, create_condition, drop_condition):

        db = self.connect2db()
        cursor = db.cursor()
        if re.search('\D+', symbol.split('.')[0]).group() == 'TC':
            table_name = 'ZC' + re.search('\d+', symbol.split('.')[0]).group()

        elif re.search('\D+', symbol.split('.')[0]).group() == 'ME':
            table_name = 'MA' + re.search('\d+', symbol.split('.')[0]).group()

        else:
            table_name = Util.get_code_split(symbol)[0]

        self.drop_existed_table(db, table_name, drop_condition)
        self.create_new_table(db, table_name, create_condition)

        start_date = Util.find_last_bd(cursor, self.find_start_date(db, symbol, table_name))

        kl_df = get_price(symbol, start_date=start_date, end_date=end_date, frequency='daily')
        kl_df['volume'] = kl_df['volume'].replace(0, np.nan)
        kl_df['trade_date'] = kl_df.index
        kl_df['vwap'] = kl_df['money'] / (kl_df['volume'] * contract_multiplier)
        kl_df['pct_chg'] = kl_df['close'].diff(1) / kl_df['close'].shift(1)
        kl_df['log_return'] = np.log(kl_df['close'] / kl_df['close'].shift(1))
        kl_df['money'] = kl_df['money'] / 1000000

        kl_df = kl_df.where(kl_df.notnull(), None)
        kl_df = kl_df.iloc[1:, :]
        # Util.if_insert_date(db, table_name, future)

        sql3 = '''INSERT INTO {:s} (trade_date, open, close, high, 
                low, vwap, volume, amount, pct_chg, log_return) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''.format(table_name)

        param = [(kl_df.index[i].to_pydatetime().date().strftime('%Y-%m-%d'), kl_df.open[i],
                  kl_df.close[i], kl_df.high[i], kl_df.low[i], kl_df.vwap[i], kl_df.volume[i],
                  kl_df.money[i], kl_df.pct_chg[i], kl_df.log_return[i]) for i in xrange(kl_df.shape[0])]

        cursor.executemany(sql3, param)

        db.commit()
        cursor.close()

    def batch_data_handle(self, symbols, end_date, create_condition=False, drop_condition=False):
        """symbols为各品种代码组成的列表"""

        info_dict = Util.load_future_info()

        for i in xrange(len(symbols)):
            print(symbols[i])
            if re.search('\D+', symbols[i].split('.')[0]).group() in ['ER', 'FB', 'GN', 'JR', 'LR', 'PM', 'RI', 'RS',
                                                                      'WS', 'WT', 'RO']:
                pass

            else:
                if re.search('\D+', symbols[i].split('.')[0]).group() == 'TC':
                    multiplier = info_dict['ZC'][1]

                elif re.search('\D+', symbols[i].split('.')[0]).group() == 'ME':
                    multiplier = info_dict['MA'][1]

                else:
                    multiplier = info_dict[re.search('\D+', symbols[i].split('.')[0]).group()][1]

                self.data_handle(symbols[i], multiplier, end_date, create_condition, drop_condition)

"""************************************************************************************************************"""






