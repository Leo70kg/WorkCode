# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
import math
from WindPy import w
import pymysql
import datetime
import numpy as np
import pandas as pd
from DataStoreBase import DataStoreBase
import Util


class IndexMinuteData(DataStoreBase):

    def __init__(self, freq):

        self.freq = freq

    def connect2db(self):

        db = pymysql.connect(host='192.168.16.23', user='zuoyou', password='bhrsysp', db='indices_information',
                             charset='utf8')
        return db

    def create_new_table(self, db, table_name, condition):
        if condition:
            cursor = db.cursor()

            sql = """CREATE TABLE IF NOT EXISTS {:s}_{:d}minute(
            id int primary key auto_increment,
            TRADE_TIME datetime not null,
            TIME_STAMP int not null,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            LOG_RETURN float)
            ENGINE=myisam
            """.format(table_name, self.freq)

            cursor.execute(sql)

        else:
            pass

    def drop_existed_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """drop table if EXISTS {:s}_{:d}minute""".format(table_name, self.freq)
            cursor.execute(sql)

        else:
            pass

    def find_start_date(self, db, symbol, table_name):

        cursor = db.cursor()

        sql = """SELECT trade_time from {:s}_{:d}minute
                  where id=(select max(id) from {:s}_{:d}minute)""".format(table_name, self.freq, table_name, self.freq)

        row_num = cursor.execute(sql)

        if row_num == 0:
            start_date = datetime.datetime(2017, 1, 1)
        else:
            start_date = Util.datetime2date(cursor.fetchone()[0])

        return start_date

    @Util.deco1
    @Util.deco2
    def data_handle(self, symbol, end_date, create_condition, drop_condition):

        db = self.connect2db()
        cursor = db.cursor()

        name_lis = Util.get_code_split(symbol)
        table_name = name_lis[0]

        self.drop_existed_table(db, table_name, drop_condition)
        self.create_new_table(db, table_name, create_condition)

        start_date = self.find_start_date(db, symbol, table_name)

        future = w.wsi(symbol, 'open, high, low, close',
                       "{:%Y-%m-%d} 15:00:00".format(start_date), "{:%Y-%m-%d} 16:00:00".format(
                        end_date), "BarSize={:d}".format(self.freq))

        df = pd.DataFrame(future.Data[3], columns=['close'], index=future.Times)
        ret_df = np.log(df).diff(1).iloc[1:]

        sql3 = '''INSERT INTO {:s}_{:d}minute (TRADE_TIME, TIME_STAMP, OPEN, HIGH, LOW, CLOSE, LOG_RETURN
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''.format(table_name, self.freq)

        param = [(future.Times[i+1].strftime('%Y-%m-%d %H:%M:%S'),
                  future.Times[i+1].hour * 60 * 60 + future.Times[i+1].minute * 60,
                  future.Data[0][i+1], future.Data[1][i+1],
                  future.Data[2][i+1], future.Data[3][i+1],
                  None if math.isnan(ret_df.iloc[i, 0]) else float(ret_df.iloc[i, 0])
                  ) for i in xrange(len(future.Data[0][1:]))]

        cursor.executemany(sql3, param)

        db.commit()
        cursor.close()

    def batch_data_handle(self, symbols, end_date, create_condition=False, drop_condition=False):
        """symbols为各品种代码组成的列表"""
        for symbol in symbols:
            self.data_handle(symbol, end_date, create_condition, drop_condition)

