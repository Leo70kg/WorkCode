# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
import math
from WindPy import w
import pymysql
import datetime
import numpy as np
from DataStoreBase import DataStoreBase
import Util


class StockDailyData(DataStoreBase):

    def connect2db(self):

        db = pymysql.connect(host='192.168.16.23', user='zuoyou', password='bhrsysp', db='stock_db', charset='utf8')
        return db

    def create_new_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()
            sql = """CREATE TABLE IF NOT EXISTS {:s}_daily(
                id int primary key auto_increment,
                TRADE_DATE date not null,
                PRE_CLOSE float,
                OPEN float,
                HIGH float,
                LOW float,
                CLOSE float,
                VWAP float,
                PCT_CHG float,
                LOG_RETURN float,
                VOLUME int,
                AMT DECIMAL(20,4),
                DEALNUM int,
                TURN float,
                ADJFACTOR float, 
                TRADE_STATUS varchar(15),
                SUSP_DAYS int,
                SUSP_REASON varchar(150),
                MAX_UP_OR_DOWN int)
                ENGINE=myisam
                """.format(table_name)

            cursor.execute(sql)

        else:
            pass

    def drop_existed_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """drop table if EXISTS {:s}_daily""".format(table_name)
            cursor.execute(sql)

        else:
            pass

    def find_start_date(self, db, symbol, table_name):

        cursor = db.cursor()

        sql1 = """SELECT trade_date from {:s}_DAILY
                where id=(select max(id) from {:s}_DAILY)""".format(table_name, table_name)

        row_num = cursor.execute(sql1)

        if row_num == 0:

            ipo_date = w.wss(symbol, "ipo_date").Data[0][0]
            start_date = np.maximum(datetime.datetime(2011, 1, 4), ipo_date)
        else:
            last_date = Util.datetime2date(cursor.fetchone()[0])
            sql2 = """select TRADE_DATE from trade_date where id= 
                    (select id from trade_date where TRADE_DATE = '{:%Y-%m-%d}')+1""".format(last_date)

            cursor.execute(sql2)
            start_date = cursor.fetchone()[0]

        return start_date

    @Util.deco1
    @Util.deco2
    def data_handle(self, symbol, end_date, create_condition, drop_condition):

        db = self.connect2db()
        cursor = db.cursor()
        table_name = Util.get_code_split(symbol)[0]

        self.drop_existed_table(db, table_name, drop_condition)
        self.create_new_table(db, table_name, create_condition)

        start_date = self.find_start_date(db, symbol, table_name)
        stock = w.wsd(symbol, '''pre_close, open, high, low, close, vwap, pct_chg, volume, amt, dealnum, turn, 
                        adjfactor, trade_status, susp_days, susp_reason, maxupordown''', start_date, end_date, "")

        # Util.if_insert_date(db, table_name, stock)

        sql3 = '''INSERT INTO {:s}_DAILY (TRADE_DATE, PRE_CLOSE, OPEN, HIGH, 
                LOW, CLOSE, VWAP, PCT_CHG, LOG_RETURN, VOLUME, AMT, DEALNUM, TURN, ADJFACTOR, TRADE_STATUS, SUSP_DAYS, 
                SUSP_REASON, MAX_UP_OR_DOWN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s)'''.format(table_name)

        log_lis = []

        for i in xrange(len(stock.Data[0])):

            pre_close = stock.Data[0][i]
            close_price = stock.Data[4][i]

            adj_factor = stock.Data[11][i]
            date = stock.Times[i]
            last_date = Util.find_last_bd(cursor, date)

            if i == 0:
                pre_adj_factor = w.wsd(symbol, 'adjfactor', last_date, last_date, '').Data[0][0]

            else:
                pre_adj_factor = stock.Data[11][i - 1]

            try:
                log_ret = math.log(close_price * adj_factor / (pre_close * pre_adj_factor))

            except TypeError, e:
                print (e, date)
                log_ret = None

            log_lis.append(log_ret)

        param = [(stock.Times[i].strftime('%Y-%m-%d'), stock.Data[0][i], stock.Data[1][i],
                  stock.Data[2][i], stock.Data[3][i], stock.Data[4][i], stock.Data[5][i],
                  stock.Data[6][i]/100 if stock.Data[6][i] is not None else None,
                  log_lis[i], stock.Data[7][i], stock.Data[8][i], stock.Data[9][i],
                  stock.Data[10][i]/100 if stock.Data[10][i] is not None else None,
                  stock.Data[11][i], stock.Data[12][i], stock.Data[13][i], stock.Data[14][i],
                  stock.Data[15][i]) for i in xrange(len(stock.Data[0]))]

        cursor.executemany(sql3, param)

        db.commit()
        cursor.close()

    def batch_data_handle(self, symbols, end_date, create_condition=False, drop_condition=False):
        """symbols为各品种代码组成的列表"""
        for symbol in symbols:
            self.data_handle(symbol, end_date, create_condition, drop_condition)

