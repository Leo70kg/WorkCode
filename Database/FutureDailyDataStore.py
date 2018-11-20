# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
from WindPy import w
import pymysql
import datetime
import numpy as np
from DataStoreBase import DataStoreBase
import Util
import math


class FutureDailyData(DataStoreBase):

    def connect2db(self):

        db = pymysql.connect(host='192.168.16.23', user='zuoyou', password='bhrsysp', db='future_information',
                             charset='utf8')
        return db

    def create_new_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """CREATE TABLE IF NOT EXISTS {:s}_DAILY(
            id int primary key auto_increment,
            TRADE_DATE date not null,
            PRE_CLOSE float,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            VWAP float,
            LOG_RETURN float,
            VOLUME int,
            OI int,
            OI_CHG int,
            TRADE_HISCODE varchar(15),
            ST_STOCK int)
            ENGINE=myisam
            """.format(table_name)

            cursor.execute(sql)

        else:
            pass

    def drop_existed_table(self, db, table_name, condition):

        if condition:
            cursor = db.cursor()

            sql = """drop table if EXISTS {:s}_DAILY""".format(table_name)
            cursor.execute(sql)

        else:
            pass

    def find_start_date(self, db, symbol, table_name):

        cursor = db.cursor()

        sql1 = """SELECT trade_date from {:s}_DAILY
                where id=(select max(id) from {:s}_DAILY)""".format(table_name, table_name)

        row_num = cursor.execute(sql1)

        if row_num == 0:

            ipo_date = w.wss(symbol, "contract_issuedate").Data[0][0]
            start_date = np.maximum(datetime.datetime(2013, 12, 31), ipo_date)
        else:
            last_date = Util.datetime2date(cursor.fetchone()[0])
            sql2 = """select TRADE_DATE from trade_date where id= 
                    (select id from trade_date where TRADE_DATE = '{:%Y-%m-%d}')""".format(last_date)

            cursor.execute(sql2)
            start_date = cursor.fetchone()[0]

        return start_date

    @Util.deco1
    @Util.deco2
    def add_field(self, symbol, field_name, field_type, data_list):
        """
        对日级别数据表添加字段并填充数据
        :param symbol: 标的代码
        :param field_name: 字段名称, str格式
        :param field_type: 字段数据格式, str格式
        :param data_list: 需要插入的数据列表
        :return:
        """
        db = self.connect2db()
        cursor = db.cursor()
        table_name = Util.get_code_split(symbol)[0]

        sql1 = """alter table {:s}_DAILY add {:s} {:s}""".format(table_name, field_name, field_type)
        cursor.execute(sql1)

        for i in range(len(data_list)):

            sql2 = """update {:s}_DAILY set {:s} = {:f} where id = {:d}""".format(table_name, field_name,
                                                                                  data_list[i], i+1)
            cursor.execute(sql2)

        db.commit()
        cursor.close()

    @Util.deco1
    @Util.deco2
    def data_handle(self, symbol, end_date, create_condition, drop_condition):

        db = self.connect2db()
        cursor = db.cursor()
        table_name = Util.get_code_split(symbol)[0]

        self.drop_existed_table(db, table_name, drop_condition)
        self.create_new_table(db, table_name, create_condition)

        start_date = self.find_start_date(db, symbol, table_name)

        future = w.wsd(symbol, '''pre_close, open, high, low, close, vwap,
                           volume, oi, oi_chg, trade_hiscode, st_stock''', start_date, end_date)

        Util.if_insert_date(db, table_name, future)

        sql3 = '''INSERT INTO {:s}_DAILY (TRADE_DATE, PRE_CLOSE, OPEN, HIGH, 
                LOW, CLOSE, VWAP, LOG_RETURN, VOLUME, OI, OI_CHG, TRADE_HISCODE, ST_STOCK) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''.format(table_name)

        log_lis = []

        for i in xrange(len(future.Data[0])):

            pre_close = future.Data[0][i]
            close_price = future.Data[4][i]
            date = future.Times[i].strftime('%Y-%m-%d')

            if i == 0:

                try:
                    log_ret = math.log(close_price / pre_close)
                except TypeError, e:
                    print (e, date)

                    log_ret = None

            else:

                if future.Data[9][i] == future.Data[9][i - 1]:
                    try:
                        log_ret = math.log(close_price / pre_close)
                    except TypeError, e:
                        print e, date

                        log_ret = None
                else:
                    data = w.wsd(future.Data[9][i], 'pre_close, close', date, date, '')
                    try:
                        log_ret = math.log(data.Data[1][0] / data.Data[0][0])
                    except TypeError, e:
                        print e, date

                        log_ret = None

            log_lis.append(log_ret)

        param = [(future.Times[i].strftime('%Y-%m-%d'), future.Data[0][i], future.Data[1][i],
                  future.Data[2][i], future.Data[3][i], future.Data[4][i], future.Data[5][i],
                  log_lis[i], future.Data[6][i], future.Data[7][i], future.Data[8][i],
                  future.Data[9][i], future.Data[10][i]) for i in xrange(1, len(future.Data[0]))]

        cursor.executemany(sql3, param)

        db.commit()
        cursor.close()

    def batch_data_handle(self, symbols, end_date, create_condition=False, drop_condition=False):
        """symbols为各品种代码组成的列表"""
        for symbol in symbols:
            self.data_handle(symbol, end_date, create_condition, drop_condition)

    def batch_add_field(self, symbols, field_name, field_type):

        db = self.connect2db()
        cursor = db.cursor()

        for symbol in symbols:
            table_name = Util.get_code_split(symbol)[0]
            sql = """select TRADE_DATE from {:s}_DAILY where id = 1""".format(table_name)
            cursor.execute(sql)
            start_date = cursor.fetchone()[0]

            data_list = w.wsd(symbol, 'st_stock', start_date, '2018-10-08').Data[0]

            self.add_field(symbol, field_name, field_type, data_list)

        db.commit()
        cursor.close()

"""************************************************************************************************************"""





