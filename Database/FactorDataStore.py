# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
from __future__ import print_function

from jqdatasdk import *
import pymysql
import re
import numpy
import collections
import pandas as pd
from sqlalchemy import create_engine
from DBUtils.PooledDB import PooledDB
from concurrent.futures import ThreadPoolExecutor, as_completed

from Config import JqConfig, MysqlConfig
from DataStoreBase import DataStoreBase
import Util


class FactorDailyData(DataStoreBase):

    host = MysqlConfig.host.value
    user = MysqlConfig.user.value
    password = MysqlConfig.password.value
    db1 = MysqlConfig.db1.value
    db2 = MysqlConfig.db5.value
    charset = 'utf8'

    def __init__(self):

        self.pool1 = PooledDB(pymysql, 3, host=self.host, user=self.user, passwd=self.password, db=self.db1,
                              charset=self.charset, use_unicode=True)

        self.pool2 = PooledDB(pymysql, 3, host=self.host, user=self.user, passwd=self.password, db=self.db2,
                              charset=self.charset, use_unicode=True)

        self.engine1 = self._get_engine(MysqlConfig.db1.value)   # 连接行情数据库
        self.engine2 = self._get_engine(MysqlConfig.db5.value)   # 连接因子数据库

    def connect2db(self):
        host = MysqlConfig.host.value
        user = MysqlConfig.user.value
        password = MysqlConfig.password.value
        db = MysqlConfig.db1.value

        con = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8')
        return con

    @classmethod
    def _get_engine(cls, db):

        engine = create_engine("{:s}://{:s}:{:s}@{:s}/{:s}?charset=utf8".format(MysqlConfig.engine.value,
                                                                                MysqlConfig.user.value,
                                                                                MysqlConfig.password.value,
                                                                                MysqlConfig.host.value,
                                                                                db))

        return engine

    def find_start_date(self, symbol, table_name):

        conn = self.pool2.connection()
        cursor = conn.cursor()

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

        cursor.close()
        conn.close()

        return start_date

    def create_new_table(self, table_name, old_field_name_type, field_name_type, condition):
        """field_name_type为需要添加的字段名称和该字段数据类型，dict形式"""

        conn = self.pool2.connection()

        if condition:

            cursor = conn.cursor()
            field = collections.OrderedDict(old_field_name_type.items() + field_name_type.items())

            try:
                field.pop('id')
                field.pop('trade_date')
            except:
                pass

            field_str = Util.str_expand(field)

            sql = """CREATE TABLE IF NOT EXISTS {:s}(
                    id int primary key auto_increment,
                    trade_date date not null,
                    """.format(table_name) + field_str + ")\nENGINE=myisam"
            cursor.execute(sql)
            cursor.close()
        else:
            pass
        conn.close()

    def _get_exist_field(self, table_name):
        """获取旧表中的字段名称和数据类型"""
        conn = self.pool2.connection()

        sql = """select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS 
                where table_name = '{:s}' and table_schema = 'future_contracts_daily_factor'""".format(table_name)

        dic = collections.OrderedDict()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            tu = cursor.fetchall()
            for item in tu:
                dic[item[0]] = item[1]

            cursor.close()
            conn.close()
            return dic

        except:
            conn.close()
            return dic

    def drop_existed_table(self, table_name, condition):

        conn = self.pool2.connection()
        if condition:
            cursor = conn.cursor()

            sql = """drop table if EXISTS {:s}""".format(table_name)
            cursor.execute(sql)
            cursor.close()
        else:
            pass

        conn.close()

    def _get_table_data(self, engine, table_name):
        """获取原数据表中的所有数据，在add_new_field中使用,
        如果原表不存在，输出None"""

        sql = """select * from {:s}""".format(table_name)
        try:
            kl_df = pd.read_sql(sql, engine)
            return kl_df

        except:
            return None

    def _calc_factor_value(self, kl_df, factor_calc_func, *param):
        """根据已有的因子计算公式进行因子计算，
        :return DataFrame形式"""

        try:
            value = factor_calc_func(kl_df, param)
            return value

        except:
            return

    def _insert_data(self, table_name, kl_df):

        conn = self.pool2.connection()
        cursor = conn.cursor()

        length = kl_df.shape[1]
        kl_df = kl_df.where(kl_df.notnull(), None)

        sql = "INSERT INTO {:s}_temp () VALUES ".format(table_name) + "(" + "%s, " * (length-1) + "%s)"

        param = []
        for i in xrange(kl_df.shape[0]):
            lis = kl_df.loc[i].tolist()

            for j in xrange(kl_df.shape[1]):
                temp_data = lis[j]
                if type(temp_data) == numpy.int64:
                    lis[j] = int(temp_data)

                elif type(temp_data) == numpy.float64:
                    lis[j] = float(temp_data)

                else:
                    pass

            param.append(tuple(lis))

        cursor.executemany(sql, param)

        cursor.close()
        conn.close()

    def _rename_table(self, table_name):

        conn = self.pool2.connection()
        cursor = conn.cursor()
        sql = 'alter table {:s}_temp rename {:s}'.format(table_name, table_name)

        cursor.execute(sql)
        cursor.close()
        conn.close()

    def _get_all_code(self):

        conn = self.pool1.connection()
        cursor = conn.cursor()

        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'future_contracts_daily'"
        cursor.execute(sql)

        symbols = cursor.fetchall()
        cursor.close()
        conn.close()

        return symbols

    def data_handle(self, symbol, field_name_type, factor_calc_func_dict):
        """
        对日级别数据表添加字段并填充数据,
        由于update效率比较低，当字段数量很大的时候，通过创建一个临时表，把旧表数据全部取出，
        通过insert语句统一将新旧数据写入临时表，之后删除旧表，对临时表更改名称
        :param symbol: 标的代码
        :param field_name_type: 字段名称和字段数据类型, dict格式
        :param factor_calc_func_dict: 因子计算函数和所需参数，dict形式
        :return:
        """
        if re.match('\w+\_\w+', symbol):
            pass

        else:
            print(symbol)
            table_name = symbol

            "获取旧表的字段名称和数据类型"
            old_field_name_type = self._get_exist_field(table_name)

            "获取因子旧表的所有数据"
            factor_df = self._get_table_data(self.engine2, table_name)

            "创建单个合约的因子表"
            if factor_df is None:
                self.create_new_table(table_name, old_field_name_type, field_name_type, condition=True)

            "创建添加字段的临时表"
            self.create_new_table(table_name+'_temp', old_field_name_type, field_name_type, condition=True)

            "获取行情数据"
            kl_df = self._get_table_data(self.engine1, table_name)

            if factor_df is None:
                factor_df = pd.DataFrame()
                factor_df['id'] = kl_df.id
                factor_df['trade_date'] = kl_df.trade_date

            "计算因子指标并插入数据库"
            for i in xrange(len(factor_calc_func_dict)):

                factor_value = self._calc_factor_value(kl_df, factor_calc_func_dict.keys()[i],
                                                       factor_calc_func_dict.values()[i])

                factor_df[field_name_type.keys()[i]] = factor_value

            self._insert_data(table_name, factor_df)
            self.drop_existed_table(table_name, condition=False)
            self._rename_table(table_name)

    @Util.deco1
    def batch_data_handle(self, symbols, field_name_type, factor_calc_func_dict):
        """symbols为各品种代码组成的列表"""

        with ThreadPoolExecutor() as executor:
            future_to_symbol = {executor.submit(self.data_handle, symbol[0], field_name_type, factor_calc_func_dict):
                                symbol for symbol in symbols}

            for future in as_completed(future_to_symbol):

                try:
                    future.result()
                except:
                    pass


"""**********************************************************************************"""

