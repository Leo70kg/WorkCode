# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
from __future__ import print_function

import StockDailyDataStore
from WindPy import w
import datetime


if __name__ == '__main__':
    w.start()
    end_date = datetime.date(2018, 9, 11)

    symbols = w.wset("sectorconstituent","date=2018-09-11;sectorid=a001010100000000;field=wind_code,sec_name").Data[0][33:1000]
    a = StockDailyDataStore.StockDailyData()
    a.batch_data_handle(symbols, end_date, create_condition=True)
