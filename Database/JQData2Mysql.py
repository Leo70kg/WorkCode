# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import division
from __future__ import print_function

from jqdatasdk import *
from Config import JqConfig
import datetime
import collections
from JQFutureDailyDataStore import FutureDailyData
from FactorDataStore import FactorDailyData
import FactorCal


def kline_write():

    auth(JqConfig.user.value, JqConfig.password.value)

    contracts_info = get_all_securities(types=['futures'], date=datetime.date.today())
    end = datetime.date.today()
    # contracts_info['start'] = contracts_info.start_date.apply(timestamp_to_date)
    # contracts_info['end'] = contracts_info.end_date.apply(timestamp_to_date)
    #
    # new_contracts_info = contracts_info[contracts_info.end <= datetime.date.today()]

    symbol_lis = contracts_info.index

    a = FutureDailyData()
    a.dominant_contract(end_date=datetime.date.today(), create_condition=False)
    a.batch_data_handle(symbol_lis, end, create_condition=True)


def factor_write():

    a = FactorDailyData()
    symbols = a._get_all_code()

    field_name_type = collections.OrderedDict([('atr14', 'float'), ('ma10', 'float'), ('dsi10', 'float')])
    factor_calc_func_dict = collections.OrderedDict([(FactorCal.TechnicalFactor.atr, 14),
                                                     (FactorCal.TechnicalFactor.moving_average, 10),
                                                     (FactorCal.TechnicalFactor.dsi, 10)])

    a.batch_data_handle(symbols, field_name_type, factor_calc_func_dict)


if __name__ == '__main__':
    kline_write()
