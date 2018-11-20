# -*- coding: utf-8 -*-
# Leo70kg
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from enum import Enum


class JqConfig(Enum):
    user = '13186165953'
    password = 'bhrsysp'


class MysqlConfig(Enum):
    engine = 'mysql+pymysql'
    host = '192.168.16.23'
    user = 'zuoyou'
    password = 'bhrsysp'
    db1 = 'future_contracts_daily'
    db2 = 'future_contracts_1minute'
    db3 = 'future_contracts_5minute'
    db4 = 'future_contracts_15minute'
    db5 = 'future_contracts_daily_factor'
